package nbd

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"slices"
	"strings"
	"testing"
	"time"
)

const nbdTestSize = 16 << 20

func nbdDial(t *testing.T, s *nbdFakeServer, opts Options) *Client {
	t.Helper()
	c, err := Dial(t.Context(), s.path, opts)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

// nbdRawGo answers NBD_OPT_GO with the given replies, then ends the handshake.
func nbdRawGo(replies ...[]byte) func(fc *nbdFakeConn) {
	return func(fc *nbdFakeConn) { fc.send(replies...) }
}

func TestHandshake(t *testing.T) {
	export := func(size uint64) []byte {
		return nbdEncOptReply(optGo, repInfo, nbdEncInfoExport(size, tflagHasFlags))
	}
	ack := nbdEncOptReply(optGo, repAck, nil)
	tests := []struct {
		name    string
		cfg     nbdFakeConfig
		opts    Options
		wantErr string // substring; "" = success
		proto   bool   // error must wrap ErrProtocol
		check   func(t *testing.T, c *Client)
	}{
		{
			name: "defaults without block size info",
			cfg:  nbdFakeConfig{size: nbdTestSize},
			check: func(t *testing.T, c *Client) {
				if c.Size() != nbdTestSize || c.ReadOnly() || c.MinBlockSize() != 1 || c.MaxPayload() != 32<<20 {
					t.Errorf("size %d ro %v min %d max %d", c.Size(), c.ReadOnly(), c.MinBlockSize(), c.MaxPayload())
				}
			},
		},
		{
			name: "block size info honoured",
			cfg:  nbdFakeConfig{size: nbdTestSize, blockSize: []uint32{512, 4096, 1 << 20}, tflags: tflagHasFlags | tflagReadOnly},
			check: func(t *testing.T, c *Client) {
				if !c.ReadOnly() || c.MinBlockSize() != 512 || c.MaxPayload() != 1<<20 {
					t.Errorf("ro %v min %d max %d", c.ReadOnly(), c.MinBlockSize(), c.MaxPayload())
				}
			},
		},
		{
			name: "maximum above 32 MiB is capped",
			cfg:  nbdFakeConfig{size: nbdTestSize, blockSize: []uint32{1, 4096, 0xffffffff}},
			check: func(t *testing.T, c *Client) {
				if c.MaxPayload() != 32<<20 {
					t.Errorf("max payload %d", c.MaxPayload())
				}
			},
		},
		{
			name: "meta contexts granted",
			cfg:  nbdFakeConfig{size: nbdTestSize},
			opts: Options{MetaContexts: []string{"base:allocation", "qemu:dirty-bitmap:x"}},
			check: func(t *testing.T, c *Client) {
				if c.contexts["base:allocation"] != 10 || c.contexts["qemu:dirty-bitmap:x"] != 11 {
					t.Errorf("contexts %v", c.contexts)
				}
			},
		},
		{
			name:    "not fixed newstyle",
			cfg:     nbdFakeConfig{size: nbdTestSize, noFixedNewstyle: true},
			wantErr: "fixed newstyle", proto: true,
		},
		{
			name: "oldstyle server",
			cfg: nbdFakeConfig{handshake: func(fc *nbdFakeConn) bool {
				b := binary.BigEndian.AppendUint64(nil, magicNBD)
				b = binary.BigEndian.AppendUint64(b, magicOldstyle)
				fc.send(append(b, 0, 0))
				return false
			}},
			wantErr: "oldstyle", proto: true,
		},
		{
			name: "bad greeting",
			cfg: nbdFakeConfig{handshake: func(fc *nbdFakeConn) bool {
				fc.send([]byte("HTTP/1.1 400 Bad Request\r\n\r\n"))
				return false
			}},
			wantErr: "greeting magic", proto: true,
		},
		{
			name:    "structured replies refused",
			cfg:     nbdFakeConfig{size: nbdTestSize, refuseStructured: true},
			wantErr: "structured replies are required",
		},
		{
			name: "meta context not granted",
			cfg: nbdFakeConfig{size: nbdTestSize, grant: func(n string, i int) (uint32, bool) {
				return uint32(i + 1), n != "qemu:dirty-bitmap:missing"
			}},
			opts:    Options{MetaContexts: []string{"base:allocation", "qemu:dirty-bitmap:missing"}},
			wantErr: `"qemu:dirty-bitmap:missing" not granted`,
		},
		{
			name: "meta context id reused",
			cfg: nbdFakeConfig{size: nbdTestSize, grant: func(string, int) (uint32, bool) {
				return 7, true
			}},
			opts:    Options{MetaContexts: []string{"base:allocation", "qemu:allocation-depth"}},
			wantErr: "id 7 given to both", proto: true,
		},
		{
			name: "unrequested meta context",
			cfg: nbdFakeConfig{handshake: func(fc *nbdFakeConn) bool {
				fc.send(nbdEncGreeting(flagFixedNewstyle))
				fc.send(nbdEncOptReply(optStructuredReply, repAck, nil))
				fc.send(nbdEncOptReply(optSetMetaContext, repMetaContext, nbdEncMetaContext(1, "qemu:dirty-bitmap:other")))
				fc.send(nbdEncOptReply(optSetMetaContext, repAck, nil))
				io.Copy(io.Discard, fc.r)
				return false
			}},
			opts:    Options{MetaContexts: []string{"base:allocation"}},
			wantErr: "not requested", proto: true,
		},
		{
			name: "oversized option reply",
			cfg: nbdFakeConfig{handshake: func(fc *nbdFakeConn) bool {
				fc.send(nbdEncGreeting(flagFixedNewstyle))
				b := nbdEncOptReply(optStructuredReply, repAck, nil)
				binary.BigEndian.PutUint32(b[16:], 1<<30) // claims a 1 GiB payload
				fc.send(b)
				io.Copy(io.Discard, fc.r)
				return false
			}},
			wantErr: "reply of 1073741824 bytes", proto: true,
		},
		{
			name: "reply to another option",
			cfg: nbdFakeConfig{handshake: func(fc *nbdFakeConn) bool {
				fc.send(nbdEncGreeting(flagFixedNewstyle))
				fc.send(nbdEncOptReply(optGo, repAck, nil))
				io.Copy(io.Discard, fc.r)
				return false
			}},
			wantErr: "while waiting for NBD_OPT_STRUCTURED_REPLY", proto: true,
		},
		{
			name:    "go refused",
			cfg:     nbdFakeConfig{goReplies: nbdRawGo(nbdEncOptReply(optGo, repErrBit|6, []byte("no such export")))},
			wantErr: "NBD_REP_ERR_UNKNOWN",
		},
		{
			name:    "go ack without export info",
			cfg:     nbdFakeConfig{goReplies: nbdRawGo(ack)},
			wantErr: "without NBD_INFO_EXPORT", proto: true,
		},
		{
			name:    "export info of wrong length",
			cfg:     nbdFakeConfig{goReplies: nbdRawGo(nbdEncOptReply(optGo, repInfo, nbdEncInfoExport(4096, 1)[:11]), ack)},
			wantErr: "NBD_INFO_EXPORT of 11 bytes", proto: true,
		},
		{
			name:    "size zero",
			cfg:     nbdFakeConfig{goReplies: nbdRawGo(export(0), ack)},
			wantErr: "export size 0", proto: true,
		},
		{
			name:    "size not a multiple of the minimum block",
			cfg:     nbdFakeConfig{size: 4096 + 512, blockSize: []uint32{4096, 4096, 1 << 20}},
			wantErr: "not a multiple of the minimum block size", proto: true,
		},
		{
			name:    "minimum block not a power of two",
			cfg:     nbdFakeConfig{size: 3 * 4096, blockSize: []uint32{3, 4096, 1 << 20}},
			wantErr: "minimum block size 3", proto: true,
		},
		{
			name:    "minimum block above 64 KiB",
			cfg:     nbdFakeConfig{size: 1 << 20, blockSize: []uint32{128 << 10, 128 << 10, 1 << 20}},
			wantErr: "minimum block size 131072", proto: true,
		},
		{
			name:    "maximum below minimum",
			cfg:     nbdFakeConfig{size: nbdTestSize, blockSize: []uint32{4096, 4096, 512}},
			wantErr: "maximum block size 512", proto: true,
		},
		{
			name:    "preferred below minimum",
			cfg:     nbdFakeConfig{size: nbdTestSize, blockSize: []uint32{4096, 512, 1 << 20}},
			wantErr: "preferred block size 512", proto: true,
		},
		{
			name:    "unexpected reply type to go",
			cfg:     nbdFakeConfig{goReplies: nbdRawGo(nbdEncOptReply(optGo, repMetaContext, nbdEncMetaContext(1, "x")))},
			wantErr: "unexpected NBD_REP_META_CONTEXT", proto: true,
		},
		{
			name:    "server hangs up",
			cfg:     nbdFakeConfig{handshake: func(*nbdFakeConn) bool { return false }},
			wantErr: "read greeting", proto: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := nbdStartFake(t, tt.cfg)
			c, err := Dial(t.Context(), s.path, tt.opts)
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("Dial: %v", err)
				}
				defer c.Close()
				if tt.check != nil {
					tt.check(t, c)
				}
				return
			}
			if err == nil {
				c.Close()
				t.Fatalf("Dial succeeded, want error containing %q", tt.wantErr)
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("Dial error %q, want it to contain %q", err, tt.wantErr)
			}
			if tt.proto != errors.Is(err, ErrProtocol) {
				t.Errorf("errors.Is(%v, ErrProtocol) = %v, want %v", err, !tt.proto, tt.proto)
			}
		})
	}
}

func TestHandshakeOptionError(t *testing.T) {
	s := nbdStartFake(t, nbdFakeConfig{size: nbdTestSize, refuseStructured: true})
	_, err := Dial(t.Context(), s.path, Options{})
	oe, ok := errors.AsType[*OptionError](err)
	if !ok {
		t.Fatalf("Dial error %v is not an *OptionError", err)
	}
	if oe.Option != optStructuredReply || oe.Reply != repErrBit|1 || oe.Message != "no structured replies here" {
		t.Errorf("OptionError %+v", oe)
	}
}

func TestDialOptionsValidation(t *testing.T) {
	tests := []struct {
		name string
		opts Options
		want string
	}{
		{"export name too long", Options{Export: strings.Repeat("e", maxStringLen+1)}, "export name of 4097 bytes"},
		{"export name not UTF-8", Options{Export: "\xff"}, "not valid UTF-8"},
		{"empty meta context", Options{MetaContexts: []string{""}}, "empty meta context"},
		{"meta context too long", Options{MetaContexts: []string{strings.Repeat("m", maxStringLen+1)}}, "meta context of 4097 bytes"},
		{"duplicate meta context", Options{MetaContexts: []string{"base:allocation", "base:allocation"}}, "requested twice"},
		{"too many meta contexts", Options{MetaContexts: make([]string, maxMetaContexts+1)}, "at most 64"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// No server: validation happens before dialing.
			_, err := Dial(t.Context(), "/nonexistent/nbd.sock", tt.opts)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Errorf("Dial error %v, want %q", err, tt.want)
			}
		})
	}
}

func TestDialCheckPeer(t *testing.T) {
	s := nbdStartFake(t, nbdFakeConfig{size: nbdTestSize})
	var called bool
	c := nbdDial(t, s, Options{CheckPeer: func(uc *net.UnixConn) error {
		called = uc != nil
		return nil
	}})
	if !called {
		t.Error("CheckPeer not called with the connection")
	}
	c.Close()

	refuse := errors.New("peer is not QEMU")
	_, err := Dial(t.Context(), s.path, Options{CheckPeer: func(*net.UnixConn) error { return refuse }})
	if !errors.Is(err, refuse) {
		t.Errorf("Dial error %v, want the CheckPeer error", err)
	}
}

func TestDialContextCancelled(t *testing.T) {
	// The server never sends its greeting.
	s := nbdStartFake(t, nbdFakeConfig{handshake: func(fc *nbdFakeConn) bool {
		io.Copy(io.Discard, fc.r)
		return false
	}})
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	_, err := Dial(ctx, s.path, Options{})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Dial error %v, want DeadlineExceeded", err)
	}
}

func TestReadWriteRoundTrip(t *testing.T) {
	disk := nbdPattern(nbdTestSize, 1)
	s := nbdStartFake(t, nbdFakeConfig{size: nbdTestSize, disk: disk, blockSize: []uint32{512, 4096, 1 << 20}})
	c := nbdDial(t, s, Options{})
	ctx := t.Context()

	got := make([]byte, 3<<20+512)
	if err := c.Read(ctx, 4096, got); err != nil {
		t.Fatalf("Read: %v", err)
	}
	if !bytes.Equal(got, disk[4096:4096+len(got)]) {
		t.Error("Read returned wrong data")
	}
	data := nbdPattern(2<<20, 9)
	if err := c.Write(ctx, 1<<20, data); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := c.WriteZeroes(ctx, 8<<20, 4<<20); err != nil {
		t.Fatalf("WriteZeroes: %v", err)
	}
	if err := c.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Requests are split at the 1 MiB maximum payload, WRITE_ZEROES never
	// carries NO_HOLE, and Close sends DISC.
	type sent struct {
		typ    uint16
		flags  uint16
		off    uint64
		length uint32
	}
	want := []sent{
		{cmdRead, 0, 4096, 1 << 20}, {cmdRead, 0, 4096 + 1<<20, 1 << 20}, {cmdRead, 0, 4096 + 2<<20, 1 << 20}, {cmdRead, 0, 4096 + 3<<20, 512},
		{cmdWrite, 0, 1 << 20, 1 << 20}, {cmdWrite, 0, 2 << 20, 1 << 20},
		{cmdWriteZeroes, 0, 8 << 20, 4 << 20},
		{cmdFlush, 0, 0, 0},
		{cmdDisc, 0, 0, 0},
	}
	reqs := nbdWaitRequests(t, s, len(want))
	if len(reqs) != len(want) {
		t.Fatalf("server got %d requests, want %d: %+v", len(reqs), len(want), reqs)
	}
	for i, r := range reqs {
		if g := (sent{r.typ, r.flags, r.off, r.length}); g != want[i] {
			t.Errorf("request %d = %+v, want %+v", i, g, want[i])
		}
		if i > 0 && r.cookie == reqs[i-1].cookie {
			t.Errorf("request %d reuses cookie %d", i, r.cookie)
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !bytes.Equal(disk[1<<20:3<<20], data) {
		t.Error("Write did not reach the disk")
	}
	if !bytes.Equal(disk[8<<20:12<<20], make([]byte, 4<<20)) {
		t.Error("WriteZeroes did not zero the range")
	}
}

func TestWriteZeroesSplit(t *testing.T) {
	s := nbdStartFake(t, nbdFakeConfig{size: 3 << 30})
	c := nbdDial(t, s, Options{})
	if err := c.WriteZeroes(t.Context(), 0, 3<<30); err != nil {
		t.Fatalf("WriteZeroes: %v", err)
	}
	reqs := s.requests()
	if len(reqs) != 3 || reqs[2].off != 2<<30 || reqs[2].length != 1<<30 {
		t.Errorf("requests %+v, want three of 1 GiB", reqs)
	}
}

func TestReadSparseChunks(t *testing.T) {
	disk := nbdPattern(nbdTestSize, 3)
	s := nbdStartFake(t, nbdFakeConfig{size: nbdTestSize, handle: func(fc *nbdFakeConn, r nbdFakeReq) bool {
		// Out of order: data in the second half, a hole, then data in the
		// first quarter, ending with a NONE chunk.
		q := uint64(r.length / 4)
		fc.send(
			nbdEncData(r.cookie, 0, r.off+2*q, disk[r.off+2*q:r.off+4*q]),
			nbdEncHole(r.cookie, 0, r.off+q, uint32(q)),
			nbdEncData(r.cookie, 0, r.off, disk[r.off:r.off+q]),
			nbdEncNone(r.cookie),
		)
		return true
	}})
	c := nbdDial(t, s, Options{})
	got := bytes.Repeat([]byte{0xee}, 64<<10)
	if err := c.Read(t.Context(), 1<<20, got); err != nil {
		t.Fatalf("Read: %v", err)
	}
	want := bytes.Clone(disk[1<<20 : 1<<20+64<<10])
	clear(want[16<<10 : 32<<10])
	if !bytes.Equal(got, want) {
		t.Error("sparse Read: data or zero-filled hole wrong")
	}
}

// TestServerErrors checks that errors the server reports keep the Client
// usable.
func TestServerErrors(t *testing.T) {
	tests := []struct {
		name      string
		reply     func(r nbdFakeReq) []byte
		call      func(ctx context.Context, c *Client) error
		wantErrno uint32
		wantMsg   string
	}{
		{
			name:      "simple error to read",
			reply:     func(r nbdFakeReq) []byte { return nbdEncSimple(r.cookie, errnoEIO, nil) },
			call:      func(ctx context.Context, c *Client) error { return c.Read(ctx, 0, make([]byte, 4096)) },
			wantErrno: errnoEIO,
		},
		{
			name: "error offset chunk then more data",
			reply: func(r nbdFakeReq) []byte {
				return slicesConcat(
					nbdEncData(r.cookie, 0, r.off, make([]byte, 2048)),
					nbdEncErrOffset(r.cookie, 0, errnoEIO, "bad sector", r.off+2048),
					nbdEncData(r.cookie, replyFlagDone, r.off+2048, make([]byte, 2048)),
				)
			},
			call:      func(ctx context.Context, c *Client) error { return c.Read(ctx, 0, make([]byte, 4096)) },
			wantErrno: errnoEIO, wantMsg: "bad sector",
		},
		{
			name:      "structured error to write",
			reply:     func(r nbdFakeReq) []byte { return nbdEncErr(r.cookie, replyFlagDone, errnoENOSPC, "disk full") },
			call:      func(ctx context.Context, c *Client) error { return c.Write(ctx, 0, make([]byte, 4096)) },
			wantErrno: errnoENOSPC, wantMsg: "disk full",
		},
		{
			name:      "simple error to flush",
			reply:     func(r nbdFakeReq) []byte { return nbdEncSimple(r.cookie, errnoEPERM, nil) },
			call:      func(ctx context.Context, c *Client) error { return c.Flush(ctx) },
			wantErrno: errnoEPERM,
		},
		{
			name: "unknown error type",
			reply: func(r nbdFakeReq) []byte {
				p := binary.BigEndian.AppendUint32(nil, errnoEINVAL)
				p = binary.BigEndian.AppendUint16(p, 2)
				p = append(p, "no!extra"...)
				return nbdEncChunk(r.cookie, replyFlagDone, replyErrBit|99, p)
			},
			call:      func(ctx context.Context, c *Client) error { return c.WriteZeroes(ctx, 0, 4096) },
			wantErrno: errnoEINVAL, wantMsg: "no",
		},
		{
			name: "error chunk in block status",
			reply: func(r nbdFakeReq) []byte {
				return nbdEncErr(r.cookie, replyFlagDone, errnoENOTSUP, "")
			},
			call: func(ctx context.Context, c *Client) error {
				_, err := c.BlockStatus(ctx, "base:allocation", 0, 4096)
				return err
			},
			wantErrno: errnoENOTSUP,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			first := true
			s := nbdStartFake(t, nbdFakeConfig{size: nbdTestSize, handle: func(fc *nbdFakeConn, r nbdFakeReq) bool {
				if first {
					first = false
					fc.send(tt.reply(r))
					return true
				}
				return fc.defaultHandle(r)
			}})
			c := nbdDial(t, s, Options{MetaContexts: []string{"base:allocation"}})
			err := tt.call(t.Context(), c)
			e, ok := errors.AsType[*Error](err)
			if !ok {
				t.Fatalf("error %v is not an *Error", err)
			}
			if e.Errno != tt.wantErrno || e.Message != tt.wantMsg {
				t.Errorf("Error %+v, want errno %d message %q", e, tt.wantErrno, tt.wantMsg)
			}
			if !strings.Contains(err.Error(), errnoName(tt.wantErrno)) {
				t.Errorf("error %q does not name %s", err, errnoName(tt.wantErrno))
			}
			if errors.Is(err, ErrBroken) {
				t.Errorf("server error marked the Client broken: %v", err)
			}
			if err := c.Read(t.Context(), 0, make([]byte, 4096)); err != nil {
				t.Errorf("Read after a server error: %v", err)
			}
		})
	}
}

func slicesConcat(parts ...[]byte) []byte { return slices.Concat(parts...) }

// TestHostileReplies checks every bound on replies: each is a protocol error
// that leaves the Client broken, never a panic.
func TestHostileReplies(t *testing.T) {
	const n = 4096 // every call asks 4096 bytes at offset 8192
	const off = 8192
	read := func(ctx context.Context, c *Client) error { return c.Read(ctx, off, make([]byte, n)) }
	status := func(ctx context.Context, c *Client) error {
		_, err := c.BlockStatus(ctx, "qemu:dirty-bitmap:d", off, n)
		return err
	}
	write := func(ctx context.Context, c *Client) error { return c.Write(ctx, off, make([]byte, n)) }
	done := replyFlagDone
	tests := []struct {
		name  string
		call  func(context.Context, *Client) error
		reply func(r nbdFakeReq) []byte
		want  string
	}{
		{"wrong cookie", read, func(r nbdFakeReq) []byte { return nbdEncData(r.cookie+1, done, off, make([]byte, n)) }, "cookie"},
		{"bad reply magic", read, func(r nbdFakeReq) []byte { return make([]byte, 32) }, "bad reply magic"},
		{"extended reply header", read, func(r nbdFakeReq) []byte {
			return binary.BigEndian.AppendUint32(nil, magicExtendedRply)
		}, "extended reply"},
		{"simple reply with data to read", read, func(r nbdFakeReq) []byte { return nbdEncSimple(r.cookie, 0, make([]byte, n)) }, "simple reply to a read"},
		{"data before the range", read, func(r nbdFakeReq) []byte { return nbdEncData(r.cookie, done, off-512, make([]byte, 512)) }, "outside the request"},
		{"data past the range", read, func(r nbdFakeReq) []byte { return nbdEncData(r.cookie, done, off+n-512, make([]byte, 1024)) }, "outside the request"},
		{"data longer than the read", read, func(r nbdFakeReq) []byte { return nbdEncData(r.cookie, done, off, make([]byte, n+1)) }, "data chunk of"},
		{"data chunk without data", read, func(r nbdFakeReq) []byte { return nbdEncData(r.cookie, done, off, nil) }, "data chunk of 8 bytes"},
		{"overlapping data", read, func(r nbdFakeReq) []byte {
			return slicesConcat(nbdEncData(r.cookie, 0, off, make([]byte, 2048)), nbdEncData(r.cookie, done, off+1024, make([]byte, 3072)))
		}, "overlaps"},
		{"hole overlapping data", read, func(r nbdFakeReq) []byte {
			return slicesConcat(nbdEncData(r.cookie, 0, off+1024, make([]byte, 3072)), nbdEncHole(r.cookie, done, off, 2048))
		}, "overlaps"},
		{"same chunk twice", read, func(r nbdFakeReq) []byte {
			return slicesConcat(nbdEncHole(r.cookie, 0, off, n), nbdEncHole(r.cookie, done, off, n))
		}, "overlaps"},
		{"gap in coverage", read, func(r nbdFakeReq) []byte {
			return slicesConcat(nbdEncData(r.cookie, 0, off, make([]byte, 1024)), nbdEncHole(r.cookie, done, off+2048, 2048))
		}, "cover 3072 of 4096"},
		{"no chunk covers the read", read, func(r nbdFakeReq) []byte { return nbdEncNone(r.cookie) }, "cover 0 of 4096"},
		{"zero-length hole", read, func(r nbdFakeReq) []byte { return nbdEncHole(r.cookie, done, off, 0) }, "empty chunk"},
		{"hole past the range", read, func(r nbdFakeReq) []byte { return nbdEncHole(r.cookie, done, off, n+512) }, "outside the request"},
		{"hole chunk wrong size", read, func(r nbdFakeReq) []byte {
			return nbdEncChunk(r.cookie, done, replyOffsetHole, make([]byte, 16))
		}, "hole chunk of 16 bytes"},
		{"none without done", read, func(r nbdFakeReq) []byte { return nbdEncChunk(r.cookie, 0, replyNone, nil) }, "NBD_REPLY_TYPE_NONE"},
		{"none with payload", write, func(r nbdFakeReq) []byte { return nbdEncChunk(r.cookie, done, replyNone, []byte{1}) }, "NBD_REPLY_TYPE_NONE"},
		{"unknown chunk type in read", read, func(r nbdFakeReq) []byte { return nbdEncChunk(r.cookie, done, 77, nil) }, "chunk type 0x4d"},
		{"block status in a read", read, func(r nbdFakeReq) []byte { return nbdEncStatus(r.cookie, done, 10, [2]uint32{n, 0}) }, "chunk type 0x5"},
		{"data in a write reply", write, func(r nbdFakeReq) []byte { return nbdEncData(r.cookie, done, off, make([]byte, n)) }, "reply without data"},
		{"simple after structured", write, func(r nbdFakeReq) []byte {
			return slicesConcat(nbdEncErr(r.cookie, 0, errnoEIO, ""), nbdEncSimple(r.cookie, 0, nil))
		}, "simple reply after structured"},
		{"error chunk with errno 0", read, func(r nbdFakeReq) []byte { return nbdEncErr(r.cookie, done, 0, "") }, "errno 0"},
		{"error chunk message length mismatch", read, func(r nbdFakeReq) []byte {
			b := nbdEncErr(r.cookie, done, errnoEIO, "abc")
			binary.BigEndian.PutUint16(b[24:], 2) // message length field
			return b
		}, "with a 2 byte message"},
		{"error chunk too long", read, func(r nbdFakeReq) []byte {
			return nbdEncErr(r.cookie, done, errnoEIO, strings.Repeat("x", maxErrorMessage+100))
		}, "error chunk of"},
		{"error offset outside the read", read, func(r nbdFakeReq) []byte {
			return nbdEncErrOffset(r.cookie, done, errnoEIO, "", off+n)
		}, "error offset"},
		{"too many chunks", read, func(r nbdFakeReq) []byte {
			return bytes.Repeat(nbdEncErr(r.cookie, 0, errnoEIO, ""), maxChunks)
		}, "more than 65536 chunks"},
		{"connection closed mid-reply", read, func(r nbdFakeReq) []byte {
			return nbdEncData(r.cookie, done, off, make([]byte, n))[:100]
		}, "closed by server"},
		{"simple reply with errno 0 to block status", status, func(r nbdFakeReq) []byte { return nbdEncSimple(r.cookie, 0, nil) }, "simple reply to block status"},
		{"unknown context id", status, func(r nbdFakeReq) []byte { return nbdEncStatus(r.cookie, done, 99, [2]uint32{n, 0}) }, "unknown meta context id 99"},
		{"context id twice", status, func(r nbdFakeReq) []byte {
			return slicesConcat(nbdEncStatus(r.cookie, 0, 11, [2]uint32{n, 0}), nbdEncStatus(r.cookie, done, 11, [2]uint32{n, 0}))
		}, "two block status chunks"},
		{"requested context missing", status, func(r nbdFakeReq) []byte { return nbdEncStatus(r.cookie, done, 10, [2]uint32{n, 0}) }, "no block status for meta context"},
		{"zero-length extent", status, func(r nbdFakeReq) []byte {
			return nbdEncStatus(r.cookie, done, 11, [2]uint32{1024, 1}, [2]uint32{0, 0}, [2]uint32{3072, 0})
		}, "zero-length extent"},
		{"non-final extent past the range", status, func(r nbdFakeReq) []byte {
			return nbdEncStatus(r.cookie, done, 11, [2]uint32{n + 512, 1}, [2]uint32{512, 0})
		}, "runs past the request"},
		{"extents after the range is covered", status, func(r nbdFakeReq) []byte {
			return nbdEncStatus(r.cookie, done, 11, [2]uint32{n, 1}, [2]uint32{512, 0})
		}, "after the end of the request"},
		{"block status without descriptors", status, func(r nbdFakeReq) []byte { return nbdEncStatus(r.cookie, done, 11) }, "block status chunk of 4 bytes"},
		{"block status of ragged length", status, func(r nbdFakeReq) []byte {
			return nbdEncChunk(r.cookie, done, replyBlockStatus, make([]byte, 4+8+3))
		}, "block status chunk of 15 bytes"},
		{"block status too large", status, func(r nbdFakeReq) []byte {
			b := nbdEncStatus(r.cookie, done, 11, [2]uint32{n, 0})
			binary.BigEndian.PutUint32(b[16:], maxBlockStatusPayload+8)
			return b
		}, "block status chunk of"},
		{"data in a block status reply", status, func(r nbdFakeReq) []byte { return nbdEncData(r.cookie, done, off, make([]byte, n)) }, "in a block status reply"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := nbdStartFake(t, nbdFakeConfig{size: nbdTestSize, handle: func(fc *nbdFakeConn, r nbdFakeReq) bool {
				fc.send(tt.reply(r))
				return !strings.Contains(tt.name, "closed")
			}})
			c := nbdDial(t, s, Options{MetaContexts: []string{"base:allocation", "qemu:dirty-bitmap:d"}})
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			err := tt.call(ctx, c)
			if !errors.Is(err, ErrProtocol) || !errors.Is(err, ErrBroken) {
				t.Fatalf("error %v, want ErrProtocol and ErrBroken", err)
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Errorf("error %q, want it to contain %q", err, tt.want)
			}
			if err := c.Read(ctx, 0, make([]byte, 512)); !errors.Is(err, ErrBroken) {
				t.Errorf("Read after a protocol error: %v, want ErrBroken", err)
			}
			if err := c.Close(); err != nil {
				t.Errorf("Close: %v", err)
			}
		})
	}
}

func TestRequestChecks(t *testing.T) {
	s := nbdStartFake(t, nbdFakeConfig{size: nbdTestSize, blockSize: []uint32{512, 4096, 1 << 20}, tflags: tflagHasFlags | tflagReadOnly})
	c := nbdDial(t, s, Options{MetaContexts: []string{"base:allocation"}})
	ctx := t.Context()
	tests := []struct {
		name string
		call func() error
		want string
	}{
		{"read past the end", func() error { return c.Read(ctx, nbdTestSize-512, make([]byte, 1024)) }, "outside the export"},
		{"read offset overflow", func() error { return c.Read(ctx, 1<<64-512, make([]byte, 1024)) }, "outside the export"},
		{"misaligned offset", func() error { return c.Read(ctx, 100, make([]byte, 512)) }, "not aligned"},
		{"misaligned length", func() error { return c.Read(ctx, 0, make([]byte, 100)) }, "not aligned"},
		{"write to read-only export", func() error { return c.Write(ctx, 0, make([]byte, 512)) }, "read-only"},
		{"write zeroes to read-only export", func() error { return c.WriteZeroes(ctx, 0, 512) }, "read-only"},
		{"flush not advertised", func() error { return c.Flush(ctx) }, "NBD_CMD_FLUSH"},
		{"meta context not negotiated", func() error {
			_, err := c.BlockStatus(ctx, "qemu:allocation-depth", 0, 512)
			return err
		}, "was not negotiated"},
		{"block status past the end", func() error {
			_, err := c.BlockStatus(ctx, "base:allocation", nbdTestSize, 512)
			return err
		}, "outside the export"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.call()
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Errorf("error %v, want %q", err, tt.want)
			}
			if errors.Is(err, ErrBroken) {
				t.Errorf("a rejected request broke the Client: %v", err)
			}
		})
	}
	if got := len(s.requests()); got != 0 {
		t.Errorf("rejected requests reached the server: %d", got)
	}
	if err := c.Read(ctx, 0, make([]byte, 512)); err != nil {
		t.Errorf("Read after rejected requests: %v", err)
	}

	s2 := nbdStartFake(t, nbdFakeConfig{size: nbdTestSize, tflags: tflagHasFlags})
	c2 := nbdDial(t, s2, Options{})
	if err := c2.WriteZeroes(ctx, 0, 512); !errors.Is(err, ErrUnsupported) {
		t.Errorf("WriteZeroes without SEND_WRITE_ZEROES: %v, want ErrUnsupported", err)
	}
}

func TestBlockStatus(t *testing.T) {
	// Context 10 = base:allocation (one extent), 11 = dirty bitmap: dirty
	// 4 MiB chunk 1, clean elsewhere, answering in at most 3 extents and
	// with a last extent running past the request.
	status := func(id uint32, off uint64, length uint32) [][2]uint32 {
		if id == 10 {
			return [][2]uint32{{length, 0}}
		}
		const chunk = 4 << 20
		var out [][2]uint32
		for pos := off; len(out) < 3; {
			end := (pos/chunk + 1) * chunk
			flag := uint32(0)
			if pos/chunk == 1 {
				flag = 1
			}
			out = append(out, [2]uint32{uint32(end - pos), flag})
			pos = end
			if pos >= off+uint64(length) {
				break
			}
		}
		return out
	}
	s := nbdStartFake(t, nbdFakeConfig{size: nbdTestSize, status: status})
	c := nbdDial(t, s, Options{MetaContexts: []string{"base:allocation", "qemu:dirty-bitmap:d"}})
	ctx := t.Context()

	got, err := c.BlockStatus(ctx, "qemu:dirty-bitmap:d", 2<<20, 4<<20)
	if err != nil {
		t.Fatalf("BlockStatus: %v", err)
	}
	// [2M,4M) clean, then [4M,6M) dirty: the server's [4M,8M) is cut at 6M.
	want := []Extent{{2 << 20, 2 << 20, 0}, {4 << 20, 2 << 20, 1}}
	if !slices.Equal(got, want) {
		t.Errorf("BlockStatus = %v, want %v", got, want)
	}

	// A short answer: 3 extents of a 16 MiB request.
	got, err = c.BlockStatus(ctx, "qemu:dirty-bitmap:d", 0, nbdTestSize)
	if err != nil {
		t.Fatalf("BlockStatus: %v", err)
	}
	if len(got) != 3 || got[2].Offset+got[2].Length != 12<<20 {
		t.Errorf("short BlockStatus = %v", got)
	}

	var all []Extent
	for e, err := range c.Extents(ctx, "qemu:dirty-bitmap:d", 0, nbdTestSize) {
		if err != nil {
			t.Fatalf("Extents: %v", err)
		}
		all = append(all, e)
	}
	want = []Extent{{0, 4 << 20, 0}, {4 << 20, 4 << 20, 1}, {8 << 20, 4 << 20, 0}, {12 << 20, 4 << 20, 0}}
	if !slices.Equal(all, want) {
		t.Errorf("Extents = %v, want %v", all, want)
	}

	// Breaking out of the iteration early is fine and the Client stays in sync.
	for range c.Extents(ctx, "qemu:dirty-bitmap:d", 0, nbdTestSize) {
		break
	}
	if _, err := c.BlockStatus(ctx, "base:allocation", 0, 4096); err != nil {
		t.Errorf("BlockStatus after an early break: %v", err)
	}
	for _, err := range c.Extents(ctx, "qemu:dirty-bitmap:d", 1<<63, 1<<63) {
		if err == nil || !strings.Contains(err.Error(), "overflows") {
			t.Errorf("Extents over an overflowing range: %v", err)
		}
	}
}

func TestContextCancel(t *testing.T) {
	// The server reads requests but never answers them.
	s := nbdStartFake(t, nbdFakeConfig{size: nbdTestSize, handle: func(*nbdFakeConn, nbdFakeReq) bool { return true }})

	t.Run("cancel mid-request", func(t *testing.T) {
		c := nbdDial(t, s, Options{})
		ctx, cancel := context.WithCancel(t.Context())
		time.AfterFunc(50*time.Millisecond, cancel)
		err := c.Read(ctx, 0, make([]byte, 4096))
		if !errors.Is(err, context.Canceled) || !errors.Is(err, ErrBroken) {
			t.Fatalf("Read error %v, want Canceled and ErrBroken", err)
		}
		if err := c.Flush(t.Context()); !errors.Is(err, ErrBroken) {
			t.Errorf("Flush after a cancelled call: %v, want ErrBroken", err)
		}
		closeReturns(t, c)
	})
	t.Run("deadline mid-request", func(t *testing.T) {
		c := nbdDial(t, s, Options{})
		ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
		defer cancel()
		_, err := c.BlockStatus(ctx, "x", 0, 4096) // not negotiated: rejected, not broken
		if err == nil || errors.Is(err, ErrBroken) {
			t.Fatalf("BlockStatus of an unknown context: %v", err)
		}
		err = c.Write(ctx, 0, make([]byte, 4096))
		if !errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, ErrBroken) {
			t.Fatalf("Write error %v, want DeadlineExceeded and ErrBroken", err)
		}
		closeReturns(t, c)
	})
	t.Run("already cancelled", func(t *testing.T) {
		c := nbdDial(t, s, Options{})
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		err := c.Read(ctx, 0, make([]byte, 4096))
		if !errors.Is(err, context.Canceled) || errors.Is(err, ErrBroken) {
			t.Fatalf("Read error %v, want Canceled without ErrBroken", err)
		}
		closeReturns(t, c)
	})
	t.Run("close aborts a call in flight", func(t *testing.T) {
		c := nbdDial(t, s, Options{})
		errc := make(chan error, 1)
		go func() { errc <- c.Read(context.Background(), 0, make([]byte, 4096)) }()
		time.Sleep(50 * time.Millisecond) // let the Read block on the server
		closeReturns(t, c)
		select {
		case err := <-errc:
			if !errors.Is(err, ErrClosed) {
				t.Errorf("Read aborted by Close: %v, want ErrClosed", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("Read did not return after Close")
		}
	})
}

// closeReturns closes c and fails if Close blocks.
func closeReturns(t *testing.T, c *Client) {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- c.Close() }()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Close: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close blocked")
	}
}

func TestClose(t *testing.T) {
	s := nbdStartFake(t, nbdFakeConfig{size: nbdTestSize})
	c := nbdDial(t, s, Options{})
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Errorf("second Close: %v", err)
	}
	if err := c.Read(t.Context(), 0, make([]byte, 512)); !errors.Is(err, ErrClosed) {
		t.Errorf("Read after Close: %v, want ErrClosed", err)
	}
	if reqs := nbdWaitRequests(t, s, 1); len(reqs) != 1 || reqs[0].typ != cmdDisc {
		t.Fatalf("server requests %+v, want one DISC", reqs)
	}

	// A broken Client closes without sending DISC.
	s2 := nbdStartFake(t, nbdFakeConfig{size: nbdTestSize, handle: func(fc *nbdFakeConn, r nbdFakeReq) bool {
		fc.send(nbdEncSimple(r.cookie+5, 0, nil))
		return true
	}})
	c2 := nbdDial(t, s2, Options{})
	if err := c2.Flush(t.Context()); !errors.Is(err, ErrBroken) {
		t.Fatalf("Flush: %v, want ErrBroken", err)
	}
	closeReturns(t, c2)
	time.Sleep(50 * time.Millisecond)
	for _, r := range s2.requests() {
		if r.typ == cmdDisc {
			t.Error("broken Client sent DISC")
		}
	}
}

// nbdWaitRequests waits until s has received n requests (or 5 s passed) and
// returns them.
func nbdWaitRequests(t *testing.T, s *nbdFakeServer, n int) []nbdFakeReq {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		reqs := s.requests()
		if len(reqs) >= n || time.Now().After(deadline) {
			return reqs
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestErrorStrings(t *testing.T) {
	if got := (&Error{Errno: errnoEIO}).Error(); got != "NBD server error EIO" {
		t.Errorf("Error() = %q", got)
	}
	if got := (&Error{Errno: 200, Message: "x\n"}).Error(); got != `NBD server error errno 200: "x\n"` {
		t.Errorf("Error() = %q", got)
	}
}
