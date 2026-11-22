package nbd

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
	"maps"
	"net"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
)

// Wire encoders shared by the fake server and the fuzz seeds.

func nbdEncGreeting(hsFlags uint16) []byte {
	b := binary.BigEndian.AppendUint64(nil, magicNBD)
	b = binary.BigEndian.AppendUint64(b, magicIHaveOpt)
	return binary.BigEndian.AppendUint16(b, hsFlags)
}

func nbdEncOptReply(opt, typ uint32, data []byte) []byte {
	b := binary.BigEndian.AppendUint64(nil, magicOptReply)
	b = binary.BigEndian.AppendUint32(b, opt)
	b = binary.BigEndian.AppendUint32(b, typ)
	b = binary.BigEndian.AppendUint32(b, uint32(len(data)))
	return append(b, data...)
}

func nbdEncInfoExport(size uint64, tflags uint16) []byte {
	b := binary.BigEndian.AppendUint16(nil, infoExport)
	b = binary.BigEndian.AppendUint64(b, size)
	return binary.BigEndian.AppendUint16(b, tflags)
}

func nbdEncInfoBlockSize(minB, pref, maxB uint32) []byte {
	b := binary.BigEndian.AppendUint16(nil, infoBlockSize)
	b = binary.BigEndian.AppendUint32(b, minB)
	b = binary.BigEndian.AppendUint32(b, pref)
	return binary.BigEndian.AppendUint32(b, maxB)
}

func nbdEncMetaContext(id uint32, name string) []byte {
	return append(binary.BigEndian.AppendUint32(nil, id), name...)
}

func nbdEncSimple(cookie uint64, errno uint32, data []byte) []byte {
	b := binary.BigEndian.AppendUint32(nil, magicSimpleReply)
	b = binary.BigEndian.AppendUint32(b, errno)
	b = binary.BigEndian.AppendUint64(b, cookie)
	return append(b, data...)
}

func nbdEncChunk(cookie uint64, flags, typ uint16, payload []byte) []byte {
	b := binary.BigEndian.AppendUint32(nil, magicStructured)
	b = binary.BigEndian.AppendUint16(b, flags)
	b = binary.BigEndian.AppendUint16(b, typ)
	b = binary.BigEndian.AppendUint64(b, cookie)
	b = binary.BigEndian.AppendUint32(b, uint32(len(payload)))
	return append(b, payload...)
}

func nbdEncData(cookie uint64, flags uint16, off uint64, data []byte) []byte {
	return nbdEncChunk(cookie, flags, replyOffsetData, append(binary.BigEndian.AppendUint64(nil, off), data...))
}

func nbdEncHole(cookie uint64, flags uint16, off uint64, n uint32) []byte {
	p := binary.BigEndian.AppendUint64(nil, off)
	return nbdEncChunk(cookie, flags, replyOffsetHole, binary.BigEndian.AppendUint32(p, n))
}

func nbdEncErr(cookie uint64, flags uint16, errno uint32, msg string) []byte {
	p := binary.BigEndian.AppendUint32(nil, errno)
	p = binary.BigEndian.AppendUint16(p, uint16(len(msg)))
	return nbdEncChunk(cookie, flags, replyError, append(p, msg...))
}

func nbdEncErrOffset(cookie uint64, flags uint16, errno uint32, msg string, off uint64) []byte {
	p := binary.BigEndian.AppendUint32(nil, errno)
	p = binary.BigEndian.AppendUint16(p, uint16(len(msg)))
	p = append(p, msg...)
	return nbdEncChunk(cookie, flags, replyErrorOffset, binary.BigEndian.AppendUint64(p, off))
}

// nbdEncStatus encodes a BLOCK_STATUS chunk; descs are (length, flags) pairs.
func nbdEncStatus(cookie uint64, flags uint16, id uint32, descs ...[2]uint32) []byte {
	p := binary.BigEndian.AppendUint32(nil, id)
	for _, d := range descs {
		p = binary.BigEndian.AppendUint32(p, d[0])
		p = binary.BigEndian.AppendUint32(p, d[1])
	}
	return nbdEncChunk(cookie, flags, replyBlockStatus, p)
}

func nbdEncNone(cookie uint64) []byte { return nbdEncChunk(cookie, replyFlagDone, replyNone, nil) }

// nbdFakeReq is a request the fake server received.
type nbdFakeReq struct {
	flags, typ uint16
	cookie     uint64
	off        uint64
	length     uint32
	data       []byte
}

// nbdFakeConfig scripts a fake NBD server. The zero value serves a default
// export of size bytes (set it) without meta contexts.
type nbdFakeConfig struct {
	// handshake replaces the whole handshake when set; it returns false to
	// end the connection.
	handshake func(fc *nbdFakeConn) bool
	// noFixedNewstyle clears NBD_FLAG_FIXED_NEWSTYLE in the greeting.
	noFixedNewstyle  bool
	refuseStructured bool
	// grant decides the meta contexts granted; nil grants every one
	// requested, with ids 10, 11, ... in request order.
	grant func(name string, i int) (id uint32, ok bool)
	// goReplies replaces the NBD_OPT_GO replies when set.
	goReplies func(fc *nbdFakeConn)
	size      uint64
	tflags    uint16   // 0 means HAS_FLAGS|SEND_FLUSH|SEND_WRITE_ZEROES
	blockSize []uint32 // min, preferred, max; nil sends no NBD_INFO_BLOCK_SIZE
	disk      []byte   // served by the default READ/WRITE handlers when set
	status    func(id uint32, off uint64, length uint32) [][2]uint32
	// handle replaces the default request handler; it returns false to end
	// the connection.
	handle func(fc *nbdFakeConn, req nbdFakeReq) bool
}

// nbdFakeServer is an in-process NBD server on a unix socket.
type nbdFakeServer struct {
	cfg  nbdFakeConfig
	path string

	mu      sync.Mutex
	reqs    []nbdFakeReq
	granted map[string]uint32
}

// nbdFakeConn is one connection to the fake server.
type nbdFakeConn struct {
	srv *nbdFakeServer
	c   net.Conn
	r   *bufio.Reader
}

func (fc *nbdFakeConn) send(parts ...[]byte) {
	for _, p := range parts {
		if _, err := fc.c.Write(p); err != nil {
			return
		}
	}
}

func (fc *nbdFakeConn) readOpt() (opt uint32, data []byte, err error) {
	var h [16]byte
	if _, err := io.ReadFull(fc.r, h[:]); err != nil {
		return 0, nil, err
	}
	if binary.BigEndian.Uint64(h[:]) != magicIHaveOpt {
		return 0, nil, errors.New("fake: bad option magic")
	}
	opt = binary.BigEndian.Uint32(h[8:])
	data = make([]byte, binary.BigEndian.Uint32(h[12:]))
	_, err = io.ReadFull(fc.r, data)
	return opt, data, err
}

func (fc *nbdFakeConn) readReq() (nbdFakeReq, error) {
	var h [requestHeaderLength]byte
	if _, err := io.ReadFull(fc.r, h[:]); err != nil {
		return nbdFakeReq{}, err
	}
	if binary.BigEndian.Uint32(h[:]) != magicRequest {
		return nbdFakeReq{}, errors.New("fake: bad request magic")
	}
	req := nbdFakeReq{
		flags:  binary.BigEndian.Uint16(h[4:]),
		typ:    binary.BigEndian.Uint16(h[6:]),
		cookie: binary.BigEndian.Uint64(h[8:]),
		off:    binary.BigEndian.Uint64(h[16:]),
		length: binary.BigEndian.Uint32(h[24:]),
	}
	if req.typ == cmdWrite {
		req.data = make([]byte, req.length)
		if _, err := io.ReadFull(fc.r, req.data); err != nil {
			return nbdFakeReq{}, err
		}
	}
	return req, nil
}

// nbdFakeParseMeta parses "u32 len, export name, u32 n, n × (u32 len, name)".
func nbdFakeParseMeta(data []byte) ([]string, error) {
	if len(data) < 8 {
		return nil, errors.New("fake: short meta context option")
	}
	el := binary.BigEndian.Uint32(data)
	data = data[4+el:]
	n := binary.BigEndian.Uint32(data)
	data = data[4:]
	var names []string
	for range n {
		l := binary.BigEndian.Uint32(data)
		names = append(names, string(data[4:4+l]))
		data = data[4+l:]
	}
	return names, nil
}

func (fc *nbdFakeConn) defaultHandshake() bool {
	cfg := &fc.srv.cfg
	hs := flagFixedNewstyle | flagNoZeroes
	if cfg.noFixedNewstyle {
		hs = flagNoZeroes
	}
	fc.send(nbdEncGreeting(hs))
	var cf [4]byte
	if _, err := io.ReadFull(fc.r, cf[:]); err != nil {
		return false
	}
	for {
		opt, data, err := fc.readOpt()
		if err != nil {
			return false
		}
		switch opt {
		case optStructuredReply:
			if cfg.refuseStructured {
				fc.send(nbdEncOptReply(opt, repErrBit|1, []byte("no structured replies here")))
				continue
			}
			fc.send(nbdEncOptReply(opt, repAck, nil))
		case optSetMetaContext:
			names, err := nbdFakeParseMeta(data)
			if err != nil {
				return false
			}
			for i, n := range names {
				id, ok := uint32(10+i), true
				if cfg.grant != nil {
					id, ok = cfg.grant(n, i)
				}
				if ok {
					fc.srv.mu.Lock()
					fc.srv.granted[n] = id
					fc.srv.mu.Unlock()
					fc.send(nbdEncOptReply(opt, repMetaContext, nbdEncMetaContext(id, n)))
				}
			}
			fc.send(nbdEncOptReply(opt, repAck, nil))
		case optGo:
			if cfg.goReplies != nil {
				cfg.goReplies(fc)
				return true
			}
			tf := cfg.tflags
			if tf == 0 {
				tf = tflagHasFlags | tflagSendFlush | tflagSendWriteZeros
			}
			fc.send(nbdEncOptReply(opt, repInfo, nbdEncInfoExport(cfg.size, tf)))
			if cfg.blockSize != nil {
				fc.send(nbdEncOptReply(opt, repInfo, nbdEncInfoBlockSize(cfg.blockSize[0], cfg.blockSize[1], cfg.blockSize[2])))
			}
			fc.send(nbdEncOptReply(opt, repAck, nil))
			return true
		default:
			fc.send(nbdEncOptReply(opt, repErrBit|1, nil))
		}
	}
}

func (fc *nbdFakeConn) defaultHandle(req nbdFakeReq) bool {
	cfg := &fc.srv.cfg
	switch req.typ {
	case cmdRead:
		data := make([]byte, req.length)
		if cfg.disk != nil {
			copy(data, cfg.disk[req.off:])
		}
		fc.send(nbdEncData(req.cookie, replyFlagDone, req.off, data))
	case cmdWrite:
		if cfg.disk != nil {
			fc.srv.mu.Lock()
			copy(cfg.disk[req.off:], req.data)
			fc.srv.mu.Unlock()
		}
		fc.send(nbdEncSimple(req.cookie, 0, nil))
	case cmdWriteZeroes:
		if cfg.disk != nil {
			fc.srv.mu.Lock()
			clear(cfg.disk[req.off : req.off+uint64(req.length)])
			fc.srv.mu.Unlock()
		}
		fc.send(nbdEncNone(req.cookie))
	case cmdFlush:
		fc.send(nbdEncSimple(req.cookie, 0, nil))
	case cmdBlockStatus:
		fc.srv.mu.Lock()
		ids := slices.Sorted(maps.Values(fc.srv.granted))
		fc.srv.mu.Unlock()
		for _, id := range ids {
			var descs [][2]uint32
			if cfg.status != nil {
				descs = cfg.status(id, req.off, req.length)
			} else {
				descs = [][2]uint32{{req.length, 0}}
			}
			fc.send(nbdEncStatus(req.cookie, 0, id, descs...))
		}
		fc.send(nbdEncNone(req.cookie))
	default:
		fc.send(nbdEncSimple(req.cookie, errnoEINVAL, nil))
	}
	return true
}

func (s *nbdFakeServer) serve(c net.Conn) {
	defer c.Close()
	fc := &nbdFakeConn{srv: s, c: c, r: bufio.NewReader(c)}
	hs := fc.defaultHandshake
	if s.cfg.handshake != nil {
		hs = func() bool { return s.cfg.handshake(fc) }
	}
	if !hs() {
		return
	}
	for {
		req, err := fc.readReq()
		if err != nil {
			return
		}
		s.mu.Lock()
		s.reqs = append(s.reqs, req)
		s.mu.Unlock()
		if req.typ == cmdDisc {
			return
		}
		h := fc.defaultHandle
		if s.cfg.handle != nil {
			h = func(r nbdFakeReq) bool { return s.cfg.handle(fc, r) }
		}
		if !h(req) {
			return
		}
	}
}

// requests returns a copy of the requests received so far.
func (s *nbdFakeServer) requests() []nbdFakeReq {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.reqs)
}

// nbdShortTempDir returns a temporary directory with a path short enough for
// a unix socket (t.TempDir paths can exceed the 108-byte limit).
func nbdShortTempDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "nbdt")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return dir
}

// nbdStartFake serves cfg on a new unix socket until the test ends.
func nbdStartFake(t *testing.T, cfg nbdFakeConfig) *nbdFakeServer {
	t.Helper()
	s := &nbdFakeServer{cfg: cfg, path: filepath.Join(nbdShortTempDir(t), "s"), granted: map[string]uint32{}}
	ln, err := net.Listen("unix", s.path)
	if err != nil {
		t.Fatal(err)
	}
	var (
		wg    sync.WaitGroup
		mu    sync.Mutex
		conns []net.Conn
	)
	wg.Go(func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			conns = append(conns, c)
			mu.Unlock()
			wg.Go(func() { s.serve(c) })
		}
	})
	t.Cleanup(func() {
		ln.Close()
		mu.Lock()
		for _, c := range conns {
			c.Close()
		}
		mu.Unlock()
		wg.Wait()
	})
	return s
}

// nbdPattern returns n bytes of a position-dependent pattern.
func nbdPattern(n int, seed byte) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i/512) ^ byte(i) ^ seed
	}
	return b
}
