package nbd

import (
	"bufio"
	"bytes"
	"io"
	"testing"
)

// nbdFuzzClient returns a Client in transmission phase whose server stream
// is data: a 64 KiB export, minimum block 1, contexts 10 and 11.
func nbdFuzzClient(data []byte) *Client {
	return &Client{
		r:          bufio.NewReader(bytes.NewReader(data)),
		w:          bufio.NewWriter(io.Discard),
		size:       64 << 10,
		minBlock:   1,
		maxPayload: maxPayloadCap,
		contexts:   map[string]uint32{"base:allocation": 10, "qemu:dirty-bitmap:d": 11},
		ctxNames:   map[uint32]string{10: "base:allocation", 11: "qemu:dirty-bitmap:d"},
	}
}

// FuzzReplies feeds arbitrary server bytes to the reply parsers of READ,
// BLOCK_STATUS and commands without reply data: they must not panic, and
// what they accept must satisfy the request's invariants.
func FuzzReplies(f *testing.F) {
	const off, n = 4096, 4096
	const cookie = 1
	f.Add(byte(0), nbdEncData(cookie, replyFlagDone, off, make([]byte, n)))
	f.Add(byte(0), slicesConcat(
		nbdEncHole(cookie, 0, off+n/2, n/2),
		nbdEncErrOffset(cookie, 0, errnoEIO, "x", off),
		nbdEncData(cookie, 0, off, make([]byte, n/2)),
		nbdEncNone(cookie)))
	f.Add(byte(0), nbdEncSimple(cookie, errnoEIO, nil))
	f.Add(byte(1), slicesConcat(
		nbdEncStatus(cookie, 0, 10, [2]uint32{n, 0}),
		nbdEncStatus(cookie, replyFlagDone, 11, [2]uint32{1024, 1}, [2]uint32{8192, 0})))
	f.Add(byte(1), nbdEncErr(cookie, replyFlagDone, errnoENOTSUP, "no"))
	f.Add(byte(2), nbdEncSimple(cookie, 0, nil))
	f.Add(byte(2), slicesConcat(nbdEncErr(cookie, 0, errnoENOSPC, ""), nbdEncNone(cookie)))
	f.Fuzz(func(t *testing.T, which byte, data []byte) {
		c := nbdFuzzClient(data)
		switch which % 3 {
		case 0:
			buf := make([]byte, n)
			_ = c.recvRead(cookie, off, buf)
		case 1:
			exts, err := c.recvBlockStatus(cookie, 11, off, n)
			if err != nil {
				return
			}
			pos := uint64(off)
			for _, e := range exts {
				if e.Offset != pos || e.Length == 0 {
					t.Fatalf("extents not contiguous from %d: %v", off, exts)
				}
				pos += e.Length
			}
			if len(exts) == 0 || pos > off+n {
				t.Fatalf("extents %v outside %d+%d", exts, off, n)
			}
		case 2:
			_ = c.recvNoData(cookie)
		}
	})
}

// FuzzHandshake feeds arbitrary server bytes to the handshake: it must not
// panic, and an accepted export must have a valid geometry.
func FuzzHandshake(f *testing.F) {
	good := slicesConcat(
		nbdEncGreeting(flagFixedNewstyle|flagNoZeroes),
		nbdEncOptReply(optStructuredReply, repAck, nil),
		nbdEncOptReply(optSetMetaContext, repMetaContext, nbdEncMetaContext(1, "base:allocation")),
		nbdEncOptReply(optSetMetaContext, repAck, nil),
		nbdEncOptReply(optGo, repInfo, nbdEncInfoExport(1<<20, tflagHasFlags|tflagReadOnly)),
		nbdEncOptReply(optGo, repInfo, nbdEncInfoBlockSize(512, 4096, 32<<20)),
		nbdEncOptReply(optGo, repAck, nil))
	f.Add(good)
	f.Add(good[:40])
	f.Add(slicesConcat(nbdEncGreeting(flagFixedNewstyle), nbdEncOptReply(optStructuredReply, repErrBit|1, []byte("no"))))
	f.Fuzz(func(t *testing.T, data []byte) {
		c := &Client{r: bufio.NewReader(bytes.NewReader(data)), w: bufio.NewWriter(io.Discard)}
		err := c.handshake(Options{MetaContexts: []string{"base:allocation"}})
		if err != nil {
			return
		}
		if c.size == 0 || c.size%uint64(c.minBlock) != 0 || !isPow2(c.minBlock) || c.maxPayload < c.minBlock || c.maxPayload > maxPayloadCap {
			t.Fatalf("accepted geometry size %d min %d max payload %d", c.size, c.minBlock, c.maxPayload)
		}
		if _, ok := c.contexts["base:allocation"]; !ok {
			t.Fatal("accepted handshake without the requested meta context")
		}
	})
}
