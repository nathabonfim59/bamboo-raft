package nbd

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"iter"
	"slices"
)

// Extent is one run of a meta context's status, as BlockStatus returns it.
type Extent struct {
	// Offset is the extent's first byte in the export.
	Offset uint64
	// Length is the extent's length in bytes, never 0.
	Length uint64
	// Flags is the context's status for the extent: for base:allocation
	// NBD_STATE_HOLE (1) and NBD_STATE_ZERO (2); for qemu:dirty-bitmap:<name>
	// 1 = dirty; for qemu:allocation-depth the depth in the backing chain
	// (0 = unallocated, 1 = the top image).
	Flags uint32
}

// Read reads len(buf) bytes at off into buf, splitting the range into
// requests of at most MaxPayload bytes. Holes the server reports are
// zero-filled. off and len(buf) must be multiples of MinBlockSize and lie
// within the export.
func (c *Client) Read(ctx context.Context, off uint64, buf []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.check(cmdRead, off, uint64(len(buf))); err != nil {
		return err
	}
	return c.withContext(ctx, func() error {
		for len(buf) > 0 {
			n := min(len(buf), int(c.maxPayload))
			cookie, err := c.request(cmdRead, off, uint32(n), nil)
			if err == nil {
				err = c.recvRead(cookie, off, buf[:n])
			}
			if err != nil {
				return fmt.Errorf("nbd: read %d+%d: %w", off, n, err)
			}
			off += uint64(n)
			buf = buf[n:]
		}
		return nil
	})
}

// Write writes data at off, splitting it into requests of at most MaxPayload
// bytes. off and len(data) must be multiples of MinBlockSize and lie within
// the export.
func (c *Client) Write(ctx context.Context, off uint64, data []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.check(cmdWrite, off, uint64(len(data))); err != nil {
		return err
	}
	return c.withContext(ctx, func() error {
		for len(data) > 0 {
			n := min(len(data), int(c.maxPayload))
			cookie, err := c.request(cmdWrite, off, uint32(n), data[:n])
			if err == nil {
				err = c.recvNoData(cookie)
			}
			if err != nil {
				return fmt.Errorf("nbd: write %d+%d: %w", off, n, err)
			}
			off += uint64(n)
			data = data[n:]
		}
		return nil
	})
}

// WriteZeroes makes length bytes at off read as zeroes. It does not send
// NBD_CMD_FLAG_NO_HOLE, so a qcow2 server may record zero clusters, which
// mask the backing file. off and length must be multiples of MinBlockSize and
// lie within the export.
func (c *Client) WriteZeroes(ctx context.Context, off, length uint64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.check(cmdWriteZeroes, off, length); err != nil {
		return err
	}
	return c.withContext(ctx, func() error {
		for length > 0 {
			n := min(length, maxWriteZeroesLen)
			cookie, err := c.request(cmdWriteZeroes, off, uint32(n), nil)
			if err == nil {
				err = c.recvNoData(cookie)
			}
			if err != nil {
				return fmt.Errorf("nbd: write zeroes %d+%d: %w", off, n, err)
			}
			off += n
			length -= n
		}
		return nil
	})
}

// Flush asks the server to make every completed write durable.
func (c *Client) Flush(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.check(cmdFlush, 0, 0); err != nil {
		return err
	}
	return c.withContext(ctx, func() error {
		cookie, err := c.request(cmdFlush, 0, 0, nil)
		if err == nil {
			err = c.recvNoData(cookie)
		}
		if err != nil {
			return fmt.Errorf("nbd: flush: %w", err)
		}
		return nil
	})
}

// BlockStatus returns metaContext's status from off onwards, at most length
// bytes (and at most 2 GiB per call). The server may describe less than asked;
// the extents are contiguous from off, and the last one is cut at the end of
// the requested range. Use Extents to cover a whole range. metaContext must
// have been granted at Dial. off and length must be multiples of
// MinBlockSize and lie within the export.
func (c *Client) BlockStatus(ctx context.Context, metaContext string, off, length uint64) ([]Extent, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.usable(); err != nil {
		return nil, err
	}
	id, ok := c.contexts[metaContext]
	if !ok {
		return nil, fmt.Errorf("nbd: block status: meta context %q was not negotiated", metaContext)
	}
	length = min(length, maxBlockStatusLen)
	if err := c.check(cmdBlockStatus, off, length); err != nil {
		return nil, err
	}
	if length == 0 {
		return nil, nil
	}
	var exts []Extent
	err := c.withContext(ctx, func() error {
		cookie, err := c.request(cmdBlockStatus, off, uint32(length), nil)
		if err == nil {
			exts, err = c.recvBlockStatus(cookie, id, off, length)
		}
		if err != nil {
			return fmt.Errorf("nbd: block status %q %d+%d: %w", metaContext, off, length, err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return exts, nil
}

// Extents yields metaContext's status over [off, off+length), calling
// BlockStatus as often as needed. It stops after the first error.
func (c *Client) Extents(ctx context.Context, metaContext string, off, length uint64) iter.Seq2[Extent, error] {
	return func(yield func(Extent, error) bool) {
		end := off + length
		if end < off {
			yield(Extent{}, fmt.Errorf("nbd: extents %d+%d: range overflows", off, length))
			return
		}
		for off < end {
			exts, err := c.BlockStatus(ctx, metaContext, off, end-off)
			if err != nil {
				yield(Extent{}, err)
				return
			}
			for _, e := range exts {
				if !yield(e, nil) {
					return
				}
			}
			last := exts[len(exts)-1] // recvBlockStatus never returns none
			off = last.Offset + last.Length
		}
	}
}

// check validates a request before it is sent. Callers hold c.mu.
func (c *Client) check(cmd uint16, off, length uint64) error {
	if err := c.usable(); err != nil {
		return err
	}
	switch cmd {
	case cmdWrite, cmdWriteZeroes:
		if c.ReadOnly() {
			return fmt.Errorf("nbd: %s: export is read-only: %w", cmdName(cmd), ErrUnsupported)
		}
		if cmd == cmdWriteZeroes && c.tflags&tflagSendWriteZeros == 0 {
			return fmt.Errorf("nbd: %s: server does not support NBD_CMD_WRITE_ZEROES: %w", cmdName(cmd), ErrUnsupported)
		}
	case cmdFlush:
		if c.tflags&tflagSendFlush == 0 {
			return fmt.Errorf("nbd: flush: server does not support NBD_CMD_FLUSH: %w", ErrUnsupported)
		}
		return nil
	}
	if off > c.size || length > c.size-off {
		return fmt.Errorf("nbd: %s %d+%d: outside the export (%d bytes)", cmdName(cmd), off, length, c.size)
	}
	if mb := uint64(c.minBlock); off%mb != 0 || length%mb != 0 {
		return fmt.Errorf("nbd: %s %d+%d: not aligned to the minimum block size %d", cmdName(cmd), off, length, mb)
	}
	return nil
}

// request sends one request and returns its cookie.
func (c *Client) request(cmd uint16, off uint64, length uint32, data []byte) (uint64, error) {
	c.cookie++
	err := c.writeRequest(cmd, 0, c.cookie, off, length, data)
	if err == nil {
		err = c.w.Flush()
	}
	if err != nil {
		return 0, fmt.Errorf("send request: %w", err)
	}
	return c.cookie, nil
}

func (c *Client) writeRequest(cmd, flags uint16, cookie, off uint64, length uint32, data []byte) error {
	var h [requestHeaderLength]byte
	binary.BigEndian.PutUint32(h[0:], magicRequest)
	binary.BigEndian.PutUint16(h[4:], flags)
	binary.BigEndian.PutUint16(h[6:], cmd)
	binary.BigEndian.PutUint64(h[8:], cookie)
	binary.BigEndian.PutUint64(h[16:], off)
	binary.BigEndian.PutUint32(h[24:], length)
	if _, err := c.w.Write(h[:]); err != nil {
		return err
	}
	if len(data) > 0 {
		if _, err := c.w.Write(data); err != nil {
			return err
		}
	}
	return nil
}

// replyHeader is a simple reply (simple set, errno) or the header of one
// structured chunk.
type replyHeader struct {
	simple bool
	errno  uint32
	flags  uint16
	typ    uint16
	cookie uint64
	length uint32
}

func (h replyHeader) done() bool { return h.simple || h.flags&replyFlagDone != 0 }

func (c *Client) readReplyHeader(cookie uint64) (replyHeader, error) {
	var b [20]byte
	if _, err := io.ReadFull(c.r, b[:4]); err != nil {
		return replyHeader{}, fmt.Errorf("read reply: %w", err)
	}
	var h replyHeader
	switch m := binary.BigEndian.Uint32(b[:4]); m {
	case magicSimpleReply:
		if _, err := io.ReadFull(c.r, b[4:16]); err != nil {
			return replyHeader{}, fmt.Errorf("read reply: %w", err)
		}
		h = replyHeader{simple: true, errno: binary.BigEndian.Uint32(b[4:]), cookie: binary.BigEndian.Uint64(b[8:])}
	case magicStructured:
		if _, err := io.ReadFull(c.r, b[4:20]); err != nil {
			return replyHeader{}, fmt.Errorf("read reply: %w", err)
		}
		h = replyHeader{
			flags:  binary.BigEndian.Uint16(b[4:]),
			typ:    binary.BigEndian.Uint16(b[6:]),
			cookie: binary.BigEndian.Uint64(b[8:]),
			length: binary.BigEndian.Uint32(b[16:]),
		}
	case magicExtendedRply:
		return replyHeader{}, fmt.Errorf("%w: extended reply header, which was not negotiated", ErrProtocol)
	default:
		return replyHeader{}, fmt.Errorf("%w: bad reply magic %#x", ErrProtocol, m)
	}
	if h.cookie != cookie {
		return replyHeader{}, fmt.Errorf("%w: reply for cookie %d while waiting for %d", ErrProtocol, h.cookie, cookie)
	}
	return h, nil
}

// readErrorChunk reads the payload of an error chunk (h.typ has the error
// bit). For NBD_REPLY_TYPE_ERROR_OFFSET it also returns the offset.
func (c *Client) readErrorChunk(h replyHeader) (*Error, uint64, error) {
	const fixed = 6
	if h.length < fixed || h.length > fixed+maxErrorMessage+8 {
		return nil, 0, fmt.Errorf("%w: error chunk of %d bytes", ErrProtocol, h.length)
	}
	p := make([]byte, h.length)
	if _, err := io.ReadFull(c.r, p); err != nil {
		return nil, 0, fmt.Errorf("read error chunk: %w", err)
	}
	errno := binary.BigEndian.Uint32(p)
	msgLen := uint32(binary.BigEndian.Uint16(p[4:]))
	if errno == 0 {
		return nil, 0, fmt.Errorf("%w: error chunk with errno 0", ErrProtocol)
	}
	var off uint64
	switch h.typ {
	case replyError:
		if h.length != fixed+msgLen {
			return nil, 0, fmt.Errorf("%w: error chunk of %d bytes with a %d byte message", ErrProtocol, h.length, msgLen)
		}
	case replyErrorOffset:
		if h.length != fixed+msgLen+8 {
			return nil, 0, fmt.Errorf("%w: error-offset chunk of %d bytes with a %d byte message", ErrProtocol, h.length, msgLen)
		}
		off = binary.BigEndian.Uint64(p[fixed+msgLen:])
	default:
		// An unknown error type: the payload starts like NBD_REPLY_TYPE_ERROR.
		if fixed+msgLen > h.length {
			return nil, 0, fmt.Errorf("%w: error chunk type %#x of %d bytes with a %d byte message", ErrProtocol, h.typ, h.length, msgLen)
		}
	}
	return &Error{Errno: errno, Message: string(p[fixed : fixed+msgLen])}, off, nil
}

// span is a half-open byte range relative to a request's offset.
type span struct{ start, end uint64 }

// coverage records the disjoint ranges a reply's chunks filled.
type coverage struct {
	spans []span // sorted, disjoint, adjacent spans merged
	total uint64
}

// add records [start, end), failing if it overlaps an earlier range.
func (cv *coverage) add(start, end uint64) error {
	i, _ := slices.BinarySearchFunc(cv.spans, start, func(s span, v uint64) int {
		switch {
		case s.start < v:
			return -1
		case s.start > v:
			return 1
		}
		return 0
	})
	if (i > 0 && cv.spans[i-1].end > start) || (i < len(cv.spans) && cv.spans[i].start < end) {
		return fmt.Errorf("%w: chunk %d+%d overlaps an earlier chunk", ErrProtocol, start, end-start)
	}
	cv.total += end - start
	mergePrev := i > 0 && cv.spans[i-1].end == start
	mergeNext := i < len(cv.spans) && cv.spans[i].start == end
	switch {
	case mergePrev && mergeNext:
		cv.spans[i-1].end = cv.spans[i].end
		cv.spans = slices.Delete(cv.spans, i, i+1)
	case mergePrev:
		cv.spans[i-1].end = end
	case mergeNext:
		cv.spans[i].start = start
	default:
		cv.spans = slices.Insert(cv.spans, i, span{start, end})
	}
	return nil
}

// recvRead reads the reply to a READ of len(buf) bytes at off into buf.
func (c *Client) recvRead(cookie, off uint64, buf []byte) error {
	var (
		cov      coverage
		firstErr error
		n        = uint64(len(buf))
	)
	for chunk := 0; ; chunk++ {
		if chunk == maxChunks {
			return fmt.Errorf("%w: more than %d chunks in one reply", ErrProtocol, maxChunks)
		}
		h, err := c.readReplyHeader(cookie)
		if err != nil {
			return err
		}
		if h.simple {
			if chunk > 0 {
				return fmt.Errorf("%w: simple reply after structured chunks", ErrProtocol)
			}
			if h.errno == 0 {
				return fmt.Errorf("%w: simple reply to a read, structured replies were negotiated", ErrProtocol)
			}
			return &Error{Errno: h.errno}
		}
		switch h.typ {
		case replyOffsetData:
			if h.length <= 8 || uint64(h.length-8) > n {
				return fmt.Errorf("%w: data chunk of %d bytes for a %d byte read", ErrProtocol, h.length, n)
			}
			var b [8]byte
			if _, err := io.ReadFull(c.r, b[:]); err != nil {
				return fmt.Errorf("read data chunk: %w", err)
			}
			start, end, err := chunkRange(binary.BigEndian.Uint64(b[:]), uint64(h.length-8), off, n)
			if err != nil {
				return err
			}
			if err := cov.add(start, end); err != nil {
				return err
			}
			if _, err := io.ReadFull(c.r, buf[start:end]); err != nil {
				return fmt.Errorf("read data chunk: %w", err)
			}
		case replyOffsetHole:
			if h.length != 12 {
				return fmt.Errorf("%w: hole chunk of %d bytes", ErrProtocol, h.length)
			}
			var b [12]byte
			if _, err := io.ReadFull(c.r, b[:]); err != nil {
				return fmt.Errorf("read hole chunk: %w", err)
			}
			start, end, err := chunkRange(binary.BigEndian.Uint64(b[:]), uint64(binary.BigEndian.Uint32(b[8:])), off, n)
			if err != nil {
				return err
			}
			if err := cov.add(start, end); err != nil {
				return err
			}
			clear(buf[start:end])
		case replyNone:
			if err := checkNone(h); err != nil {
				return err
			}
		default:
			if h.typ&replyErrBit == 0 {
				return fmt.Errorf("%w: chunk type %#x in a read reply", ErrProtocol, h.typ)
			}
			e, eoff, err := c.readErrorChunk(h)
			if err != nil {
				return err
			}
			if h.typ == replyErrorOffset && (eoff < off || eoff-off >= n) {
				return fmt.Errorf("%w: error offset %d outside the read %d+%d", ErrProtocol, eoff, off, n)
			}
			if firstErr == nil {
				firstErr = e
			}
		}
		if h.done() {
			break
		}
	}
	if firstErr != nil {
		return firstErr
	}
	if cov.total != n {
		return fmt.Errorf("%w: chunks cover %d of %d bytes", ErrProtocol, cov.total, n)
	}
	return nil
}

// chunkRange converts a chunk at offset coff of length clen into a range
// relative to a request of n bytes at off, failing if it falls outside.
func chunkRange(coff, clen, off, n uint64) (start, end uint64, err error) {
	if clen == 0 {
		return 0, 0, fmt.Errorf("%w: empty chunk at %d", ErrProtocol, coff)
	}
	if coff < off || coff-off > n || clen > n-(coff-off) {
		return 0, 0, fmt.Errorf("%w: chunk %d+%d outside the request %d+%d", ErrProtocol, coff, clen, off, n)
	}
	return coff - off, coff - off + clen, nil
}

func checkNone(h replyHeader) error {
	if h.length != 0 || h.flags&replyFlagDone == 0 {
		return fmt.Errorf("%w: NBD_REPLY_TYPE_NONE of %d bytes, flags %#x", ErrProtocol, h.length, h.flags)
	}
	return nil
}

// recvNoData reads the reply to a command without reply payload.
func (c *Client) recvNoData(cookie uint64) error {
	var firstErr error
	for chunk := 0; ; chunk++ {
		if chunk == maxChunks {
			return fmt.Errorf("%w: more than %d chunks in one reply", ErrProtocol, maxChunks)
		}
		h, err := c.readReplyHeader(cookie)
		if err != nil {
			return err
		}
		if h.simple {
			if chunk > 0 {
				return fmt.Errorf("%w: simple reply after structured chunks", ErrProtocol)
			}
			if h.errno != 0 {
				return &Error{Errno: h.errno}
			}
			return nil
		}
		switch {
		case h.typ == replyNone:
			if err := checkNone(h); err != nil {
				return err
			}
		case h.typ&replyErrBit != 0:
			e, _, err := c.readErrorChunk(h)
			if err != nil {
				return err
			}
			if firstErr == nil {
				firstErr = e
			}
		default:
			return fmt.Errorf("%w: chunk type %#x in a reply without data", ErrProtocol, h.typ)
		}
		if h.done() {
			return firstErr
		}
	}
}

// recvBlockStatus reads the reply to a BLOCK_STATUS of length bytes at off
// and returns the extents of context id.
func (c *Client) recvBlockStatus(cookie uint64, id uint32, off, length uint64) ([]Extent, error) {
	var (
		exts     []Extent
		found    bool
		seen     = make(map[uint32]bool, len(c.ctxNames))
		firstErr error
	)
	for chunk := 0; ; chunk++ {
		if chunk == maxChunks {
			return nil, fmt.Errorf("%w: more than %d chunks in one reply", ErrProtocol, maxChunks)
		}
		h, err := c.readReplyHeader(cookie)
		if err != nil {
			return nil, err
		}
		if h.simple {
			if chunk > 0 {
				return nil, fmt.Errorf("%w: simple reply after structured chunks", ErrProtocol)
			}
			if h.errno == 0 {
				return nil, fmt.Errorf("%w: simple reply to block status, structured replies were negotiated", ErrProtocol)
			}
			return nil, &Error{Errno: h.errno}
		}
		switch {
		case h.typ == replyBlockStatus:
			if h.length < 12 || (h.length-4)%8 != 0 || h.length > maxBlockStatusPayload {
				return nil, fmt.Errorf("%w: block status chunk of %d bytes", ErrProtocol, h.length)
			}
			if cap(c.scratch) < int(h.length) {
				c.scratch = make([]byte, h.length)
			}
			p := c.scratch[:h.length]
			if _, err := io.ReadFull(c.r, p); err != nil {
				return nil, fmt.Errorf("read block status chunk: %w", err)
			}
			cid := binary.BigEndian.Uint32(p)
			if _, ok := c.ctxNames[cid]; !ok {
				return nil, fmt.Errorf("%w: block status for unknown meta context id %d", ErrProtocol, cid)
			}
			if seen[cid] {
				return nil, fmt.Errorf("%w: two block status chunks for meta context id %d", ErrProtocol, cid)
			}
			seen[cid] = true
			if cid == id {
				exts, err = parseExtents(p[4:], off, length)
				if err != nil {
					return nil, err
				}
				found = true
			}
		case h.typ == replyNone:
			if err := checkNone(h); err != nil {
				return nil, err
			}
		case h.typ&replyErrBit != 0:
			e, _, err := c.readErrorChunk(h)
			if err != nil {
				return nil, err
			}
			if firstErr == nil {
				firstErr = e
			}
		default:
			return nil, fmt.Errorf("%w: chunk type %#x in a block status reply", ErrProtocol, h.typ)
		}
		if h.done() {
			break
		}
	}
	if firstErr != nil {
		return nil, firstErr
	}
	if !found {
		return nil, fmt.Errorf("%w: no block status for meta context %q", ErrProtocol, c.ctxNames[id])
	}
	return exts, nil
}

// parseExtents decodes block status descriptors for a request of length
// bytes at off. The last extent is cut at the end of the request; any other
// extent reaching past it is an error.
func parseExtents(p []byte, off, length uint64) ([]Extent, error) {
	n := len(p) / 8
	exts := make([]Extent, 0, n)
	pos, end := off, off+length
	for i := range n {
		l := uint64(binary.BigEndian.Uint32(p[8*i:]))
		flags := binary.BigEndian.Uint32(p[8*i+4:])
		if l == 0 {
			return nil, fmt.Errorf("%w: zero-length extent at %d", ErrProtocol, pos)
		}
		if l > end-pos {
			if i != n-1 {
				return nil, fmt.Errorf("%w: extent %d+%d runs past the request %d+%d", ErrProtocol, pos, l, off, length)
			}
			l = end - pos
		}
		exts = append(exts, Extent{Offset: pos, Length: l, Flags: flags})
		pos += l
		if pos == end && i != n-1 {
			return nil, fmt.Errorf("%w: %d extents after the end of the request %d+%d", ErrProtocol, n-1-i, off, length)
		}
	}
	return exts, nil
}
