package api

import (
	"bytes"
	"strings"
	"unicode/utf8"
)

// commandCappedBuffer accumulates command output up to limit bytes. Once the
// limit is reached it keeps accepting writes so the caller can drain the node
// stream, but only sets truncated.
type commandCappedBuffer struct {
	buf       []byte
	limit     int
	truncated bool
}

// Write appends as much of p as fits. It never fails; a command's output is
// not an error condition.
func (b *commandCappedBuffer) Write(p []byte) {
	if len(p) == 0 {
		return
	}
	remaining := b.limit - len(b.buf)
	if remaining <= 0 {
		b.truncated = true
		return
	}
	if len(p) <= remaining {
		b.buf = append(b.buf, p...)
		return
	}
	b.buf = append(b.buf, p[:remaining]...)
	b.truncated = true
}

// String returns valid UTF-8. At a truncation boundary it drops a trailing
// partial rune so the cap never splits a whole character.
func (b *commandCappedBuffer) String() string {
	data := b.buf
	if b.truncated {
		data = trimIncompleteRune(data)
	}
	return strings.ToValidUTF8(string(data), "\uFFFD")
}

// commandUTF8Stream joins node chunks into valid UTF-8, holding at most the
// three bytes of an incomplete rune between chunks. Node stdout and stderr are
// byte streams; the HTTP stream promises UTF-8 data, so a single rune may be
// split across two frames.
type commandUTF8Stream struct {
	pending []byte
	// order is the Exec event sequence in which the current pending tail
	// started. Zero means nothing is pending.
	order uint64
}

// Write returns the valid UTF-8 prefix completed by chunk. sequence is the
// node event order; it is recorded when a new incomplete tail starts so exit
// tails can be flushed in original receive order. It returns "" while the
// chunk only extends an incomplete rune.
func (s *commandUTF8Stream) Write(chunk []byte, sequence uint64) string {
	if len(chunk) == 0 {
		return ""
	}
	oldPending := len(s.pending)
	buf := make([]byte, 0, oldPending+len(chunk))
	buf = append(buf, s.pending...)
	buf = append(buf, chunk...)
	cut := completeUTF8Prefix(buf)
	ready := ""
	if cut > 0 {
		ready = strings.ToValidUTF8(string(buf[:cut]), "\uFFFD")
	}
	s.pending = bytes.Clone(buf[cut:])
	switch {
	case len(s.pending) == 0:
		s.order = 0
	case cut < oldPending:
		// Some of the earlier tail is still incomplete; keep its
		// original position.
	default:
		// The earlier tail was consumed, or there was none: the new
		// tail starts at this event.
		s.order = sequence
	}
	if len(s.pending) >= utf8.UTFMax {
		// More than a rune can use cannot be completed later; replace it
		// now instead of buffering without a bound.
		ready += strings.ToValidUTF8(string(s.pending), "\uFFFD")
		s.pending = nil
		s.order = 0
	}
	return ready
}

// Flush returns whatever is still buffered, replacing an incomplete final rune.
func (s *commandUTF8Stream) Flush() string {
	if len(s.pending) == 0 {
		return ""
	}
	ready := strings.ToValidUTF8(string(s.pending), "\uFFFD")
	s.pending = nil
	s.order = 0
	return ready
}

// completeUTF8Prefix returns the length of b that ends on a rune boundary. An
// incomplete rune at the end is left out; an invalid tail is returned as bytes
// so the caller can replace it.
func completeUTF8Prefix(b []byte) int {
	for i := 1; i <= utf8.UTFMax && i <= len(b); i++ {
		start := len(b) - i
		if !utf8.RuneStart(b[start]) {
			continue
		}
		if utf8.FullRune(b[start:]) {
			return len(b)
		}
		return start
	}
	return len(b)
}

// trimIncompleteRune drops a trailing rune that the byte cap cut in half.
func trimIncompleteRune(b []byte) []byte {
	for i := 1; i <= utf8.UTFMax && i <= len(b); i++ {
		start := len(b) - i
		if !utf8.RuneStart(b[start]) {
			continue
		}
		if !utf8.FullRune(b[start:]) {
			return b[:start]
		}
		return b
	}
	return b
}
