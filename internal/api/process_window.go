package api

import (
	"strings"
	"unicode/utf8"
)

// commandWindow is one decoded log window. start and next are raw byte offsets
// in the guest log; text is their valid-UTF-8 JSON representation.
type commandWindow struct {
	text      string
	start     int64
	next      int64
	truncated bool
}

// commandWindowInput is one node log window. finalEOF is true only when the
// process ended and the node reported no further output for either stream.
type commandWindowInput struct {
	data       []byte
	start      int64
	offsetMode bool
	finalEOF   bool
	truncated  bool
}

// processOutputWindow turns the raw bytes GetProcess returned into a JSON-safe
// string while keeping byte offsets exact. In offset mode the end is clamped
// down to a rune boundary so the next read resumes without a gap or a repeat;
// a requested offset in the middle of a rune advances to the next rune start and
// is reported as the window start. A trailing partial rune is replaced only at
// a definite log EOF (process ended and native More false); otherwise the
// cursor stays before it and a later read sees the completed rune. At raw
// offset zero an invalid leading byte is replaced, never dropped; a start above
// zero is a mid-rune cut, so leading continuation bytes are skipped to the next
// rune start.
//
// Interior invalid UTF-8 is replaced by U+FFFD. Offsets always count raw bytes,
// never the length of the decoded string.
func processOutputWindow(input commandWindowInput) commandWindow {
	window := commandWindow{
		start:     input.start,
		next:      input.start,
		truncated: input.truncated,
	}
	if len(input.data) == 0 {
		return window
	}
	data := input.data
	if !utf8.RuneStart(data[0]) && input.start > 0 {
		cut := 0
		for cut < len(data) && !utf8.RuneStart(data[cut]) {
			cut++
		}
		window.start += int64(cut)
		window.next = window.start
		if cut == len(data) {
			if !input.offsetMode && window.start > 0 {
				window.truncated = true
			}
			return window
		}
		data = data[cut:]
		if !input.offsetMode && window.start > 0 {
			window.truncated = true
		}
	}
	if input.offsetMode {
		cut := len(data)
		if !input.finalEOF {
			cut = completeUTF8Prefix(data)
		}
		if cut == 0 {
			window.next = window.start
			return window
		}
		window.text = strings.ToValidUTF8(string(data[:cut]), "\uFFFD")
		window.next = window.start + int64(cut)
		return window
	}
	window.text = strings.ToValidUTF8(string(data), "\uFFFD")
	if window.start > 0 {
		window.truncated = true
	}
	return window
}
