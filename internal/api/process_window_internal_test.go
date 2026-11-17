package api

import "testing"

func TestProcessOutputWindowBoundaries(t *testing.T) {
	tests := []struct {
		name      string
		input     commandWindowInput
		wantText  string
		wantStart int64
		wantNext  int64
		wantTrunc bool
	}{
		{
			name: "tail cut advances leading continuation even when ended",
			input: commandWindowInput{
				data: []byte{0x80, 'a'}, start: 4, finalEOF: true,
			},
			wantText:  "a",
			wantStart: 5,
			wantNext:  5,
			wantTrunc: true,
		},
		{
			name: "invalid leading byte at raw zero is replaced",
			input: commandWindowInput{
				data: []byte{0x80, 'a'}, start: 0, finalEOF: true,
			},
			wantText:  "\uFFFDa",
			wantStart: 0,
			wantNext:  0,
		},
		{
			name: "multi-window partial rune is not replaced",
			input: commandWindowInput{
				data: []byte{'a', 0xC3}, start: 0, offsetMode: true,
			},
			wantText:  "a",
			wantStart: 0,
			wantNext:  1,
		},
		{
			name: "ended final partial rune is replaced and advances",
			input: commandWindowInput{
				data: []byte{'a', 0xC3}, start: 0, offsetMode: true, finalEOF: true,
			},
			wantText:  "a\uFFFD",
			wantStart: 0,
			wantNext:  2,
		},
		{
			name: "partial rune waits without progress",
			input: commandWindowInput{
				data: []byte{0xC3}, start: 0, offsetMode: true,
			},
			wantText:  "",
			wantStart: 0,
			wantNext:  0,
		},
		{
			name: "all continuation bytes at a nonzero start advance the cursor",
			input: commandWindowInput{
				data: []byte{0x80, 0x80}, start: 3, offsetMode: true, finalEOF: true,
			},
			wantText:  "",
			wantStart: 5,
			wantNext:  5,
		},
		{
			name: "final EOF replaces an invalid interior byte",
			input: commandWindowInput{
				data: []byte{'a', 0xFF, 'b'}, start: 0, offsetMode: true, finalEOF: true,
			},
			wantText:  "a\uFFFDb",
			wantStart: 0,
			wantNext:  3,
		},
		{
			name: "stale tail size with native More still clamps a partial rune",
			input: commandWindowInput{
				data: append([]byte("aaaaaaaaa"), 0xC3), start: 0, offsetMode: true,
			},
			wantText:  "aaaaaaaaa",
			wantStart: 0,
			wantNext:  9,
		},
		{
			name: "stale tail size at native EOF consumes the invalid suffix",
			input: commandWindowInput{
				data: append([]byte("aaaaaaaaa"), 0xC3), start: 0, offsetMode: true, finalEOF: true,
			},
			wantText:  "aaaaaaaaa\uFFFD",
			wantStart: 0,
			wantNext:  10,
		},
		{
			name: "clamped EOF is empty",
			input: commandWindowInput{
				data: nil, start: 0, offsetMode: true, finalEOF: true,
			},
			wantText:  "",
			wantStart: 0,
			wantNext:  0,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			window := processOutputWindow(test.input)
			if window.text != test.wantText || window.start != test.wantStart ||
				window.next != test.wantNext || window.truncated != test.wantTrunc {
				t.Errorf("window = %+v, want text %q start %d next %d truncated %v",
					window, test.wantText, test.wantStart, test.wantNext, test.wantTrunc)
			}
		})
	}
}
