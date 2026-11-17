package guest

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"testing"

	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

// TestReadHelperFrameBounds proves frame lengths are validated before any
// payload allocation.
func TestReadHelperFrameBounds(t *testing.T) {
	header := func(n uint32) []byte {
		return binary.BigEndian.AppendUint32(nil, n)
	}
	valid := append(header(2), frameResult, 'x')
	for _, tc := range []struct {
		name    string
		in      []byte
		kind    byte
		payload string
		wantErr error
	}{
		{name: "zero length", in: header(0), wantErr: errHelperProtocol},
		{name: "too large", in: header(maxHelperFrame + 1), wantErr: errHelperProtocol},
		{name: "truncated kind", in: header(2), wantErr: io.EOF},
		{name: "truncated payload", in: append(header(3), frameResult, 'x'), wantErr: io.ErrUnexpectedEOF},
		{name: "valid", in: valid, kind: frameResult, payload: "x"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			kind, payload, err := readHelperFrame(bytes.NewReader(tc.in))
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("readHelperFrame = %d, %q, %v; want %v", kind, payload, err, tc.wantErr)
				}
				return
			}
			if err != nil || kind != tc.kind || string(payload) != tc.payload {
				t.Fatalf("readHelperFrame = %d, %q, %v; want %d, %q", kind, payload, err, tc.kind, tc.payload)
			}
		})
	}
}

// TestWriteHelperFrameBounds rejects an oversized frame before writing.
func TestWriteHelperFrameBounds(t *testing.T) {
	var buf bytes.Buffer
	out := bufio.NewWriter(&buf)
	if err := writeHelperFrame(out, frameResult, make([]byte, maxHelperFrame)); !errors.Is(err, errHelperProtocol) {
		t.Fatalf("writeHelperFrame oversized = %v, want errHelperProtocol", err)
	}
	if buf.Len() != 0 {
		t.Fatalf("oversized frame wrote %d bytes", buf.Len())
	}
}

// TestFileInfoFromDTORejectsInvalidType keeps a malformed child result from
// smuggling an unknown enum onto the wire.
func TestFileInfoFromDTORejectsInvalidType(t *testing.T) {
	if _, err := fileInfoFromDTO(fileInfoDTO{Type: int32(nodev1.FileType_FILE_TYPE_OTHER) + 1}); !errors.Is(err, errHelperProtocol) {
		t.Fatalf("fileInfoFromDTO invalid type = %v, want errHelperProtocol", err)
	}
	if _, err := fileInfoFromDTO(fileInfoDTO{Type: -1}); !errors.Is(err, errHelperProtocol) {
		t.Fatalf("fileInfoFromDTO negative type = %v, want errHelperProtocol", err)
	}
}
