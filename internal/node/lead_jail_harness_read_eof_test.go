package node

import (
	"strings"
	"testing"
	"testing/iotest"
)

func TestLeadJailBoundedReadFinalDataWithEOF(t *testing.T) {
	if got, err := jailKVMReadBounded(iotest.DataErrReader(strings.NewReader(strings.Repeat("x", 65))), 64); err == nil {
		t.Fatalf("accepted %d bytes over limit64 when data and EOF arrived together", len(got))
	}
}
