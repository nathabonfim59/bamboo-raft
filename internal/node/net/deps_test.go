package nodenet

import (
	"bytes"
	"context"
	"testing"
)

func TestLimitedBuffer(t *testing.T) {
	tests := []struct {
		name   string
		limit  int
		writes []string
		want   string
	}{
		{name: "under the limit", limit: 8, writes: []string{"abc", "de"}, want: "abcde"},
		{name: "exactly the limit", limit: 5, writes: []string{"abcde"}, want: "abcde"},
		{name: "truncates the tail", limit: 5, writes: []string{"abc", "defgh"}, want: "abcde"},
		{name: "empty writes", limit: 4, writes: []string{"", "ab", ""}, want: "ab"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := &limitedBuffer{limit: tt.limit}
			for _, write := range tt.writes {
				n, err := buf.Write([]byte(write))
				if err != nil {
					t.Fatalf("Write: %v", err)
				}
				if n != len(write) {
					t.Fatalf("Write reported %d of %d bytes; a child must never see a short write", n, len(write))
				}
			}
			if got := string(buf.Bytes()); got != tt.want {
				t.Fatalf("buffer = %q, want %q", got, tt.want)
			}
			if buf.buf.Len() > tt.limit {
				t.Fatalf("buffer holds %d bytes, limit %d", buf.buf.Len(), tt.limit)
			}
		})
	}
}

// TestHostCommandKillsTheProcessGroup proves a cancelled command dies with its
// context instead of stealing the node's lifetime. The test uses a real shell
// child because the behaviour under test is the exec plumbing.
func TestHostCommandKillsTheProcessGroup(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	cmd := hostCommand(ctx, "sleep", "30")
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		t.Skipf("sleep is unavailable: %v", err)
	}
	cancel()
	if err := cmd.Wait(); err == nil {
		t.Fatal("a cancelled command exited successfully")
	}
}
