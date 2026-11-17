package node

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLeadJailCPAMountOrder(t *testing.T) {
	for _, tc := range []struct {
		name, raw string
		wantErr   bool
	}{
		{"writable top first", "600 500 253:1 /jail/root / rw - ext4 /dev/vda1 rw\n500 499 253:1 /jail/root / ro - ext4 /dev/vda1 rw\n", true},
		{"readonly top first", "600 500 253:1 /jail/root / ro - ext4 /dev/vda1 rw\n500 499 253:1 /jail/root / rw - ext4 /dev/vda1 rw\n", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := jailKVMRootMountReadOnly(tc.raw)
			if (err != nil) != tc.wantErr {
				t.Fatalf("error %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}

func TestLeadJailCPAMalformedProcsNotRecoverable(t *testing.T) {
	d := t.TempDir()
	for n, v := range map[string]string{"cgroup.events": "populated 0\n", "cgroup.procs": "not-a-pid\n"} {
		if err := os.WriteFile(filepath.Join(d, n), []byte(v), 0600); err != nil {
			t.Fatal(err)
		}
	}
	err := jailKVMSubtreeEmpty(t.Context(), d, 16)
	if err == nil || errors.Is(err, errJailKVMPopulated) {
		t.Fatalf("malformed process evidence classified as recoverable: %v", err)
	}
}

func TestLeadJailCPAWaitCancellation(t *testing.T) {
	for _, during := range []bool{false, true} {
		t.Run(map[bool]string{false: "before", true: "at-handoff"}[during], func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if !during {
				cancel()
			}
			called := false
			_, err := jailKVMWaitNewLink(ctx, nil, func(context.Context) (jailKVMLink, error) {
				called = true
				cancel()
				return jailKVMLink{Done: make(chan struct{})}, nil
			}, time.Millisecond)
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("new link returned after cancellation: %v", err)
			}
			if !during && called {
				t.Fatal("wait called for pre-canceled context")
			}
		})
	}
}
