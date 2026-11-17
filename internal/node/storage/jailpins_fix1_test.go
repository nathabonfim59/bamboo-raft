package storage

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"uuid"
)

// Slice2 fix round 1: honest cancellation after a published intent, and the
// inode and directory barriers a jailed layout needs before it is used.

func (f *storageFaults) count(op string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls[op]
}

func (f *storageFaults) reset(fail func(op string, n int, root *os.Root) error) {
	f.mu.Lock()
	f.calls = nil
	f.fail = fail
	f.mu.Unlock()
}

// TestStoreJailedIntentCanceledDuringPublish (lead regression): a context
// canceled while the intent is being published is reported, with the intent
// classified as published, never as a clean success.
func TestStoreJailedIntentCanceledDuringPublish(t *testing.T) {
	faults := &storageFaults{}
	s, _ := storageJailFixture(t, nil, faults)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	faults.reset(func(op string, n int, _ *os.Root) error {
		if op == "rename" && n == 1 {
			cancel()
		}
		return nil
	})
	attempt := uuid.NewV7().String()
	err := s.WriteJailedLaunchIntent(ctx, storageJailID, attempt, strings.Repeat("ab", 32))
	if !errors.Is(err, context.Canceled) || !errors.Is(err, ErrIntentPublished) {
		t.Fatalf("intent canceled during publish = %v; want Canceled and ErrIntentPublished", err)
	}
	if meta, _ := s.ReadVM(storageJailID); meta.QEMU == nil || meta.QEMU.JailAttempt != attempt {
		t.Fatal("the published intent is not the attempt reported")
	}
}

// TestStorePinLaunchCanceledAfterLayoutPublish: cancellation after the
// layout record is published returns an error and no pins; the published
// layout is reused by a retry.
func TestStorePinLaunchCanceledAfterLayoutPublish(t *testing.T) {
	faults := &storageFaults{}
	s, img := storageJailFixture(t, nil, faults)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	faults.reset(func(op string, n int, _ *os.Root) error {
		if op == "rename" && n == 1 {
			cancel()
		}
		return nil
	})
	if pins, err := s.PinLaunch(ctx, storageJailID, img); !errors.Is(err, context.Canceled) {
		pins.Close()
		t.Fatalf("PinLaunch = %v, want Canceled", err)
	}
	meta, err := s.ReadVM(storageJailID)
	if err != nil || meta.Control == nil {
		t.Fatalf("published layout lost: %v %+v", err, meta.Control)
	}
	faults.reset(nil)
	pins, err := s.PinLaunch(t.Context(), storageJailID, img)
	if err != nil {
		t.Fatal(err)
	}
	defer pins.Close()
	if storageIdentity(t, pins.Control) != *meta.Control {
		t.Fatal("retry did not reuse the published layout")
	}
}

// TestStoreLayoutBarriers: new layout entries get their own inode barriers
// and the parent barrier before the naming manifest is written, and every
// pin (including a retry after an uncertain manifest sync) re-establishes
// the inode and directory barriers before returning.
func TestStoreLayoutBarriers(t *testing.T) {
	faults := &storageFaults{}
	s, img := storageJailFixture(t, nil, faults)
	var order []string
	faults.reset(func(op string, n int, _ *os.Root) error {
		order = append(order, op)
		return nil
	})
	pins, err := s.PinLaunch(t.Context(), storageJailID, img)
	if err != nil {
		t.Fatal(err)
	}
	pins.Close()
	index := func(op string) int {
		for i, o := range order {
			if o == op {
				return i
			}
		}
		return -1
	}
	rename := index("rename")
	for _, barrier := range []string{"syncfile:" + ControlDirName, "syncfile:" + ConsoleLogName, "syncdir"} {
		if i := index(barrier); i < 0 || i > rename {
			t.Fatalf("barrier %s at %d, manifest rename at %d: %v", barrier, i, rename, order)
		}
	}
	// A pin of the recorded layout re-runs its barriers.
	faults.reset(nil)
	pins, err = s.PinLaunch(t.Context(), storageJailID, img)
	if err != nil {
		t.Fatal(err)
	}
	pins.Close()
	for _, op := range []string{"syncfile:" + ControlDirName, "syncfile:" + ConsoleLogName, "syncdir"} {
		if faults.count(op) == 0 {
			t.Fatalf("a recorded-layout pin skipped %s", op)
		}
	}
}

func TestStorePinLaunchRetryAfterUncertainManifestSync(t *testing.T) {
	injected := errors.New("injected fault")
	faults := &storageFaults{}
	s, img := storageJailFixture(t, nil, faults)
	// The post-rename directory sync of the manifest fails: the layout is
	// visible but its durability is unproven.
	faults.reset(func(op string, n int, _ *os.Root) error {
		if op == "syncdir" && n == 2 {
			return injected
		}
		return nil
	})
	if _, err := s.PinLaunch(t.Context(), storageJailID, img); !errors.Is(err, ErrManifestDurabilityUnknown) {
		t.Fatalf("PinLaunch = %v, want ErrManifestDurabilityUnknown", err)
	}
	// A failing barrier on the retry is reported, not skipped.
	faults.reset(func(op string, n int, _ *os.Root) error {
		if op == "syncdir" {
			return injected
		}
		return nil
	})
	if pins, err := s.PinLaunch(t.Context(), storageJailID, img); !errors.Is(err, injected) {
		pins.Close()
		t.Fatalf("retry with a failing directory barrier = %v", err)
	}
	faults.reset(nil)
	pins, err := s.PinLaunch(t.Context(), storageJailID, img)
	if err != nil {
		t.Fatal(err)
	}
	pins.Close()
	if faults.count("syncdir") == 0 {
		t.Fatal("the successful retry did not re-establish the directory barrier")
	}
}

func TestStoreSyncEntry(t *testing.T) {
	faults := &storageFaults{}
	s, _ := storageJailFixture(t, nil, faults)
	f, err := s.OpenVMEntry(t.Context(), storageJailID, diskName, os.O_RDONLY)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	faults.reset(nil)
	if err := s.SyncEntry(t.Context(), f); err != nil {
		t.Fatal(err)
	}
	if faults.count("syncfile:"+diskName) != 1 {
		t.Fatal("SyncEntry did not use the store's sync seam")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := s.SyncEntry(ctx, f); !errors.Is(err, context.Canceled) {
		t.Fatalf("SyncEntry canceled = %v", err)
	}
}
