package node

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

// jailKVMFakeUnit scripts the unit at each Show (the last step repeats) and
// serves journals per invocation.
type jailKVMFakeUnit struct {
	mu        sync.Mutex
	steps     []jailKVMFakeUnitStep
	shows     int
	journals  map[string][]string
	journalOK func(n int) bool // optional: whether the n-th journal read works
	reads     int
	queried   []string
	calls     []string
}

type jailKVMFakeUnitStep struct{ inv, state, restarts string }

func (f *jailKVMFakeUnit) ops() jailKVMReadyOps {
	return jailKVMReadyOps{
		Show: func(context.Context) (string, string, string, error) {
			f.mu.Lock()
			defer f.mu.Unlock()
			s := f.steps[min(f.shows, len(f.steps)-1)]
			f.shows++
			f.calls = append(f.calls, "show")
			return s.inv, s.state, s.restarts, nil
		},
		Journal: func(_ context.Context, inv string) ([]string, error) {
			f.mu.Lock()
			defer f.mu.Unlock()
			f.reads++
			f.queried = append(f.queried, inv)
			f.calls = append(f.calls, "journal")
			if f.journalOK != nil && !f.journalOK(f.reads) {
				return nil, errors.New("journal busy")
			}
			// The n-th read of an invocation sees its first n lines.
			lines := f.journals[inv]
			n := 0
			for _, q := range f.queried {
				if q == inv {
					n++
				}
			}
			return slices.Clone(lines[:min(n, len(lines))]), nil
		},
	}
}

const jailKVMFakeReady = `time=T level=INFO msg="bbraft-node ready" version=dev`

// TestJailKVMAwaitReady: the startup poll follows exactly the original
// invocation: its journal only, and a refusal the moment the unit shows
// another InvocationID or more restarts (Restart=on-failure keeps a
// crash-looping unit "active"), instead of polling a restarted unit until
// the deadline.
func TestJailKVMAwaitReady(t *testing.T) {
	await := func(f *jailKVMFakeUnit, timeout time.Duration) ([]string, error) {
		return jailKVMAwaitReady(t.Context(), "inv1", timeout, time.Millisecond, f.ops())
	}
	orig := []string{`msg="jail admitted"`, `msg="cpu realized"`, jailKVMFakeReady}
	other := []string{`msg="jail admitted"`, jailKVMFakeReady}
	t.Run("ready in the original invocation", func(t *testing.T) {
		f := &jailKVMFakeUnit{steps: []jailKVMFakeUnitStep{{"inv1", "activating", "0"}, {"inv1", "active", "0"}}, journals: map[string][]string{"inv1": orig, "inv2": other}}
		lines, err := await(f, 5*time.Second)
		if err != nil || !slices.Equal(lines, orig) {
			t.Fatalf("lines %q, %v; want the original invocation's whole journal", lines, err)
		}
		f.mu.Lock()
		defer f.mu.Unlock()
		if slices.ContainsFunc(f.queried, func(q string) bool { return q != "inv1" }) {
			t.Fatalf("journals of %q read; only inv1 may be", f.queried)
		}
		// Ready is confirmed by a Show after the journal read.
		if f.calls[len(f.calls)-1] != "show" || f.calls[len(f.calls)-2] != "journal" {
			t.Fatalf("calls %q: the ready journal must be followed by a confirming show", f.calls)
		}
	})
	t.Run("a transient journal error is retried", func(t *testing.T) {
		f := &jailKVMFakeUnit{steps: []jailKVMFakeUnitStep{{"inv1", "active", "0"}}, journals: map[string][]string{"inv1": orig},
			journalOK: func(n int) bool { return n%2 == 0 }}
		if lines, err := await(f, 5*time.Second); err != nil || !slices.Equal(lines, orig) {
			t.Fatalf("lines %q, %v", lines, err)
		}
	})
	for name, tc := range map[string]struct {
		steps []jailKVMFakeUnitStep
		want  string
	}{
		"restarted under a new invocation":  {[]jailKVMFakeUnitStep{{"inv1", "active", "0"}, {"inv2", "active", "1"}}, "inv2"},
		"new invocation, same count":        {[]jailKVMFakeUnitStep{{"inv1", "active", "0"}, {"inv2", "active", "0"}}, "inv2"},
		"restart count grew":                {[]jailKVMFakeUnitStep{{"inv1", "active", "0"}, {"inv1", "active", "1"}}, "NRestarts"},
		"another invocation from the start": {[]jailKVMFakeUnitStep{{"inv0", "active", "3"}}, "inv0"},
		"failed":                            {[]jailKVMFakeUnitStep{{"inv1", "active", "0"}, {"inv1", "failed", "0"}}, "failed"},
		"inactive":                          {[]jailKVMFakeUnitStep{{"inv1", "inactive", "0"}}, "inactive"},
		"deactivating":                      {[]jailKVMFakeUnitStep{{"inv1", "deactivating", "0"}}, "deactivating"},
		"unparsable restart count":          {[]jailKVMFakeUnitStep{{"inv1", "active", "x"}}, "NRestarts"},
	} {
		t.Run(name, func(t *testing.T) {
			f := &jailKVMFakeUnit{steps: tc.steps, journals: map[string][]string{"inv1": orig[:2], "inv2": other}}
			lines, err := await(f, 5*time.Second)
			if err == nil || errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), tc.want) || lines != nil {
				t.Fatalf("lines %q, err %v: want an immediate refusal naming %q", lines, err, tc.want)
			}
			f.mu.Lock()
			defer f.mu.Unlock()
			if slices.ContainsFunc(f.queried, func(q string) bool { return q != "inv1" }) {
				t.Fatalf("journals of %q read; only inv1 may be", f.queried)
			}
		})
	}
	t.Run("ready seen, then the confirming show is another invocation", func(t *testing.T) {
		f := &jailKVMFakeUnit{steps: []jailKVMFakeUnitStep{{"inv1", "active", "0"}, {"inv2", "active", "1"}}, journals: map[string][]string{"inv1": orig[2:]}}
		if lines, err := await(f, 5*time.Second); err == nil || lines != nil {
			t.Fatalf("lines %q, %v: a ready line of an invocation that is gone was accepted", lines, err)
		}
	})
	t.Run("never ready times out", func(t *testing.T) {
		f := &jailKVMFakeUnit{steps: []jailKVMFakeUnitStep{{"inv1", "active", "0"}}, journals: map[string][]string{"inv1": orig[:2]}}
		if _, err := await(f, 30*time.Millisecond); !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "cpu realized") {
			t.Fatalf("err %v: want DeadlineExceeded with the original journal tail", err)
		}
	})
	t.Run("pre-canceled reads nothing", func(t *testing.T) {
		f := &jailKVMFakeUnit{steps: []jailKVMFakeUnitStep{{"inv1", "active", "0"}}, journals: map[string][]string{"inv1": orig}}
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		if _, err := jailKVMAwaitReady(ctx, "inv1", time.Second, time.Millisecond, f.ops()); !errors.Is(err, context.Canceled) {
			t.Fatalf("err %v", err)
		}
		if len(f.calls) != 0 {
			t.Fatalf("calls %q", f.calls)
		}
	})
}
