package node

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

// TestJailKVMWaitNewLink: after a restart the Hub's map may still hold the
// old link; the waiter returns only a link whose Done differs from the old
// one, waiting for the old one to close, bounded by ctx.
func TestJailKVMWaitNewLink(t *testing.T) {
	oldDone, newDone := make(chan struct{}), make(chan struct{})
	old, fresh := jailKVMLink{Done: oldDone}, jailKVMLink{Done: newDone}
	t.Run("old entry until it closes, then the new link", func(t *testing.T) {
		// Witness ordering: the Hub returns the old entry first; the test
		// closes the old link only after that ask returned, and every later
		// ask must come after the close (the Hub keeps the stale entry for
		// one more ask, then returns the new link).
		first := make(chan struct{})
		var mu sync.Mutex
		calls, early := 0, 0
		wait := func(context.Context) (jailKVMLink, error) {
			mu.Lock()
			defer mu.Unlock()
			calls++
			if calls == 1 {
				close(first)
				return old, nil
			}
			select {
			case <-oldDone:
			default:
				early++
			}
			if calls == 2 {
				return old, nil
			}
			return fresh, nil
		}
		type res struct {
			l   jailKVMLink
			err error
		}
		out := make(chan res, 1)
		go func() {
			l, err := jailKVMWaitNewLink(t.Context(), oldDone, wait, time.Millisecond)
			out <- res{l, err}
		}()
		<-first
		close(oldDone)
		r := <-out
		mu.Lock()
		defer mu.Unlock()
		if r.err != nil || r.l.Done != fresh.Done || calls != 3 || early != 0 {
			t.Fatalf("link %v, %v after %d asks (%d before the old link closed): want the new link on the third ask, none early", r.l, r.err, calls, early)
		}
	})
	t.Run("no previous link takes the first", func(t *testing.T) {
		got, err := jailKVMWaitNewLink(t.Context(), nil, func(context.Context) (jailKVMLink, error) { return old, nil }, time.Millisecond)
		if err != nil || got.Done != old.Done {
			t.Fatalf("link %v, %v", got, err)
		}
	})
	t.Run("an old link that never closes is bounded", func(t *testing.T) {
		stuck := make(chan struct{})
		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
		defer cancel()
		_, err := jailKVMWaitNewLink(ctx, stuck, func(context.Context) (jailKVMLink, error) { return jailKVMLink{Done: stuck}, nil }, time.Millisecond)
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("err %v, want DeadlineExceeded", err)
		}
	})
	t.Run("a wait error is returned", func(t *testing.T) {
		boom := errors.New("hub closed")
		if _, err := jailKVMWaitNewLink(t.Context(), oldDone, func(context.Context) (jailKVMLink, error) { return jailKVMLink{}, boom }, time.Millisecond); !errors.Is(err, boom) {
			t.Fatalf("err %v", err)
		}
	})
}

// jailKVMFakeNode is a NodeClient with scripted ListVMs and DeleteVM.
type jailKVMFakeNode struct {
	nodev1.NodeClient
	mu        sync.Mutex
	calls     *[]string
	vms       []string
	deleteErr error
	keep      bool // DeleteVM succeeds but the VM stays listed
}

func (f *jailKVMFakeNode) ListVMs(context.Context, *nodev1.ListVMsRequest, ...grpc.CallOption) (*nodev1.ListVMsResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	*f.calls = append(*f.calls, "list")
	out := &nodev1.ListVMsResponse{}
	for _, id := range f.vms {
		out.Vms = append(out.Vms, &nodev1.VM{SandboxId: id})
	}
	return out, nil
}

func (f *jailKVMFakeNode) DeleteVM(_ context.Context, r *nodev1.DeleteVMRequest, _ ...grpc.CallOption) (*nodev1.DeleteVMResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	*f.calls = append(*f.calls, "delete "+r.GetSandboxId())
	if f.deleteErr != nil {
		return nil, f.deleteErr
	}
	if !f.keep {
		f.vms = slices.DeleteFunc(f.vms, func(id string) bool { return id == r.GetSandboxId() })
	}
	return &nodev1.DeleteVMResponse{}, nil
}

// TestJailKVMRecoverOnce: the teardown's one bounded recovery restarts the
// unit, reaches it through a new link, deletes every VM through the API and
// proves the list empty, then always stops the unit and re-proves the stop
// and the subtree, whatever failed before; any failure is reported so the
// plan preserves everything.
func TestJailKVMRecoverOnce(t *testing.T) {
	type tc struct {
		startErr, connectErr, deleteErr error
		keep, stopped                   bool
		subtree                         error
		wantErr                         string
		wantCalls                       string
	}
	for name, c := range map[string]tc{
		"recovered":       {stopped: true, wantCalls: "start connect list delete bx_a delete bx_b list stop subtree"},
		"start fails":     {startErr: errors.New("start refused"), stopped: true, wantErr: "start refused", wantCalls: "start stop subtree"},
		"connect fails":   {connectErr: errors.New("no link"), stopped: true, wantErr: "no link", wantCalls: "start connect stop subtree"},
		"delete fails":    {deleteErr: errors.New("busy"), stopped: true, wantErr: "busy", wantCalls: "start connect list delete bx_a delete bx_b stop subtree"},
		"still listed":    {keep: true, stopped: true, wantErr: "still lists", wantCalls: "start connect list delete bx_a delete bx_b list stop subtree"},
		"stop unproven":   {stopped: false, wantErr: "", wantCalls: "start connect list delete bx_a delete bx_b list stop subtree"},
		"still populated": {stopped: true, subtree: errJailKVMPopulated, wantCalls: "start connect list delete bx_a delete bx_b list stop subtree"},
	} {
		t.Run(name, func(t *testing.T) {
			var calls []string
			node := &jailKVMFakeNode{calls: &calls, vms: []string{"bx_a", "bx_b"}, deleteErr: c.deleteErr, keep: c.keep}
			stopped, subtree, err := jailKVMRecoverOnce(t.Context(), jailKVMRecoverOps{
				Start: func(context.Context) error { calls = append(calls, "start"); return c.startErr },
				Connect: func(context.Context) (nodev1.NodeClient, error) {
					calls = append(calls, "connect")
					if c.connectErr != nil {
						return nil, c.connectErr
					}
					return node, nil
				},
				Stop:    func(context.Context) (bool, error) { calls = append(calls, "stop"); return c.stopped, nil },
				Subtree: func(context.Context) error { calls = append(calls, "subtree"); return c.subtree },
			})
			if got := strings.Join(calls, " "); got != c.wantCalls {
				t.Fatalf("calls %q, want %q", got, c.wantCalls)
			}
			if stopped != c.stopped || !errors.Is(subtree, c.subtree) || (subtree == nil) != (c.subtree == nil) {
				t.Fatalf("stopped %v subtree %v, want %v and %v", stopped, subtree, c.stopped, c.subtree)
			}
			if c.wantErr == "" && err != nil || c.wantErr != "" && (err == nil || !strings.Contains(err.Error(), c.wantErr)) {
				t.Fatalf("err %v, want %q", err, c.wantErr)
			}
		})
	}
	t.Run("canceled before start does nothing", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		var calls []string
		op := func(context.Context) error { calls = append(calls, "op"); return nil }
		_, _, err := jailKVMRecoverOnce(ctx, jailKVMRecoverOps{Start: op, Subtree: op,
			Connect: func(context.Context) (nodev1.NodeClient, error) { calls = append(calls, "connect"); return nil, nil },
			Stop:    func(context.Context) (bool, error) { calls = append(calls, "stop"); return true, nil }})
		if !errors.Is(err, context.Canceled) || len(calls) != 0 {
			t.Fatalf("err %v calls %q", err, calls)
		}
	})
}

// TestJailKVMCleanupPlanRecovery: a failed recovery preserves everything.
func TestJailKVMCleanupPlanRecovery(t *testing.T) {
	plan := jailKVMCleanupPlan(jailKVMTeardown{UnitStopped: true, Recovery: errors.New("no link")})
	if len(plan.Preserve) == 0 || len(plan.Remove) != 0 || !strings.Contains(strings.Join(plan.Preserve, ";"), "no link") {
		t.Fatalf("plan %+v: want everything preserved with the recovery failure", plan)
	}
}
