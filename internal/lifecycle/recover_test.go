package lifecycle_test

// Real-Postgres + nodelink + nodegateway + nodefake tests for sandbox
// recovery: end-to-end API -> job -> ResetVM -> sandbox.recovered, concurrent
// API recovers, FAILED_PRECONDITION -> sandbox.recover_failed with no retry,
// UNAVAILABLE backoff retries, skip on state/node change, and lease loss
// before completion writing no success event. Gated by BBRAFT_TEST_DATABASE_URL.

import (
	"context"
	"encoding/json/v2"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"uuid"

	"github.com/nathabonfim59/bamboo-raft/internal/api"
	"github.com/nathabonfim59/bamboo-raft/internal/auth"
	"github.com/nathabonfim59/bamboo-raft/internal/db"
	"github.com/nathabonfim59/bamboo-raft/internal/lifecycle"
	"github.com/nathabonfim59/bamboo-raft/internal/nodefake"
	"github.com/nathabonfim59/bamboo-raft/internal/nodelink"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

// recoverTestNode wraps *nodefake.Node to count ResetVM calls and inject
// ResetVM or Exec behaviour without modifying nodefake.
type recoverTestNode struct {
	*nodefake.Node

	mu        sync.Mutex
	resets    []string
	resetHook func(context.Context, *nodev1.ResetVMRequest) (*nodev1.VM, error)
	hangExec  atomic.Bool
}

func (n *recoverTestNode) setResetHook(h func(context.Context, *nodev1.ResetVMRequest) (*nodev1.VM, error)) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.resetHook = h
}

func (n *recoverTestNode) resetCalls() []string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return append([]string(nil), n.resets...)
}

func (n *recoverTestNode) ResetVM(ctx context.Context, req *nodev1.ResetVMRequest) (*nodev1.VM, error) {
	n.mu.Lock()
	n.resets = append(n.resets, req.GetSandboxId())
	hook := n.resetHook
	n.mu.Unlock()
	if hook != nil {
		return hook(ctx, req)
	}
	return n.Node.ResetVM(ctx, req)
}

func (n *recoverTestNode) Exec(req *nodev1.ExecRequest, stream nodev1.Node_ExecServer) error {
	if n.hangExec.Load() {
		<-stream.Context().Done()
		return status.FromContextError(stream.Context().Err()).Err()
	}
	return n.Node.Exec(req, stream)
}

func recoverStartNode(t *testing.T, e *lifecycleEnv, id string) *recoverTestNode {
	t.Helper()
	fake := nodefake.New(id, t.TempDir())
	fake.VCPUs = 64
	fake.MemoryMiB = 256 << 10
	n := &recoverTestNode{Node: fake}

	srv := grpc.NewServer(nodelink.ServerOptions()...)
	nodev1.RegisterNodeServer(srv, n)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_ = nodelink.Serve(ctx, nodelink.DialConfig{
			Addr: e.addr, NodeID: id, Token: "node-token", Log: quiet(),
			MinBackoff: 10 * time.Millisecond, MaxBackoff: 50 * time.Millisecond,
		}, srv)
		close(done)
	}()
	t.Cleanup(func() { cancel(); <-done })

	waitCtx, waitCancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer waitCancel()
	if _, err := e.hub.Wait(waitCtx, id); err != nil {
		t.Fatalf("node %s did not connect: %v", id, err)
	}
	return n
}

func recoverAPIHandler(t *testing.T, e *lifecycleEnv, now func() time.Time) http.HandlerFunc {
	t.Helper()
	clients := func(nodeID string) (nodev1.NodeClient, func() error, error) {
		c, err := e.factory.Dial(nodeID)
		if err != nil {
			return nil, nil, err
		}
		return c, c.Close, nil
	}
	h, err := api.NewRecoverHandler(api.RecoverOptions{
		Store:        e.store,
		Clients:      clients,
		Logger:       quiet(),
		Now:          now,
		ProbeTimeout: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewRecoverHandler: %v", err)
	}
	return h
}

func recoverPost(h http.HandlerFunc, own uuid.UUID, sandboxID string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/api/v1/sandboxes/"+sandboxID+"/recover", nil)
	req.SetPathValue(api.ParamSandboxID, sandboxID)
	req = req.WithContext(auth.WithIdentity(req.Context(), auth.Identity{Owner: own.String()}))
	rec := httptest.NewRecorder()
	h(rec, req)
	return rec
}

func recoverEventsOfType(t *testing.T, e *lifecycleEnv, own uuid.UUID, sandboxID, eventType string) []db.SandboxEvent {
	t.Helper()
	all, err := e.store.SandboxEvents(t.Context(), own, sandboxID, 100)
	if err != nil {
		t.Fatalf("SandboxEvents: %v", err)
	}
	var out []db.SandboxEvent
	for _, ev := range all {
		if ev.Type == eventType {
			out = append(out, ev)
		}
	}
	return out
}

func TestRecoverHappyPathEndToEnd(t *testing.T) {
	e := newLifecycleEnv(t)
	e.registerNode(t, "node-a", db.Resources{VCPUs: 4, MemoryMiB: 4096, DiskGiB: 20})
	n := recoverStartNode(t, e, "node-a")
	own := e.owner(t)
	res := e.createSandbox(t, own, "node-a", 3)
	w := provisionReady(t, e, own, res)

	now := time.Date(2026, 11, 21, 12, 0, 0, 0, time.UTC)
	h := recoverAPIHandler(t, e, func() time.Time { return now })

	// Healthy VM is refused with 409 sandbox_is_reachable.
	rec := recoverPost(h, own, res.Sandbox.ID)
	if rec.Code != http.StatusConflict || !strings.Contains(rec.Body.String(), `"sandbox_is_reachable"`) {
		t.Fatalf("healthy recover: status=%d body=%s", rec.Code, rec.Body.String())
	}

	// Make Exec hang to simulate an unresponsive guest; recover is accepted.
	n.hangExec.Store(true)
	rec = recoverPost(h, own, res.Sandbox.ID)
	if rec.Code != http.StatusOK {
		t.Fatalf("unresponsive recover: status=%d body=%s", rec.Code, rec.Body.String())
	}
	sbx := e.sandbox(t, own, res.Sandbox.ID)
	if sbx.LastRecoverAt == nil || !sbx.LastRecoverAt.Equal(now) {
		t.Fatalf("LastRecoverAt = %v, want %v", sbx.LastRecoverAt, now)
	}
	if evs := recoverEventsOfType(t, e, own, res.Sandbox.ID, "sandbox.recovering"); len(evs) != 1 {
		t.Fatalf("sandbox.recovering events = %d, want 1", len(evs))
	}

	// Worker executes the recover job and calls ResetVM once.
	if !mustProcess(t, w) {
		t.Fatal("recover job was not claimed")
	}
	if calls := n.resetCalls(); len(calls) != 1 || calls[0] != res.Sandbox.ID {
		t.Fatalf("ResetVM calls = %v, want [%s]", calls, res.Sandbox.ID)
	}
	if state := e.jobStateFor(t, db.JobRecover, res.Sandbox.ID); state != db.JobDone {
		t.Fatalf("recover job state = %s, want done", state)
	}
	if evs := recoverEventsOfType(t, e, own, res.Sandbox.ID, "sandbox.recovered"); len(evs) != 1 {
		t.Fatalf("sandbox.recovered events = %d, want 1", len(evs))
	}
	if evs := recoverEventsOfType(t, e, own, res.Sandbox.ID, "sandbox.recover_failed"); len(evs) != 0 {
		t.Fatalf("unexpected sandbox.recover_failed events: %+v", evs)
	}
}

func TestRecoverConcurrentHTTP(t *testing.T) {
	e := newLifecycleEnv(t)
	e.registerNode(t, "node-a", db.Resources{VCPUs: 4, MemoryMiB: 4096, DiskGiB: 20})
	n := recoverStartNode(t, e, "node-a")
	own := e.owner(t)
	res := e.createSandbox(t, own, "node-a", 3)
	_ = provisionReady(t, e, own, res)
	n.hangExec.Store(true)

	now := time.Date(2026, 11, 21, 12, 0, 0, 0, time.UTC)
	h := recoverAPIHandler(t, e, func() time.Time { return now })

	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		okCount int
		tooSoon int
		others  []string
	)
	start := make(chan struct{})
	for range 2 {
		wg.Go(func() {
			<-start
			rec := recoverPost(h, own, res.Sandbox.ID)
			mu.Lock()
			defer mu.Unlock()
			switch rec.Code {
			case http.StatusOK:
				okCount++
			case http.StatusConflict:
				var env struct {
					Code string `json:"code"`
				}
				if err := json.Unmarshal(rec.Body.Bytes(), &env); err == nil && env.Code == "too_soon" {
					tooSoon++
					return
				}
				others = append(others, rec.Body.String())
			default:
				others = append(others, rec.Body.String())
			}
		})
	}
	close(start)
	wg.Wait()

	if len(others) != 0 || okCount != 1 || tooSoon != 1 {
		t.Fatalf("ok=%d too_soon=%d others=%v, want 1 and 1", okCount, tooSoon, others)
	}
	if got := e.count(t, `SELECT count(*) FROM jobs WHERE kind = 'sandbox.recover' AND sandbox_id = $1`, res.Sandbox.ID); got != 1 {
		t.Fatalf("recover jobs = %d, want 1", got)
	}
}

func TestRecoverResetFailedPreconditionNoRetry(t *testing.T) {
	e := newLifecycleEnv(t)
	e.registerNode(t, "node-a", db.Resources{VCPUs: 4, MemoryMiB: 4096, DiskGiB: 20})
	n := recoverStartNode(t, e, "node-a")
	own := e.owner(t)
	res := e.createSandbox(t, own, "node-a", 3)
	w := provisionReady(t, e, own, res)

	const secretText = "secret-node-credential-do-not-leak"
	n.setResetHook(func(context.Context, *nodev1.ResetVMRequest) (*nodev1.VM, error) {
		return nil, status.Error(codes.FailedPrecondition, secretText)
	})
	if _, err := e.store.AcceptRecover(t.Context(), own, res.Sandbox.ID, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}

	if !mustProcess(t, w) {
		t.Fatal("recover job was not claimed")
	}
	if mustProcess(t, w) {
		t.Fatal("FAILED_PRECONDITION recover job must not retry")
	}
	if calls := n.resetCalls(); len(calls) != 1 {
		t.Fatalf("ResetVM calls = %d, want 1", len(calls))
	}
	if state := e.jobStateFor(t, db.JobRecover, res.Sandbox.ID); state != db.JobDone {
		t.Fatalf("job state = %s, want done", state)
	}
	failed := recoverEventsOfType(t, e, own, res.Sandbox.ID, "sandbox.recover_failed")
	if len(failed) != 1 {
		t.Fatalf("sandbox.recover_failed events = %d, want 1", len(failed))
	}
	var data struct {
		Code string `json:"code"`
	}
	if err := json.Unmarshal([]byte(failed[0].Data), &data); err != nil {
		t.Fatalf("decode event data: %v (%s)", err, failed[0].Data)
	}
	if data.Code != "reset_failed" {
		t.Fatalf("event code = %q, want reset_failed", data.Code)
	}
	if strings.Contains(failed[0].Data, secretText) {
		t.Fatal("node error secret leaked into sandbox.recover_failed event")
	}
	if evs := recoverEventsOfType(t, e, own, res.Sandbox.ID, "sandbox.recovered"); len(evs) != 0 {
		t.Fatalf("unexpected sandbox.recovered events: %d", len(evs))
	}
}

func TestRecoverUnavailableRetriesThenSucceeds(t *testing.T) {
	e := newLifecycleEnv(t)
	e.registerNode(t, "node-a", db.Resources{VCPUs: 4, MemoryMiB: 4096, DiskGiB: 20})
	n := recoverStartNode(t, e, "node-a")
	own := e.owner(t)
	res := e.createSandbox(t, own, "node-a", 3)
	w := provisionReady(t, e, own, res)

	var attempts atomic.Int32
	n.setResetHook(func(ctx context.Context, req *nodev1.ResetVMRequest) (*nodev1.VM, error) {
		if attempts.Add(1) == 1 {
			return nil, status.Error(codes.Unavailable, "transient link drop")
		}
		return n.Node.ResetVM(ctx, req)
	})
	if _, err := e.store.AcceptRecover(t.Context(), own, res.Sandbox.ID, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for e.jobStateFor(t, db.JobRecover, res.Sandbox.ID) != db.JobDone {
		if time.Now().After(deadline) {
			t.Fatalf("recover job did not finish: %s", e.jobStateFor(t, db.JobRecover, res.Sandbox.ID))
		}
		_, _ = w.ProcessNext(t.Context())
		time.Sleep(10 * time.Millisecond)
	}
	if got := len(n.resetCalls()); got != 2 {
		t.Fatalf("ResetVM calls = %d, want 2", got)
	}
	if evs := recoverEventsOfType(t, e, own, res.Sandbox.ID, "sandbox.recovered"); len(evs) != 1 {
		t.Fatalf("sandbox.recovered events = %d, want 1", len(evs))
	}
}

func TestRecoverUnavailableExhaustedWritesFailedEvent(t *testing.T) {
	e := newLifecycleEnv(t)
	e.registerNode(t, "node-a", db.Resources{VCPUs: 4, MemoryMiB: 4096, DiskGiB: 20})
	n := recoverStartNode(t, e, "node-a")
	own := e.owner(t)
	res := e.createSandbox(t, own, "node-a", 3)
	w := provisionReady(t, e, own, res)

	n.setResetHook(func(context.Context, *nodev1.ResetVMRequest) (*nodev1.VM, error) {
		return nil, status.Error(codes.Unavailable, "node link stays down")
	})
	if _, err := e.store.AcceptRecover(t.Context(), own, res.Sandbox.ID, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	// Cap max_attempts to 2 so retries exhaust quickly in tests.
	if _, err := e.store.Pool().Exec(t.Context(),
		`UPDATE jobs SET max_attempts = 2 WHERE kind = 'sandbox.recover' AND sandbox_id = $1`, res.Sandbox.ID); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for e.jobStateFor(t, db.JobRecover, res.Sandbox.ID) != db.JobFailed {
		if time.Now().After(deadline) {
			t.Fatalf("recover job did not exhaust: %s", e.jobStateFor(t, db.JobRecover, res.Sandbox.ID))
		}
		_, _ = w.ProcessNext(t.Context())
		time.Sleep(10 * time.Millisecond)
	}
	if got := len(n.resetCalls()); got != 2 {
		t.Fatalf("ResetVM calls = %d, want 2", got)
	}
	if evs := recoverEventsOfType(t, e, own, res.Sandbox.ID, "sandbox.recover_failed"); len(evs) != 1 {
		t.Fatalf("sandbox.recover_failed events = %d, want 1", len(evs))
	}
}

func TestRecoverSkipsWhenSandboxLeftReadyOrMovedNode(t *testing.T) {
	t.Run("moved node", func(t *testing.T) {
		e := newLifecycleEnv(t)
		e.registerNode(t, "node-a", db.Resources{VCPUs: 4, MemoryMiB: 4096, DiskGiB: 20})
		e.registerNode(t, "node-b", db.Resources{VCPUs: 4, MemoryMiB: 4096, DiskGiB: 20})
		n := recoverStartNode(t, e, "node-a")
		own := e.owner(t)
		res := e.createSandbox(t, own, "node-a", 3)
		w := provisionReady(t, e, own, res)

		if _, err := e.store.AcceptRecover(t.Context(), own, res.Sandbox.ID, time.Now().UTC()); err != nil {
			t.Fatal(err)
		}
		if _, err := e.store.Pool().Exec(t.Context(),
			`UPDATE sandboxes SET node_id = 'node-b' WHERE id = $1`, res.Sandbox.ID); err != nil {
			t.Fatal(err)
		}
		if !mustProcess(t, w) {
			t.Fatal("recover job was not claimed")
		}
		if got := len(n.resetCalls()); got != 0 {
			t.Fatalf("ResetVM called %d times after node move, want 0", got)
		}
		if state := e.jobStateFor(t, db.JobRecover, res.Sandbox.ID); state != db.JobDone {
			t.Fatalf("job state = %s, want done", state)
		}
		if evs := recoverEventsOfType(t, e, own, res.Sandbox.ID, "sandbox.recovered"); len(evs) != 0 {
			t.Fatalf("unexpected sandbox.recovered events: %d", len(evs))
		}
	})

	t.Run("left ready state", func(t *testing.T) {
		e := newLifecycleEnv(t)
		e.registerNode(t, "node-a", db.Resources{VCPUs: 4, MemoryMiB: 4096, DiskGiB: 20})
		n := recoverStartNode(t, e, "node-a")
		own := e.owner(t)
		res := e.createSandbox(t, own, "node-a", 3)
		w := provisionReady(t, e, own, res)

		if _, err := e.store.AcceptRecover(t.Context(), own, res.Sandbox.ID, time.Now().UTC()); err != nil {
			t.Fatal(err)
		}
		if _, err := e.store.Pool().Exec(t.Context(),
			`UPDATE sandboxes SET state = 'archived' WHERE id = $1`, res.Sandbox.ID); err != nil {
			t.Fatal(err)
		}
		if !mustProcess(t, w) {
			t.Fatal("recover job was not claimed")
		}
		if got := len(n.resetCalls()); got != 0 {
			t.Fatalf("ResetVM called %d times after archive, want 0", got)
		}
		if state := e.jobStateFor(t, db.JobRecover, res.Sandbox.ID); state != db.JobDone {
			t.Fatalf("job state = %s, want done", state)
		}
	})

	t.Run("changed lifecycle generation", func(t *testing.T) {
		e := newLifecycleEnv(t)
		e.registerNode(t, "node-a", db.Resources{VCPUs: 4, MemoryMiB: 4096, DiskGiB: 20})
		n := recoverStartNode(t, e, "node-a")
		own := e.owner(t)
		res := e.createSandbox(t, own, "node-a", 3)
		w := provisionReady(t, e, own, res)

		if _, err := e.store.AcceptRecover(t.Context(), own, res.Sandbox.ID, time.Now().UTC()); err != nil {
			t.Fatal(err)
		}
		if _, err := e.store.Pool().Exec(t.Context(),
			`UPDATE sandboxes SET lifecycle_generation = lifecycle_generation + 1 WHERE id = $1`, res.Sandbox.ID); err != nil {
			t.Fatal(err)
		}
		if !mustProcess(t, w) {
			t.Fatal("recover job was not claimed")
		}
		if got := len(n.resetCalls()); got != 0 {
			t.Fatalf("ResetVM called %d times after generation change, want 0", got)
		}
		if state := e.jobStateFor(t, db.JobRecover, res.Sandbox.ID); state != db.JobDone {
			t.Fatalf("job state = %s, want done", state)
		}
		if evs := recoverEventsOfType(t, e, own, res.Sandbox.ID, "sandbox.recovered"); len(evs) != 0 {
			t.Fatalf("unexpected sandbox.recovered events: %d", len(evs))
		}
	})
}

func TestRecoverLeaseLossBeforeCompletionWritesNoSuccessEvent(t *testing.T) {
	t.Run("lease expired right before event write", func(t *testing.T) {
		e := newLifecycleEnv(t)
		e.registerNode(t, "node-a", db.Resources{VCPUs: 4, MemoryMiB: 4096, DiskGiB: 20})
		n := recoverStartNode(t, e, "node-a")
		own := e.owner(t)
		res := e.createSandbox(t, own, "node-a", 3)
		_ = provisionReady(t, e, own, res)

		if _, err := e.store.AcceptRecover(t.Context(), own, res.Sandbox.ID, time.Now().UTC()); err != nil {
			t.Fatal(err)
		}
		job, err := e.store.LatestSandboxJob(t.Context(), own, res.Sandbox.ID, db.JobRecover)
		if err != nil {
			t.Fatal(err)
		}

		// Expire the job's lease in Postgres right as ResetVM completes, proving
		// lease-fenced event persistence never writes sandbox.recovered after a
		// lost claim even if ResetVM already returned READY.
		n.setResetHook(func(ctx context.Context, req *nodev1.ResetVMRequest) (*nodev1.VM, error) {
			vm, err := n.Node.ResetVM(ctx, req)
			if err != nil {
				return nil, err
			}
			if _, execErr := e.store.Pool().Exec(context.Background(),
				`UPDATE jobs SET leased_until = now() - interval '1 second' WHERE id = $1`, job.ID); execErr != nil {
				t.Errorf("expire lease: %v", execErr)
			}
			return vm, nil
		})

		w := e.worker(t, lifecycle.Options{Lease: 30 * time.Second})
		if !mustProcess(t, w) {
			t.Fatal("recover job was not claimed")
		}
		if evs := recoverEventsOfType(t, e, own, res.Sandbox.ID, "sandbox.recovered"); len(evs) != 0 {
			t.Fatalf("lost lease wrote %d sandbox.recovered events, want 0", len(evs))
		}
	})

	t.Run("lease expired while ResetVM is in flight", func(t *testing.T) {
		e := newLifecycleEnv(t)
		e.registerNode(t, "node-a", db.Resources{VCPUs: 4, MemoryMiB: 4096, DiskGiB: 20})
		n := recoverStartNode(t, e, "node-a")
		own := e.owner(t)
		res := e.createSandbox(t, own, "node-a", 3)
		_ = provisionReady(t, e, own, res)

		if _, err := e.store.AcceptRecover(t.Context(), own, res.Sandbox.ID, time.Now().UTC()); err != nil {
			t.Fatal(err)
		}
		job, err := e.store.LatestSandboxJob(t.Context(), own, res.Sandbox.ID, db.JobRecover)
		if err != nil {
			t.Fatal(err)
		}

		started := make(chan struct{})
		n.setResetHook(func(ctx context.Context, req *nodev1.ResetVMRequest) (*nodev1.VM, error) {
			close(started)
			<-ctx.Done()
			return nil, status.FromContextError(ctx.Err()).Err()
		})

		w := e.worker(t, lifecycle.Options{Lease: 300 * time.Millisecond})
		done := make(chan error, 1)
		go func() {
			_, err := w.ProcessNext(t.Context())
			done <- err
		}()
		select {
		case <-started:
		case <-time.After(5 * time.Second):
			t.Fatal("ResetVM did not start")
		}
		if _, err := e.store.Pool().Exec(t.Context(),
			`UPDATE jobs SET leased_until = now() - interval '1 second' WHERE id = $1`, job.ID); err != nil {
			t.Fatal(err)
		}
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("ProcessNext: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("worker did not abort after lease loss")
		}
		if evs := recoverEventsOfType(t, e, own, res.Sandbox.ID, "sandbox.recovered"); len(evs) != 0 {
			t.Fatalf("lost lease wrote %d sandbox.recovered events, want 0", len(evs))
		}
		if evs := recoverEventsOfType(t, e, own, res.Sandbox.ID, "sandbox.recover_failed"); len(evs) != 0 {
			t.Fatalf("lost lease wrote %d sandbox.recover_failed events, want 0", len(evs))
		}
	})
}
