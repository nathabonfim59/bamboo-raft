package cli

import (
	"context"
	"errors"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nathabonfim59/bamboo-raft/internal/api"
)

// runLeaseTestRuntime builds a private runtime with a manual ticker.
func runLeaseTestRuntime(ticks chan time.Time) runRuntime {
	return runRuntime{
		heartbeatTicker: func(time.Duration) (<-chan time.Time, func()) {
			return ticks, func() {}
		},
	}
}

// TestRunPatchPermanentClassification pins the retry policy: permanent 4xx and
// explicit retryable=false versus bounded transport/5xx retries.
func TestRunPatchPermanentClassification(t *testing.T) {
	cases := []struct {
		name          string
		err           error
		wantPermanent bool
	}{
		{name: "nil", err: nil},
		{name: "bad request", err: &APIError{Status: 400}, wantPermanent: true},
		{name: "forbidden", err: &APIError{Status: 403}, wantPermanent: true},
		{name: "not found", err: &APIError{Status: 404}, wantPermanent: true},
		{name: "conflict", err: &APIError{Status: 409}, wantPermanent: true},
		{name: "too many requests", err: &APIError{Status: 429}, wantPermanent: true},
		{name: "internal", err: &APIError{Status: 500}},
		{name: "bad gateway", err: &APIError{Status: 502}},
		{name: "unavailable", err: &APIError{Status: 503}},
		{name: "gateway timeout", err: &APIError{Status: 504}},
		{name: "other 5xx", err: &APIError{Status: 501}, wantPermanent: true},
		{name: "explicit false", err: &APIError{Status: 503, Retryable: new(false)}, wantPermanent: true},
		{name: "explicit true", err: &APIError{Status: 500, Retryable: new(true)}},
		{name: "transport", err: &requestError{method: http.MethodPatch, kind: errRequestFailed}},
		{name: "deadline", err: context.DeadlineExceeded},
		{name: "cancelled", err: context.Canceled},
		{name: "invalid response", err: &CLIError{Code: "invalid_response"}, wantPermanent: true},
		{name: "unknown", err: errors.New("boom"), wantPermanent: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := runPatchPermanent(tc.err); got != tc.wantPermanent {
				t.Fatalf("runPatchPermanent(%v) = %v, want %v", tc.err, got, tc.wantPermanent)
			}
		})
	}
}

// TestRunLeaseStopIsIdempotentAndNotLost proves a normal stop joins the
// goroutine and never reports a lost lease.
func TestRunLeaseStopIsIdempotentAndNotLost(t *testing.T) {
	fake := runTestNewAPI(t)
	runTestSetRolling(t, fake)
	env := cliTestNew(t, fake.serveHTTP, nil)
	client, err := NewClient(env.server.URL, "test-key", env.server.Client())
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	ticks := make(chan time.Time, 16)
	runtime := runLeaseTestRuntime(ticks)
	canceled := make(chan error, 1)
	_, cancel := context.WithCancelCause(t.Context())
	defer cancel(nil)
	lease, err := startRunLease(t.Context(), func(err error) {
		select {
		case canceled <- err:
		default:
		}
		cancel(err)
	}, client, cliTestSandboxID, int(runRollingTTLSeconds), runtime)
	if err != nil {
		t.Fatalf("startRunLease: %v", err)
	}
	defer lease.stop()
	if fake.patchRequestCount() != 1 {
		t.Fatalf("initial PATCH requests = %d, want 1", fake.patchRequestCount())
	}
	lease.stop()
	lease.stop()
	select {
	case got := <-canceled:
		t.Fatalf("normal stop reported a lost lease: %v", got)
	default:
	}
	select {
	case <-lease.done:
	default:
		t.Fatal("lease goroutine was not joined by stop")
	}
	_ = ticks
}

// TestRunLeaseInFlightStopIsClean proves stop cancels only its own in-flight
// renewal request and does not mark the work lease lost.
func TestRunLeaseInFlightStopIsClean(t *testing.T) {
	fake := runTestNewAPI(t)
	runTestSetRolling(t, fake)
	entered := make(chan struct{})
	release := newRunTestGate(t)
	var calls atomic.Int64
	fake.patchFunc = func(w http.ResponseWriter, r *http.Request, id string, _ api.UpdateSandboxRequest) bool {
		if calls.Add(1) == 1 {
			runTestWritePatchResponse(t, w, id, int(runRollingTTLSeconds), api.StateReady)
			return true
		}
		close(entered)
		if !release.Wait(r) {
			return true
		}
		runTestWritePatchResponse(t, w, id, int(runRollingTTLSeconds), api.StateReady)
		return true
	}
	env := cliTestNew(t, fake.serveHTTP, nil)
	client, err := NewClient(env.server.URL, "test-key", env.server.Client())
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	ticks := make(chan time.Time, 16)
	runtime := runLeaseTestRuntime(ticks)
	canceled := make(chan error, 1)
	lease, err := startRunLease(t.Context(), func(err error) {
		canceled <- err
	}, client, cliTestSandboxID, int(runRollingTTLSeconds), runtime)
	if err != nil {
		t.Fatalf("startRunLease: %v", err)
	}
	defer lease.stop()
	runTestSendTick(t, ticks)
	waitRunTestSignal(t, entered, "in-flight renewal")
	stopped := make(chan struct{})
	go func() {
		lease.stop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(2 * time.Second):
		t.Fatal("stop did not cancel the in-flight renewal")
	}
	release.Release()
	select {
	case got := <-canceled:
		t.Fatalf("stop during renewal reported a lost lease: %v", got)
	default:
	}
}

// TestRunLeaseTwoRetryableFailuresAbort proves the bounded two-strike policy
// cancels the work with the lease-lost cause.
func TestRunLeaseTwoRetryableFailuresAbort(t *testing.T) {
	fake := runTestNewAPI(t)
	runTestSetRolling(t, fake)
	var calls atomic.Int64
	fake.patchFunc = func(w http.ResponseWriter, _ *http.Request, id string, _ api.UpdateSandboxRequest) bool {
		if calls.Add(1) == 1 {
			runTestWritePatchResponse(t, w, id, int(runRollingTTLSeconds), api.StateReady)
			return true
		}
		cliTestWriteJSON(t, w, http.StatusServiceUnavailable,
			cliTestErrorEnvelope(http.StatusServiceUnavailable, "sandbox_direct_failed", "retry"))
		return true
	}
	env := cliTestNew(t, fake.serveHTTP, nil)
	client, err := NewClient(env.server.URL, "test-key", env.server.Client())
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	ticks := make(chan time.Time, 16)
	runtime := runLeaseTestRuntime(ticks)
	canceled := make(chan error, 1)
	_, cancel := context.WithCancelCause(t.Context())
	defer cancel(nil)
	lease, err := startRunLease(t.Context(), func(err error) {
		select {
		case canceled <- err:
		default:
		}
		cancel(err)
	}, client, cliTestSandboxID, int(runRollingTTLSeconds), runtime)
	if err != nil {
		t.Fatalf("startRunLease: %v", err)
	}
	defer lease.stop()
	runTestSendTick(t, ticks)
	runTestWaitPatchCount(t, fake, 2)
	runTestSendTick(t, ticks)
	runTestWaitPatchCount(t, fake, 3)
	select {
	case got := <-canceled:
		if !errors.Is(got, errRunLeaseLost) {
			t.Fatalf("cancel cause = %v, want errRunLeaseLost", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("two retryable failures did not abort the lease")
	}
}
