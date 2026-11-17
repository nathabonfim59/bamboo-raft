package cli

import (
	"context"
	"encoding/json/jsontext"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nathabonfim59/bamboo-raft/internal/api"
)

// runTestWriteInfo writes a metadata envelope. detached is any JSON value; nil
// omits the member (legacy streaming server).
func runTestWriteInfo(t *testing.T, w http.ResponseWriter, streamMax int, detached any) {
	t.Helper()
	commands := map[string]any{
		"streamMaxSeconds": streamMax,
		"syncMaxSeconds":   600,
	}
	if detached != nil {
		commands["detached"] = detached
	}
	cliTestWriteJSON(t, w, http.StatusOK, map[string]any{
		"ok":       true,
		"type":     execInfoType,
		"version":  "run-test",
		"commands": commands,
	})
}

// runTestWritePatchResponse writes a valid renewal answer for id.
func runTestWritePatchResponse(t *testing.T, w http.ResponseWriter, id string, ttl int, state api.SandboxState) {
	t.Helper()
	sandbox := cliTestSandbox(id, state)
	sandbox.Type = "default"
	sandbox.TTLSeconds = new(ttl)
	archive := time.Now().Add(time.Duration(ttl) * time.Second)
	sandbox.ArchiveAfter = &archive
	cliTestWriteJSON(t, w, http.StatusOK, api.NewSandboxInfoResponse(sandbox))
}

// runTestRuntime builds a private runtime with a manual ticker and a lease
// observer. Nothing is shared with production or another test.
func runTestRuntime(t *testing.T) (runtime runRuntime, ticks chan time.Time, leaseDone <-chan chan struct{}) {
	t.Helper()
	ticks = make(chan time.Time, 16)
	done := make(chan chan struct{}, 1)
	return runRuntime{
		heartbeatTicker: func(time.Duration) (<-chan time.Time, func()) {
			return ticks, func() {}
		},
		leaseObserver: func(lease *runLease) { done <- lease.done },
	}, ticks, done
}

// runTestNewEnvWithRuntime wires the fake API and replaces only the run
// command with one using the private runtime.
func runTestNewEnvWithRuntime(t *testing.T, fake *runTestAPI, mutate func(*Options), runtime runRuntime) *cliTestEnv {
	t.Helper()
	env := cliTestNew(t, fake.serveHTTP, mutate)
	for _, command := range env.app.root.Commands() {
		if command.Name() == "run" {
			env.app.root.RemoveCommand(command)
		}
	}
	env.app.root.AddCommand(newRunCommandWithRuntime(env.app, runtime))
	return env
}

// runTestSendTick feeds one controlled heartbeat tick.
func runTestSendTick(t *testing.T, ticks chan time.Time) {
	t.Helper()
	select {
	case ticks <- time.Now():
	case <-time.After(2 * time.Second):
		t.Fatal("lease did not consume a heartbeat tick")
	}
}

// runTestWaitPatchCount waits until the fake recorded want PATCH requests.
func runTestWaitPatchCount(t *testing.T, fake *runTestAPI, want int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for fake.patchRequestCount() < want {
		if time.Now().After(deadline) {
			t.Fatalf("PATCH requests = %d, want at least %d", fake.patchRequestCount(), want)
		}
		time.Sleep(time.Millisecond)
	}
}

// runTestAssertLeaseJoined proves the captured lease goroutine has exited.
func runTestAssertLeaseJoined(t *testing.T, done <-chan chan struct{}) {
	t.Helper()
	select {
	case leaseDone := <-done:
		select {
		case <-leaseDone:
		case <-time.After(2 * time.Second):
			t.Fatal("lease goroutine was not joined")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("rolling mode did not start a lease")
	}
}

// runTestGate is a bounded start/release gate. Cleanup releases it on every
// failure path so no handler goroutine can outlive its test.
type runTestGate struct {
	ch   chan struct{}
	once sync.Once
}

// newRunTestGate builds a gate released by the test when it finishes.
func newRunTestGate(t *testing.T) *runTestGate {
	t.Helper()
	gate := &runTestGate{ch: make(chan struct{})}
	t.Cleanup(gate.Release)
	return gate
}

// Wait blocks until the test releases the gate or the request is cancelled.
func (g *runTestGate) Wait(r *http.Request) bool {
	select {
	case <-g.ch:
		return true
	case <-r.Context().Done():
		return false
	}
}

// Release opens the gate exactly once.
func (g *runTestGate) Release() {
	g.once.Do(func() { close(g.ch) })
}

// runTestSetRolling makes the fake advertise detached commands.
func runTestSetRolling(t *testing.T, fake *runTestAPI) {
	t.Helper()
	fake.infoFunc = func(w http.ResponseWriter, _ *http.Request) {
		runTestWriteInfo(t, w, 7200, true)
	}
}

// TestRunRollingTTLHeartbeatsAcrossSetupAndCommand proves create uses 900 s,
// one PATCH is verified immediately and renewal continues through readiness and
// command execution.
func TestRunRollingTTLHeartbeatsAcrossSetupAndCommand(t *testing.T) {
	fake := runTestNewAPI(t)
	runTestSetRolling(t, fake)
	runtime, ticks, leaseDone := runTestRuntime(t)

	getEntered := make(chan struct{})
	readyGate := newRunTestGate(t)
	var getOnce sync.Once
	fake.getFunc = func(w http.ResponseWriter, r *http.Request, _ string) bool {
		getOnce.Do(func() { close(getEntered) })
		if !readyGate.Wait(r) {
			return true
		}
		sandbox := fake.sandbox(api.StateReady)
		cliTestWriteJSON(t, w, http.StatusOK, api.NewSandboxInfoResponse(sandbox))
		return true
	}
	commandEntered := make(chan struct{})
	commandGate := newRunTestGate(t)
	fake.commandFunc = func(w http.ResponseWriter, r *http.Request, _ api.CommandRequest) bool {
		close(commandEntered)
		if !commandGate.Wait(r) {
			return true
		}
		runTestWriteStream(t, w, runTestFrames("hello\n", "", new(0), false))
		return true
	}

	env := runTestNewEnvWithRuntime(t, fake, nil, runtime)
	done, finished := runTestStartCommand(t, env, t.Context(), "run", "--timeout", "30", "--", "true")

	waitRunTestSignal(t, getEntered, "ready")
	beforeReady := fake.patchRequestCount()
	runTestSendTick(t, ticks)
	runTestWaitPatchCount(t, fake, beforeReady+1)
	readyGate.Release()

	waitRunTestSignal(t, commandEntered, "command")
	beforeCommand := fake.patchRequestCount()
	runTestSendTick(t, ticks)
	runTestWaitPatchCount(t, fake, beforeCommand+1)
	commandGate.Release()

	if status := waitRunTestStatus(t, done); status != 0 {
		t.Fatalf("exit status = %d, want 0; stderr=%q", status, env.stderr.String())
	}
	<-finished

	creates := fake.createRequests()
	if len(creates) != 1 {
		t.Fatalf("create requests = %d, want 1", len(creates))
	}
	if fake.infoRequestCount() != 1 {
		t.Fatalf("metadata requests = %d, want 1", fake.infoRequestCount())
	}
	if len(fake.commandRequests()) != 1 {
		t.Fatalf("command POSTs = %d, want 1", len(fake.commandRequests()))
	}
	if fake.killCount() != 0 {
		t.Fatalf("detached stop calls = %d, want 0", fake.killCount())
	}
	if ttl, ok := creates[0].TTLSeconds.Seconds(); !ok || ttl != int(runRollingTTLSeconds) {
		t.Fatalf("create ttl = %d set=%v, want %d", ttl, ok, runRollingTTLSeconds)
	}
	patches := fake.patchRequests()
	if len(patches) < 3 {
		t.Fatalf("PATCH requests = %d, want the initial, readiness and command renewals", len(patches))
	}
	for i, patch := range patches {
		if got := string(patch.TTLSeconds); got != "900" {
			t.Fatalf("PATCH %d ttl = %q, want 900", i, got)
		}
	}
	if len(fake.deleteRequests()) != 1 {
		t.Fatal("sandbox was not deleted after a successful run")
	}
	if !strings.Contains(env.stderr.String(), " deleted") {
		t.Fatalf("stderr = %q, want a deleted summary", env.stderr.String())
	}
	runTestAssertLeaseJoined(t, leaseDone)
}

// TestRunRollingHeartbeatDuringPush proves the same lease renews while the
// upload/extraction phase blocks, not only during readiness and the command.
func TestRunRollingHeartbeatDuringPush(t *testing.T) {
	source := pushTestSource(t, map[string]string{"file.txt": "hello\n"})
	capture := new(pushTestCapture)
	upload := httptest.NewServer(pushTestTarHandler(t, capture, http.StatusNoContent))
	t.Cleanup(upload.Close)

	fake := runTestNewAPI(t)
	runTestSetRolling(t, fake)
	fake.uploadBase = upload.URL
	runtime, ticks, leaseDone := runTestRuntime(t)
	pushEntered := make(chan struct{})
	pushGate := newRunTestGate(t)
	fake.commandFunc = func(w http.ResponseWriter, r *http.Request, request api.CommandRequest) bool {
		if request.Stream != nil && *request.Stream {
			runTestWriteStream(t, w, runTestFrames("", "", new(0), false))
			return true
		}
		close(pushEntered)
		if !pushGate.Wait(r) {
			return true
		}
		code := 0
		cliTestWriteJSON(t, w, http.StatusOK, api.NewCommandResponse(true, &code, "", "", false))
		return true
	}
	env := runTestNewEnvWithRuntime(t, fake, nil, runtime)
	done, finished := runTestStartCommand(t, env, t.Context(),
		"run", "--push", source, "--timeout", "30", "--", "true")

	waitRunTestSignal(t, pushEntered, "push extraction")
	beforePush := fake.patchRequestCount()
	runTestSendTick(t, ticks)
	runTestWaitPatchCount(t, fake, beforePush+1)
	pushGate.Release()

	if status := waitRunTestStatus(t, done); status != 0 {
		t.Fatalf("exit status = %d, want 0; stderr=%q", status, env.stderr.String())
	}
	<-finished
	if capture.pushTestUploads() != 1 {
		t.Fatalf("uploads = %d, want 1", capture.pushTestUploads())
	}
	if fake.infoRequestCount() != 1 {
		t.Fatalf("metadata requests = %d, want 1", fake.infoRequestCount())
	}
	if fake.patchRequestCount() != 2 {
		t.Fatalf("PATCH requests = %d, want initial + push heartbeat", fake.patchRequestCount())
	}
	if len(fake.commandRequests()) != 2 {
		t.Fatalf("command POSTs = %d, want extraction + user command", len(fake.commandRequests()))
	}
	if len(fake.deleteRequests()) != 1 {
		t.Fatal("sandbox was not deleted")
	}
	runTestAssertLeaseJoined(t, leaseDone)
}

// TestRunRollingZeroStaysUnlimitedDetached proves timeout 0 is not replaced by
// the server stream cap and that no second metadata GET is made.
func TestRunRollingZeroStaysUnlimitedDetached(t *testing.T) {
	fake := runTestNewAPI(t)
	runTestSetRolling(t, fake)
	runtime, _, leaseDone := runTestRuntime(t)
	var sawDetached atomic.Bool
	fake.commandFunc = func(_ http.ResponseWriter, _ *http.Request, request api.CommandRequest) bool {
		if request.Detached == nil || !*request.Detached {
			t.Error("timeout 0 did not start a detached command")
		}
		if request.TimeoutSeconds != nil {
			t.Errorf("detached unlimited command carried timeoutSeconds = %v", *request.TimeoutSeconds)
		}
		sawDetached.Store(true)
		return false
	}
	fake.attachFunc = func(_ http.ResponseWriter, _ *http.Request) bool {
		return false
	}
	attachDeadline := make(chan bool, 1)
	mutate := func(opts *Options) {
		base := opts.HTTPClient.Transport
		opts.HTTPClient = &http.Client{Transport: execTestRoundTripper(func(req *http.Request) (*http.Response, error) {
			if strings.HasSuffix(req.URL.Path, "/stream") {
				_, hasDeadline := req.Context().Deadline()
				select {
				case attachDeadline <- hasDeadline:
				default:
				}
			}
			return base.RoundTrip(req)
		})}
	}
	env := runTestNewEnvWithRuntime(t, fake, mutate, runtime)
	if status := env.run(t, "run", "--timeout", "0", "--", "true"); status != 0 {
		t.Fatalf("exit status = %d, want 0; stderr=%q", status, env.stderr.String())
	}
	if !sawDetached.Load() {
		t.Fatal("detached command was not observed")
	}
	select {
	case hasDeadline := <-attachDeadline:
		if hasDeadline {
			t.Fatal("unlimited attach client request carries an overall deadline")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("attach request was not observed by the client transport")
	}
	if fake.infoRequestCount() != 1 {
		t.Fatalf("metadata requests = %d, want exactly one pre-create read", fake.infoRequestCount())
	}
	if len(fake.commandRequests()) != 1 {
		t.Fatalf("command POSTs = %d, want one detached start", len(fake.commandRequests()))
	}
	if fake.killCount() != 0 {
		t.Fatalf("detached stop calls = %d, want 0", fake.killCount())
	}
	creates := fake.createRequests()
	if len(creates) != 1 {
		t.Fatalf("create requests = %d", len(creates))
	}
	if ttl, ok := creates[0].TTLSeconds.Seconds(); !ok || ttl != int(runRollingTTLSeconds) {
		t.Fatalf("create ttl = %d set=%v, want %d", ttl, ok, runRollingTTLSeconds)
	}
	if fake.patchRequestCount() != 1 {
		t.Fatalf("PATCH requests = %d, want the initial verification only", fake.patchRequestCount())
	}
	if got := env.stdout.String(); got != "detached\n" {
		t.Fatalf("stdout = %q, want the attach output", got)
	}
	if len(fake.deleteRequests()) != 1 {
		t.Fatal("sandbox was not deleted")
	}
	runTestAssertLeaseJoined(t, leaseDone)
}

// TestRunLegacyServerFallback proves old servers keep the fixed TTL, no PATCH,
// the 600-second note only for timeout 0, and reject --keep before create.
func TestRunLegacyServerFallback(t *testing.T) {
	cases := []struct {
		name        string
		timeout     string
		info        func(*testing.T, http.ResponseWriter)
		wantTTL     int
		wantTimeout int
		wantNote    bool
	}{
		{
			name: "explicit timeout 404", timeout: "30", wantTTL: 3750, wantTimeout: 30,
		},
		{
			name: "zero timeout 404", timeout: "0", wantTTL: 4320, wantTimeout: 600, wantNote: true,
		},
		{
			name:    "zero timeout detached false",
			timeout: "0",
			info: func(t *testing.T, w http.ResponseWriter) {
				runTestWriteInfo(t, w, 7200, false)
			},
			wantTTL: 10920, wantTimeout: 7200,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fake := runTestNewAPI(t)
			if tc.info != nil {
				fake.infoFunc = func(w http.ResponseWriter, _ *http.Request) { tc.info(t, w) }
			}
			fake.commandFunc = func(w http.ResponseWriter, _ *http.Request, request api.CommandRequest) bool {
				if request.TimeoutSeconds == nil || *request.TimeoutSeconds != tc.wantTimeout {
					t.Errorf("command timeout = %v, want %d", request.TimeoutSeconds, tc.wantTimeout)
				}
				runTestWriteStream(t, w, runTestFrames("", "", new(0), false))
				return true
			}
			env := runTestNewEnv(t, fake, nil)
			if status := env.run(t, "run", "--timeout", tc.timeout, "--", "true"); status != 0 {
				t.Fatalf("exit status = %d, want 0; stderr=%q", status, env.stderr.String())
			}
			creates := fake.createRequests()
			if len(creates) != 1 {
				t.Fatalf("create requests = %d", len(creates))
			}
			if ttl, ok := creates[0].TTLSeconds.Seconds(); !ok || ttl != tc.wantTTL {
				t.Fatalf("create ttl = %d set=%v, want %d", ttl, ok, tc.wantTTL)
			}
			if fake.patchRequestCount() != 0 {
				t.Fatalf("legacy run sent %d PATCH requests", fake.patchRequestCount())
			}
			hasNote := strings.Contains(env.stderr.String(), "600-second stream timeout")
			if hasNote != tc.wantNote {
				t.Fatalf("fallback note = %v, want %v; stderr=%q", hasNote, tc.wantNote, env.stderr.String())
			}
		})
	}

	t.Run("keep is unsupported before create", func(t *testing.T) {
		fake := runTestNewAPI(t)
		env := runTestNewEnv(t, fake, nil)
		if status := env.run(t, "run", "--keep", "--timeout", "30", "--", "true"); status != 1 {
			t.Fatalf("exit status = %d, want 1", status)
		}
		if len(fake.createRequests()) != 0 {
			t.Fatal("sandbox created despite unsupported --keep")
		}
		if !strings.Contains(env.stderr.String(), "--keep needs a server") {
			t.Fatalf("stderr = %q, want the keep capability diagnostic", env.stderr.String())
		}
	})
}

// TestRunRollingInitialPatchFailure proves a permanent initial rejection stops
// before readiness or a command POST and still deletes the sandbox.
func TestRunRollingInitialPatchFailure(t *testing.T) {
	cases := []struct {
		name   string
		status int
		code   string
	}{
		{name: "patch unavailable", status: http.StatusNotFound, code: "not_found"},
		{name: "forbidden", status: http.StatusForbidden, code: "forbidden"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fake := runTestNewAPI(t)
			runTestSetRolling(t, fake)
			fake.patchFunc = func(w http.ResponseWriter, _ *http.Request, _ string, _ api.UpdateSandboxRequest) bool {
				cliTestWriteJSON(t, w, tc.status,
					cliTestErrorEnvelope(tc.status, tc.code, "renewal rejected"))
				return true
			}
			env := runTestNewEnv(t, fake, nil)
			if status := env.run(t, "run", "--timeout", "30", "--", "true"); status != 1 {
				t.Fatalf("exit status = %d, want 1", status)
			}
			if len(fake.commandRequests()) != 0 {
				t.Fatal("command posted after a failed initial renewal")
			}
			if len(fake.deleteRequests()) != 1 {
				t.Fatal("sandbox was not deleted after a failed initial renewal")
			}
			if !strings.Contains(env.stderr.String(), "renewal is unavailable") {
				t.Fatalf("stderr = %q, want the renewal diagnostic", env.stderr.String())
			}
		})
	}
}

// TestRunRollingRenewalRetryPolicy covers transient retry, the two-failure
// abort, permanent 4xx and explicit retryable=false.
func TestRunRollingRenewalRetryPolicy(t *testing.T) {
	t.Run("transient then success continues", func(t *testing.T) {
		fake := runTestNewAPI(t)
		runTestSetRolling(t, fake)
		runtime, ticks, leaseDone := runTestRuntime(t)
		var calls atomic.Int64
		fake.patchFunc = func(w http.ResponseWriter, _ *http.Request, id string, _ api.UpdateSandboxRequest) bool {
			if calls.Add(1) == 2 {
				cliTestWriteJSON(t, w, http.StatusInternalServerError,
					cliTestErrorEnvelope(http.StatusInternalServerError, "http_500", "retry"))
				return true
			}
			runTestWritePatchResponse(t, w, id, int(runRollingTTLSeconds), api.StateReady)
			return true
		}
		getEntered := make(chan struct{})
		readyGate := newRunTestGate(t)
		var getOnce sync.Once
		fake.getFunc = func(w http.ResponseWriter, r *http.Request, _ string) bool {
			getOnce.Do(func() { close(getEntered) })
			if !readyGate.Wait(r) {
				return true
			}
			runTestWritePatchResponse(t, w, cliTestSandboxID, int(runRollingTTLSeconds), api.StateReady)
			return true
		}
		env := runTestNewEnvWithRuntime(t, fake, nil, runtime)
		done, finished := runTestStartCommand(t, env, t.Context(), "run", "--timeout", "30", "--", "true")
		waitRunTestSignal(t, getEntered, "ready")
		runTestSendTick(t, ticks)
		runTestWaitPatchCount(t, fake, 2)
		runTestSendTick(t, ticks)
		runTestWaitPatchCount(t, fake, 3)
		readyGate.Release()
		if status := waitRunTestStatus(t, done); status != 0 {
			t.Fatalf("exit status = %d, want 0; stderr=%q", status, env.stderr.String())
		}
		<-finished
		runTestAssertLeaseJoined(t, leaseDone)
	})

	t.Run("two transient failures abort", func(t *testing.T) {
		fake := runTestNewAPI(t)
		runTestSetRolling(t, fake)
		runtime, ticks, leaseDone := runTestRuntime(t)
		var calls atomic.Int64
		fake.patchFunc = func(w http.ResponseWriter, _ *http.Request, id string, _ api.UpdateSandboxRequest) bool {
			if calls.Add(1) == 1 {
				runTestWritePatchResponse(t, w, id, int(runRollingTTLSeconds), api.StateReady)
				return true
			}
			cliTestWriteJSON(t, w, http.StatusBadGateway,
				cliTestErrorEnvelope(http.StatusBadGateway, "sandbox_direct_failed", "retry"))
			return true
		}
		getEntered := make(chan struct{})
		var getOnce sync.Once
		fake.getFunc = func(_ http.ResponseWriter, r *http.Request, _ string) bool {
			getOnce.Do(func() { close(getEntered) })
			<-r.Context().Done()
			return true
		}
		env := runTestNewEnvWithRuntime(t, fake, nil, runtime)
		done, finished := runTestStartCommand(t, env, t.Context(), "run", "--timeout", "30", "--", "true")
		waitRunTestSignal(t, getEntered, "ready")
		runTestSendTick(t, ticks)
		runTestWaitPatchCount(t, fake, 2)
		runTestSendTick(t, ticks)
		runTestWaitPatchCount(t, fake, 3)
		if status := waitRunTestStatus(t, done); status != 1 {
			t.Fatalf("exit status = %d, want 1; stderr=%q", status, env.stderr.String())
		}
		<-finished
		if len(fake.commandRequests()) != 0 {
			t.Fatal("command posted after the lease was lost")
		}
		if len(fake.deleteRequests()) != 1 {
			t.Fatal("sandbox was not deleted after the lease was lost")
		}
		if !strings.Contains(env.stderr.String(), "could not be renewed") {
			t.Fatalf("stderr = %q, want the renewal failure", env.stderr.String())
		}
		runTestAssertLeaseJoined(t, leaseDone)
	})

	permanent := []struct {
		name   string
		status int
		code   string
		detail map[string]any
	}{
		{name: "permanent 4xx", status: http.StatusConflict, code: "sandbox_not_ready"},
		{name: "retryable false", status: http.StatusServiceUnavailable, code: "out_of_capacity", detail: map[string]any{"retryable": false}},
	}
	for _, tc := range permanent {
		t.Run(tc.name+" aborts at once", func(t *testing.T) {
			fake := runTestNewAPI(t)
			runTestSetRolling(t, fake)
			runtime, ticks, leaseDone := runTestRuntime(t)
			var calls atomic.Int64
			fake.patchFunc = func(w http.ResponseWriter, _ *http.Request, id string, _ api.UpdateSandboxRequest) bool {
				if calls.Add(1) == 1 {
					runTestWritePatchResponse(t, w, id, int(runRollingTTLSeconds), api.StateReady)
					return true
				}
				if tc.detail != nil {
					cliTestWriteJSON(t, w, tc.status,
						cliTestErrorEnvelopeWithDetails(tc.status, tc.code, "rejected", tc.detail))
					return true
				}
				cliTestWriteJSON(t, w, tc.status, cliTestErrorEnvelope(tc.status, tc.code, "rejected"))
				return true
			}
			getEntered := make(chan struct{})
			var getOnce sync.Once
			fake.getFunc = func(_ http.ResponseWriter, r *http.Request, _ string) bool {
				getOnce.Do(func() { close(getEntered) })
				<-r.Context().Done()
				return true
			}
			env := runTestNewEnvWithRuntime(t, fake, nil, runtime)
			done, finished := runTestStartCommand(t, env, t.Context(), "run", "--timeout", "30", "--", "true")
			waitRunTestSignal(t, getEntered, "ready")
			runTestSendTick(t, ticks)
			runTestWaitPatchCount(t, fake, 2)
			if status := waitRunTestStatus(t, done); status != 1 {
				t.Fatalf("exit status = %d, want 1; stderr=%q", status, env.stderr.String())
			}
			<-finished
			if len(fake.commandRequests()) != 0 {
				t.Fatal("command posted after a permanent renewal rejection")
			}
			runTestAssertLeaseJoined(t, leaseDone)
		})
	}
}

// TestRunRollingRenewalResponseValidation rejects every invalid renewal answer
// before accepting it.
func TestRunRollingRenewalResponseValidation(t *testing.T) {
	archive := time.Now().Add(900 * time.Second)
	cases := []struct {
		name  string
		write func(*testing.T, http.ResponseWriter, string)
	}{
		{
			name: "wrong id",
			write: func(t *testing.T, w http.ResponseWriter, _ string) {
				runTestWritePatchResponse(t, w, cliTestSandboxID2, int(runRollingTTLSeconds), api.StateReady)
			},
		},
		{
			name: "nil ttl",
			write: func(t *testing.T, w http.ResponseWriter, id string) {
				sandbox := cliTestSandbox(id, api.StateReady)
				sandbox.Type = "default"
				sandbox.ArchiveAfter = &archive
				cliTestWriteJSON(t, w, http.StatusOK, api.NewSandboxInfoResponse(sandbox))
			},
		},
		{
			name: "wrong ttl",
			write: func(t *testing.T, w http.ResponseWriter, id string) {
				runTestWritePatchResponse(t, w, id, int(runRollingTTLSeconds)-1, api.StateReady)
			},
		},
		{
			name: "nil archiveAfter",
			write: func(t *testing.T, w http.ResponseWriter, id string) {
				sandbox := cliTestSandbox(id, api.StateReady)
				sandbox.Type = "default"
				sandbox.TTLSeconds = new(int(runRollingTTLSeconds))
				cliTestWriteJSON(t, w, http.StatusOK, api.NewSandboxInfoResponse(sandbox))
			},
		},
		{
			name: "archived state",
			write: func(t *testing.T, w http.ResponseWriter, id string) {
				runTestWritePatchResponse(t, w, id, int(runRollingTTLSeconds), api.StateArchived)
			},
		},
		{
			name: "malformed body",
			write: func(t *testing.T, w http.ResponseWriter, _ string) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
				_, _ = io.WriteString(w, "{")
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fake := runTestNewAPI(t)
			runTestSetRolling(t, fake)
			fake.patchFunc = func(w http.ResponseWriter, _ *http.Request, id string, _ api.UpdateSandboxRequest) bool {
				tc.write(t, w, id)
				return true
			}
			env := runTestNewEnv(t, fake, nil)
			if status := env.run(t, "run", "--timeout", "30", "--", "true"); status != 1 {
				t.Fatalf("exit status = %d, want 1; stderr=%q", status, env.stderr.String())
			}
			if len(fake.commandRequests()) != 0 {
				t.Fatal("command posted after an invalid renewal answer")
			}
			if len(fake.deleteRequests()) != 1 {
				t.Fatal("sandbox was not deleted")
			}
		})
	}
}

// TestRunKeepLeavesSandbox covers confirmed keeps for success, remote nonzero
// and timeout, and that the final renewal is the only post-command PATCH.
func TestRunKeepLeavesSandbox(t *testing.T) {
	cases := []struct {
		name     string
		frames   string
		wantCode int
	}{
		{name: "success", frames: runTestFrames("", "", new(0), false), wantCode: 0},
		{name: "remote seven", frames: runTestFrames("", "", new(7), false), wantCode: 7},
		{name: "timeout", frames: runTestFrames("", "", nil, true), wantCode: 124},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fake := runTestNewAPI(t)
			runTestSetRolling(t, fake)
			runtime, _, leaseDone := runTestRuntime(t)
			fake.commandFunc = func(w http.ResponseWriter, _ *http.Request, _ api.CommandRequest) bool {
				runTestWriteStream(t, w, tc.frames)
				return true
			}
			env := runTestNewEnvWithRuntime(t, fake, nil, runtime)
			if status := env.run(t, "run", "--keep", "--timeout", "30", "--", "true"); status != tc.wantCode {
				t.Fatalf("exit status = %d, want %d; stderr=%q", status, tc.wantCode, env.stderr.String())
			}
			if len(fake.deleteRequests()) != 0 {
				t.Fatal("sandbox deleted despite --keep")
			}
			if fake.infoRequestCount() != 1 {
				t.Fatalf("metadata requests = %d, want 1", fake.infoRequestCount())
			}
			if len(fake.commandRequests()) != 1 {
				t.Fatalf("command POSTs = %d, want 1", len(fake.commandRequests()))
			}
			if fake.killCount() != 0 {
				t.Fatalf("detached stop calls = %d, want 0", fake.killCount())
			}
			patches := fake.patchRequests()
			if len(patches) != 2 {
				t.Fatalf("PATCH requests = %d, want initial + final keep", len(patches))
			}
			if got := string(patches[len(patches)-1].TTLSeconds); got != "900" {
				t.Fatalf("final keep ttl = %q, want 900", got)
			}
			if !strings.Contains(env.stderr.String(), " kept") {
				t.Fatalf("stderr = %q, want a kept summary", env.stderr.String())
			}
			if strings.Contains(env.stderr.String(), "not deleted") {
				t.Fatalf("stderr = %q, want kept, not the failure wording", env.stderr.String())
			}
			runTestAssertLeaseJoined(t, leaseDone)
		})
	}
}

// TestRunKeepUnconfirmed covers a failed final renewal and unconfirmed command
// outcomes, which must never keep the sandbox.
func TestRunKeepUnconfirmed(t *testing.T) {
	t.Run("final renewal failure leaves sandbox and reports", func(t *testing.T) {
		fake := runTestNewAPI(t)
		runTestSetRolling(t, fake)
		runtime, _, leaseDone := runTestRuntime(t)
		var calls atomic.Int64
		fake.patchFunc = func(w http.ResponseWriter, _ *http.Request, id string, _ api.UpdateSandboxRequest) bool {
			if calls.Add(1) == 1 {
				runTestWritePatchResponse(t, w, id, int(runRollingTTLSeconds), api.StateReady)
				return true
			}
			cliTestWriteJSON(t, w, http.StatusInternalServerError,
				cliTestErrorEnvelope(http.StatusInternalServerError, "http_500", "failed"))
			return true
		}
		env := runTestNewEnvWithRuntime(t, fake, nil, runtime)
		if status := env.run(t, "run", "--keep", "--timeout", "30", "--", "true"); status != 1 {
			t.Fatalf("exit status = %d, want 1", status)
		}
		if len(fake.deleteRequests()) != 0 {
			t.Fatal("sandbox deleted after a requested keep")
		}
		if fake.patchRequestCount() != 3 {
			t.Fatalf("PATCH requests = %d, want initial + two bounded keep attempts", fake.patchRequestCount())
		}
		if !strings.Contains(env.stderr.String(), "could not be confirmed") {
			t.Fatalf("stderr = %q, want the unconfirmed renewal diagnostic", env.stderr.String())
		}
		if strings.Contains(env.stderr.String(), "900 seconds") {
			t.Fatalf("stderr promises remaining time: %q", env.stderr.String())
		}
		if !strings.Contains(env.stderr.String(), "not deleted") {
			t.Fatalf("stderr = %q, want the not-deleted summary", env.stderr.String())
		}
		runTestAssertLeaseJoined(t, leaseDone)
	})

	t.Run("output write failure deletes", func(t *testing.T) {
		fake := runTestNewAPI(t)
		runTestSetRolling(t, fake)
		runtime, _, leaseDone := runTestRuntime(t)
		fake.commandFunc = func(w http.ResponseWriter, _ *http.Request, _ api.CommandRequest) bool {
			runTestWriteStream(t, w, runTestFrames("lost\n", "", new(0), false))
			return true
		}
		env := runTestNewEnvWithRuntime(t, fake, func(opts *Options) { opts.Stdout = new(runTestFailWriter) }, runtime)
		if status := env.run(t, "run", "--keep", "--timeout", "30", "--", "true"); status != 1 {
			t.Fatalf("exit status = %d, want 1", status)
		}
		if len(fake.deleteRequests()) != 1 {
			t.Fatal("ambiguous completed result was kept")
		}
		if fake.patchRequestCount() != 1 {
			t.Fatalf("PATCH requests = %d, want only the initial verification", fake.patchRequestCount())
		}
		runTestAssertLeaseJoined(t, leaseDone)
	})

	t.Run("ambiguous command start deletes", func(t *testing.T) {
		fake := runTestNewAPI(t)
		runTestSetRolling(t, fake)
		runtime, _, leaseDone := runTestRuntime(t)
		fake.commandFunc = func(w http.ResponseWriter, _ *http.Request, _ api.CommandRequest) bool {
			cliTestWriteJSON(t, w, http.StatusInternalServerError,
				cliTestErrorEnvelope(http.StatusInternalServerError, "http_500", "boom"))
			return true
		}
		env := runTestNewEnvWithRuntime(t, fake, nil, runtime)
		if status := env.run(t, "run", "--keep", "--timeout", "30", "--", "true"); status != 1 {
			t.Fatalf("exit status = %d, want 1", status)
		}
		if len(fake.commandRequests()) != 1 {
			t.Fatalf("command POSTs = %d, want exactly one", len(fake.commandRequests()))
		}
		if len(fake.deleteRequests()) != 1 {
			t.Fatal("ambiguous command start was kept")
		}
		if fake.patchRequestCount() != 1 {
			t.Fatalf("PATCH requests = %d, want only the initial verification", fake.patchRequestCount())
		}
		runTestAssertLeaseJoined(t, leaseDone)
	})
}

// TestRunKeepCtrlCAlwaysDeletes proves cancellation during the command or
// during the final keep renewal deletes and exits 130.
func TestRunKeepCtrlCAlwaysDeletes(t *testing.T) {
	t.Run("during command", func(t *testing.T) {
		fake := runTestNewAPI(t)
		runTestSetRolling(t, fake)
		runtime, _, leaseDone := runTestRuntime(t)
		commandEntered := make(chan struct{})
		fake.commandFunc = func(_ http.ResponseWriter, r *http.Request, _ api.CommandRequest) bool {
			close(commandEntered)
			<-r.Context().Done()
			return true
		}
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		env := runTestNewEnvWithRuntime(t, fake, nil, runtime)
		done, finished := runTestStartCommand(t, env, ctx, "run", "--keep", "--timeout", "30", "--", "true")
		waitRunTestSignal(t, commandEntered, "command")
		cancel()
		if status := waitRunTestStatus(t, done); status != 130 {
			t.Fatalf("exit status = %d, want 130", status)
		}
		<-finished
		if len(fake.deleteRequests()) != 1 {
			t.Fatal("sandbox was not deleted after Ctrl-C")
		}
		if fake.patchRequestCount() != 1 {
			t.Fatalf("PATCH requests = %d, want no final keep after Ctrl-C", fake.patchRequestCount())
		}
		runTestAssertLeaseJoined(t, leaseDone)
	})

	t.Run("during final keep renewal", func(t *testing.T) {
		fake := runTestNewAPI(t)
		runTestSetRolling(t, fake)
		runtime, _, leaseDone := runTestRuntime(t)
		finalStarted := make(chan struct{})
		finalGate := newRunTestGate(t)
		var calls atomic.Int64
		fake.patchFunc = func(w http.ResponseWriter, r *http.Request, id string, _ api.UpdateSandboxRequest) bool {
			if calls.Add(1) == 1 {
				runTestWritePatchResponse(t, w, id, int(runRollingTTLSeconds), api.StateReady)
				return true
			}
			close(finalStarted)
			if !finalGate.Wait(r) {
				return true
			}
			runTestWritePatchResponse(t, w, id, int(runRollingTTLSeconds), api.StateReady)
			return true
		}
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		env := runTestNewEnvWithRuntime(t, fake, nil, runtime)
		done, finished := runTestStartCommand(t, env, ctx, "run", "--keep", "--timeout", "30", "--", "true")
		waitRunTestSignal(t, finalStarted, "final keep")
		cancel()
		finalGate.Release()
		if status := waitRunTestStatus(t, done); status != 130 {
			t.Fatalf("exit status = %d, want 130", status)
		}
		<-finished
		if len(fake.deleteRequests()) != 1 {
			t.Fatal("late Ctrl-C did not delete the kept sandbox")
		}
		if !strings.Contains(env.stderr.String(), " deleted") || strings.Contains(env.stderr.String(), " kept") {
			t.Fatalf("stderr = %q, want a deleted summary after a late Ctrl-C", env.stderr.String())
		}
		runTestAssertLeaseJoined(t, leaseDone)
	})
}

// TestRunLeaseLostDetachedKillsOnce proves a failed renewal during a detached
// command stops the known process exactly once, never re-POSTs it, and deletes
// the sandbox.
func TestRunLeaseLostDetachedKillsOnce(t *testing.T) {
	fake := runTestNewAPI(t)
	runTestSetRolling(t, fake)
	runtime, ticks, leaseDone := runTestRuntime(t)
	var calls atomic.Int64
	fake.patchFunc = func(w http.ResponseWriter, _ *http.Request, id string, _ api.UpdateSandboxRequest) bool {
		if calls.Add(1) == 1 {
			runTestWritePatchResponse(t, w, id, int(runRollingTTLSeconds), api.StateReady)
			return true
		}
		cliTestWriteJSON(t, w, http.StatusBadGateway,
			cliTestErrorEnvelope(http.StatusBadGateway, "sandbox_direct_failed", "retry"))
		return true
	}
	fake.commandFunc = func(_ http.ResponseWriter, _ *http.Request, request api.CommandRequest) bool {
		if request.Detached == nil || !*request.Detached {
			t.Error("long run did not use the detached path")
		}
		return false
	}
	attachEntered := make(chan struct{})
	fake.attachFunc = func(_ http.ResponseWriter, r *http.Request) bool {
		close(attachEntered)
		<-r.Context().Done()
		return true
	}
	env := runTestNewEnvWithRuntime(t, fake, nil, runtime)
	done, finished := runTestStartCommand(t, env, t.Context(), "run", "--timeout", "0", "--", "true")
	waitRunTestSignal(t, attachEntered, "attach")
	runTestSendTick(t, ticks)
	runTestWaitPatchCount(t, fake, 2)
	runTestSendTick(t, ticks)
	runTestWaitPatchCount(t, fake, 3)
	if status := waitRunTestStatus(t, done); status != 1 {
		t.Fatalf("exit status = %d, want 1; stderr=%q", status, env.stderr.String())
	}
	<-finished
	if len(fake.commandRequests()) != 1 {
		t.Fatalf("command POSTs = %d, want exactly one", len(fake.commandRequests()))
	}
	if fake.infoRequestCount() != 1 {
		t.Fatalf("metadata requests = %d, want 1", fake.infoRequestCount())
	}
	if fake.killCount() != 1 {
		t.Fatalf("detached stop calls = %d, want exactly one", fake.killCount())
	}
	if len(fake.deleteRequests()) != 1 {
		t.Fatal("sandbox was not deleted after the lost lease")
	}
	if strings.Contains(env.stderr.String(), "stopping it failed") {
		t.Fatalf("validated stopCommand was rejected: %q", env.stderr.String())
	}
	runTestAssertLeaseJoined(t, leaseDone)
}

// TestRunSetupFailureKeepsTimeoutAttribution proves a readiness or push
// deadline still maps to the setup timeout after the work context split.
func TestRunSetupFailureKeepsTimeoutAttribution(t *testing.T) {
	workCtx, cancelWork := context.WithCancelCause(t.Context())
	defer cancelWork(nil)
	setupCtx, cancelSetup := context.WithTimeoutCause(workCtx, time.Nanosecond, errRunSetupTimeout)
	<-setupCtx.Done()
	cancelSetup()
	code, primary := runSetupFailureOutcome(t.Context(), workCtx, setupCtx, context.Canceled)
	if code != 1 || primary == nil || !strings.Contains(primary.Error(), "sandbox setup timed out") {
		t.Fatalf("setup deadline outcome = %d, %v; want the setup timeout", code, primary)
	}

	leaseCtx, cancelLease := context.WithCancelCause(t.Context())
	cancelLease(errRunLeaseLost)
	code, primary = runSetupFailureOutcome(t.Context(), leaseCtx, t.Context(), errors.New("setup failed"))
	if code != 1 || primary == nil || !strings.Contains(primary.Error(), "could not be renewed") {
		t.Fatalf("lease outcome = %d, %v; want the renewal failure", code, primary)
	}
}

// TestRunRollingAmbiguousCreateNamesRollingTTL proves the ambiguity message
// reports the exact requested TTL and no PATCH is attempted.
func TestRunRollingAmbiguousCreateNamesRollingTTL(t *testing.T) {
	fake := runTestNewAPI(t)
	runTestSetRolling(t, fake)
	fake.createFunc = func(w http.ResponseWriter, _ *http.Request, _ api.CreateSandboxRequest) bool {
		w.Header().Set("Content-Type", "text/html")
		w.WriteHeader(http.StatusBadGateway)
		_, _ = io.WriteString(w, "<html>sentinel</html>")
		return true
	}
	env := runTestNewEnv(t, fake, nil)
	if status := env.run(t, "run", "--timeout", "30", "--", "true"); status != 1 {
		t.Fatalf("exit status = %d, want 1", status)
	}
	if fake.patchRequestCount() != 0 {
		t.Fatal("PATCH sent without a confirmed sandbox id")
	}
	if !strings.Contains(env.stderr.String(), "auto-stops after 900 seconds") {
		t.Fatalf("stderr = %q, want the rolling TTL in the ambiguity message", env.stderr.String())
	}
}

// TestRunPatchBodyIsExact pins the renewal wire body.
func TestRunPatchBodyIsExact(t *testing.T) {
	fake := runTestNewAPI(t)
	runTestSetRolling(t, fake)
	runtime, _, leaseDone := runTestRuntime(t)
	env := runTestNewEnvWithRuntime(t, fake, nil, runtime)
	if status := env.run(t, "run", "--timeout", "30", "--", "true"); status != 0 {
		t.Fatalf("exit status = %d, want 0", status)
	}
	patches := fake.patchRequests()
	if len(patches) == 0 {
		t.Fatal("no PATCH recorded")
	}
	if got := string(patches[0].TTLSeconds); got != "900" {
		t.Fatalf("first PATCH ttl = %q, want raw 900", got)
	}
	var _ jsontext.Value = patches[0].TTLSeconds
	runTestAssertLeaseJoined(t, leaseDone)
}
