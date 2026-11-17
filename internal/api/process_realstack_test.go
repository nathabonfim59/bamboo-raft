package api_test

import (
	"context"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/nathabonfim59/bamboo-raft/internal/api"
	"github.com/nathabonfim59/bamboo-raft/internal/db"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

// processRPCounter wraps the real gateway client so tests can count RPCs and
// inject one post-start transport failure.
type processRPCounter struct {
	base       api.NodeClientFactory
	startCalls atomic.Int64
	getCalls   atomic.Int64
	afterStart func(call int64, process *nodev1.Process) error
	mu         sync.Mutex
	starts     []*nodev1.Process
}

func (c *processRPCounter) factory(nodeID string) (nodev1.NodeClient, func() error, error) {
	client, closeClient, err := c.base(nodeID)
	if err != nil {
		return nil, nil, err
	}
	return &processRPCClient{NodeClient: client, counter: c}, closeClient, nil
}

type processRPCClient struct {
	nodev1.NodeClient
	counter *processRPCounter
}

func (c *processRPCClient) StartProcess(ctx context.Context, request *nodev1.StartProcessRequest, opts ...grpc.CallOption) (*nodev1.Process, error) {
	call := c.counter.startCalls.Add(1)
	process, err := c.NodeClient.StartProcess(ctx, request, opts...)
	if err != nil {
		return nil, err
	}
	c.counter.mu.Lock()
	c.counter.starts = append(c.counter.starts, process)
	c.counter.mu.Unlock()
	if c.counter.afterStart != nil {
		if hookErr := c.counter.afterStart(call, process); hookErr != nil {
			return nil, hookErr
		}
	}
	return process, nil
}

func (c *processRPCClient) GetProcess(ctx context.Context, request *nodev1.GetProcessRequest, opts ...grpc.CallOption) (*nodev1.ProcessOutput, error) {
	c.counter.getCalls.Add(1)
	return c.NodeClient.GetProcess(ctx, request, opts...)
}

func processRealStackStatusHandler(t *testing.T, env *commandTestEnv, store api.CommandStore, counter *processRPCounter) http.Handler {
	t.Helper()
	handler, err := api.NewCommandStatusHandler(api.CommandsOptions{
		Lookup:  commandTestWithReadyLookup(commandTestNodeID),
		Clients: counter.factory,
		Store:   store,
		Logger:  commandTestLogger(),
	})
	if err != nil {
		t.Fatalf("NewCommandStatusHandler: %v", err)
	}
	return handler
}

func processRealStackStartHandler(t *testing.T, env *commandTestEnv, store api.CommandStore, counter *processRPCounter) http.Handler {
	t.Helper()
	return env.handler(t, func(options *api.CommandsOptions) {
		options.Store = store
		options.Clients = counter.factory
	})
}

func processTestPollStatus(t *testing.T, handler http.Handler, query string, done func(api.CommandStatusResponse) bool) api.CommandStatusResponse {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		recorder := processTestServeStatus(t, handler, processTestOwner, commandTestSandboxID, "1", query)
		if recorder.Code != http.StatusOK {
			t.Fatalf("status = %d\nbody: %s", recorder.Code, recorder.Body.Bytes())
		}
		processTestValidate(t, "commandStatus", recorder)
		response := processTestDecodeStatus(t, recorder)
		if done(response) {
			return response
		}
		if time.Now().After(deadline) {
			t.Fatalf("condition not reached; last status = %+v", response)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func processRealStackCleanup(t *testing.T, env *commandTestEnv) {
	t.Helper()
	t.Cleanup(func() {
		_, _ = env.node.StopVM(context.Background(), &nodev1.StopVMRequest{SandboxId: commandTestSandboxID})
	})
}

func TestDetachedRealStackAmbiguousThenSameIDRecovery(t *testing.T) {
	env := commandTestStartEnv(t)
	processRealStackCleanup(t, env)
	store := newProcessTestStore()
	counter := &processRPCounter{base: env.factory()}
	counter.afterStart = func(call int64, _ *nodev1.Process) error {
		if call == 1 {
			return status.Error(codes.Unavailable, "response lost after the process started")
		}
		return nil
	}
	startHandler := processRealStackStartHandler(t, env, store, counter)
	statusHandler := processRealStackStatusHandler(t, env, store, counter)

	recorder := processTestServeStart(t, startHandler, processTestOwner, commandTestSandboxID,
		`{"command":"printf real-stack","detached":true}`)
	response := processTestDecodeStarted(t, recorder)
	processTestValidate(t, "command", recorder)
	if counter.startCalls.Load() != 2 {
		t.Fatalf("StartProcess calls = %d, want 2 same-id attempts", counter.startCalls.Load())
	}
	counter.mu.Lock()
	starts := append([]*nodev1.Process(nil), counter.starts...)
	counter.mu.Unlock()
	if len(starts) != 2 || starts[0].Pid != starts[1].Pid || starts[0].ProcessId != starts[1].ProcessId {
		t.Fatalf("the retry created a different process: %+v", starts)
	}
	if response.PID != starts[0].Pid || response.ProcessID != starts[0].ProcessId {
		t.Errorf("started response = %+v, first process = %+v", response, starts[0])
	}
	statusResponse := processTestPollStatus(t, statusHandler, "", func(response api.CommandStatusResponse) bool {
		return response.Status != "running"
	})
	if statusResponse.Stdout != "real-stack" {
		t.Errorf("stdout = %q, want real-stack", statusResponse.Stdout)
	}
}

func TestDetachedRealStackLostAfterStopStart(t *testing.T) {
	env := commandTestStartEnv(t)
	processRealStackCleanup(t, env)
	store := newProcessTestStore()
	counter := &processRPCounter{base: env.factory()}
	startHandler := processRealStackStartHandler(t, env, store, counter)
	statusHandler := processRealStackStatusHandler(t, env, store, counter)

	recorder := processTestServeStart(t, startHandler, processTestOwner, commandTestSandboxID,
		`{"command":"printf before; sleep 600","detached":true}`)
	processTestDecodeStarted(t, recorder)
	processTestValidate(t, "command", recorder)
	processTestPollStatus(t, statusHandler, "", func(response api.CommandStatusResponse) bool {
		return response.Running && response.Stdout == "before"
	})

	if _, err := env.node.StopVM(t.Context(), &nodev1.StopVMRequest{SandboxId: commandTestSandboxID}); err != nil {
		t.Fatalf("StopVM: %v", err)
	}
	if _, err := env.node.StartVM(t.Context(), &nodev1.StartVMRequest{SandboxId: commandTestSandboxID}); err != nil {
		t.Fatalf("StartVM: %v", err)
	}

	response := processTestPollStatus(t, statusHandler, "", func(response api.CommandStatusResponse) bool {
		return response.Status == "lost"
	})
	if response.Running || response.FinishedAt != nil {
		t.Errorf("lost status = %+v", response)
	}
	if response.Stdout != "before" {
		t.Errorf("lost stdout = %q, want the on-disk log", response.Stdout)
	}
	stored, _ := store.record(1)
	if stored.Status != db.CommandLost {
		t.Errorf("record status = %q, want lost", stored.Status)
	}
}

// TestCommandStatusConcurrentOutputGateway exercises the real
// nodefake->nodelink->gateway path while the guest writes. It records any
// window where the returned end is past the Process size captured just before
// the read, which is the frozen reference's known stat-then-read race.
func TestCommandStatusConcurrentOutputGateway(t *testing.T) {
	env := commandTestStartEnv(t)
	processRealStackCleanup(t, env)
	client, err := env.serve.Dial(commandTestNodeID)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer client.Close()

	const processID = 101
	command := "for i in $(seq 1 24); do head -c 1048576 /dev/zero | tr '\\0' a; sleep 0.02; done"
	if _, err := client.StartProcess(t.Context(), &nodev1.StartProcessRequest{
		SandboxId: commandTestSandboxID, ProcessId: processID, Command: command,
	}); err != nil {
		t.Fatalf("StartProcess: %v", err)
	}

	store := newProcessTestStore()
	record := processTestRecord(processID, db.CommandRunning)
	record.SandboxID = commandTestSandboxID
	record.LogPath = "/home/user/.ascii/processes/101.log"
	record.ErrLogPath = "/home/user/.ascii/processes/101.err.log"
	store.seed(record)
	counter := &processRPCounter{base: env.factory()}
	statusHandler := processRealStackStatusHandler(t, env, store, counter)

	var calls, staleSize, tailAhead, statusOK int
	const maxCalls = 100
	deadline := time.Now().Add(5 * time.Second)
	for calls < maxCalls && time.Now().Before(deadline) {
		output, err := client.GetProcess(t.Context(), &nodev1.GetProcessRequest{
			SandboxId: commandTestSandboxID, ProcessId: processID, LimitBytes: 1 << 20,
		})
		if err != nil {
			t.Fatalf("GetProcess: %v", err)
		}
		calls++
		if int64(len(output.Stdout)) > output.Process.StdoutSize-output.StdoutOffset {
			staleSize++
		}
		if output.StdoutOffset > output.Process.StdoutSize {
			tailAhead++
		}
		recorder := processTestServeStatus(t, statusHandler, processTestOwner, commandTestSandboxID, "101", "")
		if recorder.Code != http.StatusOK {
			t.Fatalf("status during growth = %d\nbody: %s", recorder.Code, recorder.Body.Bytes())
		}
		processTestValidate(t, "commandStatus", recorder)
		statusOK++
		if output.Process.State != nodev1.ProcessState_PROCESS_STATE_RUNNING {
			break
		}
	}
	t.Logf("concurrent output: calls=%d stale-size windows=%d tail-start-ahead=%d status-ok=%d",
		calls, staleSize, tailAhead, statusOK)
	if calls == 0 || statusOK == 0 {
		t.Fatal("no concurrent status calls succeeded")
	}
}
