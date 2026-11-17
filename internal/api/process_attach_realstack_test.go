package api_test

import (
	"bufio"
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

	"github.com/nathabonfim59/bamboo-raft/internal/api"
	"github.com/nathabonfim59/bamboo-raft/internal/auth"
	"github.com/nathabonfim59/bamboo-raft/internal/db"
	"github.com/nathabonfim59/bamboo-raft/internal/nodefake"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

// processAttachTrackClient counts AttachProcess/KillProcess calls and records
// when the attach RPC context ends, so a real-stack test can prove cancellation
// reached the receiver.
type processAttachTrackClient struct {
	nodev1.NodeClient
	attachCalls atomic.Int32
	killCalls   atomic.Int32
	mu          sync.Mutex
	streamDone  chan struct{}
}

func (c *processAttachTrackClient) AttachProcess(ctx context.Context, request *nodev1.AttachProcessRequest, options ...grpc.CallOption) (grpc.ServerStreamingClient[nodev1.ProcessEvent], error) {
	c.attachCalls.Add(1)
	done := make(chan struct{})
	c.mu.Lock()
	c.streamDone = done
	c.mu.Unlock()
	go func() {
		<-ctx.Done()
		close(done)
	}()
	return c.NodeClient.AttachProcess(ctx, request, options...)
}

func (c *processAttachTrackClient) KillProcess(ctx context.Context, request *nodev1.KillProcessRequest, options ...grpc.CallOption) (*nodev1.KillProcessResponse, error) {
	c.killCalls.Add(1)
	return c.NodeClient.KillProcess(ctx, request, options...)
}

func (c *processAttachTrackClient) waitStreamDone(t *testing.T) {
	t.Helper()
	c.mu.Lock()
	done := c.streamDone
	c.mu.Unlock()
	if done == nil {
		t.Fatal("AttachProcess was never called")
	}
	processAttachTestWaitClosed(t, "attach RPC cancellation", done)
}

func processAttachRealStackHandler(t *testing.T, store api.CommandStore, counting *processAttachTrackClient) http.Handler {
	t.Helper()
	return processAttachTestHandler(t, store, counting, func(opts *api.CommandAttachOptions) {
		opts.Lookup = commandTestWithReadyLookup(commandTestNodeID)
		opts.Clients = func(string) (nodev1.NodeClient, func() error, error) {
			return counting, func() error { return nil }, nil
		}
	})
}

func processAttachRealStackRecord(process *nodev1.Process, status db.CommandStatus) db.CommandRecord {
	record := processAttachTestRecord(process, status)
	record.SandboxID = commandTestSandboxID
	return record
}

func TestAttachRealStackStreamsOutputAndExit(t *testing.T) {
	env := commandTestStartEnv(t)
	rawClient, err := env.serve.Dial(commandTestNodeID)
	if err != nil {
		t.Fatalf("dial gateway: %v", err)
	}
	t.Cleanup(func() { _ = rawClient.Close() })
	counting := &processAttachTrackClient{NodeClient: rawClient}
	store := newProcessTestStore()
	handler := processAttachRealStackHandler(t, store, counting)

	process, err := env.node.StartProcess(t.Context(), &nodev1.StartProcessRequest{
		SandboxId: commandTestSandboxID,
		ProcessId: processAttachTestProcessID,
		Command:   `printf 'out'; printf 'err' >&2; printf '€'; exit 7`,
	})
	if err != nil {
		t.Fatalf("StartProcess: %v", err)
	}
	store.seed(processAttachRealStackRecord(process, db.CommandRunning))

	recorder := processAttachTestServe(t, handler, processTestOwner, commandTestSandboxID, "7", "")
	processAttachTestValidate(t, recorder.Code, recorder.Header(), recorder.Body.Bytes())
	frames := processAttachTestParseFrames(t, recorder.Body.Bytes())
	if len(frames) < 3 || frames[0].Type != api.CommandAttachAttached {
		t.Fatalf("frames = %+v", frames)
	}
	var stdout, stderr strings.Builder
	terminal := frames[len(frames)-1]
	for _, frame := range frames[1:] {
		switch frame.Type {
		case api.CommandAttachStdout:
			stdout.WriteString(frame.Data)
		case api.CommandAttachStderr:
			stderr.WriteString(frame.Data)
		}
	}
	if terminal.Type != api.CommandAttachExit {
		t.Fatalf("terminal = %+v, want exit", terminal)
	}
	if stdout.String() != "out€" || stderr.String() != "err" {
		t.Errorf("stdout/stderr = %q/%q", stdout.String(), stderr.String())
	}
	if terminal.ExitCode.Value == nil || *terminal.ExitCode.Value != 7 {
		t.Errorf("exit = %+v", terminal)
	}
	if got := counting.attachCalls.Load(); got != 1 {
		t.Errorf("AttachProcess calls = %d, want 1", got)
	}
	record, _ := store.record(processAttachTestProcessID)
	if record.Status != db.CommandExited || record.ExitCode == nil || *record.ExitCode != 7 {
		t.Errorf("stored record = %+v", record)
	}
}

func TestAttachRealStackAlreadyExited(t *testing.T) {
	env := commandTestStartEnv(t)
	rawClient, err := env.serve.Dial(commandTestNodeID)
	if err != nil {
		t.Fatalf("dial gateway: %v", err)
	}
	t.Cleanup(func() { _ = rawClient.Close() })
	counting := &processAttachTrackClient{NodeClient: rawClient}
	store := newProcessTestStore()
	handler := processAttachRealStackHandler(t, store, counting)

	if _, err := env.node.StartProcess(t.Context(), &nodev1.StartProcessRequest{
		SandboxId: commandTestSandboxID,
		ProcessId: processAttachTestProcessID,
		Command:   `printf 'done'`,
	}); err != nil {
		t.Fatalf("StartProcess: %v", err)
	}
	final := processAttachTestWaitFinal(t, env.node)
	store.seed(processAttachRealStackRecord(final, db.CommandExited))

	recorder := processAttachTestServe(t, handler, processTestOwner, commandTestSandboxID, "7", "")
	processAttachTestValidate(t, recorder.Code, recorder.Header(), recorder.Body.Bytes())
	frames := processAttachTestParseFrames(t, recorder.Body.Bytes())
	if len(frames) != 3 {
		t.Fatalf("frames = %+v", frames)
	}
	if frames[1].Data != "done" || frames[2].Type != api.CommandAttachExit {
		t.Errorf("output/exit = %+v/%+v", frames[1], frames[2])
	}
	if frames[2].ExitCode.Value == nil || *frames[2].ExitCode.Value != 0 {
		t.Errorf("exit = %+v", frames[2])
	}
	if got := counting.attachCalls.Load(); got != 1 {
		t.Errorf("AttachProcess calls = %d, want 1", got)
	}
}

// processAttachTestWaitFinal polls the real nodefake until the process leaves
// RUNNING. This is a process-completion barrier, not a timer assertion.
func processAttachTestWaitFinal(t *testing.T, node *nodefake.Node) *nodev1.Process {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		output, err := node.GetProcess(t.Context(), &nodev1.GetProcessRequest{
			SandboxId: commandTestSandboxID, ProcessId: processAttachTestProcessID,
		})
		if err == nil && output.Process.State != nodev1.ProcessState_PROCESS_STATE_RUNNING {
			return output.Process
		}
		if time.Now().After(deadline) {
			t.Fatalf("process did not end: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestAttachRealStackCancellationKeepsProcessRunning(t *testing.T) {
	env := commandTestStartEnv(t)
	rawClient, err := env.serve.Dial(commandTestNodeID)
	if err != nil {
		t.Fatalf("dial gateway: %v", err)
	}
	t.Cleanup(func() { _ = rawClient.Close() })
	counting := &processAttachTrackClient{NodeClient: rawClient}
	store := newProcessTestStore()
	handler := processAttachRealStackHandler(t, store, counting)

	process, err := env.node.StartProcess(t.Context(), &nodev1.StartProcessRequest{
		SandboxId: commandTestSandboxID,
		ProcessId: processAttachTestProcessID,
		Command:   "sleep 30",
	})
	if err != nil {
		t.Fatalf("StartProcess: %v", err)
	}
	store.seed(processAttachRealStackRecord(process, db.CommandRunning))
	t.Cleanup(func() {
		_, _ = env.node.KillProcess(context.WithoutCancel(t.Context()), &nodev1.KillProcessRequest{
			SandboxId: commandTestSandboxID, ProcessId: processAttachTestProcessID,
		})
	})

	mux := http.NewServeMux()
	mux.Handle("GET "+api.CommandAttachPath, handler)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mux.ServeHTTP(w, r.WithContext(auth.WithIdentity(r.Context(), auth.Identity{Owner: processTestOwner})))
	}))
	t.Cleanup(server.Close)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet,
		server.URL+"/api/v1/sandboxes/"+commandTestSandboxID+"/commands/7/stream", nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("open attach: %v", err)
	}
	t.Cleanup(func() { _ = response.Body.Close() })
	reader := bufio.NewReader(response.Body)
	line, err := reader.ReadString('\n')
	if err != nil {
		response.Body.Close()
		t.Fatalf("read attached: %v", err)
	}
	var attached api.CommandAttachFrame
	if err := json.Unmarshal([]byte(line), &attached); err != nil || attached.Type != api.CommandAttachAttached {
		response.Body.Close()
		t.Fatalf("first frame = %q, %v", line, err)
	}
	cancel()
	response.Body.Close()
	counting.waitStreamDone(t)

	output, err := env.node.GetProcess(t.Context(), &nodev1.GetProcessRequest{
		SandboxId: commandTestSandboxID, ProcessId: processAttachTestProcessID,
	})
	if err != nil {
		t.Fatalf("GetProcess after cancellation: %v", err)
	}
	if output.Process.State != nodev1.ProcessState_PROCESS_STATE_RUNNING {
		t.Errorf("cancelling the HTTP attach changed the process: %+v", output.Process)
	}
}
