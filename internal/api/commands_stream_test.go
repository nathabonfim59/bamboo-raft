package api_test

import (
	"bytes"
	"context"
	"encoding/json/v2"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/nathabonfim59/bamboo-raft/internal/api"
	"github.com/nathabonfim59/bamboo-raft/internal/auth"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

const commandTestStreamBody = `{"command":"true","stream":true}`

func TestCommandStreamFrames(t *testing.T) {
	client, request, calls := commandTestStaticClient([]*nodev1.ExecEvent{
		commandTestStarted(),
		commandTestStdout("a"),
		commandTestStderr("b"),
		commandTestStdout("c"),
		commandTestExit(new(int32(0)), "", false, false),
	}, nil)
	handler := commandTestHandler(t, commandTestWithReadyLookup(commandTestNodeID), commandTestFactory(client))
	recorder := commandTestServe(t, handler, commandTestOwner, commandTestSandboxID, commandTestStreamBody)
	frames := commandTestFrames(t, recorder)

	if *calls != 1 {
		t.Fatalf("Exec calls = %d, want 1", *calls)
	}
	if (*request).GetTimeoutSeconds() != 30 {
		t.Errorf("timeout = %d, want default 30", (*request).GetTimeoutSeconds())
	}
	wantTypes := []api.CommandFrameType{
		api.CommandFrameStarted, api.CommandFrameStdout, api.CommandFrameStderr, api.CommandFrameStdout, api.CommandFrameExit,
	}
	if len(frames) != len(wantTypes) {
		t.Fatalf("frames = %v, want %v", frames, wantTypes)
	}
	for i, want := range wantTypes {
		if frames[i].Type != want {
			t.Errorf("frame %d = %q, want %q", i, frames[i].Type, want)
		}
	}
	if frames[1].Data != "a" || frames[2].Data != "b" || frames[3].Data != "c" {
		t.Errorf("output order = %q/%q/%q", frames[1].Data, frames[2].Data, frames[3].Data)
	}
	last := frames[len(frames)-1]
	if !last.ExitCode.Present || last.ExitCode.Value == nil || *last.ExitCode.Value != 0 {
		t.Errorf("exitCode = %+v", last.ExitCode)
	}
	if !last.Success.Present || last.Success.Value == nil || !*last.Success.Value {
		t.Errorf("success = %+v", last.Success)
	}
	if !last.TimedOut.Present || last.TimedOut.Value == nil || *last.TimedOut.Value {
		t.Errorf("timedOut = %+v", last.TimedOut)
	}
	commandTestValidateNDJSON(t, recorder)
}

func TestCommandStreamUTF8Split(t *testing.T) {
	euro := []byte("€")
	client, _, _ := commandTestStaticClient([]*nodev1.ExecEvent{
		commandTestStarted(),
		commandTestStdout("a"),
		commandTestStdoutBytes(euro[:2]),
		commandTestStdoutBytes(euro[2:]),
		commandTestStdout("b"),
		commandTestExit(new(int32(0)), "", false, false),
	}, nil)
	handler := commandTestHandler(t, commandTestWithReadyLookup(commandTestNodeID), commandTestFactory(client))
	recorder := commandTestServe(t, handler, commandTestOwner, commandTestSandboxID, commandTestStreamBody)
	frames := commandTestFrames(t, recorder)

	var output strings.Builder
	for _, frame := range frames {
		switch frame.Type {
		case api.CommandFrameStdout:
			if !validUTF8(frame.Data) {
				t.Errorf("stdout frame is not UTF-8: %q", frame.Data)
			}
			output.WriteString(frame.Data)
		}
	}
	if output.String() != "a€b" {
		t.Errorf("output = %q, want %q", output.String(), "a€b")
	}
	if strings.Contains(output.String(), "\uFFFD") {
		t.Errorf("valid split rune was replaced: %q", output.String())
	}
	commandTestValidateNDJSON(t, recorder)
}

func TestCommandStreamFinalTailReceiveOrder(t *testing.T) {
	euro := []byte("€")
	client, _, _ := commandTestStaticClient([]*nodev1.ExecEvent{
		commandTestStarted(),
		commandTestStderrBytes(euro[:2]), // pending first
		commandTestStdoutBytes(euro[:2]), // pending second
		commandTestExit(new(int32(0)), "", false, false),
	}, nil)
	handler := commandTestHandler(t, commandTestWithReadyLookup(commandTestNodeID), commandTestFactory(client))
	recorder := commandTestServe(t, handler, commandTestOwner, commandTestSandboxID, commandTestStreamBody)
	frames := commandTestFrames(t, recorder)
	var tailKinds []api.CommandFrameType
	for _, frame := range frames {
		if frame.Type == api.CommandFrameStderr || frame.Type == api.CommandFrameStdout {
			tailKinds = append(tailKinds, frame.Type)
			if frame.Data != "\uFFFD" {
				t.Errorf("tail %q = %q, want replacement", frame.Type, frame.Data)
			}
		}
	}
	want := []api.CommandFrameType{api.CommandFrameStderr, api.CommandFrameStdout}
	if len(tailKinds) != len(want) || tailKinds[0] != want[0] || tailKinds[1] != want[1] {
		t.Fatalf("tail order = %v, want %v", tailKinds, want)
	}
	commandTestValidateNDJSON(t, recorder)
}

func TestCommandStreamInterleavedSplitRune(t *testing.T) {
	euro := []byte("€")
	client, _, _ := commandTestStaticClient([]*nodev1.ExecEvent{
		commandTestStarted(),
		commandTestStdoutBytes(euro[:1]),
		commandTestStderr("x"),
		commandTestStdoutBytes(euro[1:]),
		commandTestStderr("y"),
		commandTestExit(new(int32(0)), "", false, false),
	}, nil)
	handler := commandTestHandler(t, commandTestWithReadyLookup(commandTestNodeID), commandTestFactory(client))
	recorder := commandTestServe(t, handler, commandTestOwner, commandTestSandboxID, commandTestStreamBody)
	frames := commandTestFrames(t, recorder)
	var got []string
	for _, frame := range frames {
		switch frame.Type {
		case api.CommandFrameStdout:
			got = append(got, "out:"+frame.Data)
		case api.CommandFrameStderr:
			got = append(got, "err:"+frame.Data)
		}
	}
	want := []string{"err:x", "out:€", "err:y"}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] || got[2] != want[2] {
		t.Fatalf("output frames = %v, want %v", got, want)
	}
	commandTestValidateNDJSON(t, recorder)
}

func TestCommandStreamInvalidUTF8(t *testing.T) {
	client, _, _ := commandTestStaticClient([]*nodev1.ExecEvent{
		commandTestStarted(),
		commandTestStdoutBytes([]byte{0xff}),
		commandTestExit(new(int32(0)), "", false, false),
	}, nil)
	handler := commandTestHandler(t, commandTestWithReadyLookup(commandTestNodeID), commandTestFactory(client))
	recorder := commandTestServe(t, handler, commandTestOwner, commandTestSandboxID, commandTestStreamBody)
	frames := commandTestFrames(t, recorder)
	output := ""
	for _, frame := range frames {
		if frame.Type == api.CommandFrameStdout {
			if !validUTF8(frame.Data) {
				t.Errorf("stdout frame is not UTF-8: %q", frame.Data)
			}
			output += frame.Data
		}
	}
	if output != "\uFFFD" {
		t.Errorf("output = %q, want replacement rune", output)
	}
}

func TestCommandStreamExitVariants(t *testing.T) {
	tests := []struct {
		name     string
		exit     *nodev1.ExecEvent
		success  bool
		timedOut bool
		code     *int
	}{
		{"success", commandTestExit(new(int32(0)), "", false, false), true, false, new(0)},
		{"nonzero", commandTestExit(new(int32(9)), "", false, false), false, false, new(9)},
		{"signal", commandTestExit(nil, "SIGTERM", false, false), false, false, nil},
		{"timeout", commandTestExit(nil, "", true, false), false, true, nil},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client, _, _ := commandTestStaticClient([]*nodev1.ExecEvent{commandTestStarted(), test.exit}, nil)
			handler := commandTestHandler(t, commandTestWithReadyLookup(commandTestNodeID), commandTestFactory(client))
			recorder := commandTestServe(t, handler, commandTestOwner, commandTestSandboxID, commandTestStreamBody)
			frames := commandTestFrames(t, recorder)
			last := frames[len(frames)-1]
			if last.Type != api.CommandFrameExit {
				t.Fatalf("terminal = %q, want exit", last.Type)
			}
			if !last.ExitCode.Present {
				t.Errorf("exitCode member is omitted")
			}
			switch {
			case test.code == nil && last.ExitCode.Value != nil:
				t.Errorf("exitCode = %v, want null", *last.ExitCode.Value)
			case test.code != nil && (last.ExitCode.Value == nil || *last.ExitCode.Value != *test.code):
				t.Errorf("exitCode = %+v, want %d", last.ExitCode, *test.code)
			}
			if !last.Success.Present || last.Success.Value == nil || *last.Success.Value != test.success {
				t.Errorf("success = %+v, want %v", last.Success, test.success)
			}
			if !last.TimedOut.Present || last.TimedOut.Value == nil || *last.TimedOut.Value != test.timedOut {
				t.Errorf("timedOut = %+v, want %v", last.TimedOut, test.timedOut)
			}
			commandTestValidateNDJSON(t, recorder)
		})
	}
}

func TestCommandStreamProtocolBeforeStarted(t *testing.T) {
	tests := []struct {
		name   string
		events []*nodev1.ExecEvent
		err    error
		status int
		code   string
	}{
		{"output first", []*nodev1.ExecEvent{commandTestStdout("x")}, nil, http.StatusBadGateway, "sandbox_direct_failed"},
		{"exit first", []*nodev1.ExecEvent{commandTestExit(new(int32(0)), "", false, false)}, nil, http.StatusBadGateway, "sandbox_direct_failed"},
		{"empty first", []*nodev1.ExecEvent{{}}, nil, http.StatusBadGateway, "sandbox_direct_failed"},
		{"early EOF", nil, nil, http.StatusBadGateway, "sandbox_direct_failed"},
		{"exec error", nil, status.Error(codes.NotFound, "gone"), http.StatusNotFound, "not_found"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client, _, _ := commandTestStaticClient(test.events, test.err)
			handler := commandTestHandler(t, commandTestWithReadyLookup(commandTestNodeID), commandTestFactory(client))
			recorder := commandTestServe(t, handler, commandTestOwner, commandTestSandboxID, commandTestStreamBody)
			if recorder.Code != test.status {
				t.Fatalf("status = %d, want %d\nbody: %s", recorder.Code, test.status, recorder.Body.Bytes())
			}
			envelope := commandTestDecodeError(t, recorder)
			if envelope.Code != test.code {
				t.Errorf("code = %q, want %q", envelope.Code, test.code)
			}
			commandTestValidateJSON(t, recorder)
		})
	}
}

func TestCommandStreamProtocolAfterStarted(t *testing.T) {
	tests := []struct {
		name   string
		events []*nodev1.ExecEvent
		err    error
		code   string
	}{
		{"early EOF", []*nodev1.ExecEvent{commandTestStarted()}, nil, "sandbox_direct_failed"},
		{"duplicate started", []*nodev1.ExecEvent{commandTestStarted(), commandTestStarted(), commandTestExit(new(int32(0)), "", false, false)}, nil, "sandbox_direct_failed"},
		{"empty event", []*nodev1.ExecEvent{commandTestStarted(), {}}, nil, "sandbox_direct_failed"},
		{"node error", []*nodev1.ExecEvent{commandTestStarted()}, status.Error(codes.Internal, "hidden node text"), "sandbox_direct_failed"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var streamCtx context.Context
			client := &commandTestClient{exec: func(ctx context.Context, _ *nodev1.ExecRequest) (grpc.ServerStreamingClient[nodev1.ExecEvent], error) {
				streamCtx = ctx
				return &commandTestStream{ctx: ctx, events: test.events, finalErr: test.err}, nil
			}}
			handler := commandTestHandler(t, commandTestWithReadyLookup(commandTestNodeID), commandTestFactory(client))
			recorder := commandTestServe(t, handler, commandTestOwner, commandTestSandboxID, commandTestStreamBody)
			frames := commandTestFrames(t, recorder)
			if frames[0].Type != api.CommandFrameStarted {
				t.Fatalf("first frame = %q", frames[0].Type)
			}
			terminals := 0
			for _, frame := range frames[1:] {
				if frame.Type != api.CommandFrameExit && frame.Type != api.CommandFrameError {
					continue
				}
				terminals++
				if frame.Type != api.CommandFrameError {
					t.Errorf("terminal = %q, want error", frame.Type)
					continue
				}
				if frame.Error != test.code {
					t.Errorf("error = %q, want %q", frame.Error, test.code)
				}
				if !frame.Retryable.Present || frame.Retryable.Value == nil || *frame.Retryable.Value {
					t.Errorf("retryable = %+v, want false", frame.Retryable)
				}
				if strings.Contains(frame.Message, "hidden node text") {
					t.Errorf("node error text leaked into the stream: %q", frame.Message)
				}
			}
			if terminals != 1 {
				t.Errorf("terminal frames = %d, want exactly 1", terminals)
			}
			if test.name == "duplicate started" && streamCtx != nil && streamCtx.Err() == nil {
				t.Errorf("duplicate started did not cancel the remote RPC")
			}
			commandTestValidateNDJSON(t, recorder)
		})
	}
}

func TestCommandStreamWatchdog(t *testing.T) {
	t.Run("hung node ends with timed-out exit", func(t *testing.T) {
		client := commandTestBlockingClient(commandTestStarted(), commandTestStdout("before"))
		handler := commandTestHandler(t, commandTestWithReadyLookup(commandTestNodeID), commandTestFactory(client),
			func(options *api.CommandsOptions) {
				options.ExecutionBudget = func(time.Duration) time.Duration { return 20 * time.Millisecond }
			})
		start := time.Now()
		recorder := commandTestServe(t, handler, commandTestOwner, commandTestSandboxID,
			`{"command":"true","stream":true,"timeoutSeconds":1}`)
		if elapsed := time.Since(start); elapsed > time.Second {
			t.Fatalf("watchdog took %v", elapsed)
		}
		frames := commandTestFrames(t, recorder)
		terminals := 0
		for _, frame := range frames {
			if frame.Type == api.CommandFrameExit || frame.Type == api.CommandFrameError {
				terminals++
				if frame.Type != api.CommandFrameExit {
					t.Errorf("terminal = %q, want exit", frame.Type)
					continue
				}
				if !frame.TimedOut.Present || frame.TimedOut.Value == nil || !*frame.TimedOut.Value {
					t.Errorf("timedOut = %+v", frame.TimedOut)
				}
				if !frame.ExitCode.Present || frame.ExitCode.Value != nil {
					t.Errorf("exitCode = %+v, want explicit null", frame.ExitCode)
				}
			}
		}
		if terminals != 1 {
			t.Errorf("terminal frames = %d, want exactly 1", terminals)
		}
		commandTestValidateNDJSON(t, recorder)
	})

	t.Run("normal node timeout wins", func(t *testing.T) {
		client, _, _ := commandTestStaticClient([]*nodev1.ExecEvent{
			commandTestStarted(), commandTestExit(nil, "", true, false),
		}, nil)
		handler := commandTestHandler(t, commandTestWithReadyLookup(commandTestNodeID), commandTestFactory(client),
			func(options *api.CommandsOptions) {
				options.ExecutionBudget = func(time.Duration) time.Duration { return 5 * time.Second }
			})
		start := time.Now()
		recorder := commandTestServe(t, handler, commandTestOwner, commandTestSandboxID,
			`{"command":"true","stream":true,"timeoutSeconds":1}`)
		if elapsed := time.Since(start); elapsed > time.Second {
			t.Fatalf("normal timeout exit did not win: %v", elapsed)
		}
		frames := commandTestFrames(t, recorder)
		last := frames[len(frames)-1]
		if last.Type != api.CommandFrameExit || !last.TimedOut.Present || last.TimedOut.Value == nil || !*last.TimedOut.Value {
			t.Errorf("terminal = %+v", last)
		}
	})

	t.Run("hung before started is an HTTP error", func(t *testing.T) {
		client := commandTestBlockingClient()
		handler := commandTestHandler(t, commandTestWithReadyLookup(commandTestNodeID), commandTestFactory(client),
			func(options *api.CommandsOptions) {
				options.ExecutionBudget = func(time.Duration) time.Duration { return 20 * time.Millisecond }
			})
		recorder := commandTestServe(t, handler, commandTestOwner, commandTestSandboxID,
			`{"command":"true","stream":true,"timeoutSeconds":1}`)
		envelope := commandTestDecodeError(t, recorder)
		if recorder.Code != http.StatusBadGateway || envelope.Code != "sandbox_direct_failed" {
			t.Fatalf("status/code = %d/%q", recorder.Code, envelope.Code)
		}
		commandTestValidateJSON(t, recorder)
	})
}

type commandTestFlushErrorWriter struct {
	header http.Header
	body   bytes.Buffer
	err    error
}

func (w *commandTestFlushErrorWriter) Header() http.Header { return w.header }
func (w *commandTestFlushErrorWriter) WriteHeader(int)     {}
func (w *commandTestFlushErrorWriter) Write(p []byte) (int, error) {
	return w.body.Write(p)
}
func (w *commandTestFlushErrorWriter) FlushError() error { return w.err }

type commandTestWriteErrorWriter struct {
	header http.Header
}

func (w *commandTestWriteErrorWriter) Header() http.Header { return w.header }
func (w *commandTestWriteErrorWriter) WriteHeader(int)     {}
func (w *commandTestWriteErrorWriter) Write([]byte) (int, error) {
	return 0, errors.New("write failed")
}

func TestCommandStreamWriterFailuresCancelRemote(t *testing.T) {
	tests := []struct {
		name   string
		writer http.ResponseWriter
	}{
		{"flush error", &commandTestFlushErrorWriter{header: http.Header{}, err: errors.New("flush failed")}},
		{"write error", &commandTestWriteErrorWriter{header: http.Header{}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var streamCtx context.Context
			client := &commandTestClient{exec: func(ctx context.Context, _ *nodev1.ExecRequest) (grpc.ServerStreamingClient[nodev1.ExecEvent], error) {
				streamCtx = ctx
				return &commandTestStream{ctx: ctx, events: []*nodev1.ExecEvent{commandTestStarted()}, blockAfter: true}, nil
			}}
			handler := commandTestHandler(t, commandTestWithReadyLookup(commandTestNodeID), commandTestFactory(client))
			mux := http.NewServeMux()
			mux.Handle("POST "+api.CommandsPath, handler)
			request := httptest.NewRequest(http.MethodPost,
				"/api/v1/sandboxes/"+commandTestSandboxID+"/commands", strings.NewReader(commandTestStreamBody))
			request.Header.Set("Content-Type", "application/json")
			request = request.WithContext(auth.WithIdentity(request.Context(), auth.Identity{Owner: commandTestOwner}))
			mux.ServeHTTP(test.writer, request)
			if streamCtx == nil {
				t.Fatal("node Exec never started")
			}
			deadline := time.Now().Add(time.Second)
			for streamCtx.Err() == nil {
				if time.Now().After(deadline) {
					t.Fatal("writer failure did not cancel the remote RPC")
				}
				time.Sleep(time.Millisecond)
			}
		})
	}
}

func TestCommandStreamErrorTextNotLeaked(t *testing.T) {
	const secret = "STREAM_SECRET_COMMAND_OUTPUT"
	client, _, _ := commandTestStaticClient([]*nodev1.ExecEvent{commandTestStarted()}, status.Error(codes.Internal, secret))
	handler := commandTestHandler(t, commandTestWithReadyLookup(commandTestNodeID), commandTestFactory(client))
	recorder := commandTestServe(t, handler, commandTestOwner, commandTestSandboxID, commandTestStreamBody)
	// Check the bytes before a decoder can echo them in a failure message.
	commandTestRequireNoSecret(t, "stream body", recorder.Body.Bytes(), secret)
	var terminal api.CommandStreamFrame
	for _, line := range bytes.Split(recorder.Body.Bytes(), []byte{'\n'}) {
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		var frame api.CommandStreamFrame
		if err := json.Unmarshal(line, &frame); err != nil {
			t.Fatal("stream frame is not valid JSON")
		}
		if frame.Type == api.CommandFrameError {
			terminal = frame
		}
	}
	if terminal.Type != api.CommandFrameError || terminal.Message != "the sandbox machine could not complete the call" {
		t.Errorf("terminal frame has type %q, want a fixed error message", terminal.Type)
	}
	commandTestValidateNDJSON(t, recorder)
}
