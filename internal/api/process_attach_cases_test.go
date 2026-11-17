package api_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/nathabonfim59/bamboo-raft/internal/api"
	"github.com/nathabonfim59/bamboo-raft/internal/auth"
	"github.com/nathabonfim59/bamboo-raft/internal/db"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

func TestNewProcessHandlersNeedStore(t *testing.T) {
	if _, err := api.NewCommandAttachHandler(api.CommandAttachOptions{}); err == nil {
		t.Error("attach accepted a nil store")
	}
	if _, err := api.NewCommandStopHandler(api.CommandsOptions{}); err == nil {
		t.Error("stop accepted a nil store")
	}
	store := newProcessTestStore()
	client := &processAttachTestClient{}
	if _, err := api.NewCommandAttachHandler(api.CommandAttachOptions{CommandsOptions: api.CommandsOptions{
		Lookup: processAttachTestLookup(), Clients: processTestFactory(client), Store: store,
	}}); err != nil {
		t.Errorf("valid attach options: %v", err)
	}
	if _, err := api.NewCommandStopHandler(api.CommandsOptions{
		Lookup: processAttachTestLookup(), Clients: processTestFactory(client), Store: store,
	}); err != nil {
		t.Errorf("valid stop options: %v", err)
	}
}

func TestAttachRequestValidation(t *testing.T) {
	tests := []struct {
		name      string
		owner     string
		processID string
		query     string
		seed      bool
		status    db.CommandStatus
		targets   []api.CommandTarget
		mutate    func(*db.CommandRecord)
		recordErr error
		want      int
		code      string
	}{
		{name: "unauthenticated", processID: "7", want: http.StatusUnauthorized, code: "unauthorized"},
		{name: "bad owner", owner: "not-a-uuid", processID: "7", want: http.StatusUnauthorized, code: "unauthorized"},
		{name: "zero process", owner: processTestOwner, processID: "0", want: http.StatusNotFound, code: "not_found"},
		{name: "negative process", owner: processTestOwner, processID: "-1", want: http.StatusNotFound, code: "not_found"},
		{name: "text process", owner: processTestOwner, processID: "abc", want: http.StatusNotFound, code: "not_found"},
		{name: "overflow process", owner: processTestOwner, processID: "4294967296", want: http.StatusNotFound, code: "not_found"},
		{name: "negative offset", owner: processTestOwner, processID: "7", query: "stdoutOffset=-1", want: http.StatusBadRequest, code: "invalid_query"},
		{name: "text offset", owner: processTestOwner, processID: "7", query: "stderrOffset=x", want: http.StatusBadRequest, code: "invalid_query"},
		{name: "unknown query", owner: processTestOwner, processID: "7", query: "tailBytes=1", want: http.StatusBadRequest, code: "invalid_query"},
		{name: "repeated offset", owner: processTestOwner, processID: "7", query: "stdoutOffset=1&stdoutOffset=2", want: http.StatusBadRequest, code: "invalid_query"},
		{name: "missing record", owner: processTestOwner, processID: "7", want: http.StatusNotFound, code: "not_found"},
		{name: "failed start", owner: processTestOwner, processID: "7", seed: true, status: db.CommandFailed, want: http.StatusNotFound, code: "not_found"},
		{name: "record read fault", owner: processTestOwner, processID: "7", seed: true, status: db.CommandRunning, recordErr: errors.New("private store detail"), want: http.StatusInternalServerError, code: "http_500"},
		{name: "foreign owner record", owner: processTestOwner, processID: "7", seed: true, status: db.CommandRunning,
			mutate: func(r *db.CommandRecord) { r.OwnerID = processAttachTestUUID("018f4b1c-7e2d-7a3b-9c4d-000000000002") }, want: http.StatusInternalServerError, code: "http_500"},
		{name: "foreign sandbox record", owner: processTestOwner, processID: "7", seed: true, status: db.CommandRunning,
			mutate: func(r *db.CommandRecord) { r.SandboxID = "bx_other" }, want: http.StatusInternalServerError, code: "http_500"},
		{name: "wrong mode record", owner: processTestOwner, processID: "7", seed: true, status: db.CommandRunning,
			mutate: func(r *db.CommandRecord) { r.Mode = "sync" }, want: http.StatusInternalServerError, code: "http_500"},
		{name: "not ready", owner: processTestOwner, processID: "7", seed: true, status: db.CommandRunning,
			targets: []api.CommandTarget{{State: api.StateArchived}}, want: http.StatusConflict, code: "sandbox_not_ready"},
		{name: "starting stays starting", owner: processTestOwner, processID: "7", seed: true, status: db.CommandRunning,
			targets: []api.CommandTarget{{State: api.StateProvisioning}}, want: http.StatusConflict, code: "sandbox_starting"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := newProcessTestStore()
			if test.seed {
				record := processTestRecord(processAttachTestProcessID, test.status)
				if test.mutate != nil {
					test.mutate(&record)
				}
				store.seed(record)
			}
			store.recordErr = test.recordErr
			client := &processAttachTestClient{}
			lookup := processAttachTestLookup()
			if test.targets != nil {
				lookup.targets = test.targets
			}
			handler := processAttachTestHandler(t, store, client, func(opts *api.CommandAttachOptions) {
				opts.Lookup = lookup
				opts.ReadyDeadline = 20 * time.Millisecond
			})
			recorder := processAttachTestServe(t, handler, test.owner, processTestSandboxID, test.processID, test.query)
			envelope := commandTestDecodeError(t, recorder)
			if recorder.Code != test.want || envelope.Code != test.code {
				t.Fatalf("error = %d %q, want %d %q\nbody: %s", recorder.Code, envelope.Code, test.want, test.code, recorder.Body.Bytes())
			}
			if attachCalls, _ := client.counts(); attachCalls != 0 {
				t.Errorf("invalid request reached AttachProcess: %d calls", attachCalls)
			}
			processTestValidate(t, "attachCommand", recorder)
		})
	}
}

func TestAttachFramesAndExit(t *testing.T) {
	store := newProcessTestStore()
	store.seed(processTestRecord(processAttachTestProcessID, db.CommandRunning))
	client := &processAttachTestClient{}
	client.attach = func(ctx context.Context, _ *nodev1.AttachProcessRequest) (grpc.ServerStreamingClient[nodev1.ProcessEvent], error) {
		return newProcessAttachTestStream(ctx, []*nodev1.ProcessEvent{
			processAttachTestAttached(processAttachTestProcessID, 0),
			processAttachTestChunk(api.CommandAttachStdout, []byte("ab"), 2),
			processAttachTestChunk(api.CommandAttachStderr, []byte("er"), 2),
			processAttachTestExit(processAttachTestProcessID, new(int32(0)), ""),
		}, nil, false), nil
	}
	handler := processAttachTestHandler(t, store, client)
	recorder := processAttachTestServe(t, handler, processTestOwner, processTestSandboxID, "7", "")

	processAttachTestValidate(t, recorder.Code, recorder.Header(), recorder.Body.Bytes())
	frames := processAttachTestParseFrames(t, recorder.Body.Bytes())
	wantTypes := []api.CommandAttachFrameType{
		api.CommandAttachAttached, api.CommandAttachStdout, api.CommandAttachStderr, api.CommandAttachExit,
	}
	if len(frames) != len(wantTypes) {
		t.Fatalf("frames = %+v, want %v", frames, wantTypes)
	}
	for i, want := range wantTypes {
		if frames[i].Type != want {
			t.Fatalf("frame %d = %q, want %q", i, frames[i].Type, want)
		}
	}
	if frames[0].ProcessID != processAttachTestProcessID ||
		frames[0].StdoutOffset == nil || *frames[0].StdoutOffset != 0 ||
		frames[0].StderrOffset == nil || *frames[0].StderrOffset != 0 {
		t.Errorf("attached frame = %+v", frames[0])
	}
	if frames[1].Data != "ab" || frames[1].NextOffset == nil || *frames[1].NextOffset != 2 {
		t.Errorf("stdout frame = %+v", frames[1])
	}
	if frames[2].Data != "er" || frames[2].NextOffset == nil || *frames[2].NextOffset != 2 {
		t.Errorf("stderr frame = %+v", frames[2])
	}
	last := frames[3]
	if last.Status != "exited" || !last.ExitCode.Present || last.ExitCode.Value == nil || *last.ExitCode.Value != 0 {
		t.Errorf("exit frame = %+v", last)
	}
	if !last.Signal.Present || last.Signal.Value != nil {
		t.Errorf("exit signal = %+v, want explicit null", last.Signal)
	}
	if last.StdoutOffset == nil || *last.StdoutOffset != 2 || last.StderrOffset == nil || *last.StderrOffset != 2 {
		t.Errorf("exit offsets = %+v", last)
	}

	record, ok := store.record(processAttachTestProcessID)
	if !ok || record.Status != db.CommandExited || record.ExitCode == nil || *record.ExitCode != 0 {
		t.Errorf("record = %+v", record)
	}
	if _, observe, _ := store.callCounts(); observe != 2 {
		t.Errorf("ObserveCommand calls = %d, want attached and exit", observe)
	}
	if _, err := client.attachRequest(); !err {
		t.Error("AttachProcess was not called")
	}
}

func TestAttachQueryOffsetsReachNode(t *testing.T) {
	store := newProcessTestStore()
	store.seed(processTestRecord(processAttachTestProcessID, db.CommandRunning))
	client := &processAttachTestClient{}
	var request *nodev1.AttachProcessRequest
	client.attach = func(ctx context.Context, attachRequest *nodev1.AttachProcessRequest) (grpc.ServerStreamingClient[nodev1.ProcessEvent], error) {
		request = attachRequest
		return newProcessAttachTestStream(ctx, []*nodev1.ProcessEvent{
			processAttachTestAttachedProcess(processAttachTestProcessID, 5, 9, processTestRunningProcess(processAttachTestProcessID, "cmd")),
			processAttachTestChunk(api.CommandAttachStdout, []byte("x"), 6),
			processAttachTestExit(processAttachTestProcessID, new(int32(0)), ""),
		}, nil, false), nil
	}
	handler := processAttachTestHandler(t, store, client)
	recorder := processAttachTestServe(t, handler, processTestOwner, processTestSandboxID, "7", "stdoutOffset=5&stderrOffset=9")
	processAttachTestValidate(t, recorder.Code, recorder.Header(), recorder.Body.Bytes())
	if request == nil || request.GetStdoutOffset() != 5 || request.GetStderrOffset() != 9 {
		t.Fatalf("AttachProcess request = %+v", request)
	}
	frames := processAttachTestParseFrames(t, recorder.Body.Bytes())
	if frames[0].StdoutOffset == nil || *frames[0].StdoutOffset != 5 || frames[0].StderrOffset == nil || *frames[0].StderrOffset != 9 {
		t.Errorf("attached offsets = %+v", frames[0])
	}
}

func TestAttachAlreadyExited(t *testing.T) {
	store := newProcessTestStore()
	store.seed(processTestRecord(processAttachTestProcessID, db.CommandExited))
	exited := processTestExitedProcess(processAttachTestProcessID, "cmd", new(int32(3)), "")
	exited.StdoutSize = 0
	exited.StderrSize = 0
	client := &processAttachTestClient{}
	client.attach = func(ctx context.Context, _ *nodev1.AttachProcessRequest) (grpc.ServerStreamingClient[nodev1.ProcessEvent], error) {
		return newProcessAttachTestStream(ctx, []*nodev1.ProcessEvent{
			processAttachTestAttachedProcess(processAttachTestProcessID, 0, 0, exited),
			processAttachTestChunk(api.CommandAttachStdout, []byte("tail"), 4),
			&nodev1.ProcessEvent{Event: &nodev1.ProcessEvent_Exit{Exit: exited}},
		}, nil, false), nil
	}
	handler := processAttachTestHandler(t, store, client)
	recorder := processAttachTestServe(t, handler, processTestOwner, processTestSandboxID, "7", "")
	processAttachTestValidate(t, recorder.Code, recorder.Header(), recorder.Body.Bytes())
	frames := processAttachTestParseFrames(t, recorder.Body.Bytes())
	if len(frames) != 3 || frames[0].Type != api.CommandAttachAttached || frames[1].Data != "tail" || frames[2].Type != api.CommandAttachExit {
		t.Fatalf("frames = %+v", frames)
	}
	if frames[2].ExitCode.Value == nil || *frames[2].ExitCode.Value != 3 {
		t.Errorf("exit = %+v", frames[2])
	}
}

func TestAttachUTF8Boundaries(t *testing.T) {
	euro := []byte("€")
	t.Run("split rune across chunks", func(t *testing.T) {
		store := newProcessTestStore()
		store.seed(processTestRecord(processAttachTestProcessID, db.CommandRunning))
		client := &processAttachTestClient{}
		client.attach = func(ctx context.Context, _ *nodev1.AttachProcessRequest) (grpc.ServerStreamingClient[nodev1.ProcessEvent], error) {
			return newProcessAttachTestStream(ctx, []*nodev1.ProcessEvent{
				processAttachTestAttached(processAttachTestProcessID, 0),
				processAttachTestChunk(api.CommandAttachStdout, euro[:1], 1),
				processAttachTestChunk(api.CommandAttachStderr, []byte("x"), 1),
				processAttachTestChunk(api.CommandAttachStdout, euro[1:], 3),
				processAttachTestExit(processAttachTestProcessID, new(int32(0)), ""),
			}, nil, false), nil
		}
		recorder := processAttachTestServe(t, processAttachTestHandler(t, store, client), processTestOwner, processTestSandboxID, "7", "")
		processAttachTestValidate(t, recorder.Code, recorder.Header(), recorder.Body.Bytes())
		var output strings.Builder
		for _, frame := range processAttachTestParseFrames(t, recorder.Body.Bytes()) {
			if frame.Type == api.CommandAttachStdout {
				output.WriteString(frame.Data)
			}
			if strings.Contains(frame.Data, "\uFFFD") {
				t.Errorf("split rune was replaced: %+v", frame)
			}
		}
		if output.String() != "€" {
			t.Errorf("stdout = %q, want €", output.String())
		}
	})

	t.Run("invalid byte uses replacement with raw offset", func(t *testing.T) {
		store := newProcessTestStore()
		store.seed(processTestRecord(processAttachTestProcessID, db.CommandRunning))
		client := &processAttachTestClient{}
		client.attach = func(ctx context.Context, _ *nodev1.AttachProcessRequest) (grpc.ServerStreamingClient[nodev1.ProcessEvent], error) {
			return newProcessAttachTestStream(ctx, []*nodev1.ProcessEvent{
				processAttachTestAttached(processAttachTestProcessID, 0),
				processAttachTestChunk(api.CommandAttachStdout, []byte{0xff, 'a'}, 2),
				processAttachTestExit(processAttachTestProcessID, new(int32(0)), ""),
			}, nil, false), nil
		}
		recorder := processAttachTestServe(t, processAttachTestHandler(t, store, client), processTestOwner, processTestSandboxID, "7", "")
		processAttachTestValidate(t, recorder.Code, recorder.Header(), recorder.Body.Bytes())
		frames := processAttachTestParseFrames(t, recorder.Body.Bytes())
		if len(frames) < 2 || frames[1].Data != "\uFFFD"+"a" || frames[1].NextOffset == nil || *frames[1].NextOffset != 2 {
			t.Fatalf("frames = %+v", frames)
		}
	})

	t.Run("validated exit flushes incomplete tail", func(t *testing.T) {
		store := newProcessTestStore()
		store.seed(processTestRecord(processAttachTestProcessID, db.CommandRunning))
		client := &processAttachTestClient{}
		client.attach = func(ctx context.Context, _ *nodev1.AttachProcessRequest) (grpc.ServerStreamingClient[nodev1.ProcessEvent], error) {
			return newProcessAttachTestStream(ctx, []*nodev1.ProcessEvent{
				processAttachTestAttached(processAttachTestProcessID, 0),
				processAttachTestChunk(api.CommandAttachStdout, euro[:2], 2),
				processAttachTestExit(processAttachTestProcessID, new(int32(0)), ""),
			}, nil, false), nil
		}
		recorder := processAttachTestServe(t, processAttachTestHandler(t, store, client), processTestOwner, processTestSandboxID, "7", "")
		processAttachTestValidate(t, recorder.Code, recorder.Header(), recorder.Body.Bytes())
		frames := processAttachTestParseFrames(t, recorder.Body.Bytes())
		if len(frames) != 3 {
			t.Fatalf("frames = %+v", frames)
		}
		if frames[1].Type != api.CommandAttachStdout || frames[1].Data != "\uFFFD" ||
			frames[1].NextOffset == nil || *frames[1].NextOffset != 2 {
			t.Errorf("flushed tail = %+v", frames[1])
		}
		if frames[2].StdoutOffset == nil || *frames[2].StdoutOffset != 2 {
			t.Errorf("exit offsets = %+v", frames[2])
		}
	})
}

func TestAttachErrorAndEOFPreservePendingRune(t *testing.T) {
	euro := []byte("€")
	errorsUnderTest := []struct {
		name string
		err  error
	}{
		{"node error", status.Error(codes.Unavailable, "transport")},
		{"EOF without native exit", io.EOF},
	}
	for _, test := range errorsUnderTest {
		t.Run(test.name, func(t *testing.T) {
			store := newProcessTestStore()
			store.seed(processTestRecord(processAttachTestProcessID, db.CommandRunning))
			client := &processAttachTestClient{}
			client.attach = func(ctx context.Context, _ *nodev1.AttachProcessRequest) (grpc.ServerStreamingClient[nodev1.ProcessEvent], error) {
				return newProcessAttachTestStream(ctx, []*nodev1.ProcessEvent{
					processAttachTestAttached(processAttachTestProcessID, 0),
					processAttachTestChunk(api.CommandAttachStdout, euro[:2], 2),
				}, test.err, false), nil
			}
			recorder := processAttachTestServe(t, processAttachTestHandler(t, store, client), processTestOwner, processTestSandboxID, "7", "")
			processAttachTestValidate(t, recorder.Code, recorder.Header(), recorder.Body.Bytes())
			frames := processAttachTestParseFrames(t, recorder.Body.Bytes())
			if len(frames) != 2 || frames[1].Type != api.CommandAttachError {
				t.Fatalf("frames = %+v, want attached + error", frames)
			}
			if frames[1].Error != "sandbox_direct_failed" || frames[1].StdoutOffset == nil || *frames[1].StdoutOffset != 0 {
				t.Errorf("error frame = %+v, want emitted offset 0", frames[1])
			}
			if record, _ := store.record(processAttachTestProcessID); record.Status != db.CommandRunning {
				t.Errorf("uncertain error changed the record: %+v", record)
			}

			// Replaying from the reported offsets completes the rune
			// exactly once.
			client.attach = func(ctx context.Context, request *nodev1.AttachProcessRequest) (grpc.ServerStreamingClient[nodev1.ProcessEvent], error) {
				if request.GetStdoutOffset() != 0 {
					t.Errorf("reconnect offset = %d, want 0", request.GetStdoutOffset())
				}
				return newProcessAttachTestStream(ctx, []*nodev1.ProcessEvent{
					processAttachTestAttached(processAttachTestProcessID, 0),
					processAttachTestChunk(api.CommandAttachStdout, euro[:1], 1),
					processAttachTestChunk(api.CommandAttachStdout, euro[1:], 3),
					processAttachTestExit(processAttachTestProcessID, new(int32(0)), ""),
				}, nil, false), nil
			}
			recorder = processAttachTestServe(t, processAttachTestHandler(t, store, client), processTestOwner, processTestSandboxID, "7", "")
			var output strings.Builder
			for _, frame := range processAttachTestParseFrames(t, recorder.Body.Bytes()) {
				if frame.Type == api.CommandAttachStdout {
					output.WriteString(frame.Data)
				}
			}
			if output.String() != "€" {
				t.Errorf("replayed stdout = %q, want one €", output.String())
			}
		})
	}
}

func TestAttachReconnectPreservesPendingRune(t *testing.T) {
	euro := []byte("€")
	store := newProcessTestStore()
	store.seed(processTestRecord(processAttachTestProcessID, db.CommandRunning))
	stream := newProcessAttachTestStream(t.Context(), nil, nil, true)
	client := &processAttachTestClient{}
	client.attach = func(ctx context.Context, _ *nodev1.AttachProcessRequest) (grpc.ServerStreamingClient[nodev1.ProcessEvent], error) {
		stream.ctx = ctx
		stream.events = []*nodev1.ProcessEvent{
			processAttachTestAttached(processAttachTestProcessID, 0),
			processAttachTestChunk(api.CommandAttachStdout, euro[:2], 2),
		}
		return stream, nil
	}
	handler := processAttachTestHandler(t, store, client, func(opts *api.CommandAttachOptions) {
		opts.ReconnectAfter = 40 * time.Millisecond
		opts.Heartbeat = time.Second
	})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	writer, done := processAttachTestBegin(t, handler, ctx, processTestOwner, processTestSandboxID, "7", "")
	writer.waitType(t, api.CommandAttachAttached, 1)
	reconnect := writer.waitType(t, api.CommandAttachReconnect, 1)
	if reconnect.StdoutOffset == nil || *reconnect.StdoutOffset != 0 ||
		reconnect.StderrOffset == nil || *reconnect.StderrOffset != 0 {
		t.Errorf("reconnect offsets = %+v, want emitted 0/0", reconnect)
	}
	processAttachTestWaitDone(t, done)
	processAttachTestWaitClosed(t, "receiver join", stream.done)
	processAttachTestValidateWriter(t, writer)

	// The next attach starts at the reported offset and completes the rune.
	client.attach = func(ctx context.Context, request *nodev1.AttachProcessRequest) (grpc.ServerStreamingClient[nodev1.ProcessEvent], error) {
		return newProcessAttachTestStream(ctx, []*nodev1.ProcessEvent{
			processAttachTestAttached(processAttachTestProcessID, request.GetStdoutOffset()),
			processAttachTestChunk(api.CommandAttachStdout, euro[:1], 1),
			processAttachTestChunk(api.CommandAttachStdout, euro[1:], 3),
			processAttachTestExit(processAttachTestProcessID, new(int32(0)), ""),
		}, nil, false), nil
	}
	recorder := processAttachTestServe(t, processAttachTestHandler(t, store, client), processTestOwner, processTestSandboxID, "7", "stdoutOffset=0")
	var output strings.Builder
	for _, frame := range processAttachTestParseFrames(t, recorder.Body.Bytes()) {
		if frame.Type == api.CommandAttachStdout {
			output.WriteString(frame.Data)
		}
	}
	if output.String() != "€" {
		t.Errorf("replayed stdout = %q, want one €", output.String())
	}
}

func TestAttachHeartbeat(t *testing.T) {
	store := newProcessTestStore()
	store.seed(processTestRecord(processAttachTestProcessID, db.CommandRunning))
	stream := newProcessAttachTestStream(t.Context(), nil, nil, true)
	client := &processAttachTestClient{}
	client.attach = func(ctx context.Context, _ *nodev1.AttachProcessRequest) (grpc.ServerStreamingClient[nodev1.ProcessEvent], error) {
		stream.ctx = ctx
		stream.events = []*nodev1.ProcessEvent{processAttachTestAttached(processAttachTestProcessID, 0)}
		return stream, nil
	}
	handler := processAttachTestHandler(t, store, client, func(opts *api.CommandAttachOptions) {
		opts.Heartbeat = 20 * time.Millisecond
		opts.ReconnectAfter = 10 * time.Second
	})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	writer, done := processAttachTestBegin(t, handler, ctx, processTestOwner, processTestSandboxID, "7", "")
	writer.waitType(t, api.CommandAttachAttached, 1)
	heartbeat := writer.waitType(t, api.CommandAttachHeartbeat, 1)
	if heartbeat.StdoutOffset != nil || heartbeat.StderrOffset != nil || heartbeat.Data != "" {
		t.Errorf("heartbeat = %+v, want type only", heartbeat)
	}
	cancel()
	processAttachTestWaitDone(t, done)
	processAttachTestWaitClosed(t, "receiver join", stream.done)
	processAttachTestValidateWriter(t, writer)
}

func TestAttachBootstrapTimeoutBeforeHeaders(t *testing.T) {
	store := newProcessTestStore()
	store.seed(processTestRecord(processAttachTestProcessID, db.CommandRunning))
	stream := newProcessAttachTestStream(t.Context(), nil, nil, true)
	var streamCtx context.Context
	client := &processAttachTestClient{}
	client.attach = func(ctx context.Context, _ *nodev1.AttachProcessRequest) (grpc.ServerStreamingClient[nodev1.ProcessEvent], error) {
		streamCtx = ctx
		stream.ctx = ctx
		return stream, nil
	}
	handler := processAttachTestHandler(t, store, client, func(opts *api.CommandAttachOptions) {
		opts.BootstrapTimeout = 20 * time.Millisecond
	})
	recorder := processAttachTestServe(t, handler, processTestOwner, processTestSandboxID, "7", "")
	envelope := commandTestDecodeError(t, recorder)
	if recorder.Code != http.StatusBadGateway || envelope.Code != "sandbox_direct_failed" {
		t.Fatalf("bootstrap timeout = %d %q", recorder.Code, envelope.Code)
	}
	processAttachTestWaitClosed(t, "receiver join", stream.done)
	if streamCtx == nil {
		t.Fatal("AttachProcess was not called")
	}
	if _, hasDeadline := streamCtx.Deadline(); hasDeadline {
		t.Error("attach RPC context carries the bootstrap deadline")
	}
}

func TestAttachBootstrapTimeoutStopsAfterAttached(t *testing.T) {
	store := newProcessTestStore()
	store.seed(processTestRecord(processAttachTestProcessID, db.CommandRunning))
	stream := newProcessAttachTestStream(t.Context(), nil, nil, true)
	client := &processAttachTestClient{}
	client.attach = func(ctx context.Context, _ *nodev1.AttachProcessRequest) (grpc.ServerStreamingClient[nodev1.ProcessEvent], error) {
		stream.ctx = ctx
		stream.events = []*nodev1.ProcessEvent{processAttachTestAttached(processAttachTestProcessID, 0)}
		return stream, nil
	}
	handler := processAttachTestHandler(t, store, client, func(opts *api.CommandAttachOptions) {
		opts.BootstrapTimeout = 500 * time.Millisecond
		opts.Heartbeat = 750 * time.Millisecond
		opts.ReconnectAfter = 10 * time.Second
	})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	writer, done := processAttachTestBegin(t, handler, ctx, processTestOwner, processTestSandboxID, "7", "")
	writer.waitType(t, api.CommandAttachAttached, 1)
	writer.waitType(t, api.CommandAttachHeartbeat, 1)
	select {
	case <-done:
		t.Fatal("bootstrap timeout ended the stream after attached")
	default:
	}
	cancel()
	processAttachTestWaitDone(t, done)
	processAttachTestWaitClosed(t, "receiver join", stream.done)
}

func TestAttachNodeFailureBeforeHeaders(t *testing.T) {
	badProcess := processTestRunningProcess(processAttachTestProcessID, "cmd")
	badProcess.ProcessId = 8
	tests := []struct {
		name      string
		err       error
		first     *nodev1.ProcessEvent
		query     string
		status    int
		code      string
		retryable *bool
	}{
		{"not found", status.Error(codes.NotFound, "gone"), nil, "", http.StatusNotFound, "not_found", nil},
		{"invalid argument", status.Error(codes.InvalidArgument, "bad"), nil, "", http.StatusBadRequest, "invalid_path", nil},
		{"failed precondition", status.Error(codes.FailedPrecondition, "not ready"), nil, "", http.StatusConflict, "sandbox_not_ready", new(false)},
		{"resource exhausted is not capacity", status.Error(codes.ResourceExhausted, "size"), nil, "", http.StatusBadGateway, "sandbox_direct_failed", new(false)},
		{"unavailable", status.Error(codes.Unavailable, "lost"), nil, "", http.StatusBadGateway, "sandbox_direct_failed", new(false)},
		{"eof", io.EOF, nil, "", http.StatusBadGateway, "sandbox_direct_failed", new(false)},
		{"output first", nil, processAttachTestChunk(api.CommandAttachStdout, []byte("x"), 1), "", http.StatusBadGateway, "sandbox_direct_failed", nil},
		{"exit first", nil, processAttachTestExit(processAttachTestProcessID, new(int32(0)), ""), "", http.StatusBadGateway, "sandbox_direct_failed", nil},
		{"empty event", nil, &nodev1.ProcessEvent{}, "", http.StatusBadGateway, "sandbox_direct_failed", nil},
		{"typed nil attached wrapper", nil, &nodev1.ProcessEvent{Event: (*nodev1.ProcessEvent_Attached_)(nil)}, "", http.StatusBadGateway, "sandbox_direct_failed", nil},
		{"typed nil attached payload", nil, &nodev1.ProcessEvent{Event: &nodev1.ProcessEvent_Attached_{Attached: nil}}, "", http.StatusBadGateway, "sandbox_direct_failed", nil},
		{"wrong process id", nil, processAttachTestAttachedProcess(processAttachTestProcessID, 0, 0, badProcess), "", http.StatusBadGateway, "sandbox_direct_failed", nil},
		{"negative offset", nil, processAttachTestAttachedProcess(processAttachTestProcessID, -1, 0, processTestRunningProcess(processAttachTestProcessID, "cmd")), "", http.StatusBadGateway, "sandbox_direct_failed", nil},
		{"offset past request", nil, processAttachTestAttachedProcess(processAttachTestProcessID, 2, 0, processTestRunningProcess(processAttachTestProcessID, "cmd")), "stdoutOffset=1", http.StatusBadGateway, "sandbox_direct_failed", nil},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := newProcessTestStore()
			store.seed(processTestRecord(processAttachTestProcessID, db.CommandRunning))
			client := &processAttachTestClient{}
			client.attach = func(ctx context.Context, _ *nodev1.AttachProcessRequest) (grpc.ServerStreamingClient[nodev1.ProcessEvent], error) {
				if test.err != nil {
					return nil, test.err
				}
				return newProcessAttachTestStream(ctx, []*nodev1.ProcessEvent{test.first}, nil, false), nil
			}
			handler := processAttachTestHandler(t, store, client)
			recorder := processAttachTestServe(t, handler, processTestOwner, processTestSandboxID, "7", test.query)
			envelope := commandTestDecodeError(t, recorder)
			if recorder.Code != test.status || envelope.Code != test.code {
				t.Fatalf("error = %d %q, want %d %q\nbody: %s", recorder.Code, envelope.Code, test.status, test.code, recorder.Body.Bytes())
			}
			if test.retryable != nil {
				got, ok := envelope.Error.Details["retryable"].(bool)
				if !ok || got != *test.retryable {
					t.Errorf("retryable = %v, want %v", envelope.Error.Details["retryable"], *test.retryable)
				}
			}
			if _, observe, _ := store.callCounts(); observe != 0 {
				t.Errorf("malformed first event was observed: %d calls", observe)
			}
			processTestValidate(t, "attachCommand", recorder)
		})
	}
}

func TestAttachNodeFailureAfterHeaders(t *testing.T) {
	tests := []struct {
		name   string
		events []*nodev1.ProcessEvent
		err    error
		query  string
	}{
		{"duplicate attached", []*nodev1.ProcessEvent{
			processAttachTestAttached(processAttachTestProcessID, 0),
			processAttachTestAttached(processAttachTestProcessID, 0),
		}, nil, ""},
		{"empty event", []*nodev1.ProcessEvent{
			processAttachTestAttached(processAttachTestProcessID, 0),
			{},
		}, nil, ""},
		{"nil chunk", []*nodev1.ProcessEvent{
			processAttachTestAttached(processAttachTestProcessID, 0),
			{Event: &nodev1.ProcessEvent_Stdout{}},
		}, nil, ""},
		{"bad next offset", []*nodev1.ProcessEvent{
			processAttachTestAttached(processAttachTestProcessID, 0),
			processAttachTestChunk(api.CommandAttachStdout, []byte("a"), 2),
		}, nil, ""},
		{"decreasing offset", []*nodev1.ProcessEvent{
			processAttachTestAttached(processAttachTestProcessID, 0),
			processAttachTestChunk(api.CommandAttachStdout, []byte("a"), 1),
			processAttachTestChunk(api.CommandAttachStdout, []byte("b"), 1),
		}, nil, ""},
		{"overflow offset", []*nodev1.ProcessEvent{
			processAttachTestAttachedProcess(processAttachTestProcessID, 9223372036854775806, 0, processTestRunningProcess(processAttachTestProcessID, "cmd")),
			processAttachTestChunk(api.CommandAttachStdout, []byte("abc"), 9223372036854775807),
		}, nil, "stdoutOffset=9223372036854775806"},
		{"node error", []*nodev1.ProcessEvent{
			processAttachTestAttached(processAttachTestProcessID, 0),
		}, status.Error(codes.Internal, "hidden"), ""},
		{"eof without exit", []*nodev1.ProcessEvent{
			processAttachTestAttached(processAttachTestProcessID, 0),
		}, io.EOF, ""},
		{"running exit", []*nodev1.ProcessEvent{
			processAttachTestAttached(processAttachTestProcessID, 0),
			{Event: &nodev1.ProcessEvent_Exit{Exit: processTestRunningProcess(processAttachTestProcessID, "cmd")}},
		}, nil, ""},
		{"exit code and signal", []*nodev1.ProcessEvent{
			processAttachTestAttached(processAttachTestProcessID, 0),
			{Event: &nodev1.ProcessEvent_Exit{Exit: processTestExitedProcess(processAttachTestProcessID, "cmd", new(int32(0)), "SIGTERM")}},
		}, nil, ""},
		{"premature exit then data", []*nodev1.ProcessEvent{
			processAttachTestAttached(processAttachTestProcessID, 0),
			processAttachTestExit(processAttachTestProcessID, new(int32(0)), ""),
			processAttachTestChunk(api.CommandAttachStdout, []byte("late"), 4),
		}, nil, ""},
		{"duplicate exit", []*nodev1.ProcessEvent{
			processAttachTestAttached(processAttachTestProcessID, 0),
			processAttachTestExit(processAttachTestProcessID, new(int32(0)), ""),
			processAttachTestExit(processAttachTestProcessID, new(int32(0)), ""),
		}, nil, ""},
		{"typed nil stdout wrapper", []*nodev1.ProcessEvent{
			processAttachTestAttached(processAttachTestProcessID, 0),
			{Event: (*nodev1.ProcessEvent_Stdout)(nil)},
		}, nil, ""},
		{"typed nil exit wrapper", []*nodev1.ProcessEvent{
			processAttachTestAttached(processAttachTestProcessID, 0),
			{Event: (*nodev1.ProcessEvent_Exit)(nil)},
		}, nil, ""},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := newProcessTestStore()
			store.seed(processTestRecord(processAttachTestProcessID, db.CommandRunning))
			stream := newProcessAttachTestStream(t.Context(), nil, test.err, false)
			client := &processAttachTestClient{}
			client.attach = func(ctx context.Context, _ *nodev1.AttachProcessRequest) (grpc.ServerStreamingClient[nodev1.ProcessEvent], error) {
				stream.ctx = ctx
				stream.events = test.events
				return stream, nil
			}
			handler := processAttachTestHandler(t, store, client)
			recorder := processAttachTestServe(t, handler, processTestOwner, processTestSandboxID, "7", test.query)
			processAttachTestValidate(t, recorder.Code, recorder.Header(), recorder.Body.Bytes())
			frames := processAttachTestParseFrames(t, recorder.Body.Bytes())
			if len(frames) < 2 || frames[0].Type != api.CommandAttachAttached {
				t.Fatalf("frames = %+v", frames)
			}
			terminal := frames[len(frames)-1]
			if terminal.Type != api.CommandAttachError || terminal.Error != "sandbox_direct_failed" {
				t.Fatalf("terminal = %+v, want sandbox_direct_failed error", terminal)
			}
			terminals := 0
			for _, frame := range frames {
				if frame.Type == api.CommandAttachExit || frame.Type == api.CommandAttachError || frame.Type == api.CommandAttachReconnect {
					terminals++
				}
			}
			if terminals != 1 {
				t.Errorf("terminal frames = %d, want exactly 1", terminals)
			}
		})
	}
}

func TestAttachStoreFaults(t *testing.T) {
	t.Run("attached observation", func(t *testing.T) {
		store := newProcessTestStore()
		store.seed(processTestRecord(processAttachTestProcessID, db.CommandRunning))
		store.observeErr = errors.New("private store detail")
		client := &processAttachTestClient{}
		client.attach = func(ctx context.Context, _ *nodev1.AttachProcessRequest) (grpc.ServerStreamingClient[nodev1.ProcessEvent], error) {
			return newProcessAttachTestStream(ctx, []*nodev1.ProcessEvent{processAttachTestAttached(processAttachTestProcessID, 0)}, nil, false), nil
		}
		recorder := processAttachTestServe(t, processAttachTestHandler(t, store, client), processTestOwner, processTestSandboxID, "7", "")
		envelope := commandTestDecodeError(t, recorder)
		if recorder.Code != http.StatusInternalServerError || envelope.Code != "http_500" {
			t.Fatalf("attached store fault = %d %q", recorder.Code, envelope.Code)
		}
		if strings.Contains(recorder.Body.String(), "private store detail") {
			t.Error("store detail leaked")
		}
	})

	t.Run("exit observation", func(t *testing.T) {
		store := newProcessTestStore()
		store.seed(processTestRecord(processAttachTestProcessID, db.CommandRunning))
		store.observeHook = func() {
			store.mu.Lock()
			store.observeErr = errors.New("private store detail")
			store.mu.Unlock()
		}
		client := &processAttachTestClient{}
		client.attach = func(ctx context.Context, _ *nodev1.AttachProcessRequest) (grpc.ServerStreamingClient[nodev1.ProcessEvent], error) {
			return newProcessAttachTestStream(ctx, []*nodev1.ProcessEvent{
				processAttachTestAttached(processAttachTestProcessID, 0),
				processAttachTestExit(processAttachTestProcessID, new(int32(0)), ""),
			}, nil, false), nil
		}
		recorder := processAttachTestServe(t, processAttachTestHandler(t, store, client), processTestOwner, processTestSandboxID, "7", "")
		processAttachTestValidate(t, recorder.Code, recorder.Header(), recorder.Body.Bytes())
		frames := processAttachTestParseFrames(t, recorder.Body.Bytes())
		if len(frames) != 2 || frames[1].Type != api.CommandAttachError {
			t.Fatalf("frames = %+v, want attached + error", frames)
		}
		if frames[1].Error != "http_500" || frames[1].Message != "internal server error" {
			t.Errorf("store error frame = %+v", frames[1])
		}
		if strings.Contains(recorder.Body.String(), "private store detail") {
			t.Error("store detail leaked")
		}
	})

	t.Run("malformed observation", func(t *testing.T) {
		store := newProcessTestStore()
		store.seed(processTestRecord(processAttachTestProcessID, db.CommandRunning))
		store.observeMutate = func(record *db.CommandRecord) {
			record.OwnerID = processAttachTestUUID("018f4b1c-7e2d-7a3b-9c4d-000000000002")
		}
		client := &processAttachTestClient{}
		client.attach = func(ctx context.Context, _ *nodev1.AttachProcessRequest) (grpc.ServerStreamingClient[nodev1.ProcessEvent], error) {
			return newProcessAttachTestStream(ctx, []*nodev1.ProcessEvent{processAttachTestAttached(processAttachTestProcessID, 0)}, nil, false), nil
		}
		recorder := processAttachTestServe(t, processAttachTestHandler(t, store, client), processTestOwner, processTestSandboxID, "7", "")
		envelope := commandTestDecodeError(t, recorder)
		if recorder.Code != http.StatusInternalServerError || envelope.Code != "http_500" {
			t.Fatalf("malformed observation = %d %q", recorder.Code, envelope.Code)
		}
	})
}

type processAttachFailWriter struct {
	header        http.Header
	writes        int
	flushes       int
	deadlineCalls int
	failWrite     bool
	failFlush     bool
	failSet       bool
	failClear     bool
}

func (w *processAttachFailWriter) Header() http.Header { return w.header }
func (w *processAttachFailWriter) WriteHeader(int)     {}
func (w *processAttachFailWriter) Write(data []byte) (int, error) {
	w.writes++
	if w.failWrite {
		return 0, errors.New("write failed")
	}
	return len(data), nil
}
func (w *processAttachFailWriter) FlushError() error {
	w.flushes++
	if w.failFlush {
		return errors.New("flush failed")
	}
	return nil
}
func (w *processAttachFailWriter) SetWriteDeadline(deadline time.Time) error {
	w.deadlineCalls++
	if deadline.IsZero() {
		if w.failClear {
			return errors.New("clear deadline failed")
		}
		return nil
	}
	if w.failSet {
		return errors.New("set deadline failed")
	}
	return nil
}

func TestAttachWriterFailureCancelsAndJoins(t *testing.T) {
	tests := []struct {
		name   string
		writer *processAttachFailWriter
		writes int
	}{
		{"write", &processAttachFailWriter{header: http.Header{}, failWrite: true}, 1},
		{"flush", &processAttachFailWriter{header: http.Header{}, failFlush: true}, 1},
		{"set deadline", &processAttachFailWriter{header: http.Header{}, failSet: true}, 0},
		{"clear deadline", &processAttachFailWriter{header: http.Header{}, failClear: true}, 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := newProcessTestStore()
			store.seed(processTestRecord(processAttachTestProcessID, db.CommandRunning))
			stream := newProcessAttachTestStream(t.Context(), nil, nil, true)
			client := &processAttachTestClient{}
			client.attach = func(ctx context.Context, _ *nodev1.AttachProcessRequest) (grpc.ServerStreamingClient[nodev1.ProcessEvent], error) {
				stream.ctx = ctx
				stream.events = []*nodev1.ProcessEvent{processAttachTestAttached(processAttachTestProcessID, 0)}
				return stream, nil
			}
			handler := processAttachTestHandler(t, store, client)
			mux := http.NewServeMux()
			mux.Handle("GET "+api.CommandAttachPath, handler)
			request := httptest.NewRequest(http.MethodGet, processAttachTestTarget(processTestSandboxID, "7", ""), nil)
			request = request.WithContext(auth.WithIdentity(request.Context(), auth.Identity{Owner: processTestOwner}))
			done := make(chan struct{})
			go func() {
				mux.ServeHTTP(test.writer, request)
				close(done)
			}()
			processAttachTestWaitDone(t, done)
			processAttachTestWaitClosed(t, "receiver join", stream.done)
			if test.writer.writes != test.writes || test.writer.flushes > 1 {
				t.Errorf("writer saw writes=%d flushes=%d, want writes=%d and at most one flush after a terminal failure", test.writer.writes, test.writer.flushes, test.writes)
			}
		})
	}
}

func TestAttachWriteDeadlineClearedWhileWaiting(t *testing.T) {
	store := newProcessTestStore()
	store.seed(processTestRecord(processAttachTestProcessID, db.CommandRunning))
	stream := newProcessAttachTestStream(t.Context(), nil, nil, true)
	var streamCtx context.Context
	client := &processAttachTestClient{}
	client.attach = func(ctx context.Context, _ *nodev1.AttachProcessRequest) (grpc.ServerStreamingClient[nodev1.ProcessEvent], error) {
		streamCtx = ctx
		stream.ctx = ctx
		stream.events = []*nodev1.ProcessEvent{processAttachTestAttached(processAttachTestProcessID, 0)}
		return stream, nil
	}
	handler := processAttachTestHandler(t, store, client)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	writer, done := processAttachTestBegin(t, handler, ctx, processTestOwner, processTestSandboxID, "7", "")
	writer.waitType(t, api.CommandAttachAttached, 1)
	processAttachTestWaitClosed(t, "receiver barrier", stream.blocked)
	writer.waitDeadlineCleared(t)
	if streamCtx == nil {
		t.Fatal("AttachProcess was not called")
	}
	if _, hasDeadline := streamCtx.Deadline(); hasDeadline {
		t.Error("attach RPC carries a fixed stream deadline")
	}
	cancel()
	processAttachTestWaitDone(t, done)
}

func TestAttachReconnectMidChunkReplaysUnsentBytes(t *testing.T) {
	data := bytes.Repeat([]byte("a"), (2<<20)+7)
	store := newProcessTestStore()
	store.seed(processTestRecord(processAttachTestProcessID, db.CommandRunning))
	client := &processAttachTestClient{}
	client.attach = func(ctx context.Context, _ *nodev1.AttachProcessRequest) (grpc.ServerStreamingClient[nodev1.ProcessEvent], error) {
		return newProcessAttachTestStream(ctx, []*nodev1.ProcessEvent{
			processAttachTestAttached(processAttachTestProcessID, 0),
			processAttachTestChunk(api.CommandAttachStdout, data, int64(len(data))),
		}, nil, true), nil
	}
	clock := newProcessAttachTestClock()
	handler := processAttachTestHandler(t, store, client, func(opts *api.CommandAttachOptions) {
		opts.ReconnectAfter = 10 * time.Minute
		opts.Heartbeat = 10 * time.Second
		opts.Now = clock.Now
	})
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseNow := func() { releaseOnce.Do(func() { close(release) }) }
	gate := newProcessAttachGateWriter(2, release)
	ctx, cancel := context.WithCancel(t.Context())
	done := processAttachTestBeginWriter(t, handler, ctx, processTestOwner, processTestSandboxID, "7", "", gate)
	t.Cleanup(func() {
		releaseNow()
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("mid-chunk attach handler did not return")
		}
	})
	processAttachTestWaitClosed(t, "chunk write gate", gate.blocked)
	clock.Advance(11 * time.Minute)
	releaseNow()
	processAttachTestWaitDone(t, done)

	frames := gate.inner.frames(t)
	if len(frames) < 2 || frames[0].Type != api.CommandAttachAttached {
		t.Fatalf("frames = %+v", frames)
	}
	last := frames[len(frames)-1]
	if last.Type != api.CommandAttachReconnect || last.StdoutOffset == nil {
		t.Fatalf("terminal = %+v, want reconnect with offsets", last)
	}
	sent := int64(0)
	var firstOutput []byte
	for _, frame := range frames {
		if frame.Type == api.CommandAttachStdout {
			if frame.NextOffset == nil || *frame.NextOffset > int64(len(data)) {
				t.Fatalf("stdout nextOffset = %v", frame.NextOffset)
			}
			if *frame.NextOffset == int64(len(data)) {
				t.Fatal("the whole multi-frame chunk was written past the reconnect deadline")
			}
			sent = *frame.NextOffset
			firstOutput = append(firstOutput, frame.Data...)
		}
	}
	if sent == 0 || *last.StdoutOffset != sent {
		t.Fatalf("reconnect offset = %v, last sent = %d", last.StdoutOffset, sent)
	}
	if len(firstOutput) != int(sent) {
		t.Fatalf("first output = %d bytes, want %d", len(firstOutput), sent)
	}

	// Replaying from the reconnect offset sends exactly the unsent bytes.
	client.attach = func(ctx context.Context, request *nodev1.AttachProcessRequest) (grpc.ServerStreamingClient[nodev1.ProcessEvent], error) {
		start := request.GetStdoutOffset()
		if start != sent {
			t.Errorf("replay offset = %d, want %d", start, sent)
		}
		remaining := data[start:]
		return newProcessAttachTestStream(ctx, []*nodev1.ProcessEvent{
			processAttachTestAttachedProcess(processAttachTestProcessID, start, 0, processTestRunningProcess(processAttachTestProcessID, "cmd")),
			processAttachTestChunk(api.CommandAttachStdout, remaining, int64(len(data))),
			processAttachTestExit(processAttachTestProcessID, new(int32(0)), ""),
		}, nil, false), nil
	}
	recorder := processAttachTestServe(t, processAttachTestHandler(t, store, client), processTestOwner, processTestSandboxID, "7", "stdoutOffset="+strconv.FormatInt(sent, 10))
	processAttachTestValidate(t, recorder.Code, recorder.Header(), recorder.Body.Bytes())
	total := append([]byte(nil), firstOutput...)
	for _, frame := range processAttachTestParseFrames(t, recorder.Body.Bytes()) {
		if frame.Type == api.CommandAttachStdout {
			total = append(total, frame.Data...)
		}
	}
	if !bytes.Equal(total, data) {
		t.Errorf("replayed output = %d bytes, want %d", len(total), len(data))
	}
}
func TestAttachExitSizeAndEOFOrdering(t *testing.T) {
	t.Run("exit larger than delivered output is an error", func(t *testing.T) {
		store := newProcessTestStore()
		store.seed(processTestRecord(processAttachTestProcessID, db.CommandRunning))
		late := processTestExitedProcess(processAttachTestProcessID, "cmd", new(int32(0)), "")
		late.StdoutSize = 5
		late.StderrSize = 0
		client := &processAttachTestClient{}
		client.attach = func(ctx context.Context, _ *nodev1.AttachProcessRequest) (grpc.ServerStreamingClient[nodev1.ProcessEvent], error) {
			return newProcessAttachTestStream(ctx, []*nodev1.ProcessEvent{
				processAttachTestAttached(processAttachTestProcessID, 0),
				{Event: &nodev1.ProcessEvent_Exit{Exit: late}},
			}, nil, false), nil
		}
		recorder := processAttachTestServe(t, processAttachTestHandler(t, store, client), processTestOwner, processTestSandboxID, "7", "")
		processAttachTestValidate(t, recorder.Code, recorder.Header(), recorder.Body.Bytes())
		frames := processAttachTestParseFrames(t, recorder.Body.Bytes())
		if len(frames) != 2 || frames[1].Type != api.CommandAttachError {
			t.Fatalf("frames = %+v, want attached + error", frames)
		}
		if _, observe, _ := store.callCounts(); observe != 1 {
			t.Errorf("ObserveCommand calls = %d, want only the attached observation", observe)
		}
	})

	t.Run("exit smaller than delivered output is allowed", func(t *testing.T) {
		store := newProcessTestStore()
		store.seed(processTestRecord(processAttachTestProcessID, db.CommandRunning))
		lagging := processTestExitedProcess(processAttachTestProcessID, "cmd", new(int32(0)), "")
		lagging.StdoutSize = 2
		lagging.StderrSize = 0
		client := &processAttachTestClient{}
		client.attach = func(ctx context.Context, _ *nodev1.AttachProcessRequest) (grpc.ServerStreamingClient[nodev1.ProcessEvent], error) {
			return newProcessAttachTestStream(ctx, []*nodev1.ProcessEvent{
				processAttachTestAttached(processAttachTestProcessID, 0),
				processAttachTestChunk(api.CommandAttachStdout, []byte("hello"), 5),
				{Event: &nodev1.ProcessEvent_Exit{Exit: lagging}},
			}, nil, false), nil
		}
		recorder := processAttachTestServe(t, processAttachTestHandler(t, store, client), processTestOwner, processTestSandboxID, "7", "")
		processAttachTestValidate(t, recorder.Code, recorder.Header(), recorder.Body.Bytes())
		frames := processAttachTestParseFrames(t, recorder.Body.Bytes())
		if frames[len(frames)-1].Type != api.CommandAttachExit {
			t.Fatalf("terminal = %+v, want exit", frames[len(frames)-1])
		}
		record, _ := store.record(processAttachTestProcessID)
		if record.Status != db.CommandExited || record.StdoutBytes != 5 {
			t.Errorf("record = %+v, want exited with 5 delivered bytes", record)
		}
	})

	t.Run("exit without a closing stream keeps heartbeat/reconnect alive", func(t *testing.T) {
		store := newProcessTestStore()
		store.seed(processTestRecord(processAttachTestProcessID, db.CommandRunning))
		stream := newProcessAttachTestStream(t.Context(), nil, nil, true)
		client := &processAttachTestClient{}
		client.attach = func(ctx context.Context, _ *nodev1.AttachProcessRequest) (grpc.ServerStreamingClient[nodev1.ProcessEvent], error) {
			stream.ctx = ctx
			stream.events = []*nodev1.ProcessEvent{
				processAttachTestAttached(processAttachTestProcessID, 0),
				processAttachTestExit(processAttachTestProcessID, new(int32(0)), ""),
			}
			return stream, nil
		}
		handler := processAttachTestHandler(t, store, client, func(opts *api.CommandAttachOptions) {
			opts.ReconnectAfter = 30 * time.Millisecond
			opts.Heartbeat = 10 * time.Second
		})
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		writer, done := processAttachTestBegin(t, handler, ctx, processTestOwner, processTestSandboxID, "7", "")
		writer.waitType(t, api.CommandAttachAttached, 1)
		reconnect := writer.waitType(t, api.CommandAttachReconnect, 1)
		if reconnect.StdoutOffset == nil || *reconnect.StdoutOffset != 0 {
			t.Errorf("reconnect = %+v", reconnect)
		}
		processAttachTestWaitDone(t, done)
		for _, frame := range writer.frames(t) {
			if frame.Type == api.CommandAttachExit {
				t.Error("emitted exit before the node closed the stream")
			}
		}
	})
}

func TestAttachLargeChunkIsSplitIntoBoundedFrames(t *testing.T) {
	store := newProcessTestStore()
	store.seed(processTestRecord(processAttachTestProcessID, db.CommandRunning))
	data := bytes.Repeat([]byte("a"), (1<<20)+64)
	client := &processAttachTestClient{}
	client.attach = func(ctx context.Context, _ *nodev1.AttachProcessRequest) (grpc.ServerStreamingClient[nodev1.ProcessEvent], error) {
		return newProcessAttachTestStream(ctx, []*nodev1.ProcessEvent{
			processAttachTestAttached(processAttachTestProcessID, 0),
			processAttachTestChunk(api.CommandAttachStdout, data, int64(len(data))),
			processAttachTestExit(processAttachTestProcessID, new(int32(0)), ""),
		}, nil, false), nil
	}
	recorder := processAttachTestServe(t, processAttachTestHandler(t, store, client), processTestOwner, processTestSandboxID, "7", "")
	processAttachTestValidate(t, recorder.Code, recorder.Header(), recorder.Body.Bytes())
	frames := processAttachTestParseFrames(t, recorder.Body.Bytes())
	var output []byte
	var lastOffset int64
	dataFrames := 0
	for _, frame := range frames {
		if frame.Type != api.CommandAttachStdout {
			continue
		}
		dataFrames++
		if len(frame.Data) > 1<<20 {
			t.Errorf("frame data = %d bytes, want at most 1 MiB", len(frame.Data))
		}
		if frame.NextOffset == nil || *frame.NextOffset <= lastOffset {
			t.Errorf("nextOffset = %v, last %d", frame.NextOffset, lastOffset)
		}
		lastOffset = *frame.NextOffset
		output = append(output, frame.Data...)
	}
	if dataFrames < 2 {
		t.Errorf("data frames = %d, want the legal chunk split", dataFrames)
	}
	if !bytes.Equal(output, data) || lastOffset != int64(len(data)) {
		t.Errorf("output = %d bytes offset %d, want %d/%d", len(output), lastOffset, len(data), len(data))
	}
}
