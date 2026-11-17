package api_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/nathabonfim59/bamboo-raft/internal/api"
	"github.com/nathabonfim59/bamboo-raft/internal/db"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

// processCancelTestNoResponse asserts the handler wrote no status, headers or
// body after the request context ended.
func processCancelTestNoResponse(t *testing.T, writer *processAttachTestWriter) {
	t.Helper()
	status, body := writer.snapshot()
	header := writer.headerSnapshot()
	if status != 0 || len(body) != 0 || header.Get("Content-Type") != "" {
		t.Fatalf("cancelled handler wrote status=%d content-type=%q body=%s", status, header.Get("Content-Type"), body)
	}
}

func TestAttachCancellationWins(t *testing.T) {
	t.Run("before entry", func(t *testing.T) {
		store := newProcessTestStore()
		store.seed(processTestRecord(processAttachTestProcessID, db.CommandRunning))
		client := &processAttachTestClient{}
		handler := processAttachTestHandler(t, store, client)
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		writer, done := processAttachTestBegin(t, handler, ctx, processTestOwner, processTestSandboxID, "7", "")
		processAttachTestWaitDone(t, done)
		processCancelTestNoResponse(t, writer)
		if attach, _ := client.counts(); attach != 0 {
			t.Errorf("AttachProcess calls = %d", attach)
		}
		if _, _, reads := store.callCounts(); reads != 0 {
			t.Errorf("CommandRecord calls = %d", reads)
		}
	})

	t.Run("after record read", func(t *testing.T) {
		store := newProcessTestStore()
		store.seed(processTestRecord(processAttachTestProcessID, db.CommandRunning))
		client := &processAttachTestClient{}
		handler := processAttachTestHandler(t, store, client)
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		store.recordHook = cancel
		writer, done := processAttachTestBegin(t, handler, ctx, processTestOwner, processTestSandboxID, "7", "")
		processAttachTestWaitDone(t, done)
		processCancelTestNoResponse(t, writer)
		if attach, _ := client.counts(); attach != 0 {
			t.Errorf("AttachProcess calls = %d", attach)
		}
	})

	t.Run("dial factory cancels before AttachProcess", func(t *testing.T) {
		store := newProcessTestStore()
		store.seed(processTestRecord(processAttachTestProcessID, db.CommandRunning))
		client := &processAttachTestClient{}
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		handler := processAttachTestHandler(t, store, client, func(opts *api.CommandAttachOptions) {
			opts.Clients = func(string) (nodev1.NodeClient, func() error, error) {
				cancel()
				return client, func() error { return nil }, nil
			}
		})
		writer, done := processAttachTestBegin(t, handler, ctx, processTestOwner, processTestSandboxID, "7", "")
		processAttachTestWaitDone(t, done)
		processCancelTestNoResponse(t, writer)
		if attach, _ := client.counts(); attach != 0 {
			t.Errorf("AttachProcess calls = %d", attach)
		}
	})

	t.Run("attached received after cancel is not observed", func(t *testing.T) {
		store := newProcessTestStore()
		store.seed(processTestRecord(processAttachTestProcessID, db.CommandRunning))
		client := &processAttachTestClient{}
		started := make(chan struct{})
		release := make(chan struct{})
		var releaseOnce sync.Once
		releaseNow := func() { releaseOnce.Do(func() { close(release) }) }
		client.attach = func(ctx context.Context, _ *nodev1.AttachProcessRequest) (grpc.ServerStreamingClient[nodev1.ProcessEvent], error) {
			close(started)
			<-release
			return newProcessAttachTestStream(ctx, []*nodev1.ProcessEvent{processAttachTestAttached(processAttachTestProcessID, 0)}, nil, true), nil
		}
		handler := processAttachTestHandler(t, store, client)
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		writer, done := processAttachTestBegin(t, handler, ctx, processTestOwner, processTestSandboxID, "7", "")
		t.Cleanup(func() {
			releaseNow()
			cancel()
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Error("cancelled attach handler did not return")
			}
		})
		processAttachTestWaitClosed(t, "AttachProcess start", started)
		cancel()
		releaseNow()
		processAttachTestWaitDone(t, done)
		processCancelTestNoResponse(t, writer)
		if _, observe, _ := store.callCounts(); observe != 0 {
			t.Errorf("ObserveCommand calls after cancellation = %d", observe)
		}
	})
}

func TestStopCancellationWins(t *testing.T) {
	t.Run("before entry", func(t *testing.T) {
		store := newProcessTestStore()
		store.seed(processTestRecord(processAttachTestProcessID, db.CommandRunning))
		client := &processAttachTestClient{}
		handler := processAttachTestStopHandler(t, store, client)
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		writer := newProcessAttachTestWriter()
		done := processStopTestBegin(t, handler, ctx, processTestOwner, processTestSandboxID, "7", writer)
		processAttachTestWaitDone(t, done)
		processCancelTestNoResponse(t, writer)
		if _, kill := client.counts(); kill != 0 {
			t.Errorf("KillProcess calls = %d", kill)
		}
	})

	t.Run("terminal shortcut after record read", func(t *testing.T) {
		store := newProcessTestStore()
		store.seed(processTestExitedRecord(processAttachTestProcessID, new(int32(0))))
		client := &processAttachTestClient{}
		handler := processAttachTestStopHandler(t, store, client)
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		store.recordHook = cancel
		writer := newProcessAttachTestWriter()
		done := processStopTestBegin(t, handler, ctx, processTestOwner, processTestSandboxID, "7", writer)
		processAttachTestWaitDone(t, done)
		processCancelTestNoResponse(t, writer)
		if _, kill := client.counts(); kill != 0 {
			t.Errorf("KillProcess calls = %d", kill)
		}
	})

	t.Run("dial factory cancels before KillProcess", func(t *testing.T) {
		store := newProcessTestStore()
		store.seed(processTestRecord(processAttachTestProcessID, db.CommandRunning))
		client := &processAttachTestClient{}
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		handler := processAttachTestStopHandler(t, store, client, func(opts *api.CommandsOptions) {
			opts.Clients = func(string) (nodev1.NodeClient, func() error, error) {
				cancel()
				return client, func() error { return nil }, nil
			}
		})
		writer := newProcessAttachTestWriter()
		done := processStopTestBegin(t, handler, ctx, processTestOwner, processTestSandboxID, "7", writer)
		processAttachTestWaitDone(t, done)
		processCancelTestNoResponse(t, writer)
		if _, kill := client.counts(); kill != 0 {
			t.Errorf("KillProcess calls = %d", kill)
		}
	})

	t.Run("after kill before observation", func(t *testing.T) {
		store := newProcessTestStore()
		store.seed(processTestRecord(processAttachTestProcessID, db.CommandRunning))
		client := &processAttachTestClient{kill: processStopTestKill(processTestExitedProcess(processAttachTestProcessID, "cmd", new(int32(0)), ""), true)}
		handler := processAttachTestStopHandler(t, store, client)
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		store.observeHook = cancel
		writer := newProcessAttachTestWriter()
		done := processStopTestBegin(t, handler, ctx, processTestOwner, processTestSandboxID, "7", writer)
		processAttachTestWaitDone(t, done)
		processCancelTestNoResponse(t, writer)
		if _, kill := client.counts(); kill != 1 {
			t.Errorf("KillProcess calls = %d, want 1", kill)
		}
	})
}
