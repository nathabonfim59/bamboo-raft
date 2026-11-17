package cli

import (
	"context"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nathabonfim59/bamboo-raft/internal/api"
)

// cliTestDeletion builds a pinned operation for id.
func cliTestDeletion(id, target string, status api.DeletionStatus) api.DeletionOperation {
	return cliTestOperation(id, target, status)
}

func TestDeleteRequiresYesWhenUnattended(t *testing.T) {
	var hits atomic.Int64
	env := cliTestNew(t, func(http.ResponseWriter, *http.Request) { hits.Add(1) }, nil)
	if status := env.run(t, "delete", cliTestSandboxID); status != 2 {
		t.Fatalf("exit status = %d, want 2", status)
	}
	if hits.Load() != 0 {
		t.Fatalf("server received %d requests, want 0", hits.Load())
	}
}

func TestDeleteExplicitJSONRefusesWithoutYesOnTTY(t *testing.T) {
	var hits atomic.Int64
	env := cliTestNew(t, func(http.ResponseWriter, *http.Request) { hits.Add(1) }, func(opts *Options) {
		opts.IsTTY = func(io.Writer) bool { return true }
		opts.Stdin = strings.NewReader("y\n")
	})
	if status := env.run(t, "--json", "delete", cliTestSandboxID); status != 2 {
		t.Fatalf("exit status = %d, want 2", status)
	}
	if hits.Load() != 0 {
		t.Fatalf("server received %d requests, want 0", hits.Load())
	}
}

func TestDeleteSendsConfirmationHeader(t *testing.T) {
	const id = cliTestSandboxID
	env := cliTestNew(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete || r.URL.Path != "/api/v1/sandboxes/"+id {
			t.Errorf("request = %s %s", r.Method, r.URL.Path)
		}
		if got := r.Header.Get("X-Ascii-Confirm-Delete"); got != id {
			t.Errorf("X-Ascii-Confirm-Delete = %q, want %q", got, id)
		}
		cliTestWriteJSON(t, w, http.StatusAccepted, api.NewSandboxDeletingResponse(cliTestDeletion(cliTestOperationID, id, api.DeletionPending)))
	}, nil)
	if status := env.run(t, "delete", id, "--yes"); status != 0 {
		t.Fatalf("exit status = %d, stderr = %s", status, env.stderr)
	}
	lines := cliTestJSONLines(t, env.stdout)
	if lines[0]["type"] != api.TypeSandboxDeleting {
		t.Fatalf("type = %v, want %s", lines[0]["type"], api.TypeSandboxDeleting)
	}
	operation := lines[0]["operation"].(map[string]any)
	if operation["id"] != cliTestOperationID || operation["targetId"] != id {
		t.Fatalf("operation = %v", operation)
	}
}

func TestDeleteWaitPollsToCompletion(t *testing.T) {
	const id = cliTestSandboxID
	var polls atomic.Int64
	env := cliTestNew(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			cliTestWriteJSON(t, w, http.StatusAccepted, api.NewSandboxDeletingResponse(cliTestDeletion(cliTestOperationID, id, api.DeletionPending)))
			return
		}
		status := api.DeletionProcessing
		if polls.Add(1) >= 2 {
			status = api.DeletionCompleted
		}
		cliTestWriteJSON(t, w, http.StatusOK, api.NewDeletionOperationResponse(cliTestDeletion(cliTestOperationID, id, status)))
	}, nil)
	if status := env.run(t, "delete", id, "--yes", "--wait"); status != 0 {
		t.Fatalf("exit status = %d, stderr = %s", status, env.stderr)
	}
	if polls.Load() != 2 {
		t.Fatalf("polls = %d, want 2", polls.Load())
	}
	lines := cliTestJSONLines(t, env.stdout)
	if lines[0]["type"] != api.TypeDeletionOperation {
		t.Fatalf("type = %v, want %s", lines[0]["type"], api.TypeDeletionOperation)
	}
	operation := lines[0]["operation"].(map[string]any)
	if operation["status"] != "completed" {
		t.Fatalf("status = %v, want completed", operation["status"])
	}
}

func TestDeleteWaitBlockedIsFailure(t *testing.T) {
	const id = cliTestSandboxID
	env := cliTestNew(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			cliTestWriteJSON(t, w, http.StatusAccepted, api.NewSandboxDeletingResponse(cliTestDeletion(cliTestOperationID, id, api.DeletionPending)))
			return
		}
		cliTestWriteJSON(t, w, http.StatusOK, api.NewDeletionOperationResponse(cliTestDeletion(cliTestOperationID, id, api.DeletionBlocked)))
	}, nil)
	if status := env.run(t, "delete", id, "--yes", "--wait"); status != 1 {
		t.Fatalf("exit status = %d, want 1", status)
	}
	lines := cliTestJSONLines(t, env.stdout)
	last := lines[len(lines)-1]
	if last["event"] != "error" || last["code"] != "deletion_blocked" {
		t.Fatalf("last event = %v, want deletion_blocked", last)
	}
	if strings.Contains(env.stdout.String(), `"deleted"`) {
		t.Fatalf("stdout claimed success: %q", env.stdout)
	}
}

func TestDeleteWaitTimeout(t *testing.T) {
	const id = cliTestSandboxID
	env := cliTestNew(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			cliTestWriteJSON(t, w, http.StatusAccepted, api.NewSandboxDeletingResponse(cliTestDeletion(cliTestOperationID, id, api.DeletionPending)))
			return
		}
		cliTestWriteJSON(t, w, http.StatusOK, api.NewDeletionOperationResponse(cliTestDeletion(cliTestOperationID, id, api.DeletionProcessing)))
	}, func(opts *Options) {
		opts.WaitTimeout = 20 * time.Millisecond
		opts.PollInterval = 2 * time.Millisecond
	})
	if status := env.run(t, "delete", id, "--yes", "--wait"); status != 1 {
		t.Fatalf("exit status = %d, want 1", status)
	}
	lines := cliTestJSONLines(t, env.stdout)
	last := lines[len(lines)-1]
	if last["event"] != "error" || last["code"] != "timeout" {
		t.Fatalf("last event = %v, want timeout", last)
	}
}

func TestDeleteWaitRejectsDifferentOperation(t *testing.T) {
	const id = cliTestSandboxID
	env := cliTestNew(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			cliTestWriteJSON(t, w, http.StatusAccepted, api.NewSandboxDeletingResponse(cliTestDeletion(cliTestOperationID, id, api.DeletionPending)))
			return
		}
		cliTestWriteJSON(t, w, http.StatusOK, api.NewDeletionOperationResponse(cliTestDeletion(cliTestOperationID2, id, api.DeletionCompleted)))
	}, nil)
	if status := env.run(t, "delete", id, "--yes", "--wait"); status != 1 {
		t.Fatalf("exit status = %d, want 1", status)
	}
	lines := cliTestJSONLines(t, env.stdout)
	last := lines[len(lines)-1]
	if last["event"] != "error" || last["code"] != "invalid_response" {
		t.Fatalf("last event = %v, want invalid_response", last)
	}
}

func TestDeleteInteractivePrompt(t *testing.T) {
	const id = cliTestSandboxID
	env := cliTestNew(t, func(w http.ResponseWriter, r *http.Request) {
		cliTestWriteJSON(t, w, http.StatusAccepted, api.NewSandboxDeletingResponse(cliTestDeletion(cliTestOperationID, id, api.DeletionPending)))
	}, func(opts *Options) {
		opts.IsTTY = func(io.Writer) bool { return true }
		opts.Stdin = strings.NewReader("y\n")
	})
	if status := env.run(t, "delete", id); status != 0 {
		t.Fatalf("exit status = %d, stderr = %s", status, env.stderr)
	}
	if !strings.Contains(env.stderr.String(), "cannot be undone") {
		t.Fatalf("stderr = %q, want the confirmation prompt", env.stderr)
	}
	if !strings.Contains(env.stderr.String(), "Saved snapshots are kept") {
		t.Fatalf("stderr = %q, want the saved-snapshot note", env.stderr)
	}
	if strings.Contains(env.stderr.String(), "and its snapshots") {
		t.Fatalf("stderr = %q, still claims saved snapshots are deleted", env.stderr)
	}
}

// TestDeleteConfirmCancelsBlockedStdin proves a cancelled context unblocks and
// joins the interactive confirmation read.
func TestDeleteConfirmCancelsBlockedStdin(t *testing.T) {
	var hits atomic.Int64
	reader, writer := io.Pipe()
	defer writer.Close()
	barrier := cliTestNewBarrierReader(reader)
	env := cliTestNew(t, func(http.ResponseWriter, *http.Request) { hits.Add(1) }, func(opts *Options) {
		opts.IsTTY = func(io.Writer) bool { return true }
		opts.Stdin = barrier
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan int, 1)
	go func() { done <- env.runContext(ctx, "delete", cliTestSandboxID) }()
	select {
	case <-barrier.started:
	case <-time.After(time.Second):
		t.Fatal("delete did not start reading stdin")
	}
	cancel()
	select {
	case code := <-done:
		if code != 1 {
			t.Fatalf("exit status = %d, want 1", code)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("delete did not return after cancellation")
	}
	if hits.Load() != 0 {
		t.Fatalf("server received %d requests, want 0", hits.Load())
	}
	if !strings.Contains(env.stderr.String(), "cannot be undone") {
		t.Fatal("confirmation prompt was not written")
	}
	closed := make(chan error, 1)
	go func() {
		_, err := writer.Write([]byte("x"))
		closed <- err
	}()
	select {
	case err := <-closed:
		if err == nil {
			t.Fatal("stdin is still open after cancellation")
		}
	case <-time.After(time.Second):
		t.Fatal("write blocked; stdin was not closed")
	}
}
