package server_test

// Focused regression for the real cut proxy: the cut must abort the client
// body without a handler panic, and must capture the committed stdout cursor.
// The target is a plain httptest stream, so this needs no PostgreSQL, node or
// daemon and runs in the normal package suite.

import (
	"context"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// TestPhaseExitCutProxyAbortsWithoutPanic proves the cut uses an abort path
// that net/http accepts, rather than leaving ReverseProxy to flush a hijacked
// response.
func TestPhaseExitCutProxyAbortsWithoutPanic(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/x-ndjson")
		w.WriteHeader(http.StatusOK)
		for _, line := range []string{
			`{"type":"attached","processId":7,"stdoutOffset":0,"stderrOffset":0}`,
			`{"type":"stdout","data":"first","nextOffset":5}`,
			`{"type":"stdout","data":"second","nextOffset":11}`,
			`{"type":"exit","status":"exited","exitCode":0,"signal":null,"stdoutOffset":11,"stderrOffset":0}`,
		} {
			if _, err := io.WriteString(w, line+"\n"); err != nil {
				return
			}
			w.(http.Flusher).Flush()
		}
	}))
	defer target.Close()

	var errorLog daemonProcessLog
	proxy := newPhaseExitCutProxyWithErrorLog(t, target.URL, 1, log.New(&errorLog, "", 0))

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet,
		proxy.url()+"/api/v1/sandboxes/bx_cut/commands/7/stream?stdoutOffset=0&stderrOffset=0", nil)
	if err != nil {
		t.Fatal("build cut proxy request failed")
	}
	client := &http.Client{Timeout: 5 * time.Second}
	response, err := client.Do(request)
	if err != nil {
		t.Fatalf("cut proxy request failed: %v", err)
	}
	body, readErr := io.ReadAll(response.Body)
	response.Body.Close()
	if readErr == nil {
		t.Fatal("cut proxy left the client body without an error")
	}
	if strings.Contains(string(body), `"type":"exit"`) {
		t.Fatal("cut proxy delivered the terminal exit frame")
	}
	proxy.stop(t)
	captures := proxy.snapshots()
	if len(captures) != 1 || !captures[0].cut {
		t.Fatal("cut proxy did not record its one cut capture")
	}
	cursor, ok := phaseExitFirstStdoutCursor(t, captures[0].body)
	if !ok || cursor != 5 {
		t.Fatalf("cut capture cursor = %d (present %v), want 5", cursor, ok)
	}
	if strings.Contains(errorLog.String(), "panic") ||
		strings.Contains(errorLog.String(), "FlushError") {
		t.Fatal("cut proxy sent a panic to the server error log")
	}
}

// TestPhaseExitCutWriterRaisesAbort pins the writer-level abort sentinel. This
// is a deterministic fail-before signal independent of server recovery-log
// timing: the previous implementation returned io.ErrClosedPipe here.
func TestPhaseExitCutWriterRaisesAbort(t *testing.T) {
	capture := &phaseExitAttachCapture{}
	writer := &phaseExitCutWriter{
		ResponseWriter: httptest.NewRecorder(),
		capture:        capture,
		cutOutputs:     1,
	}
	defer func() {
		if got := recover(); got != http.ErrAbortHandler {
			t.Fatalf("cut Write panic = %v, want http.ErrAbortHandler", got)
		}
		if !capture.cut {
			t.Fatal("cut Write did not mark the capture")
		}
	}()
	_, err := writer.Write([]byte(
		`{"type":"attached","processId":7,"stdoutOffset":0,"stderrOffset":0}` + "\n" +
			`{"type":"stdout","data":"first","nextOffset":5}` + "\n" +
			`{"type":"exit","status":"exited","exitCode":0,"signal":null,"stdoutOffset":5,"stderrOffset":0}` + "\n",
	))
	t.Fatalf("cut Write returned %v, want http.ErrAbortHandler panic", err)
}
