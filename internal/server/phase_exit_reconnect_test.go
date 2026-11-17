package server_test

// Controlled attach reconnect coverage. A local HTTP proxy cuts the first
// attach response after one real stdout frame (the output is intentionally
// larger than the 1 MiB server frame bound), closes the client connection and
// records the reconnect request offsets. The real CLI must resume at the last
// committed raw cursor; the test proves the combined output is byte-exact and
// that both the cut and reconnect streams validate as pinned NDJSON.

import (
	"bytes"
	"encoding/json/v2"
	"log"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nathabonfim59/bamboo-raft/internal/api"
	"github.com/nathabonfim59/bamboo-raft/internal/compat"
)

// phaseExitReconnectCommand writes deterministic output larger than the 1 MiB
// attach frame bound, so at least two stdout frames are required.
const phaseExitReconnectCommand = "line=$(printf '%100s' '' | tr ' ' x); i=0; while [ $i -lt 12000 ]; do printf '%s\\n' \"$line\"; i=$((i+1)); done"

// phaseExitCutProxy is an HTTP-aware cut proxy for one attach reader.
type phaseExitCutProxy struct {
	server     *httptest.Server
	proxy      *httputil.ReverseProxy
	cutOutputs int

	wg sync.WaitGroup

	mu       sync.Mutex
	claimed  bool
	captures []*phaseExitAttachCapture
	requests []phaseExitAttachRequest
}

// phaseExitAttachRequest is the offset query the CLI sent on one attach.
type phaseExitAttachRequest struct {
	stdoutOffset int64
	stderrOffset int64
	valid        bool
}

// phaseExitAttachCapture is the decoded response body one attach connection
// received plus whether the proxy deliberately cut it. Each capture has its
// own lock because the proxy handler writes while the test snapshots.
type phaseExitAttachCapture struct {
	mu   sync.Mutex
	body bytes.Buffer
	cut  bool
}

func (c *phaseExitAttachCapture) write(data []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.body.Write(data)
}

func (c *phaseExitAttachCapture) markCut() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cut = true
}

func (c *phaseExitAttachCapture) snapshot() ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return bytes.Clone(c.body.Bytes()), c.cut
}

// phaseExitAttachSnapshot is the test-side copy of one capture.
type phaseExitAttachSnapshot struct {
	body []byte
	cut  bool
}

// newPhaseExitCutProxy targets the real API address and cuts the first attach
// response after cutOutputs complete stdout frames.
func newPhaseExitCutProxy(t *testing.T, target string, cutOutputs int) *phaseExitCutProxy {
	t.Helper()
	return newPhaseExitCutProxyWithErrorLog(t, target, cutOutputs, nil)
}

// newPhaseExitCutProxyWithErrorLog builds the proxy with an optional server
// error log. The focused regression uses a captured log to prove a cut never
// reports a handler panic.
func newPhaseExitCutProxyWithErrorLog(t *testing.T, target string, cutOutputs int, errorLog *log.Logger) *phaseExitCutProxy {
	t.Helper()
	targetURL, err := url.Parse(target)
	if err != nil {
		t.Fatal("cut proxy target is malformed")
	}
	p := &phaseExitCutProxy{cutOutputs: cutOutputs}
	proxy := httputil.NewSingleHostReverseProxy(targetURL)
	proxy.FlushInterval = -1
	proxy.ErrorHandler = func(http.ResponseWriter, *http.Request, error) {
		// The cut intentionally aborts one response; the handler must not
		// append a second status or error body.
	}
	p.proxy = proxy
	p.server = httptest.NewUnstartedServer(http.HandlerFunc(p.serveHTTP))
	if errorLog != nil {
		p.server.Config.ErrorLog = errorLog
	}
	p.server.Start()
	t.Cleanup(p.server.Close)
	return p
}

// url returns the proxy base URL for --api-url.
func (p *phaseExitCutProxy) url() string { return p.server.URL }

// serveHTTP captures every attach body and request offsets, and cuts only the
// first connection after its first committed stdout frame.
func (p *phaseExitCutProxy) serveHTTP(w http.ResponseWriter, r *http.Request) {
	if !strings.Contains(r.URL.Path, "/commands/") || !strings.HasSuffix(r.URL.Path, "/stream") {
		p.proxy.ServeHTTP(w, r)
		return
	}
	p.wg.Add(1)
	defer p.wg.Done()

	clientOut, outErr := strconv.ParseInt(r.URL.Query().Get("stdoutOffset"), 10, 64)
	clientErr, errErr := strconv.ParseInt(r.URL.Query().Get("stderrOffset"), 10, 64)
	request := phaseExitAttachRequest{
		stdoutOffset: clientOut,
		stderrOffset: clientErr,
		valid:        outErr == nil && errErr == nil,
	}
	capture := &phaseExitAttachCapture{}
	p.mu.Lock()
	first := !p.claimed
	if first {
		p.claimed = true
	}
	p.captures = append(p.captures, capture)
	p.requests = append(p.requests, request)
	p.mu.Unlock()

	if first {
		// Even if hijacking is unavailable, this makes the handler return a
		// clean EOF to the CLI instead of an idle keep-alive connection.
		w.Header().Set("Connection", "close")
		p.proxy.ServeHTTP(&phaseExitCutWriter{
			ResponseWriter: w,
			capture:        capture,
			cutOutputs:     p.cutOutputs,
		}, r)
		return
	}
	p.proxy.ServeHTTP(&phaseExitCaptureWriter{ResponseWriter: w, capture: capture}, r)
}

// stop closes the frontend and waits for every attach handler and any
// net/http recovery logging before the test snapshots captures or logs.
func (p *phaseExitCutProxy) stop(t *testing.T) {
	t.Helper()
	p.server.Close()
	p.wait(t, 5*time.Second)
}

// waitBounds waits for every attach handler to finish after the CLI exits.
func (p *phaseExitCutProxy) wait(t *testing.T, timeout time.Duration) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(timeout):
		t.Fatal("attach proxy handlers did not finish")
	}
}

// snapshots returns copies of the captured attach bodies.
func (p *phaseExitCutProxy) snapshots() []phaseExitAttachSnapshot {
	p.mu.Lock()
	captures := append([]*phaseExitAttachCapture(nil), p.captures...)
	p.mu.Unlock()
	out := make([]phaseExitAttachSnapshot, len(captures))
	for i, capture := range captures {
		body, cut := capture.snapshot()
		out[i] = phaseExitAttachSnapshot{body: body, cut: cut}
	}
	return out
}

// attachRequests returns copies of the observed attach request offsets.
func (p *phaseExitCutProxy) attachRequests() []phaseExitAttachRequest {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]phaseExitAttachRequest(nil), p.requests...)
}

// phaseExitCaptureWriter records a complete response body.
type phaseExitCaptureWriter struct {
	http.ResponseWriter
	capture *phaseExitAttachCapture
}

// Write records the body and forwards it.
func (w *phaseExitCaptureWriter) Write(data []byte) (int, error) {
	w.capture.write(data)
	return w.ResponseWriter.Write(data)
}

// Unwrap exposes the underlying writer to http.ResponseController.
func (w *phaseExitCaptureWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

// phaseExitCutWriter emits complete NDJSON lines, counting real stdout frames.
// Once cutOutputs stdout frames have been written it closes the client
// connection; the committed cursor is the last written frame's nextOffset.
type phaseExitCutWriter struct {
	http.ResponseWriter
	capture    *phaseExitAttachCapture
	cutOutputs int
	pending    []byte
	writtenOut int
}

// Write buffers input and forwards whole lines until the cut point.
func (w *phaseExitCutWriter) Write(data []byte) (int, error) {
	w.pending = append(w.pending, data...)
	for {
		end := bytes.IndexByte(w.pending, '\n')
		if end < 0 {
			return len(data), nil
		}
		line := w.pending[:end+1]
		w.pending = w.pending[end+1:]
		if _, err := w.ResponseWriter.Write(line); err != nil {
			return len(data), err
		}
		w.capture.write(line)
		if flusher, ok := w.ResponseWriter.(http.Flusher); ok {
			flusher.Flush()
		}
		var frame struct {
			Type string `json:"type"`
		}
		if json.Unmarshal(line, &frame) == nil && frame.Type == string(api.CommandAttachStdout) {
			w.writtenOut++
			if w.writtenOut >= w.cutOutputs {
				w.capture.markCut()
				// Abort the response through net/http's sentinel instead of
				// hijacking here. ReverseProxy flushes its writer after Write
				// returns, so closing underneath it would make that flush
				// target a dead response. ErrAbortHandler closes the client
				// connection without a panic log and gives the client a body
				// error, which is exactly the reconnect trigger.
				panic(http.ErrAbortHandler)
			}
		}
	}
}

// Unwrap exposes the underlying writer to http.ResponseController.
func (w *phaseExitCutWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

// reconnect starts a detached output command, reads it through the cut proxy,
// and proves the CLI reconnects at the last committed cursor without losing or
// duplicating bytes.
func (h *phaseExitHarness) reconnect(t *testing.T, id string) {
	t.Helper()
	started := h.detach(t, id, phaseExitReconnectCommand)
	proxy := newPhaseExitCutProxy(t, "http://"+h.fixture.apiAddr, 1)

	stdout, stderr, code := h.cli.run(t, "attach reconnect", t.TempDir(), nil,
		"--api-url", proxy.url(), "exec", id, "--attach",
		strconv.FormatUint(uint64(started.ProcessID), 10), "--json")
	h.cli.checkSecrets(t, "attach reconnect", stdout, stderr)
	if code != 0 {
		t.Fatalf("reconnect attach exit = %d, want 0", code)
	}
	proxy.stop(t)

	frames := e2eParseCommandFrames(t, "attach reconnect", stdout)
	h.checkStreamFrames(t, frames, 0, false)
	var output strings.Builder
	for _, frame := range frames {
		if frame.Type == api.CommandFrameStdout {
			output.WriteString(frame.Data)
		}
	}
	want := phaseExitReconnectOutput()
	if got := output.String(); got != want {
		t.Fatalf("reconnect output length = %d, want %d", len(got), len(want))
	}

	captures := proxy.snapshots()
	if len(captures) < 2 {
		t.Fatalf("attach reconnect used %d connections, want at least 2", len(captures))
	}
	if !captures[0].cut {
		t.Fatal("cut proxy did not cut the first attach response")
	}
	firstCursor, firstHasOutput := phaseExitFirstStdoutCursor(t, captures[0].body)
	if !firstHasOutput || firstCursor <= 0 || firstCursor >= int64(len(want)) {
		t.Fatalf("first committed cursor = %d, want 0 < cursor < %d", firstCursor, len(want))
	}
	requests := proxy.attachRequests()
	if len(requests) < 2 || !requests[0].valid || !requests[1].valid {
		t.Fatal("attach request offsets are missing")
	}
	if requests[0].stdoutOffset != 0 || requests[1].stdoutOffset != firstCursor ||
		requests[1].stderrOffset != 0 {
		t.Fatalf("reconnect offsets = %+v, want stdoutOffset %d", requests, firstCursor)
	}

	header := http.Header{"Content-Type": []string{"application/x-ndjson"}}
	for _, check := range []struct {
		name      string
		validator *compat.Validator
	}{
		{name: "v1", validator: h.v1},
	} {
		if err := check.validator.NDJSON("attachCommand", http.StatusOK, header, captures[0].body); err != nil {
			t.Errorf("%s cut reconnect frames failed wire validation", check.name)
		}
		if err := check.validator.NDJSON("attachCommand", http.StatusOK, header, captures[len(captures)-1].body); err != nil {
			t.Errorf("%s reconnect frames failed wire validation", check.name)
		}
	}
	stopped := h.kill(t, id, started.ProcessID)
	if stopped.Signaled {
		t.Fatal("reconnect process was still running after attach ended")
	}
}

// phaseExitFirstStdoutCursor returns the last stdout nextOffset in one captured
// attach body and whether any stdout frame was present.
func phaseExitFirstStdoutCursor(t *testing.T, body []byte) (int64, bool) {
	t.Helper()
	var cursor int64
	found := false
	for _, line := range bytes.Split(body, []byte{'\n'}) {
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		var frame api.CommandAttachFrame
		if err := json.Unmarshal(line, &frame); err != nil {
			t.Fatal("cut capture contains a malformed frame")
		}
		if frame.Type == api.CommandAttachStdout && frame.NextOffset != nil {
			cursor = *frame.NextOffset
			found = true
		}
	}
	return cursor, found
}

// phaseExitReconnectOutput is the exact stdout the command writes.
func phaseExitReconnectOutput() string {
	return strings.Repeat(strings.Repeat("x", 100)+"\n", 12000)
}
