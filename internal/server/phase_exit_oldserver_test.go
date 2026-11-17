package server_test

// Old-server fallback coverage for the exact dogfood exec invocation. One shim
// removes the optional commands.detached member while forwarding the real
// extension response; a second shim returns 404 for the extension endpoint, the
// v1-only case. Both must keep the legacy streamed path, one command POST,
// and never send a zero timeout.

import (
	"bytes"
	"encoding/json/v2"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"strconv"
	"strings"
	"testing"

	"github.com/nathabonfim59/bamboo-raft/internal/api"
)

// phaseExitInfoShim is a recording-free HTTP shim for legacy-server behavior.
type phaseExitInfoShim struct {
	server *httptest.Server
}

// newPhaseExitInfoShim builds the shim. missing returns a 404 for the extension
// endpoint; otherwise it forwards the real response with commands.detached
// removed.
func newPhaseExitInfoShim(t *testing.T, target string, missing bool) *phaseExitInfoShim {
	t.Helper()
	targetURL, err := url.Parse(target)
	if err != nil {
		t.Fatal("info shim target is malformed")
	}
	proxy := httputil.NewSingleHostReverseProxy(targetURL)
	proxy.ErrorHandler = func(w http.ResponseWriter, _ *http.Request, _ error) {
		w.WriteHeader(http.StatusBadGateway)
	}
	proxy.ModifyResponse = func(response *http.Response) error {
		if missing || response.StatusCode != http.StatusOK ||
			response.Request == nil || response.Request.URL.Path != api.PathInfo {
			return nil
		}
		raw, err := io.ReadAll(io.LimitReader(response.Body, 64<<10))
		if err != nil {
			return err
		}
		response.Body.Close()
		var members map[string]any
		if err := json.Unmarshal(raw, &members); err != nil {
			return err
		}
		if commands, ok := members["commands"].(map[string]any); ok {
			delete(commands, "detached")
		}
		body, err := json.Marshal(members)
		if err != nil {
			return err
		}
		response.Body = io.NopCloser(bytes.NewReader(body))
		response.ContentLength = int64(len(body))
		response.Header.Set("Content-Length", strconv.Itoa(len(body)))
		return nil
	}
	handler := http.Handler(proxy)
	if missing {
		handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == api.PathInfo {
				http.Error(w, "not found", http.StatusNotFound)
				return
			}
			proxy.ServeHTTP(w, r)
		})
	}
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	return &phaseExitInfoShim{server: server}
}

// url returns the shim base URL for --api-url.
func (s *phaseExitInfoShim) url() string { return s.server.URL }

// oldServerFallback runs the exact dogfood argv against both legacy shapes.
func (h *phaseExitHarness) oldServerFallback(t *testing.T, id string) {
	t.Helper()
	t.Run("absent capability", func(t *testing.T) {
		shim := newPhaseExitInfoShim(t, h.apiProxy.url(), false)
		beforeInfo := h.apiProxy.count("bbraftInfo")
		beforeStart := h.apiProxy.count("command")
		beforeAttach := h.apiProxy.count("attachCommand")
		frames, stderr, code := h.dogfoodThrough(t, shim.url(), id)
		if code != 7 {
			t.Fatalf("legacy exec exit = %d, want 7", code)
		}
		h.checkStreamFrames(t, frames, 7, false)
		h.checkStreamOutput(t, frames, "out")
		if len(bytes.TrimSpace(stderr)) != 0 {
			t.Fatal("a valid legacy info response wrote an unexpected fallback note")
		}
		if got := h.apiProxy.count("bbraftInfo"); got != beforeInfo+1 {
			t.Fatalf("legacy metadata requests = %d, want 1", got-beforeInfo)
		}
		if got := h.apiProxy.count("command"); got != beforeStart+1 {
			t.Fatalf("legacy command POSTs = %d, want 1", got-beforeStart)
		}
		if got := h.apiProxy.count("attachCommand"); got != beforeAttach {
			t.Fatalf("legacy exec used %d attach requests, want 0", got-beforeAttach)
		}
		body := h.lastCommand(t)
		if body.Command != e2eTestVMArgvCommand || body.Stream == nil || !*body.Stream ||
			body.TimeoutSeconds == nil || *body.TimeoutSeconds != 7200 || body.Detached != nil {
			t.Fatal("legacy exec request differs")
		}
	})

	t.Run("missing endpoint", func(t *testing.T) {
		shim := newPhaseExitInfoShim(t, h.apiProxy.url(), true)
		beforeInfo := h.apiProxy.count("bbraftInfo")
		beforeStart := h.apiProxy.count("command")
		frames, stderr, code := h.dogfoodThrough(t, shim.url(), id)
		if code != 7 {
			t.Fatalf("missing-endpoint exec exit = %d, want 7", code)
		}
		h.checkStreamFrames(t, frames, 7, false)
		h.checkStreamOutput(t, frames, "out")
		if !strings.Contains(string(stderr), "600-second") ||
			!strings.Contains(string(stderr), api.PathInfo) {
			t.Fatal("missing endpoint did not write the one fallback note")
		}
		if got := h.apiProxy.count("bbraftInfo"); got != beforeInfo {
			t.Fatalf("missing endpoint forwarded %d metadata requests, want 0", got-beforeInfo)
		}
		if got := h.apiProxy.count("command"); got != beforeStart+1 {
			t.Fatalf("missing-endpoint command POSTs = %d, want 1", got-beforeStart)
		}
		body := h.lastCommand(t)
		if body.Command != e2eTestVMArgvCommand || body.Stream == nil || !*body.Stream ||
			body.TimeoutSeconds == nil || *body.TimeoutSeconds != 600 || body.Detached != nil {
			t.Fatal("missing-endpoint exec request differs")
		}
	})
}

// dogfoodThrough runs the exact agent-harness argv form against apiURL.
func (h *phaseExitHarness) dogfoodThrough(t *testing.T, apiURL, id string) ([]api.CommandStreamFrame, []byte, int) {
	t.Helper()
	args := []string{
		"--api-url", apiURL,
		"exec", "--timeout", "0", "--stream", "--json", id, "--",
		"bash", "-lc", e2eExecCode7Command,
	}
	stdout, stderr, code := h.cli.run(t, "legacy dogfood", t.TempDir(), nil, args...)
	h.cli.checkSecrets(t, "legacy dogfood", stdout, stderr)
	return e2eParseCommandFrames(t, "legacy dogfood", stdout), stderr, code
}
