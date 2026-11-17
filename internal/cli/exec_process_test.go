package cli

import (
	"context"
	"encoding/json/v2"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

// execProcessTestClient builds the real client against a test handler.
func execProcessTestClient(t *testing.T, handler http.HandlerFunc) execProcessAPI {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	client, err := NewClient(server.URL, "test-key", server.Client())
	if err != nil {
		t.Fatal(err)
	}
	return execProcessAPI{client: client}
}

// execProcessTestStarted builds a pinned command.started body.
func execProcessTestStarted(command string) map[string]any {
	return map[string]any{
		"ok":        true,
		"type":      "command.started",
		"success":   true,
		"commandId": "cmd_test",
		"processId": 7,
		"pid":       123,
		"command":   command,
		"startedAt": "2026-01-02T03:04:05Z",
	}
}

// TestExecProcessStartDetached pins the request and the success envelope.
func TestExecProcessStartDetached(t *testing.T) {
	api := execProcessTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("method = %s, want POST", r.Method)
		}
		if got := r.URL.EscapedPath(); got != "/api/v1/sandboxes/bx_23456789/commands" {
			t.Errorf("path = %q", got)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer test-key" {
			t.Errorf("Authorization = %q", got)
		}
		if got := r.Header.Get("Content-Type"); got != "application/json" {
			t.Errorf("Content-Type = %q", got)
		}
		raw, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read body: %v", err)
			return
		}
		var body map[string]any
		if err := json.Unmarshal(raw, &body); err != nil {
			t.Errorf("decode body: %v", err)
			return
		}
		if body["command"] != "echo hi" || body["cwd"] != "/home/user/src" || body["detached"] != true {
			t.Errorf("body = %#v", body)
		}
		for _, absent := range []string{"timeoutSeconds", "stream"} {
			if _, ok := body[absent]; ok {
				t.Errorf("detached request sent %q", absent)
			}
		}
		cliTestWriteJSON(t, w, http.StatusOK, execProcessTestStarted("echo hi"))
	})
	started, err := api.startDetached(t.Context(), "bx_23456789", "echo hi", "/home/user/src")
	if err != nil {
		t.Fatal(err)
	}
	if started.ProcessID != 7 || started.CommandID != "cmd_test" || started.Command != "echo hi" || started.StartedAt.IsZero() {
		t.Fatalf("started = %+v", started)
	}
}

// TestExecProcessStartDetachedRejections covers malformed success envelopes.
func TestExecProcessStartDetachedRejections(t *testing.T) {
	cases := map[string]map[string]any{
		"not ok":            {"ok": false},
		"wrong type":        {"type": "sandbox.info"},
		"not success":       {"success": false},
		"zero processId":    {"processId": 0},
		"float processId":   {"processId": 1.5},
		"missing command":   {"command": nil},
		"wrong command":     {"command": "other"},
		"missing startedAt": {"startedAt": nil},
	}
	for name, override := range cases {
		t.Run(name, func(t *testing.T) {
			body := execProcessTestStarted("echo hi")
			for key, value := range override {
				body[key] = value
			}
			api := execProcessTestClient(t, func(w http.ResponseWriter, _ *http.Request) {
				cliTestWriteJSON(t, w, http.StatusOK, body)
			})
			if _, err := api.startDetached(t.Context(), "bx_23456789", "echo hi", ""); err == nil {
				t.Fatal("malformed started envelope accepted")
			}
		})
	}
}

// TestExecProcessStartDetachedError checks the pinned error path.
func TestExecProcessStartDetachedError(t *testing.T) {
	api := execProcessTestClient(t, func(w http.ResponseWriter, _ *http.Request) {
		cliTestWriteJSON(t, w, http.StatusConflict,
			cliTestErrorEnvelope(http.StatusConflict, "sandbox_not_ready", "not ready"))
	})
	_, err := api.startDetached(t.Context(), "bx_23456789", "echo hi", "")
	apiErr, ok := errors.AsType[*APIError](err)
	if !ok || apiErr.Status != http.StatusConflict || apiErr.Code != "sandbox_not_ready" {
		t.Fatalf("err = %v", err)
	}
}

// TestExecProcessStatus checks both the tail and the offset request shapes plus
// the success and malformed envelopes.
func TestExecProcessStatus(t *testing.T) {
	statusBody := func(status string, running bool, exitCode any, success bool) map[string]any {
		return map[string]any{
			"ok": true, "type": "command.status", "success": success,
			"processId": 7, "status": status, "running": running,
			"exitCode": exitCode, "stdout": "out", "stderr": "err",
		}
	}
	t.Run("tail", func(t *testing.T) {
		api := execProcessTestClient(t, func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodGet {
				t.Errorf("method = %s", r.Method)
			}
			if got := r.URL.EscapedPath(); got != "/api/v1/sandboxes/bx_23456789/commands/7" {
				t.Errorf("path = %q", got)
			}
			if got := r.URL.Query().Get("tailBytes"); got != "65536" {
				t.Errorf("tailBytes = %q", got)
			}
			if _, ok := r.URL.Query()["stdoutOffset"]; ok {
				t.Error("tail request sent an offset")
			}
			cliTestWriteJSON(t, w, http.StatusOK, statusBody("exited", false, 0, true))
		})
		status, err := api.status(t.Context(), "bx_23456789", 7, url.Values{"tailBytes": {"65536"}})
		if err != nil {
			t.Fatal(err)
		}
		if status.Status != "exited" || status.ExitCode == nil || *status.ExitCode != 0 || status.Stdout != "out" {
			t.Fatalf("status = %+v", status)
		}
	})
	t.Run("offset", func(t *testing.T) {
		api := execProcessTestClient(t, func(w http.ResponseWriter, r *http.Request) {
			if got := r.URL.Query().Get("stdoutOffset"); got != "3" {
				t.Errorf("stdoutOffset = %q", got)
			}
			body := statusBody("running", true, nil, false)
			body["stdoutOffset"], body["stdoutNextOffset"] = 3, 6
			body["stderrOffset"], body["stderrNextOffset"] = 0, 0
			cliTestWriteJSON(t, w, http.StatusOK, body)
		})
		status, err := api.status(t.Context(), "bx_23456789", 7, url.Values{
			"stdoutOffset": {"3"}, "stderrOffset": {"0"},
		})
		if err != nil {
			t.Fatal(err)
		}
		if status.Running != true || status.StdoutOffset == nil || *status.StdoutOffset != 3 {
			t.Fatalf("status = %+v", status)
		}
	})
	// M2 reports success=false for running, lost, signal and non-zero exits
	// while the status read itself succeeded.
	accept := map[string]map[string]any{
		"running success false":         statusBody("running", true, nil, false),
		"lost success false":            statusBody("lost", false, nil, false),
		"exited non-zero success false": statusBody("exited", false, 7, false),
		"exited signal success false":   statusBody("exited", false, nil, false),
	}
	accept["exited signal success false"]["signal"] = "SIGTERM"
	for name, body := range accept {
		t.Run(name, func(t *testing.T) {
			api := execProcessTestClient(t, func(w http.ResponseWriter, _ *http.Request) {
				cliTestWriteJSON(t, w, http.StatusOK, body)
			})
			status, err := api.status(t.Context(), "bx_23456789", 7, nil)
			if err != nil {
				t.Fatal(err)
			}
			if status.Success {
				t.Error("success=false was not retained")
			}
		})
	}

	rejections := map[string]map[string]any{
		"other process":         statusBody("running", true, nil, false),
		"unknown status":        statusBody("starting", false, nil, false),
		"running mismatch":      statusBody("running", false, nil, false),
		"running with exit":     statusBody("running", true, 0, false),
		"lost is not running":   statusBody("lost", true, nil, false),
		"lost with exit":        statusBody("lost", false, 0, false),
		"exited without result": statusBody("exited", false, nil, true),
		"exit code too high":    statusBody("exited", false, 300, false),
		"exit code negative":    statusBody("exited", false, -1, false),
		"null stdout":           statusBody("running", true, nil, false),
		"null success":          statusBody("running", true, nil, false),
		"missing success":       statusBody("running", true, nil, false),
	}
	rejections["other process"]["processId"] = 8
	rejections["null stdout"]["stdout"] = nil
	rejections["null success"]["success"] = nil
	delete(rejections["missing success"], "success")
	both := statusBody("exited", false, 0, false)
	both["signal"] = "SIGTERM"
	rejections["exited with code and signal"] = both
	for name, body := range rejections {
		t.Run(name, func(t *testing.T) {
			api := execProcessTestClient(t, func(w http.ResponseWriter, _ *http.Request) {
				cliTestWriteJSON(t, w, http.StatusOK, body)
			})
			if _, err := api.status(t.Context(), "bx_23456789", 7, nil); err == nil {
				t.Fatal("malformed status envelope accepted")
			}
		})
	}
	t.Run("backwards offsets", func(t *testing.T) {
		body := statusBody("running", true, nil, false)
		body["stdoutOffset"], body["stdoutNextOffset"] = 6, 3
		api := execProcessTestClient(t, func(w http.ResponseWriter, _ *http.Request) {
			cliTestWriteJSON(t, w, http.StatusOK, body)
		})
		if _, err := api.status(t.Context(), "bx_23456789", 7, nil); err == nil {
			t.Fatal("backwards offset window accepted")
		}
	})
}

// TestExecProcessKill checks the stop request and the idempotent answer.
func TestExecProcessKill(t *testing.T) {
	record := map[string]any{
		"commandId": "cmd_test", "sandboxId": "bx_23456789", "command": "sleep 1",
		"cwd": nil, "mode": "detached", "processId": 7, "status": "killed",
		"exitCode": nil, "signal": "SIGTERM", "stdoutBytes": 0, "stderrBytes": 0,
		"outputTruncated": false, "startedAt": "2026-01-02T03:04:05Z", "finishedAt": nil,
	}
	stopped := map[string]any{
		"ok": true, "type": "command.stopped", "processId": 7,
		"running": false, "signaled": false, "command": record,
	}
	api := execProcessTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			t.Errorf("method = %s", r.Method)
		}
		if got := r.URL.EscapedPath(); got != "/api/v1/sandboxes/bx_23456789/commands/7" {
			t.Errorf("path = %q", got)
		}
		cliTestWriteJSON(t, w, http.StatusOK, stopped)
	})
	response, err := api.kill(t.Context(), "bx_23456789", 7)
	if err != nil {
		t.Fatal(err)
	}
	if response.Signaled || response.ProcessID != 7 || response.Command == nil ||
		response.Command.CommandID != "cmd_test" {
		t.Fatalf("response = %+v", response)
	}

	t.Run("null command", func(t *testing.T) {
		body := map[string]any{
			"ok": true, "type": "command.stopped", "processId": 7,
			"running": false, "signaled": true, "command": nil,
		}
		api := execProcessTestClient(t, func(w http.ResponseWriter, _ *http.Request) {
			cliTestWriteJSON(t, w, http.StatusOK, body)
		})
		if _, err := api.kill(t.Context(), "bx_23456789", 7); err != nil {
			t.Fatal(err)
		}
	})

	cloneRecord := func() map[string]any {
		clone := make(map[string]any, len(record))
		for key, value := range record {
			clone[key] = value
		}
		return clone
	}
	stopBody := func(record map[string]any) map[string]any {
		return map[string]any{
			"ok": true, "type": "command.stopped", "processId": 7,
			"running": false, "signaled": true, "command": record,
		}
	}
	valid := map[string]func(map[string]any){
		"omitted optional processId": func(r map[string]any) { delete(r, "processId") },
		"null optional processId":    func(r map[string]any) { r["processId"] = nil },
		"mode process":               func(r map[string]any) { r["mode"] = "process" },
		"mode sync":                  func(r map[string]any) { r["mode"] = "sync" },
		"mode stream":                func(r map[string]any) { r["mode"] = "stream" },
	}
	for name, mutate := range valid {
		t.Run(name, func(t *testing.T) {
			body := cloneRecord()
			mutate(body)
			api := execProcessTestClient(t, func(w http.ResponseWriter, _ *http.Request) {
				cliTestWriteJSON(t, w, http.StatusOK, stopBody(body))
			})
			response, err := api.kill(t.Context(), "bx_23456789", 7)
			if err != nil {
				t.Fatal(err)
			}
			if response.Command == nil {
				t.Fatal("valid record was dropped")
			}
		})
	}

	invalid := map[string]func(map[string]any){
		"foreign sandbox":        func(r map[string]any) { r["sandboxId"] = "bx_other" },
		"running record":         func(r map[string]any) { r["status"] = "running" },
		"unknown mode":           func(r map[string]any) { r["mode"] = "batch" },
		"wrong optional process": func(r map[string]any) { r["processId"] = 8 },
		"missing commandId":      func(r map[string]any) { delete(r, "commandId") },
		"missing command":        func(r map[string]any) { delete(r, "command") },
		"missing sandboxId":      func(r map[string]any) { delete(r, "sandboxId") },
		"missing mode":           func(r map[string]any) { delete(r, "mode") },
		"missing status":         func(r map[string]any) { delete(r, "status") },
		"missing stdoutBytes":    func(r map[string]any) { delete(r, "stdoutBytes") },
		"negative stdoutBytes":   func(r map[string]any) { r["stdoutBytes"] = -1 },
		"missing stderrBytes":    func(r map[string]any) { delete(r, "stderrBytes") },
		"negative stderrBytes":   func(r map[string]any) { r["stderrBytes"] = -1 },
		"missing truncated flag": func(r map[string]any) { delete(r, "outputTruncated") },
		"non-bool truncated flag": func(r map[string]any) {
			r["outputTruncated"] = "no"
		},
		"missing startedAt":  func(r map[string]any) { delete(r, "startedAt") },
		"invalid startedAt":  func(r map[string]any) { r["startedAt"] = "not-a-date" },
		"non-string command": func(r map[string]any) { r["command"] = 5 },
		"bad exitCode":       func(r map[string]any) { r["exitCode"] = 300 },
		"non-string cwd":     func(r map[string]any) { r["cwd"] = 5 },
		"non-string signal":  func(r map[string]any) { r["signal"] = 5 },
	}
	for name, mutate := range invalid {
		t.Run(name, func(t *testing.T) {
			body := cloneRecord()
			mutate(body)
			api := execProcessTestClient(t, func(w http.ResponseWriter, _ *http.Request) {
				cliTestWriteJSON(t, w, http.StatusOK, stopBody(body))
			})
			if _, err := api.kill(t.Context(), "bx_23456789", 7); err == nil {
				t.Fatal("malformed stop record accepted")
			}
		})
	}

	t.Run("non-object command", func(t *testing.T) {
		body := stopBody(record)
		body["command"] = "nope"
		api := execProcessTestClient(t, func(w http.ResponseWriter, _ *http.Request) {
			cliTestWriteJSON(t, w, http.StatusOK, body)
		})
		if _, err := api.kill(t.Context(), "bx_23456789", 7); err == nil {
			t.Fatal("non-object record accepted")
		}
	})
	t.Run("missing command member", func(t *testing.T) {
		body := stopBody(record)
		delete(body, "command")
		api := execProcessTestClient(t, func(w http.ResponseWriter, _ *http.Request) {
			cliTestWriteJSON(t, w, http.StatusOK, body)
		})
		if _, err := api.kill(t.Context(), "bx_23456789", 7); err == nil {
			t.Fatal("missing command member accepted")
		}
	})
	t.Run("top-level process mismatch", func(t *testing.T) {
		body := stopBody(record)
		body["processId"] = 8
		api := execProcessTestClient(t, func(w http.ResponseWriter, _ *http.Request) {
			cliTestWriteJSON(t, w, http.StatusOK, body)
		})
		if _, err := api.kill(t.Context(), "bx_23456789", 7); err == nil {
			t.Fatal("mismatched top-level process accepted")
		}
	})
	t.Run("top-level running", func(t *testing.T) {
		body := stopBody(record)
		body["running"] = true
		api := execProcessTestClient(t, func(w http.ResponseWriter, _ *http.Request) {
			cliTestWriteJSON(t, w, http.StatusOK, body)
		})
		if _, err := api.kill(t.Context(), "bx_23456789", 7); err == nil {
			t.Fatal("running stop accepted")
		}
	})
}

// TestExecProcessAttach checks the request and the media-type gate.
func TestExecProcessAttach(t *testing.T) {
	api := execProcessTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.EscapedPath(); got != "/api/v1/sandboxes/bx_23456789/commands/7/stream" {
			t.Errorf("path = %q", got)
		}
		if got := r.URL.Query().Get("stdoutOffset"); got != "3" {
			t.Errorf("stdoutOffset = %q", got)
		}
		if got := r.URL.Query().Get("stderrOffset"); got != "5" {
			t.Errorf("stderrOffset = %q", got)
		}
		if got := r.Header.Get("Accept"); got != "application/x-ndjson" {
			t.Errorf("Accept = %q", got)
		}
		w.Header().Set("Content-Type", "application/x-ndjson")
		_, _ = io.WriteString(w, "{\"type\":\"attached\",\"processId\":7,\"stdoutOffset\":3,\"stderrOffset\":5}\n")
	})
	resp, err := api.attach(t.Context(), "bx_23456789", 7, 3, 5)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(raw), `"attached"`) {
		t.Fatalf("body = %q", raw)
	}

	t.Run("wrong media type", func(t *testing.T) {
		api := execProcessTestClient(t, func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, "{}")
		})
		if _, err := api.attach(t.Context(), "bx_23456789", 7, 0, 0); err == nil {
			t.Fatal("wrong media type accepted")
		}
	})
	t.Run("error envelope", func(t *testing.T) {
		api := execProcessTestClient(t, func(w http.ResponseWriter, _ *http.Request) {
			cliTestWriteJSON(t, w, http.StatusNotFound, cliTestErrorEnvelope(http.StatusNotFound, "not_found", "missing"))
		})
		_, err := api.attach(t.Context(), "bx_23456789", 7, 0, 0)
		apiErr, ok := errors.AsType[*APIError](err)
		if !ok || apiErr.Status != http.StatusNotFound {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("context cancellation", func(t *testing.T) {
		api := execProcessTestClient(t, func(w http.ResponseWriter, r *http.Request) {
			<-r.Context().Done()
		})
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		if _, err := api.attach(ctx, "bx_23456789", 7, 0, 0); err == nil {
			t.Fatal("cancelled attach returned no error")
		}
	})
}

// TestExecProcessBounds pins the response bounds and the bounded reader.
func TestExecProcessBounds(t *testing.T) {
	if execStartMaxBytes < 8<<20 || execKillMaxBytes < 8<<20 {
		t.Fatalf("start/kill bounds = %d/%d", execStartMaxBytes, execKillMaxBytes)
	}
	if execStatusMaxBytes < 96<<20 {
		t.Fatalf("status bound = %d, want at least 96 MiB", execStatusMaxBytes)
	}
	if _, err := execReadLimited(strings.NewReader(strings.Repeat("x", 65)), 64); err == nil {
		t.Fatal("oversized body accepted")
	}
}
