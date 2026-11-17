package apierr

import (
	"encoding/json/v2"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// decode reads the response body as one JSON object.
func decode(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("response is not a JSON object: %v\n%s", err, rec.Body.String())
	}
	return body
}

func TestWriteErrorEnvelope(t *testing.T) {
	rec := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/api/v1/sandboxes", nil)
	WriteError(rec, r, New(http.StatusConflict, CodeSandboxNotReady, "sandbox is not ready"))

	if rec.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("content type = %q", ct)
	}
	body := decode(t, rec)

	// The pinned ErrorEnvelope, exact field set and values. requestId is
	// generated even without the middleware, so it is nonempty here.
	reqID, ok := body["requestId"].(string)
	if !ok || !strings.HasPrefix(reqID, "req_") {
		t.Fatalf("requestId = %v", body["requestId"])
	}
	want := map[string]any{
		"ok":      false,
		"type":    "sandbox.error",
		"status":  float64(409),
		"code":    "sandbox_not_ready",
		"message": "sandbox is not ready",
		"error": map[string]any{
			"code":    "sandbox_not_ready",
			"message": "sandbox is not ready",
			"status":  float64(409),
		},
		"requestId": reqID,
	}
	if diff := diffMaps(want, body); diff != "" {
		t.Fatalf("envelope differs: %s\nbody: %s", diff, rec.Body.String())
	}

	// Field order must match the spec example: ok, type, status, code,
	// message, error, requestId.
	order := []string{`"ok"`, `"type"`, `"status"`, `"code"`, `"message"`, `"error"`, `"requestId"`}
	last := -1
	for _, key := range order {
		i := strings.Index(rec.Body.String(), key)
		if i < 0 {
			t.Fatalf("envelope misses %s: %s", key, rec.Body.String())
		}
		if i < last {
			t.Fatalf("envelope field %s out of order: %s", key, rec.Body.String())
		}
		last = i
	}
}

func TestErrorString(t *testing.T) {
	e := New(http.StatusConflict, CodeSandboxNotReady, "sandbox is not ready")
	if got := e.Error(); got != "409 sandbox_not_ready: sandbox is not ready" {
		t.Fatalf("Error() = %q", got)
	}
}

func TestWriteErrorDetails(t *testing.T) {
	rec := httptest.NewRecorder()
	e := New(http.StatusBadGateway, CodeSandboxDirectFailed, "the sandbox machine could not complete the call").
		Retryable(true).
		RetryAfterSeconds(3)
	WriteError(rec, nil, e)

	body := decode(t, rec)
	details, ok := body["error"].(map[string]any)["details"].(map[string]any)
	if !ok {
		t.Fatalf("no error.details in %s", rec.Body.String())
	}
	if details["retryable"] != true || details["retryAfterSeconds"] != float64(3) {
		t.Fatalf("details = %v", details)
	}
	if _, ok := body["details"]; ok {
		t.Fatalf("details leaked to top level: %s", rec.Body.String())
	}
}

func TestWriteErrorNoDetailsOmitted(t *testing.T) {
	rec := httptest.NewRecorder()
	WriteError(rec, nil, New(http.StatusNotFound, CodeNotFound, "not found"))
	if strings.Contains(rec.Body.String(), "details") {
		t.Fatalf("details key present without details: %s", rec.Body.String())
	}
}

func TestWriteErrorUnknownIsSafe(t *testing.T) {
	rec := httptest.NewRecorder()
	WriteError(rec, nil, errors.New("postgres: password hunter2 refused"))

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", rec.Code)
	}
	body := decode(t, rec)
	if body["code"] != CodeInternal {
		t.Fatalf("code = %v, want %s", body["code"], CodeInternal)
	}
	if strings.Contains(rec.Body.String(), "hunter2") {
		t.Fatalf("internals leaked: %s", rec.Body.String())
	}
}

func TestWriteSuccess(t *testing.T) {
	rec := httptest.NewRecorder()
	type payload struct {
		ID   string `json:"id"`
		Name string `json:"name"`
	}
	Write(rec, http.StatusOK, "sandbox.info", payload{ID: "bx_23456789", Name: "test"})

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	body := decode(t, rec)
	want := map[string]any{
		"ok":   true,
		"type": "sandbox.info",
		"id":   "bx_23456789",
		"name": "test",
	}
	if diff := diffMaps(want, body); diff != "" {
		t.Fatalf("envelope differs: %s", diff)
	}
}

func TestWriteSuccessNilAndOverrides(t *testing.T) {
	// A nil payload is exactly {"ok":true,"type":...}. Compare it
	// structurally: json/v2 does not promise map member order.
	rec := httptest.NewRecorder()
	Write(rec, http.StatusAccepted, "sandbox.created", nil)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("content type = %q", ct)
	}
	body := decode(t, rec)
	want := map[string]any{"ok": true, "type": "sandbox.created"}
	if len(body) != len(want) {
		t.Fatalf("member count = %d, want %d: %s", len(body), len(want), rec.Body.String())
	}
	if diff := diffMaps(want, body); diff != "" {
		t.Fatalf("envelope differs: %s\nbody: %s", diff, rec.Body.String())
	}

	// A hostile payload cannot forge ok or type.
	rec = httptest.NewRecorder()
	Write(rec, http.StatusOK, "sandbox.list", map[string]any{"ok": false, "type": "x", "items": []int{1}})
	body = decode(t, rec)
	if body["ok"] != true || body["type"] != "sandbox.list" || body["items"] == nil {
		t.Fatalf("payload overrode envelope: %s", rec.Body.String())
	}
}

func TestWriteNonObjectIs500(t *testing.T) {
	rec := httptest.NewRecorder()
	Write(rec, http.StatusOK, "sandbox.list", []string{"a", "b"})
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", rec.Code)
	}
	body := decode(t, rec)
	if body["code"] != CodeInternal {
		t.Fatalf("code = %v", body["code"])
	}
	if reqID, _ := body["requestId"].(string); !strings.HasPrefix(reqID, "req_") {
		t.Fatalf("failure path has no requestId: %s", rec.Body.String())
	}
}

func TestWriteSuccessBigInt(t *testing.T) {
	// Regression: merging the payload through map[string]any must not
	// lose integer precision beyond 2^53 (float64 rounds it).
	rec := httptest.NewRecorder()
	Write(rec, http.StatusOK, "sandbox.info", struct {
		Big   int64 `json:"big"`
		Small int   `json:"small"`
	}{Big: 9007199254740993, Small: 7})
	if !strings.Contains(rec.Body.String(), `"big":9007199254740993`) {
		t.Fatalf("integer lost precision: %s", rec.Body.String())
	}
}

func TestWriteErrorUnmarshalableDetails(t *testing.T) {
	// A detail value that cannot marshal must still produce the complete
	// required envelope through the last-resort fallback, with the
	// middleware's request id in body and header.
	h := RequestID(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		WriteError(w, r, New(http.StatusBadGateway, CodeSandboxDirectFailed, "x").
			WithDetail("bad", func() {}))
	}))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", rec.Code)
	}
	body := decode(t, rec)
	for _, k := range []string{"ok", "type", "status", "code", "message", "error", "requestId"} {
		if _, ok := body[k]; !ok {
			t.Fatalf("fallback envelope misses %s: %s", k, rec.Body.String())
		}
	}
	if body["ok"] != false || body["type"] != "sandbox.error" || body["code"] != CodeInternal {
		t.Fatalf("fallback envelope = %s", rec.Body.String())
	}
	reqID, _ := body["requestId"].(string)
	if !strings.HasPrefix(reqID, "req_") {
		t.Fatalf("fallback requestId = %q", reqID)
	}
	if hdr := rec.Header().Get("X-Request-Id"); hdr != reqID {
		t.Fatalf("header %q != body requestId %q", hdr, reqID)
	}
}

func TestWriteFailureKeepsRequestID(t *testing.T) {
	// Success writes that fail mid-way must keep the middleware's request
	// id: the body and the X-Request-Id header must match.
	for _, tc := range []struct {
		name string
		v    any
	}{
		{"unmarshalable payload", map[string]any{"f": func() {}}},
		{"non-object payload", []string{"a"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := RequestID(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				Write(w, http.StatusOK, "sandbox.list", tc.v)
			}))
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
			if rec.Code != http.StatusInternalServerError {
				t.Fatalf("status = %d, want 500", rec.Code)
			}
			body := decode(t, rec)
			reqID, _ := body["requestId"].(string)
			if !strings.HasPrefix(reqID, "req_") {
				t.Fatalf("requestId = %q", reqID)
			}
			if hdr := rec.Header().Get("X-Request-Id"); hdr != reqID {
				t.Fatalf("header %q != body requestId %q", hdr, reqID)
			}
		})
	}
}

func TestRequestID(t *testing.T) {
	var seen []string
	h := RequestID(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := RequestIDFrom(r.Context())
		if !strings.HasPrefix(id, "req_") || len(id) != 30 {
			t.Fatalf("request id = %q", id)
		}
		seen = append(seen, id)
		WriteError(w, r, New(http.StatusNotFound, CodeNotFound, "not found"))
	}))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if body := decode(t, rec); body["requestId"] != seen[0] {
		t.Fatalf("requestId %v does not match the context id %q", body["requestId"], seen[0])
	}
	if hdr := rec.Header().Get("X-Request-Id"); hdr != seen[0] {
		t.Fatalf("X-Request-Id header = %q, want %q", hdr, seen[0])
	}
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if decode(t, rec)["requestId"] == seen[0] {
		t.Fatal("request ids not unique")
	}
	if RequestIDFrom(t.Context()) != "" {
		t.Fatal("request id without middleware")
	}
}

// diffMaps reports the first differences between want and got, shallowly
// nested for the error object.
func diffMaps(want, got map[string]any) string {
	if len(want) != len(got) {
		return "key count differs"
	}
	for k, w := range want {
		g, ok := got[k]
		if !ok {
			return "missing key " + k
		}
		if wm, ok := w.(map[string]any); ok {
			gm, ok := g.(map[string]any)
			if !ok {
				return k + " is not an object"
			}
			if d := diffMaps(wm, gm); d != "" {
				return k + "." + d
			}
			continue
		}
		if w != g {
			return k + " differs"
		}
	}
	return ""
}
