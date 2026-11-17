package cli

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nathabonfim59/bamboo-raft/internal/api"
)

func TestClientMeSendsPinnedRequest(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != api.PathMe {
			t.Errorf("request = %s %s, want GET %s", r.Method, r.URL.Path, api.PathMe)
		}
		if got := r.Header.Get("Authorization"); !cliTestSecretEqual(got, "Bearer test-key") {
			t.Error("Authorization header does not carry the expected key")
		}
		if got := r.Header.Get("Accept"); got != "application/json" {
			t.Errorf("Accept = %q", got)
		}
		if got := r.Header.Get("User-Agent"); !strings.HasPrefix(got, "bbraft/") {
			t.Errorf("User-Agent = %q", got)
		}
		cliTestWriteJSON(t, w, http.StatusOK, api.NewMeResponse(api.MeUser{ID: "acct_1", Login: "alice"}))
	}))
	defer server.Close()
	client, err := NewClient(server.URL, "test-key", server.Client())
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	me, err := client.Me(t.Context())
	if err != nil {
		t.Fatalf("me: %v", err)
	}
	if me.User.ID != "acct_1" || me.User.Login != "alice" {
		t.Fatalf("user = %+v", me.User)
	}
}

func TestClientAPIErrorIsSafe(t *testing.T) {
	body := `{"ok":false,"type":"sandbox.error","status":404,"code":"not_found","message":"sandbox not found",` +
		`"error":{"status":404,"code":"not_found","message":"sandbox not found","details":{"token":"super-secret"}},` +
		`"requestId":"req_1"}`
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(body))
	}))
	defer server.Close()
	client, err := NewClient(server.URL, "test-key", server.Client())
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	_, err = client.Me(t.Context())
	if err == nil {
		t.Fatal("me succeeded, want an API error")
	}
	apiErr, ok := errors.AsType[*APIError](err)
	if !ok {
		t.Fatalf("error = %T, want *APIError", err)
	}
	if apiErr.Status != 404 || apiErr.Code != "not_found" || apiErr.RequestID != "req_1" {
		t.Fatalf("APIError = %+v", apiErr)
	}
	if strings.Contains(err.Error(), "super-secret") {
		t.Fatal("error contains response details")
	}
}

func TestClientRefusesCrossOriginRedirect(t *testing.T) {
	var stolen atomic.Int64
	other := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		stolen.Add(1)
	}))
	defer other.Close()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, other.URL+"/steal", http.StatusFound)
	}))
	defer server.Close()
	client, err := NewClient(server.URL, "test-key", server.Client())
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	if _, err := client.Me(t.Context()); err == nil {
		t.Fatal("me succeeded, want a redirect refusal")
	}
	if stolen.Load() != 0 {
		t.Fatalf("other origin received %d requests, want 0", stolen.Load())
	}
}

func TestClientBoundsResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(strings.Repeat("x", maxResponseBytes+1)))
	}))
	defer server.Close()
	client, err := NewClient(server.URL, "test-key", server.Client())
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	_, err = client.Me(t.Context())
	if err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("error = %v, want a size error", err)
	}
}

func TestClientRejectsWrongEnvelope(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{name: "empty object", body: `{}`},
		{name: "ok false", body: `{"ok":false,"type":"user.info","user":{"id":"acct_1"}}`},
		{name: "wrong type", body: `{"ok":true,"type":"sandbox.list","user":{"id":"acct_1"}}`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer server.Close()
			client, err := NewClient(server.URL, "test-key", server.Client())
			if err != nil {
				t.Fatalf("new client: %v", err)
			}
			_, err = client.Me(t.Context())
			if err == nil {
				t.Fatal("me succeeded, want an envelope error")
			}
			cliErr, ok := errors.AsType[*CLIError](err)
			if !ok || cliErr.Code != "invalid_response" {
				t.Fatalf("error = %v, want invalid_response", err)
			}
		})
	}
}

func TestClientErrorStatusIsActualHTTPStatus(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cliTestWriteJSON(t, w, http.StatusNotFound, cliTestErrorEnvelope(http.StatusInternalServerError, "http_500", "server says 500"))
	}))
	defer server.Close()
	client, err := NewClient(server.URL, "test-key", server.Client())
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	_, err = client.Me(t.Context())
	apiErr, ok := errors.AsType[*APIError](err)
	if !ok {
		t.Fatalf("error = %T, want *APIError", err)
	}
	if apiErr.Status != http.StatusNotFound {
		t.Fatalf("status = %d, want the actual HTTP 404", apiErr.Status)
	}
}

// TestDecodeAPIErrorEnvelope proves only the complete pinned envelope is
// trusted. Missing or null members, mismatched status/code and non-string
// request ids fall back to the actual HTTP status; both messages must be
// present but their wording need not match; only a positive integer retry
// hint survives.
func TestDecodeAPIErrorEnvelope(t *testing.T) {
	const base = `{"ok":false,"type":"sandbox.error","status":503,"code":"out_of_capacity",` +
		`"message":"no capacity is available right now",` +
		`"error":{"status":503,"code":"out_of_capacity","message":"no capacity is available right now"},` +
		`"requestId":"req_1"}`
	withDetails := func(details string) string {
		return strings.Replace(base, `"},"requestId"`, `","details":`+details+`},"requestId"`, 1)
	}
	cases := []struct {
		name      string
		status    int
		body      string
		wantOK    bool
		wantRetry int
		wantCode  string
		wantMsg   string
		wantReqID string
	}{
		{name: "valid", status: http.StatusServiceUnavailable, body: base, wantOK: true, wantCode: "out_of_capacity", wantMsg: "no capacity is available right now", wantReqID: "req_1"},
		{name: "positive hint", status: http.StatusServiceUnavailable, body: withDetails(`{"retryAfterSeconds":5}`), wantOK: true, wantRetry: 5, wantCode: "out_of_capacity", wantMsg: "no capacity is available right now", wantReqID: "req_1"},
		{name: "zero hint", status: http.StatusServiceUnavailable, body: withDetails(`{"retryAfterSeconds":0}`), wantOK: true, wantCode: "out_of_capacity", wantMsg: "no capacity is available right now", wantReqID: "req_1"},
		{name: "negative hint", status: http.StatusServiceUnavailable, body: withDetails(`{"retryAfterSeconds":-1}`), wantOK: true, wantCode: "out_of_capacity", wantMsg: "no capacity is available right now", wantReqID: "req_1"},
		{name: "fractional hint", status: http.StatusServiceUnavailable, body: withDetails(`{"retryAfterSeconds":1.5}`), wantOK: true, wantCode: "out_of_capacity", wantMsg: "no capacity is available right now", wantReqID: "req_1"},
		{name: "string hint", status: http.StatusServiceUnavailable, body: withDetails(`{"retryAfterSeconds":"5"}`), wantOK: true, wantCode: "out_of_capacity", wantMsg: "no capacity is available right now", wantReqID: "req_1"},
		{name: "overflow hint", status: http.StatusServiceUnavailable, body: withDetails(`{"retryAfterSeconds":99999999999999999999}`), wantOK: true, wantCode: "out_of_capacity", wantMsg: "no capacity is available right now", wantReqID: "req_1"},
		{name: "exponent hint", status: http.StatusServiceUnavailable, body: withDetails(`{"retryAfterSeconds":1e3}`), wantOK: true, wantCode: "out_of_capacity", wantMsg: "no capacity is available right now", wantReqID: "req_1"},
		{name: "empty request id is allowed", status: http.StatusServiceUnavailable, body: strings.Replace(base, `"req_1"`, `""`, 1), wantOK: true, wantCode: "out_of_capacity", wantMsg: "no capacity is available right now"},
		{name: "nested message wording may differ", status: http.StatusServiceUnavailable, body: strings.Replace(base, `"error":{"status":503,"code":"out_of_capacity","message":"no capacity is available right now"}`, `"error":{"status":503,"code":"out_of_capacity","message":"capacity unavailable"}`, 1), wantOK: true, wantCode: "out_of_capacity", wantMsg: "no capacity is available right now", wantReqID: "req_1"},
		{name: "missing request id", status: http.StatusServiceUnavailable, body: strings.Replace(base, `,"requestId":"req_1"`, "", 1)},
		{name: "null request id", status: http.StatusServiceUnavailable, body: strings.Replace(base, `"req_1"`, `null`, 1)},
		{name: "number request id", status: http.StatusServiceUnavailable, body: strings.Replace(base, `"req_1"`, `5`, 1)},
		{name: "missing ok", status: http.StatusServiceUnavailable, body: strings.Replace(base, `"ok":false,`, "", 1)},
		{name: "null ok", status: http.StatusServiceUnavailable, body: strings.Replace(base, `"ok":false`, `"ok":null`, 1)},
		{name: "string ok", status: http.StatusServiceUnavailable, body: strings.Replace(base, `"ok":false`, `"ok":"false"`, 1)},
		{name: "number ok", status: http.StatusServiceUnavailable, body: strings.Replace(base, `"ok":false`, `"ok":0`, 1)},
		{name: "true ok", status: http.StatusServiceUnavailable, body: strings.Replace(base, `"ok":false`, `"ok":true`, 1)},
		{name: "string status", status: http.StatusServiceUnavailable, body: strings.Replace(base, `"status":503`, `"status":"503"`, 1)},
		{name: "number code", status: http.StatusServiceUnavailable, body: strings.Replace(base, `"code":"out_of_capacity"`, `"code":5`, 1)},
		{name: "wrong type", status: http.StatusServiceUnavailable, body: strings.Replace(base, `"sandbox.error"`, `"sandbox.created"`, 1)},
		{name: "top status mismatch", status: http.StatusServiceUnavailable, body: strings.Replace(base, `"status":503`, `"status":502`, 1)},
		{name: "nested status mismatch", status: http.StatusServiceUnavailable, body: strings.Replace(base, `"error":{"status":503`, `"error":{"status":502`, 1)},
		{name: "nested code mismatch", status: http.StatusServiceUnavailable, body: strings.Replace(base, `"error":{"status":503,"code":"out_of_capacity"`, `"error":{"status":503,"code":"no_ready_machine"`, 1)},
		{name: "empty code", status: http.StatusServiceUnavailable, body: strings.Replace(base, `"code":"out_of_capacity"`, `"code":""`, 1)},
		{name: "empty message", status: http.StatusServiceUnavailable, body: strings.Replace(base, `"message":"no capacity is available right now",`, `"message":"",`, 1)},
		{name: "empty nested message", status: http.StatusServiceUnavailable, body: strings.Replace(base, `"message":"no capacity is available right now"}`, `"message":""}`, 1)},
		{name: "plain text", status: http.StatusServiceUnavailable, body: "Service Unavailable"},
		{name: "arbitrary object", status: http.StatusServiceUnavailable, body: `{"error":"out_of_capacity"}`},
		{name: "malformed json", status: http.StatusServiceUnavailable, body: `{"ok":false`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := decodeAPIError(tc.status, []byte(tc.body))
			apiErr, ok := errors.AsType[*APIError](err)
			if !ok {
				t.Fatalf("error = %T, want *APIError", err)
			}
			if apiErr.Status != tc.status {
				t.Fatalf("status = %d, want the actual HTTP %d", apiErr.Status, tc.status)
			}
			if !tc.wantOK {
				if apiErr.Envelope {
					t.Fatalf("Envelope = true for a body outside the pin")
				}
				if apiErr.Code != fmt.Sprintf("http_%d", tc.status) || apiErr.Message != http.StatusText(tc.status) {
					t.Fatalf("fallback = %+v, want the generic status error", apiErr)
				}
				if apiErr.RetryAfterSeconds != 0 || apiErr.RequestID != "" {
					t.Fatalf("fallback retained envelope fields: %+v", apiErr)
				}
				return
			}
			if !apiErr.Envelope {
				t.Fatal("Envelope = false for a valid pinned envelope")
			}
			if apiErr.RetryAfterSeconds != tc.wantRetry {
				t.Fatalf("retryAfterSeconds = %d, want %d", apiErr.RetryAfterSeconds, tc.wantRetry)
			}
			if apiErr.Code != tc.wantCode || apiErr.Message != tc.wantMsg || apiErr.RequestID != tc.wantReqID {
				t.Fatalf("APIError = %+v", apiErr)
			}
		})
	}
}

func TestClientRawDoOutlivesJSONBound(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(40 * time.Millisecond)
		cliTestWriteJSON(t, w, http.StatusOK, api.NewMeResponse(api.MeUser{ID: "acct_1"}))
	}))
	defer server.Close()
	client, err := NewClient(server.URL, "test-key", server.Client())
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	client.jsonTimeout = 10 * time.Millisecond
	if _, err := client.Me(t.Context()); err == nil {
		t.Fatal("me succeeded, want the JSON bound to expire")
	}
	req, err := client.NewRequest(t.Context(), http.MethodGet, api.PathMe, nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("raw Do: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
}

func TestClientEncodeErrorIsGeneric(t *testing.T) {
	client, err := NewClient("http://127.0.0.1:1", "test-key", nil)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	var out api.MeResponse
	err = client.do(context.Background(), http.MethodPost, "/api/v1/me", nil, nil, make(chan int), &out, api.TypeUserInfo)
	if err == nil {
		t.Fatal("do succeeded, want an encode error")
	}
	if strings.Contains(err.Error(), "chan") {
		t.Fatalf("encode error leaked the value type: %v", err)
	}
}

func TestValidateAPIURLDoesNotEchoSecret(t *testing.T) {
	const secret = "super-secret"
	err := ValidateAPIURL("https://user:" + secret + "@[::1")
	if err == nil {
		t.Fatal("ValidateAPIURL succeeded, want an error")
	}
	if strings.Contains(err.Error(), secret) {
		t.Fatal("error contains the secret")
	}
}

func TestClientDeleteSendsConfirmationHeader(t *testing.T) {
	const id = "bx_23456789"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete || r.URL.Path != "/api/v1/sandboxes/"+id {
			t.Errorf("request = %s %s", r.Method, r.URL.Path)
		}
		if got := r.Header.Get("X-Ascii-Confirm-Delete"); got != id {
			t.Errorf("X-Ascii-Confirm-Delete = %q, want %q", got, id)
		}
		op := cliTestOperation(cliTestOperationID, id, api.DeletionPending)
		cliTestWriteJSON(t, w, http.StatusAccepted, api.NewSandboxDeletingResponse(op))
	}))
	defer server.Close()
	client, err := NewClient(server.URL, "test-key", server.Client())
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	response, err := client.DeleteSandbox(context.Background(), id)
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	if response.Operation.ID != cliTestOperationID {
		t.Fatalf("operation = %+v", response.Operation)
	}
}

func TestClientEscapesSnapshotName(t *testing.T) {
	const name = "my snap/é"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got, want := r.URL.Path, "/api/v1/saved-snapshots/"+name; got != want {
			t.Errorf("path = %q, want %q", got, want)
		}
		if got, want := r.URL.EscapedPath(), "/api/v1/saved-snapshots/my%20snap%2F%C3%A9"; got != want {
			t.Errorf("escaped path = %q, want %q", got, want)
		}
		// The unsafe name is refused, so this stays a negative fixture.
		cliTestWriteJSON(t, w, http.StatusBadRequest, cliTestErrorEnvelope(http.StatusBadRequest, "invalid_name", "invalid snapshot name"))
	}))
	defer server.Close()
	client, err := NewClient(server.URL, "test-key", server.Client())
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	_, err = client.GetSavedSnapshot(t.Context(), name)
	apiErr, ok := errors.AsType[*APIError](err)
	if !ok || apiErr.Code != "invalid_name" {
		t.Fatalf("error = %v, want invalid_name", err)
	}
}

// TestClientRawErrorsDoNotEchoTokens proves transport, redirect and URL errors
// never repeat an upload token carried in the query.
func TestClientRawErrorsDoNotEchoTokens(t *testing.T) {
	const token = "sentinel-upload-token"

	t.Run("closed server", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
		base := server.URL
		server.Close()
		client, err := NewClient(base, "test-key", nil)
		if err != nil {
			t.Fatalf("new client: %v", err)
		}
		req, err := client.NewRequest(t.Context(), http.MethodGet, "/upload?token="+token, nil)
		if err != nil {
			t.Fatalf("new request: %v", err)
		}
		if _, err := client.Do(req); err == nil {
			t.Fatal("Do succeeded, want a transport error")
		} else if strings.Contains(err.Error(), token) {
			t.Fatal("transport error echoed the token")
		}
	})

	t.Run("redirect", func(t *testing.T) {
		other := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
		defer other.Close()
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Redirect(w, r, other.URL+"/steal?token="+token, http.StatusFound)
		}))
		defer server.Close()
		client, err := NewClient(server.URL, "test-key", server.Client())
		if err != nil {
			t.Fatalf("new client: %v", err)
		}
		req, err := client.NewRequest(t.Context(), http.MethodGet, api.PathMe, nil)
		if err != nil {
			t.Fatalf("new request: %v", err)
		}
		if _, err := client.Do(req); err == nil {
			t.Fatal("Do succeeded, want a redirect refusal")
		} else if strings.Contains(err.Error(), token) {
			t.Fatal("redirect error echoed the token")
		}
	})

	t.Run("malformed url", func(t *testing.T) {
		client, err := NewClient("http://127.0.0.1:1", "test-key", nil)
		if err != nil {
			t.Fatalf("new client: %v", err)
		}
		if _, err := client.NewRequest(t.Context(), http.MethodGet, "/upload?token="+token+"\x7f", nil); err == nil {
			t.Fatal("NewRequest succeeded, want an error")
		} else if strings.Contains(err.Error(), token) {
			t.Fatal("URL error echoed the token")
		}
	})
}

func TestClientDoKeepsCancellation(t *testing.T) {
	client, err := NewClient("http://127.0.0.1:1", "test-key", nil)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	req, err := client.NewRequest(ctx, http.MethodGet, api.PathMe, nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	if _, err := client.Do(req); !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}
}
