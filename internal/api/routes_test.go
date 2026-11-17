package api

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestRegisterRoutes(t *testing.T) {
	var calls []string
	record := func(name string) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			calls = append(calls, name)
			w.WriteHeader(http.StatusNoContent)
		}
	}
	mux := http.NewServeMux()
	Register(mux, Handlers{
		Me:                   record("me"),
		ListSandboxes:        record("list"),
		CreateSandbox:        record("create"),
		GetSandbox:           record("get"),
		UpdateSandbox:        record("update"),
		DeleteSandbox:        record("delete"),
		GetDeletionOperation: record("operation"),
		StopSandbox:          record("stop"),
		GetSandboxStop:       record("stop-status"),
		ResumeSandbox:        record("resume"),
	})

	tests := []struct {
		method string
		path   string
		want   string
	}{
		{http.MethodGet, "/api/v1/me", "me"},
		{http.MethodGet, "/api/v1/sandboxes", "list"},
		{http.MethodPost, "/api/v1/sandboxes", "create"},
		{http.MethodGet, "/api/v1/sandboxes/bx_23456789", "get"},
		{http.MethodPatch, "/api/v1/sandboxes/bx_23456789", "update"},
		{http.MethodDelete, "/api/v1/sandboxes/bx_23456789", "delete"},
		{http.MethodGet, "/api/v1/deletion-operations/bdop_0123456789abcdef0123456789abcdef", "operation"},
		{http.MethodPost, "/api/v1/sandboxes/bx_23456789/stop", "stop"},
		{http.MethodGet, "/api/v1/sandboxes/bx_23456789/stops/stop_0123456789abcdef", "stop-status"},
		{http.MethodPost, "/api/v1/sandboxes/bx_23456789/resume", "resume"},
	}
	for _, test := range tests {
		t.Run(test.method+" "+test.path, func(t *testing.T) {
			calls = nil
			recorder := httptest.NewRecorder()
			mux.ServeHTTP(recorder, httptest.NewRequest(test.method, test.path, nil))
			if recorder.Code != http.StatusNoContent {
				t.Fatalf("status = %d, want 204", recorder.Code)
			}
			if len(calls) != 1 || calls[0] != test.want {
				t.Errorf("calls = %v, want %s", calls, test.want)
			}
		})
	}

	t.Run("method not allowed", func(t *testing.T) {
		recorder := httptest.NewRecorder()
		mux.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/api/v1/me", nil))
		if recorder.Code != http.StatusMethodNotAllowed {
			t.Errorf("status = %d, want 405", recorder.Code)
		}
	})

	t.Run("nil handler leaves route unregistered", func(t *testing.T) {
		mux := http.NewServeMux()
		Register(mux, Handlers{})
		recorder := httptest.NewRecorder()
		mux.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/api/v1/me", nil))
		if recorder.Code != http.StatusNotFound {
			t.Errorf("status = %d, want 404", recorder.Code)
		}
	})
}

func TestRoutePathValues(t *testing.T) {
	mux := http.NewServeMux()
	Register(mux, Handlers{
		GetSandbox: func(w http.ResponseWriter, r *http.Request) {
			if got := r.PathValue(ParamSandboxID); got != "bx_23456789" {
				t.Errorf("sandbox id = %q", got)
			}
		},
		GetDeletionOperation: func(w http.ResponseWriter, r *http.Request) {
			if got := r.PathValue(ParamOperationID); got != "bdop_0123456789abcdef0123456789abcdef" {
				t.Errorf("operation id = %q", got)
			}
		},
		GetSandboxStop: func(w http.ResponseWriter, r *http.Request) {
			if got := r.PathValue(ParamSandboxID); got != "bx_23456789" {
				t.Errorf("sandbox id = %q", got)
			}
			if got := r.PathValue(ParamStopID); got != "stop_0123456789abcdef" {
				t.Errorf("stop id = %q", got)
			}
		},
	})
	mux.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/api/v1/sandboxes/bx_23456789", nil))
	mux.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/api/v1/deletion-operations/bdop_0123456789abcdef0123456789abcdef", nil))
	mux.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/api/v1/sandboxes/bx_23456789/stops/stop_0123456789abcdef", nil))
}
