package api_test

// Create with count: the response carries every sandbox in creation order and
// validates against the v1 API document.

import (
	"context"
	"encoding/json/v2"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/nathabonfim59/bamboo-raft/internal/api"
	"github.com/nathabonfim59/bamboo-raft/internal/apierr"
	"github.com/nathabonfim59/bamboo-raft/internal/db"
)

type coreCompatBatch struct {
	results  []api.AdmissionResult
	err      error
	requests []api.AdmissionRequest
}

func (b *coreCompatBatch) AdmitBatch(_ context.Context, requests []api.AdmissionRequest) ([]api.AdmissionResult, error) {
	b.requests = requests
	return b.results, b.err
}

func countCompatHandlers(t *testing.T, batch api.BatchAdmission) api.Handlers {
	t.Helper()
	handlers, err := api.NewCoreHandlers(api.CoreOptions{
		Store: &coreCompatStore{},
		Admission: coreCompatAdmission{
			err: errors.New("single admission must not run for a batch"),
		},
		Batch: batch,
		Now:   func() time.Time { return coreCompatTime },
	})
	if err != nil {
		t.Fatalf("NewCoreHandlers: %v", err)
	}
	return handlers
}

// countCompatBody decodes one response object.
func countCompatBody(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("response is not a JSON object: %v", err)
	}
	return body
}

func TestCreateCountCompatBytes(t *testing.T) {
	validators := coreCompatValidators(t)
	first := coreCompatSandbox()
	second := first
	second.ID = "bx_abcdefgh"

	t.Run("202 batch", func(t *testing.T) {
		batch := &coreCompatBatch{results: []api.AdmissionResult{
			{Sandbox: first}, {Sandbox: second},
		}}
		handlers := countCompatHandlers(t, batch)
		request := coreCompatRequest(http.MethodPost, api.PathSandboxes,
			`{"from":"base","count":2}`, coreCompatOwner)
		request.Header.Set("Content-Type", "application/json")
		recorder := coreCompatServe(handlers.CreateSandbox, request)
		coreCompatAssert(t, validators, "create", http.StatusAccepted, recorder)
		if len(batch.requests) != 2 {
			t.Fatalf("batch requests = %d, want 2", len(batch.requests))
		}
		body := countCompatBody(t, recorder)
		sandboxes, ok := body["sandboxes"].([]any)
		if !ok || len(sandboxes) != 2 {
			t.Fatalf("sandboxes = %v", body["sandboxes"])
		}
	})

	t.Run("400 count without from", func(t *testing.T) {
		handlers := countCompatHandlers(t, &coreCompatBatch{})
		request := coreCompatRequest(http.MethodPost, api.PathSandboxes, `{"count":2}`, coreCompatOwner)
		request.Header.Set("Content-Type", "application/json")
		recorder := coreCompatServe(handlers.CreateSandbox, request)
		coreCompatAssert(t, validators, "create", http.StatusBadRequest, recorder)
		if body := countCompatBody(t, recorder); body["code"] != apierr.CodeCountRequiresFrom {
			t.Fatalf("code = %v, want %s", body["code"], apierr.CodeCountRequiresFrom)
		}
	})

	t.Run("400 count with failFast", func(t *testing.T) {
		handlers := countCompatHandlers(t, &coreCompatBatch{})
		request := coreCompatRequest(http.MethodPost, api.PathSandboxes,
			`{"from":"base","count":2,"failFast":true}`, coreCompatOwner)
		request.Header.Set("Content-Type", "application/json")
		recorder := coreCompatServe(handlers.CreateSandbox, request)
		coreCompatAssert(t, validators, "create", http.StatusBadRequest, recorder)
		if body := countCompatBody(t, recorder); body["code"] != apierr.CodeCountWithFailFast {
			t.Fatalf("code = %v, want %s", body["code"], apierr.CodeCountWithFailFast)
		}
	})

	t.Run("503 batch capacity", func(t *testing.T) {
		handlers := countCompatHandlers(t, &coreCompatBatch{err: errors.Join(db.ErrNoCapacity, errors.New("full"))})
		request := coreCompatRequest(http.MethodPost, api.PathSandboxes,
			`{"from":"base","count":2}`, coreCompatOwner)
		request.Header.Set("Content-Type", "application/json")
		recorder := coreCompatServe(handlers.CreateSandbox, request)
		coreCompatAssert(t, validators, "create", http.StatusServiceUnavailable, recorder)
	})
}
