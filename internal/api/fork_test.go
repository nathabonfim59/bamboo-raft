package api

// Fork handler tests: request parsing, the source-state and snapshot-flag
// refusals, the admission seam and the 202 envelope. They use the fake store
// and fake admission, so they need no database.

import (
	"context"
	"crypto/sha256"
	"encoding/json/v2"
	"errors"
	"net/http"
	"strings"
	"testing"

	"uuid"

	"github.com/nathabonfim59/bamboo-raft/internal/apierr"
	"github.com/nathabonfim59/bamboo-raft/internal/compat"
	"github.com/nathabonfim59/bamboo-raft/internal/db"
)

// forkFakeStore serves one source row and its latest generation.
type forkFakeStore struct {
	row    db.Sandbox
	err    error
	gen    db.Generation
	hasGen bool
	genErr error
}

func (s *forkFakeStore) Sandbox(context.Context, uuid.UUID, string) (db.Sandbox, error) {
	return s.row, s.err
}

func (s *forkFakeStore) LatestGeneration(context.Context, uuid.UUID, string) (db.Generation, bool, error) {
	return s.gen, s.hasGen, s.genErr
}

// forkFakeAdmission records the admission contract and returns a fixed fork.
type forkFakeAdmission struct {
	calls  []ForkRequest
	result ForkResult
	err    error
}

func (a *forkFakeAdmission) Fork(_ context.Context, request ForkRequest) (ForkResult, error) {
	a.calls = append(a.calls, request)
	if a.err != nil {
		return ForkResult{}, a.err
	}
	result := a.result
	if result.Sandbox.ID == "" {
		result.Sandbox = db.Sandbox{
			ID: request.SandboxID, OwnerID: request.Owner, State: db.StateProvisioning,
			MachineType: request.MachineType, Labels: request.Labels,
			TTLSeconds: request.TTLSeconds, SnapshotsEnabled: true,
			CreatedAt: coreFixedNow, UpdatedAt: coreFixedNow,
		}
	}
	return result, nil
}

// forkReadySource is one running source sandbox fixture.
func forkReadySource(owner uuid.UUID) db.Sandbox {
	return db.Sandbox{
		ID: "bx_23456789", OwnerID: owner, State: db.StateReady,
		MachineType: "default", Name: "source", SnapshotsEnabled: true,
		Reserved:  db.Resources{VCPUs: 4, MemoryMiB: 8192, DiskGiB: 51},
		Desired:   db.Resources{VCPUs: 4, MemoryMiB: 8192, DiskGiB: 51},
		NodeID:    new("node-a"),
		CreatedAt: coreFixedNow, UpdatedAt: coreFixedNow,
	}
}

// forkTestHandler builds the fork handler over the fakes.
func forkTestHandler(t *testing.T, store ForkStore, admission ForkAdmission, objectStore bool) http.HandlerFunc {
	t.Helper()
	handler, err := NewForkHandler(ForkOptions{
		Store: store, Admission: admission, ObjectStore: objectStore,
	})
	if err != nil {
		t.Fatalf("NewForkHandler: %v", err)
	}
	return handler
}

// forkRequestBody builds one written fork request for the source id.
func forkRequestBody(sourceID, body string) *http.Request {
	request := coreTestRequest(http.MethodPost, PathSandboxFork, strings.NewReader(body), coreTestOwner)
	request.Header.Set("Content-Type", "application/json")
	request.SetPathValue(ParamSandboxID, sourceID)
	return request
}

// TestForkAcceptsAndCarriesTheRequest checks the 202 envelope and that every
// mutable member reaches admission as the pinned default or the request value.
func TestForkAcceptsAndCarriesTheRequest(t *testing.T) {
	validator := compat.Load(t)
	tests := []struct {
		name        string
		body        string
		source      db.Sandbox
		wantTTL     *int
		wantType    string
		wantLabels  map[string]string
		wantEnv     map[string]string
		wantInherit bool
		wantFast    bool
		wantReserve db.Resources
	}{
		{
			name:        "omitted body inherits type and env, ttl 3600",
			body:        "",
			source:      forkReadySource(coreTestOwner),
			wantTTL:     new(3600),
			wantType:    "default",
			wantInherit: true,
			wantReserve: db.Resources{VCPUs: 4, MemoryMiB: 8192, DiskGiB: 51},
		},
		{
			name:       "explicit members replace the source's",
			body:       `{"type":"large","ttlSeconds":null,"labels":{"team":"ci"},"env":{"TOKEN":"x"},"failFast":true,"environment":"base","noEnv":false}`,
			source:     forkReadySource(coreTestOwner),
			wantTTL:    nil,
			wantType:   "large",
			wantLabels: map[string]string{"team": "ci"},
			wantEnv:    map[string]string{"TOKEN": "x"},
			wantFast:   true,
			wantReserve: db.Resources{
				VCPUs: 8, MemoryMiB: 16384, DiskGiB: max(111, 51),
			},
		},
		{
			name:        "noEnv is accepted like create",
			body:        `{"noEnv":true}`,
			source:      forkReadySource(coreTestOwner),
			wantTTL:     new(3600),
			wantType:    "default",
			wantInherit: true,
			wantReserve: db.Resources{VCPUs: 4, MemoryMiB: 8192, DiskGiB: 51},
		},
		{
			name:        "empty env object means no env",
			body:        `{"env":{}}`,
			source:      forkReadySource(coreTestOwner),
			wantTTL:     new(3600),
			wantType:    "default",
			wantEnv:     map[string]string{},
			wantReserve: db.Resources{VCPUs: 4, MemoryMiB: 8192, DiskGiB: 51},
		},
		{
			name:        "small type keeps the larger source disk",
			body:        `{"type":"small"}`,
			source:      forkReadySource(coreTestOwner),
			wantTTL:     new(3600),
			wantType:    "small",
			wantInherit: true,
			wantReserve: db.Resources{VCPUs: 2, MemoryMiB: 4096, DiskGiB: 51},
		},
		{
			name: "archived source is admitted with its latest generation",
			body: `{}`,
			source: func() db.Sandbox {
				row := forkReadySource(coreTestOwner)
				row.State = db.StateArchived
				return row
			}(),
			wantTTL:     new(3600),
			wantType:    "default",
			wantInherit: true,
			wantReserve: db.Resources{VCPUs: 4, MemoryMiB: 8192, DiskGiB: 51},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := &forkFakeStore{row: test.source}
			if test.source.State == db.StateArchived {
				store.hasGen = true
				store.gen = db.Generation{ID: uuid.NewV7(), SandboxID: test.source.ID, Number: 3}
			}
			admission := &forkFakeAdmission{}
			handler := forkTestHandler(t, store, admission, true)
			recorder := coreServe(handler, forkRequestBody(test.source.ID, test.body))
			if recorder.Code != http.StatusAccepted {
				t.Fatalf("status = %d, want 202 (%s)", recorder.Code, recorder.Body.String())
			}
			if err := validator.Response("fork", recorder.Code, recorder.Header(), recorder.Body.Bytes()); err != nil {
				t.Fatalf("202 failed validation: %v", err)
			}
			var body struct {
				OK      bool   `json:"ok"`
				Type    string `json:"type"`
				ID      string `json:"id"`
				Status  string `json:"status"`
				Sandbox struct {
					ID    string `json:"id"`
					State string `json:"state"`
				} `json:"sandbox"`
			}
			if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
				t.Fatalf("decode 202: %v", err)
			}
			if !body.OK || body.Type != TypeSandboxForking || body.Status != "forking" {
				t.Fatalf("envelope = %+v", body)
			}
			if len(admission.calls) != 1 {
				t.Fatalf("admission calls = %d, want 1", len(admission.calls))
			}
			got := admission.calls[0]
			if body.ID != got.SandboxID || body.ID == test.source.ID {
				t.Fatalf("response id = %q, want the fork id %q (not the source)", body.ID, got.SandboxID)
			}
			if body.Sandbox.ID != body.ID || body.Sandbox.State != string(db.StateProvisioning) {
				t.Fatalf("sandbox body = %+v, want the provisioning fork", body.Sandbox)
			}
			if got.SourceID != test.source.ID || got.Source.ID != test.source.ID {
				t.Fatalf("admission source = %q/%q", got.SourceID, got.Source.ID)
			}
			if (got.TTLSeconds == nil) != (test.wantTTL == nil) ||
				(got.TTLSeconds != nil && *got.TTLSeconds != *test.wantTTL) {
				t.Fatalf("ttl = %v, want %v", got.TTLSeconds, test.wantTTL)
			}
			if got.MachineType != test.wantType {
				t.Fatalf("type = %q, want %q", got.MachineType, test.wantType)
			}
			if !mapsEqual(got.Labels, test.wantLabels) {
				t.Fatalf("labels = %v, want %v", got.Labels, test.wantLabels)
			}
			if !mapsEqual(got.Env, test.wantEnv) || got.InheritEnv != test.wantInherit {
				t.Fatalf("env = %v inherit=%t, want %v inherit=%t",
					got.Env, got.InheritEnv, test.wantEnv, test.wantInherit)
			}
			if got.FailFast != test.wantFast {
				t.Fatalf("failFast = %t, want %t", got.FailFast, test.wantFast)
			}
			if got.Resources != test.wantReserve {
				t.Fatalf("reserve = %+v, want %+v", got.Resources, test.wantReserve)
			}
			if test.source.State == db.StateArchived {
				if got.Latest == nil || got.Latest.ID != store.gen.ID {
					t.Fatalf("archived fork latest = %+v, want %v", got.Latest, store.gen.ID)
				}
			}
		})
	}
}

// TestForkRefusals checks every pinned refusal: the source state, its flags,
// a too-small type and a missing object store never reach admission, and each
// answer validates through compat.
func TestForkRefusals(t *testing.T) {
	validator := compat.Load(t)
	tests := []struct {
		name        string
		source      db.Sandbox
		body        string
		objectStore bool
		err         error
		gen         bool
		wantStatus  int
		wantCode    string
	}{
		{
			name: "missing source", objectStore: true,
			err: db.ErrNotFound, wantStatus: http.StatusNotFound, wantCode: apierr.CodeNotFound,
		},
		{
			name: "snapshots off", objectStore: true,
			source: func() db.Sandbox {
				row := forkReadySource(coreTestOwner)
				row.SnapshotsEnabled = false
				return row
			}(),
			wantStatus: http.StatusConflict, wantCode: apierr.CodeSnapshotsDisabled,
		},
		{
			name: "data expired", objectStore: true,
			source: func() db.Sandbox {
				row := forkReadySource(coreTestOwner)
				row.State = db.StateArchived
				row.DataExpiredAt = new(coreFixedNow)
				return row
			}(),
			wantStatus: http.StatusConflict, wantCode: apierr.CodeSnapshotDataExpired,
		},
		{
			name: "no object store", objectStore: false,
			source:     forkReadySource(coreTestOwner),
			wantStatus: http.StatusConflict, wantCode: apierr.CodeSnapshotsDisabled,
		},
		{
			name: "archived without a generation", objectStore: true,
			source: func() db.Sandbox {
				row := forkReadySource(coreTestOwner)
				row.State = db.StateArchived
				return row
			}(),
			wantStatus: http.StatusConflict, wantCode: codeForkFailed,
		},
		{
			name: "provisioning source", objectStore: true,
			source: func() db.Sandbox {
				row := forkReadySource(coreTestOwner)
				row.State = db.StateProvisioning
				return row
			}(),
			wantStatus: http.StatusConflict, wantCode: apierr.CodeSandboxResuming,
		},
		{
			name: "archiving source", objectStore: true,
			source: func() db.Sandbox {
				row := forkReadySource(coreTestOwner)
				row.State = db.StateArchiving
				return row
			}(),
			wantStatus: http.StatusConflict, wantCode: apierr.CodeSandboxStopping,
		},
		{
			name: "error source", objectStore: true,
			source: func() db.Sandbox {
				row := forkReadySource(coreTestOwner)
				row.State = db.StateError
				return row
			}(),
			wantStatus: http.StatusConflict, wantCode: codeForkFailed,
		},
		{
			name: "type smaller than the source disk", objectStore: true,
			body: `{"type":"small"}`,
			source: func() db.Sandbox {
				row := forkReadySource(coreTestOwner)
				row.MachineType = "large"
				row.Reserved.DiskGiB = 111
				row.Desired.DiskGiB = 111
				return row
			}(),
			wantStatus: http.StatusConflict, wantCode: "type_too_small",
		},
		{
			name: "unsupported environment", objectStore: true,
			body:       `{"environment":"customer-demos"}`,
			source:     forkReadySource(coreTestOwner),
			wantStatus: http.StatusBadRequest, wantCode: apierr.CodeUnknownEnvironment,
		},
		{
			name: "monitoring is refused like create", objectStore: true,
			body:       `{"monitoring":false}`,
			source:     forkReadySource(coreTestOwner),
			wantStatus: http.StatusBadRequest, wantCode: apierr.CodeInvalidJSON,
		},
		{
			name: "unknown member", objectStore: true,
			body:       `{"snapshots":false}`,
			source:     forkReadySource(coreTestOwner),
			wantStatus: http.StatusBadRequest, wantCode: apierr.CodeInvalidJSON,
		},
		{
			name: "zero ttl", objectStore: true,
			body:       `{"ttlSeconds":0}`,
			source:     forkReadySource(coreTestOwner),
			wantStatus: http.StatusBadRequest, wantCode: apierr.CodeInvalidJSON,
		},
		{
			name: "unknown type", objectStore: true,
			body:       `{"type":"gigantic"}`,
			source:     forkReadySource(coreTestOwner),
			wantStatus: http.StatusBadRequest, wantCode: apierr.CodeInvalidJSON,
		},
		{
			name: "bad env key", objectStore: true,
			body:       `{"env":{"1BAD":"x"}}`,
			source:     forkReadySource(coreTestOwner),
			wantStatus: http.StatusBadRequest, wantCode: apierr.CodeInvalidEnv,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := &forkFakeStore{row: test.source, err: test.err}
			if test.gen {
				store.hasGen = true
			}
			admission := &forkFakeAdmission{}
			handler := forkTestHandler(t, store, admission, test.objectStore)
			recorder := coreServe(handler, forkRequestBody(test.source.ID, test.body))
			if recorder.Code != test.wantStatus {
				t.Fatalf("status = %d, want %d (%s)", recorder.Code, test.wantStatus, recorder.Body.String())
			}
			coreAssertError(t, recorder, test.wantStatus, test.wantCode)
			if err := validator.Response("fork", recorder.Code, recorder.Header(), recorder.Body.Bytes()); err != nil {
				t.Fatalf("%d %s failed validation: %v", test.wantStatus, test.wantCode, err)
			}
			if len(admission.calls) != 0 {
				t.Fatalf("refused fork reached admission: %+v", admission.calls)
			}
		})
	}
}

// TestForkAdmissionFailures checks the admission error mapping and that a
// malformed result is never reported as a successful fork.
func TestForkAdmissionFailures(t *testing.T) {
	validator := compat.Load(t)
	tests := []struct {
		name       string
		body       string
		err        error
		result     ForkResult
		wantStatus int
		wantCode   string
		wantRetry  bool
	}{
		{
			name: "failFast capacity is no_ready_machine", body: `{"failFast":true}`,
			err:        errors.Join(db.ErrNoCapacity, errors.New("scheduler: no node with capacity")),
			wantStatus: http.StatusServiceUnavailable, wantCode: apierr.CodeNoReadyMachine, wantRetry: true,
		},
		{
			name:       "queued fork without room is out_of_capacity",
			err:        db.ErrNoCapacity,
			wantStatus: http.StatusServiceUnavailable, wantCode: apierr.CodeOutOfCapacity, wantRetry: true,
		},
		{
			name: "no connected node is no_ready_machine", err: ErrNoReadyMachine,
			wantStatus: http.StatusServiceUnavailable, wantCode: apierr.CodeNoReadyMachine, wantRetry: true,
		},
		{
			name: "source vanished before admission", err: db.ErrNotFound,
			wantStatus: http.StatusNotFound, wantCode: apierr.CodeNotFound,
		},
		{
			name: "pinned generation vanished", err: db.ErrSnapshotNotFound,
			wantStatus: http.StatusNotFound, wantCode: apierr.CodeSnapshotNotFound,
		},
		{
			name: "generation disk grew past the type", err: db.ErrSnapshotDiskTooSmall,
			wantStatus: http.StatusConflict, wantCode: "type_too_small",
		},
		{
			name: "idempotency key reused", err: db.ErrIdempotencyReused,
			wantStatus: http.StatusConflict, wantCode: apierr.CodeIdempotencyKeyReused,
		},
		{
			name: "idempotency in progress", err: db.ErrIdempotencyInProgress,
			wantStatus: http.StatusConflict, wantCode: apierr.CodeIdempotencyInProgress, wantRetry: true,
		},
		{
			name: "admission returned a source-shaped sandbox",
			result: ForkResult{Sandbox: db.Sandbox{
				ID: "bx_23456789", OwnerID: coreTestOwner, State: db.StateProvisioning,
			}},
			wantStatus: http.StatusInternalServerError, wantCode: apierr.CodeInternal,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := &forkFakeStore{row: forkReadySource(coreTestOwner)}
			admission := &forkFakeAdmission{err: test.err, result: test.result}
			handler := forkTestHandler(t, store, admission, true)
			recorder := coreServe(handler, forkRequestBody("bx_23456789", test.body))
			if recorder.Code != test.wantStatus {
				t.Fatalf("status = %d, want %d (%s)", recorder.Code, test.wantStatus, recorder.Body.String())
			}
			coreAssertError(t, recorder, test.wantStatus, test.wantCode)
			if err := validator.Response("fork", recorder.Code, recorder.Header(), recorder.Body.Bytes()); err != nil {
				t.Fatalf("%d %s failed validation: %v", test.wantStatus, test.wantCode, err)
			}
			if test.wantRetry {
				body := coreDecodeError(t, recorder)
				if retry, _ := body.Error.Details["retryable"].(bool); !retry {
					t.Fatalf("retryable = %v, want true", body.Error.Details["retryable"])
				}
			}
		})
	}
}

// TestForkIdempotencyCarriesTheKeyAndRequestHash checks the header and exact
// body reach admission, which keys the replay on them.
func TestForkIdempotencyCarriesTheKeyAndRequestHash(t *testing.T) {
	body := `{"type":"small","ttlSeconds":900}`
	store := &forkFakeStore{row: forkReadySource(coreTestOwner)}
	admission := &forkFakeAdmission{}
	handler := forkTestHandler(t, store, admission, true)
	request := forkRequestBody("bx_23456789", body)
	request.Header.Set("Idempotency-Key", "fork-key-1")
	recorder := coreServe(handler, request)
	if recorder.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202 (%s)", recorder.Code, recorder.Body.String())
	}
	if len(admission.calls) != 1 {
		t.Fatalf("admission calls = %d", len(admission.calls))
	}
	got := admission.calls[0]
	if got.IdempotencyKey != "fork-key-1" {
		t.Fatalf("idempotency key = %q", got.IdempotencyKey)
	}
	if want := sha256.Sum256([]byte(body)); got.RequestSHA256 != want {
		t.Fatalf("request hash = %x, want %x", got.RequestSHA256, want)
	}

	// An omitted body hashes as the empty pinned object.
	recorder = coreServe(handler, forkRequestBody("bx_23456789", ""))
	if recorder.Code != http.StatusAccepted {
		t.Fatalf("omitted body status = %d, want 202", recorder.Code)
	}
	if want := sha256.Sum256([]byte("{}")); admission.calls[1].RequestSHA256 != want {
		t.Fatalf("omitted body hash = %x, want %x", admission.calls[1].RequestSHA256, want)
	}
}

// TestForkRequiresDependencies keeps the constructor strict.
func TestForkRequiresDependencies(t *testing.T) {
	if _, err := NewForkHandler(ForkOptions{Admission: &forkFakeAdmission{}}); err == nil {
		t.Error("NewForkHandler accepted a nil store")
	}
	if _, err := NewForkHandler(ForkOptions{Store: &forkFakeStore{}}); err == nil {
		t.Error("NewForkHandler accepted a nil admission")
	}
}

// TestForkSourceGenerationReadDisabledIsInternal checks a source-read fault
// is the approved 500 and never a fork.
func TestForkSourceGenerationReadDisabledIsInternal(t *testing.T) {
	validator := compat.Load(t)
	store := &forkFakeStore{row: forkReadySource(coreTestOwner), genErr: errors.New("boom")}
	admission := &forkFakeAdmission{}
	handler := forkTestHandler(t, store, admission, true)
	recorder := coreServe(handler, forkRequestBody("bx_23456789", ""))
	if recorder.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", recorder.Code)
	}
	if err := validator.Response("fork", recorder.Code, recorder.Header(), recorder.Body.Bytes()); err != nil {
		t.Fatalf("500 failed validation: %v", err)
	}
	if len(admission.calls) != 0 {
		t.Fatal("fork reached admission after a source read fault")
	}
}

// mapsEqual compares two string maps.
func mapsEqual(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for key, value := range a {
		if other, ok := b[key]; !ok || other != value {
			return false
		}
	}
	return true
}
