package server

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"uuid"

	"github.com/nathabonfim59/bamboo-raft/internal/api"
	"github.com/nathabonfim59/bamboo-raft/internal/apierr"
	"github.com/nathabonfim59/bamboo-raft/internal/auth"
	"github.com/nathabonfim59/bamboo-raft/internal/compat"
	"github.com/nathabonfim59/bamboo-raft/internal/db"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
	"github.com/nathabonfim59/bamboo-raft/internal/scheduler"
	"github.com/nathabonfim59/bamboo-raft/internal/server/nodegateway"
)

func testLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// fakeAuthStore records bootstrap and last-used writes without PostgreSQL.
type fakeAuthStore struct {
	account     db.Account
	ensured     []string
	keyRecords  []db.APIKey
	touched     []uuid.UUID
	touchErr    error
	ensureErr   error
	ensureKeyFn func(accountID uuid.UUID, name, prefix string, digest [32]byte) (db.APIKey, error)
}

func (s *fakeAuthStore) EnsureAccount(_ context.Context, name string) (db.Account, error) {
	if s.ensureErr != nil {
		return db.Account{}, s.ensureErr
	}
	s.ensured = append(s.ensured, name)
	if s.account.ID == uuid.Nil() {
		s.account = db.Account{ID: uuid.NewV7(), Name: name}
	}
	return s.account, nil
}

func (s *fakeAuthStore) EnsureAPIKey(_ context.Context, accountID uuid.UUID, name, prefix string, digest [32]byte) (db.APIKey, error) {
	if s.ensureKeyFn != nil {
		return s.ensureKeyFn(accountID, name, prefix, digest)
	}
	key := db.APIKey{ID: uuid.NewV7(), AccountID: accountID, Name: name, Prefix: prefix, KeySHA256: digest}
	s.keyRecords = append(s.keyRecords, key)
	return key, nil
}

func (s *fakeAuthStore) TouchAPIKeyUsed(_ context.Context, id uuid.UUID) error {
	s.touched = append(s.touched, id)
	return s.touchErr
}

func TestBootstrapAuthMapsConfiguredNamesAndIDs(t *testing.T) {
	store := &fakeAuthStore{}
	keys, err := auth.NewKeys([]auth.KeyConfig{{ID: "main", Owner: "account-name", Raw: "bmb_secret"}})
	if err != nil {
		t.Fatal(err)
	}
	mapping, err := bootstrapAuth(t.Context(), store, keys, "bmb_", testLogger())
	if err != nil {
		t.Fatal(err)
	}
	if len(store.ensured) != 1 || store.ensured[0] != "account-name" {
		t.Fatalf("ensured accounts = %v", store.ensured)
	}
	if len(store.keyRecords) != 1 {
		t.Fatalf("ensured key count = %d, want 1", len(store.keyRecords))
	}
	if store.keyRecords[0].Name != "main" || store.keyRecords[0].Prefix != "bmb_" {
		t.Fatal("stored key name/prefix mismatch")
	}
	want := sha256.Sum256([]byte("bmb_secret"))
	if subtle.ConstantTimeCompare(store.keyRecords[0].KeySHA256[:], want[:]) != 1 {
		t.Fatal("stored digest is not the SHA-256 of the raw key")
	}

	var seen auth.Identity
	handler := mapping.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen, _ = auth.IdentityFrom(r.Context())
		w.WriteHeader(http.StatusNoContent)
	}))
	request := httptest.NewRequest(http.MethodGet, "/api/v1/me", nil)
	request = request.WithContext(auth.WithIdentity(request.Context(), auth.Identity{Owner: "account-name", KeyID: "main"}))
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusNoContent)
	}
	if seen.Owner != store.account.ID.String() || seen.KeyID != store.keyRecords[0].ID.String() {
		t.Fatal("mapped identity does not use durable UUIDs")
	}
	if len(store.touched) != 1 || store.touched[0] != store.keyRecords[0].ID {
		t.Fatalf("touched keys = %v", store.touched)
	}
}

func TestAuthMiddlewareFailuresAreSafe500(t *testing.T) {
	tests := []struct {
		name      string
		store     *fakeAuthStore
		identity  auth.Identity
		wantCode  int
		wantTouch int
	}{
		{"touch database failure", &fakeAuthStore{touchErr: errors.New("connection string secret")}, auth.Identity{Owner: "o", KeyID: "k"}, http.StatusInternalServerError, 1},
		{"unknown mapping", &fakeAuthStore{}, auth.Identity{Owner: "missing", KeyID: "k"}, http.StatusInternalServerError, 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			keys, err := auth.NewKeys([]auth.KeyConfig{{ID: tc.identity.KeyID, Owner: tc.identity.Owner, Raw: "bmb_secret"}})
			if err != nil {
				t.Fatal(err)
			}
			// Build the mapping first so the mapping table exists.
			mapping, err := bootstrapAuth(t.Context(), tc.store, keys, "bmb_", testLogger())
			if err != nil {
				t.Fatal(err)
			}
			called := false
			handler := mapping.Middleware(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { called = true }))
			request := httptest.NewRequest(http.MethodGet, "/api/v1/me", nil)
			if tc.name == "unknown mapping" {
				request = request.WithContext(auth.WithIdentity(request.Context(), auth.Identity{Owner: "other", KeyID: "missing"}))
			} else {
				request = request.WithContext(auth.WithIdentity(request.Context(), tc.identity))
			}
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, request)
			if recorder.Code != tc.wantCode {
				t.Fatalf("status = %d, want %d", recorder.Code, tc.wantCode)
			}
			if called {
				t.Fatal("handler ran after an authentication storage failure")
			}
			if len(tc.store.touched) != tc.wantTouch {
				t.Fatalf("touch calls = %d, want %d", len(tc.store.touched), tc.wantTouch)
			}
			if strings.Contains(recorder.Body.String(), "connection string secret") {
				t.Fatal("500 body leaks the storage error")
			}
			var body struct {
				Type string `json:"type"`
				Code string `json:"code"`
			}
			if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
				t.Fatalf("body is not an error envelope: %v", err)
			}
			if body.Type != "sandbox.error" || body.Code != "http_500" {
				t.Fatalf("unexpected error code %q/%q", body.Type, body.Code)
			}
		})
	}
}

func TestAdmissionMapsAPIRequest(t *testing.T) {
	placer := &fakePlacer{}
	owner := uuid.NewV7()
	ttl := 600
	hash := sha256.Sum256([]byte("{}"))
	result, err := (apiAdmission{placer: placer}).Admit(t.Context(), api.AdmissionRequest{
		Owner:          owner,
		SandboxID:      "bx_test",
		MachineType:    "small",
		Resources:      db.Resources{VCPUs: 2, MemoryMiB: 1024, DiskGiB: 10},
		Name:           "name",
		Labels:         map[string]string{"a": "b"},
		Env:            map[string]string{"C": "V"},
		TTLSeconds:     &ttl,
		FailFast:       true,
		IdempotencyKey: "key",
		RequestSHA256:  hash,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !result.Replayed || result.Sandbox.ID != "bx_test" {
		t.Fatalf("replayed=%v sandbox=%q", result.Replayed, result.Sandbox.ID)
	}
	got := placer.request
	if got.ID != "bx_test" || got.Owner != owner || got.MachineType != "small" {
		t.Fatalf("id/owner/machine mismatch: id=%q owner_match=%v machine=%q", got.ID, got.Owner == owner, got.MachineType)
	}
	if got.TTLSeconds == nil || *got.TTLSeconds != ttl || got.ExpiresAt != nil {
		t.Fatal("ttl was not forwarded as a duration without a wall-clock expiry")
	}
	if !got.FailFast || got.IdempotencyKey != "key" || string(got.RequestSHA256) != string(hash[:]) {
		t.Fatalf("failfast=%v key=%q hash_match=%v", got.FailFast, got.IdempotencyKey, string(got.RequestSHA256) == string(hash[:]))
	}
	if got.Reserve != (db.Resources{VCPUs: 2, MemoryMiB: 1024, DiskGiB: 10}) {
		t.Fatalf("reserve = %+v", got.Reserve)
	}
}

// fakeRestoreStore records the replay/resolve order.
type fakeRestoreStore struct {
	order      []string
	replayed   db.CreateSandboxResult
	replayDone bool
	replayErr  error
	source     db.SnapshotSource
	sourceErr  error
}

func (s *fakeRestoreStore) ReplayCreateSandbox(_ context.Context, _ uuid.UUID, _ string, _ []byte) (db.CreateSandboxResult, bool, error) {
	s.order = append(s.order, "replay")
	return s.replayed, s.replayDone, s.replayErr
}

func (s *fakeRestoreStore) SnapshotRestoreSource(_ context.Context, _ uuid.UUID, _ string) (db.SnapshotSource, error) {
	s.order = append(s.order, "resolve")
	return s.source, s.sourceErr
}

func TestAdmissionForwardsRestoreFields(t *testing.T) {
	owner := uuid.NewV7()
	placer := &fakePlacer{}
	admission := apiAdmission{placer: placer}
	_, err := admission.Admit(t.Context(), api.AdmissionRequest{
		Owner: owner, SandboxID: "bx_new", From: "base", TypeExplicit: true,
		MachineType: "large", IdempotencyKey: "key",
	})
	if err != nil {
		t.Fatal(err)
	}
	got := placer.request
	if got.From != "base" || !got.TypeExplicit || got.MachineType != "large" {
		t.Fatalf("forwarded from=%q explicit=%v machine=%q", got.From, got.TypeExplicit, got.MachineType)
	}
	if got.ID != "bx_new" || got.Owner != owner || got.IdempotencyKey != "key" {
		t.Fatalf("identity = %q owner_match=%v key=%q", got.ID, got.Owner == owner, got.IdempotencyKey)
	}
}

func TestSnapshotRestoreResolverMapsPolicy(t *testing.T) {
	owner := uuid.NewV7()
	source := db.SnapshotSource{
		RowID: uuid.NewV7(), OwnerID: owner, Name: "base",
		SourceSandboxID: "bx_source", ArtifactID: "snap-" + strings.Repeat("a", 32),
		NodeID: "node-a", MachineType: "large", DiskGiB: 111, Generation: 2,
	}
	store := &fakeRestoreStore{source: source}
	resolver := snapshotRestoreResolver{store: store}
	gotSource, machine, resources, err := resolver.ResolveRestore(t.Context(), owner, "base", false, "")
	if err != nil {
		t.Fatal(err)
	}
	if gotSource != source || machine != "large" || resources.DiskGiB != 111 {
		t.Fatalf("resolved row %s machine %q disk %d", gotSource.RowID, machine, resources.DiskGiB)
	}

	store.sourceErr = db.ErrSnapshotNotFound
	_, _, _, err = resolver.ResolveRestore(t.Context(), owner, "gone", false, "")
	apiErr, ok := errors.AsType[*apierr.Error](err)
	if !ok || apiErr.Status != http.StatusNotFound || apiErr.Code != apierr.CodeSnapshotNotFound {
		t.Fatalf("not found = %v", err)
	}
}

func TestAdmissionMapsNoNodeAndCapacity(t *testing.T) {
	admission := apiAdmission{placer: &fakePlacer{err: scheduler.ErrNoNode}}
	if _, err := admission.Admit(t.Context(), api.AdmissionRequest{}); !errors.Is(err, api.ErrNoReadyMachine) {
		t.Fatalf("ErrNoNode mapped to %v", err)
	}
	admission = apiAdmission{placer: &fakePlacer{err: db.ErrNoCapacity}}
	if _, err := admission.Admit(t.Context(), api.AdmissionRequest{}); !errors.Is(err, db.ErrNoCapacity) {
		t.Fatalf("ErrNoCapacity mapped to %v", err)
	}
}

// TestAdmissionNormalizesSchedulerSentinels pins the adapter seam: the
// scheduler's capacity and connectivity refusals must arrive at the API as
// the durable sentinels the pinned failFast policy already knows, including
// wrapped and joined forms. The classification is preserved alongside them
// (errors.Is still sees the scheduler sentinel and any context cause) so
// cancellation and diagnostics never get lost in translation.
func TestAdmissionNormalizesSchedulerSentinels(t *testing.T) {
	secret := "postgres://user:pass@host/db?token=SECRET"
	for _, test := range []struct {
		name string
		err  error
		want []error
		deny []error
	}{
		{
			name: "capacity",
			err:  scheduler.ErrNoCapacity,
			want: []error{db.ErrNoCapacity, scheduler.ErrNoCapacity},
		},
		{
			name: "wrapped capacity",
			err:  fmt.Errorf("place %s: %w", secret, scheduler.ErrNoCapacity),
			want: []error{db.ErrNoCapacity, scheduler.ErrNoCapacity},
		},
		{
			name: "joined capacity",
			err:  errors.Join(scheduler.ErrNoCapacity, errors.New(secret)),
			want: []error{db.ErrNoCapacity, scheduler.ErrNoCapacity},
			deny: []error{api.ErrNoReadyMachine},
		},
		{
			name: "no node",
			err:  scheduler.ErrNoNode,
			want: []error{api.ErrNoReadyMachine, scheduler.ErrNoNode},
			deny: []error{db.ErrNoCapacity},
		},
		{
			name: "wrapped no node",
			err:  fmt.Errorf("probe %s: %w", secret, scheduler.ErrNoNode),
			want: []error{api.ErrNoReadyMachine, scheduler.ErrNoNode},
		},
		{
			name: "no node deadline",
			err:  errors.Join(scheduler.ErrNoNode, context.DeadlineExceeded),
			want: []error{api.ErrNoReadyMachine, scheduler.ErrNoNode, context.DeadlineExceeded},
			deny: []error{db.ErrNoCapacity},
		},
		{
			name: "no node cancel",
			err:  errors.Join(scheduler.ErrNoNode, context.Canceled),
			want: []error{api.ErrNoReadyMachine, scheduler.ErrNoNode, context.Canceled},
			deny: []error{db.ErrNoCapacity},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			admission := apiAdmission{placer: &fakePlacer{err: test.err}}
			_, err := admission.Admit(t.Context(), api.AdmissionRequest{})
			if err == nil {
				t.Fatal("Admit accepted a sentinel failure")
			}
			for _, want := range test.want {
				if !errors.Is(err, want) {
					t.Errorf("errors.Is(%v) = false, want true", want)
				}
			}
			for _, deny := range test.deny {
				if errors.Is(err, deny) {
					t.Errorf("errors.Is(%v) = true, want false", deny)
				}
			}
		})
	}
}

// admissionHTTPStore satisfies api.CoreStore for the create refusal paths,
// which never read the store. Any accidental read fails the test loudly.
type admissionHTTPStore struct{ t *testing.T }

func (s admissionHTTPStore) Sandbox(context.Context, uuid.UUID, string) (db.Sandbox, error) {
	s.t.Fatal("create refusal path read the sandbox store")
	return db.Sandbox{}, db.ErrNotFound
}

func (s admissionHTTPStore) ListSandboxes(context.Context, db.SandboxListQuery) ([]db.Sandbox, *db.PageCursor, error) {
	s.t.Fatal("create refusal path listed sandboxes")
	return nil, nil, db.ErrNotFound
}

func (s admissionHTTPStore) AcceptSandboxDeletion(context.Context, uuid.UUID, string, db.DeletionReason) (db.DeletionOperation, db.Sandbox, error) {
	s.t.Fatal("create refusal path accepted a deletion")
	return db.DeletionOperation{}, db.Sandbox{}, db.ErrNotFound
}

func (s admissionHTTPStore) DeletionOperation(context.Context, uuid.UUID, string) (db.DeletionOperation, error) {
	s.t.Fatal("create refusal path read a deletion operation")
	return db.DeletionOperation{}, db.ErrNotFound
}

// TestAdmissionCapacityHTTPBytesPinnedAndSubset drives a scheduler capacity
// refusal through the real API handlers and validates the recorded response
// bytes against the v1 API document. Both
// failFast modes must keep their existing 503 pair and positive retry hint,
// and the scheduler's own text (or anything wrapped with it) must never
// reach the wire.
func TestAdmissionCapacityHTTPBytesPinnedAndSubset(t *testing.T) {
	const secret = "postgres://user:pass@host/db?token=SECRET"
	v1, _ := runTestValidators(t)
	validators := map[string]*compat.Validator{"v1": v1}
	for _, test := range []struct {
		name string
		body string
		err  error
		code string
	}{
		{"fail fast capacity", `{"failFast":true}`, scheduler.ErrNoCapacity, apierr.CodeNoReadyMachine},
		{"queued capacity", `{}`, scheduler.ErrNoCapacity, apierr.CodeOutOfCapacity},
		{"fail fast wrapped capacity", `{"failFast":true}`, fmt.Errorf("reserve %s: %w", secret, scheduler.ErrNoCapacity), apierr.CodeNoReadyMachine},
		{"queued joined capacity", `{}`, errors.Join(scheduler.ErrNoCapacity, errors.New(secret)), apierr.CodeOutOfCapacity},
	} {
		t.Run(test.name, func(t *testing.T) {
			handlers, err := api.NewCoreHandlers(api.CoreOptions{
				Store:     admissionHTTPStore{t: t},
				Admission: apiAdmission{placer: &fakePlacer{err: test.err}},
			})
			if err != nil {
				t.Fatalf("NewCoreHandlers: %v", err)
			}
			request := httptest.NewRequest(http.MethodPost, api.PathSandboxes, strings.NewReader(test.body))
			request = request.WithContext(auth.WithIdentity(request.Context(),
				auth.Identity{Owner: uuid.NewV7().String(), KeyID: uuid.NewV7().String()}))
			recorder := httptest.NewRecorder()
			handlers.CreateSandbox(recorder, request)

			if recorder.Code != http.StatusServiceUnavailable {
				t.Fatalf("status = %d, want 503", recorder.Code)
			}
			// Sweep the actual bytes for cause text before any validator,
			// decoder or field diagnostic: those may echo the body.
			body := recorder.Body.Bytes()
			for _, leak := range []string{"scheduler", secret, "user:pass"} {
				if strings.Contains(string(body), leak) {
					t.Fatalf("response body leaked cause text")
				}
			}
			for name, validator := range validators {
				if err := validator.Response("create", recorder.Code, recorder.Header(), body); err != nil {
					t.Errorf("%s create %d failed validation", name, recorder.Code)
				}
			}
			var envelope struct {
				Status int    `json:"status"`
				Code   string `json:"code"`
				Error  struct {
					Code    string         `json:"code"`
					Status  int            `json:"status"`
					Details map[string]any `json:"details"`
				} `json:"error"`
			}
			if err := json.Unmarshal(body, &envelope); err != nil {
				t.Fatalf("response is not JSON")
			}
			if envelope.Status != http.StatusServiceUnavailable || envelope.Error.Status != http.StatusServiceUnavailable {
				t.Errorf("envelope status = %d/%d, want 503", envelope.Status, envelope.Error.Status)
			}
			if envelope.Code != test.code || envelope.Error.Code != test.code {
				t.Errorf("code = %q/%q, want %q", envelope.Code, envelope.Error.Code, test.code)
			}
			retry, ok := envelope.Error.Details["retryAfterSeconds"].(float64)
			if !ok || retry <= 0 {
				t.Errorf("retryAfterSeconds = %v, want a positive hint", envelope.Error.Details["retryAfterSeconds"])
			}
		})
	}
}

type fakePlacer struct {
	request scheduler.AdmitRequest
	err     error
}

func (p *fakePlacer) Admit(_ context.Context, request scheduler.AdmitRequest) (db.CreateSandboxResult, error) {
	p.request = request
	if p.err != nil {
		return db.CreateSandboxResult{}, p.err
	}
	return db.CreateSandboxResult{Sandbox: db.Sandbox{ID: request.ID}, Replayed: true}, nil
}

type fakeCommandStore struct {
	sandbox db.Sandbox
	err     error
}

func (s fakeCommandStore) Sandbox(context.Context, uuid.UUID, string) (db.Sandbox, error) {
	return s.sandbox, s.err
}

func TestCommandTargetLookup(t *testing.T) {
	nodeID := "node-a"
	owner := uuid.NewV7()
	target, err := (commandTargets{store: fakeCommandStore{sandbox: db.Sandbox{NodeID: &nodeID, State: db.StateReady}}}).TargetLookup(t.Context(), owner.String(), "bx_test")
	if err != nil {
		t.Fatal(err)
	}
	if target.NodeID != nodeID || target.State != api.StateReady {
		t.Fatalf("target = %+v", target)
	}

	if _, err := (commandTargets{store: fakeCommandStore{err: db.ErrNotFound}}).TargetLookup(t.Context(), owner.String(), "bx_missing"); !errors.Is(err, api.ErrCommandTargetNotFound) {
		t.Fatalf("missing target error = %v", err)
	}
	if _, err := (commandTargets{store: fakeCommandStore{err: errors.New("sql detail")}}).TargetLookup(t.Context(), owner.String(), "bx_test"); errors.Is(err, api.ErrCommandTargetNotFound) {
		t.Fatalf("internal error became missing target: %v", err)
	}
	if _, err := (commandTargets{store: fakeCommandStore{}}).TargetLookup(t.Context(), "not-a-uuid", "bx_test"); err == nil || errors.Is(err, api.ErrCommandTargetNotFound) {
		t.Fatalf("malformed owner error = %v", err)
	}
}

type fakeInventoryStore struct {
	upserts   []db.NodeInfo
	upsertErr error
	touches   []struct {
		id string
		at time.Time
	}
	touchErr error
}

func (s *fakeInventoryStore) UpsertNodeInfo(_ context.Context, info db.NodeInfo) error {
	s.upserts = append(s.upserts, info)
	return s.upsertErr
}

func (s *fakeInventoryStore) TouchNodeSeen(_ context.Context, id string, at time.Time) error {
	s.touches = append(s.touches, struct {
		id string
		at time.Time
	}{id, at})
	return s.touchErr
}

func TestInventoryWriter(t *testing.T) {
	now := time.Date(2026, 11, 9, 12, 0, 0, 0, time.UTC)
	store := &fakeInventoryStore{}
	writer := inventoryWriter{store: store, now: func() time.Time { return now }}

	err := writer.WriteNode(t.Context(), nodegateway.NodeState{
		NodeID: "node-a", Connected: true, ConnectedAt: now.Add(-time.Hour),
		Info: &nodev1.InfoResponse{NodeId: "node-a", Vcpus: 4, MemoryMib: 4096, DiskFreeMib: 20480, CpuModel: "x86-64-v3", QemuVersion: "10.0", DefaultImage: "base"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(store.upserts) != 1 {
		t.Fatalf("upserts = %+v", store.upserts)
	}
	got := store.upserts[0]
	if got.ID != "node-a" || got.Capacity != (db.Resources{VCPUs: 4, MemoryMiB: 4096}) || got.DiskFreeGiB != 20 || !got.SeenAt.Equal(now) {
		t.Fatalf("node info = %+v", got)
	}
	if len(store.touches) != 0 {
		t.Fatalf("healthy info touched staleness: %+v", store.touches)
	}

	// Info failure on a live link and a disconnect both stop placement
	// immediately, regardless of how fresh ConnectedAt is.
	for _, state := range []nodegateway.NodeState{
		{NodeID: "node-a", Connected: true, ConnectedAt: now},
		{NodeID: "node-a", Connected: false, ConnectedAt: now},
	} {
		if err := writer.WriteNode(t.Context(), state); err != nil {
			t.Fatal(err)
		}
	}
	if len(store.touches) != 2 {
		t.Fatalf("touches = %+v", store.touches)
	}
	for _, touch := range store.touches {
		if !touch.at.Equal(time.Unix(0, 0).UTC()) {
			t.Fatalf("staleness time = %v, want Unix epoch", touch.at)
		}
	}
}

func TestInventoryWriterMismatchedNode(t *testing.T) {
	const sentinel = "token-env-SENTINEL-node"
	store := &fakeInventoryStore{}
	writer := inventoryWriter{store: store, now: time.Now}
	for _, info := range []*nodev1.InfoResponse{{NodeId: sentinel}, {}} {
		err := writer.WriteNode(t.Context(), nodegateway.NodeState{
			NodeID: "node-a", Connected: true, Info: info,
		})
		if err == nil {
			t.Fatal("accepted inventory from a mismatched node")
		}
		if errors.Unwrap(err) != nil || strings.Contains(err.Error(), sentinel) {
			t.Fatal("mismatch error is not a fixed safe message")
		}
	}
	if len(store.upserts) != 0 || len(store.touches) != 0 {
		t.Fatalf("wrote mismatched state: %d upserts, %d touches", len(store.upserts), len(store.touches))
	}
}

func TestInventoryWriterErrorsAreSafe(t *testing.T) {
	const sentinel = "postgres://user:SENTINEL-password@example.test/db"
	writer := inventoryWriter{store: &fakeInventoryStore{
		upsertErr: errors.New(sentinel),
		touchErr:  errors.New(sentinel),
	}, now: time.Now}
	for _, state := range []nodegateway.NodeState{
		{NodeID: "node-a", Connected: true, Info: &nodev1.InfoResponse{NodeId: "node-a"}},
		{NodeID: "node-a", Connected: false},
	} {
		err := writer.WriteNode(t.Context(), state)
		if err == nil {
			t.Fatal("accepted a failing inventory store")
		}
		if errors.Unwrap(err) != nil {
			t.Fatal("inventory error still wraps the store cause")
		}
		if strings.Contains(err.Error(), sentinel) || strings.Contains(err.Error(), "SENTINEL") {
			t.Fatal("inventory error leaks the store cause")
		}
	}
}

func TestInventoryWriterUnknownDisconnectIsBenign(t *testing.T) {
	store := &fakeInventoryStore{touchErr: db.ErrNotFound}
	writer := inventoryWriter{store: store, now: time.Now}
	if err := writer.WriteNode(t.Context(), nodegateway.NodeState{NodeID: "missing"}); err != nil {
		t.Fatalf("unknown offline node = %v", err)
	}
}

// blockingRestoreStore blocks the owner-scoped source lookup until the
// admission context ends. Replay always misses and ignores cancellation so the
// test reaches the resolver-error path even after a caller cancel.
type blockingRestoreStore struct {
	started chan struct{}
}

func (s *blockingRestoreStore) ReplayCreateSandbox(context.Context, uuid.UUID, string, []byte) (db.CreateSandboxResult, bool, error) {
	return db.CreateSandboxResult{}, false, nil
}

func (s *blockingRestoreStore) NodesWithCapacity(context.Context, db.Resources, time.Duration) ([]db.Node, error) {
	return nil, errors.New("restore wiring test must not list nodes")
}

func (s *blockingRestoreStore) CreateSandbox(context.Context, db.CreateSandboxParams) (db.CreateSandboxResult, error) {
	return db.CreateSandboxResult{}, errors.New("restore wiring test must not create")
}

// Sandbox and AcceptSandboxResume satisfy the resume-admission surface the
// scheduler now consumes; the restore wiring test never resumes.
func (s *blockingRestoreStore) Sandbox(context.Context, uuid.UUID, string) (db.Sandbox, error) {
	return db.Sandbox{}, errors.New("restore wiring test must not read sandboxes")
}

func (s *blockingRestoreStore) AcceptSandboxResume(context.Context, db.ResumeParams) (db.ResumeAcceptance, error) {
	return db.ResumeAcceptance{}, errors.New("restore wiring test must not resume")
}

func (s *blockingRestoreStore) SnapshotRestoreSource(ctx context.Context, _ uuid.UUID, _ string) (db.SnapshotSource, error) {
	select {
	case <-s.started:
	default:
		close(s.started)
	}
	<-ctx.Done()
	return db.SnapshotSource{}, ctx.Err()
}

// wiredRestoreAdmission builds the real apiAdmission -> scheduler ->
// snapshotRestoreResolver chain; only the durable store and node dial are
// fakes, so the resolver adapter is not bypassed.
func wiredRestoreAdmission(t *testing.T, store *blockingRestoreStore, budget time.Duration) apiAdmission {
	t.Helper()
	placer, err := scheduler.New(scheduler.Options{
		Store: store, Log: testLogger(), Restore: snapshotRestoreResolver{store: store},
		Dial: func(string) (scheduler.NodeClient, io.Closer, error) {
			return nil, nil, errors.New("node dial must not run")
		},
		FailFastBudget: budget,
	})
	if err != nil {
		t.Fatalf("scheduler.New: %v", err)
	}
	return apiAdmission{placer: placer}
}

func TestRestoreLookupDeadlineMapsToNoReadyMachine(t *testing.T) {
	store := &blockingRestoreStore{started: make(chan struct{})}
	admission := wiredRestoreAdmission(t, store, 50*time.Millisecond)
	start := time.Now()
	_, err := admission.Admit(t.Context(), api.AdmissionRequest{
		Owner: uuid.NewV7(), SandboxID: "bx_new", From: "base", FailFast: true, IdempotencyKey: "key",
	})
	if elapsed := time.Since(start); elapsed >= time.Second {
		t.Fatalf("blocked lookup elapsed = %v", elapsed)
	}
	if !errors.Is(err, api.ErrNoReadyMachine) {
		t.Fatalf("lookup deadline = %T %v, want ErrNoReadyMachine", err, err)
	}
}

func TestRestoreLookupCallerCancelIsPreserved(t *testing.T) {
	store := &blockingRestoreStore{started: make(chan struct{})}
	admission := wiredRestoreAdmission(t, store, time.Second)
	ctx, cancel := context.WithCancel(t.Context())
	finished := make(chan struct{})
	t.Cleanup(func() {
		cancel()
		<-finished
	})
	go func() {
		defer close(finished)
		select {
		case <-store.started:
			cancel()
		case <-ctx.Done():
		}
	}()
	_, err := admission.Admit(ctx, api.AdmissionRequest{
		Owner: uuid.NewV7(), SandboxID: "bx_new", From: "base", IdempotencyKey: "key",
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("caller cancel = %T %v, want context.Canceled", err, err)
	}
	if errors.Is(err, api.ErrNoReadyMachine) {
		t.Fatal("caller cancel was classified as a capacity refusal")
	}
}

func (s *fakeRestoreStore) CreateSandboxes(context.Context, []db.CreateSandboxParams) ([]db.CreateSandboxResult, error) {
	return nil, errors.New("fakeRestoreStore: batch create not used")
}

func (s *fakeRestoreStore) ReplayCreateSandboxBatch(context.Context, uuid.UUID, string, []byte) ([]db.CreateSandboxResult, bool, error) {
	return nil, false, nil
}

func (s *blockingRestoreStore) CreateSandboxes(context.Context, []db.CreateSandboxParams) ([]db.CreateSandboxResult, error) {
	return nil, errors.New("blockingRestoreStore: batch create not used")
}

func (s *blockingRestoreStore) ReplayCreateSandboxBatch(context.Context, uuid.UUID, string, []byte) ([]db.CreateSandboxResult, bool, error) {
	return nil, false, nil
}
