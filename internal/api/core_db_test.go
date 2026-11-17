package api

import (
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"uuid"

	"github.com/nathabonfim59/bamboo-raft/internal/apierr"
	"github.com/nathabonfim59/bamboo-raft/internal/db"
	"github.com/nathabonfim59/bamboo-raft/internal/db/dbtest"
	"github.com/nathabonfim59/bamboo-raft/internal/ids"
)

// corePGAdmission is the production-shaped adapter used only by the Postgres
// tests: it turns the API-owned request into the atomic store create. The real
// server adapter also chooses the node from the scheduler.
type corePGAdmission struct {
	store  *db.Store
	nodeID string
}

func (a corePGAdmission) Admit(ctx context.Context, request AdmissionRequest) (AdmissionResult, error) {
	result, err := a.store.CreateSandbox(ctx, db.CreateSandboxParams{
		ID:             request.SandboxID,
		Owner:          request.Owner,
		MachineType:    request.MachineType,
		Name:           request.Name,
		Labels:         request.Labels,
		Env:            request.Env,
		TTLSeconds:     request.TTLSeconds,
		NodeID:         a.nodeID,
		Reserve:        request.Resources,
		IdempotencyKey: request.IdempotencyKey,
		RequestSHA256:  request.RequestSHA256[:],
	})
	if err != nil {
		return AdmissionResult{}, err
	}
	return AdmissionResult{Sandbox: result.Sandbox, Replayed: result.Replayed}, nil
}

// corePGAccount creates one account and returns its id.
func corePGAccount(t *testing.T, store *db.Store) uuid.UUID {
	t.Helper()
	account, err := store.EnsureAccount(t.Context(), "core-"+uuid.NewV7().String())
	if err != nil {
		t.Fatalf("EnsureAccount: %v", err)
	}
	return account.ID
}

// corePGNode registers one node with room for several sandboxes.
func corePGNode(t *testing.T, store *db.Store, id string) {
	t.Helper()
	err := store.UpsertNodeInfo(t.Context(), db.NodeInfo{
		ID: id, CPUModel: "test-cpu", QEMUVersion: "9.2", DefaultImage: "img",
		Capacity:    db.Resources{VCPUs: 64, MemoryMiB: 64 << 10},
		DiskFreeGiB: 1000,
		SeenAt:      time.Now(),
	})
	if err != nil {
		t.Fatalf("UpsertNodeInfo: %v", err)
	}
}

// corePGHandlers wires the real store with the test admission adapter.
func corePGHandlers(t *testing.T, store *db.Store, admission Admission) Handlers {
	t.Helper()
	handlers, err := NewCoreHandlers(CoreOptions{Store: store, Admission: admission})
	if err != nil {
		t.Fatalf("NewCoreHandlers: %v", err)
	}
	return handlers
}

// corePGReadResponse decodes one successful handler body.
func corePGReadResponse[T any](t *testing.T, recorderBody []byte) T {
	t.Helper()
	var response T
	if err := json.Unmarshal(recorderBody, &response); err != nil {
		t.Fatalf("decode response failed (%d bytes)", len(recorderBody))
	}
	return response
}

func TestCorePostgresCreateIdempotencyAndDeletion(t *testing.T) {
	store := dbtest.Open(t)
	owner := corePGAccount(t, store)
	other := corePGAccount(t, store)
	nodeID := "core-node-a"
	corePGNode(t, store, nodeID)
	handlers := corePGHandlers(t, store, corePGAdmission{store: store, nodeID: nodeID})

	body := `{"type":"small","ttlSeconds":60,"name":"job","labels":{"session":"abc"},"env":{"FOO":"bar"},"failFast":true}`
	request := coreTestRequest(http.MethodPost, PathSandboxes, strings.NewReader(body), owner)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set(IdempotencyKeyHeader, "core-key")
	recorder := coreServe(handlers.CreateSandbox, request)
	if recorder.Code != http.StatusAccepted {
		t.Fatalf("create status = %d body = %s", recorder.Code, recorder.Body.String())
	}
	created := corePGReadResponse[CreateSandboxResponse](t, recorder.Body.Bytes())
	if !ids.ValidSandbox(created.Sandbox.ID) {
		t.Fatalf("sandbox id = %q", created.Sandbox.ID)
	}
	if created.Status != CreateProvisioning || created.TTLSeconds == nil || *created.TTLSeconds != 60 {
		t.Errorf("create response = %+v", created)
	}
	stored, err := store.Sandbox(t.Context(), owner, created.Sandbox.ID)
	if err != nil {
		t.Fatalf("Sandbox: %v", err)
	}
	if stored.State != db.StateProvisioning || stored.Name != "job" || stored.Labels["session"] != "abc" ||
		stored.Reserved.VCPUs != 2 || stored.ExpiresAt == nil {
		t.Errorf("stored = %+v", stored)
	}
	var exactTTL bool
	if err := store.Pool().QueryRow(t.Context(), `
		SELECT expires_at - created_at = interval '60 seconds'
		FROM sandboxes WHERE id = $1 AND owner_id = $2`,
		created.Sandbox.ID, owner).Scan(&exactTTL); err != nil {
		t.Fatalf("read stored ttl: %v", err)
	}
	if !exactTTL {
		t.Errorf("expires_at - created_at is not exactly 60 seconds")
	}

	// Same key and body replays the same id before placement.
	replayRequest := func() *http.Request {
		request := coreTestRequest(http.MethodPost, PathSandboxes, strings.NewReader(body), owner)
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set(IdempotencyKeyHeader, "core-key")
		return request
	}
	recorder = coreServe(handlers.CreateSandbox, replayRequest())
	if recorder.Code != http.StatusAccepted {
		t.Fatalf("replay status = %d body = %s", recorder.Code, recorder.Body.String())
	}
	replayed := corePGReadResponse[CreateSandboxResponse](t, recorder.Body.Bytes())
	if replayed.Sandbox.ID != created.Sandbox.ID {
		t.Errorf("replay id = %q, want %q", replayed.Sandbox.ID, created.Sandbox.ID)
	}
	if replayed.Status != CreateProvisioning {
		t.Errorf("replay status = %q", replayed.Status)
	}

	// A retry that finds the sandbox ready reports ready.
	if _, err := store.TransitionSandbox(t.Context(), db.TransitionParams{
		SandboxID:       created.Sandbox.ID,
		Owner:           owner,
		FromStates:      []db.SandboxState{db.StateProvisioning},
		ExpectedVersion: stored.Version,
		To:              db.StateReady,
	}); err != nil {
		t.Fatalf("TransitionSandbox: %v", err)
	}
	recorder = coreServe(handlers.CreateSandbox, replayRequest())
	if recorder.Code != http.StatusAccepted {
		t.Fatalf("ready replay status = %d body = %s", recorder.Code, recorder.Body.String())
	}
	ready := corePGReadResponse[CreateSandboxResponse](t, recorder.Body.Bytes())
	if ready.Status != CreateReady || ready.Sandbox.ID != created.Sandbox.ID {
		t.Errorf("ready replay = %+v", ready)
	}

	// Same key, different body conflicts and creates nothing.
	conflict := coreTestRequest(http.MethodPost, PathSandboxes, strings.NewReader(`{"type":"large"}`), owner)
	conflict.Header.Set("Content-Type", "application/json")
	conflict.Header.Set(IdempotencyKeyHeader, "core-key")
	recorder = coreServe(handlers.CreateSandbox, conflict)
	coreAssertError(t, recorder, http.StatusConflict, apierr.CodeIdempotencyKeyReused)
	var count int64
	if err := store.Pool().QueryRow(t.Context(), `SELECT count(*) FROM sandboxes WHERE owner_id = $1`, owner).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Errorf("sandbox count = %d, want 1", count)
	}

	// Foreign get is the same 404 as a missing id.
	foreignGet := coreTestRequest(http.MethodGet, PathSandbox, nil, other)
	foreignGet.SetPathValue(ParamSandboxID, created.Sandbox.ID)
	recorder = coreServe(handlers.GetSandbox, foreignGet)
	coreAssertError(t, recorder, http.StatusNotFound, apierr.CodeNotFound)

	// Delete requires the exact header, then accepts and is idempotent.
	deleteRequest := func() *http.Request {
		request := coreTestRequest(http.MethodDelete, PathSandbox, nil, owner)
		request.SetPathValue(ParamSandboxID, created.Sandbox.ID)
		request.Header.Set(ConfirmDeleteHeader, created.Sandbox.ID)
		return request
	}
	recorder = coreServe(handlers.DeleteSandbox, deleteRequest())
	if recorder.Code != http.StatusAccepted {
		t.Fatalf("delete status = %d body = %s", recorder.Code, recorder.Body.String())
	}
	deleted := corePGReadResponse[DeletionOperationResponse](t, recorder.Body.Bytes())
	if deleted.Type != TypeSandboxDeleting || deleted.Operation.TargetID != created.Sandbox.ID ||
		deleted.Operation.Status != DeletionPending || deleted.Operation.Reason != DeletionReasonExplicit {
		t.Errorf("delete response = %+v", deleted)
	}
	recorder = coreServe(handlers.DeleteSandbox, deleteRequest())
	if recorder.Code != http.StatusAccepted {
		t.Fatalf("repeat delete status = %d body = %s", recorder.Code, recorder.Body.String())
	}
	repeated := corePGReadResponse[DeletionOperationResponse](t, recorder.Body.Bytes())
	if repeated.Operation.ID != deleted.Operation.ID {
		t.Errorf("repeat delete operation = %q, want %q", repeated.Operation.ID, deleted.Operation.ID)
	}

	// Deleted rows leave every owner-visible list and lookup.
	if _, err := store.Sandbox(t.Context(), owner, created.Sandbox.ID); !errors.Is(err, db.ErrNotFound) {
		t.Errorf("deleted Sandbox error = %v, want ErrNotFound", err)
	}
	listRequest := coreTestRequest(http.MethodGet, PathSandboxes, nil, owner)
	recorder = coreServe(handlers.ListSandboxes, listRequest)
	list := corePGReadResponse[SandboxListResponse](t, recorder.Body.Bytes())
	if len(list.Sandboxes) != 0 {
		t.Errorf("list after delete = %+v", list.Sandboxes)
	}

	// Operation polling is owner-scoped and no-store.
	poll := coreTestRequest(http.MethodGet, PathDeletionOperation, nil, owner)
	poll.SetPathValue(ParamOperationID, deleted.Operation.ID)
	recorder = coreServe(handlers.GetDeletionOperation, poll)
	if recorder.Code != http.StatusOK || recorder.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("poll status = %d headers = %v body = %s", recorder.Code, recorder.Header(), recorder.Body.String())
	}
	foreignPoll := coreTestRequest(http.MethodGet, PathDeletionOperation, nil, other)
	foreignPoll.SetPathValue(ParamOperationID, deleted.Operation.ID)
	recorder = coreServe(handlers.GetDeletionOperation, foreignPoll)
	coreAssertError(t, recorder, http.StatusNotFound, apierr.CodeNotFound)

	// A replay still uses the original id even after soft deletion.
	afterDelete := coreTestRequest(http.MethodPost, PathSandboxes, strings.NewReader(body), owner)
	afterDelete.Header.Set("Content-Type", "application/json")
	afterDelete.Header.Set(IdempotencyKeyHeader, "core-key")
	recorder = coreServe(handlers.CreateSandbox, afterDelete)
	if recorder.Code != http.StatusAccepted {
		t.Fatalf("post-delete replay status = %d body = %s", recorder.Code, recorder.Body.String())
	}
	afterDeleteResponse := corePGReadResponse[CreateSandboxResponse](t, recorder.Body.Bytes())
	if afterDeleteResponse.Sandbox.ID != created.Sandbox.ID {
		t.Errorf("post-delete replay id = %q, want %q", afterDeleteResponse.Sandbox.ID, created.Sandbox.ID)
	}
}

// corePGInsertSandbox writes a list fixture row with an explicit creation
// time.
func corePGInsertSandbox(t *testing.T, store *db.Store, owner uuid.UUID, id, name string, state db.SandboxState, labels map[string]string, created time.Time) {
	t.Helper()
	raw, err := json.Marshal(labels)
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.Pool().Exec(t.Context(), `
		INSERT INTO sandboxes (id, owner_id, state, machine_type, name, labels, created_at, updated_at)
		VALUES ($1, $2, $3, 'default', $4, $5::jsonb, $6, $6)`,
		id, owner, state, name, string(raw), created)
	if err != nil {
		t.Fatalf("insert sandbox %s: %v", id, err)
	}
}

// corePGListPage calls the list handler and decodes one page.
func corePGListPage(t *testing.T, handler http.HandlerFunc, owner uuid.UUID, target string) SandboxListResponse {
	t.Helper()
	recorder := coreServe(handler, coreTestRequest(http.MethodGet, target, nil, owner))
	if recorder.Code != http.StatusOK {
		t.Fatalf("list status = %d body = %s", recorder.Code, recorder.Body.String())
	}
	return corePGReadResponse[SandboxListResponse](t, recorder.Body.Bytes())
}

func TestCorePostgresListPaginationFiltersAndOwnerScope(t *testing.T) {
	store := dbtest.Open(t)
	owner := corePGAccount(t, store)
	other := corePGAccount(t, store)
	handlers := corePGHandlers(t, store, &coreFakeAdmission{})

	created := time.Date(2026, 11, 9, 12, 0, 0, 123456000, time.UTC)
	idsA := []string{ids.Sandbox(), ids.Sandbox(), ids.Sandbox()}
	// Keep the expected order independent of generation order.
	for i := range idsA {
		for j := i + 1; j < len(idsA); j++ {
			if idsA[j] < idsA[i] {
				idsA[i], idsA[j] = idsA[j], idsA[i]
			}
		}
	}
	corePGInsertSandbox(t, store, owner, idsA[0], "alpha", db.StateReady, map[string]string{"team": "a"}, created)
	corePGInsertSandbox(t, store, owner, idsA[1], "beta", db.StateIdle, map[string]string{"team": "a"}, created)
	corePGInsertSandbox(t, store, owner, idsA[2], "gamma", db.StateProvisioning, map[string]string{"team": "b"}, created)
	otherID := ids.Sandbox()
	corePGInsertSandbox(t, store, other, otherID, "alpha", db.StateReady, map[string]string{"team": "a"}, created)

	// Descending, equal timestamps: id descending, one row per page.
	var gotDesc []string
	target := PathSandboxes + "?limit=1&sort=desc"
	for page := range 3 {
		response := corePGListPage(t, handlers.ListSandboxes, owner, target)
		if len(response.Sandboxes) != 1 {
			t.Fatalf("page %d rows = %d", page, len(response.Sandboxes))
		}
		id := response.Sandboxes[0].ID
		if id == otherID {
			t.Fatalf("owner A list leaked owner B row %s", id)
		}
		gotDesc = append(gotDesc, id)
		if page < 2 {
			if response.PageInfo.NextCursor == nil || !response.PageInfo.HasMore {
				t.Fatalf("page %d pageInfo = %+v", page, response.PageInfo)
			}
			target = PathSandboxes + "?limit=1&sort=desc&cursor=" + url.QueryEscape(*response.PageInfo.NextCursor)
		} else if response.PageInfo.NextCursor != nil || response.PageInfo.HasMore {
			t.Fatalf("last page pageInfo = %+v", response.PageInfo)
		}
	}
	wantDesc := []string{idsA[2], idsA[1], idsA[0]}
	if fmt.Sprint(gotDesc) != fmt.Sprint(wantDesc) {
		t.Errorf("descending pages = %v, want %v", gotDesc, wantDesc)
	}

	// Ascending, equal timestamps: id ascending.
	var gotAsc []string
	target = PathSandboxes + "?limit=1&sort=asc"
	for range 3 {
		response := corePGListPage(t, handlers.ListSandboxes, owner, target)
		gotAsc = append(gotAsc, response.Sandboxes[0].ID)
		if response.PageInfo.NextCursor != nil {
			target = PathSandboxes + "?limit=1&sort=asc&cursor=" + url.QueryEscape(*response.PageInfo.NextCursor)
		}
	}
	if fmt.Sprint(gotAsc) != fmt.Sprint(idsA) {
		t.Errorf("ascending ids = %v, want %v", gotAsc, idsA)
	}

	// A cursor from owner A never escapes the owner filter: owner B sees only
	// B's row (or nothing) but never an A row.
	firstPage := corePGListPage(t, handlers.ListSandboxes, owner, PathSandboxes+"?limit=1&sort=desc")
	if firstPage.PageInfo.NextCursor == nil {
		t.Fatal("expected a next cursor")
	}
	crossTarget := PathSandboxes + "?limit=10&sort=desc&cursor=" + url.QueryEscape(*firstPage.PageInfo.NextCursor)
	cross := corePGListPage(t, handlers.ListSandboxes, other, crossTarget)
	for _, sandbox := range cross.Sandboxes {
		if sandbox.ID != otherID {
			t.Errorf("cross-owner cursor escaped: %s", sandbox.ID)
		}
	}

	// Filters combine with AND and stay owner-scoped.
	filtered := corePGListPage(t, handlers.ListSandboxes, owner,
		PathSandboxes+"?state=ready&name=alpha&namePrefix=al&label=team%3Da")
	if len(filtered.Sandboxes) != 1 || filtered.Sandboxes[0].ID != idsA[0] {
		t.Errorf("filtered = %+v", filtered.Sandboxes)
	}
	if filtered.Sandboxes[0].URL != nil || filtered.Sandboxes[0].IP != nil {
		t.Errorf("connection fields = %+v", filtered.Sandboxes[0])
	}

	// Equal-timestamp descending order is exactly id descending.
	target = PathSandboxes + "?limit=10&sort=desc"
	all := corePGListPage(t, handlers.ListSandboxes, owner, target)
	var allIDs []string
	for _, sandbox := range all.Sandboxes {
		allIDs = append(allIDs, sandbox.ID)
	}
	wantAll := []string{idsA[2], idsA[1], idsA[0]}
	if fmt.Sprint(allIDs) != fmt.Sprint(wantAll) {
		t.Errorf("descending ids = %v, want %v", allIDs, wantAll)
	}
}
