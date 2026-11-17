package api

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"uuid"

	"github.com/nathabonfim59/bamboo-raft/internal/apierr"
	"github.com/nathabonfim59/bamboo-raft/internal/db"
	"github.com/nathabonfim59/bamboo-raft/internal/db/dbtest"
	"github.com/nathabonfim59/bamboo-raft/internal/ids"
	"github.com/nathabonfim59/bamboo-raft/internal/machinetype"
)

// lifecyclePGResume is the production-shaped resume seam over the accepted
// store, used by the Postgres handler tests.
type lifecyclePGResume struct {
	store *db.Store
}

func (r lifecyclePGResume) Resume(ctx context.Context, request ResumeRequest) (ResumeResult, error) {
	accepted, err := r.store.AcceptSandboxResume(ctx, db.ResumeParams{
		Owner:           request.Owner,
		SandboxID:       request.SandboxID,
		MachineType:     request.MachineType,
		Resources:       request.Resources,
		Env:             request.Env,
		TTL:             request.TTL,
		SupersedeStopID: request.SupersedeStopID,
	})
	if err != nil {
		return ResumeResult{}, err
	}
	return ResumeResult{Sandbox: accepted.Sandbox, Superseded: accepted.Superseded}, nil
}

// lifecyclePGHandlers wires the real store to both constructor paths.
func lifecyclePGHandlers(t *testing.T, store *db.Store) (Handlers, Handlers) {
	t.Helper()
	core, err := NewCoreHandlers(CoreOptions{Store: store, Admission: &coreFakeAdmission{}})
	if err != nil {
		t.Fatalf("NewCoreHandlers: %v", err)
	}
	lifecycle, err := NewLifecycleHandlers(LifecycleOptions{
		Store:  store,
		Resume: lifecyclePGResume{store: store},
	})
	if err != nil {
		t.Fatalf("NewLifecycleHandlers: %v", err)
	}
	return core, lifecycle
}

// lifecyclePGInsert writes one sandbox fixture row. A stopped sandbox keeps
// its disk reservation but releases compute; a live one holds both.
func lifecyclePGInsert(t *testing.T, store *db.Store, owner uuid.UUID, id string, state db.SandboxState, nodeID *string, desired db.Resources, ttl *int) {
	t.Helper()
	reserved := desired
	if state == db.StateArchived {
		reserved.VCPUs = 0
		reserved.MemoryMiB = 0
	}
	_, err := store.Pool().Exec(t.Context(), `
		INSERT INTO sandboxes (
			id, owner_id, state, machine_type, name, labels, node_id,
			reserved_vcpus, reserved_memory_mib, reserved_disk_gib,
			desired_vcpus, desired_memory_mib, desired_disk_gib,
			ttl_seconds, created_at, updated_at
		) VALUES ($1, $2, $3, 'default', 'fixture', '{}'::jsonb, $4,
			$5, $6, $7, $8, $9, $10, $11, now(), now())`,
		id, owner, state, nodeID,
		reserved.VCPUs, reserved.MemoryMiB, reserved.DiskGiB,
		desired.VCPUs, desired.MemoryMiB, desired.DiskGiB, ttl)
	if err != nil {
		t.Fatalf("insert sandbox %s: %v", id, err)
	}
}

func TestLifecyclePostgresPatchAndOwnerScope(t *testing.T) {
	store := dbtest.Open(t)
	owner := corePGAccount(t, store)
	other := corePGAccount(t, store)
	id := ids.Sandbox()
	lifecyclePGInsert(t, store, owner, id, db.StateReady, nil, db.Resources{}, new(3600))
	_, handlers := lifecyclePGHandlers(t, store)

	patch := func(owner uuid.UUID, body string) *httptest.ResponseRecorder {
		t.Helper()
		request := lifecycleSetPath(coreTestRequest(http.MethodPatch, PathSandbox, strings.NewReader(body), owner), id, "")
		request.Header.Set("Content-Type", "application/json")
		return coreServe(handlers.UpdateSandbox, request)
	}

	recorder := patch(owner, `{"name":"renamed","ttlSeconds":7200,"subdomain":"acme-staging"}`)
	if recorder.Code != http.StatusOK {
		t.Fatalf("patch status = %d body = %s", recorder.Code, recorder.Body.String())
	}
	response := lifecycleDecode[SandboxInfoResponse](t, recorder.Body.Bytes())
	if response.Sandbox.Name != "renamed" || response.Sandbox.Subdomain == nil || *response.Sandbox.Subdomain != "acme-staging" ||
		response.Sandbox.TTLSeconds == nil || *response.Sandbox.TTLSeconds != 7200 {
		t.Errorf("patch response = %+v", response.Sandbox)
	}
	row, err := store.Sandbox(t.Context(), owner, id)
	if err != nil {
		t.Fatalf("Sandbox: %v", err)
	}
	if row.Name != "renamed" || row.Subdomain == nil || *row.Subdomain != "acme-staging" ||
		row.TTLSeconds == nil || *row.TTLSeconds != 7200 || row.ExpiresAt == nil {
		t.Errorf("stored row = %+v", row)
	}

	// Omitted TTL keeps the configured duration; null clears both.
	if recorder := patch(owner, `{"name":"renamed-2"}`); recorder.Code != http.StatusOK {
		t.Fatalf("omit ttl status = %d body = %s", recorder.Code, recorder.Body.String())
	}
	row, err = store.Sandbox(t.Context(), owner, id)
	if err != nil || row.TTLSeconds == nil || *row.TTLSeconds != 7200 {
		t.Fatalf("omitted ttl row = %+v err = %v", row, err)
	}
	if recorder := patch(owner, `{"ttlSeconds":null}`); recorder.Code != http.StatusOK {
		t.Fatalf("null ttl status = %d body = %s", recorder.Code, recorder.Body.String())
	}
	row, err = store.Sandbox(t.Context(), owner, id)
	if err != nil || row.TTLSeconds != nil || row.ExpiresAt != nil {
		t.Fatalf("null ttl row = %+v err = %v", row, err)
	}

	// A foreign owner gets the same 404 as a missing id and no row changes.
	recorder = patch(other, `{"name":"stolen"}`)
	coreAssertError(t, recorder, http.StatusNotFound, apierr.CodeNotFound)
	row, err = store.Sandbox(t.Context(), owner, id)
	if err != nil || row.Name != "renamed-2" {
		t.Fatalf("foreign patch changed row = %+v err = %v", row, err)
	}

	// A subdomain is globally unique among live sandboxes.
	second := ids.Sandbox()
	lifecyclePGInsert(t, store, owner, second, db.StateReady, nil, db.Resources{}, nil)
	secondPatch := lifecycleSetPath(coreTestRequest(http.MethodPatch, PathSandbox, strings.NewReader(`{"subdomain":"acme-staging"}`), owner), second, "")
	recorder = coreServe(handlers.UpdateSandbox, secondPatch)
	coreAssertError(t, recorder, http.StatusBadRequest, apierr.CodeInvalidJSON)

	// Unknown and malformed requests never reach the store.
	for body, code := range map[string]string{
		`{"name":""}`:          apierr.CodeInvalidName,
		`{"ttlSeconds":0}`:     apierr.CodeInvalidJSON,
		`{"subdomain":"ACME"}`: apierr.CodeInvalidJSON,
		`{"subdomain":null}`:   apierr.CodeInvalidJSON,
		`{"unknown":true}`:     apierr.CodeInvalidJSON,
	} {
		recorder = patch(owner, body)
		coreAssertError(t, recorder, http.StatusBadRequest, code)
	}
}

func TestLifecyclePostgresStopReplayForceAndStatus(t *testing.T) {
	store := dbtest.Open(t)
	owner := corePGAccount(t, store)
	other := corePGAccount(t, store)
	node := "lifecycle-node"
	corePGNode(t, store, node)
	id := ids.Sandbox()
	lifecyclePGInsert(t, store, owner, id, db.StateReady, &node, db.Resources{VCPUs: 4, MemoryMiB: 8192, DiskGiB: 51}, new(3600))
	_, handlers := lifecyclePGHandlers(t, store)

	stop := func(owner uuid.UUID, body string) *httptest.ResponseRecorder {
		t.Helper()
		request := lifecycleSetPath(coreTestRequest(http.MethodPost, PathSandboxStop, strings.NewReader(body), owner), id, "")
		return coreServe(handlers.StopSandbox, request)
	}

	recorder := stop(owner, `{}`)
	if recorder.Code != http.StatusAccepted {
		t.Fatalf("stop status = %d body = %s", recorder.Code, recorder.Body.String())
	}
	first := lifecycleDecode[SandboxActionResponse](t, recorder.Body.Bytes())
	if first.Type != TypeSandboxStopping || first.Status != "archiving" || first.Operation == nil {
		t.Fatalf("stop response = %+v", first)
	}
	if first.StatusURL == nil || *first.StatusURL != "/api/v1/sandboxes/"+id+"/stops/"+first.Operation.ID {
		t.Errorf("statusUrl = %v", first.StatusURL)
	}
	row, err := store.Sandbox(t.Context(), owner, id)
	if err != nil || row.State != db.StateArchiving || row.StopOperationID == nil || *row.StopOperationID != first.Operation.ID {
		t.Fatalf("archiving row = %+v err = %v", row, err)
	}
	var jobs int
	if err := store.Pool().QueryRow(t.Context(), `
		SELECT count(*) FROM jobs WHERE owner_id = $1 AND sandbox_id = $2 AND kind = 'sandbox.stop'`,
		owner, id).Scan(&jobs); err != nil {
		t.Fatal(err)
	}
	if jobs != 1 {
		t.Errorf("stop jobs = %d, want 1", jobs)
	}

	// A repeat returns the same operation and enqueues nothing new.
	recorder = stop(owner, `{}`)
	if recorder.Code != http.StatusAccepted {
		t.Fatalf("repeat stop status = %d body = %s", recorder.Code, recorder.Body.String())
	}
	repeated := lifecycleDecode[SandboxActionResponse](t, recorder.Body.Bytes())
	if repeated.Operation == nil || repeated.Operation.ID != first.Operation.ID {
		t.Errorf("repeat operation = %+v, want %s", repeated.Operation, first.Operation.ID)
	}
	if err := store.Pool().QueryRow(t.Context(), `
		SELECT count(*) FROM jobs WHERE owner_id = $1 AND sandbox_id = $2 AND kind = 'sandbox.stop'`,
		owner, id).Scan(&jobs); err != nil {
		t.Fatal(err)
	}
	if jobs != 1 {
		t.Errorf("repeat stop jobs = %d, want 1", jobs)
	}

	// force escalates the canonical pending operation.
	recorder = stop(owner, `{"force":true}`)
	if recorder.Code != http.StatusAccepted {
		t.Fatalf("force stop status = %d body = %s", recorder.Code, recorder.Body.String())
	}
	forced := lifecycleDecode[SandboxActionResponse](t, recorder.Body.Bytes())
	if forced.Forced == nil || !*forced.Forced {
		t.Errorf("forced = %v", forced.Forced)
	}
	operation, err := store.StopOperation(t.Context(), owner, id, first.Operation.ID)
	if err != nil || !operation.ForceRequested {
		t.Fatalf("operation force = %+v err = %v", operation, err)
	}

	// A foreign stop lookup is the same 404 and cannot read the operation.
	foreign := lifecycleSetPath(coreTestRequest(http.MethodGet, PathSandboxStops, nil, other), id, first.Operation.ID)
	coreAssertError(t, coreServe(handlers.GetSandboxStop, foreign), http.StatusNotFound, apierr.CodeNotFound)

	// The status URL reads the stop by id and keeps requestedAt.
	statusRequest := lifecycleSetPath(coreTestRequest(http.MethodGet, PathSandboxStops, nil, owner), id, first.Operation.ID)
	recorder = coreServe(handlers.GetSandboxStop, statusRequest)
	if recorder.Code != http.StatusOK || recorder.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("status = %d headers = %v body = %s", recorder.Code, recorder.Header(), recorder.Body.String())
	}
	status := lifecycleDecode[StopOperationResponse](t, recorder.Body.Bytes())
	if status.Type != TypeStopStatus || status.ID != id || status.Stop.ID != first.Operation.ID ||
		status.Stop.Status != StopPending || status.Stop.RequestedAt == nil {
		t.Errorf("status response = %+v", status)
	}

	// Simulate the worker finishing the stop: a later stop is 200 with the
	// last completed operation and a null requestedAt.
	if _, err := store.Pool().Exec(t.Context(), `
		UPDATE sandboxes SET state = 'archived', stopped_at = now(), version = version + 1
		WHERE id = $1 AND owner_id = $2`, id, owner); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Pool().Exec(t.Context(), `
		UPDATE stop_operations SET status = 'completed', ended_at = now(), last_attempt_at = now()
		WHERE id = $1 AND owner_id = $2`, first.Operation.ID, owner); err != nil {
		t.Fatal(err)
	}
	recorder = stop(owner, `{}`)
	if recorder.Code != http.StatusOK {
		t.Fatalf("archived stop status = %d body = %s", recorder.Code, recorder.Body.String())
	}
	archived := lifecycleDecode[SandboxActionResponse](t, recorder.Body.Bytes())
	if archived.Type != TypeSandboxStopped || archived.Status != "archived" ||
		archived.Operation == nil || archived.Operation.RequestedAt != nil {
		t.Errorf("archived response = %+v", archived)
	}
	if archived.Sandbox == nil || archived.Sandbox.StoppedAt == nil || archived.Sandbox.Stop == nil {
		t.Errorf("archived sandbox metadata = %+v", archived.Sandbox)
	}

	// get/list expose the persisted stop metadata.
	core, _ := lifecyclePGHandlers(t, store)
	getRequest := lifecycleSetPath(coreTestRequest(http.MethodGet, PathSandbox, nil, owner), id, "")
	recorder = coreServe(core.GetSandbox, getRequest)
	if recorder.Code != http.StatusOK {
		t.Fatalf("get status = %d body = %s", recorder.Code, recorder.Body.String())
	}
	info := lifecycleDecode[SandboxInfoResponse](t, recorder.Body.Bytes())
	if info.Sandbox.StoppedAt == nil || info.Sandbox.Stop == nil || info.Sandbox.Stop.ID != first.Operation.ID {
		t.Errorf("get metadata = %+v", info.Sandbox)
	}
	list := corePGListPage(t, core.ListSandboxes, owner, PathSandboxes)
	if len(list.Sandboxes) != 1 || list.Sandboxes[0].Stop == nil || list.Sandboxes[0].StoppedAt == nil {
		t.Errorf("list metadata = %+v", list.Sandboxes)
	}
}

func TestLifecyclePostgresResume(t *testing.T) {
	store := dbtest.Open(t)
	owner := corePGAccount(t, store)
	other := corePGAccount(t, store)
	node := "lifecycle-resume-node"
	corePGNode(t, store, node)
	defaultType, _ := machinetype.Lookup("default")
	largeType, _ := machinetype.Lookup("large")
	id := ids.Sandbox()
	lifecyclePGInsert(t, store, owner, id, db.StateArchived, &node,
		db.Resources{VCPUs: defaultType.VCPUs, MemoryMiB: defaultType.MemoryMiB, DiskGiB: defaultType.DiskGiB}, new(3600))
	_, handlers := lifecyclePGHandlers(t, store)

	resume := func(owner uuid.UUID, body string) *httptest.ResponseRecorder {
		t.Helper()
		request := lifecycleSetPath(coreTestRequest(http.MethodPost, PathSandboxResume, strings.NewReader(body), owner), id, "")
		return coreServe(handlers.ResumeSandbox, request)
	}

	recorder := resume(owner, `{"type":"large","ttlSeconds":7200,"env":{"FOO":"bar"}}`)
	if recorder.Code != http.StatusAccepted {
		t.Fatalf("resume status = %d body = %s", recorder.Code, recorder.Body.String())
	}
	response := lifecycleDecode[SandboxActionResponse](t, recorder.Body.Bytes())
	if response.Type != TypeSandboxResuming || response.Status != "resuming" || response.Sandbox == nil ||
		response.Sandbox.State != StateProvisioning {
		t.Errorf("resume response = %+v", response)
	}
	row, err := store.Sandbox(t.Context(), owner, id)
	if err != nil {
		t.Fatalf("Sandbox: %v", err)
	}
	if row.State != db.StateProvisioning || row.MachineType != "large" ||
		row.Desired.VCPUs != largeType.VCPUs || row.Reserved.VCPUs != largeType.VCPUs ||
		row.LifecycleGeneration != 1 || row.TTLSeconds == nil || *row.TTLSeconds != 7200 || row.StoppedAt != nil {
		t.Errorf("resumed row = %+v", row)
	}
	var resumeJobs int
	if err := store.Pool().QueryRow(t.Context(), `
		SELECT count(*) FROM jobs WHERE owner_id = $1 AND sandbox_id = $2 AND kind = 'sandbox.resume' AND state = 'queued'`,
		owner, id).Scan(&resumeJobs); err != nil {
		t.Fatal(err)
	}
	if resumeJobs != 1 {
		t.Errorf("resume jobs = %d, want 1", resumeJobs)
	}

	// A foreign owner cannot resume; a running sandbox with a change needs a
	// stop first; a missing node is a capacity-style 503.
	coreAssertError(t, resume(other, `{}`), http.StatusNotFound, apierr.CodeNotFound)

	running := ids.Sandbox()
	lifecyclePGInsert(t, store, owner, running, db.StateRunning, &node, db.Resources{VCPUs: 4, MemoryMiB: 8192, DiskGiB: 51}, nil)
	runningRequest := lifecycleSetPath(coreTestRequest(http.MethodPost, PathSandboxResume, strings.NewReader(`{"type":"large"}`), owner), running, "")
	coreAssertError(t, coreServe(handlers.ResumeSandbox, runningRequest), http.StatusConflict, apierr.CodeSandboxRunningStopFirst)

	lost := ids.Sandbox()
	lifecyclePGInsert(t, store, owner, lost, db.StateArchived, nil, db.Resources{VCPUs: 4, MemoryMiB: 8192, DiskGiB: 51}, nil)
	lostRequest := lifecycleSetPath(coreTestRequest(http.MethodPost, PathSandboxResume, nil, owner), lost, "")
	coreAssertError(t, coreServe(handlers.ResumeSandbox, lostRequest), http.StatusServiceUnavailable, apierr.CodeNoReadyMachine)

	// The retained disk may not shrink below its size.
	big := ids.Sandbox()
	lifecyclePGInsert(t, store, owner, big, db.StateArchived, &node, db.Resources{VCPUs: 8, MemoryMiB: 16384, DiskGiB: 111}, nil)
	bigRequest := lifecycleSetPath(coreTestRequest(http.MethodPost, PathSandboxResume, strings.NewReader(`{"type":"small"}`), owner), big, "")
	coreAssertError(t, coreServe(handlers.ResumeSandbox, bigRequest), http.StatusConflict, "type_too_small")
}

func TestLifecyclePostgresResumeInitialBootStopConflict(t *testing.T) {
	store := dbtest.Open(t)
	owner := corePGAccount(t, store)
	node := "lifecycle-boot-node"
	corePGNode(t, store, node)
	id := ids.Sandbox()
	if _, err := store.CreateSandbox(t.Context(), db.CreateSandboxParams{
		ID: id, Owner: owner, MachineType: "default", Name: "boot",
		TTLSeconds: new(3600), NodeID: node,
		Reserve: db.Resources{VCPUs: 4, MemoryMiB: 8192, DiskGiB: 51},
	}); err != nil {
		t.Fatalf("CreateSandbox: %v", err)
	}
	_, handlers := lifecyclePGHandlers(t, store)

	// The first boot is still queued, so the stop can only retain its disk.
	stopRequest := lifecycleSetPath(coreTestRequest(http.MethodPost, PathSandboxStop, nil, owner), id, "")
	if recorder := coreServe(handlers.StopSandbox, stopRequest); recorder.Code != http.StatusAccepted {
		t.Fatalf("stop status = %d body = %s", recorder.Code, recorder.Body.String())
	}
	stopping, err := store.Sandbox(t.Context(), owner, id)
	if err != nil || stopping.State != db.StateArchiving || stopping.StopOperationID == nil {
		t.Fatalf("archiving row = %+v err = %v", stopping, err)
	}

	// A resume during that archiving stop cannot take over before the first
	// disk exists: pinned sandbox_stopping with the stop status URL.
	resumeRequest := lifecycleSetPath(coreTestRequest(http.MethodPost, PathSandboxResume, nil, owner), id, "")
	recorder := coreServe(handlers.ResumeSandbox, resumeRequest)
	body := coreAssertError(t, recorder, http.StatusConflict, apierr.CodeSandboxStopping)
	if body.Error.Details["statusUrl"] != "/api/v1/sandboxes/"+id+"/stops/"+*stopping.StopOperationID {
		t.Errorf("details = %+v", body.Error.Details)
	}
	row, err := store.Sandbox(t.Context(), owner, id)
	if err != nil || row.State != db.StateArchiving || row.LifecycleGeneration != 0 {
		t.Fatalf("resume changed the archiving row = %+v err = %v", row, err)
	}
}

func TestLifecyclePostgresErrorSandboxResumeRefusedAndDeleted(t *testing.T) {
	store := dbtest.Open(t)
	owner := corePGAccount(t, store)
	id := ids.Sandbox()
	lifecyclePGInsert(t, store, owner, id, db.StateError, nil, db.Resources{}, new(3600))
	stopID := "stop_0123456789abcdef"
	tx, err := store.Pool().Begin(t.Context())
	if err != nil {
		t.Fatalf("begin fixture: %v", err)
	}
	defer func() { _ = tx.Rollback(t.Context()) }()
	if _, err := tx.Exec(t.Context(), `
		INSERT INTO stop_operations (id, owner_id, sandbox_id, status, requested_at, last_attempt_at, error)
		VALUES ($1, $2, $3, 'failing', now() - interval '5 seconds', now(), 'vm_missing')`,
		stopID, owner, id); err != nil {
		t.Fatalf("insert failing stop: %v", err)
	}
	if _, err := tx.Exec(t.Context(), `
		UPDATE sandboxes SET error_reason = 'vm_missing', stopped_at = now(), stop_operation_id = $3
		WHERE id = $1 AND owner_id = $2`, id, owner, stopID); err != nil {
		t.Fatalf("mark error sandbox: %v", err)
	}
	if err := tx.Commit(t.Context()); err != nil {
		t.Fatalf("commit fixture: %v", err)
	}
	core, handlers := lifecyclePGHandlers(t, store)

	// GET shows the durable error and the failing stop metadata.
	getRequest := lifecycleSetPath(coreTestRequest(http.MethodGet, PathSandbox, nil, owner), id, "")
	recorder := coreServe(core.GetSandbox, getRequest)
	if recorder.Code != http.StatusOK {
		t.Fatalf("get status = %d body = %s", recorder.Code, recorder.Body.String())
	}
	info := lifecycleDecode[SandboxInfoResponse](t, recorder.Body.Bytes())
	if info.Sandbox.State != StateError || info.Sandbox.Error == nil || *info.Sandbox.Error != "vm_missing" ||
		info.Sandbox.Stop == nil || info.Sandbox.Stop.Status != StopFailing ||
		info.Sandbox.Stop.Error == nil || *info.Sandbox.Stop.Error != "vm_missing" {
		t.Errorf("error sandbox metadata = %+v", info.Sandbox)
	}
	before, err := store.Sandbox(t.Context(), owner, id)
	if err != nil {
		t.Fatalf("Sandbox: %v", err)
	}
	list := corePGListPage(t, core.ListSandboxes, owner, PathSandboxes)
	if len(list.Sandboxes) != 1 || list.Sandboxes[0].Error == nil || *list.Sandboxes[0].Error != "vm_missing" ||
		list.Sandboxes[0].Stop == nil || list.Sandboxes[0].Stop.Status != StopFailing {
		t.Errorf("error sandbox list metadata = %+v", list.Sandboxes)
	}

	// Resume is refused before admission; no resume job or row change appears.
	resumeRequest := lifecycleSetPath(coreTestRequest(http.MethodPost, PathSandboxResume, nil, owner), id, "")
	recorder = coreServe(handlers.ResumeSandbox, resumeRequest)
	body := coreAssertError(t, recorder, http.StatusConflict, apierr.CodeSandboxNotReady)
	if retryable, ok := body.Error.Details["retryable"].(bool); !ok || retryable {
		t.Errorf("details.retryable = %v, want false", body.Error.Details["retryable"])
	}
	after, err := store.Sandbox(t.Context(), owner, id)
	if err != nil || after.State != db.StateError || after.Version != before.Version {
		t.Fatalf("resume changed the error row = %+v err = %v", after, err)
	}
	var resumeJobs int
	if err := store.Pool().QueryRow(t.Context(), `
		SELECT count(*) FROM jobs WHERE owner_id = $1 AND sandbox_id = $2 AND kind = 'sandbox.resume'`,
		owner, id).Scan(&resumeJobs); err != nil {
		t.Fatal(err)
	}
	if resumeJobs != 0 {
		t.Errorf("resume jobs = %d, want 0", resumeJobs)
	}

	// DELETE still removes the error row through the normal path.
	deleteRequest := lifecycleSetPath(coreTestRequest(http.MethodDelete, PathSandbox, nil, owner), id, "")
	deleteRequest.Header.Set(ConfirmDeleteHeader, id)
	recorder = coreServe(core.DeleteSandbox, deleteRequest)
	if recorder.Code != http.StatusAccepted {
		t.Fatalf("delete status = %d body = %s", recorder.Code, recorder.Body.String())
	}
	if _, err := store.Sandbox(t.Context(), owner, id); !errors.Is(err, db.ErrNotFound) {
		t.Errorf("deleted Sandbox error = %v, want ErrNotFound", err)
	}
}

func TestLifecyclePostgresStopInvalidState(t *testing.T) {
	store := dbtest.Open(t)
	owner := corePGAccount(t, store)
	id := ids.Sandbox()
	lifecyclePGInsert(t, store, owner, id, db.StateError, nil, db.Resources{}, nil)
	_, handlers := lifecyclePGHandlers(t, store)
	request := lifecycleSetPath(coreTestRequest(http.MethodPost, PathSandboxStop, nil, owner), id, "")
	coreAssertError(t, coreServe(handlers.StopSandbox, request), http.StatusBadRequest, apierr.CodeInvalidJSON)
}
