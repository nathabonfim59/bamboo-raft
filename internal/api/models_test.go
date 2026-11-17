package api

import (
	"encoding/json/v2"
	"slices"
	"testing"
	"time"
)

func jsonMap(t *testing.T, value any) map[string]any {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("unmarshal %s: %v", raw, err)
	}
	return decoded
}

func TestSandboxWireShape(t *testing.T) {
	created := time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC)
	after := created.Add(time.Hour)
	sandbox := Sandbox{
		ID:                "bx_23456789",
		Name:              "Sandbox 2026-11-08 12:00",
		State:             StateProvisioning,
		Type:              "default",
		VCPU:              4,
		MemoryGB:          8,
		BillingMultiplier: 1,
		CreatedAt:         &created,
		UpdatedAt:         &created,
		ArchiveAfter:      &after,
		TTLSeconds:        new(3600),
		DesktopAvailable:  false,
		SnapshotAvailable: false,
	}

	got := jsonMap(t, sandbox)
	if got["id"] != "bx_23456789" || got["state"] != "provisioning" {
		t.Errorf("identity fields = %v", got)
	}
	for _, field := range []string{"desktopAvailable", "snapshotAvailable"} {
		value, ok := got[field]
		if !ok {
			t.Errorf("%s must always be present, also when false", field)
		}
		if value != false {
			t.Errorf("%s = %v, want false", field, value)
		}
	}
	for _, field := range []string{"url", "ip", "desktopUrl", "snapshotCompletedAt"} {
		value, ok := got[field]
		if !ok {
			t.Errorf("%s must be present as explicit null", field)
		}
		if value != nil {
			t.Errorf("%s = %v, want null", field, value)
		}
	}
	for _, field := range []string{"error", "labels", "snapshots", "monitoring"} {
		if _, ok := got[field]; ok {
			t.Errorf("%s = %v, want omitted when unset", field, got[field])
		}
	}
	if got["createdAt"] != "2026-11-08T12:00:00Z" {
		t.Errorf("createdAt = %v", got["createdAt"])
	}
}

func TestOptionalDistinguishesAbsentAndNull(t *testing.T) {
	type payload struct {
		Absent Optional[int] `json:"absent,omitzero"`
		Value  Optional[int] `json:"value,omitzero"`
		Null   Optional[int] `json:"null,omitzero"`
	}
	got := jsonMap(t, payload{Value: Some(7), Null: Null[int]()})
	if _, ok := got["absent"]; ok {
		t.Error("absent Optional must be omitted")
	}
	if got["value"] != float64(7) {
		t.Errorf("value = %v", got["value"])
	}
	value, ok := got["null"]
	if !ok || value != nil {
		t.Errorf("null = %v, present=%v; want explicit null", value, ok)
	}
}

func TestCommandExitFrameCanCarryNullExitCode(t *testing.T) {
	frame := CommandStreamFrame{Type: CommandFrameExit, ExitCode: Null[int](), Success: Some(true), TimedOut: Some(true)}
	got := jsonMap(t, frame)
	value, ok := got["exitCode"]
	if !ok || value != nil {
		t.Errorf("exitCode = %v, present=%v; want explicit null", value, ok)
	}
	if got["type"] != "exit" || got["success"] != true || got["timedOut"] != true {
		t.Errorf("frame = %v", got)
	}
}

func TestPageInfoNextCursorIsExplicitNull(t *testing.T) {
	got := jsonMap(t, NewPageInfo(nil, 100))
	if value, ok := got["nextCursor"]; !ok || value != nil {
		t.Errorf("nextCursor = %v, present=%v; want explicit null", value, ok)
	}
	if got["hasMore"] != false || got["limit"] != float64(100) {
		t.Errorf("page info = %v", got)
	}

	cursor := "opaque"
	got = jsonMap(t, NewPageInfo(&cursor, 50))
	if got["nextCursor"] != "opaque" || got["hasMore"] != true {
		t.Errorf("page info with cursor = %v", got)
	}
}

func TestConstructorsSetEnvelopeTypes(t *testing.T) {
	tests := []struct {
		name string
		got  string
		want string
	}{
		{"list", NewSandboxListResponse(nil, NewPageInfo(nil, 100)).Type, TypeSandboxList},
		{"info", NewSandboxInfoResponse(Sandbox{}).Type, TypeSandboxInfo},
		{"create", NewCreateSandboxResponse(CreateProvisioning, nil, Sandbox{}).Type, TypeSandboxCreated},
		{"me", NewMeResponse(MeUser{}).Type, TypeUserInfo},
		{"command", NewCommandResponse(true, nil, "", "", false).Type, TypeCommandFinished},
		{"deletion", NewDeletionOperationResponse(DeletionOperation{}).Type, TypeDeletionOperation},
		{"sandbox deleting", NewSandboxDeletingResponse(DeletionOperation{}).Type, TypeSandboxDeleting},
		{"snapshot list", NewSavedSnapshotListResponse(nil).Type, TypeSavedSnapshotList},
		{"snapshot info", NewSavedSnapshotInfoResponse(SavedSnapshot{}).Type, TypeSavedSnapshotInfo},
		{"snapshot saving", NewSavedSnapshotSavingResponse(SavedSnapshot{}).Type, TypeSavedSnapshotSaving},
		{"snapshot deleted", NewSavedSnapshotDeletedResponse("name").Type, TypeSavedSnapshotDeleted},
		{"file upload", NewFileUploadURLResponse("https://example.test/u", "/tmp/f", time.Now()).Type, TypeFileUploadURL},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.got != test.want {
				t.Errorf("type = %q, want %q", test.got, test.want)
			}
		})
	}
	for _, response := range []SuccessBase{
		NewSandboxListResponse(nil, NewPageInfo(nil, 100)).SuccessBase,
		NewSandboxInfoResponse(Sandbox{}).SuccessBase,
		NewCreateSandboxResponse(CreateProvisioning, nil, Sandbox{}).SuccessBase,
		NewMeResponse(MeUser{}).SuccessBase,
		NewDeletionOperationResponse(DeletionOperation{}).SuccessBase,
	} {
		if !response.OK {
			t.Errorf("envelope %q must set ok=true", response.Type)
		}
	}
}

func TestSandboxStatesMatchThePin(t *testing.T) {
	got := []SandboxState{
		StateInit, StateProvisioning, StateProvisioned, StateCloning, StateReady,
		StateIdle, StateRunning, StateArchiving, StateArchived, StateError, StateCancelled,
	}
	want := []string{
		"init", "provisioning", "provisioned", "cloning", "ready",
		"idle", "running", "archiving", "archived", "error", "cancelled",
	}
	if len(got) != len(want) {
		t.Fatalf("states = %v", got)
	}
	for i := range want {
		if string(got[i]) != want[i] {
			t.Errorf("state %d = %q, want %q", i, got[i], want[i])
		}
	}
	for _, state := range []SandboxState{"deleting", "deleted"} {
		if slices.Contains(got, state) {
			t.Errorf("soft-delete state %q leaked into the public enum", state)
		}
	}
}

func TestDeletionOperationRequiredNulls(t *testing.T) {
	requested := time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC)
	op := DeletionOperation{
		ID:           "bdop_0123456789abcdef0123456789abcdef",
		Kind:         DeletionKindSandbox,
		TargetID:     "bx_23456789",
		Reason:       DeletionReasonExplicit,
		Status:       DeletionPending,
		AttemptCount: 0,
		RequestedAt:  requested,
	}
	got := jsonMap(t, NewSandboxDeletingResponse(op))
	if got["type"] != "sandbox.deleting" || got["ok"] != true {
		t.Errorf("envelope = %v", got)
	}
	operation, ok := got["operation"].(map[string]any)
	if !ok {
		t.Fatalf("operation = %v", got["operation"])
	}
	if value, ok := operation["completedAt"]; !ok || value != nil {
		t.Errorf("completedAt = %v, present=%v; want explicit null", value, ok)
	}
	if _, ok := operation["stage"]; ok {
		t.Error("stage must be omitted when unset")
	}
	if operation["attemptCount"] != float64(0) {
		t.Errorf("attemptCount = %v", operation["attemptCount"])
	}
}
