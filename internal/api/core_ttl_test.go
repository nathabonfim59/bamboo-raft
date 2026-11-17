package api

import (
	"encoding/json/v2"
	"net/http"
	"testing"
	"time"

	"github.com/nathabonfim59/bamboo-raft/internal/db"
)

// TestCreateReplayReportsDurableConfiguredTTL covers an idempotent create
// replay after PATCH or resume: the stored configured duration differs from
// ExpiresAt-CreatedAt, and both the top-level ttlSeconds and the embedded
// sandbox must report the durable value.
func TestCreateReplayReportsDurableConfiguredTTL(t *testing.T) {
	created := coreFixedNow
	legacyExpiry := created.Add(time.Hour)
	row := db.Sandbox{
		ID: "bx_23456789", OwnerID: coreTestOwner, State: db.StateReady,
		MachineType: "default", Name: "job",
		TTLSeconds: new(7200), ExpiresAt: &legacyExpiry,
		CreatedAt: created, UpdatedAt: created,
	}
	admission := &coreFakeAdmission{result: AdmissionResult{Sandbox: row, Replayed: true}}
	handlers := coreTestHandlers(t, &coreFakeStore{}, admission)

	request := coreTestRequest(http.MethodPost, PathSandboxes, nil, coreTestOwner)
	recorder := coreServe(handlers.CreateSandbox, request)
	if recorder.Code != http.StatusAccepted {
		t.Fatalf("create replay status = %d body = %s", recorder.Code, recorder.Body.String())
	}
	var response CreateSandboxResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode create replay: %v", err)
	}
	if response.TTLSeconds == nil {
		t.Fatalf("top-level ttlSeconds is null, want the durable 7200")
	}
	if *response.TTLSeconds != 7200 {
		t.Errorf("top-level ttlSeconds = %d, want the durable 7200", *response.TTLSeconds)
	}
	if response.Sandbox.TTLSeconds == nil {
		t.Fatalf("sandbox.ttlSeconds is null, want the durable 7200")
	}
	if *response.Sandbox.TTLSeconds != 7200 {
		t.Errorf("sandbox.ttlSeconds = %d, want the durable 7200", *response.Sandbox.TTLSeconds)
	}
}
