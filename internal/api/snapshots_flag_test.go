package api

import (
	"context"
	"encoding/json/v2"
	"net/http"
	"strings"
	"testing"

	"uuid"

	"github.com/nathabonfim59/bamboo-raft/internal/apierr"
	"github.com/nathabonfim59/bamboo-raft/internal/compat"
	"github.com/nathabonfim59/bamboo-raft/internal/db"
)

// TestCreateSnapshotsFlag checks that create accepts the snapshots member,
// hands it to admission unchanged (nil when omitted, which the store reads
// as on) and reports the stored value on the sandbox body.
func TestCreateSnapshotsFlag(t *testing.T) {
	validator := compat.Load(t)
	tests := []struct {
		name string
		body string
		want *bool
		wire bool
	}{
		{"omitted", `{}`, nil, true},
		{"true", `{"snapshots":true}`, new(true), true},
		{"false", `{"snapshots":false}`, new(false), false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			admission := &coreFakeAdmission{}
			admission.onAdmit = func(request AdmissionRequest) {
				admission.result = AdmissionResult{Sandbox: coreSandboxFromRequest(request, db.StateProvisioning)}
			}
			handlers := coreTestHandlers(t, &coreFakeStore{}, admission)
			request := coreTestRequest(http.MethodPost, PathSandboxes, strings.NewReader(test.body), coreTestOwner)
			request.Header.Set("Content-Type", "application/json")
			recorder := coreServe(handlers.CreateSandbox, request)
			if recorder.Code != http.StatusAccepted {
				t.Fatalf("status = %d, want 202", recorder.Code)
			}
			if err := validator.Response("create", recorder.Code, recorder.Header(), recorder.Body.Bytes()); err != nil {
				t.Errorf("create response failed validation: %v", err)
			}
			if len(admission.calls) != 1 {
				t.Fatalf("admission calls = %d, want 1", len(admission.calls))
			}
			got := admission.calls[0].SnapshotsEnabled
			if (got == nil) != (test.want == nil) || (got != nil && *got != *test.want) {
				t.Errorf("admission SnapshotsEnabled = %v, want %v", got, test.want)
			}
			var body struct {
				Sandbox struct {
					Snapshots *bool `json:"snapshots"`
				} `json:"sandbox"`
			}
			if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
				t.Fatalf("decode: %v", err)
			}
			if body.Sandbox.Snapshots == nil || *body.Sandbox.Snapshots != test.wire {
				t.Errorf("sandbox.snapshots = %v, want %v", body.Sandbox.Snapshots, test.wire)
			}
		})
	}
}

// TestResumeSnapshotsDisabled checks that a stopping or stopped sandbox
// created with snapshots off answers 409 snapshots_disabled before admission
// (its stop erases the disk), while the same states with snapshots on still
// reach admission.
func TestResumeSnapshotsDisabled(t *testing.T) {
	validator := compat.Load(t)
	tests := []struct {
		name      string
		state     db.SandboxState
		enabled   bool
		wantCode  string
		wantAdmit bool
	}{
		{"archived snapshots off", db.StateArchived, false, apierr.CodeSnapshotsDisabled, false},
		{"archiving snapshots off", db.StateArchiving, false, apierr.CodeSnapshotsDisabled, false},
		{"archived snapshots on", db.StateArchived, true, "", true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			row := lifecycleSandbox(coreTestOwner)
			row.State = test.state
			row.SnapshotsEnabled = test.enabled
			store := &lifecycleFakeStore{sandbox: func(_ context.Context, _ uuid.UUID, _ string) (db.Sandbox, error) {
				return row, nil
			}}
			accepted := row
			accepted.State = db.StateProvisioning
			resume := &lifecycleFakeResume{result: ResumeResult{Sandbox: accepted}}
			handlers := lifecycleTestHandlers(t, store, resume)
			request := lifecycleSetPath(coreTestRequest(http.MethodPost, PathSandboxResume, strings.NewReader(`{}`), coreTestOwner), row.ID, "")
			recorder := coreServe(handlers.ResumeSandbox, request)
			if err := validator.Response("resume", recorder.Code, recorder.Header(), recorder.Body.Bytes()); err != nil {
				t.Errorf("resume response failed validation: %v", err)
			}
			if got := len(resume.calls) == 1; got != test.wantAdmit {
				t.Errorf("admission called = %v, want %v", got, test.wantAdmit)
			}
			if test.wantCode == "" {
				if recorder.Code != http.StatusAccepted {
					t.Errorf("status = %d, want 202", recorder.Code)
				}
				return
			}
			coreAssertError(t, recorder, http.StatusConflict, test.wantCode)
		})
	}
}
