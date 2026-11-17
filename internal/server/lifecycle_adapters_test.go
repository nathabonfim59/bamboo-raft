package server

import (
	"context"
	"errors"
	"testing"

	"uuid"

	"github.com/nathabonfim59/bamboo-raft/internal/api"
	"github.com/nathabonfim59/bamboo-raft/internal/db"
	"github.com/nathabonfim59/bamboo-raft/internal/scheduler"
)

// lifecycleResumeSchedulerFake records the scheduler admission the adapter
// makes.
type lifecycleResumeSchedulerFake struct {
	calls    []scheduler.ResumeRequest
	accepted db.ResumeAcceptance
	err      error
}

func (f *lifecycleResumeSchedulerFake) Resume(_ context.Context, request scheduler.ResumeRequest) (db.ResumeAcceptance, error) {
	f.calls = append(f.calls, request)
	return f.accepted, f.err
}

var _ resumeScheduler = (*lifecycleResumeSchedulerFake)(nil)

func TestAPIResumeAdmissionMapsRequestAndResult(t *testing.T) {
	owner := uuid.MustParse("018f4b1c-7e2d-7a3b-9c4d-123456789abc")
	superseded := db.StopOperation{ID: "stop_live", Status: db.StopSuperseded}
	store := &lifecycleResumeSchedulerFake{accepted: db.ResumeAcceptance{
		Sandbox:    db.Sandbox{ID: "bx_23456789", OwnerID: owner, LifecycleGeneration: 2},
		Superseded: &superseded,
	}}
	adapter := apiResumeAdmission{scheduler: store}

	result, err := adapter.Resume(t.Context(), api.ResumeRequest{
		Owner:           owner,
		SandboxID:       "bx_23456789",
		MachineType:     "large",
		Resources:       db.Resources{VCPUs: 8, MemoryMiB: 16384, DiskGiB: 111},
		Env:             db.EnvChange{Set: true, Values: map[string]string{"FOO": "bar"}},
		TTL:             db.TTLOption{Set: true, Seconds: new(7200)},
		SupersedeStopID: "stop_live",
		FailFast:        true,
	})
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if len(store.calls) != 1 {
		t.Fatalf("calls = %d", len(store.calls))
	}
	call := store.calls[0]
	if call.Owner != owner || call.SandboxID != "bx_23456789" || call.MachineType != "large" ||
		call.Resources.VCPUs != 8 || call.SupersedeStopID != "stop_live" {
		t.Errorf("mapped request = %+v", call)
	}
	if !call.FailFast {
		t.Error("FailFast was not forwarded to scheduler.Resume")
	}
	if call.ExpectedVersion != 0 || call.MaxAttempts != 0 {
		t.Errorf("expected version/attempts = %d/%d, want zero", call.ExpectedVersion, call.MaxAttempts)
	}
	if !call.Env.Set || call.Env.Values["FOO"] != "bar" {
		t.Errorf("mapped env = %+v", call.Env)
	}
	if !call.TTL.Set || call.TTL.Seconds == nil || *call.TTL.Seconds != 7200 {
		t.Errorf("mapped ttl = %+v", call.TTL)
	}
	if result.Sandbox.ID != "bx_23456789" || result.Superseded == nil || result.Superseded.ID != "stop_live" {
		t.Errorf("result = %+v", result)
	}
}

func TestAPIResumeAdmissionForwardsFailFastFalse(t *testing.T) {
	store := &lifecycleResumeSchedulerFake{}
	adapter := apiResumeAdmission{scheduler: store}
	if _, err := adapter.Resume(t.Context(), api.ResumeRequest{SandboxID: "bx_23456789"}); err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if len(store.calls) != 1 || store.calls[0].FailFast {
		t.Errorf("scheduler calls = %+v, want one FailFast=false", store.calls)
	}
}

func TestAPIResumeAdmissionPreservesErrors(t *testing.T) {
	sentinel := errors.New("resume refused")
	adapter := apiResumeAdmission{scheduler: &lifecycleResumeSchedulerFake{
		err: errors.Join(db.ErrNoCapacity, scheduler.ErrNoNode, sentinel),
	}}
	_, err := adapter.Resume(t.Context(), api.ResumeRequest{})
	if !errors.Is(err, db.ErrNoCapacity) || !errors.Is(err, scheduler.ErrNoNode) || !errors.Is(err, sentinel) {
		t.Fatalf("error = %v, want the scheduler's joined classification", err)
	}
}
