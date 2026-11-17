package server

import (
	"context"

	"github.com/nathabonfim59/bamboo-raft/internal/api"
	"github.com/nathabonfim59/bamboo-raft/internal/db"
	"github.com/nathabonfim59/bamboo-raft/internal/scheduler"
)

// resumeScheduler is the scheduler's owner-scoped resume admission surface.
// The scheduler owns the pinned-node probe, the failFast budget and the
// atomic store accept; this adapter only translates the API seam.
type resumeScheduler interface {
	Resume(ctx context.Context, request scheduler.ResumeRequest) (db.ResumeAcceptance, error)
}

// apiResumeAdmission wires the API resume seam to the scheduler. It passes
// only the validated request: no node, hub or gateway handle crosses into the
// API layer.
type apiResumeAdmission struct {
	scheduler resumeScheduler
}

// Resume implements [api.ResumeAdmission]. ExpectedVersion and MaxAttempts
// stay zero; the store chooses the lifecycle generation and attempts.
func (a apiResumeAdmission) Resume(ctx context.Context, request api.ResumeRequest) (api.ResumeResult, error) {
	accepted, err := a.scheduler.Resume(ctx, scheduler.ResumeRequest{
		Owner:           request.Owner,
		SandboxID:       request.SandboxID,
		MachineType:     request.MachineType,
		Resources:       request.Resources,
		Env:             request.Env,
		TTL:             request.TTL,
		SupersedeStopID: request.SupersedeStopID,
		FailFast:        request.FailFast,
	})
	if err != nil {
		return api.ResumeResult{}, err
	}
	return api.ResumeResult{Sandbox: accepted.Sandbox, Superseded: accepted.Superseded}, nil
}

// ResumeAdmissionContract keeps the adapter inside the API seam.
var _ api.ResumeAdmission = apiResumeAdmission{}
