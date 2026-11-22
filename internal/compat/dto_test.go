package compat

import (
	"bytes"
	"encoding/json/v2"
	"net/http"
	"testing"
	"time"

	"github.com/nathabonfim59/bamboo-raft/internal/api"
)

type dtoCase struct {
	name        string
	operationID string
	status      int
	mediaType   string
	value       any
}

func TestAPIDTOsValidateThroughCompat(t *testing.T) {
	created := time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC)
	after := created.Add(time.Hour)
	ready := api.Sandbox{
		ID:                "bx_23456789",
		Name:              "Sandbox 2026-11-08 12:00",
		State:             api.StateReady,
		Labels:            map[string]string{"session": "abc"},
		Type:              "default",
		VCPU:              4,
		MemoryGB:          8,
		BillingMultiplier: 1,
		IP:                new("203.0.113.10"),
		CreatedAt:         &created,
		UpdatedAt:         &created,
		ArchiveAfter:      &after,
		TTLSeconds:        new(3600),
		DesktopAvailable:  false,
		SnapshotAvailable: false,
	}
	provisioning := ready
	provisioning.State = api.StateProvisioning
	provisioning.IP = nil

	stage := api.DeletionStageRemoving
	operation := api.DeletionOperation{
		ID:           "bdop_0123456789abcdef0123456789abcdef",
		Kind:         api.DeletionKindSandbox,
		TargetID:     "bx_23456789",
		Reason:       api.DeletionReasonExplicit,
		Status:       api.DeletionProcessing,
		Stage:        &stage,
		AttemptCount: 0,
		RequestedAt:  created,
	}
	saved := api.SavedSnapshot{
		Name:            "make-check-base",
		Status:          api.SavedSnapshotReady,
		SourceSandboxID: "bx_23456789",
		SnapshotID:      "snap-0123456789abcdef0123456789abcdef",
		Type:            "default",
		SizeBytes:       new(int64(123456)),
		CreatedAt:       created,
	}
	saving := saved
	saving.Status = api.SavedSnapshotSaving

	done := created.Add(5 * time.Minute)
	stop := api.StopOperation{
		ID:            "stop_0123456789abcdef",
		Status:        api.StopCompleted,
		LastAttemptAt: &done,
		EndedAt:       &done,
	}
	pendingStop := api.StopOperation{
		ID:          "stop_0123456789abcdef",
		Status:      api.StopPending,
		RequestedAt: &created,
	}
	stopped := ready
	stopped.State = api.StateArchived
	stopped.StoppedAt = &done
	stopped.Subdomain = new("acme-staging")
	stopped.Stop = &stop

	tests := []dtoCase{
		{"list empty", "sandboxes", http.StatusOK, jsonMediaType, api.NewSandboxListResponse(nil, api.NewPageInfo(nil, 100))},
		{"list one", "sandboxes", http.StatusOK, jsonMediaType, api.NewSandboxListResponse([]api.Sandbox{ready}, api.NewPageInfo(nil, 100))},
		{"list stopped", "sandboxes", http.StatusOK, jsonMediaType, api.NewSandboxListResponse([]api.Sandbox{stopped}, api.NewPageInfo(nil, 100))},
		{"info", "get", http.StatusOK, jsonMediaType, api.NewSandboxInfoResponse(ready)},
		{"info stopped", "get", http.StatusOK, jsonMediaType, api.NewSandboxInfoResponse(stopped)},
		{"stop status", "getStop", http.StatusOK, jsonMediaType, api.NewStopOperationResponse(stopped.ID, pendingStop)},
		{"stop pending", "stop", http.StatusAccepted, jsonMediaType, api.NewStopActionResponse(
			api.TypeSandboxStopping, ready.ID, "archiving", ready, pendingStop,
			"/api/v1/sandboxes/bx_23456789/stops/stop_0123456789abcdef", false,
		)},
		{"stop archived", "stop", http.StatusOK, jsonMediaType, api.NewStopActionResponse(
			api.TypeSandboxStopped, stopped.ID, "archived", stopped, stop,
			"/api/v1/sandboxes/bx_23456789/stops/stop_0123456789abcdef", false,
		)},
		{"resume", "resume", http.StatusAccepted, jsonMediaType, api.NewResumeActionResponse(provisioning.ID, provisioning)},
		{"fork", "fork", http.StatusAccepted, jsonMediaType, api.NewForkActionResponse(provisioning.ID, provisioning)},
		{"create provisioning", "create", http.StatusAccepted, jsonMediaType, api.NewCreateSandboxResponse(api.CreateProvisioning, nil, provisioning)},
		{"create ready", "create", http.StatusAccepted, jsonMediaType, api.NewCreateSandboxResponse(api.CreateReady, new(3600), ready)},
		{"delete sandbox", "deleteSandbox", http.StatusAccepted, jsonMediaType, api.NewSandboxDeletingResponse(operation)},
		{"deletion operation", "getDeletionOperation", http.StatusOK, jsonMediaType, api.NewDeletionOperationResponse(operation)},
		{"me", "me", http.StatusOK, jsonMediaType, api.NewMeResponse(api.MeUser{
			ID:                "acct_example",
			Login:             "user",
			DisplayName:       "User",
			ConnectionMethod:  "service",
			ConnectionMethods: []string{"service"},
			AccountPlan:       "service",
			ServiceAccount:    new(true),
			AccountStatus:     "active",
			Suspended:         new(false),
			Closed:            new(false),
			ZeroDataRetention: new(false),
		})},
		{"command success", "command", http.StatusOK, jsonMediaType, api.NewCommandResponse(true, new(0), "ok\n", "", false)},
		{"command timeout", "command", http.StatusOK, jsonMediaType, api.NewCommandResponse(false, nil, "", "", true)},
		{"command stopped", "stopCommand", http.StatusOK, jsonMediaType, api.CommandStopResponse{
			SuccessBase: api.SuccessBase{OK: true, Type: api.TypeCommandStopped},
			ProcessID:   7,
			Running:     false,
			Signaled:    true,
			Command: &api.CommandRecord{
				CommandID:       "cmd_processaaaaa",
				SandboxID:       "bx_3456789a",
				Command:         "sleep 30",
				Cwd:             new("/home/user"),
				Mode:            "detached",
				ProcessID:       new(uint32(7)),
				Status:          "killed",
				ExitCode:        new(0),
				StdoutBytes:     12,
				StderrBytes:     0,
				OutputTruncated: false,
				StartedAt:       created,
				FinishedAt:      &after,
			},
		}},
		{"command status", "commandStatus", http.StatusOK, jsonMediaType, api.CommandStatusResponse{
			SuccessBase: api.SuccessBase{OK: true, Type: api.TypeCommandStatus},
			Success:     true,
			ProcessID:   7,
			Status:      "exited",
			Running:     false,
			ExitCode:    new(0),
			Stdout:      "out€",
			Stderr:      "err",
		}},
		{"saved list empty", "listSavedSnapshots", http.StatusOK, jsonMediaType, api.NewSavedSnapshotListResponse(nil)},
		{"saved list one", "listSavedSnapshots", http.StatusOK, jsonMediaType, api.NewSavedSnapshotListResponse([]api.SavedSnapshot{saved})},
		{"saved info", "getSavedSnapshot", http.StatusOK, jsonMediaType, api.NewSavedSnapshotInfoResponse(saved)},
		{"saved saving", "saveSavedSnapshot", http.StatusAccepted, jsonMediaType, api.NewSavedSnapshotSavingResponse(saving)},
		{"saved deleted", "deleteSavedSnapshot", http.StatusOK, jsonMediaType, api.NewSavedSnapshotDeletedResponse(saved.Name)},
		{"file upload url", "createUploadUrl", http.StatusOK, jsonMediaType, api.NewFileUploadURLResponse(
			"https://api.example.test/api/v1/uploads/token", "/tmp/archive.tar", created.Add(15*time.Minute),
		)},
	}

	pinned := Load(t)
	subset := Load(t)
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			body, err := json.Marshal(test.value)
			if err != nil {
				t.Fatalf("marshal DTO: %v", err)
			}
			if err := pinned.Response(test.operationID, test.status, mediaType(test.mediaType), body); err != nil {
				t.Errorf("DTO does not validate against the pin: %v\nbody: %s", err, body)
			}
			if err := subset.Response(test.operationID, test.status, mediaType(test.mediaType), body); err != nil {
				t.Errorf("DTO does not validate against api/bbraft-v1.yaml: %v\nbody: %s", err, body)
			}
		})
	}

	t.Run("command frames", func(t *testing.T) {
		frames := []api.CommandStreamFrame{
			{Type: api.CommandFrameStarted, CommandID: "cmd_example"},
			{Type: api.CommandFrameStdout, Data: "ok\n"},
			{Type: api.CommandFrameStderr, Data: "warning\n"},
			{Type: api.CommandFrameExit, CommandID: "cmd_example", ExitCode: api.Some(0), Success: api.Some(true), TimedOut: api.Some(false)},
			{Type: api.CommandFrameExit, ExitCode: api.Null[int](), Success: api.Some(false), TimedOut: api.Some(true)},
		}
		var body bytes.Buffer
		for _, frame := range frames {
			line, err := json.Marshal(frame)
			if err != nil {
				t.Fatalf("marshal frame: %v", err)
			}
			body.Write(line)
			body.WriteByte('\n')
		}
		for name, validator := range map[string]*Validator{"pin": pinned, "subset": subset} {
			t.Run(name, func(t *testing.T) {
				if err := validator.NDJSON("command", http.StatusOK, mediaType(ndjsonMediaType), body.Bytes()); err != nil {
					t.Errorf("frames do not validate: %v\nbody: %s", err, body.Bytes())
				}
			})
		}
	})

	t.Run("attach frames", func(t *testing.T) {
		frames := []api.CommandAttachFrame{
			{Type: api.CommandAttachAttached, ProcessID: 7, StdoutOffset: new(int64(0)), StderrOffset: new(int64(0))},
			{Type: api.CommandAttachStdout, Data: "€", NextOffset: new(int64(3))},
			{Type: api.CommandAttachStderr, Data: "warning\n", NextOffset: new(int64(8))},
			{Type: api.CommandAttachHeartbeat},
			{Type: api.CommandAttachExit, Status: "exited", ExitCode: api.Some(0), Signal: api.Null[string](), StdoutOffset: new(int64(3)), StderrOffset: new(int64(8))},
			{Type: api.CommandAttachReconnect, StdoutOffset: new(int64(3)), StderrOffset: new(int64(8))},
			{Type: api.CommandAttachError, Error: "sandbox_direct_failed", Message: "the sandbox machine could not complete the call", StdoutOffset: new(int64(3)), StderrOffset: new(int64(8))},
		}
		var body bytes.Buffer
		for _, frame := range frames {
			line, err := json.Marshal(frame)
			if err != nil {
				t.Fatalf("marshal attach frame: %v", err)
			}
			body.Write(line)
			body.WriteByte('\n')
		}
		for name, validator := range map[string]*Validator{"pin": pinned, "subset": subset} {
			t.Run(name, func(t *testing.T) {
				if err := validator.NDJSON("attachCommand", http.StatusOK, mediaType(ndjsonMediaType), body.Bytes()); err != nil {
					t.Errorf("attach frames do not validate: %v\nbody: %s", err, body.Bytes())
				}
			})
		}
	})
}
