package server

// Integrated lifecycle coverage: the public HTTP endpoints against the real
// api/lifecycle/nodehub roles and a real nodefake through the gateway. The
// shared run helpers skip this test unless BBRAFT_TEST_DATABASE_URL names a
// PostgreSQL database, matching the other server integration tests.

import (
	"encoding/json/v2"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
	"github.com/nathabonfim59/bamboo-raft/internal/server/config"
)

// lifecycleRunCommandResponse is the pinned synchronous command result subset
// this test reads.
type lifecycleRunCommandResponse struct {
	Success  bool   `json:"success"`
	ExitCode *int   `json:"exitCode"`
	Stdout   string `json:"stdout"`
	Stderr   string `json:"stderr"`
}

// lifecycleRunSandbox is the subset of the pinned sandbox object this test
// reads back.
type lifecycleRunSandbox struct {
	ID         string  `json:"id"`
	Name       string  `json:"name"`
	State      string  `json:"state"`
	Error      *string `json:"error"`
	Subdomain  *string `json:"subdomain"`
	TTLSeconds *int    `json:"ttlSeconds"`
	StoppedAt  *string `json:"stoppedAt"`
	Stop       *struct {
		ID     string `json:"id"`
		Status string `json:"status"`
	} `json:"stop"`
}

// lifecycleRunInfo is the sandbox member of GET /sandboxes/{id}.
type lifecycleRunInfo struct {
	Type    string              `json:"type"`
	Sandbox lifecycleRunSandbox `json:"sandbox"`
}

// lifecycleRunWaitState polls GET until the sandbox reaches state.
func (e *runEnv) lifecycleRunWaitState(id, state string, timeout time.Duration) lifecycleRunInfo {
	e.t.Helper()
	var last lifecycleRunInfo
	e.waitFor(timeout, "sandbox state "+state, func() bool {
		response := e.do(http.MethodGet, "/api/v1/sandboxes/"+id, e.key, "", nil)
		if response.status != http.StatusOK {
			return false
		}
		if err := json.Unmarshal(response.body, &last); err != nil {
			return false
		}
		return last.Sandbox.State == state
	})
	return last
}

// lifecycleRunCommand runs one synchronous command over the public endpoint
// and returns its raw and decoded results.
func (e *runEnv) lifecycleRunCommand(id, command string) (runResponse, lifecycleRunCommandResponse) {
	e.t.Helper()
	response := e.do(http.MethodPost, "/api/v1/sandboxes/"+id+"/commands", e.key,
		`{"command":`+strconv.Quote(command)+`,"timeoutSeconds":5}`, nil)
	if response.status != http.StatusOK {
		e.t.Fatalf("command status = %d body = %s", response.status, response.body)
	}
	var result lifecycleRunCommandResponse
	if err := json.Unmarshal(response.body, &result); err != nil {
		e.t.Fatalf("command response is malformed: %v", err)
	}
	return response, result
}

func TestRunLifecyclePatchStopResumeDelete(t *testing.T) {
	env := newRunEnv(t)
	v1, _ := runTestValidators(t)
	env.start(config.Roles{config.RoleAPI: true, config.RoleLifecycle: true, config.RoleNodeHub: true})
	env.startNode(0)
	env.waitNodeFresh()
	env.waitAPI()

	id := env.createSandbox(`{"type":"small","ttlSeconds":600,"name":"lifecycle-run"}`)
	env.waitReady(id)

	// PATCH persists the name, a new TTL and a unique subdomain, and the
	// response is the pinned sandbox info shape.
	patched := env.do(http.MethodPatch, "/api/v1/sandboxes/"+id, env.key,
		`{"name":"lifecycle-renamed","ttlSeconds":7200,"subdomain":"lifecycle-run"}`, nil)
	if patched.status != http.StatusOK {
		t.Fatalf("patch status = %d body = %s", patched.status, patched.body)
	}
	runTestCheck(t, v1, "update", patched)
	var patchInfo lifecycleRunInfo
	if err := json.Unmarshal(patched.body, &patchInfo); err != nil {
		t.Fatalf("patch response is malformed: %v", err)
	}
	if patchInfo.Sandbox.Name != "lifecycle-renamed" ||
		patchInfo.Sandbox.TTLSeconds == nil || *patchInfo.Sandbox.TTLSeconds != 7200 ||
		patchInfo.Sandbox.Subdomain == nil || *patchInfo.Sandbox.Subdomain != "lifecycle-run" {
		t.Fatalf("patched sandbox = %+v", patchInfo.Sandbox)
	}

	// Write a durable marker inside the guest before the stop. A resume that
	// silently replaces the disk with a fresh VM cannot return these bytes.
	const marker = "lifecycle marker bytes"
	markerCommand := `printf 'lifecycle marker bytes' > "$HOME/lifecycle-marker" && cat "$HOME/lifecycle-marker"`
	writtenRaw, written := env.lifecycleRunCommand(id, markerCommand)
	runTestCheck(t, v1, "command", writtenRaw)
	if !written.Success || written.ExitCode == nil || *written.ExitCode != 0 || written.Stdout != marker {
		t.Fatalf("marker write = %+v, want exit 0 and %q", written, marker)
	}
	vm, err := env.node.GetVM(t.Context(), &nodev1.GetVMRequest{SandboxId: id})
	if err != nil {
		t.Fatalf("GetVM before stop: %v", err)
	}
	if vm.State != nodev1.VMState_VM_STATE_READY {
		t.Fatalf("VM state before stop = %s, want READY", vm.State)
	}

	// Stop is accepted with the canonical operation and status URL.
	stopped := env.do(http.MethodPost, "/api/v1/sandboxes/"+id+"/stop", env.key, `{}`, nil)
	if stopped.status != http.StatusAccepted {
		t.Fatalf("stop status = %d body = %s", stopped.status, stopped.body)
	}
	runTestCheck(t, v1, "stop", stopped)
	var stopBody struct {
		Type      string `json:"type"`
		ID        string `json:"id"`
		Status    string `json:"status"`
		StatusURL string `json:"statusUrl"`
		Operation struct {
			ID     string `json:"id"`
			Status string `json:"status"`
		} `json:"operation"`
	}
	if err := json.Unmarshal(stopped.body, &stopBody); err != nil || stopBody.Operation.ID == "" {
		t.Fatalf("stop response is malformed: %v", err)
	}
	if stopBody.Type != "sandbox.stopping" || stopBody.Status != "archiving" ||
		stopBody.StatusURL != "/api/v1/sandboxes/"+id+"/stops/"+stopBody.Operation.ID {
		t.Fatalf("stop response = %+v", stopBody)
	}

	// The worker drives archiving to archived; poll the public GET, then read
	// the stop by its status URL.
	archived := env.lifecycleRunWaitState(id, "archived", 30*time.Second)
	if archived.Sandbox.StoppedAt == nil || archived.Sandbox.Stop == nil ||
		archived.Sandbox.Stop.ID != stopBody.Operation.ID || archived.Sandbox.Stop.Status != "completed" {
		t.Fatalf("archived sandbox metadata = %+v", archived.Sandbox)
	}
	vm, err = env.node.GetVM(t.Context(), &nodev1.GetVMRequest{SandboxId: id})
	if err != nil {
		t.Fatalf("GetVM after stop: %v", err)
	}
	if vm.State != nodev1.VMState_VM_STATE_STOPPED {
		t.Fatalf("VM state after stop = %s, want STOPPED", vm.State)
	}
	stopStatus := env.do(http.MethodGet, stopBody.StatusURL, env.key, "", nil)
	if stopStatus.status != http.StatusOK {
		t.Fatalf("stop status = %d body = %s", stopStatus.status, stopStatus.body)
	}
	runTestCheck(t, v1, "getStop", stopStatus)

	// A failFast resume with the node full is the pinned 503 and leaves the
	// sandbox archived. The reservation is released again before the real
	// resume so the same node can start it.
	if _, err := env.store.Pool().Exec(t.Context(), `
		UPDATE nodes SET reserved_vcpus = total_vcpus, reserved_memory_mib = total_memory_mib
		WHERE id = $1`, runTestNodeID); err != nil {
		t.Fatalf("fill node capacity: %v", err)
	}
	refused := env.do(http.MethodPost, "/api/v1/sandboxes/"+id+"/resume", env.key, `{"failFast":true}`, nil)
	if refused.status != http.StatusServiceUnavailable {
		t.Fatalf("failFast resume status = %d body = %s", refused.status, refused.body)
	}
	runTestCheck(t, v1, "resume", refused)
	if current := env.lifecycleRunWaitState(id, "archived", 5*time.Second); current.Sandbox.State != "archived" {
		t.Fatalf("refused resume changed state = %q", current.Sandbox.State)
	}
	if _, err := env.store.Pool().Exec(t.Context(), `
		UPDATE nodes SET reserved_vcpus = 0, reserved_memory_mib = 0
		WHERE id = $1`, runTestNodeID); err != nil {
		t.Fatalf("release node capacity: %v", err)
	}

	// Resume with an explicit null TTL returns the pinned resuming shape and
	// the worker brings the same sandbox back to ready without a new disk.
	resumed := env.do(http.MethodPost, "/api/v1/sandboxes/"+id+"/resume", env.key, `{"ttlSeconds":null}`, nil)
	if resumed.status != http.StatusAccepted {
		t.Fatalf("resume status = %d body = %s", resumed.status, resumed.body)
	}
	runTestCheck(t, v1, "resume", resumed)
	env.waitReady(id)
	ready := env.lifecycleRunWaitState(id, "ready", 5*time.Second)
	if ready.Sandbox.StoppedAt != nil || ready.Sandbox.TTLSeconds != nil {
		t.Fatalf("resumed sandbox metadata = %+v", ready.Sandbox)
	}
	vm, err = env.node.GetVM(t.Context(), &nodev1.GetVMRequest{SandboxId: id})
	if err != nil {
		t.Fatalf("GetVM after resume: %v", err)
	}
	if vm.State != nodev1.VMState_VM_STATE_READY {
		t.Fatalf("VM state after resume = %s, want READY", vm.State)
	}
	readRaw, read := env.lifecycleRunCommand(id, `cat "$HOME/lifecycle-marker"`)
	runTestCheck(t, v1, "command", readRaw)
	if !read.Success || read.ExitCode == nil || *read.ExitCode != 0 || read.Stdout != marker {
		t.Fatalf("marker after resume = %+v, want exit 0 and %q", read, marker)
	}

	// DELETE still uses the normal confirmation and operation poll.
	operation, deleted := env.deleteSandbox(id)
	runTestCheck(t, v1, "deleteSandbox", deleted)
	completed := env.waitDeletionComplete(operation)
	runTestCheck(t, v1, "getDeletionOperation", completed)
}
