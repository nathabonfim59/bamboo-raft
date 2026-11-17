package server

// End-to-end detached command coverage through the public HTTP API: the real
// api/lifecycle/nodehub roles, a real PostgreSQL schema and a real nodefake
// behind nodelink/nodegateway. It covers create, detached start, status tail
// and offset windows, attach to an already exited process, kill plus an
// idempotent second stop, and DELETE cleanup.
//
// Like the other server integration tests, newRunEnv requires
// BBRAFT_TEST_DATABASE_URL and skips locally when it is unset. The suite is
// meant to run in the team's root VM, where PostgreSQL and the nodefake guest
// commands are available; it does not need KVM or a booted image.

import (
	"encoding/json/v2"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/nathabonfim59/bamboo-raft/internal/compat"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
	"github.com/nathabonfim59/bamboo-raft/internal/server/config"
)

type processRunStarted struct {
	Type      string `json:"type"`
	ProcessID uint32 `json:"processId"`
}

type processRunStatus struct {
	Type             string `json:"type"`
	ProcessID        uint32 `json:"processId"`
	Status           string `json:"status"`
	Running          bool   `json:"running"`
	ExitCode         *int   `json:"exitCode"`
	Stdout           string `json:"stdout"`
	Stderr           string `json:"stderr"`
	StdoutOffset     *int64 `json:"stdoutOffset"`
	StderrOffset     *int64 `json:"stderrOffset"`
	StdoutNextOffset *int64 `json:"stdoutNextOffset"`
	StderrNextOffset *int64 `json:"stderrNextOffset"`
	More             *bool  `json:"more"`
}

type processRunStop struct {
	Type      string `json:"type"`
	ProcessID uint32 `json:"processId"`
	Running   bool   `json:"running"`
	Signaled  bool   `json:"signaled"`
	Command   *struct {
		Status   string `json:"status"`
		ExitCode *int   `json:"exitCode"`
	} `json:"command"`
}

type processRunAttachFrame struct {
	Type         string `json:"type"`
	Data         string `json:"data"`
	NextOffset   *int64 `json:"nextOffset"`
	StdoutOffset *int64 `json:"stdoutOffset"`
	StderrOffset *int64 `json:"stderrOffset"`
	Status       string `json:"status"`
	ExitCode     *int   `json:"exitCode"`
	Error        string `json:"error"`
}

func TestRunDetachedCommandsStatusAttachStopDelete(t *testing.T) {
	env := newRunEnv(t)
	v1, _ := runTestValidators(t)
	env.start(config.Roles{config.RoleAPI: true, config.RoleLifecycle: true, config.RoleNodeHub: true})
	env.startNode(0)
	env.waitNodeFresh()
	env.waitAPI()

	id := env.createSandbox(`{"type":"small","ttlSeconds":600,"name":"process-run"}`)
	env.waitReady(id)

	started := env.do(http.MethodPost, "/api/v1/sandboxes/"+id+"/commands", env.key,
		`{"command":"printf 'out'; printf '€'; printf 'err' >&2; sleep 0.2; exit 7","detached":true}`, nil)
	if started.status != http.StatusOK {
		t.Fatalf("detached start = %d body = %s", started.status, started.body)
	}
	runTestCheck(t, v1, "command", started)
	var startedBody processRunStarted
	if err := json.Unmarshal(started.body, &startedBody); err != nil || startedBody.Type != "command.started" || startedBody.ProcessID == 0 {
		t.Fatalf("detached start body = %s (%v)", started.body, err)
	}
	processID := startedBody.ProcessID

	// Without offsets the status is each log's tail.
	var statusResponse runResponse
	var status processRunStatus
	env.waitFor(30*time.Second, "detached command exited", func() bool {
		statusResponse = env.do(http.MethodGet,
			"/api/v1/sandboxes/"+id+"/commands/"+processIDString(processID), env.key, "", nil)
		if statusResponse.status != http.StatusOK {
			return false
		}
		return json.Unmarshal(statusResponse.body, &status) == nil && status.Status == "exited"
	})
	runTestCheck(t, v1, "commandStatus", statusResponse)
	if status.ExitCode == nil || *status.ExitCode != 7 || status.Stdout != "out€" || status.Stderr != "err" {
		t.Fatalf("status = %+v", status)
	}

	// Offset mode returns both streams from byte zero and exact next offsets.
	offsetResponse := env.do(http.MethodGet,
		"/api/v1/sandboxes/"+id+"/commands/"+processIDString(processID)+"?stdoutOffset=0&stderrOffset=0", env.key, "", nil)
	if offsetResponse.status != http.StatusOK {
		t.Fatalf("offset status = %d body = %s", offsetResponse.status, offsetResponse.body)
	}
	runTestCheck(t, v1, "commandStatus", offsetResponse)
	var offsetStatus processRunStatus
	if err := json.Unmarshal(offsetResponse.body, &offsetStatus); err != nil {
		t.Fatalf("offset status body = %s: %v", offsetResponse.body, err)
	}
	if offsetStatus.Stdout != "out€" || offsetStatus.Stderr != "err" ||
		offsetStatus.StdoutNextOffset == nil || *offsetStatus.StdoutNextOffset != 6 ||
		offsetStatus.StderrNextOffset == nil || *offsetStatus.StderrNextOffset != 3 ||
		offsetStatus.More == nil || *offsetStatus.More {
		t.Fatalf("offset status = %+v", offsetStatus)
	}

	// Attaching to the already exited process replays its log and terminal
	// exit, validating every NDJSON line against the pin and subset.
	attachBody := processRunAttach(t, env, v1, id, processID, "")
	attachFrames := processRunAttachFrames(t, attachBody)
	if len(attachFrames) < 3 || attachFrames[0].Type != "attached" {
		t.Fatalf("attach frames = %+v", attachFrames)
	}
	var stdout, stderr strings.Builder
	for _, frame := range attachFrames[1:] {
		switch frame.Type {
		case "stdout":
			stdout.WriteString(frame.Data)
		case "stderr":
			stderr.WriteString(frame.Data)
		}
	}
	terminal := attachFrames[len(attachFrames)-1]
	if terminal.Type != "exit" || terminal.ExitCode == nil || *terminal.ExitCode != 7 {
		t.Fatalf("attach terminal = %+v", terminal)
	}
	if stdout.String() != "out€" || stderr.String() != "err" {
		t.Fatalf("attach stdout/stderr = %q/%q", stdout.String(), stderr.String())
	}

	// Kill a running detached process. The durable record then answers a
	// second stop; the real-stack unit test proves the RPC count.
	running := env.do(http.MethodPost, "/api/v1/sandboxes/"+id+"/commands", env.key,
		`{"command":"sleep 30","detached":true}`, nil)
	if running.status != http.StatusOK {
		t.Fatalf("running start = %d body = %s", running.status, running.body)
	}
	runTestCheck(t, v1, "command", running)
	var runningBody processRunStarted
	if err := json.Unmarshal(running.body, &runningBody); err != nil || runningBody.ProcessID == 0 {
		t.Fatalf("running start body = %s", running.body)
	}
	stoppedResponse := env.do(http.MethodDelete,
		"/api/v1/sandboxes/"+id+"/commands/"+processIDString(runningBody.ProcessID), env.key, "", nil)
	if stoppedResponse.status != http.StatusOK {
		t.Fatalf("stop = %d body = %s", stoppedResponse.status, stoppedResponse.body)
	}
	runTestCheck(t, v1, "stopCommand", stoppedResponse)
	var stopped processRunStop
	if err := json.Unmarshal(stoppedResponse.body, &stopped); err != nil {
		t.Fatalf("stop body = %s: %v", stoppedResponse.body, err)
	}
	if stopped.Type != "command.stopped" || stopped.Running || !stopped.Signaled || stopped.Command == nil ||
		stopped.Command.Status != "killed" {
		t.Fatalf("stop = %+v", stopped)
	}
	final, err := env.node.GetProcess(t.Context(), &nodev1.GetProcessRequest{
		SandboxId: id, ProcessId: runningBody.ProcessID,
	})
	if err != nil {
		t.Fatalf("GetProcess after stop: %v", err)
	}
	if final.Process.State == nodev1.ProcessState_PROCESS_STATE_RUNNING {
		t.Error("process still running after stop")
	}

	// The durable terminal record answers the second stop. The real-stack
	// unit test proves the RPC count.
	second := env.do(http.MethodDelete,
		"/api/v1/sandboxes/"+id+"/commands/"+processIDString(runningBody.ProcessID), env.key, "", nil)
	if second.status != http.StatusOK {
		t.Fatalf("second stop = %d body = %s", second.status, second.body)
	}
	runTestCheck(t, v1, "stopCommand", second)
	var secondBody processRunStop
	if err := json.Unmarshal(second.body, &secondBody); err != nil {
		t.Fatalf("second stop body = %s: %v", second.body, err)
	}
	if secondBody.Signaled || secondBody.Command == nil || secondBody.Command.Status != "killed" {
		t.Fatalf("second stop = %+v", secondBody)
	}

	operationID, _ := env.deleteSandbox(id)
	env.waitDeletionComplete(operationID)
}

func processIDString(id uint32) string {
	return strconv.FormatUint(uint64(id), 10)
}

func processRunAttach(t *testing.T, env *runEnv, v1 *compat.Validator, sandboxID string, processID uint32, query string) []byte {
	t.Helper()
	target := env.apiBase + "/api/v1/sandboxes/" + sandboxID + "/commands/" + processIDString(processID) + "/stream"
	if query != "" {
		target += "?" + query
	}
	request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, target, nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer "+env.key)
	response, err := env.client.Do(request)
	if err != nil {
		t.Fatalf("attach request: %v", err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("read attach: %v", err)
	}
	if response.StatusCode != http.StatusOK {
		t.Fatalf("attach status = %d body = %s", response.StatusCode, body)
	}
	for name, validator := range map[string]*compat.Validator{"v1": v1} {
		if err := validator.NDJSON("attachCommand", response.StatusCode, response.Header, body); err != nil {
			t.Errorf("attach stream does not validate against %s: %v\nbody: %s", name, err, body)
		}
	}
	return body
}

func processRunAttachFrames(t *testing.T, body []byte) []processRunAttachFrame {
	t.Helper()
	var frames []processRunAttachFrame
	for _, line := range strings.Split(string(body), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		var frame processRunAttachFrame
		if err := json.Unmarshal([]byte(line), &frame); err != nil {
			t.Fatalf("attach frame %s: %v", line, err)
		}
		frames = append(frames, frame)
	}
	if len(frames) == 0 {
		t.Fatal("attach stream has no frames")
	}
	return frames
}
