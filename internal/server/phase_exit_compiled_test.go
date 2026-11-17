package server_test

// Phase-exit coverage through the compiled bbraft CLI and bbraft-server. The
// parent test is a PostgreSQL integration test: it skips unless
// BBRAFT_TEST_DATABASE_URL is set and BBRAFT_TEST_REQUIRE_ALL=1 turns a missing
// database or pinned spec into a failure. Both binaries are built only after
// that gate. The daemon runs every role with real TCP listeners, the real node
// gateway and nodefake behind the real nodelink transport. No test StoreOpener
// or route hook is used.
//
// The flow exercises the public control-plane routes and the accepted CLI
// behavior: login/create/PATCH/stop/resume/delete, representative core file
// operations and a signed download, sync/stream/detached/status/attach/kill,
// the exact dogfood exec argv, the old-server info fallback, and a controlled
// attach reconnect. Run cleanup and keep are added in their own file once the
// rolling-TTL handoff is accepted.

import (
	"bytes"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/nathabonfim59/bamboo-raft/internal/api"
	"github.com/nathabonfim59/bamboo-raft/internal/compat"
	"github.com/nathabonfim59/bamboo-raft/internal/db"
	"github.com/nathabonfim59/bamboo-raft/internal/db/dbtest"
)

// phaseExitHarness owns the compiled fixture, recording proxies and real CLI.
type phaseExitHarness struct {
	fixture     *daemonProcessFixture
	node        *e2eNode
	apiProxy    *e2eProxy
	signedProxy *e2eProxy
	cli         *e2eCLI
	v1          *compat.Validator
	extension   *compat.Validator
}

// phaseExitResponse is one direct HTTP answer captured through the recording
// proxy. Bodies stay in memory only and are never printed on failure.
type phaseExitResponse struct {
	status int
	header http.Header
	body   []byte
}

// phaseExitCreateBody is the create request subset this test asserts.
type phaseExitCreateBody struct {
	Type       *string `json:"type,omitzero"`
	TTLSeconds *int    `json:"ttlSeconds,omitzero"`
	Name       *string `json:"name,omitzero"`
}

// phaseExitUpdateBody is the PATCH request subset this test asserts. The raw
// TTL member keeps an omitted field distinguishable from an explicit null.
type phaseExitUpdateBody struct {
	Name       *string        `json:"name,omitzero"`
	TTLSeconds jsontext.Value `json:"ttlSeconds,omitzero"`
}

// phaseExitCommandBody is the command request subset this test asserts.
type phaseExitCommandBody struct {
	Command        string  `json:"command"`
	Cwd            *string `json:"cwd,omitzero"`
	TimeoutSeconds *int    `json:"timeoutSeconds,omitzero"`
	Stream         *bool   `json:"stream,omitzero"`
	Detached       *bool   `json:"detached,omitzero"`
}

// phaseExitLifecycleEvent is one new/resume progress event.
type phaseExitLifecycleEvent struct {
	Event string `json:"event"`
	ID    string `json:"id"`
	State string `json:"state"`
}

// TestPhaseExitCompiledCLI drives the real CLI against the compiled daemon.
func TestPhaseExitCompiledCLI(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("compiled CLI/server phase-exit tests use /proc, process groups, signals and a PTY")
	}
	// Gate before any build: no database means a skip, and REQUIRE_ALL means a
	// failure. The v1 API document is loaded before any process starts.
	dbtest.PoolConfig(t)
	v1, extension := daemonProcessValidators(t)
	root := compat.Root(t)

	serverBinary := daemonProcessBuild(t, root)
	cliBinary := e2eBuildCLI(t, root)

	f := newDaemonProcessFixture(t, serverBinary, root, v1, extension)
	account, err := f.inspection.EnsureAccount(t.Context(), f.owner)
	if err != nil {
		t.Fatal("owner account setup failed")
	}
	f.ownerID = account.ID

	apiProxy := newE2EProxy(t, "http://"+f.apiAddr, f.secrets)
	signedProxy := newE2EProxy(t, "http://"+f.apiAddr, f.secrets)

	args := e2eSetArg(f.args("all", f.files.dbAll), "-server.public-url", signedProxy.url())
	// The shipped default is 7200 seconds; pin it here so the old-server
	// fallback assertion checks the real cap instead of the smoke fixture's
	// smaller test override.
	args = e2eSetArg(args, "-commands.stream-max-seconds", "7200")
	f.startProcess(t, "all", args)
	f.waitAPI(t)
	f.waitDBBackend(t, f.allApp)
	node := e2eStartNode(t, f)
	f.waitNodeFresh(t, time.Time{})

	cli := e2eNewCLI(t, cliBinary, apiProxy.url(), f.secrets, signedProxy)
	h := &phaseExitHarness{
		fixture:     f,
		node:        node,
		apiProxy:    apiProxy,
		signedProxy: signedProxy,
		cli:         cli,
		v1:          v1,
		extension:   extension,
	}
	// Registered after the daemon/node: LIFO means CLI processes stop first,
	// then this validation runs, then the node/daemon stop and proxies close.
	t.Cleanup(func() { h.validateWire(t) })
	h.run(t)
}

// run is the ordered phase-exit scenario.
func (h *phaseExitHarness) run(t *testing.T) {
	h.cli.login(t, h.fixture.apiKey)
	h.checkInfo(t)
	id := h.create(t)
	h.patch(t, id)
	h.files(t, id)
	h.commands(t, id)
	h.oldServerFallback(t, id)
	h.reconnect(t, id)
	h.runFlows(t)
	h.lifecycle(t, id)
	h.delete(t, id)
}

// checkInfo proves the real role wiring advertises detached commands and keeps
// the shipped streamed cap.
func (h *phaseExitHarness) checkInfo(t *testing.T) {
	t.Helper()
	response := h.api(t, http.MethodGet, api.PathInfo, nil, "")
	if response.status != http.StatusOK {
		t.Fatalf("info status = %d", response.status)
	}
	var info api.InfoResponse
	if err := json.Unmarshal(response.body, &info); err != nil {
		t.Fatal("info response is malformed")
	}
	if !info.OK || info.Type != api.TypeBBraftInfo || info.Version == "" {
		t.Fatal("info envelope is not bbraft.info")
	}
	if !info.Commands.Detached {
		t.Fatal("compiled server does not advertise commands.detached")
	}
	if info.Commands.StreamMaxSeconds != 7200 || info.Commands.SyncMaxSeconds != api.DefaultSyncMaxSeconds {
		t.Fatalf("info command caps = %d/%d", info.Commands.StreamMaxSeconds, info.Commands.SyncMaxSeconds)
	}
}

// create makes the one sandbox used by the flow.
func (h *phaseExitHarness) create(t *testing.T) string {
	t.Helper()
	id := h.cli.newSandbox(t, "--type", "small", "--ttl", "600", "--name", "phase-exit")
	exchange, ok := h.apiProxy.last("create")
	if !ok {
		t.Fatal("no create request was captured")
	}
	var body phaseExitCreateBody
	if err := json.Unmarshal(exchange.reqBody, &body); err != nil {
		t.Fatal("create request is malformed")
	}
	if body.Type == nil || *body.Type != "small" || body.TTLSeconds == nil || *body.TTLSeconds != 600 ||
		body.Name == nil || *body.Name != "phase-exit" {
		t.Fatal("create request differs")
	}
	h.fixture.waitJobDone(t, id, db.JobProvision)
	return id
}

// patch exercises rename and the three extend selectors through the CLI.
func (h *phaseExitHarness) patch(t *testing.T, id string) {
	t.Helper()
	raw := h.cli.runOK(t, "rename", t.TempDir(), "rename", id, "phase-exit-renamed", "--json")
	var renamed api.SandboxInfoResponse
	e2eDecode(t, "rename", raw, &renamed)
	if renamed.Sandbox.ID != id || renamed.Sandbox.Name != "phase-exit-renamed" {
		t.Fatal("rename response differs")
	}
	body := h.lastUpdate(t)
	if body.Name == nil || *body.Name != "phase-exit-renamed" || len(body.TTLSeconds) != 0 {
		t.Fatal("rename request must carry only the name")
	}

	raw = h.cli.runOK(t, "extend hours", t.TempDir(), "extend", id, "--hours", "2", "--json")
	var extended api.SandboxInfoResponse
	e2eDecode(t, "extend hours", raw, &extended)
	if extended.Sandbox.TTLSeconds == nil || *extended.Sandbox.TTLSeconds != 7200 ||
		extended.Sandbox.ArchiveAfter == nil {
		t.Fatal("extend --hours response differs")
	}
	body = h.lastUpdate(t)
	if !bytes.Equal(body.TTLSeconds, []byte("7200")) {
		t.Fatalf("extend --hours request TTL = %s", body.TTLSeconds)
	}

	raw = h.cli.runOK(t, "extend off", t.TempDir(), "extend", id, "--no-auto-stop", "--json")
	var disabled api.SandboxInfoResponse
	e2eDecode(t, "extend off", raw, &disabled)
	if disabled.Sandbox.TTLSeconds != nil || disabled.Sandbox.ArchiveAfter != nil {
		t.Fatal("extend --no-auto-stop did not disable the timer")
	}
	body = h.lastUpdate(t)
	if !bytes.Equal(body.TTLSeconds, []byte("null")) {
		t.Fatalf("extend --no-auto-stop request TTL = %s", body.TTLSeconds)
	}

	raw = h.cli.runOK(t, "extend ttl", t.TempDir(), "extend", id, "--ttl", "600", "--json")
	var restored api.SandboxInfoResponse
	e2eDecode(t, "extend ttl", raw, &restored)
	if restored.Sandbox.TTLSeconds == nil || *restored.Sandbox.TTLSeconds != 600 {
		t.Fatal("extend --ttl response differs")
	}
	body = h.lastUpdate(t)
	if !bytes.Equal(body.TTLSeconds, []byte("600")) {
		t.Fatalf("extend --ttl request TTL = %s", body.TTLSeconds)
	}
}

// commands covers sync, the exact dogfood argv through the detached path, and
// explicit detach/status/attach/kill with one start POST each.
func (h *phaseExitHarness) commands(t *testing.T, id string) {
	t.Helper()
	h.execSync(t, id)
	h.execDogfood(t, id)
	h.execDetachedFlow(t, id)
	h.execLongKill(t, id)
	h.execAttachCancel(t, id)
}

// execSync runs one ordinary synchronous command and checks its exact output
// and remote exit status.
func (h *phaseExitHarness) execSync(t *testing.T, id string) {
	t.Helper()
	stdout, stderr, code := h.cli.run(t, "exec sync", t.TempDir(), nil,
		"exec", "--json", id, e2eExecCode7Command)
	h.cli.checkSecrets(t, "exec sync", stdout, stderr)
	if code != 7 {
		t.Fatalf("sync exec exit = %d, want 7", code)
	}
	var response api.CommandResponse
	e2eDecode(t, "exec sync", stdout, &response)
	if !response.OK || response.Type != api.TypeCommandFinished || response.Success ||
		response.ExitCode == nil || *response.ExitCode != 7 ||
		response.Stdout != "out" || response.Stderr != "err" {
		t.Fatal("sync exec response differs")
	}
	body := h.lastCommand(t)
	if body.TimeoutSeconds == nil || *body.TimeoutSeconds != api.DefaultCommandTimeoutSeconds ||
		(body.Stream != nil && *body.Stream) || body.Detached != nil {
		t.Fatal("sync exec request differs")
	}
}

// execDogfood runs the exact agent-harness argv form with --timeout 0 and proves the
// advertised capability selects one detached start plus one attach stream.
func (h *phaseExitHarness) execDogfood(t *testing.T, id string) {
	t.Helper()
	beforeInfo := h.apiProxy.count("bbraftInfo")
	beforeStart := h.apiProxy.count("command")
	beforeAttach := h.apiProxy.count("attachCommand")
	frames, code := h.cli.execArgvFrames(t, id, "0", "bash", "-lc", e2eExecCode7Command)
	if code != 7 {
		t.Fatalf("dogfood exec exit = %d, want 7", code)
	}
	h.checkStreamFrames(t, frames, 7, false)
	body := h.lastCommand(t)
	if body.Detached == nil || !*body.Detached || body.TimeoutSeconds != nil ||
		(body.Stream != nil && *body.Stream) || body.Command != e2eTestVMArgvCommand {
		t.Fatal("dogfood exec did not use the detached start shape")
	}
	if got := h.apiProxy.count("bbraftInfo"); got != beforeInfo+1 {
		t.Fatalf("dogfood metadata requests = %d, want 1", got-beforeInfo)
	}
	if got := h.apiProxy.count("command"); got != beforeStart+1 {
		t.Fatalf("dogfood command POSTs = %d, want 1", got-beforeStart)
	}
	if got := h.apiProxy.count("attachCommand"); got != beforeAttach+1 {
		t.Fatalf("dogfood attach requests = %d, want 1", got-beforeAttach)
	}
}

// execDetachedFlow starts a quick detached process, reads its status and
// replays its output through attach. Attach must not POST another command.
func (h *phaseExitHarness) execDetachedFlow(t *testing.T, id string) {
	t.Helper()
	started := h.detach(t, id, "printf 'detach-out'; printf 'detach-err' >&2; exit 7")
	if started.Command != "printf 'detach-out'; printf 'detach-err' >&2; exit 7" {
		t.Fatal("detached start did not echo the exact command")
	}

	beforeStatus := h.apiProxy.count("commandStatus")
	status := h.waitDetachedExit(t, id, started.ProcessID)
	if status.ExitCode == nil || *status.ExitCode != 7 || status.Stdout != "detach-out" || status.Stderr != "detach-err" {
		t.Fatal("detached status output/exit differs")
	}
	if h.apiProxy.count("commandStatus") == beforeStatus {
		t.Fatal("detached status did not use the status route")
	}

	beforeStart := h.apiProxy.count("command")
	stdout, stderr, code := h.cli.run(t, "attach exited", t.TempDir(), nil,
		"exec", id, "--attach", strconv.FormatUint(uint64(started.ProcessID), 10), "--json")
	h.cli.checkSecrets(t, "attach exited", stdout, stderr)
	if code != 7 {
		t.Fatalf("attach exit = %d, want 7", code)
	}
	parsed := e2eParseCommandFrames(t, "attach exited", stdout)
	h.checkStreamFrames(t, parsed, 7, false)
	h.checkStreamOutput(t, parsed, "detach-out")
	if h.apiProxy.count("command") != beforeStart {
		t.Fatal("attach posted the command again")
	}

	stopped := h.kill(t, id, started.ProcessID)
	if stopped.Signaled || stopped.Running || stopped.Command == nil || stopped.Command.Status != "exited" {
		t.Fatal("killing an already ended process did not return its terminal record")
	}
}

// execLongKill starts a long detached process, kills it, and proves the stop
// route reports a signalled process that is no longer running.
func (h *phaseExitHarness) execLongKill(t *testing.T, id string) {
	t.Helper()
	started := h.detach(t, id, "sleep 30")
	stopped := h.kill(t, id, started.ProcessID)
	if !stopped.Signaled || stopped.Running || stopped.Command == nil ||
		stopped.Command.Status != "killed" {
		t.Fatal("long kill did not signal a running process")
	}
	status := h.status(t, id, started.ProcessID)
	if status.Running || status.Status != "exited" {
		t.Fatalf("killed process status = %q running=%v, want exited/false", status.Status, status.Running)
	}
	// The pinned wire enum is running/exited/lost: the durable killed
	// record maps to exited here. A signal kill must carry exactly the
	// signal member, never an exit code.
	if status.ExitCode != nil || status.Signal == nil || *status.Signal == "" {
		t.Fatal("killed process status exit members are not coherent")
	}
}

// execAttachCancel proves cancelling an explicit attach exits 130 and leaves
// the detached process running until an explicit kill.
func (h *phaseExitHarness) execAttachCancel(t *testing.T, id string) {
	t.Helper()
	started := h.detach(t, id, "printf 'attach-started\\n'; sleep 30")
	pid := strconv.FormatUint(uint64(started.ProcessID), 10)
	running := h.cli.start(t, "attach cancel", t.TempDir(), "exec", id, "--attach", pid, "--json")
	h.fixture.waitFor(t, 15*time.Second, "attach output started", func() bool {
		return bytes.Contains(running.stdout.Bytes(), []byte("attach-started"))
	})
	if err := running.signal(os.Interrupt); err != nil {
		t.Fatal("attach cancel: SIGINT could not be delivered")
	}
	stdout, stderr, code := running.wait(t, 30*time.Second)
	h.cli.checkSecrets(t, "attach cancel", stdout, stderr)
	if code != 130 {
		t.Fatalf("attach cancel exit = %d, want 130", code)
	}
	status := h.status(t, id, started.ProcessID)
	if !status.Running {
		t.Fatal("cancelling attach stopped the detached process")
	}
	stopped := h.kill(t, id, started.ProcessID)
	if !stopped.Signaled {
		t.Fatal("cleanup kill did not signal the detached process")
	}
}

// detach starts one detached command and proves it used exactly one POST.
func (h *phaseExitHarness) detach(t *testing.T, id, command string) api.CommandStartedResponse {
	t.Helper()
	before := h.apiProxy.count("command")
	raw := h.cli.runOK(t, "detach", t.TempDir(), "exec", "--detach", "--json", id, command)
	var started api.CommandStartedResponse
	e2eDecode(t, "detach", raw, &started)
	if !started.OK || started.Type != api.TypeCommandStarted || started.ProcessID == 0 || started.PID <= 0 {
		t.Fatal("detached start response differs")
	}
	if got := h.apiProxy.count("command"); got != before+1 {
		t.Fatalf("detached start POSTs = %d, want 1", got-before)
	}
	body := h.lastCommand(t)
	if body.Detached == nil || !*body.Detached || body.Command != command {
		t.Fatal("detached start request differs")
	}
	h.ownDetached(t, started)
	return started
}

// ownDetached registers best-effort host cleanup for a nodefake process group.
func (h *phaseExitHarness) ownDetached(t *testing.T, started api.CommandStartedResponse) {
	t.Helper()
	pid := int(started.PID)
	start, ok := e2eProcessStartTime(pid)
	if !ok {
		return
	}
	t.Cleanup(func() { e2eKillOwnedGroup(pid, 0, start, 0) })
}

// status reads one detached process status through the CLI.
func (h *phaseExitHarness) status(t *testing.T, id string, processID uint32) api.CommandStatusResponse {
	t.Helper()
	raw := h.cli.runOK(t, "status", t.TempDir(),
		"exec", id, "--status", strconv.FormatUint(uint64(processID), 10), "--json")
	var status api.CommandStatusResponse
	e2eDecode(t, "status", raw, &status)
	if !status.OK || status.Type != api.TypeCommandStatus || status.ProcessID != processID {
		t.Fatal("status response differs")
	}
	return status
}

// waitDetachedExit polls the status route until the process is terminal.
func (h *phaseExitHarness) waitDetachedExit(t *testing.T, id string, processID uint32) api.CommandStatusResponse {
	t.Helper()
	var status api.CommandStatusResponse
	h.fixture.waitFor(t, 30*time.Second, "detached command ended", func() bool {
		status = h.status(t, id, processID)
		return !status.Running && status.Status == "exited"
	})
	return status
}

// kill stops one detached process through the CLI.
func (h *phaseExitHarness) kill(t *testing.T, id string, processID uint32) api.CommandStopResponse {
	t.Helper()
	raw := h.cli.runOK(t, "kill", t.TempDir(),
		"exec", id, "--kill", strconv.FormatUint(uint64(processID), 10), "--json")
	var stopped api.CommandStopResponse
	e2eDecode(t, "kill", raw, &stopped)
	if !stopped.OK || stopped.Type != api.TypeCommandStopped || stopped.ProcessID != processID {
		t.Fatal("kill response differs")
	}
	return stopped
}

// checkStreamFrames validates the legacy stream shape the CLI emits for both
// streamed and transparent detached executions.
func (h *phaseExitHarness) checkStreamFrames(t *testing.T, frames []api.CommandStreamFrame, want int, timedOut bool) {
	t.Helper()
	if len(frames) == 0 || frames[len(frames)-1].Type != api.CommandFrameExit {
		t.Fatal("command stream has no terminal exit frame")
	}
	last := frames[len(frames)-1]
	if !last.Success.Present || last.Success.Value == nil || *last.Success.Value != (want == 0) ||
		!last.TimedOut.Present || last.TimedOut.Value == nil || *last.TimedOut.Value != timedOut {
		t.Fatal("exit frame flags differ")
	}
	if timedOut {
		if !last.ExitCode.Present || last.ExitCode.Value != nil {
			t.Fatal("timeout exit frame must carry a null exitCode")
		}
		return
	}
	if !last.ExitCode.Present || last.ExitCode.Value == nil || *last.ExitCode.Value != want {
		t.Fatal("exit frame exitCode differs")
	}
}

// checkStreamOutput requires one stdout substring across the stream frames.
func (h *phaseExitHarness) checkStreamOutput(t *testing.T, frames []api.CommandStreamFrame, want string) {
	t.Helper()
	var output strings.Builder
	for _, frame := range frames {
		if frame.Type == api.CommandFrameStdout {
			output.WriteString(frame.Data)
		}
	}
	if !strings.Contains(output.String(), want) {
		t.Fatalf("command output is missing %q", want)
	}
}

// lifecycle stops and resumes the same retained disk through the CLI.
func (h *phaseExitHarness) lifecycle(t *testing.T, id string) {
	t.Helper()
	before, err := h.fixture.inspection.Sandbox(t.Context(), h.fixture.ownerID, id)
	if err != nil || before.NodeID == nil {
		t.Fatal("sandbox row is missing before stop")
	}
	nodeID := *before.NodeID

	raw := h.cli.runOK(t, "stop", t.TempDir(), "stop", id, "--json")
	var stopping api.SandboxActionResponse
	e2eDecode(t, "stop", raw, &stopping)
	if stopping.ID != id || stopping.Type != api.TypeSandboxStopping || stopping.Operation == nil ||
		stopping.Operation.ID == "" || stopping.StatusURL == nil {
		t.Fatal("stop acceptance response differs")
	}
	operationID := stopping.Operation.ID

	archived := h.waitSandboxState(t, id, api.StateArchived)
	if archived.Sandbox.StoppedAt == nil || archived.Sandbox.Stop == nil ||
		archived.Sandbox.Stop.ID != operationID || archived.Sandbox.Stop.Status != api.StopCompleted {
		t.Fatal("archived sandbox does not export its completed stop")
	}
	if archived.Sandbox.SSHEndpoint != nil {
		t.Fatal("stop did not clear the SSH endpoint")
	}
	stops := h.cli.runOK(t, "stop status", t.TempDir(), "stop", id, "--status", operationID, "--json")
	var stopStatus api.StopOperationResponse
	e2eDecode(t, "stop status", stops, &stopStatus)
	if stopStatus.ID != id || stopStatus.Stop.ID != operationID ||
		stopStatus.Stop.Status != api.StopCompleted || stopStatus.Stop.EndedAt == nil {
		t.Fatal("stop status response differs")
	}

	resume := h.cli.runOK(t, "resume", t.TempDir(), "resume", id, "--json")
	if !phaseExitResumeReady(t, resume, id) {
		t.Fatal("resume did not emit exactly one ready event")
	}
	ready := h.cli.info(t, id)
	if ready.Sandbox.State != api.StateReady || ready.Sandbox.SSHEndpoint != nil {
		t.Fatal("resumed sandbox state/SSH endpoint differs")
	}
	body := h.lastCommand(t)
	if body.Command != "true" || body.TimeoutSeconds == nil || *body.TimeoutSeconds != 30 ||
		body.Detached != nil {
		t.Fatal("resume readiness probe request differs")
	}
	after, err := h.fixture.inspection.Sandbox(t.Context(), h.fixture.ownerID, id)
	if err != nil || after.NodeID == nil || *after.NodeID != nodeID {
		t.Fatal("resume did not keep the retained disk on its node")
	}
	if _, err := os.Stat(filepath.Join(h.fixture.nodeRoot, "vms", id)); err != nil {
		t.Fatal("resume lost the retained VM directory")
	}
}

// phaseExitResumeReady parses resume's JSON lines and requires states before a
// terminal ready event naming the sandbox.
func phaseExitResumeReady(t *testing.T, raw []byte, id string) bool {
	t.Helper()
	ready := 0
	var states []string
	for _, line := range bytes.Split(raw, []byte{'\n'}) {
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		var event phaseExitLifecycleEvent
		e2eDecode(t, "resume", line, &event)
		switch event.Event {
		case "state":
			if event.ID != id || event.State == "" {
				t.Fatalf("resume state event differs")
			}
			states = append(states, event.State)
		case "ready":
			if event.ID != id {
				t.Fatalf("resume ready names %q, want %q", event.ID, id)
			}
			ready++
		default:
			t.Fatalf("resume emitted unknown event %q", event.Event)
		}
	}
	return ready == 1 && len(states) > 0
}

// waitSandboxState polls GET through the proxy until the sandbox reaches state.
func (h *phaseExitHarness) waitSandboxState(t *testing.T, id string, state api.SandboxState) api.SandboxInfoResponse {
	t.Helper()
	var info api.SandboxInfoResponse
	h.fixture.waitFor(t, 30*time.Second, "sandbox state "+string(state), func() bool {
		response := h.api(t, http.MethodGet, "/api/v1/sandboxes/"+id, nil, "")
		if response.status != http.StatusOK {
			return false
		}
		if err := json.Unmarshal(response.body, &info); err != nil {
			return false
		}
		return info.Sandbox.State == state
	})
	return info
}

// delete removes the sandbox and proves the durable cleanup.
func (h *phaseExitHarness) delete(t *testing.T, id string) {
	t.Helper()
	operationID := h.cli.deleteWait(t, id)
	h.fixture.waitJobDone(t, id, db.JobDeleteVM)
	row, err := h.fixture.inspection.SandboxAny(t.Context(), h.fixture.ownerID, id)
	if err != nil || row.DeletedAt == nil {
		t.Fatal("deleted sandbox row is still live")
	}
	operation, err := h.fixture.inspection.DeletionOperation(t.Context(), h.fixture.ownerID, operationID)
	if err != nil || operation.Status != db.DeletionCompleted {
		t.Fatal("deletion operation row is not completed")
	}
	h.fixture.waitFor(t, 10*time.Second, "vm directory removed", func() bool {
		_, err := os.Stat(filepath.Join(h.fixture.nodeRoot, "vms", id))
		return os.IsNotExist(err)
	})
	h.fixture.waitFor(t, 15*time.Second, "node reservations released", func() bool {
		node, err := h.fixture.inspection.Node(t.Context(), h.fixture.nodeID)
		return err == nil && node.Reserved == (db.Resources{})
	})
}

// api performs one authenticated request through the recording proxy.
func (h *phaseExitHarness) api(t *testing.T, method, path string, body []byte, contentType string) phaseExitResponse {
	t.Helper()
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	request, err := http.NewRequestWithContext(t.Context(), method, h.apiProxy.url()+path, reader)
	if err != nil {
		t.Fatal("build API request failed")
	}
	request.Header.Set("Authorization", "Bearer "+h.fixture.apiKey)
	if body != nil {
		if contentType == "" {
			contentType = "application/json"
		}
		request.Header.Set("Content-Type", contentType)
	}
	response, err := h.fixture.httpClient.Do(request)
	if err != nil {
		t.Fatal("API request failed")
	}
	defer response.Body.Close()
	data, err := io.ReadAll(io.LimitReader(response.Body, 8<<20))
	if err != nil {
		t.Fatal("API response read failed")
	}
	return phaseExitResponse{status: response.StatusCode, header: response.Header.Clone(), body: data}
}

// apiOK performs one authenticated request and requires status.
func (h *phaseExitHarness) apiOK(t *testing.T, method, path string, body []byte, want int) phaseExitResponse {
	t.Helper()
	response := h.api(t, method, path, body, "")
	if response.status != want {
		t.Fatalf("%s %s status = %d, want %d", method, path, response.status, want)
	}
	return response
}

// lastCreate returns the newest create request body.
func (h *phaseExitHarness) lastCreate(t *testing.T) phaseExitCreateBody {
	t.Helper()
	exchange, ok := h.apiProxy.last("create")
	if !ok {
		t.Fatal("no create request was captured")
	}
	var body phaseExitCreateBody
	if err := json.Unmarshal(exchange.reqBody, &body); err != nil {
		t.Fatal("create request is malformed")
	}
	return body
}

// lastUpdate returns the newest PATCH request body.
func (h *phaseExitHarness) lastUpdate(t *testing.T) phaseExitUpdateBody {
	t.Helper()
	exchange, ok := h.apiProxy.last("update")
	if !ok {
		t.Fatal("no PATCH request was captured")
	}
	var body phaseExitUpdateBody
	if err := json.Unmarshal(exchange.reqBody, &body); err != nil {
		t.Fatal("PATCH request is malformed")
	}
	return body
}

// lastCommand returns the newest command request body.
func (h *phaseExitHarness) lastCommand(t *testing.T) phaseExitCommandBody {
	t.Helper()
	exchange, ok := h.apiProxy.last("command")
	if !ok {
		t.Fatal("no command request was captured")
	}
	var body phaseExitCommandBody
	if err := json.Unmarshal(exchange.reqBody, &body); err != nil {
		t.Fatal("command request is malformed")
	}
	return body
}

// validateWire checks every captured body, required bearer presence and the
// minimum route coverage that makes this a phase exit rather than a unit test.
func (h *phaseExitHarness) validateWire(t *testing.T) {
	t.Helper()
	h.apiProxy.validate(t, h.v1, h.extension)
	h.signedProxy.validate(t, h.v1, h.extension)
	for _, token := range h.signedProxy.tokens() {
		h.fixture.secrets.addLog("signed download token", token)
	}
	for _, reason := range h.node.violations() {
		t.Errorf("test node: %s", reason)
	}
	if t.Failed() {
		return
	}
	for _, op := range []string{
		"update", "stop", "getStop", "resume",
		"command", "commandStatus", "attachCommand", "stopCommand",
		"createFolder", "writeFile", "readFile", "getFileInfo", "listFiles",
		"setFilePermissions", "moveFile", "deleteFile", "createDownloadUrl",
		"bbraftDownloadReceiver",
	} {
		if h.apiProxy.count(op) == 0 && h.signedProxy.count(op) == 0 {
			t.Errorf("phase-exit flow never exercised %s", op)
		}
	}
	if got := h.apiProxy.count("bbraftDownloadReceiver"); got != 0 {
		t.Errorf("API proxy saw %d signed downloads, want 0", got)
	}
	if got := h.signedProxy.count("createDownloadUrl"); got != 0 {
		t.Errorf("signed proxy saw %d download-url issuances, want 0", got)
	}
	if h.apiProxy.count("create") != 3 {
		t.Errorf("create requests = %d, want 3 (base plus cleanup and keep runs)", h.apiProxy.count("create"))
	}
	if h.apiProxy.count("deleteSandbox") != 3 {
		t.Errorf("delete requests = %d, want 3 (base, cleanup run, explicit keep cleanup)", h.apiProxy.count("deleteSandbox"))
	}
	if h.node.counts().CreateVM < 3 || h.node.counts().DeleteVM < 3 {
		t.Errorf("node VM calls = create %d delete %d, want at least 3/3", h.node.counts().CreateVM, h.node.counts().DeleteVM)
	}
}

// phaseExitMarshal is a small helper for typed request bodies.
func phaseExitMarshal(t *testing.T, value any) []byte {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal request body: %v", err)
	}
	return raw
}
