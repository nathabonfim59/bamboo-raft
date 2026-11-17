package server_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/json/v2"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/nathabonfim59/bamboo-raft/internal/api"
	"github.com/nathabonfim59/bamboo-raft/internal/compat"
	"github.com/nathabonfim59/bamboo-raft/internal/db"
	"github.com/nathabonfim59/bamboo-raft/internal/db/dbtest"
)

// TestDaemonProcessSmoke builds the real bbraft-server once and runs the
// accepted B1.5 compiled-daemon checks. It is a PostgreSQL integration test:
// it skips unless BBRAFT_TEST_DATABASE_URL is set, and
// BBRAFT_TEST_REQUIRE_ALL=1 turns a missing database into a
// failure. The real binary is built and executed only inside this gated test;
// the node is nodefake, so this proves the control plane processes, not KVM or
// the compiled CLI.
func TestDaemonProcessSmoke(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("daemon process smoke tests inspect /proc and use SIGTERM")
	}
	// The parent gate runs before any build: no database means a skip, and
	// REQUIRE_ALL means a failure.
	dbtest.PoolConfig(t)
	v1, extension := daemonProcessValidators(t)
	root := compat.Root(t)
	binary := daemonProcessBuild(t, root)

	t.Run("SeparateProcesses", func(t *testing.T) {
		f := newDaemonProcessFixture(t, binary, root, v1, extension)
		f.startNodeHub(t)
		f.assertListenerOwnership(t, f.processes["nodehub"], []uint16{f.nodePort, f.gatewayPort}, []uint16{f.apiPort})
		f.startLifecycle(t)
		f.startAPI(t, "api")
		f.waitAPI(t)
		f.waitDBBackend(t, f.nodehubApp)
		f.waitDBBackend(t, f.lifecycleApp)
		f.waitDBBackend(t, f.apiApp)
		f.assertListenerOwnership(t, f.processes["api"], []uint16{f.apiPort}, []uint16{f.nodePort, f.gatewayPort})
		f.assertListenerOwnership(t, f.processes["lifecycle"], nil, []uint16{f.apiPort, f.nodePort, f.gatewayPort})
		f.assertDistinctProcesses(t, "nodehub", "lifecycle", "api")
		if len(f.processes) != 3 {
			t.Fatalf("separate mode started %d processes", len(f.processes))
		}

		f.startNode(t)
		f.waitNodeFresh(t, time.Time{})
		f.exerciseSandbox(t, "initial")

		mark := f.stopAPI(t)
		// Only the api process stopped; the surviving nodehub must write a
		// strictly newer inventory row after that observation.
		f.waitNodeFresh(t, mark)

		f.startAPI(t, "api-restart")
		f.waitAPI(t)
		f.assertListenerOwnership(t, f.processes["api-restart"], []uint16{f.apiPort}, []uint16{f.nodePort, f.gatewayPort})
		// The restarted api uses the surviving lifecycle and nodehub for a
		// complete create/command/upload/delete flow.
		f.exerciseSandbox(t, "after-restart")
	})

	t.Run("AllRolesOneProcess", func(t *testing.T) {
		f := newDaemonProcessFixture(t, binary, root, v1, extension)
		process := f.startAll(t)
		f.waitAPI(t)
		f.waitDBBackend(t, f.allApp)
		f.assertListenerOwnership(t, process, []uint16{f.apiPort, f.nodePort, f.gatewayPort}, nil)
		if len(f.processes) != 1 {
			t.Fatalf("all-roles mode started %d processes", len(f.processes))
		}

		f.startNode(t)
		f.waitNodeFresh(t, time.Time{})
		f.exerciseSandbox(t, "all-roles")
	})

	t.Run("StartupFailures", func(t *testing.T) {
		f := newDaemonProcessFixture(t, binary, root, v1, extension)
		t.Run("BadAPIKeyPrefix", func(t *testing.T) {
			process := f.startProcess(t, "bad-prefix", f.argsWithKeys("api", f.files.dbBadAPI, f.files.badKeys))
			process.expectCleanExit = false
			if err := process.waitExit(30 * time.Second); err != nil {
				t.Fatal("bad-prefix process did not exit")
			}
			if process.exitCode() == 0 {
				t.Fatal("bad-prefix process exited successfully")
			}
			log := process.log.String()
			if label, hit := f.secrets.hit(f.secrets.logs, []byte(log)); hit {
				t.Fatalf("bad-prefix log leaked %s", label)
			}
			// Production wraps a role failure in the fixed safe
			// classification; the raw credential-loader text must not
			// reach stderr.
			if !strings.Contains(log, "server: api failed:") {
				t.Fatal("bad-prefix process did not report the safe api failure classification")
			}
			if strings.Contains(log, "does not use the configured key prefix") {
				t.Fatal("bad-prefix process leaked the raw credential-loader diagnostic")
			}
			if daemonProcessAlive(process.pid) {
				t.Fatal("bad-prefix process stayed alive")
			}
			f.waitPortClosed(t, f.apiAddr, "bad-prefix api listener")
		})
		t.Run("UnknownRole", func(t *testing.T) {
			process := f.startProcess(t, "unknown-role", []string{"-roles", "nope"})
			process.expectCleanExit = false
			if err := process.waitExit(30 * time.Second); err != nil {
				t.Fatal("unknown-role process did not exit")
			}
			if process.exitCode() == 0 {
				t.Fatal("unknown-role process exited successfully")
			}
			log := process.log.String()
			if label, hit := f.secrets.hit(f.secrets.logs, []byte(log)); hit {
				t.Fatalf("unknown-role log leaked %s", label)
			}
			if !strings.Contains(log, "unknown role") {
				t.Fatal("unknown-role process did not report the refused role")
			}
		})
	})
}

// daemonProcessValidators loads the v1 API document and the extension
// document. Loading before any process starts keeps a
// missing spec from skipping after real services have run.
func daemonProcessValidators(t *testing.T) (*compat.Validator, *compat.Validator) {
	t.Helper()
	root := compat.Root(t)
	return compat.Load(t),
		compat.LoadFile(t, filepath.Join(root, "api", "bbraft-ext.yaml"))
}

// exerciseSandbox runs one authenticated plain-sandbox flow against the real
// API process: create 202 -> ready, command output and remote exit, signed
// upload issuance plus a bearerless PUT, and delete 202 -> completed. Every
// durable fact is read through the independent inspection store.
func (f *daemonProcessFixture) exerciseSandbox(t *testing.T, label string) {
	t.Helper()
	account, err := f.inspection.EnsureAccount(t.Context(), f.owner)
	if err != nil {
		t.Fatalf("%s: owner account missing", label)
	}
	f.ownerID = account.ID

	// Unauthenticated access is refused before any sandbox effect.
	unauthorized := f.doJSON(t, label+": me", http.MethodGet, api.PathMe, "", nil, nil)
	if unauthorized.status != http.StatusUnauthorized {
		t.Fatalf("%s: unauthenticated me status = %d", label, unauthorized.status)
	}
	daemonProcessCheck(t, f.v1, "me", unauthorized)

	// The actual daemon used the configured stream cap.
	info := f.doJSON(t, label+": info", http.MethodGet, api.PathInfo, f.apiKey, nil, nil)
	if info.status != http.StatusOK {
		t.Fatalf("%s: info status = %d", label, info.status)
	}
	daemonProcessCheck(t, f.extension, "bbraftInfo", info)
	var infoBody struct {
		Commands struct {
			StreamMaxSeconds int `json:"streamMaxSeconds"`
			SyncMaxSeconds   int `json:"syncMaxSeconds"`
		} `json:"commands"`
	}
	if err := json.Unmarshal(info.body, &infoBody); err != nil {
		t.Fatalf("%s: info response is malformed", label)
	}
	if infoBody.Commands.StreamMaxSeconds != 901 || infoBody.Commands.SyncMaxSeconds != 600 {
		t.Fatalf("%s: info caps = %d/%d", label, infoBody.Commands.StreamMaxSeconds, infoBody.Commands.SyncMaxSeconds)
	}

	created := f.doJSON(t, label+": create", http.MethodPost, "/api/v1/sandboxes", f.apiKey,
		[]byte(`{"type":"small","env":{"DAEMON_SMOKE_SENTINEL":"daemon-sentinel-env-value"},"ttlSeconds":600}`), nil)
	if created.status != http.StatusAccepted {
		t.Fatalf("%s: create status = %d", label, created.status)
	}
	daemonProcessCheck(t, f.v1, "create", created)
	var createdBody struct {
		Status  string `json:"status"`
		Sandbox struct {
			ID    string `json:"id"`
			State string `json:"state"`
		} `json:"sandbox"`
	}
	if err := json.Unmarshal(created.body, &createdBody); err != nil || createdBody.Sandbox.ID == "" {
		t.Fatalf("%s: create response is malformed", label)
	}
	if createdBody.Status != "provisioning" || createdBody.Sandbox.State != "provisioning" {
		t.Fatalf("%s: create status/state = %q/%q", label, createdBody.Status, createdBody.Sandbox.State)
	}
	id := createdBody.Sandbox.ID

	var got daemonProcessResponse
	f.waitFor(t, 20*time.Second, "sandbox ready", func() bool {
		got = f.doJSON(t, label+": get", http.MethodGet, "/api/v1/sandboxes/"+id, f.apiKey, nil, nil)
		if got.status != http.StatusOK {
			return false
		}
		var body struct {
			Sandbox struct {
				State string `json:"state"`
			} `json:"sandbox"`
		}
		if json.Unmarshal(got.body, &body) != nil {
			return false
		}
		return body.Sandbox.State == "ready" || body.Sandbox.State == "idle"
	})
	daemonProcessCheck(t, f.v1, "get", got)

	// The ready state precedes CompleteJob, so the durable job barrier comes
	// before the reservation assertions.
	f.waitJobDone(t, id, db.JobProvision)
	sandbox, err := f.inspection.Sandbox(t.Context(), f.ownerID, id)
	if err != nil {
		t.Fatalf("%s: sandbox row missing", label)
	}
	if sandbox.State != db.StateReady && sandbox.State != db.StateIdle {
		t.Fatalf("%s: sandbox row state = %s", label, sandbox.State)
	}
	if sandbox.NodeID == nil || *sandbox.NodeID != f.nodeID {
		t.Fatalf("%s: sandbox row has the wrong node", label)
	}
	if sandbox.Reserved.VCPUs == 0 || sandbox.SnapshotPin {
		t.Fatalf("%s: sandbox reservation/pin = %+v/%v", label, sandbox.Reserved, sandbox.SnapshotPin)
	}
	node, err := f.inspection.Node(t.Context(), f.nodeID)
	if err != nil {
		t.Fatalf("%s: node row missing", label)
	}
	if node.Reserved.VCPUs < sandbox.Reserved.VCPUs || node.Reserved.MemoryMiB < sandbox.Reserved.MemoryMiB {
		t.Fatalf("%s: node reservation is below the sandbox", label)
	}

	// Durable auth mapping and last-used.
	digest := sha256.Sum256([]byte(f.apiKey))
	keyRow, err := f.inspection.APIKeyBySHA256(t.Context(), digest)
	if err != nil || keyRow.LastUsedAt == nil || keyRow.Name != "main" {
		t.Fatalf("%s: api key row is not persisted/used", label)
	}
	if keyRow.AccountID != f.ownerID {
		t.Fatalf("%s: api key is mapped to the wrong account", label)
	}

	// Another tenant cannot read the sandbox.
	foreign := f.doJSON(t, label+": foreign get", http.MethodGet, "/api/v1/sandboxes/"+id, f.otherKey, nil, nil)
	if foreign.status != http.StatusNotFound {
		t.Fatalf("%s: foreign get status = %d", label, foreign.status)
	}
	daemonProcessCheck(t, f.v1, "get", foreign)

	// Command stdout and the remote non-zero exit.
	command := f.doJSON(t, label+": command", http.MethodPost, "/api/v1/sandboxes/"+id+"/commands", f.apiKey,
		[]byte(`{"command":"echo daemon-smoke; exit 7","timeoutSeconds":10}`), nil)
	if command.status != http.StatusOK {
		t.Fatalf("%s: command status = %d", label, command.status)
	}
	daemonProcessCheck(t, f.v1, "command", command)
	var commandBody struct {
		Success  bool   `json:"success"`
		ExitCode *int   `json:"exitCode"`
		Stdout   string `json:"stdout"`
		TimedOut bool   `json:"timedOut"`
	}
	if err := json.Unmarshal(command.body, &commandBody); err != nil {
		t.Fatalf("%s: command response is malformed", label)
	}
	if commandBody.Success || commandBody.TimedOut || commandBody.ExitCode == nil ||
		*commandBody.ExitCode != 7 || !strings.Contains(commandBody.Stdout, "daemon-smoke") {
		t.Fatalf("%s: command output/exit mismatch", label)
	}

	// Signed upload issuance, then a PUT with no bearer credential.
	issued := f.doJSON(t, label+": upload-url", http.MethodPost, "/api/v1/sandboxes/"+id+"/files/upload-url", f.apiKey,
		[]byte(`{"path":"/home/user/daemon-smoke.txt","ttlSeconds":60}`), nil)
	if issued.status != http.StatusOK {
		t.Fatalf("%s: upload-url status = %d", label, issued.status)
	}
	daemonProcessCheck(t, f.v1, "createUploadUrl", issued)
	var issuedBody struct {
		URL    string `json:"url"`
		Method string `json:"method"`
		Path   string `json:"path"`
	}
	if err := json.Unmarshal(issued.body, &issuedBody); err != nil {
		t.Fatalf("%s: upload-url response is malformed", label)
	}
	prefix := f.apiBase() + api.UploadReceiverPrefix + "/"
	token, ok := strings.CutPrefix(issuedBody.URL, prefix)
	if !ok || token == "" || issuedBody.Method != http.MethodPut || issuedBody.Path != "/home/user/daemon-smoke.txt" {
		t.Fatalf("%s: upload-url response does not use the trusted base URL", label)
	}
	f.secrets.addLog("signed upload token", token)

	payload := bytes.Repeat([]byte("daemon-smoke-upload-"), 4096)
	received := f.doURL(t, label+": upload receiver", http.MethodPut, issuedBody.URL, "", payload, "application/octet-stream", nil)
	if received.status != http.StatusOK {
		t.Fatalf("%s: upload receiver status = %d", label, received.status)
	}
	daemonProcessCheck(t, f.extension, "bbraftUploadReceiver", received)
	var uploaded struct {
		Type string `json:"type"`
		Path string `json:"path"`
		Size int64  `json:"size"`
	}
	if err := json.Unmarshal(received.body, &uploaded); err != nil {
		t.Fatalf("%s: upload receiver response is malformed", label)
	}
	if uploaded.Type != "file.uploaded" || uploaded.Path != "/home/user/daemon-smoke.txt" || uploaded.Size != int64(len(payload)) {
		t.Fatalf("%s: upload receiver fields do not match", label)
	}
	stored, err := os.ReadFile(filepath.Join(f.nodeRoot, "vms", id, "home/user", "daemon-smoke.txt"))
	if err != nil || !bytes.Equal(stored, payload) {
		t.Fatalf("%s: uploaded file is missing or differs", label)
	}

	// Delete 202 -> durable operation completed.
	deleted := f.doJSON(t, label+": delete", http.MethodDelete, "/api/v1/sandboxes/"+id, f.apiKey, nil,
		map[string]string{"X-Ascii-Confirm-Delete": id})
	if deleted.status != http.StatusAccepted {
		t.Fatalf("%s: delete status = %d", label, deleted.status)
	}
	daemonProcessCheck(t, f.v1, "deleteSandbox", deleted)
	var deletedBody struct {
		Operation struct {
			ID string `json:"id"`
		} `json:"operation"`
	}
	if err := json.Unmarshal(deleted.body, &deletedBody); err != nil || deletedBody.Operation.ID == "" {
		t.Fatalf("%s: delete response is malformed", label)
	}
	operationID := deletedBody.Operation.ID
	completed := f.waitDeletion(t, operationID)
	daemonProcessCheck(t, f.v1, "getDeletionOperation", completed)

	// The completed operation precedes CompleteJob, so wait for the durable
	// delete job before checking the released reservations.
	f.waitJobDone(t, id, db.JobDeleteVM)
	operation, err := f.inspection.DeletionOperation(t.Context(), f.ownerID, operationID)
	if err != nil || operation.Status != db.DeletionCompleted || operation.CompletedAt == nil {
		t.Fatalf("%s: deletion operation row is not completed", label)
	}
	goneRow, err := f.inspection.SandboxAny(t.Context(), f.ownerID, id)
	if err != nil || goneRow.DeletedAt == nil {
		t.Fatalf("%s: deleted sandbox row is still live", label)
	}
	if goneRow.Reserved != (db.Resources{}) {
		t.Fatalf("%s: deleted sandbox still holds a reservation", label)
	}
	node, err = f.inspection.Node(t.Context(), f.nodeID)
	if err != nil || node.Reserved != (db.Resources{}) {
		t.Fatalf("%s: node reservation was not released", label)
	}
	f.waitFor(t, 5*time.Second, "vm directory removed", func() bool {
		_, err := os.Stat(filepath.Join(f.nodeRoot, "vms", id))
		return errors.Is(err, os.ErrNotExist)
	})
	gone := f.doJSON(t, label+": get after delete", http.MethodGet, "/api/v1/sandboxes/"+id, f.apiKey, nil, nil)
	if gone.status != http.StatusNotFound {
		t.Fatalf("%s: deleted get status = %d", label, gone.status)
	}
	daemonProcessCheck(t, f.v1, "get", gone)

	if f.requests == 0 {
		t.Fatalf("%s: no request reached the daemon", label)
	}
}
