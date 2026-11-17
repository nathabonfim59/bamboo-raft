package server_test

// Compiled run cleanup and keep coverage on the rolling-TTL path: one metadata
// read before create, TTL 900, an immediate verified PATCH, exact detached
// timeout-0 command bodies, default deletion, and the final keep PATCH. Timing
// and renewal-failure edges live in the focused CLI tests; this flow proves the
// real server and CLI agree on the durable shape.

import (
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/nathabonfim59/bamboo-raft/internal/db"
)

// runFlows exercises the default cleanup run and a kept run.
func (h *phaseExitHarness) runFlows(t *testing.T) {
	t.Helper()
	h.runCleanup(t)
	h.runKeep(t)
}

// runCleanup runs a pushed command that exits nonzero and proves durable
// deletion plus the rolling TTL request shape.
func (h *phaseExitHarness) runCleanup(t *testing.T) {
	t.Helper()
	dir := e2eMakeSmallDir(t, "marker.txt", "run-marker")
	beforeCreate := len(h.node.createVMs())
	beforeDelete := h.apiProxy.count("deleteSandbox")
	beforePatch := h.apiProxy.count("update")

	frames, summary, errs, code := h.cli.runJSON(t, dir, "--timeout", "0", "--push", ".",
		"--", e2eRunNonzeroCommand)
	if len(errs) != 0 {
		t.Fatal("cleanup run emitted an error event")
	}
	if code != 7 || summary.ExitCode != 7 || !summary.Deleted {
		t.Fatalf("cleanup run exit/summary = %d/%d/%v", code, summary.ExitCode, summary.Deleted)
	}
	h.checkStreamFrames(t, frames, 7, false)
	h.checkStreamOutput(t, frames, "run-marker")

	create := h.lastCreate(t)
	if create.TTLSeconds == nil || *create.TTLSeconds != 900 || create.Type != nil {
		t.Fatal("rolling run create request did not use the 900-second TTL")
	}
	update := h.lastUpdate(t)
	if !phaseExitJSONInteger(update.TTLSeconds, 900) {
		t.Fatalf("rolling run PATCH TTL = %s, want 900", update.TTLSeconds)
	}
	if got := h.apiProxy.count("update"); got <= beforePatch {
		t.Fatal("rolling run did not PATCH its TTL")
	}
	command := h.lastCommand(t)
	if command.Detached == nil || !*command.Detached || command.TimeoutSeconds != nil {
		t.Fatal("rolling timeout-0 run did not use one preconfirmed detached start")
	}

	created := h.node.createVMs()
	if len(created) < beforeCreate+1 {
		t.Fatal("cleanup run did not create a sandbox")
	}
	if h.apiProxy.count("deleteSandbox") != beforeDelete+1 {
		t.Fatal("cleanup run did not delete exactly one sandbox")
	}
	h.phaseExitWaitRunDeleted(t, created[len(created)-1].sandboxID)
}

// runKeep proves --keep leaves the sandbox with a final 900-second PATCH and no
// deletion, then deletes it explicitly.
func (h *phaseExitHarness) runKeep(t *testing.T) {
	t.Helper()
	dir := e2eMakeSmallDir(t, "marker.txt", "human-marker")
	if err := os.MkdirAll(filepath.Join(dir, "sub"), 0o755); err != nil {
		t.Fatal("keep fixture directory failed")
	}
	base := filepath.Base(dir)
	beforeCreate := len(h.node.createVMs())
	beforeDelete := h.apiProxy.count("deleteSandbox")
	beforePatch := h.apiProxy.count("update")

	frames, summary, errs, code := h.cli.runJSON(t, dir, "--keep", "--timeout", "0", "--push", ".",
		"--cwd", "/home/user/"+base+"/sub", "--", e2eRunHumanCommand)
	if len(errs) != 0 {
		t.Fatal("keep run emitted an error event")
	}
	if code != 0 || summary.ExitCode != 0 || summary.Deleted {
		t.Fatalf("keep run exit/summary = %d/%d/%v", code, summary.ExitCode, summary.Deleted)
	}
	h.checkStreamFrames(t, frames, 0, false)
	h.checkStreamOutput(t, frames, "human-marker")

	create := h.lastCreate(t)
	if create.TTLSeconds == nil || *create.TTLSeconds != 900 || create.Type != nil {
		t.Fatal("keep run create request did not use the 900-second TTL")
	}
	if got := h.apiProxy.count("update"); got < beforePatch+2 {
		t.Fatalf("keep run PATCHes = %d, want the initial and final renewal", got-beforePatch)
	}
	update := h.lastUpdate(t)
	if !phaseExitJSONInteger(update.TTLSeconds, 900) {
		t.Fatalf("final keep PATCH TTL = %s, want 900", update.TTLSeconds)
	}
	if h.apiProxy.count("deleteSandbox") != beforeDelete {
		t.Fatal("keep run deleted its sandbox")
	}

	created := h.node.createVMs()
	if len(created) < beforeCreate+1 {
		t.Fatal("keep run did not create a sandbox")
	}
	runID := created[len(created)-1].sandboxID
	info := h.cli.info(t, runID)
	if info.Sandbox.TTLSeconds == nil || *info.Sandbox.TTLSeconds != 900 || info.Sandbox.ArchiveAfter == nil {
		t.Fatal("kept sandbox does not retain the 900-second timer")
	}

	h.cli.deleteWait(t, runID)
	if h.apiProxy.count("deleteSandbox") != beforeDelete+1 {
		t.Fatal("explicit keep cleanup did not delete exactly one sandbox")
	}
	h.phaseExitWaitRunDeleted(t, runID)
}

// phaseExitWaitRunDeleted waits for the run's durable deletion and VM removal.
func (h *phaseExitHarness) phaseExitWaitRunDeleted(t *testing.T, id string) {
	t.Helper()
	h.fixture.waitJobDone(t, id, db.JobDeleteVM)
	row, err := h.fixture.inspection.SandboxAny(t.Context(), h.fixture.ownerID, id)
	if err != nil || row.DeletedAt == nil {
		t.Fatal("run sandbox row is not deleted")
	}
	h.fixture.waitFor(t, 10*time.Second, "run vm directory removed", func() bool {
		_, err := os.Stat(filepath.Join(h.fixture.nodeRoot, "vms", id))
		return errors.Is(err, os.ErrNotExist)
	})
	h.fixture.waitFor(t, 10*time.Second, "run deletion completed", func() bool {
		var status string
		err := h.fixture.inspection.Pool().QueryRow(t.Context(), `
			SELECT status FROM deletion_operations
			WHERE owner_id = $1 AND target_id = $2 AND kind = 'sandbox'
			ORDER BY requested_at DESC LIMIT 1`, h.fixture.ownerID, id).Scan(&status)
		return err == nil && status == string(db.DeletionCompleted)
	})
}

// phaseExitJSONInteger reports whether raw is exactly one JSON integer equal to want.
func phaseExitJSONInteger(raw []byte, want int) bool {
	return strings.TrimSpace(string(raw)) == strconv.Itoa(want)
}
