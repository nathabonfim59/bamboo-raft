package jailer

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// A10 D1 fix round 1: the run budget covers helper setup, settlement never
// reports a canceled call as success, and the record writer refuses a
// final symlink even to its expected inode.

// TestRunToolSetupTimeoutSettles: a helper stuck before exec is bounded by
// the run's Timeout alone, killed, reaped by the sole waiter and settled.
func TestRunToolSetupTimeoutSettles(t *testing.T) {
	env, m := jailerToolEnv(t, jailerModeHang)
	spec := jailerToolSpec("/usr/bin/true", jailerToolTarget(t, os.O_RDONLY))
	spec.Timeout = 150 * time.Millisecond
	start := time.Now()
	if _, err := m.RunTool(t.Context(), spec); err == nil {
		t.Fatal("a hung helper succeeded")
	}
	if took := time.Since(start); took > 5*time.Second {
		t.Fatalf("RunTool took %s; the Timeout did not bound setup", took)
	}
	if len(env.toolOwner.spawned()) != 1 {
		t.Fatal("no helper was spawned")
	}
	if _, ok := env.toolOwner.next(10 * time.Second); !ok {
		t.Fatal("the timed-out helper was never reaped by its sole waiter")
	}
	jailerAssertToolSettled(t, env, m, spec)
}

func TestSettleToolsPreCanceledIsNotSuccess(t *testing.T) {
	env := jailerToolEnvOnly(t, jailerModeExec)
	record := jailerToolRecord(t, env, jailerToolTarget(t, os.O_RDONLY), 0, 0, "")
	jailerPlantToolRecord(t, env, record)
	m := jailerToolOpen(t, env)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := m.SettleTools(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("SettleTools = %v, want Canceled", err)
	}
	if _, ok := m.Record(record.SandboxID); !ok || len(env.reclaims()) != 0 {
		t.Fatal("a canceled SettleTools changed state")
	}
}

// TestRecordWriterRefusesSymlinkToExpectedInode: a record name replaced by
// a relative symlink to the very inode its owner expects is refused by
// publish and remove, and the linked file is preserved byte for byte.
func TestRecordWriterRefusesSymlinkToExpectedInode(t *testing.T) {
	dir := t.TempDir()
	if err := os.Mkdir(filepath.Join(dir, recordsDir), 0o700); err != nil {
		t.Fatal(err)
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	record := jailerReviewRecord(jailerReviewID, 50000)
	identity, err := (fileStates{}).Write(root, record, resourceOwnership{}, inodeIdentity{})
	if err != nil {
		t.Fatal(err)
	}
	name := filepath.Join(dir, recordsDir, jailerReviewID+".json")
	if err := os.Rename(name, name+".kept"); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(jailerReviewID+".json.kept", name); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(name + ".kept")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := (fileStates{}).Write(root, record, resourceOwnership{}, identity); err == nil {
		t.Error("Write replaced a symlink to the expected record inode")
	}
	if err := (fileStates{}).Remove(root, jailerReviewID, identity); err == nil {
		t.Error("Remove unlinked a symlink to the expected record inode")
	}
	after, err := os.ReadFile(name + ".kept")
	if err != nil || string(after) != string(before) {
		t.Fatal("the linked record was changed")
	}
	if info, err := os.Lstat(name); err != nil || info.Mode()&os.ModeSymlink == 0 {
		t.Fatal("the symlink itself was replaced or removed")
	}
}

// TestSettleToolsLeavesReapedRunToItsCall: between the reap and the
// RunTool's own settlement, SettleTools still treats the attempt as the
// call's: retained, not reclaimed; the call then settles it exactly once.
func TestSettleToolsLeavesReapedRunToItsCall(t *testing.T) {
	env, m := jailerToolEnv(t, jailerModeExec)
	spec := jailerToolSpec("/usr/bin/true", jailerToolTarget(t, os.O_RDONLY))
	var report ToolReport
	var settleErr error
	var reclaimsDuring int
	m.hooks.beforeToolSettle = func(string) {
		report, settleErr = m.SettleTools(t.Context())
		reclaimsDuring = len(env.reclaims())
	}
	if _, err := m.RunTool(t.Context(), spec); err != nil {
		t.Fatal(err)
	}
	ref := Ref{SandboxID: ToolID(spec.Attempt), Attempt: spec.Attempt}
	if settleErr != nil || len(report.Settled) != 0 || len(report.Retained) != 1 || report.Retained[0] != ref || reclaimsDuring != 0 {
		t.Fatalf("SettleTools during the call's settlement = %+v, %v (reclaims %d); want it retained untouched", report, settleErr, reclaimsDuring)
	}
	jailerAssertToolSettled(t, env, m, spec)
}
