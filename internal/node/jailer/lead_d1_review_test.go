package jailer

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"testing"
	"time"
)

func TestLeadToolTimeoutIncludesHelperSetup(t *testing.T) {
	env, m := jailerToolEnv(t, jailerModeHang)
	ctx, cancel := context.WithCancel(t.Context())
	spec := jailerToolSpec("/usr/bin/true", jailerToolTarget(t, os.O_RDONLY))
	spec.Timeout = 100 * time.Millisecond
	entered, done := make(chan struct{}), make(chan struct{})
	env.stages.on = map[launchStage]func(){stageStatus: func() { close(entered) }}
	var runErr error
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Error("RunTool owner did not return after cancellation")
		}
	})
	go func() { _, runErr = m.RunTool(ctx, spec); close(done) }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("helper never reached status wait")
	}
	select {
	case <-done:
		if runErr == nil {
			t.Fatal("hung pre-exec helper succeeded")
		}
	case <-time.After(time.Second):
		cancel()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Fatal("RunTool ignored cleanup cancellation")
		}
		t.Fatal("ToolSpec.Timeout did not bound the helper status/setup wait")
	}
}

func TestLeadSettleToolsDoesNotKillActiveRun(t *testing.T) {
	env, m := jailerToolEnv(t, jailerModeExec)
	spec := jailerToolSpec("/usr/bin/sleep", jailerToolTarget(t, os.O_RDONLY))
	ctx, cancel := context.WithCancel(t.Context())
	entered, done := make(chan struct{}), make(chan struct{})
	env.stages.on = map[launchStage]func(){stageExecProof: func() {
		rec := m.records[ToolID(spec.Attempt)]
		own := m.ownership[rec.SandboxID]
		root, _ := ownedEntryAt(own, "root")
		exe, _ := ownedEntryAt(own, own.Exec)
		pid := strconv.Itoa(rec.PID)
		m.hooks.proc = &jailerFakeProc{status: jailerConfinedStatus(rec.UID), mountinfo: "40 1 8:1 /jail/root / ro,nosuid - ext4 /dev/example rw\n", root: root.identity(), exe: exe.identity(), ns: map[string]inodeIdentity{
			pid + "/ns/mnt": {Dev: 4, Ino: 1}, pid + "/ns/pid": {Dev: 4, Ino: 2}, pid + "/ns/ipc": {Dev: 4, Ino: 3},
			"self/ns/mnt": {Dev: 4, Ino: 11}, "self/ns/pid": {Dev: 4, Ino: 12}, "self/ns/ipc": {Dev: 4, Ino: 13},
		}}
		close(entered)
	}}
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Error("active RunTool owner did not join")
		}
	})
	go func() { _, _ = m.RunTool(ctx, spec); close(done) }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("tool never reached proof")
	}
	rec, ok := m.Record(ToolID(spec.Attempt))
	if !ok {
		t.Fatal("active tool lost record")
	}
	if err := m.VerifyProcess(t.Context(), rec); err != nil {
		t.Fatalf("confined live positive control: %v", err)
	}
	report, err := m.SettleTools(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(report.Retained, []Ref{rec.Ref()}) || len(report.Settled) != 0 {
		t.Errorf("SettleTools interfered with an active RunTool: %+v", report)
	}
	if got, ok := m.Record(rec.SandboxID); !ok || got != rec {
		t.Error("active attempt was removed")
	}
	if len(env.reclaims()) != 0 {
		t.Error("active attempt was reclaimed")
	}
}

func TestLeadStateHelpersRejectLiveInRootSymlinks(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "target"), []byte("data"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(dir, "directory"), 0o700); err != nil {
		t.Fatal(err)
	}
	for name, target := range map[string]string{"link": "target", "dirlink": "directory"} {
		if err := os.Symlink(target, filepath.Join(dir, name)); err != nil {
			t.Fatal(err)
		}
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	if _, err := statRegularNoFollow(root, "target"); err != nil {
		t.Fatal(err)
	}
	if _, err := statDirectoryNoFollow(root, "directory"); err != nil {
		t.Fatal(err)
	}
	if _, err := statRegularNoFollow(root, "link"); err == nil {
		t.Error("statRegularNoFollow followed a live relative final symlink")
	}
	if _, err := statDirectoryNoFollow(root, "dirlink"); err == nil {
		t.Error("statDirectoryNoFollow followed a live relative final symlink")
	}
	if _, _, err := readBoundedRegular(root, "link", 100); err == nil {
		t.Error("readBoundedRegular followed a live relative final symlink")
	}
}

func TestLeadSettleToolsCancellationIsNotSuccess(t *testing.T) {
	env := jailerToolEnvOnly(t, jailerModeExec)
	record := jailerToolRecord(t, env, jailerToolTarget(t, os.O_RDONLY), 0, 0, "")
	jailerPlantToolRecord(t, env, record)
	m := jailerToolOpen(t, env)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	env.onReclaim = func(context.Context, Record) error { cancel(); return context.Canceled }
	report, err := m.SettleTools(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("SettleTools returned %+v, %v after cancellation during reclaim; want Canceled", report, err)
	}
}
