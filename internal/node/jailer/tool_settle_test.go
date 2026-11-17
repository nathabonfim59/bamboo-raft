package jailer

import (
	"context"
	"encoding/json/v2"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"syscall"
	"testing"
	"time"
)

// A10 D1: tool records survive restarts; New validates them and
// SettleTools settles them only by proof.

// jailerToolRecord is a valid tool record on target with pid identity.
func jailerToolRecord(t *testing.T, env *jailerEnv, target *os.File, pid int, ticks uint64, boot string) Record {
	t.Helper()
	dev, ino := jailerInode(t, target)
	attempt := jailerAttempt()
	return Record{
		SchemaVersion: recordSchema, SandboxID: ToolID(attempt), Attempt: attempt, Kind: KindTool,
		UID: env.cfg.UIDs.Min, PID: pid, StartTicks: ticks, BootID: boot,
		TargetDev: dev, TargetIno: ino, TargetRoot: TargetRootData,
		TargetPath: "vms/bx_11111111/disk.qcow2", TargetAccess: AccessGroupRO,
		CreatedUnixMs: time.Now().UnixMilli(),
	}
}

// jailerPlantToolRecord persists record raw (bypassing Write's validation)
// with an owned fake cgroup, as an earlier process would have left it.
func jailerPlantToolRecord(t *testing.T, env *jailerEnv, record Record, pids ...int) {
	t.Helper()
	m := env.open(t)
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	cgroup, err := env.cgroups.Prepare(record.SandboxID, jailerToolLimits().limitsForTest())
	if err != nil {
		t.Fatal(err)
	}
	env.cgroups.mu.Lock()
	env.cgroups.groups[record.SandboxID].pids = pids
	env.cgroups.mu.Unlock()
	raw, err := json.Marshal(persistedRecord{Record: record, Ownership: resourceOwnership{Cgroup: cgroup}})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(env.cfg.StateDir, recordsDir, record.SandboxID+".json"), raw, 0o600); err != nil {
		t.Fatal(err)
	}
}

// limitsForTest converts tool limits into cgroup limits for fixtures.
func (l ToolLimits) limitsForTest() Limits {
	return Limits(l)
}

func TestNewLoadsValidToolRecords(t *testing.T) {
	env := jailerToolEnvOnly(t, jailerModeExec)
	target := jailerToolTarget(t, os.O_RDONLY)
	record := jailerToolRecord(t, env, target, 0, 0, "")
	jailerPlantToolRecord(t, env, record)
	m, err := New(env.cfg)
	if err != nil {
		t.Fatalf("New refused a valid tool record: %v", err)
	}
	defer m.Close()
	if got, ok := m.Record(record.SandboxID); !ok || got != record {
		t.Fatalf("Record = %+v, %v", got, ok)
	}
	if holder, held := m.TargetHolder(record.TargetDev, record.TargetIno); !held || holder != record.Ref() {
		t.Fatalf("TargetHolder = %v, %v", holder, held)
	}
}

func TestNewRefusesInvalidToolRecords(t *testing.T) {
	for name, edit := range map[string]func(*Record){
		"unknown root":       func(r *Record) { r.TargetRoot = "etc" },
		"unknown access":     func(r *Record) { r.TargetAccess = "rw" },
		"absolute path":      func(r *Record) { r.TargetPath = "/etc/shadow" },
		"dotdot path":        func(r *Record) { r.TargetPath = "vms/../../x" },
		"no target":          func(r *Record) { r.TargetIno = 0 },
		"id not of attempt":  func(r *Record) { r.SandboxID = ToolID(jailerAttempt()) },
		"control identity":   func(r *Record) { r.ControlDev, r.ControlIno = 1, 1 },
		"sandbox id as tool": func(r *Record) { r.SandboxID = "bx_11111111" },
	} {
		t.Run(name, func(t *testing.T) {
			env := jailerToolEnvOnly(t, jailerModeExec)
			record := jailerToolRecord(t, env, jailerToolTarget(t, os.O_RDONLY), 0, 0, "")
			edit(&record)
			jailerPlantToolRecord(t, env, record)
			if m, err := New(env.cfg); err == nil {
				m.Close()
				t.Fatal("New accepted an invalid tool record")
			}
		})
	}
}

func TestSettleToolsReleasesProvenEmptyAttempt(t *testing.T) {
	env := jailerToolEnvOnly(t, jailerModeExec)
	record := jailerToolRecord(t, env, jailerToolTarget(t, os.O_RDONLY), 0, 0, "")
	jailerPlantToolRecord(t, env, record)
	m := jailerToolOpen(t, env)
	report, err := m.SettleTools(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(report.Settled, []Ref{record.Ref()}) || len(report.Retained) != 0 {
		t.Fatalf("report %+v", report)
	}
	if _, ok := m.Record(record.SandboxID); ok {
		t.Fatal("the settled record survived")
	}
	if got := env.reclaims(); len(got) != 1 || got[0] != record {
		t.Fatalf("reclaims %+v", got)
	}
}

// TestSettleToolsRetainsUnverifiable: a pid that is alive but not provably
// the jailed tool is never killed or guessed absent; its target stays held.
func TestSettleToolsRetainsUnverifiable(t *testing.T) {
	env := jailerToolEnvOnly(t, jailerModeExec)
	live := exec.Command("/usr/bin/sleep", "60")
	if err := live.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = live.Process.Kill(); _ = live.Wait() })
	identity, err := readProcIdentity(live.Process.Pid)
	if err != nil {
		t.Fatal(err)
	}
	boot, err := hostBootID()
	if err != nil {
		t.Fatal(err)
	}
	record := jailerToolRecord(t, env, jailerToolTarget(t, os.O_RDONLY), live.Process.Pid, identity.startTicks, boot)
	jailerPlantToolRecord(t, env, record, live.Process.Pid)
	m := jailerToolOpen(t, env)
	report, err := m.SettleTools(context.Background())
	if err != nil {
		t.Fatalf("SettleTools failed instead of retaining: %v", err)
	}
	if !slices.Equal(report.Retained, []Ref{record.Ref()}) || len(report.Settled) != 0 {
		t.Fatalf("report %+v", report)
	}
	if _, ok := m.Record(record.SandboxID); !ok {
		t.Fatal("an unverifiable record was dropped")
	}
	if holder, held := m.TargetHolder(record.TargetDev, record.TargetIno); !held || holder != record.Ref() {
		t.Fatal("an unverifiable tool's target is not held")
	}
	if syscall.Kill(live.Process.Pid, 0) != nil {
		t.Fatal("the unverifiable process was killed")
	}
	if len(env.reclaims()) != 0 {
		t.Fatal("an unverifiable tool was reclaimed")
	}
}
