package jailer

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

// TestPrivilegedToolAccess is the kernel-level proof of a tool's access
// (lead-owned gate, root throwaway VM only, BBRAFT_TEST_JAILER=1; see
// privileged_test.go). A live tool (a staged trusted sleeper whose argv the
// privileged-only toolArgs seam replaces) must run as its pool uid with all
// five capability sets zero and NoNewPrivs 1, its bounding set having been
// dropped before the uid change (the helper refuses to exec otherwise), and
// its group-ro target must refuse an O_RDWR open and a chmod by that uid
// while a read succeeds.
//
// The exec-proof hook runs under the Manager lock, so it only collects
// bounded, error-returning observations; every assertion runs after RunTool
// has returned and settled the attempt.
func TestPrivilegedToolAccess(t *testing.T) {
	env := jailerRequirePrivileged(t)
	base := t.TempDir()
	cfg := Config{
		StateDir: filepath.Join(base, "state"), CgroupParent: env.cgroup,
		UIDs: UIDRange{Min: 61010, Max: 61011}, Reclaim: func(context.Context, Record) error { return nil },
	}
	m, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	m.hooks.toolArgs = func([]string) []string { return []string{"30"} }
	// The target's parents are traversable (0711) so the as-uid probe can
	// reach it; the target itself is the at-rest root 0600 file.
	targetDir := t.TempDir()
	jailerPrivTraversable(t, targetDir)
	target := filepath.Join(targetDir, "disk.qcow2")
	if err := os.WriteFile(target, []byte("qcow2 stand-in"), 0o600); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(target)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	spec := ToolSpec{
		Attempt: jailerAttempt(), Binary: "/usr/bin/sleep", Op: ToolInfo, Driver: "qcow2",
		Target: f, TargetRoot: TargetRootData, TargetPath: "disk.qcow2", TargetAccess: AccessGroupRO,
		Limits:  ToolLimits{CPUQuotaPercent: 100, MemoryBytes: 256 << 20, Pids: 64, IO: IOLimits{Device: env.ioDevice, ReadBPS: 1 << 20}},
		Timeout: 20 * time.Second,
	}

	// observed is filled by the hook (under the Manager lock) and read only
	// after RunTool returned.
	var observed struct {
		ran      bool
		record   Record
		status   map[string]string
		statErr  error
		probes   []string
		probeErr error
		mode     os.FileMode
		modeErr  error
		// memoryMax is the tool cgroup's memory.max; limits is the live
		// tool's /proc/pid/limits.
		memoryMax string
		limits    string
		limitsErr error
	}
	checked := make(chan struct{})
	m.hooks.stage = func(stage launchStage) error {
		if stage != stageExecProof || observed.ran {
			return nil
		}
		observed.ran = true
		defer close(checked)
		record, ok := m.records[ToolID(spec.Attempt)]
		if !ok {
			observed.statErr = errors.New("no tool record at the exec proof")
			return nil
		}
		observed.record = record
		observed.status, observed.statErr = toolPrivStatus(record.PID)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		observed.probes, observed.probeErr = toolPrivProbe(ctx, record.UID, "rw:"+target, "chmod:"+target, "r:"+target)
		info, err := os.Stat(target)
		if err == nil {
			observed.mode = info.Mode().Perm()
		}
		observed.modeErr = err
		raw, err := os.ReadFile(filepath.Join(env.cgroup, cgroupName(record.SandboxID), "memory.max"))
		if err == nil {
			observed.memoryMax = strings.TrimSpace(string(raw))
		}
		limits, lerr := os.ReadFile(fmt.Sprintf("/proc/%d/limits", record.PID))
		observed.limits, observed.limitsErr = string(limits), errors.Join(err, lerr)
		return nil
	}

	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	returned := make(chan struct{})
	var watcher sync.WaitGroup
	watcher.Go(func() {
		// End the live tool once observed; on an early setup failure the
		// hook never runs and the watcher ends with the run.
		select {
		case <-checked:
			cancel()
		case <-returned:
		}
	})
	_, runErr := m.RunTool(ctx, spec)
	close(returned)
	joined := make(chan struct{})
	go func() { watcher.Wait(); close(joined) }()
	select {
	case <-joined:
	case <-time.After(10 * time.Second):
		t.Error("the run watcher never joined")
	}

	if _, ok := m.Record(ToolID(spec.Attempt)); ok {
		t.Fatal("the tool attempt was not settled")
	}
	if !observed.ran {
		t.Fatalf("the tool never reached its exec proof: %v", runErr)
	}
	if runErr == nil {
		t.Fatal("a canceled live tool returned success")
	}
	if observed.statErr != nil {
		t.Fatalf("read the live tool's status: %v", observed.statErr)
	}
	want := strconv.Itoa(int(observed.record.UID))
	for _, key := range []string{"Uid", "Gid"} {
		for v := range strings.FieldsSeq(observed.status[key]) {
			if v != want {
				t.Errorf("%s = %q, want all %s", key, observed.status[key], want)
			}
		}
	}
	for _, key := range []string{"CapInh", "CapPrm", "CapEff", "CapBnd", "CapAmb"} {
		if v, err := strconv.ParseUint(observed.status[key], 16, 64); err != nil || v != 0 {
			t.Errorf("%s = %q, want 0", key, observed.status[key])
		}
	}
	if observed.status["NoNewPrivs"] != "1" {
		t.Errorf("NoNewPrivs = %q, want 1", observed.status["NoNewPrivs"])
	}
	if observed.probeErr != nil {
		t.Fatalf("probe as the tool uid: %v", observed.probeErr)
	}
	if got := observed.probes; got[0] != "EACCES" || got[1] != "EPERM" || got[2] != "ok" {
		t.Errorf("group-ro target as the tool uid: rw=%s chmod=%s r=%s; want EACCES, EPERM, ok", got[0], got[1], got[2])
	}
	if observed.modeErr != nil || observed.mode != 0o640 {
		t.Errorf("group-ro target mode %v, %v; want 0640 root:<uid>", observed.mode, observed.modeErr)
	}
	// The memory bound is the birth cgroup's memory.max (RLIMIT_AS is not
	// used: a Go helper's own address space exceeds any tool bound).
	if observed.limitsErr != nil {
		t.Fatalf("read the tool's memory.max or limits: %v", observed.limitsErr)
	}
	if want := strconv.FormatUint(spec.Limits.MemoryBytes, 10); observed.memoryMax != want {
		t.Errorf("tool cgroup memory.max = %q, want %s", observed.memoryMax, want)
	}
	if got := toolPrivLimit(observed.limits, "Max core file size"); got != "0 0" {
		t.Errorf("Max core file size = %q, want 0 0", got)
	}
	if got := toolPrivLimit(observed.limits, "Max address space"); got != "unlimited unlimited" {
		t.Errorf("Max address space = %q, want unlimited unlimited (no RLIMIT_AS)", got)
	}
	// Restoring root:root 0600 is the node reclaimer's job (tested there);
	// this Manager's policy is a no-op.
}

// toolPrivStatus reads /proc/pid/status without failing the test, for use
// under the Manager lock.
func toolPrivStatus(pid int) (map[string]string, error) {
	raw, err := os.ReadFile(fmt.Sprintf("/proc/%d/status", pid))
	if err != nil {
		return nil, err
	}
	out := make(map[string]string)
	for line := range strings.SplitSeq(string(raw), "\n") {
		if key, value, ok := strings.Cut(line, ":"); ok {
			out[key] = strings.TrimSpace(value)
		}
	}
	return out, nil
}

// toolPrivProbe is jailerPrivProbe bounded by ctx and returning errors
// instead of failing the test.
func toolPrivProbe(ctx context.Context, uid uint32, probes ...string) ([]string, error) {
	exe, err := os.Executable()
	if err != nil {
		return nil, err
	}
	cmd := exec.CommandContext(ctx, exe, probes...)
	cmd.Env = []string{jailerProbeEnv + "=1"}
	cmd.Dir = "/"
	cmd.SysProcAttr = &syscall.SysProcAttr{Credential: &syscall.Credential{Uid: uid, Gid: uid, Groups: []uint32{}}}
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("probe as host uid %d: %w", uid, err)
	}
	results := strings.Split(strings.TrimSpace(string(out)), "\n")
	if len(results) != len(probes) {
		return nil, fmt.Errorf("probe returned %q for %d probes", results, len(probes))
	}
	return results, nil
}

// toolPrivLimit returns the soft and hard values of one /proc/pid/limits row.
func toolPrivLimit(limits, name string) string {
	for line := range strings.SplitSeq(limits, "\n") {
		if rest, ok := strings.CutPrefix(line, name); ok {
			fields := strings.Fields(rest)
			if len(fields) >= 2 {
				return fields[0] + " " + fields[1]
			}
		}
	}
	return ""
}
