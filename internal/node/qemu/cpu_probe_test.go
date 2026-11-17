package qemu

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

func TestProbeAcceptsStatus(t *testing.T) {
	f := func(b bool) *bool { return &b }
	tests := []struct {
		name    string
		status  string
		running *bool
		want    bool
	}{
		{"prelaunch paused machine", "prelaunch", f(false), true},
		{"paused by stop", "paused", f(false), true},
		{"running machine", "running", f(true), false},
		{"running status with running=false", "running", f(false), false},
		{"prelaunch with running=true", "prelaunch", f(true), false},
		{"io-error", "io-error", f(false), false},
		{"internal-error", "internal-error", f(false), false},
		{"shutdown", "shutdown", f(false), false},
		{"missing status", "", f(false), false},
		{"missing running field", "prelaunch", nil, false},
		{"unknown future state", "colo", f(false), false},
	}
	for _, tt := range tests {
		t.Run(tt.name+"/"+tt.status, func(t *testing.T) {
			if got := probeAcceptsStatus(tt.status, tt.running); got != tt.want {
				t.Fatalf("probeAcceptsStatus(%q, %v) = %t, want %t", tt.status, tt.running, got, tt.want)
			}
		})
	}
}

func TestProbeCPUsRealized(t *testing.T) {
	one := 1
	zero := 0
	tid := int64(4321)
	qom := "/machine/unattached/device[0]"
	tests := []struct {
		name  string
		cpus  []probeCPUFast
		want  bool
		fails string
	}{
		{name: "one full cpu", cpus: []probeCPUFast{{CPUIndex: &zero, ThreadID: &tid, QOMPath: &qom}}, want: true},
		{name: "no cpus", cpus: nil, fails: "0 CPUs"},
		{name: "two cpus", cpus: []probeCPUFast{{CPUIndex: &zero, ThreadID: &tid, QOMPath: &qom}, {CPUIndex: &one, ThreadID: &tid, QOMPath: &qom}}, fails: "2 CPUs"},
		{name: "bare empty entry", cpus: []probeCPUFast{{}}, fails: "cpu-index"},
		{name: "no thread id", cpus: []probeCPUFast{{CPUIndex: &zero, QOMPath: &qom}}, fails: "thread-id"},
		{name: "zero thread id", cpus: []probeCPUFast{{CPUIndex: &zero, ThreadID: new(int64), QOMPath: &qom}}, fails: "thread-id"},
		{name: "no qom path", cpus: []probeCPUFast{{CPUIndex: &zero, ThreadID: &tid}}, fails: "qom-path"},
		{name: "empty qom path", cpus: []probeCPUFast{{CPUIndex: &zero, ThreadID: &tid, QOMPath: new(string)}}, fails: "qom-path"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := probeCPUsRealized(tt.cpus)
			if tt.want && err != nil {
				t.Fatalf("probeCPUsRealized(%+v) = %v, want nil", tt.cpus, err)
			}
			if !tt.want && (err == nil || !strings.Contains(err.Error(), tt.fails)) {
				t.Fatalf("probeCPUsRealized(%+v) = %v, want an error naming %q", tt.cpus, err, tt.fails)
			}
		})
	}
}

func TestProbeArgv(t *testing.T) {
	option := CPUOption{Model: DefaultCPUModel, Nested: "vmx"}
	argv := probeArgv("/usr/bin/qemu-system-x86_64", option, "/tmp/x/qmp.sock")

	if argv[0] != "/usr/bin/qemu-system-x86_64" {
		t.Fatalf("argv[0] = %q", argv[0])
	}
	for _, want := range []string{
		"-machine", "q35,accel=kvm",
		"-cpu", "Haswell-noTSX,+vmx,enforce",
		"-S",
		"-qmp", "unix:/tmp/x/qmp.sock,server=on,wait=off",
		"-display", "none",
		"-monitor", "none",
		"-serial", "none",
		"-sandbox", "on",
		"-nodefaults",
		"-no-user-config",
		"-no-reboot",
	} {
		if !slices.Contains(argv, want) {
			t.Errorf("probe argv is missing %q: %v", want, argv)
		}
	}
	for _, forbidden := range []string{
		"-kernel", "-initrd", "-append", "-drive", "-blockdev",
		"accel=tcg", "-accel", "tcg", "host", "max",
	} {
		if slices.Contains(argv, forbidden) {
			t.Errorf("probe argv must not contain %q: %v", forbidden, argv)
		}
	}
	cpuIdx := slices.Index(argv, "-cpu")
	if cpuIdx < 0 || argv[cpuIdx+1] != option.String() {
		t.Fatalf("probe argv -cpu = %v, want exactly the resolved %q", argv, option.String())
	}
}

func probeTestKVMDevice(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "kvm")
	if err := os.WriteFile(path, []byte("device"), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func probeTestScript(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "fake-qemu")
	if err := os.WriteFile(path, []byte(body), 0o700); err != nil {
		t.Fatal(err)
	}
	return path
}

// TestProbeCPUFakeChildMissingFeature proves the real child is launched with
// the resolved CPU argv, its bound stderr tail survives exactly (CPUID bit
// 17), and it is reaped with its owned process group empty.
func TestProbeCPUFakeChildMissingFeature(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("the CPU probe refuses to run as root")
	}
	dir := t.TempDir()
	pidFile := filepath.Join(dir, "pid")
	argvFile := filepath.Join(dir, "argv")
	script := probeTestScript(t, "#!/bin/sh\n"+
		"echo $$ > "+pidFile+"\n"+
		"printf '%s\\n' \"$@\" > "+argvFile+"\n"+
		"echo \"qemu-system-x86_64: warning: host doesn't support requested feature: CPUID.01H:ECX.pcid [bit 17]\" >&2\n"+
		"echo \"qemu-system-x86_64: Host doesn't support requested features\" >&2\n"+
		"exit 1\n")
	option := CPUOption{Model: DefaultCPUModel}
	_, err := ProbeCPU(t.Context(), option, CPUProbeOptions{
		Binary: script, KVMDevice: probeTestKVMDevice(t), WorkDir: t.TempDir(),
		Timeout: 5 * time.Second, Grace: 200 * time.Millisecond,
	})
	if err == nil {
		t.Fatal("probe succeeded with a child that cannot realize the CPU")
	}
	for _, want := range []string{"CPUID.01H:ECX.pcid", "[bit 17]", "Host doesn't support requested features"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("probe error %q is missing %q", err, want)
		}
	}
	rawPID, err := os.ReadFile(pidFile)
	if err != nil {
		t.Fatal(err)
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(rawPID)))
	if err != nil || pid <= 0 {
		t.Fatalf("child pid %q: %v", rawPID, err)
	}
	rawArgv, err := os.ReadFile(argvFile)
	if err != nil {
		t.Fatal(err)
	}
	argv := strings.Split(strings.TrimSpace(string(rawArgv)), "\n")
	cpuIdx := slices.Index(argv, "-cpu")
	if cpuIdx < 0 || cpuIdx+1 >= len(argv) {
		t.Fatalf("child argv has no -cpu: %q", argv)
	}
	cpu := argv[cpuIdx+1]
	if cpu != option.String() {
		t.Fatalf("child -cpu = %q, want exactly the resolved %q", cpu, option.String())
	}
	if !errors.Is(syscall.Kill(pid, 0), syscall.ESRCH) || !errors.Is(syscall.Kill(-pid, 0), syscall.ESRCH) {
		t.Fatalf("child pid %d or group %d survived reaping", pid, -pid)
	}
}

// TestProbeCPUCancelCleansChild proves cancelled probes kill, reap and prove
// their owned process group empty. Cleanup is registered before the goroutine
// starts, with bounded join and an owned-group fallback.
func TestProbeCPUCancelCleansChild(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("the CPU probe refuses to run as root")
	}
	dir := t.TempDir()
	pidFile := filepath.Join(dir, "pid")
	workDir := t.TempDir()
	kvmDevice := probeTestKVMDevice(t)
	script := probeTestScript(t, fmt.Sprintf("#!/bin/sh\necho $$ > %s\nexec sleep 60\n", pidFile))
	ctx, cancel := context.WithCancel(t.Context())
	var pid atomic.Int64
	result := make(chan error, 1)
	finished := make(chan struct{})
	t.Cleanup(func() {
		cancel()
		select {
		case <-finished:
		case <-time.After(10 * time.Second):
			if p := pid.Load(); p > 0 {
				_ = syscall.Kill(-int(p), syscall.SIGKILL)
			}
			t.Errorf("probe goroutine did not finish after cancel")
		}
		if p := pid.Load(); p > 0 {
			_ = syscall.Kill(-int(p), syscall.SIGKILL)
		}
	})
	go func() {
		_, err := ProbeCPU(ctx, CPUOption{Model: DefaultCPUModel}, CPUProbeOptions{
			Binary: script, KVMDevice: kvmDevice, WorkDir: workDir,
			Timeout: 10 * time.Second, Grace: 200 * time.Millisecond,
		})
		result <- err
		close(finished)
	}()
	deadline := time.Now().Add(5 * time.Second)
	for {
		raw, err := os.ReadFile(pidFile)
		if err == nil {
			n, err := strconv.Atoi(strings.TrimSpace(string(raw)))
			if err != nil || n <= 0 {
				t.Fatalf("child pid %q: %v", raw, err)
			}
			pid.Store(int64(n))
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("fake child never started")
		}
		time.Sleep(10 * time.Millisecond)
	}
	ownPID := int(pid.Load())
	if pgid, err := syscall.Getpgid(ownPID); err != nil || pgid != ownPID {
		t.Fatalf("child pgid = %d, %v; want own pid %d", pgid, err, ownPID)
	}
	if err := syscall.Kill(-ownPID, 0); err != nil {
		t.Fatalf("owned group %d not alive before cancel: %v", -ownPID, err)
	}
	cancel()
	select {
	case err := <-result:
		if err == nil {
			t.Fatal("canceled probe reported success")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("canceled probe did not return")
	}
	select {
	case <-finished:
	case <-time.After(10 * time.Second):
		t.Fatal("probe goroutine did not join")
	}
	deadline = time.Now().Add(5 * time.Second)
	for {
		leader := syscall.Kill(ownPID, 0)
		group := syscall.Kill(-ownPID, 0)
		if errors.Is(leader, syscall.ESRCH) && errors.Is(group, syscall.ESRCH) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("probe leader/group survived cleanup: leader=%v group=%v", leader, group)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
