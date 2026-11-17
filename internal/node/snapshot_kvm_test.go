package node

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/nathabonfim59/bamboo-raft/internal/node/storage"
	"github.com/nathabonfim59/bamboo-raft/internal/nodetest"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/guestv1"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

// TestKVMSnapshotIntegrityAndGrowth proves point-in-time contents, independent
// backing, and growth of both the private qcow2 and the guest ext4 filesystem.
func TestKVMSnapshotIntegrityAndGrowth(t *testing.T) {
	imageDir := requireKVM(t)
	m := newKVMManager(t, imageDir)
	client := startNodeClient(t, &harness{mg: m})
	const source = "bx_22222222"
	const child = "bx_33333333"
	if _, err := m.CreateVM(t.Context(), &nodev1.CreateVMRequest{SandboxId: source, Resources: &nodev1.Resources{Vcpus: 2, MemoryMib: 2048, DiskGib: 10}}); err != nil {
		t.Fatal(err)
	}
	defer m.DeleteVM(context.Background(), &nodev1.DeleteVMRequest{SandboxId: source})
	execKVMSnapshot(t, client, source, "mkdir -p snap && echo before-snapshot > snap/state && sync")
	const snapshotID = "kvm-integrity"
	if err := saveSnapshot(t.Context(), m, source, snapshotID); err != nil {
		t.Fatal(err)
	}
	defer m.DeleteSnapshot(context.Background(), &nodev1.DeleteSnapshotRequest{SnapshotId: snapshotID})
	execKVMSnapshot(t, client, source, "echo after-snapshot > snap/state && sync")
	if _, err := m.CreateVM(t.Context(), &nodev1.CreateVMRequest{SandboxId: child, FromSnapshot: snapshotID, Resources: &nodev1.Resources{Vcpus: 2, MemoryMib: 2048, DiskGib: 12}}); err != nil {
		t.Fatalf("larger snapshot child: %v", err)
	}
	defer m.DeleteVM(context.Background(), &nodev1.DeleteVMRequest{SandboxId: child})
	res, err := nodetest.Exec(t.Context(), client, &nodev1.ExecRequest{SandboxId: child, Command: "cat snap/state"})
	if err != nil || res.Stdout != "before-snapshot\n" {
		t.Fatalf("child point-in-time data = %q, %v", res.Stdout, err)
	}
	res, err = nodetest.Exec(t.Context(), client, &nodev1.ExecRequest{SandboxId: source, Command: "cat snap/state"})
	if err != nil || res.Stdout != "after-snapshot\n" {
		t.Fatalf("source data = %q, %v", res.Stdout, err)
	}
	res, err = nodetest.Exec(t.Context(), client, &nodev1.ExecRequest{SandboxId: child, Command: `df -B1 / | awk 'NR==2 {print $2}'`})
	if err != nil {
		t.Fatal(err)
	}
	size, err := strconv.ParseUint(strings.TrimSpace(res.Stdout), 10, 64)
	if err != nil || size < uint64(12)<<30*95/100 {
		t.Fatalf("grown child filesystem bytes=%d stdout=%q err=%v", size, res.Stdout, err)
	}

	snap, err := m.store.ReadSnapshot(t.Context(), snapshotID, m.images)
	if err != nil {
		t.Fatal(err)
	}
	imager := storage.QemuImg{}
	pin := func(p string) *os.File {
		f, err := os.Open(p)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { f.Close() })
		return f
	}
	base := filepath.Join(imageDir, snap.Meta.Image, "root.qcow2")
	ref := storage.ImageRef{
		File: pin(snap.DiskPath), Root: storage.ImageRootData, Path: "snapshot/disk.qcow2", Format: "qcow2",
		ExpectedBacking: base, Base: pin(base),
	}
	if err := imager.Check(t.Context(), ref); err != nil {
		t.Fatalf("saved qcow2 check: %v", err)
	}
	info, err := imager.Info(t.Context(), ref)
	if err != nil || info.BackingFile != filepath.Join(imageDir, snap.Meta.Image, "root.qcow2") || info.VirtualSize != 10<<30 {
		t.Fatalf("saved qcow2 info = %+v, %v", info, err)
	}

	// Deleting both source and saved snapshot cannot affect the private child.
	if _, err := m.DeleteVM(t.Context(), &nodev1.DeleteVMRequest{SandboxId: source}); err != nil {
		t.Fatal(err)
	}
	if _, err := m.DeleteSnapshot(t.Context(), &nodev1.DeleteSnapshotRequest{SnapshotId: snapshotID}); err != nil {
		t.Fatal(err)
	}
	execKVMSnapshot(t, client, child, "cat snap/state")

	// Stop QEMU before running qemu-img on the child. Its backing must be the
	// verified immutable image, never the now-deleted saved snapshot.
	v, err := m.lookup(child)
	if err != nil {
		t.Fatal(err)
	}
	v.mu.Lock()
	proc := v.process
	v.mu.Unlock()
	if proc == nil {
		t.Fatal("child process already gone")
	}
	if err := stopProcess(proc); err != nil {
		t.Fatalf("stop child for disk check: %v", err)
	}
	childRef := ref
	childRef.File = pin(m.store.DiskPath(child))
	if err := imager.Check(t.Context(), childRef); err != nil {
		t.Fatalf("child qcow2 check: %v", err)
	}
	info, err = imager.Info(t.Context(), childRef)
	if err != nil || info.BackingFile != filepath.Join(imageDir, snap.Meta.Image, "root.qcow2") || info.VirtualSize != 12<<30 {
		t.Fatalf("independent grown child info = %+v, %v", info, err)
	}
}

func execKVMSnapshot(t *testing.T, client nodev1.NodeClient, id, command string) {
	t.Helper()
	res, err := nodetest.Exec(t.Context(), client, &nodev1.ExecRequest{SandboxId: id, Command: command})
	if err != nil || res.Exit.GetExitCode() != 0 {
		t.Fatalf("Exec(%q): %+v, %v", command, res, err)
	}
}

// TestKVMNodeManagerRestartDuringFreeze simulates a node crash in the middle
// of a save: the guest is frozen with a lease token the next manager never saw
// and QEMU is paused. Restart adoption must resume QEMU, discover and thaw the
// surviving lease before READY, and leave the VM writable for Exec.
func TestKVMNodeManagerRestartDuringFreeze(t *testing.T) {
	imageDir := requireKVM(t)
	dataDir := t.TempDir()
	cpuModel, allowHostCPU := kvmCPUOverride()
	cfg := Config{
		NodeID:        "kvm-test",
		DataDir:       dataDir,
		ImageDir:      imageDir,
		CPUModel:      cpuModel,
		AllowHostCPU:  allowHostCPU,
		Capacity:      &nodev1.Resources{Vcpus: 16, MemoryMib: 32 << 10, DiskGib: 200},
		BootTimeout:   2 * time.Minute,
		SnapshotLease: 2 * time.Minute, SnapshotCleanupTimeout: 10 * time.Second,
		// KVM test factory: the dev/test user-mode network until the real-TAP
		// A8 gate.
		AllowUserNetworking: true,
	}
	m1, err := New(t.Context(), cfg)
	if err != nil {
		t.Fatalf("first manager: %v", err)
	}
	const id = "bx_22222222"
	// current tracks whichever manager owns QEMU now. Cleanup always either
	// deletes through it or, after the crash analogue, adopts from disk and
	// deletes; a failed Freeze/Pause/restart can never leak QEMU referencing
	// t.TempDir.
	var current *Manager = m1
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		if current == nil {
			adopted, err := New(ctx, cfg)
			if err != nil {
				t.Errorf("cleanup could not adopt crashed VM: %v", err)
				return
			}
			current = adopted
		}
		if _, err := current.DeleteVM(ctx, &nodev1.DeleteVMRequest{SandboxId: id}); err != nil {
			t.Errorf("cleanup DeleteVM: %v", err)
		}
		if err := current.Shutdown(ctx); err != nil {
			t.Errorf("cleanup manager shutdown: %v", err)
		}
	})
	if _, err := m1.CreateVM(t.Context(), &nodev1.CreateVMRequest{
		SandboxId: id,
		Resources: &nodev1.Resources{Vcpus: 2, MemoryMib: 2048, DiskGib: 10},
	}); err != nil {
		t.Fatalf("CreateVM: %v", err)
	}
	v, err := m1.lookup(id)
	if err != nil {
		t.Fatal(err)
	}
	v.mu.Lock()
	guestClient, controller := v.client, v.process.(VMController)
	v.mu.Unlock()
	if _, err := guestClient.Freeze(t.Context(), &guestv1.FreezeRequest{LeaseMs: 120_000}); err != nil {
		t.Fatalf("surviving Freeze: %v", err)
	}
	if err := controller.Pause(t.Context()); err != nil {
		t.Fatalf("pause for crash fixture: %v", err)
	}
	// This is the crash analogue: close control/guest connections and release
	// the data lock without resuming or thawing anything.
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), time.Minute)
	if err := m1.Shutdown(shutdownCtx); err != nil {
		shutdownCancel()
		t.Fatalf("crash fixture shutdown: %v", err)
	}
	shutdownCancel()
	current = nil

	m2, err := New(t.Context(), cfg)
	if err != nil {
		t.Fatalf("restart manager: %v", err)
	}
	current = m2
	vm, err := m2.GetVM(t.Context(), &nodev1.GetVMRequest{SandboxId: id})
	if err != nil || vm.GetState() != nodev1.VMState_VM_STATE_READY {
		t.Fatalf("recovered frozen VM = %+v, %v", vm, err)
	}
	client := startNodeClient(t, &harness{mg: m2})
	res, err := nodetest.Exec(t.Context(), client, &nodev1.ExecRequest{
		SandboxId: id, Command: "echo restart-ok > /tmp/restart-marker && cat /tmp/restart-marker",
	})
	if err != nil || strings.TrimSpace(res.Stdout) != "restart-ok" {
		t.Fatalf("write Exec after restart = %+v, %v", res, err)
	}
}

const (
	kvmKillerReadyPath   = "/dev/shm/bbraft-agent-kill-ready"
	kvmKillerTriggerPath = "/dev/shm/bbraft-agent-kill-trigger"
	kvmKillerDonePath    = "/dev/shm/bbraft-agent-kill-done"
)

// kvmKillerScript is the prearmed privileged helper. It writes readiness,
// waits for an explicit trigger (never a timer), then kills exactly the PID
// passed as $4 and writes done only when the kill succeeded. The loop is
// bounded so a failed test cannot leave a root helper alive forever.
func kvmKillerScript() string {
	return `#!/bin/sh
ready=$1
trigger=$2
donefile=$3
pid=$4
echo ready > "$ready"
i=0
while [ ! -f "$trigger" ]; do
	i=$((i+1))
	if [ "$i" -gt 600 ]; then exit 3; fi
	sleep 0.1
done
if kill -KILL "$pid" 2>/dev/null; then
	echo "done:$pid" > "$donefile"
fi
`
}

// kvmSinglePID accepts exactly one positive decimal field.
func kvmSinglePID(s string) (string, bool) {
	fields := strings.Fields(s)
	if len(fields) != 1 {
		return "", false
	}
	n, err := strconv.Atoi(fields[0])
	if err != nil || n <= 0 || strconv.Itoa(n) != fields[0] {
		return "", false
	}
	return fields[0], true
}

func TestKVMSinglePID(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want string
		ok   bool
	}{
		{"123", "123", true},
		{"", "", false},
		{"1 2", "", false},
		{"1\n2", "", false},
		{"0", "", false},
		{"-1", "", false},
		{"+1", "", false},
		{"007", "", false},
		{"abc", "", false},
	} {
		got, ok := kvmSinglePID(tc.in)
		if got != tc.want || ok != tc.ok {
			t.Fatalf("kvmSinglePID(%q) = %q,%t; want %q,%t", tc.in, got, ok, tc.want, tc.ok)
		}
	}
}

// TestKVMKillerScriptTriggerAndExactPID is a host-only regression for the
// fixture helper: no kill before the trigger, then only the exact owned child
// dies and readiness/done markers are truthful.
func TestKVMKillerScriptTriggerAndExactPID(t *testing.T) {
	dir := t.TempDir()
	ready := filepath.Join(dir, "ready")
	trigger := filepath.Join(dir, "trigger")
	done := filepath.Join(dir, "done")
	script := filepath.Join(dir, "killer.sh")
	if err := os.WriteFile(script, []byte(kvmKillerScript()), 0o600); err != nil {
		t.Fatal(err)
	}
	victim := exec.Command("sleep", "60")
	if err := victim.Start(); err != nil {
		t.Fatal(err)
	}
	victimDone := make(chan error, 1)
	victimReaped := false
	go func() { victimDone <- victim.Wait() }()
	t.Cleanup(func() {
		_ = victim.Process.Kill()
		if !victimReaped {
			<-victimDone
		}
	})
	bystander := exec.Command("sleep", "60")
	if err := bystander.Start(); err != nil {
		t.Fatal(err)
	}
	bystanderDone := make(chan error, 1)
	bystanderReaped := false
	go func() { bystanderDone <- bystander.Wait() }()
	t.Cleanup(func() {
		_ = bystander.Process.Kill()
		if !bystanderReaped {
			<-bystanderDone
		}
	})
	pid := strconv.Itoa(victim.Process.Pid)
	killer := exec.Command("/bin/sh", script, ready, trigger, done, pid)
	if err := killer.Start(); err != nil {
		t.Fatal(err)
	}
	killerDone := make(chan error, 1)
	killerReaped := false
	go func() { killerDone <- killer.Wait() }()
	t.Cleanup(func() {
		_ = killer.Process.Kill()
		if !killerReaped {
			<-killerDone
		}
	})
	waitFile := func(path string) {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		for {
			if _, err := os.Stat(path); err == nil {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("%s never appeared", path)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	waitFile(ready)
	time.Sleep(300 * time.Millisecond)
	select {
	case err := <-victimDone:
		victimReaped = true
		t.Fatalf("victim died before trigger: %v", err)
	default:
	}
	if _, err := os.Stat(done); !os.IsNotExist(err) {
		t.Fatalf("done marker written before trigger: %v", err)
	}
	if err := os.WriteFile(trigger, []byte("go"), 0o600); err != nil {
		t.Fatal(err)
	}
	waitFile(done)
	raw, err := os.ReadFile(done)
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.TrimSpace(string(raw)); got != "done:"+pid {
		t.Fatalf("done marker = %q, want done:%s", got, pid)
	}
	select {
	case err := <-victimDone:
		victimReaped = true
		exitErr, ok := errors.AsType[*exec.ExitError](err)
		if !ok {
			t.Fatalf("victim wait = %v, want an exit error", err)
		}
		status, ok := exitErr.Sys().(syscall.WaitStatus)
		if !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
			t.Fatalf("victim status = %v, want SIGKILL", exitErr)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("victim did not die after trigger")
	}
	select {
	case err := <-killerDone:
		killerReaped = true
		if err != nil {
			t.Fatalf("killer exit = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("killer did not exit")
	}
	select {
	case err := <-bystanderDone:
		bystanderReaped = true
		t.Fatalf("bystander exited before cleanup: %v", err)
	default:
	}
}

// TestKVMGuestAgentCrashDuringFreeze kills the real guest agent after Freeze
// but before QMP stop. Systemd restarts it; startup FITHAW must recover the
// root, the save must reject the replacement lease, and the VM must run again.
func TestKVMGuestAgentCrashDuringFreeze(t *testing.T) {
	imageDir := requireKVM(t)
	cpuModel, allowHostCPU := kvmCPUOverride()
	const id = "bx_22222222"
	var (
		client      nodev1.NodeClient
		oldAgentPID string
		newAgentPID string
	)
	m, err := New(t.Context(), Config{
		NodeID: "kvm-test", DataDir: t.TempDir(), ImageDir: imageDir,
		CPUModel: cpuModel, AllowHostCPU: allowHostCPU,
		Capacity:    &nodev1.Resources{Vcpus: 4, MemoryMib: 4096, DiskGib: 20},
		BootTimeout: 2 * time.Minute, SnapshotLease: 30 * time.Second,
		SnapshotCleanupTimeout: 5 * time.Second,
		// KVM test factory: the dev/test user-mode network until the real-TAP
		// A8 gate.
		AllowUserNetworking: true,
		afterSnapshotFreeze: func(ctx context.Context) error {
			// The privileged killer was armed before Freeze. Prove the
			// original PID actually died and a replacement is serving while
			// the freeze lease is still active; a mere sleep is not crash
			// proof. /dev/shm is tmpfs, so these probes do not need writes to
			// the frozen root.
			// Release the prearmed killer only now, inside the active freeze.
			// The resulting SIGKILL can interrupt this very RPC; the new PID
			// and exact done marker below are the proof, so tolerate its
			// transport error.
			_, _ = nodetest.Exec(ctx, client, &nodev1.ExecRequest{
				SandboxId: id, Command: "echo go > " + kvmKillerTriggerPath, TimeoutSeconds: 2,
			})
			pollCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			defer cancel()
			for {
				res, err := nodetest.Exec(pollCtx, client, &nodev1.ExecRequest{
					SandboxId: id, Command: "pgrep -x bbraft-guest", TimeoutSeconds: 2,
				})
				if err == nil {
					pid, ok := kvmSinglePID(res.Stdout)
					if ok && pid != oldAgentPID {
						newAgentPID = pid
						break
					}
				}
				select {
				case <-pollCtx.Done():
					return fmt.Errorf("fixture: agent PID still %q after prearmed kill", oldAgentPID)
				case <-time.After(100 * time.Millisecond):
				}
			}
			markerCtx, markerCancel := context.WithTimeout(ctx, 5*time.Second)
			defer markerCancel()
			marker, err := nodetest.Exec(markerCtx, client, &nodev1.ExecRequest{
				SandboxId: id, Command: "cat " + kvmKillerDonePath, TimeoutSeconds: 2,
			})
			if err != nil || strings.TrimSpace(marker.Stdout) != "done:"+oldAgentPID {
				return fmt.Errorf("fixture: kill marker mismatch (err=%v out=%q)", err, marker.Stdout)
			}
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		_, _ = m.DeleteVM(ctx, &nodev1.DeleteVMRequest{SandboxId: id})
		m.Shutdown(ctx)
	})
	client = startNodeClient(t, &harness{mg: m})
	if _, err := m.CreateVM(t.Context(), &nodev1.CreateVMRequest{SandboxId: id, Resources: &nodev1.Resources{Vcpus: 2, MemoryMib: 2048, DiskGib: 10}}); err != nil {
		t.Fatal(err)
	}
	res, err := nodetest.Exec(t.Context(), client, &nodev1.ExecRequest{
		SandboxId: id, Command: "pgrep -x bbraft-guest", TimeoutSeconds: 5,
	})
	if err != nil {
		t.Fatalf("read agent PID: %v", err)
	}
	oldAgentPID, ok := kvmSinglePID(res.Stdout)
	if !ok {
		t.Fatalf("agent PID output %q is not exactly one positive integer", res.Stdout)
	}
	// Arm one privileged killer in its own immediate transient unit before
	// Freeze. A separate unit is not killed by the bbraft-guest cgroup's
	// stop, and it is already running before the root freezes. Its stdio is
	// null; it waits for an explicit trigger written by afterSnapshotFreeze,
	// then kills exactly the captured PID. /dev/shm may be noexec, so invoke
	// /bin/sh with the script path explicitly.
	arm := fmt.Sprintf(`cat > /dev/shm/bbraft-agent-killer.sh <<'EOF'
%sEOF
sudo systemd-run --quiet --collect --unit=bbraft-agent-kill-helper --property=StandardOutput=null --property=StandardError=null /bin/sh /dev/shm/bbraft-agent-killer.sh %s %s %s %s >/dev/null 2>&1
`, kvmKillerScript(), kvmKillerReadyPath, kvmKillerTriggerPath, kvmKillerDonePath, oldAgentPID)
	if _, err := nodetest.Exec(t.Context(), client, &nodev1.ExecRequest{
		SandboxId: id, Command: arm, TimeoutSeconds: 5,
	}); err != nil {
		t.Fatalf("arm killer: %v", err)
	}
	readyDeadline := time.Now().Add(10 * time.Second)
	for {
		ready, err := nodetest.Exec(t.Context(), client, &nodev1.ExecRequest{
			SandboxId: id, Command: "cat " + kvmKillerReadyPath, TimeoutSeconds: 2,
		})
		if err == nil && strings.TrimSpace(ready.Stdout) == "ready" {
			break
		}
		if time.Now().After(readyDeadline) {
			t.Fatalf("privileged killer never armed: err=%v out=%q", err, ready.Stdout)
		}
		time.Sleep(50 * time.Millisecond)
	}

	err = saveSnapshot(t.Context(), m, id, "agent-crash")
	t.Logf("guest agent crash evidence: old_pid=%s new_pid=%s save_error=%v", oldAgentPID, newAgentPID, err)
	if err == nil {
		t.Fatal("save succeeded after guest-agent crash replaced its lease")
	}
	if newAgentPID == "" || newAgentPID == oldAgentPID {
		t.Fatalf("fixture did not observe replacement agent: old=%q new=%q err=%v", oldAgentPID, newAgentPID, err)
	}
	if !strings.Contains(err.Error(), "freeze lease changed") {
		t.Fatalf("save error = %v, want lease-change rejection", err)
	}
	if infoHasSnapshot(t, m, "agent-crash") {
		t.Fatal("interrupted save published")
	}
	execKVMSnapshot(t, client, id, "echo recovered > recovery-marker && cat recovery-marker")
}
