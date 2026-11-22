package node

import (
	"context"
	"encoding/json/v2"
	"os/exec"
	"slices"
	"syscall"
	"testing"
	"time"

	"github.com/nathabonfim59/bamboo-raft/internal/ids"
	"github.com/nathabonfim59/bamboo-raft/internal/node/qemu"
	"github.com/nathabonfim59/bamboo-raft/internal/nodetest"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

// Real-KVM dirty bitmap lifecycle tests (WP2.3a). Same gate as kvm_test.go:
//
//	BBRAFT_TEST_KVM=1 BBRAFT_TEST_IMAGE_DIR=/path/to/images/out \
//	  go test ./internal/node -run 'TestKVMBitmap|TestKVMPhase2Schema' -timeout 20m
//
// They use the non-jailed launcher; TestKVMBitmapDisabledIsReenabled also
// needs qemu-img on PATH (qemu-utils).

// bitmapKVMRead returns bbraft-dirty as the VM's running QEMU reports it.
func bitmapKVMRead(t *testing.T, m *Manager, id string) qemu.BlockDirtyBitmap {
	t.Helper()
	v, err := m.lookup(id)
	if err != nil {
		t.Fatal(err)
	}
	v.mu.Lock()
	client := qmpOf(v.process)
	v.mu.Unlock()
	if client == nil {
		t.Fatalf("vm %s has no QMP connection", id)
	}
	nodes, err := qemu.QueryNamedBlockNodes(t.Context(), client)
	if err != nil {
		t.Fatal(err)
	}
	bm, found, _ := qemu.FindBitmap(nodes, qemu.RootDiskNode, qemu.DirtyBitmap)
	if !found {
		t.Fatalf("vm %s: %s missing; nodes %+v", id, qemu.DirtyBitmap, nodes)
	}
	return bm
}

func bitmapKVMHealthy(t *testing.T, label string, bm qemu.BlockDirtyBitmap) {
	t.Helper()
	if !bm.Recording || !bm.Persistent || bm.Inconsistent || bm.Busy || bm.Granularity != qemu.DirtyBitmapGranularity {
		t.Fatalf("%s: bitmap %+v, want persistent, consistent, recording, granularity %d", label, bm, qemu.DirtyBitmapGranularity)
	}
}

func bitmapKVMFullScan(t *testing.T, m *Manager, id string) bool {
	t.Helper()
	st, err := m.store.ReadPhase2(id)
	if err != nil {
		t.Fatal(err)
	}
	return st.FullScanNext
}

func bitmapKVMCreate(t *testing.T, m *Manager) string {
	t.Helper()
	id := ids.Sandbox()
	if _, err := m.CreateVM(t.Context(), &nodev1.CreateVMRequest{
		SandboxId: id, Resources: &nodev1.Resources{Vcpus: 2, MemoryMib: 2048, DiskGib: 10},
	}); err != nil {
		t.Fatalf("CreateVM: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		m.DeleteVM(ctx, &nodev1.DeleteVMRequest{SandboxId: id})
	})
	return id
}

// bitmapKVMWrite makes the guest write and flush 8 MiB.
func bitmapKVMWrite(t *testing.T, m *Manager, id, name string) {
	t.Helper()
	client := startNodeClient(t, &harness{mg: m})
	res, err := nodetest.Exec(t.Context(), client, &nodev1.ExecRequest{
		SandboxId: id, Command: "dd if=/dev/urandom of=" + name + " bs=1M count=8 conv=fsync status=none && sync",
	})
	if err != nil || res.Exit.GetExitCode() != 0 {
		t.Fatalf("guest write: %+v, %v", res, err)
	}
}

func bitmapKVMStop(t *testing.T, m *Manager, id string) {
	t.Helper()
	vm, err := m.StopVM(t.Context(), &nodev1.StopVMRequest{SandboxId: id, GraceSeconds: 60})
	if err != nil || vm.GetState() != nodev1.VMState_VM_STATE_STOPPED {
		t.Fatalf("StopVM = %v, %v", vm, err)
	}
}

func bitmapKVMStart(t *testing.T, m *Manager, id string) {
	t.Helper()
	vm, err := m.StartVM(t.Context(), &nodev1.StartVMRequest{SandboxId: id})
	if err != nil || vm.GetState() != nodev1.VMState_VM_STATE_READY {
		t.Fatalf("StartVM = %v, %v", vm, err)
	}
}

// bitmapKVMStoredFlags reads the bitmap's flags from the stopped overlay.
func bitmapKVMStoredFlags(t *testing.T, m *Manager, id string) []string {
	t.Helper()
	out, err := exec.CommandContext(t.Context(), "qemu-img", "info", "--force-share", "--output=json", m.store.DiskPath(id)).Output()
	if err != nil {
		t.Fatalf("qemu-img info: %v", err)
	}
	var info struct {
		FormatSpecific struct {
			Data struct {
				Bitmaps []struct {
					Name  string   `json:"name"`
					Flags []string `json:"flags"`
				} `json:"bitmaps"`
			} `json:"data"`
		} `json:"format-specific"`
	}
	if err := json.Unmarshal(out, &info); err != nil {
		t.Fatalf("decode qemu-img info: %v", err)
	}
	for _, b := range info.FormatSpecific.Data.Bitmaps {
		if b.Name == qemu.DirtyBitmap {
			return b.Flags
		}
	}
	t.Fatalf("stopped overlay stores no %s: %s", qemu.DirtyBitmap, out)
	return nil
}

// TestKVMPhase2Schema checks the capability gate's parser against the real
// QEMU: every Phase 2 requirement must be present.
func TestKVMPhase2Schema(t *testing.T) {
	imageDir := requireKVM(t)
	m := newKVMManager(t, imageDir)
	bitmapKVMCreate(t, m)
	s, known := m.phase2Support()
	if !known {
		t.Fatal("the first launch did not run the capability check")
	}
	if !s.Available() {
		t.Fatalf("real QEMU %s: %s", s.QEMUVersion, s.Reason())
	}
	t.Logf("QEMU %s passes the Phase 2 capability gate", s.QEMUVersion)
}

// TestKVMBitmapLifecycle: a fresh VM gets the bitmap before its guest runs
// (full scan next: nothing tracked before it); a clean stop stores it and
// the next start keeps it, count included, without a full scan; kill -9
// leaves it inconsistent, and the next start replaces it and forces a full
// scan.
func TestKVMBitmapLifecycle(t *testing.T) {
	imageDir := requireKVM(t)
	m := newKVMManager(t, imageDir)
	id := bitmapKVMCreate(t, m)

	bitmapKVMHealthy(t, "fresh create", bitmapKVMRead(t, m, id))
	if !bitmapKVMFullScan(t, m, id) {
		t.Fatal("fresh create: full scan next not set")
	}
	// Stand in for a completed full snapshot.
	if err := m.store.ClearFullScanNext(id); err != nil {
		t.Fatal(err)
	}

	bitmapKVMWrite(t, m, id, "before-stop")
	before := bitmapKVMRead(t, m, id)
	if before.Count == 0 {
		t.Fatalf("guest wrote 8 MiB but the bitmap counts nothing: %+v", before)
	}
	bitmapKVMStop(t, m, id)
	if flags := bitmapKVMStoredFlags(t, m, id); slices.Contains(flags, "in-use") || !slices.Contains(flags, "auto") {
		t.Fatalf("clean stop stored flags %q, want [auto] without in-use", flags)
	}
	bitmapKVMStart(t, m, id)
	after := bitmapKVMRead(t, m, id)
	bitmapKVMHealthy(t, "clean restart", after)
	if after.Count < before.Count {
		t.Fatalf("clean restart lost dirty bits: count %d, was %d", after.Count, before.Count)
	}
	if bitmapKVMFullScan(t, m, id) {
		t.Fatal("clean restart forced a full scan")
	}

	// kill -9: the bitmap stays in-use on disk.
	v, err := m.lookup(id)
	if err != nil {
		t.Fatal(err)
	}
	v.mu.Lock()
	pid := v.process.PID()
	v.mu.Unlock()
	if err := syscall.Kill(pid, syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Minute)
	for {
		got, err := m.GetVM(t.Context(), &nodev1.GetVMRequest{SandboxId: id})
		if err != nil {
			t.Fatal(err)
		}
		if got.GetState() == nodev1.VMState_VM_STATE_FAILED {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("killed VM is still %s", got.GetState())
		}
		time.Sleep(100 * time.Millisecond)
	}
	bitmapKVMStop(t, m, id)
	if flags := bitmapKVMStoredFlags(t, m, id); !slices.Contains(flags, "in-use") {
		t.Fatalf("killed QEMU stored flags %q, want in-use", flags)
	}
	bitmapKVMStart(t, m, id)
	bitmapKVMHealthy(t, "start after kill -9", bitmapKVMRead(t, m, id))
	if !bitmapKVMFullScan(t, m, id) {
		t.Fatal("start after kill -9: full scan next not set")
	}
}

// TestKVMBitmapDisabledIsReenabled stores the bitmap disabled on the
// stopped overlay (qemu-img bitmap --disable): the next start must make it
// record again before the guest runs, and force a full scan. The full
// "write → snapshot → manifest" proof of condition 1 lands with SnapshotVM.
func TestKVMBitmapDisabledIsReenabled(t *testing.T) {
	imageDir := requireKVM(t)
	if _, err := exec.LookPath("qemu-img"); err != nil {
		t.Fatal("BBRAFT_TEST_KVM=1 needs qemu-img on PATH for this test")
	}
	m := newKVMManager(t, imageDir)
	id := bitmapKVMCreate(t, m)
	if err := m.store.ClearFullScanNext(id); err != nil {
		t.Fatal(err)
	}
	bitmapKVMStop(t, m, id)
	out, err := exec.CommandContext(t.Context(), "qemu-img", "bitmap", "--disable", "-f", "qcow2", m.store.DiskPath(id), qemu.DirtyBitmap).CombinedOutput()
	if err != nil {
		t.Fatalf("qemu-img bitmap --disable: %v: %s", err, out)
	}
	if flags := bitmapKVMStoredFlags(t, m, id); slices.Contains(flags, "auto") {
		t.Fatalf("bitmap still auto (enabled) after --disable: %q", flags)
	}
	bitmapKVMStart(t, m, id)
	bitmapKVMHealthy(t, "start with a disabled bitmap", bitmapKVMRead(t, m, id))
	if !bitmapKVMFullScan(t, m, id) {
		t.Fatal("a disabled bitmap did not force a full scan")
	}
	// Test-only: start from an empty bitmap so the write's own chunks are
	// what it counts (boot writes may already cover some).
	v, err := m.lookup(id)
	if err != nil {
		t.Fatal(err)
	}
	v.mu.Lock()
	client := qmpOf(v.process)
	v.mu.Unlock()
	if err := client.Execute(t.Context(), "block-dirty-bitmap-clear", map[string]string{"node": qemu.RootDiskNode, "name": qemu.DirtyBitmap}, nil); err != nil {
		t.Fatal(err)
	}
	bitmapKVMWrite(t, m, id, "after-enable")
	if after := bitmapKVMRead(t, m, id).Count; after < 8<<20 {
		t.Fatalf("re-enabled bitmap did not record an 8 MiB write: count %d", after)
	}
}
