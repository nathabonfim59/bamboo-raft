package nodefake_test

import (
	"math"
	"testing"

	"github.com/nathabonfim59/bamboo-raft/internal/nodefake"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

// nodefakeDiskFreeMib reads the fake's reported free disk.
func nodefakeDiskFreeMib(t *testing.T, n *nodefake.Node) uint64 {
	t.Helper()
	info, err := n.Info(t.Context(), &nodev1.InfoRequest{})
	if err != nil {
		t.Fatalf("Info: %v", err)
	}
	return info.GetDiskFreeMib()
}

// TestInfoDiskFreeTracksLiveVMs pins the real node's Info semantics for the
// fake: disk_free_mib is the offer minus the disk of every VM the node still
// holds, retained (stopped) disks included, and it returns on delete. The
// literals are the default 1 TiB offer minus a 51 GiB VM.
func TestInfoDiskFreeTracksLiveVMs(t *testing.T) {
	n := nodefake.New("fake-disk", t.TempDir())
	const offerMiB = 1 << 20 // the default 1 TiB offer
	if got := nodefakeDiskFreeMib(t, n); got != offerMiB {
		t.Fatalf("empty node free = %d MiB, want %d", got, offerMiB)
	}

	vm := &nodev1.CreateVMRequest{
		SandboxId: "bx_diskfree1",
		Resources: &nodev1.Resources{Vcpus: 1, MemoryMib: 512, DiskGib: 51},
	}
	if _, err := n.CreateVM(t.Context(), vm); err != nil {
		t.Fatalf("CreateVM: %v", err)
	}
	if got, want := nodefakeDiskFreeMib(t, n), uint64(offerMiB-51*1024); got != want {
		t.Fatalf("free after create = %d MiB, want %d", got, want)
	}

	if _, err := n.StopVM(t.Context(), &nodev1.StopVMRequest{SandboxId: vm.SandboxId}); err != nil {
		t.Fatalf("StopVM: %v", err)
	}
	if got, want := nodefakeDiskFreeMib(t, n), uint64(offerMiB-51*1024); got != want {
		t.Fatalf("free while stopped = %d MiB, want %d (a stopped VM keeps its disk)", got, want)
	}

	if _, err := n.DeleteVM(t.Context(), &nodev1.DeleteVMRequest{SandboxId: vm.SandboxId}); err != nil {
		t.Fatalf("DeleteVM: %v", err)
	}
	if got := nodefakeDiskFreeMib(t, n); got != offerMiB {
		t.Fatalf("free after delete = %d MiB, want %d", got, offerMiB)
	}
	// A repeated delete stays idempotent and must not inflate free disk.
	if _, err := n.DeleteVM(t.Context(), &nodev1.DeleteVMRequest{SandboxId: vm.SandboxId}); err != nil {
		t.Fatalf("second DeleteVM: %v", err)
	}
	if got := nodefakeDiskFreeMib(t, n); got != offerMiB {
		t.Fatalf("free after repeated delete = %d MiB, want %d", got, offerMiB)
	}
}

// TestInfoDiskFreeOfferAndSaturation covers a configured offer and the
// arithmetic edges: a node overcommitted past its offer reports 0 (no
// underflow), and absurd disk sizes neither wrap the sum nor overflow the
// MiB conversion.
func TestInfoDiskFreeOfferAndSaturation(t *testing.T) {
	n := nodefake.New("fake-disk-edges", t.TempDir())
	n.DiskGiB = 94
	if got, want := nodefakeDiskFreeMib(t, n), uint64(94*1024); got != want {
		t.Fatalf("94 GiB offer, empty: %d MiB, want %d", got, want)
	}
	create := func(id string, diskGiB uint64) {
		t.Helper()
		_, err := n.CreateVM(t.Context(), &nodev1.CreateVMRequest{
			SandboxId: id, Resources: &nodev1.Resources{Vcpus: 1, MemoryMib: 128, DiskGib: diskGiB},
		})
		if err != nil {
			t.Fatalf("CreateVM %s: %v", id, err)
		}
	}
	create("bx_edge0001", 51)
	if got, want := nodefakeDiskFreeMib(t, n), uint64(43*1024); got != want {
		t.Fatalf("94 offer, one 51 GiB VM: %d MiB, want %d", got, want)
	}
	create("bx_edge0002", 51) // fake CreateVM has no disk admission: 102 > 94
	if got := nodefakeDiskFreeMib(t, n); got != 0 {
		t.Fatalf("overcommitted node: %d MiB, want 0 (no underflow)", got)
	}
	create("bx_edge0003", math.MaxUint64) // sum would wrap to 50
	if got := nodefakeDiskFreeMib(t, n); got != 0 {
		t.Fatalf("wrapping sum: %d MiB, want 0", got)
	}

	big := nodefake.New("fake-disk-big", t.TempDir())
	big.DiskGiB = math.MaxUint64
	if got := nodefakeDiskFreeMib(t, big); got != math.MaxUint64 {
		t.Fatalf("huge offer: %d MiB, want saturated %d", got, uint64(math.MaxUint64))
	}
}
