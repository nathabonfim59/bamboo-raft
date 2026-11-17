package node

import (
	"maps"
	"math"
	"runtime"
	"strings"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/nathabonfim59/bamboo-raft/internal/node/storage"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

func overcommitUsedCIDs(m *Manager) map[uint32]struct{} {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make(map[uint32]struct{}, len(m.vms))
	for _, v := range m.vms {
		out[v.meta.VsockCID] = struct{}{}
	}
	return out
}

func overcommitTestCapacity() *nodev1.Resources {
	return &nodev1.Resources{Vcpus: 4096, MemoryMib: 1 << 20, DiskGib: 1024}
}

// TestNormalizeOvercommitDefaults verifies Config zero values: logical host
// CPUs come from runtime.NumCPU and the disk ratio defaults to one.
func TestNormalizeOvercommitDefaults(t *testing.T) {
	cfg := Config{
		NodeID: "node-test", DataDir: t.TempDir(), ImageDir: t.TempDir(),
		Capacity: &nodev1.Resources{Vcpus: 1, MemoryMib: 64, DiskGib: 1},
		// Test-factory construction: the dev/test user-mode network.
		AllowUserNetworking: true,
	}
	if err := normalizeConfig(&cfg); err != nil {
		t.Fatal(err)
	}
	if want := uint32(runtime.NumCPU()); cfg.HostVCPUs != want {
		t.Fatalf("HostVCPUs = %d, want runtime.NumCPU %d", cfg.HostVCPUs, want)
	}
	if cfg.DiskOvercommit != 1 {
		t.Fatalf("DiskOvercommit = %v, want 1", cfg.DiskOvercommit)
	}
}

func TestNormalizeOvercommitRejectsInvalidDiskRatio(t *testing.T) {
	for _, ratio := range []float64{-1, math.NaN(), math.Inf(1), math.Inf(-1)} {
		cfg := Config{
			NodeID: "node-test", DataDir: t.TempDir(), ImageDir: t.TempDir(),
			Capacity: &nodev1.Resources{Vcpus: 1, MemoryMib: 64, DiskGib: 1},
			// Test-factory construction: the dev/test user-mode network.
			AllowUserNetworking: true,
			DiskOvercommit:      ratio,
		}
		if err := normalizeConfig(&cfg); err == nil {
			t.Fatalf("DiskOvercommit %v accepted", ratio)
		}
	}
}

// TestManagerHostVCPUCeiling: overcommitted aggregate capacity still cannot
// give one VM more vCPUs than the host has; >1024 stays an invalid request and
// existing-ID idempotency still wins.
func TestManagerHostVCPUCeiling(t *testing.T) {
	h := newHarness(t, harnessOptions{
		capacity: overcommitTestCapacity(),
		configure: func(cfg *Config) {
			cfg.HostVCPUs = 4
		},
	})
	const id = "bx_22222222"
	h.create(id, &nodev1.Resources{Vcpus: 4, MemoryMib: 1024, DiskGib: 1})
	// Two host-sized VMs are allowed: the aggregate 8 exceeds the 4-CPU host,
	// proving overcommit is soft while the per-VM ceiling still holds.
	h.create("bx_33333333", &nodev1.Resources{Vcpus: 4, MemoryMib: 1024, DiskGib: 1})
	h.mg.mu.Lock()
	reserved := h.mg.reserved
	h.mg.mu.Unlock()
	if reserved.vcpus != 8 {
		t.Fatalf("aggregate reservation = %+v, want 8 vCPUs", reserved)
	}
	cidsBefore := overcommitUsedCIDs(h.mg)

	if _, err := h.mg.CreateVM(t.Context(), &nodev1.CreateVMRequest{
		SandboxId: "bx_44444444", Resources: &nodev1.Resources{Vcpus: 5, MemoryMib: 1024, DiskGib: 1},
	}); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("host+1 vCPUs = %v, want ResourceExhausted", err)
	}
	h.mg.mu.Lock()
	afterReserved := h.mg.reserved
	h.mg.mu.Unlock()
	if afterReserved != reserved {
		t.Fatalf("rejected host+1 changed reservation: %+v, want %+v", afterReserved, reserved)
	}
	if cidsAfter := overcommitUsedCIDs(h.mg); !maps.Equal(cidsAfter, cidsBefore) {
		t.Fatalf("rejected host+1 changed CIDs: %v -> %v", cidsBefore, cidsAfter)
	}
	if _, err := h.mg.CreateVM(t.Context(), &nodev1.CreateVMRequest{
		SandboxId: "bx_55555555", Resources: &nodev1.Resources{Vcpus: 1025, MemoryMib: 1024, DiskGib: 1},
	}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("1025 vCPUs = %v, want InvalidArgument", err)
	}
	// Existing-ID CreateVM ignores all other fields, including this bound.
	if _, err := h.mg.CreateVM(t.Context(), &nodev1.CreateVMRequest{
		SandboxId: id, Resources: &nodev1.Resources{Vcpus: 5, MemoryMib: 1024, DiskGib: 1},
	}); err != nil {
		t.Fatalf("existing-id create = %v", err)
	}
	if _, err := h.mg.GetVM(t.Context(), &nodev1.GetVMRequest{SandboxId: "bx_44444444"}); status.Code(err) != codes.NotFound {
		t.Fatalf("rejected host+1 left a record: %v", err)
	}
}

// TestManagerDiskOvercommitAdmissionAndInfo: ratio 2 scales the live free
// promise for both admission and Info; ratio 1 preserves the old behavior.
func TestManagerDiskOvercommitAdmissionAndInfo(t *testing.T) {
	free := uint64(10 << 30)
	h := newHarness(t, harnessOptions{
		capacity: &nodev1.Resources{Vcpus: 64, MemoryMib: 32 << 10, DiskGib: 18},
		diskFree: func() (uint64, error) { return free, nil },
		configure: func(cfg *Config) {
			cfg.HostVCPUs = 8
			cfg.DiskOvercommit = 2
		},
	})
	h.create("bx_22222222", &nodev1.Resources{Vcpus: 2, MemoryMib: 1024, DiskGib: 15})
	info, err := h.mg.Info(t.Context(), &nodev1.InfoRequest{})
	if err != nil {
		t.Fatal(err)
	}
	// Scaled free is 20 GiB, 15 is reserved, but the offer has only 3 GiB
	// left, so Info caps at the configured offer.
	if got := info.GetDiskFreeMib(); got != 3<<10 {
		t.Fatalf("Info disk_free_mib = %d, want offer-capped %d", got, 3<<10)
	}
	// A 4 GiB disk would fit the 5 GiB scaled-free remainder but not the
	// remaining configured offer (3 GiB).
	if _, err := h.mg.CreateVM(t.Context(), &nodev1.CreateVMRequest{
		SandboxId: "bx_33333333", Resources: &nodev1.Resources{Vcpus: 2, MemoryMib: 1024, DiskGib: 4},
	}); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("offer-cap admission = %v, want ResourceExhausted", err)
	}

	// Live free can shrink and then bound admission even with offer left.
	free = 7 << 30 // scaled promise 14 GiB < the 15 GiB already reserved
	info, err = h.mg.Info(t.Context(), &nodev1.InfoRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if got := info.GetDiskFreeMib(); got != 0 {
		t.Fatalf("Info after free shrink = %d, want 0", got)
	}
	if _, err := h.mg.CreateVM(t.Context(), &nodev1.CreateVMRequest{
		SandboxId: "bx_44444444", Resources: &nodev1.Resources{Vcpus: 2, MemoryMib: 1024, DiskGib: 1},
	}); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("live-free bound admission = %v, want ResourceExhausted", err)
	}

	h1 := newHarness(t, harnessOptions{
		capacity: &nodev1.Resources{Vcpus: 64, MemoryMib: 32 << 10, DiskGib: 100},
		diskFree: func() (uint64, error) { return 10 << 30, nil },
		configure: func(cfg *Config) {
			cfg.HostVCPUs = 8
		},
	})
	h1.create("bx_22222222", &nodev1.Resources{Vcpus: 2, MemoryMib: 1024, DiskGib: 8})
	info, err = h1.mg.Info(t.Context(), &nodev1.InfoRequest{})
	if err != nil {
		t.Fatal(err)
	}
	// Ratio one preserves the previous Info arithmetic: 10 free - 8 reserved.
	if got := info.GetDiskFreeMib(); got != 2<<10 {
		t.Fatalf("ratio1 Info disk_free_mib = %d, want %d", got, 2<<10)
	}
	if _, err := h1.mg.CreateVM(t.Context(), &nodev1.CreateVMRequest{
		SandboxId: "bx_33333333", Resources: &nodev1.Resources{Vcpus: 2, MemoryMib: 1024, DiskGib: 3},
	}); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("ratio1 over live free = %v, want ResourceExhausted", err)
	}
}

// TestManagerInfoFractionalOfferCap proves the offer clamp uses the whole
// scaled MiB value, including a fractional GiB remainder, and reports zero
// when the offer has no whole GiB left.
func TestManagerInfoFractionalOfferCap(t *testing.T) {
	store, err := storage.Open(storage.Config{DataDir: t.TempDir(), ImageDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	base := &Manager{
		cfg:            Config{DiskOvercommit: 2},
		diskOvercommit: 2,
		store:          store,
		capacity:       resources{vcpus: 1, memoryMib: 1, diskGiB: 20},
		diskFree:       func() (uint64, error) { return 10<<30 + 256<<20, nil },
	}
	info, err := base.Info(t.Context(), &nodev1.InfoRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if got := info.GetDiskFreeMib(); got != 20<<10 {
		t.Errorf("fractional offer cap disk_free_mib = %d, want %d", got, 20<<10)
	}

	zero := &Manager{
		cfg:            Config{DiskOvercommit: 1},
		diskOvercommit: 1,
		store:          store,
		capacity:       resources{vcpus: 1, memoryMib: 1, diskGiB: 1},
		reserved:       resources{diskGiB: 1},
		diskFree:       func() (uint64, error) { return 1536 << 20, nil },
	}
	info, err = zero.Info(t.Context(), &nodev1.InfoRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if got := info.GetDiskFreeMib(); got != 0 {
		t.Fatalf("zero offer with sub-GiB promise disk_free_mib = %d, want 0", got)
	}
}

func TestManagerDiskPromiseOverflowIsInternal(t *testing.T) {
	h := newHarness(t, harnessOptions{
		capacity: &nodev1.Resources{Vcpus: 64, MemoryMib: 32 << 10, DiskGib: 100},
		diskFree: func() (uint64, error) { return math.MaxUint64, nil },
		configure: func(cfg *Config) {
			cfg.HostVCPUs = 8
			cfg.DiskOvercommit = 2
		},
	})
	if _, err := h.mg.Info(t.Context(), &nodev1.InfoRequest{}); status.Code(err) != codes.Internal || !strings.Contains(err.Error(), "overflows uint64") {
		t.Fatalf("overflow Info = %v, want Internal overflow", err)
	}
	_, err := h.mg.CreateVM(t.Context(), &nodev1.CreateVMRequest{
		SandboxId: "bx_22222222", Resources: &nodev1.Resources{Vcpus: 2, MemoryMib: 1024, DiskGib: 1},
	})
	if status.Code(err) != codes.Internal || !strings.Contains(err.Error(), "overflows uint64") {
		t.Fatalf("overflow admission = %v, want Internal overflow", err)
	}
}
