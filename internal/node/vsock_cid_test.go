package node

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/nathabonfim59/bamboo-raft/internal/node/storage"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

func TestLocalCIDProbeOrDefault(t *testing.T) {
	if got := localCIDProbeOrDefault(nil); got == nil {
		t.Fatal("nil probe did not resolve to the production probe")
	}
	calls := 0
	fake := func(context.Context) (uint32, error) {
		calls++
		return 7, nil
	}
	got, err := localCIDProbeOrDefault(fake)(t.Context())
	if err != nil || got != 7 || calls != 1 {
		t.Fatalf("injected probe = (%d, %v), calls %d; want (7, nil), 1", got, err, calls)
	}
}

func TestProbeLocalVsockCID(t *testing.T) {
	dir := t.TempDir()
	regular := filepath.Join(dir, "not-vsock")
	if err := os.WriteFile(regular, nil, 0o600); err != nil {
		t.Fatal(err)
	}

	if cid, err := probeLocalVsockCID(t.Context(), filepath.Join(dir, "missing")); err != nil || cid != 0 {
		t.Fatalf("missing device = (%d, %v); want (0, nil)", cid, err)
	}
	if _, err := probeLocalVsockCID(t.Context(), regular); !errors.Is(err, syscall.ENOTTY) {
		t.Fatalf("regular file error = %v; want ENOTTY", err)
	}
	if _, err := probeLocalVsockCID(t.Context(), dir); err == nil || errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("directory error = %v; want a non-ENOENT ioctl failure", err)
	}
}

func TestProbeLocalVsockCIDCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := probeLocalVsockCID(ctx, filepath.Join(t.TempDir(), "missing")); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled probe error = %v; want context.Canceled before opening", err)
	}
}

func TestProbeLocalVsockCIDClosesDescriptors(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "not-vsock")
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	before := openFDCount(t)
	for range 50 {
		if _, err := probeLocalVsockCID(t.Context(), path); !errors.Is(err, syscall.ENOTTY) {
			t.Fatalf("probe error = %v; want ENOTTY", err)
		}
	}
	if after := openFDCount(t); after > before {
		t.Fatalf("probe leaked descriptors: %d -> %d", before, after)
	}
}

func openFDCount(t *testing.T) int {
	t.Helper()
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatalf("read /proc/self/fd: %v", err)
	}
	return len(entries)
}

func TestStartupLocalCID(t *testing.T) {
	tests := []struct {
		name     string
		probe    func(context.Context) (uint32, error)
		min, max uint32
		want     uint32
		wantErr  error
	}{
		{name: "in range", probe: func(context.Context) (uint32, error) { return 3, nil }, min: 3, max: 64, want: 3},
		{name: "host cid", probe: func(context.Context) (uint32, error) { return 2, nil }, min: 3, max: 64},
		{name: "below bounds", probe: func(context.Context) (uint32, error) { return 1, nil }, min: 3, max: 64},
		{name: "above bounds", probe: func(context.Context) (uint32, error) { return 65, nil }, min: 3, max: 64},
		{name: "absent device", probe: func(context.Context) (uint32, error) { return 0, nil }, min: 3, max: 64},
		{name: "joined absent and io", probe: func(context.Context) (uint32, error) {
			return 0, errors.Join(fs.ErrNotExist, syscall.EIO)
		}, min: 3, max: 64, wantErr: syscall.EIO},
		{name: "unexpected absent", probe: func(context.Context) (uint32, error) { return 0, fs.ErrNotExist }, min: 3, max: 64, wantErr: fs.ErrNotExist},
		{name: "permission", probe: func(context.Context) (uint32, error) { return 0, fs.ErrPermission }, min: 3, max: 64, wantErr: fs.ErrPermission},
		{name: "io", probe: func(context.Context) (uint32, error) { return 0, syscall.EIO }, min: 3, max: 64, wantErr: syscall.EIO},
		{name: "not a tty", probe: func(context.Context) (uint32, error) { return 0, syscall.ENOTTY }, min: 3, max: 64, wantErr: syscall.ENOTTY},
		{name: "no device", probe: func(context.Context) (uint32, error) { return 0, syscall.ENODEV }, min: 3, max: 64, wantErr: syscall.ENODEV},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			calls := 0
			cid, err := startupLocalCID(t.Context(), slog.New(slog.DiscardHandler), tt.min, tt.max, func(ctx context.Context) (uint32, error) {
				calls++
				return tt.probe(ctx)
			})
			if calls != 1 {
				t.Fatalf("probe calls = %d; want 1", calls)
			}
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("error = %v; want %v", err, tt.wantErr)
			}
			if cid != tt.want {
				t.Fatalf("cid = %d; want %d", cid, tt.want)
			}
		})
	}
}

func TestStartupLocalCIDCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	calls := 0
	_, err := startupLocalCID(ctx, slog.New(slog.DiscardHandler), 3, 64, func(context.Context) (uint32, error) {
		calls++
		return 3, nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled startup error = %v; want context.Canceled", err)
	}
	if calls != 0 {
		t.Fatalf("canceled startup probed %d times; want 0", calls)
	}
}

func TestStartupLocalCIDLogsOnlyRealExclusion(t *testing.T) {
	tests := []struct {
		name  string
		probe func(context.Context) (uint32, error)
		want  int
	}{
		{name: "excluded", probe: func(context.Context) (uint32, error) { return 3, nil }, want: 1},
		{name: "host cid", probe: func(context.Context) (uint32, error) { return 2, nil }, want: 0},
		{name: "above bounds", probe: func(context.Context) (uint32, error) { return 65, nil }, want: 0},
		{name: "absent", probe: func(context.Context) (uint32, error) { return 0, nil }, want: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			log := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
			if _, err := startupLocalCID(t.Context(), log, 3, 64, tt.probe); err != nil {
				t.Fatalf("startupLocalCID: %v", err)
			}
			if got := strings.Count(buf.String(), "level=INFO"); got != tt.want {
				t.Fatalf("INFO events = %d; want %d (log: %s)", got, tt.want, buf.String())
			}
			if tt.want == 1 && !strings.Contains(buf.String(), "cid=3") {
				t.Fatalf("INFO event does not name the excluded cid: %s", buf.String())
			}
		})
	}
}

func TestAllocCIDExcludesLocalCID(t *testing.T) {
	tests := []struct {
		name     string
		min, max uint32
		excluded uint32
		used     []uint32
		want     uint32
		wantOK   bool
	}{
		{name: "first cid excluded", min: 3, max: 64, excluded: 3, want: 4, wantOK: true},
		{name: "no exclusion", min: 3, max: 64, want: 3, wantOK: true},
		{name: "host cid", min: 3, max: 64, excluded: 2, want: 3, wantOK: true},
		{name: "outside bounds", min: 3, max: 64, excluded: 100, want: 3, wantOK: true},
		{name: "middle excluded", min: 3, max: 64, excluded: 7, used: []uint32{3, 4, 5, 6}, want: 8, wantOK: true},
		{name: "excluded and used interleave", min: 3, max: 64, excluded: 5, used: []uint32{3, 4, 6}, want: 7, wantOK: true},
		{name: "free below excluded max", min: 3, max: 5, excluded: 5, used: []uint32{4}, want: 3, wantOK: true},
		{name: "excluded max at end", min: 3, max: 5, excluded: 5, used: []uint32{3, 4}},
		{name: "only cid excluded", min: 3, max: 3, excluded: 3},
		{name: "only cid free", min: 3, max: 3, want: 3, wantOK: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := newCIDManager(tt.min, tt.max, tt.excluded, tt.used...)
			m.mu.Lock()
			cid, ok := m.allocCIDLocked()
			m.mu.Unlock()
			if ok != tt.wantOK || cid != tt.want {
				t.Fatalf("allocCIDLocked() = (%d, %v); want (%d, %v)", cid, ok, tt.want, tt.wantOK)
			}
		})
	}
}

func newCIDManager(min, max, excluded uint32, used ...uint32) *Manager {
	m := &Manager{
		cfg:         Config{MinCID: min, MaxCID: max},
		excludedCID: excluded,
		vms:         make(map[string]*vm, len(used)),
	}
	for i, cid := range used {
		id := fmt.Sprintf("bx_%08x", i)
		m.vms[id] = &vm{id: id, meta: storage.VMMeta{SandboxID: id, VsockCID: cid}}
	}
	return m
}

func TestCreateVMAllocatesAroundLocalCID(t *testing.T) {
	tests := []struct {
		name     string
		probe    func(context.Context) (uint32, error)
		wantCID  uint32
		min, max uint32
	}{
		{name: "local cid excluded", probe: func(context.Context) (uint32, error) { return 3, nil }, wantCID: 4, min: 3, max: 64},
		{name: "host cid ignored", probe: func(context.Context) (uint32, error) { return 2, nil }, wantCID: 3, min: 3, max: 64},
		{name: "absent device", probe: func(context.Context) (uint32, error) { return 0, nil }, wantCID: 3, min: 3, max: 64},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newHarness(t, harnessOptions{configure: func(cfg *Config) {
				cfg.MinCID, cfg.MaxCID = tt.min, tt.max
				cfg.localCID = tt.probe
			}})
			const id = "bx_22222222"
			h.create(id, nil)
			h.mg.mu.Lock()
			got := h.mg.vms[id].meta.VsockCID
			h.mg.mu.Unlock()
			if got != tt.wantCID {
				t.Fatalf("VsockCID = %d; want %d", got, tt.wantCID)
			}
		})
	}
}

func TestCreateVMExhaustedByLocalCID(t *testing.T) {
	h := newHarness(t, harnessOptions{configure: func(cfg *Config) {
		cfg.MinCID, cfg.MaxCID = 3, 3
		cfg.localCID = func(context.Context) (uint32, error) { return 3, nil }
	}})
	_, err := h.mg.CreateVM(t.Context(), &nodev1.CreateVMRequest{
		SandboxId: "bx_22222222",
		Resources: &nodev1.Resources{Vcpus: 2, MemoryMib: 2048, DiskGib: defaultTestBaseGiB},
	})
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("CreateVM error = %v; want ResourceExhausted", err)
	}
	if !strings.Contains(err.Error(), "no vsock CID is free") {
		t.Fatalf("CreateVM error %q does not explain the exhausted CID range", err)
	}
}

func TestNewProbeErrorFailsBeforeStore(t *testing.T) {
	dataDir := t.TempDir()
	imageDir := t.TempDir()
	_, err := New(t.Context(), Config{
		NodeID:              "node-test",
		DataDir:             dataDir,
		ImageDir:            imageDir,
		Capacity:            &nodev1.Resources{Vcpus: 1, MemoryMib: 1 << 20, DiskGib: 1},
		AllowUserNetworking: true,
		localCID:            func(context.Context) (uint32, error) { return 0, fs.ErrPermission },
	})
	if !errors.Is(err, fs.ErrPermission) {
		t.Fatalf("New error = %v; want fs.ErrPermission", err)
	}
	entries, readErr := os.ReadDir(dataDir)
	if readErr != nil {
		t.Fatalf("read data dir: %v", readErr)
	}
	if len(entries) != 0 {
		t.Fatalf("probe failure left store state: %v", entries)
	}
}

// TestKVMAllocatedCIDExcludesLocalCID boots one real VM and proves its durable
// CID is not the host's own local vsock CID. It requires BBRAFT_TEST_KVM=1 and
// BBRAFT_TEST_IMAGE_DIR (see requireKVM), so ordinary runs skip it.
func TestKVMAllocatedCIDExcludesLocalCID(t *testing.T) {
	imageDir := requireKVM(t)
	m := newKVMManager(t, imageDir)
	const id = "bx_22222222"
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		if _, err := m.DeleteVM(ctx, &nodev1.DeleteVMRequest{SandboxId: id}); err != nil {
			t.Errorf("cleanup DeleteVM: %v", err)
		}
	})
	if _, err := m.CreateVM(t.Context(), &nodev1.CreateVMRequest{
		SandboxId: id,
		Resources: &nodev1.Resources{Vcpus: 2, MemoryMib: 2048, DiskGib: 10},
	}); err != nil {
		t.Fatalf("CreateVM: %v", err)
	}
	meta, err := m.store.ReadVM(id)
	if err != nil {
		t.Fatalf("read durable VM meta: %v", err)
	}
	if m.excludedCID != 0 && meta.VsockCID == m.excludedCID {
		t.Fatalf("guest cid %d equals the node's excluded local cid", meta.VsockCID)
	}
	t.Logf("node excluded local vsock cid=%d, guest cid=%d", m.excludedCID, meta.VsockCID)
	if _, err := m.DeleteVM(t.Context(), &nodev1.DeleteVMRequest{SandboxId: id}); err != nil {
		t.Fatalf("DeleteVM: %v", err)
	}
}

func TestCreateVMProbesLocalCIDOnce(t *testing.T) {
	calls := 0
	h := newHarness(t, harnessOptions{configure: func(cfg *Config) {
		cfg.localCID = func(context.Context) (uint32, error) {
			calls++
			return 2, nil
		}
	}})
	if calls != 1 {
		t.Fatalf("New probe calls = %d; want 1", calls)
	}
	h.create("bx_22222222", nil)
	h.create("bx_33333333", nil)
	h.delete("bx_22222222")
	h.delete("bx_33333333")
	if calls != 1 {
		t.Fatalf("probe calls after allocations = %d; want 1", calls)
	}

	reopenCalls := 0
	h2 := stopStartReopen(t, h, "", func(cfg *Config) {
		cfg.localCID = func(context.Context) (uint32, error) {
			reopenCalls++
			return 2, nil
		}
	}, nil)
	if reopenCalls != 1 {
		t.Fatalf("reopened New probe calls = %d; want 1", reopenCalls)
	}
	if calls != 1 {
		t.Fatalf("original manager re-probed: %d calls; want 1", calls)
	}
	h2.create("bx_44444444", nil)
	if reopenCalls != 1 {
		t.Fatalf("reopened probe calls after allocation = %d; want 1", reopenCalls)
	}
}
