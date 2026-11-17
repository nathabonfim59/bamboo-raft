package node

import (
	"testing"

	"github.com/nathabonfim59/bamboo-raft/internal/node/qemu"
	"github.com/nathabonfim59/bamboo-raft/internal/node/storage"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

// TestLaunchConfigPropagatesNested proves every parsed policy reaches the
// Launcher's qemu.Config. It uses launchConfig directly, so it needs no VM
// boot or snapshot test scaffolding.
func TestLaunchConfigPropagatesNested(t *testing.T) {
	store, err := storage.Open(storage.Config{DataDir: t.TempDir(), ImageDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	for _, mode := range []qemu.NestedMode{qemu.NestedAuto, qemu.NestedOn, qemu.NestedOff} {
		t.Run(mode.String(), func(t *testing.T) {
			m := &Manager{cfg: Config{Nested: mode}, store: store}
			cfg := m.launchConfig(storage.VMMeta{
				SandboxID: "bx_22222222",
				Image:     "img-v0",
				Resources: storage.Resources{VCPUs: 1, MemoryMiB: 1024, DiskGiB: 1},
				VsockCID:  3,
			}, nil)
			if cfg.Nested != mode {
				t.Fatalf("launch Nested = %q, want %q", cfg.Nested, mode)
			}
		})
	}
}

// TestNormalizeConfigNested delegates only enum parsing to the shared qemu
// policy: empty/auto canonicalize to NestedAuto and invalid values fail.
func TestNormalizeConfigNested(t *testing.T) {
	base := func() Config {
		return Config{
			NodeID:   "node-test",
			DataDir:  t.TempDir(),
			ImageDir: t.TempDir(),
			Capacity: &nodev1.Resources{Vcpus: 1, MemoryMib: 1, DiskGib: 1},
			// Test-factory construction: the dev/test user-mode network.
			AllowUserNetworking: true,
		}
	}
	for _, tc := range []struct {
		name string
		in   qemu.NestedMode
		want qemu.NestedMode
		bad  bool
	}{
		{"empty is auto", "", qemu.NestedAuto, false},
		{"explicit auto", qemu.NestedAuto, qemu.NestedAuto, false},
		{"on", qemu.NestedOn, qemu.NestedOn, false},
		{"off", qemu.NestedOff, qemu.NestedOff, false},
		{"invalid", "sometimes", "", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := base()
			cfg.Nested = tc.in
			err := normalizeConfig(&cfg)
			if tc.bad {
				if err == nil {
					t.Fatal("invalid nested mode accepted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if cfg.Nested != tc.want {
				t.Fatalf("Nested = %q, want %q", cfg.Nested, tc.want)
			}
		})
	}
}
