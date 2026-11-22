package node

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/nathabonfim59/bamboo-raft/internal/ids"
	"github.com/nathabonfim59/bamboo-raft/internal/node/qemu"
	"github.com/nathabonfim59/bamboo-raft/internal/node/storage"
	nodev1 "github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

// qmpSockLeftover is what a fake QEMU leaves at its QMP path: the socket a
// crashed QEMU leaves, or a non-socket the node must never remove.
type qmpSockLeftover int

const (
	qmpSockSocket qmpSockLeftover = iota
	qmpSockFile
)

// qmpSockLauncher makes the fake launcher behave like the real one at the
// QMP path: a start refuses an existing qmp.sock exactly as qemu.Start's
// launch-time check does (any type, never removed), and the started fake
// leaves leftover there, which nothing but the node removes.
func qmpSockLauncher(t *testing.T, l *fakeLauncher, leftover qmpSockLeftover) {
	t.Helper()
	l.mu.Lock()
	defer l.mu.Unlock()
	l.newProcess = func(cfg qemu.Config) (*fakeProcess, error) {
		if _, err := os.Lstat(cfg.QMPPath()); !errors.Is(err, fs.ErrNotExist) {
			return nil, fmt.Errorf("qemu: %s already exists; refusing to reuse another process's socket", cfg.QMPPath())
		}
		if err := qmpSockLeave(cfg.QMPPath(), leftover); err != nil {
			return nil, err
		}
		return newFakeProcess(), nil
	}
}

func qmpSockLeave(path string, leftover qmpSockLeftover) error {
	switch leftover {
	case qmpSockSocket:
		return syscall.Mknod(path, syscall.S_IFSOCK|0o600, 0)
	default:
		return os.WriteFile(path, []byte("not a socket"), 0o600)
	}
}

func qmpSockMake(t *testing.T, path string, leftover qmpSockLeftover) {
	t.Helper()
	if err := qmpSockLeave(path, leftover); err != nil {
		t.Fatalf("leave %s: %v", path, err)
	}
}

// qmpSockMode returns the type at path, or ok false when nothing is there.
func qmpSockMode(t *testing.T, path string) (fs.FileMode, bool) {
	t.Helper()
	info, err := os.Lstat(path)
	if errors.Is(err, fs.ErrNotExist) {
		return 0, false
	}
	if err != nil {
		t.Fatal(err)
	}
	return info.Mode().Type(), true
}

func qmpSockWaitState(t *testing.T, m *Manager, id string, want nodev1.VMState) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		state, vm := stopStartVMState(t, m, id)
		if state == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("state = %s (%v), want %s", state, vm, want)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestCrashedQEMUSocketDoesNotBlockStart: a crashed non-jailed QEMU leaves
// its qmp.sock behind. Once the crash is settled (exit watcher, StopVM) the
// socket is gone and StartVM boots, while a non-socket at that path is kept
// and still refused by the launch.
func TestCrashedQEMUSocketDoesNotBlockStart(t *testing.T) {
	for _, tt := range []struct {
		name     string
		leftover qmpSockLeftover
	}{
		{name: "socket removed", leftover: qmpSockSocket},
		{name: "non-socket kept and refused", leftover: qmpSockFile},
	} {
		t.Run(tt.name, func(t *testing.T) {
			h := newHarness(t, harnessOptions{})
			qmpSockLauncher(t, h.launcher, tt.leftover)
			id := ids.Sandbox()
			h.create(id, nil)
			qmpPath := qemu.Config{WorkDir: h.mg.store.VMDir(id)}.QMPPath()
			if _, ok := qmpSockMode(t, qmpPath); !ok {
				t.Fatalf("fake QEMU left nothing at %s", qmpPath)
			}
			h.launcher.lastProcess().exit(errors.New("signal: killed"))
			qmpSockWaitState(t, h.mg, id, nodev1.VMState_VM_STATE_FAILED)
			stopped, err := h.mg.StopVM(t.Context(), &nodev1.StopVMRequest{SandboxId: id, GraceSeconds: 1})
			if err != nil || stopped.GetState() != nodev1.VMState_VM_STATE_STOPPED {
				t.Fatalf("StopVM after crash = %v, %v", stopped, err)
			}
			mode, present := qmpSockMode(t, qmpPath)
			started, err := h.mg.StartVM(t.Context(), &nodev1.StartVMRequest{SandboxId: id})
			switch tt.leftover {
			case qmpSockSocket:
				if present {
					t.Fatalf("crashed QEMU's socket still at %s (type %v) after the stop", qmpPath, mode)
				}
				if err != nil || started.GetState() != nodev1.VMState_VM_STATE_READY {
					t.Fatalf("StartVM after crash = %v, %v", started, err)
				}
			case qmpSockFile:
				if !present || mode != 0 {
					t.Fatalf("non-socket at %s was not kept: present %t, type %v", qmpPath, present, mode)
				}
				if err == nil || !strings.Contains(err.Error(), "already exists") {
					t.Fatalf("StartVM over a non-socket = %v, %v; want the launch refusal", started, err)
				}
			}
			h.delete(id)
		})
	}
}

// TestRefusedLaunchKeepsExistingSocket: a socket nobody proved stale (no QEMU
// of this start ever ran) is never removed by the failed start's settlement,
// so the launch keeps refusing it.
func TestRefusedLaunchKeepsExistingSocket(t *testing.T) {
	h := newHarness(t, harnessOptions{})
	id := ids.Sandbox()
	h.create(id, nil)
	if _, err := h.mg.StopVM(t.Context(), &nodev1.StopVMRequest{SandboxId: id, GraceSeconds: 1}); err != nil {
		t.Fatalf("StopVM: %v", err)
	}
	qmpSockLauncher(t, h.launcher, qmpSockSocket)
	qmpPath := qemu.Config{WorkDir: h.mg.store.VMDir(id)}.QMPPath()
	qmpSockMake(t, qmpPath, qmpSockSocket)
	for range 2 {
		if _, err := h.mg.StartVM(t.Context(), &nodev1.StartVMRequest{SandboxId: id}); err == nil || !strings.Contains(err.Error(), "already exists") {
			t.Fatalf("StartVM over a foreign socket = %v; want the launch refusal", err)
		}
		if mode, ok := qmpSockMode(t, qmpPath); !ok || mode != fs.ModeSocket {
			t.Fatalf("foreign socket at %s not kept by the failed start: present %t, type %v", qmpPath, ok, mode)
		}
		// The refused start leaves a FAILED record; its stop settles no
		// process of ours, so it must not remove the socket either.
		if _, err := h.mg.StopVM(t.Context(), &nodev1.StopVMRequest{SandboxId: id, GraceSeconds: 1}); err != nil {
			t.Fatalf("StopVM after the refused start: %v", err)
		}
		if mode, ok := qmpSockMode(t, qmpPath); !ok || mode != fs.ModeSocket {
			t.Fatalf("foreign socket at %s not kept by the stop: present %t, type %v", qmpPath, ok, mode)
		}
	}
}

// TestRestartRemovesDeadQEMUSocket: restart recovery that proves the recorded
// QEMU gone (a previous host boot, or no owned process left) removes its
// socket, so the recovered VM starts; a non-socket is kept and refused, and
// a record whose process is unresolved keeps its socket.
func TestRestartRemovesDeadQEMUSocket(t *testing.T) {
	noProcess := func(storage.VMMeta) (*pinnedProc, error) { return nil, nil }
	ambiguous := func(storage.VMMeta) (*pinnedProc, error) { return nil, errors.New("two candidates") }
	for _, tt := range []struct {
		name     string
		oldBoot  bool
		find     func(storage.VMMeta) (*pinnedProc, error)
		leftover qmpSockLeftover
		want     nodev1.VMState
		removed  bool
	}{
		{name: "previous host boot", oldBoot: true, find: noProcess, leftover: qmpSockSocket, want: nodev1.VMState_VM_STATE_STOPPED, removed: true},
		{name: "process gone", find: noProcess, leftover: qmpSockSocket, want: nodev1.VMState_VM_STATE_FAILED, removed: true},
		{name: "non-socket kept", oldBoot: true, find: noProcess, leftover: qmpSockFile, want: nodev1.VMState_VM_STATE_STOPPED},
		{name: "unresolved keeps socket", find: ambiguous, leftover: qmpSockSocket, want: nodev1.VMState_VM_STATE_FAILED},
	} {
		t.Run(tt.name, func(t *testing.T) {
			h := newHarness(t, harnessOptions{})
			id := ids.Sandbox()
			h.create(id, nil)
			if tt.oldBoot {
				if _, err := h.mg.StopVM(t.Context(), &nodev1.StopVMRequest{SandboxId: id, GraceSeconds: 1}); err != nil {
					t.Fatalf("StopVM: %v", err)
				}
				// A crash mid-start: a launch record from a previous host boot.
				stopStartEditManifest(t, h, id, func(m *storage.VMMeta) {
					m.QEMU = &storage.QEMURecord{
						State:           storage.QEMURunning,
						PID:             4242,
						BootID:          "an-old-host-boot",
						StartTicks:      99,
						StartedAtUnixMs: m.StoppedAtUnixMs,
					}
				})
			}
			qmpPath := qemu.Config{WorkDir: h.mg.store.VMDir(id)}.QMPPath()
			qmpSockMake(t, qmpPath, tt.leftover)
			h2 := stopStartReopen(t, h, "", func(c *Config) { c.findProcess = tt.find }, nil)
			if state, vm := stopStartVMState(t, h2.mg, id); state != tt.want {
				t.Fatalf("state after restart = %s (%v), want %s", state, vm, tt.want)
			}
			mode, present := qmpSockMode(t, qmpPath)
			if tt.removed {
				if present {
					t.Fatalf("dead QEMU's socket still at %s (type %v) after recovery", qmpPath, mode)
				}
			} else if !present || (tt.leftover == qmpSockSocket) != (mode == fs.ModeSocket) {
				t.Fatalf("%s not kept by recovery: present %t, type %v", qmpPath, present, mode)
			}
			if !tt.removed {
				// A stop of the recovered record settles no process handle
				// of ours (its outcome is not this test's concern), so the
				// socket nobody proved stale stays.
				_, _ = h2.mg.StopVM(t.Context(), &nodev1.StopVMRequest{SandboxId: id, GraceSeconds: 1})
				if mode2, ok := qmpSockMode(t, qmpPath); !ok || mode2 != mode {
					t.Fatalf("%s not kept by StopVM: present %t, type %v", qmpPath, ok, mode2)
				}
				return
			}
			if tt.want == nodev1.VMState_VM_STATE_FAILED {
				if _, err := h2.mg.StopVM(t.Context(), &nodev1.StopVMRequest{SandboxId: id, GraceSeconds: 1}); err != nil {
					t.Fatalf("StopVM after recovery: %v", err)
				}
			}
			qmpSockLauncher(t, h2.launcher, qmpSockSocket)
			started, err := h2.mg.StartVM(t.Context(), &nodev1.StartVMRequest{SandboxId: id})
			if err != nil || started.GetState() != nodev1.VMState_VM_STATE_READY {
				t.Fatalf("StartVM after recovery = %v, %v", started, err)
			}
			h2.delete(id)
		})
	}
}
