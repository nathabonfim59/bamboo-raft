package node

import (
	"bufio"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
	"uuid"

	"github.com/nathabonfim59/bamboo-raft/internal/ids"
	"github.com/nathabonfim59/bamboo-raft/internal/node/qemu"
	"github.com/nathabonfim59/bamboo-raft/internal/node/storage"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

// maintQMPHelperEnv makes the re-executed argv helper also serve a minimal
// QMP socket at the given path, so the socket's SO_PEERCRED is the helper's
// own pid: it answers query-name with maintQMPNameEnv, exits 0 on quit and
// exits maintHelperContExit if it is ever continued.
const (
	maintQMPHelperEnv   = "BBRAFT_NODE_MAINT_QMP_PATH"
	maintQMPNameEnv     = "BBRAFT_NODE_MAINT_QMP_NAME"
	maintHelperContExit = 9
)

// maintQMPHelper runs in the re-executed helper before it reports ready.
func maintQMPHelper() {
	path := os.Getenv(maintQMPHelperEnv)
	if path == "" {
		return
	}
	name := os.Getenv(maintQMPNameEnv)
	lis, err := net.Listen("unix", path)
	if err != nil {
		os.Exit(3)
	}
	go func() {
		for {
			conn, err := lis.Accept()
			if err != nil {
				return
			}
			go maintQMPHelperConn(conn, name)
		}
	}()
}

func maintQMPHelperConn(conn net.Conn, name string) {
	defer conn.Close()
	fmt.Fprintln(conn, `{"QMP":{"version":{"qemu":{"major":10,"minor":0,"micro":0},"package":"test"},"capabilities":[]}}`)
	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		var req struct {
			Execute string `json:"execute"`
			ID      uint64 `json:"id"`
		}
		if err := json.Unmarshal(scanner.Bytes(), &req); err != nil {
			return
		}
		var result any = map[string]any{}
		switch req.Execute {
		case "cont":
			os.Exit(maintHelperContExit)
		case "query-name":
			result = map[string]any{"name": name}
		case "query-status":
			result = map[string]any{"running": false, "status": "prelaunch"}
		}
		raw, _ := json.Marshal(map[string]any{"return": result, "id": req.ID})
		_, _ = conn.Write(append(raw, '\n'))
		if req.Execute == "quit" {
			os.Exit(0)
		}
	}
}

// maintLiveHelper starts a live process whose /proc cmdline is exactly argv
// and which serves QMP at qmpPath as its own pid. done delivers its exit.
func maintLiveHelper(t *testing.T, argv []string, qmpPath, name string) (*exec.Cmd, <-chan error) {
	t.Helper()
	cmd := exec.Command(argv[0], argv[1:]...)
	cmd.Env = append(os.Environ(), jailArgvHelperEnv+"=1", maintQMPHelperEnv+"="+qmpPath, maintQMPNameEnv+"="+name)
	readyR, readyW, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer readyR.Close()
	stdin, err := cmd.StdinPipe()
	if err != nil {
		readyW.Close()
		t.Fatal(err)
	}
	cmd.ExtraFiles = []*os.File{readyW}
	startErr := cmd.Start()
	readyW.Close()
	if startErr != nil {
		stdin.Close()
		t.Fatal(startErr)
	}
	done := make(chan error, 1)
	reaped := make(chan struct{})
	go func() {
		done <- cmd.Wait()
		close(reaped)
	}()
	t.Cleanup(func() {
		stdin.Close()
		_ = cmd.Process.Kill()
		select {
		case <-reaped:
		case <-time.After(10 * time.Second):
			t.Errorf("maintenance helper %d was not reaped", cmd.Process.Pid)
		}
	})
	if err := readyR.SetReadDeadline(time.Now().Add(30 * time.Second)); err != nil {
		t.Fatal(err)
	}
	line := make([]byte, 6)
	if _, err := io.ReadFull(readyR, line); err != nil || string(line) != "ready\n" {
		t.Fatalf("maintenance helper ready = %q, %v", line, err)
	}
	return cmd, done
}

// maintWaitExit waits for a helper's exit and returns its code.
func maintWaitExit(t *testing.T, done <-chan error) int {
	t.Helper()
	select {
	case err := <-done:
		if err == nil {
			return 0
		}
		if exitErr, ok := errors.AsType[*exec.ExitError](err); ok {
			return exitErr.ExitCode()
		}
		t.Fatalf("helper wait: %v", err)
	case <-time.After(15 * time.Second):
		t.Fatal("the recovered maintenance QEMU was left running")
	}
	return -1
}

func maintHelperExe(t *testing.T) string {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	exe, err = filepath.EvalSymlinks(exe)
	if err != nil {
		t.Fatal(err)
	}
	return exe
}

func maintHelperConfig(id, workDir, imageDir, diskPath string) qemu.Config {
	host := qemu.HostCPU{Vendor: "GenuineIntel"}
	return qemu.Config{
		Binary: os.Args[0], SandboxID: id, WorkDir: workDir, ImageDir: imageDir, DiskPath: diskPath,
		VCPUs: 1, MemoryMiB: 256, VsockCID: 9, Nested: qemu.NestedOff, HostCPU: &host,
	}
}

// TestMaintenanceRecoveryPlainQuits: a non-jailed node restarted while a
// maintenance QEMU of a STOPPED VM runs proves it, quits it (never
// continues it) and recovers the VM as STOPPED.
func TestMaintenanceRecoveryPlainQuits(t *testing.T) {
	id := ids.Sandbox()
	var done <-chan error
	h := newHarness(t, harnessOptions{
		qemuExe: maintHelperExe(t),
		prepare: func(t *testing.T, dataDir, imageDir string) {
			store, err := storage.Open(storage.Config{DataDir: dataDir, ImageDir: imageDir, Log: quietLog(),
				Imager: &fakeImager{diskGiB: defaultTestBaseGiB, basePath: filepath.Join(imageDir, testImageID, "root.qcow2")}})
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			images, err := store.VerifyImages(t.Context())
			if err != nil || len(images) != 1 {
				t.Fatalf("VerifyImages = %v, %v", images, err)
			}
			meta := storage.VMMeta{SandboxID: id, Image: testImageID, Resources: storage.Resources{VCPUs: 2, MemoryMiB: 2048, DiskGiB: defaultTestBaseGiB},
				VsockCID: 9, CreatedAtUnixMs: time.Now().UnixMilli()}
			if err := store.CreateVM(t.Context(), meta, images[0]); err != nil {
				t.Fatal(err)
			}
			if err := store.CommitStopped(id, time.Now().UnixMilli()); err != nil {
				t.Fatal(err)
			}
			cfg := maintHelperConfig(id, store.VMDir(id), store.ImagePath(testImageID), store.DiskPath(id))
			argv, err := cfg.PlainMaintenanceArgs()
			if err != nil {
				t.Fatal(err)
			}
			if err := store.WriteLaunchIntent(id); err != nil {
				t.Fatal(err)
			}
			var cmd *exec.Cmd
			cmd, done = maintLiveHelper(t, argv, cfg.QMPPath(), "guest="+id)
			if err := store.CommitRunning(id, cmd.Process.Pid); err != nil {
				t.Fatal(err)
			}
		},
	})
	if code := maintWaitExit(t, done); code != 0 {
		t.Fatalf("recovered maintenance helper exit %d, want 0 (quit; %d = continued)", code, maintHelperContExit)
	}
	got, err := h.mg.GetVM(t.Context(), &nodev1.GetVMRequest{SandboxId: id})
	if err != nil {
		t.Fatal(err)
	}
	if got.GetState() != nodev1.VMState_VM_STATE_STOPPED {
		t.Fatalf("state %s (%s), want STOPPED", got.GetState(), got.GetError())
	}
	meta, err := h.mg.store.ReadVM(id)
	if err != nil {
		t.Fatal(err)
	}
	if !meta.Stopped() {
		t.Fatalf("manifest QEMU record kept: %+v", meta.QEMU)
	}
	// The ended maintenance QEMU's socket must not refuse the next start.
	if _, err := os.Lstat(filepath.Join(h.mg.store.VMDir(id), qemu.QMPFilename)); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("recovered maintenance QEMU's qmp.sock left behind: %v", err)
	}
}

// TestMaintenanceRecoveryIgnoresVMArgv: a live VM argv carrying -S is never
// handled as maintenance (it is left alone and untouched over QMP).
func TestMaintenanceRecoveryIgnoresVMArgv(t *testing.T) {
	h := newHarness(t, harnessOptions{qemuExe: maintHelperExe(t)})
	id := ids.Sandbox()
	dir := t.TempDir()
	short, err := os.MkdirTemp("/tmp", "bbraft-maintvm-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(short) })
	cfg := maintHelperConfig(id, short, dir, filepath.Join(dir, "disk.qcow2"))
	argv, err := cfg.Args()
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(argv, "-S") {
		t.Fatalf("the non-jailed VM argv is not paused: %q", argv)
	}
	cmd, done := maintLiveHelper(t, argv, cfg.QMPPath(), "guest="+id)
	boot, err := storage.HostBootID()
	if err != nil {
		t.Fatal(err)
	}
	meta := storage.VMMeta{SandboxID: id, StoppedAtUnixMs: 1, QEMU: &storage.QEMURecord{
		State: storage.QEMURunning, PID: cmd.Process.Pid, BootID: boot, StartTicks: mustProcStartTicks(t, cmd.Process.Pid)}}
	if h.mg.recoverMaintenance(t.Context(), meta, boot) {
		t.Fatal("a VM argv with -S was handled as maintenance")
	}
	meta.QEMU.PID = 0
	if h.mg.recoverMaintenance(t.Context(), meta, boot) {
		t.Fatal("the maintenance scan matched a VM argv")
	}
	select {
	case err := <-done:
		t.Fatalf("the VM process was touched: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
}

// jailMaintFixture prepares a jailed STOPPED VM whose claimed attempt's live
// process is a helper with the exact jailed maintenance argv.
func jailMaintFixture(t *testing.T, mutateDigest bool) (*jailedHarness, string, *exec.Cmd, <-chan error) {
	t.Helper()
	id, attempt := ids.Sandbox(), uuid.NewV7().String()
	fj := newFakeJail(t)
	var cmd *exec.Cmd
	var done <-chan error
	j := newJailedHarness(t, jailedOptions{
		jail: fj, fallback: pingOnlyGuest{}, stubGuest: true,
		prepare: func(t *testing.T, dataDir, imageDir string) {
			jailPrepare(t, dataDir, imageDir, id, func(s *storage.Store) {
				if err := s.CommitStopped(id, time.Now().UnixMilli()); err != nil {
					t.Fatal(err)
				}
				images, err := s.VerifyImages(t.Context())
				if err != nil {
					t.Fatal(err)
				}
				pins, err := s.PinLaunch(t.Context(), id, images[0])
				if err != nil {
					t.Fatal(err)
				}
				control, console, disk := jailIdentityOf(t, pins.Control), jailIdentityOf(t, pins.Console), jailIdentityOf(t, pins.Overlay)
				pins.Close()
				cfg := maintHelperConfig(id, "/w", "/i", "/d")
				args, err := cfg.MaintenanceArgs()
				if err != nil {
					t.Fatal(err)
				}
				argv := append([]string{os.Args[0]}, args...)
				cmd, done = maintLiveHelper(t, argv, filepath.Join(s.VMDir(id), storage.ControlDirName, qemu.QMPFilename), "guest="+id)
				digest := argvDigest(argv)
				if mutateDigest {
					digest = strings.Repeat("cd", 32)
				}
				if err := s.WriteJailedLaunchIntent(t.Context(), id, attempt, digest); err != nil {
					t.Fatal(err)
				}
				if err := s.CommitRunning(id, cmd.Process.Pid); err != nil {
					t.Fatal(err)
				}
				rec := jailRecord(id, attempt, cmd.Process.Pid)
				rec.StartTicks = mustProcStartTicks(t, cmd.Process.Pid)
				rec.ControlDev, rec.ControlIno = control.Dev, control.Ino
				rec.LogDev, rec.LogIno = console.Dev, console.Ino
				rec.TargetDev, rec.TargetIno = disk.Dev, disk.Ino
				fj.records[id] = rec
			})
		},
		configure: func(cfg *Config) { cfg.JailBinary = os.Args[0] },
	})
	return j, id, cmd, done
}

// TestMaintenanceRecoveryJailedQuits: a restarted jailed node proves the
// claimed attempt's maintenance QEMU, quits it, releases the attempt and
// recovers the VM as STOPPED, without adopting or continuing it.
func TestMaintenanceRecoveryJailedQuits(t *testing.T) {
	j, id, _, done := jailMaintFixture(t, false)
	if code := maintWaitExit(t, done); code != 0 {
		t.Fatalf("recovered maintenance helper exit %d, want 0 (quit; %d = continued)", code, maintHelperContExit)
	}
	if _, ok := j.jail.Record(id); ok {
		t.Fatal("the maintenance attempt's reservation was not released")
	}
	got, err := j.mg.GetVM(t.Context(), &nodev1.GetVMRequest{SandboxId: id})
	if err != nil {
		t.Fatal(err)
	}
	if got.GetState() != nodev1.VMState_VM_STATE_STOPPED {
		t.Fatalf("state %s (%s), want STOPPED", got.GetState(), got.GetError())
	}
}

// TestMaintenanceRecoveryJailedUnprovenKept: a maintenance argv whose digest
// differs from the launch intent is not proven: it is neither quit nor
// released, and the VM stays FAILED and unresolved.
func TestMaintenanceRecoveryJailedUnprovenKept(t *testing.T) {
	j, id, _, done := jailMaintFixture(t, true)
	select {
	case err := <-done:
		t.Fatalf("an unproven maintenance QEMU was touched: %v", err)
	case <-time.After(200 * time.Millisecond):
	}
	if _, ok := j.jail.Record(id); !ok {
		t.Fatal("an unproven attempt was released")
	}
	if !j.unresolved(id) {
		t.Fatal("the VM is not FAILED and unresolved")
	}
}
