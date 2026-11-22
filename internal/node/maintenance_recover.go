package node

import (
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"golang.org/x/sys/unix"

	"github.com/nathabonfim59/bamboo-raft/internal/node/jailer"
	"github.com/nathabonfim59/bamboo-raft/internal/node/qemu"
	"github.com/nathabonfim59/bamboo-raft/internal/node/qmp"
	"github.com/nathabonfim59/bamboo-raft/internal/node/storage"
)

// Restart recovery of a maintenance QEMU. The explicit marker is the
// maintenance -name (qemu.MaintenanceName); both launch forms carry -S, so
// nothing else tells them apart. A recovered maintenance QEMU is proven,
// quit over QMP (killed when it does not exit) and never continued, adopted
// or booted. These hooks run before any verifyQMPAt path.

// recoverMaintenance handles a non-jailed record whose recorded live process
// is a maintenance QEMU. handled reports that the record's fate is decided.
func (m *Manager) recoverMaintenance(ctx context.Context, meta storage.VMMeta, bootID string) (handled bool) {
	q := meta.QEMU
	if q == nil || q.BootID != bootID {
		return false
	}
	var pinned *pinnedProc
	if q.PID != 0 {
		p, err := pinProc(q.PID)
		if err != nil {
			return false
		}
		if p.info.startTicks != q.StartTicks || len(p.info.argv) == 0 || !jailer.IsMaintenanceArgv(meta.SandboxID, p.info.argv[1:]) {
			p.close()
			return false
		}
		pinned = p
	} else if m.qemuExe != "" {
		p, err := findOwnedMaintenance(meta.SandboxID, m.store.VMDir(meta.SandboxID), m.store.DiskPath(meta.SandboxID), m.qemuExe)
		if err != nil {
			m.recoverUnresolved(meta, "cannot identify a maintenance QEMU safely: "+err.Error(), nil)
			return true
		}
		if p == nil {
			return false
		}
		pinned = p
	} else {
		return false
	}
	qmpPath := filepath.Join(m.store.VMDir(meta.SandboxID), qemu.QMPFilename)
	if m.qemuExe != "" && !maintOwnedPaths(pinned.info, meta.SandboxID, m.store.VMDir(meta.SandboxID), m.store.DiskPath(meta.SandboxID), m.qemuExe) {
		m.recoverUnresolved(meta, "a maintenance QEMU with this record's identity does not use its paths", pinned)
		return true
	}
	if err := m.quitRecoveredMaintenance(ctx, pinned, meta.SandboxID, qmpPath); err != nil {
		m.log.Warn("node: recovered maintenance QEMU not settled", "sandbox", meta.SandboxID, "error", err)
		m.recoverUnresolved(meta, "cannot end a recovered maintenance QEMU: "+err.Error(), pinned)
		return true
	}
	pinned.close()
	if err := m.store.RemovePhase2Scratch(meta.SandboxID); err != nil {
		m.log.Warn("node: remove maintenance scratch", "sandbox", meta.SandboxID, "error", err)
	}
	// The quit (or kill) is proven: a socket it left would refuse the start.
	m.removeStaleQMPSocket(meta.SandboxID)
	m.recoverAfterMaintenance(meta)
	return true
}

// recoverMaintenanceJailed is recoverClaimedJail's first step: when the
// claimed record's live process is a maintenance QEMU it proves it (jailer
// identity, exact argv digest and grammar, pins, QMP peer), quits it and
// releases the attempt. handled reports that it was one; gone that the
// attempt is released; pinned, when not gone, is kept for proof.
func (m *Manager) recoverMaintenanceJailed(ctx context.Context, meta storage.VMMeta, rec jailer.Record, bootID string) (handled, gone bool, pinned *pinnedProc) {
	if rec.PID == 0 || rec.BootID != bootID || meta.QEMU == nil {
		return false, false, nil
	}
	p, err := pinProc(rec.PID)
	if err != nil {
		return false, false, nil
	}
	if p.info.startTicks != rec.StartTicks {
		p.close()
		return false, false, nil
	}
	raw, err := os.ReadFile("/proc/" + strconv.Itoa(p.pid) + "/cmdline")
	if err != nil {
		p.close()
		return false, false, nil
	}
	argv := strings.Split(strings.TrimSuffix(string(raw), "\x00"), "\x00")
	if len(argv) == 0 || !jailer.IsMaintenanceArgv(meta.SandboxID, argv[1:]) {
		p.close()
		return false, false, nil
	}
	id := meta.SandboxID
	unresolved := func(err error) (bool, bool, *pinnedProc) {
		m.log.Warn("node: recovered maintenance QEMU not settled; not touching it", "sandbox", id, "error", err)
		return true, false, p
	}
	sum := sha256.Sum256(raw)
	if hex.EncodeToString(sum[:]) != meta.QEMU.ArgvSHA256 {
		return unresolved(errors.New("argv differs from the launched argv"))
	}
	if argv[0] != m.cfg.JailBinary {
		return unresolved(fmt.Errorf("argv0 is not the trusted QEMU %s", m.cfg.JailBinary))
	}
	if err := jailer.ValidateMaintenanceArgv(id, argv[1:], jailDisks()); err != nil {
		return unresolved(err)
	}
	if err := m.jail.VerifyProcess(ctx, rec); err != nil {
		return unresolved(err)
	}
	if err := m.verifyJailedPins(ctx, meta, rec); err != nil {
		return unresolved(err)
	}
	if err := m.quitRecoveredMaintenance(ctx, p, id, m.jailControlQMP(id)); err != nil {
		return unresolved(err)
	}
	p.close()
	if err := m.jail.ReleaseAttempt(ctx, rec.Ref()); err != nil {
		m.log.Warn("node: recovered maintenance jail reservation retained", "sandbox", id, "attempt", rec.Attempt, "error", err)
		return true, false, nil
	}
	return true, true, nil
}

// recoverAfterMaintenance registers a non-jailed record whose maintenance
// QEMU was ended during recovery: a stopped VM stops again, a create that
// was restoring becomes FAILED with its disk kept.
func (m *Manager) recoverAfterMaintenance(meta storage.VMMeta) {
	if meta.StoppedAtUnixMs > 0 {
		m.recoverStoppedAfterInterrupt(meta, "node restarted during a maintenance session")
		return
	}
	m.recoverFailed(meta, "node restarted during a maintenance session of a create")
}

// quitRecoveredMaintenance proves a pinned maintenance QEMU through its own
// QMP socket (peer pid, guest name) and quits it, killing it through the
// pidfd when it does not exit within maintQuitTimeout. It never sends cont.
// nil means the process is proven gone.
func (m *Manager) quitRecoveredMaintenance(ctx context.Context, pinned *pinnedProc, id, qmpPath string) error {
	if err := pinned.confirm(); err != nil {
		// Gone already, or replaced: only an exit is success.
		if exited, perr := pinned.pollForExit(0); perr == nil && exited {
			return nil
		}
		return err
	}
	dialCtx, cancel := context.WithTimeout(ctx, maintTeardownTimeout)
	defer cancel()
	client, err := qmp.Dial(dialCtx, qmpPath, m.log)
	if err != nil {
		return fmt.Errorf("qmp: %w", err)
	}
	defer client.Close()
	peer, err := client.PeerPID()
	if err != nil {
		return fmt.Errorf("qmp peer: %w", err)
	}
	if peer != pinned.pid {
		return fmt.Errorf("qmp peer is pid %d, not %d", peer, pinned.pid)
	}
	var name struct {
		Name string `json:"name"`
	}
	if err := client.Execute(dialCtx, "query-name", nil, &name); err != nil {
		return fmt.Errorf("query-name: %w", err)
	}
	if name.Name != id && name.Name != "guest="+id {
		return fmt.Errorf("qmp reports guest name %q", name.Name)
	}
	_ = client.Execute(dialCtx, "quit", nil, nil)
	if exited, err := pinned.pollForExit(cmp.Or(m.maintQuit, maintQuitTimeout)); err == nil && exited {
		return nil
	}
	if err := pinned.signal(unix.SIGKILL); err != nil {
		return fmt.Errorf("kill: %w", err)
	}
	exited, err := pinned.pollForExit(m.cleanupWait())
	if err != nil {
		return err
	}
	if !exited {
		return fmt.Errorf("pid %d did not exit after SIGKILL", pinned.pid)
	}
	return nil
}

// maintOwnedPaths is ownedPaths for a non-jailed maintenance argv: the
// trusted executable, the maintenance -name marker, the VM's QMP socket and
// disk.
func maintOwnedPaths(info procInfo, id, workDir, diskPath, expectedExe string) bool {
	if expectedExe == "" || info.exe != expectedExe {
		return false
	}
	if argAfter(info.argv, "-name") != qemu.MaintenanceName(id) {
		return false
	}
	if argAfter(info.argv, "-qmp") != "unix:"+filepath.Join(workDir, qemu.QMPFilename)+",server=on,wait=off" {
		return false
	}
	return strings.Contains(argAfter(info.argv, "-blockdev"), ",file.filename="+diskPath+",")
}

// findOwnedMaintenance scans /proc for this sandbox's non-jailed maintenance
// QEMU (a launch intent without a recorded pid).
func findOwnedMaintenance(id, workDir, diskPath, expectedExe string) (*pinnedProc, error) {
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return nil, fmt.Errorf("node: scan /proc: %w", err)
	}
	var matches []*pinnedProc
	for _, e := range entries {
		pid, err := strconv.Atoi(e.Name())
		if err != nil || pid <= 0 {
			continue
		}
		info, err := readProc(pid)
		if err != nil || !maintOwnedPaths(info, id, workDir, diskPath, expectedExe) {
			continue
		}
		pinned, err := pinProc(pid)
		if err != nil {
			continue
		}
		if !maintOwnedPaths(pinned.info, id, workDir, diskPath, expectedExe) {
			pinned.close()
			continue
		}
		matches = append(matches, pinned)
	}
	switch len(matches) {
	case 0:
		return nil, nil
	case 1:
		return matches[0], nil
	default:
		for _, p := range matches {
			p.close()
		}
		return nil, fmt.Errorf("node: %d maintenance processes use sandbox %s's owned paths", len(matches), id)
	}
}
