package node

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"

	"github.com/nathabonfim59/bamboo-raft/internal/node/jailer"
	"github.com/nathabonfim59/bamboo-raft/internal/node/qemu"
	"github.com/nathabonfim59/bamboo-raft/internal/node/qmp"
	"github.com/nathabonfim59/bamboo-raft/internal/node/storage"
)

// Restart recovery in jailed mode. The durable jail record, not a /proc
// scan, decides whether a QEMU may exist: the jailer persists an attempt's
// record before any host mutation, and the manifest names the attempt it
// launched (QEMURecord.JailAttempt) before the jailer runs.
//
//   - A record of another attempt, or one no manifest claims, is never
//     released, adopted or removed here: the VM stays FAILED and unresolved.
//   - A claimed record is released only by proof (ReleaseAttempt: pidfd or
//     boot id/start ticks, an empty owned cgroup and the reclaim policy), or
//     adopted only after VerifyProcess, the exact argv digest, the argv
//     structure, the network identity and the QMP peer all match.
//   - Without a record no jailed QEMU of the manifest's attempt can exist;
//     a legacy (unjailed) launch record still refuses startup while its
//     process lives.

// recoverJailed decides the fate of one readable VM record in jailed mode.
// A returned error refuses node startup.
func (m *Manager) recoverJailed(ctx context.Context, meta storage.VMMeta, bootID string) error {
	id := meta.SandboxID
	attempt := ""
	if meta.QEMU != nil {
		attempt = meta.QEMU.JailAttempt
	}
	if rec, ok := m.jail.Record(id); ok {
		if attempt == "" || rec.Attempt != attempt {
			m.recoverUnresolvedJail(meta, fmt.Sprintf("a retained jail reservation of attempt %s that this vm does not own blocks it; resolve it before deleting", rec.Attempt), jailer.Ref{}, nil)
			return nil
		}
		gone, pinned, client := m.recoverClaimedJail(ctx, meta, rec, bootID)
		if !gone {
			if client == nil {
				m.recoverUnresolvedJail(meta, "cannot prove the jailed VM process dead or owned; its reservation is retained", rec.Ref(), pinned)
				return nil
			}
			return m.adoptJailed(ctx, meta, pinned, client, rec)
		}
	}
	if meta.QEMU != nil && meta.QEMU.JailAttempt == "" && meta.QEMU.BootID == bootID {
		// An unjailed launch record from a previous node version. The
		// production scanner identifies it only by the trusted QEMU
		// executable: without one, "not found" proves nothing.
		if m.scanOwned && m.qemuExe == "" {
			return fmt.Errorf("node: vm %s has a legacy unjailed launch record and the trusted QEMU executable is unknown, "+
				"so recovery cannot identify its process; refusing to treat it as gone", id)
		}
		pinned, err := m.findProcess(meta)
		if err != nil {
			return fmt.Errorf("node: vm %s has a legacy unjailed launch record whose process cannot be identified: %w", id, err)
		}
		if pinned != nil {
			pinned.close()
			return fmt.Errorf("node: vm %s is a live legacy unjailed QEMU from a previous node version; "+
				"stop it with that version before starting the jailed node (docs/node.md)", id)
		}
	}
	switch {
	case meta.QEMU == nil && meta.Stopped():
		m.recoverStopped(meta)
	case meta.QEMU == nil:
		// Never launched, and no jail record can name it.
		if err := m.store.RemoveVM(id); err != nil {
			m.log.Warn("node: remove never-launched vm", "sandbox", id, "error", err)
		}
	case !m.hasImage(meta.Image):
		m.recoverFailed(meta, fmt.Sprintf("image %q is not installed", meta.Image))
	case meta.StoppedAtUnixMs > 0:
		m.recoverStoppedAfterInterrupt(meta, "node restarted during a start")
	default:
		m.recoverFailed(meta, "node restarted: the VM process is gone")
	}
	return nil
}

func (m *Manager) hasImage(id string) bool {
	_, ok := m.images[id]
	return ok
}

// recoverClaimedJail resolves the manifest's own attempt. gone reports that
// the reservation was released by proof. Otherwise a non-nil client means the
// live process passed every identity check (pinned is its pidfd); a nil client
// means the attempt is unresolved (pinned, when non-nil, is kept for proof).
func (m *Manager) recoverClaimedJail(ctx context.Context, meta storage.VMMeta, rec jailer.Record, bootID string) (gone bool, pinned *pinnedProc, client *qmp.Client) {
	if handled, released, kept := m.recoverMaintenanceJailed(ctx, meta, rec, bootID); handled {
		// A maintenance QEMU is quit and released, never adopted: this runs
		// before verifyJailed, whose QMP verification would continue it.
		return released, kept, nil
	}
	release := func() bool {
		if err := m.jail.ReleaseAttempt(ctx, rec.Ref()); err != nil {
			m.log.Warn("node: recovered jail reservation retained", "sandbox", rec.SandboxID, "attempt", rec.Attempt, "error", err)
			return false
		}
		return true
	}
	if rec.PID == 0 || rec.BootID != bootID {
		// No captured identity, or a previous host boot: only the jailer's
		// own proof (owned cgroup, boot id) can release it.
		return release(), nil, nil
	}
	pinned, err := pinProc(rec.PID)
	if err != nil || pinned.info.startTicks != rec.StartTicks {
		if pinned != nil {
			pinned.close()
		}
		return release(), nil, nil
	}
	if q := meta.QEMU; q.State == storage.QEMURunning && (q.PID != rec.PID || q.StartTicks != rec.StartTicks) {
		m.log.Warn("node: jailed process identity differs from the manifest", "sandbox", rec.SandboxID)
		return false, pinned, nil
	}
	client, err = m.verifyJailed(ctx, pinned, meta, rec)
	if err != nil {
		m.log.Warn("node: jailed vm process failed verification; not touching it", "sandbox", rec.SandboxID, "error", err)
		return false, pinned, nil
	}
	return false, pinned, client
}

// verifyJailed proves a pinned live process is the manifest's jailed QEMU:
// the jailer's confinement and identity proof, the exact launched argv
// (sha256 of /proc/pid/cmdline), its structure and the QMP peer.
func (m *Manager) verifyJailed(ctx context.Context, pinned *pinnedProc, meta storage.VMMeta, rec jailer.Record) (*qmp.Client, error) {
	if err := m.jail.VerifyProcess(ctx, rec); err != nil {
		return nil, err
	}
	raw, err := os.ReadFile("/proc/" + strconv.Itoa(pinned.pid) + "/cmdline")
	if err != nil {
		return nil, fmt.Errorf("read cmdline: %w", err)
	}
	sum := sha256.Sum256(raw)
	if hex.EncodeToString(sum[:]) != meta.QEMU.ArgvSHA256 {
		return nil, errors.New("argv differs from the launched argv")
	}
	argv := strings.Split(strings.TrimSuffix(string(raw), "\x00"), "\x00")
	if err := jailedArgvIdentity(argv, meta, m.cfg.JailBinary); err != nil {
		return nil, err
	}
	// The QMP proof below is bound to the control directory these pins
	// prove is the attempt's own.
	if err := m.verifyJailedPins(ctx, meta, rec); err != nil {
		return nil, err
	}
	if err := pinned.confirm(); err != nil {
		return nil, err
	}
	return m.verifyProc(ctx, pinned, meta)
}

// jailedArgvIdentity checks a recovered argv with the jailer's own launch
// grammar (jailer.ValidateArgv: every option form, the fixed descriptor
// graph, the read-only base linked as the overlay's backing, the disabled NIC
// option ROM, no duplicate, unknown or positional argument), in addition to
// the exact digest, and requires the trusted binary as argv0.
func jailedArgvIdentity(argv []string, meta storage.VMMeta, binary string) error {
	if len(argv) == 0 || argv[0] != binary {
		return fmt.Errorf("argv0 is not the trusted QEMU %s", binary)
	}
	return jailer.ValidateArgv(meta.SandboxID, argv[1:], jailDisks())
}

// jailDisks are the disk descriptor names of buildJailSpec, oldest first.
func jailDisks() []string { return []string{"base", "overlay"} }

// verifyJailedPins proves, read-only and without following symlinks, that
// the manifest's durable caller pins are the attempt record's and that the
// current control directory, console log and disk are still those inodes.
// A live VM's resources are never chowned, pinned or rewritten here.
func (m *Manager) verifyJailedPins(ctx context.Context, meta storage.VMMeta, rec jailer.Record) error {
	if meta.Control == nil || meta.Console == nil {
		return errors.New("the manifest records no control/console pins for the jailed attempt")
	}
	control := storage.Identity{Dev: rec.ControlDev, Ino: rec.ControlIno}
	console := storage.Identity{Dev: rec.LogDev, Ino: rec.LogIno}
	if *meta.Control != control || *meta.Console != console {
		return errors.New("the manifest pins differ from the attempt's recorded control/console")
	}
	for _, want := range []struct {
		name string
		flag int
		id   storage.Identity
		dir  bool
	}{
		{storage.ControlDirName, os.O_RDONLY | syscall.O_DIRECTORY, control, true},
		{storage.ConsoleLogName, os.O_RDONLY, console, false},
		{"disk.qcow2", os.O_RDONLY, storage.Identity{Dev: rec.TargetDev, Ino: rec.TargetIno}, false},
	} {
		f, err := m.store.OpenVMEntry(ctx, meta.SandboxID, want.name, want.flag)
		if err != nil {
			return fmt.Errorf("pin %s: %w", want.name, err)
		}
		info, err := f.Stat()
		f.Close()
		if err != nil {
			return fmt.Errorf("pin %s: %w", want.name, err)
		}
		st, ok := info.Sys().(*syscall.Stat_t)
		if !ok || info.IsDir() != want.dir || (!want.dir && !info.Mode().IsRegular()) ||
			(storage.Identity{Dev: uint64(st.Dev), Ino: st.Ino}) != want.id {
			return fmt.Errorf("%s is not the attempt's recorded inode", want.name)
		}
	}
	return nil
}

// adoptJailed takes ownership of a verified live jailed QEMU, after the same
// image and network identity rules as an unjailed adoption.
func (m *Manager) adoptJailed(ctx context.Context, meta storage.VMMeta, pinned *pinnedProc, client *qmp.Client, rec jailer.Record) error {
	id := meta.SandboxID
	refuse := func(err error) error {
		client.Close()
		pinned.close()
		return err
	}
	if !m.hasImage(meta.Image) {
		return refuse(fmt.Errorf("node: vm %s names an uninstalled image %q but its jailed process lives", id, meta.Image))
	}
	alloc, ok := m.networkAllocation(id)
	if !ok {
		return refuse(fmt.Errorf("node: jailed vm %s is live without a durable network allocation; refusing startup", id))
	}
	if err := tapArgvIdentity(pinned.info.argv, alloc); err != nil {
		return refuse(fmt.Errorf("node: jailed vm %s is live with a NIC that does not match its durable allocation: %v; refusing startup", id, err))
	}
	if meta.QEMU.State == storage.QEMULaunching {
		if err := m.store.CommitRunning(id, pinned.pid); err != nil {
			client.Close()
			m.recoverUnresolvedJail(meta, "cannot record recovered process identity: "+err.Error(), rec.Ref(), pinned)
			return nil
		}
	}
	m.adoptRef(ctx, meta, pinned, client, rec.Ref())
	return nil
}

// recoverUnresolvedJail records a FAILED VM whose ownership is unresolved,
// naming the attempt it owns (or none for an unclaimed record), with its full
// reservation forced.
func (m *Manager) recoverUnresolvedJail(meta storage.VMMeta, msg string, ref jailer.Ref, pinned *pinnedProc) {
	m.addRecoveredRef(meta, msg, pinned, true, ref)
}

// jailRefuseCorrupt refuses startup for a corrupt manifest that a retained
// jail reservation names: its process and resources cannot be identified.
func (m *Manager) jailRefuseCorrupt(name string, cause error) error {
	if m.jail == nil {
		return nil
	}
	if rec, ok := m.jail.Record(name); ok {
		return fmt.Errorf("node: corrupt vm %s (manifest: %w) has a retained jail reservation (attempt %s); resolve it before starting", name, cause, rec.Attempt)
	}
	return nil
}

// jailRefuseOrphans refuses startup while a VM jail reservation exists that
// no VM record accounts for.
func (m *Manager) jailRefuseOrphans() error {
	if m.jail == nil {
		return nil
	}
	for _, rec := range m.jail.Records() {
		if rec.Kind != jailer.KindVM {
			continue
		}
		m.mu.Lock()
		_, known := m.vms[rec.SandboxID]
		m.mu.Unlock()
		if !known {
			return fmt.Errorf("node: retained jail reservation %s attempt %s has no vm record; resolve it before starting", rec.SandboxID, rec.Attempt)
		}
	}
	return nil
}

// resolveJailUnresolved proves an unresolved jailed VM's attempt released (by
// identity) before DeleteVM may remove it. An unclaimed retained record
// blocks removal.
func (m *Manager) resolveJailUnresolved(v *vm) (handled bool, err error) {
	if m.jail == nil {
		return false, nil
	}
	v.mu.Lock()
	unresolved := v.ownershipUnresolved
	ref := v.jail
	pinned := v.unresolved
	v.mu.Unlock()
	if !unresolved {
		// A resolved VM settles its jail in its normal stop path.
		return true, nil
	}
	if ref == (jailer.Ref{}) {
		if rec, ok := m.jail.Record(v.id); ok {
			return true, fmt.Errorf("%w: %s attempt %s", errJailConflict, v.id, rec.Attempt)
		}
		return false, nil
	}
	if err := m.settleJailDetached(v, nil); err != nil {
		return true, err
	}
	if pinned != nil {
		pinned.close()
	}
	v.mu.Lock()
	v.unresolved = nil
	v.ownershipUnresolved = false
	v.mu.Unlock()
	return true, nil
}

// jailControlQMP is the host path of a jailed VM's QMP socket.
func (m *Manager) jailControlQMP(id string) string {
	return filepath.Join(m.store.VMDir(id), storage.ControlDirName, qemu.QMPFilename)
}
