package jailer

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/nathabonfim59/bamboo-raft/internal/ids"
	"golang.org/x/sys/unix"
)

// Review round 1 regressions.

// TestJailerFixtureIDsValid pins every sandbox id the tests use to the frozen
// ids contract, so no fixture exercises an id production would refuse.
func TestJailerFixtureIDsValid(t *testing.T) {
	for _, id := range []string{
		jailerReviewID, jailerReviewID2, jailerLaunchID, jailerArgvID,
		jailerPrivID1, jailerPrivID2, jailerPrivID3, jailerPrivID4,
	} {
		if !ids.ValidSandbox(id) {
			t.Errorf("fixture id %q is not a valid sandbox id", id)
		}
	}
}

// TestStartCancelBeforeSpawn cancels the startup context at each pre-spawn
// boundary: Start must stop there with context.Canceled, spawn nothing and
// roll back every resource.
func TestStartCancelBeforeSpawn(t *testing.T) {
	jailerNeedBinary(t)
	for _, stage := range []launchStage{
		stageJailBase, stageJailBaseRecord, stageJailPopulate, stageJailRecord,
		stageCgroup, stageCgroupRecord, stageOwnFiles, stageSpawn,
	} {
		t.Run(string(stage), func(t *testing.T) {
			env := newJailerEnv(t)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			env.stages.on = map[launchStage]func(){stage: cancel}
			m := env.open(t)
			defer m.Close()
			child, _, err := jailerStart(t, m, ctx, jailerSpec(t, jailerLaunchID))
			if child != nil {
				t.Fatalf("Start spawned after cancellation at %s (err %v)", stage, err)
			}
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("Start = %v, want context.Canceled", err)
			}
			if got := env.stages.last(); got != stage {
				t.Fatalf("last stage %q, want %q", got, stage)
			}
			jailerAssertReleased(t, env, m, jailerLaunchID)
		})
	}
}

// TestStartCancelAtExecProof cancels after the status pipe reported a clean
// exec of the real staged binary: Start must not report success.
func TestStartCancelAtExecProof(t *testing.T) {
	jailerNeedBinary(t)
	env := newJailerEnv(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	env.stages.on = map[launchStage]func(){stageExecProof: cancel}
	m := env.open(t)
	defer m.Close()
	child, owner, err := jailerStart(t, m, ctx, jailerSpec(t, jailerLaunchID))
	if child == nil {
		t.Fatalf("Start lost the child: %v", err)
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Start = %v after cancellation at exec proof, want context.Canceled", err)
	}
	owner.waitKilled(t)
	if err := child.Release(t.Context()); err != nil {
		t.Fatal(err)
	}
	jailerAssertReleased(t, env, m, jailerLaunchID)
}

// TestStartRejectsWrongDeviceRoles: a descriptor of the wrong device type
// for its role, and a writable role aliasing a read-only one, are refused
// before any reservation, chmod or chown.
func TestStartRejectsWrongDeviceRoles(t *testing.T) {
	jailerNeedBinary(t)
	tests := []struct {
		name   string
		edit   func(t *testing.T, s *LaunchSpec)
		reason string
	}{
		{name: "tap is an unrelated char device", edit: func(t *testing.T, s *LaunchSpec) {
			s.Files[0].File = jailerOpen(t, "/dev/random", os.O_RDWR)
		}, reason: TAPName},
		{name: "vhost-vsock is an unrelated char device", edit: func(t *testing.T, s *LaunchSpec) {
			s.Files[4].File = jailerOpen(t, "/dev/random", os.O_RDWR)
		}, reason: VhostVsockName},
		{name: "serial log aliases the kernel", edit: func(t *testing.T, s *LaunchSpec) {
			s.SerialLog = s.Files[2].File
		}, reason: "aliases"},
		{name: "overlay aliases the serial log", edit: func(t *testing.T, s *LaunchSpec) {
			s.SerialLog = s.Files[5].File
		}, reason: "aliases"},
	}
	tests = append(tests, []struct {
		name   string
		edit   func(t *testing.T, s *LaunchSpec)
		reason string
	}{
		{name: "vhost-net is the vsock device", edit: func(t *testing.T, s *LaunchSpec) {
			s.Files[1].File = jailerOpen(t, jailerDevice(VhostVsockName), os.O_RDWR)
		}, reason: VhostNetName},
		{name: "control dir reused as a disk", edit: func(t *testing.T, s *LaunchSpec) {
			s.ControlDir = jailerOpen(t, filepath.Dir(s.Files[5].File.Name()), os.O_RDONLY)
			s.SerialLog = s.Files[5].File
		}, reason: "aliases"},
	}...)
	for _, flags := range []struct {
		name  string
		flags uint16
	}{
		{"tun mode", unix.IFF_TUN | unix.IFF_NO_PI | unix.IFF_VNET_HDR},
		{"no vnet header", unix.IFF_TAP | unix.IFF_NO_PI},
		{"packet info", unix.IFF_TAP | unix.IFF_VNET_HDR},
	} {
		t.Run("tap flags "+flags.name, func(t *testing.T) {
			env := newJailerEnv(t)
			env.tapFlags = flags.flags
			m := env.open(t)
			defer m.Close()
			_, _, err := jailerStart(t, m, t.Context(), jailerSpec(t, jailerLaunchID))
			if err == nil || !strings.Contains(err.Error(), "IFF_TAP|IFF_NO_PI|IFF_VNET_HDR") {
				t.Fatalf("Start = %v, want a TAP flags refusal", err)
			}
			if _, ok := m.Record(jailerLaunchID); ok || len(env.chowned) != 0 {
				t.Fatal("a refused TAP left a reservation or chowned files")
			}
		})
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			env := newJailerEnv(t)
			m := env.open(t)
			defer m.Close()
			spec := jailerSpec(t, jailerLaunchID)
			tt.edit(t, &spec)
			_, _, err := jailerStart(t, m, t.Context(), spec)
			if err == nil || !strings.Contains(err.Error(), tt.reason) {
				t.Fatalf("Start = %v, want a refusal naming %q", err, tt.reason)
			}
			if len(env.chowned) != 0 {
				t.Fatalf("files were chowned before the refusal: %v", env.chowned)
			}
			if _, ok := m.Record(jailerLaunchID); ok {
				t.Fatal("a refused spec left a reservation")
			}
		})
	}
}

// TestStartPreservesForeignInitialRecord: a record that appears at the
// sandbox's path after New is never overwritten by the first write.
func TestStartPreservesForeignInitialRecord(t *testing.T) {
	jailerNeedBinary(t)
	env := newJailerEnv(t)
	m := env.open(t)
	defer m.Close()
	name := filepath.Join(env.cfg.StateDir, recordPath(jailerLaunchID))
	foreign := []byte("foreign record; do not replace")
	if err := os.WriteFile(name, foreign, 0o600); err != nil {
		t.Fatal(err)
	}
	before, err := os.Lstat(name)
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = jailerStart(t, m, t.Context(), jailerSpec(t, jailerLaunchID))
	if err == nil {
		t.Fatal("Start overwrote a foreign initial record")
	}
	after, statErr := os.Lstat(name)
	raw, readErr := os.ReadFile(name)
	if statErr != nil || readErr != nil || !os.SameFile(before, after) || !bytes.Equal(raw, foreign) {
		t.Fatalf("foreign record changed (same inode %v, bytes %q, %v %v)", statErr == nil && os.SameFile(before, after), raw, statErr, readErr)
	}
	jailerAssertRetained(t, m, jailerLaunchID)
}

// TestStartPreservesReplacedRecordAtCommit: an owned record replaced before
// the post-attach commit is preserved, and the reservation stays.
func TestStartPreservesReplacedRecordAtCommit(t *testing.T) {
	jailerNeedBinary(t)
	env := newJailerEnv(t)
	name := filepath.Join(env.cfg.StateDir, recordPath(jailerLaunchID))
	foreign := []byte("foreign replacement; do not replace")
	var before os.FileInfo
	env.stages.on = map[launchStage]func(){stageCommit: func() {
		tmp := name + ".foreign"
		if err := os.WriteFile(tmp, foreign, 0o600); err != nil {
			t.Error(err)
			return
		}
		if err := os.Rename(tmp, name); err != nil {
			t.Error(err)
			return
		}
		before, _ = os.Lstat(name)
	}}
	m := env.open(t)
	defer m.Close()
	child, owner, err := jailerStart(t, m, t.Context(), jailerSpec(t, jailerLaunchID))
	if child == nil {
		t.Fatalf("Start lost the child: %v", err)
	}
	if err == nil {
		t.Fatal("the commit overwrote a replaced record")
	}
	owner.waitKilled(t)
	after, statErr := os.Lstat(name)
	raw, _ := os.ReadFile(name)
	if statErr != nil || before == nil || !os.SameFile(before, after) || !bytes.Equal(raw, foreign) {
		t.Fatalf("replacement record changed: %q (%v)", raw, statErr)
	}
	jailerAssertRetained(t, m, jailerLaunchID)
	if err := child.Release(t.Context()); err == nil {
		t.Fatal("Release removed a replaced record")
	}
	if raw, _ := os.ReadFile(name); !bytes.Equal(raw, foreign) {
		t.Fatal("Release changed the replaced record")
	}
}

// TestStartJailOpFaults fails each jail builder operation right after it
// happened on disk. Recorded entries are rolled back; an entry created but
// not yet identified cannot be proven ours and is kept with the reservation.
func TestStartJailOpFaults(t *testing.T) {
	jailerNeedBinary(t)
	staged := "root" + jailerTestBinary
	for _, tt := range []struct {
		op       jailOp
		rel      string
		retained bool
	}{
		{op: opMkdirIdentity, rel: "root/proc", retained: true},
		{op: opCreateIdentity, rel: "root/serial.log", retained: true},
		{op: opCreateIdentity, rel: staged, retained: true},
		{op: opWrite, rel: staged},
		{op: opChmod, rel: staged},
		{op: opSync, rel: staged},
		{op: opClose, rel: staged},
		{op: opBarrier, rel: "root/usr/bin"},
		{op: opBarrier, rel: "."},
	} {
		t.Run(string(tt.op)+" "+tt.rel, func(t *testing.T) {
			env := newJailerEnv(t)
			var reached []string
			env.jailOp = func(op jailOp, rel string) error {
				reached = append(reached, string(op)+" "+rel)
				if op == tt.op && rel == tt.rel {
					return errJailerInjected
				}
				return nil
			}
			m := env.open(t)
			defer m.Close()
			child, _, err := jailerStart(t, m, t.Context(), jailerSpec(t, jailerLaunchID))
			if !errors.Is(err, errJailerInjected) || child != nil {
				t.Fatalf("Start = %v, %v; want the injected failure without a child", child, err)
			}
			if got := reached[len(reached)-1]; got != string(tt.op)+" "+tt.rel && tt.op != opBarrier {
				t.Fatalf("last jail op %q, want %s %s", got, tt.op, tt.rel)
			}
			if got := env.stages.last(); got != stageJailPopulate {
				t.Fatalf("last stage %q, want %q", got, stageJailPopulate)
			}
			if !tt.retained {
				jailerAssertReleased(t, env, m, jailerLaunchID)
				return
			}
			jailerAssertRetained(t, m, jailerLaunchID)
			if _, err := os.Lstat(filepath.Join(env.cfg.StateDir, jailsDir, jailerLaunchID, tt.rel)); err != nil {
				t.Fatalf("the unidentified entry was removed: %v", err)
			}
		})
	}
}

// TestStartJailForeignReplacement replaces a recorded staged file while the
// jail is populated: the rollback preserves the replacement and the
// reservation, and removes nothing it cannot prove.
func TestStartJailForeignReplacement(t *testing.T) {
	jailerNeedBinary(t)
	env := newJailerEnv(t)
	staged := "root" + jailerTestBinary
	foreign := []byte("foreign replacement")
	env.jailOp = func(op jailOp, rel string) error {
		if op != opChmod || rel != staged {
			return nil
		}
		p := filepath.Join(env.cfg.StateDir, jailsDir, jailerLaunchID, rel)
		if err := os.Rename(p, p+".moved"); err != nil {
			return err
		}
		if err := os.WriteFile(p, foreign, 0o600); err != nil {
			return err
		}
		return errJailerInjected
	}
	m := env.open(t)
	defer m.Close()
	if _, _, err := jailerStart(t, m, t.Context(), jailerSpec(t, jailerLaunchID)); !errors.Is(err, errJailerInjected) {
		t.Fatalf("Start = %v", err)
	}
	raw, err := os.ReadFile(filepath.Join(env.cfg.StateDir, jailsDir, jailerLaunchID, staged))
	if err != nil || !bytes.Equal(raw, foreign) {
		t.Fatalf("foreign replacement changed: %q, %v", raw, err)
	}
	jailerAssertRetained(t, m, jailerLaunchID)
}

// TestJailBuilderBarrierRetry: a failed parent fsync stays pending and a
// later barrier syncs exactly what is still owed.
func TestJailBuilderBarrierRetry(t *testing.T) {
	root, _, _ := jailerReviewRoots(t)
	base := filepath.Join(jailsDir, jailerReviewID)
	if err := root.Mkdir(base, 0o700); err != nil {
		t.Fatal(err)
	}
	var synced []string
	failOnce := true
	b := newJailBuilder(root, base, func(ownedEntry) {}, func(op jailOp, rel string) error {
		if op != opBarrier {
			return nil
		}
		if rel == "root/a" && failOnce {
			failOnce = false
			return errJailerInjected
		}
		synced = append(synced, rel)
		return nil
	})
	if err := b.mkdirAll("root/a/b"); err != nil {
		t.Fatal(err)
	}
	f, _, err := b.create("root/a/file", 0o600)
	if err != nil {
		t.Fatal(err)
	}
	f.Close()
	if err := b.barrier(); !errors.Is(err, errJailerInjected) {
		t.Fatalf("barrier = %v, want the injected failure", err)
	}
	if want := []string{"root", "."}; !slices.Equal(synced, want) {
		t.Fatalf("first barrier synced %v, want %v", synced, want)
	}
	synced = nil
	if err := b.barrier(); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(synced, []string{"root/a"}) {
		t.Fatalf("retry synced %v, want only the failed root/a", synced)
	}
	synced = nil
	if err := b.barrier(); err != nil || len(synced) != 0 {
		t.Fatalf("a complete barrier still synced %v (%v)", synced, err)
	}
}

// TestStateWriteRenameWindow inserts a record in the window between the
// temp write and the rename: the final-path proof right before rename keeps
// the foreign record, and the attempt leaves no temp behind.
func TestStateWriteRenameWindow(t *testing.T) {
	for _, tt := range []struct {
		name  string
		owned bool
	}{
		{name: "first write"},
		{name: "update", owned: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			root, _, stateDir := jailerReviewRoots(t)
			var expected inodeIdentity
			if tt.owned {
				var err error
				if expected, err = (fileStates{}).Write(root, jailerReviewRecord(jailerReviewID, 50000), resourceOwnership{}, inodeIdentity{}); err != nil {
					t.Fatal(err)
				}
			}
			name := filepath.Join(stateDir, recordPath(jailerReviewID))
			foreign := []byte("foreign record in the rename window")
			var before os.FileInfo
			store := fileStates{fault: func(stage stateFault) error {
				if stage != faultStateRename {
					return nil
				}
				if err := os.WriteFile(name+".x", foreign, 0o600); err != nil {
					return err
				}
				if err := os.Rename(name+".x", name); err != nil {
					return err
				}
				before, _ = os.Lstat(name)
				return nil
			}}
			identity, err := store.Write(root, jailerReviewRecord(jailerReviewID, 50000), resourceOwnership{}, expected)
			if err == nil || identity.valid() {
				t.Fatalf("Write = %v, %v; want a refusal", identity, err)
			}
			after, statErr := os.Lstat(name)
			raw, _ := os.ReadFile(name)
			if statErr != nil || !os.SameFile(before, after) || !bytes.Equal(raw, foreign) {
				t.Fatalf("foreign record changed: %q (%v)", raw, statErr)
			}
			entries, _ := os.ReadDir(filepath.Join(stateDir, recordsDir))
			for _, e := range entries {
				if strings.Contains(e.Name(), ".tmp-") {
					t.Fatalf("attempt temp %s left behind", e.Name())
				}
			}
		})
	}
}

// TestStartRejectsTAPFlags reads the live TAP's flags through TUNGETIFF on
// the caller's descriptor (the platform seam stands in for the ioctl): only
// an attached IFF_TAP|IFF_NO_PI|IFF_VNET_HDR device is accepted.
func TestStartRejectsTAPFlags(t *testing.T) {
	jailerNeedBinary(t)
	for _, tt := range []struct {
		name  string
		flags uint16
	}{
		{"tun mode", unix.IFF_TUN | unix.IFF_NO_PI | unix.IFF_VNET_HDR},
		{"tun and tap bits", unix.IFF_TUN | unix.IFF_TAP | unix.IFF_NO_PI | unix.IFF_VNET_HDR},
		{"no vnet header", unix.IFF_TAP | unix.IFF_NO_PI},
		{"packet info", unix.IFF_TAP | unix.IFF_VNET_HDR},
		{"not attached", 0},
	} {
		t.Run(tt.name, func(t *testing.T) {
			env := newJailerEnv(t)
			env.tapFlags = tt.flags
			m := env.open(t)
			defer m.Close()
			_, _, err := jailerStart(t, m, t.Context(), jailerSpec(t, jailerLaunchID))
			if err == nil || !strings.Contains(err.Error(), "IFF_TAP|IFF_NO_PI|IFF_VNET_HDR") {
				t.Fatalf("Start = %v, want a TAP flag refusal", err)
			}
			if len(env.chowned) != 0 {
				t.Fatalf("files were chowned before the refusal: %v", env.chowned)
			}
			if _, ok := m.Record(jailerLaunchID); ok {
				t.Fatal("a refused spec left a reservation")
			}
		})
	}
}
