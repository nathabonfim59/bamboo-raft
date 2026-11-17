package jailer

import (
	"context"
	"encoding/json/v2"
	"errors"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
)

// A10 regressions: attempt identity, the mandatory reclaim policy on every
// uid-freeing path, retained-record refusal, exact-attempt release, Detach,
// VerifyProcess and the fixed CPU probe.

// jailerStartedDead starts spec, kills the child and joins its sole Wait, so
// the reservation is retained for a proven-dead process.
func jailerStartedDead(t *testing.T, m *Manager, spec LaunchSpec) *Child {
	t.Helper()
	child, owner, err := jailerStart(t, m, t.Context(), spec)
	if err != nil {
		t.Fatal(err)
	}
	if err := child.Kill(); err != nil {
		t.Fatal(err)
	}
	owner.waitKilled(t)
	return child
}

func jailerRecordFileExists(t *testing.T, env *jailerEnv, id string) bool {
	t.Helper()
	_, err := os.Lstat(filepath.Join(env.cfg.StateDir, recordPath(id)))
	if errors.Is(err, fs.ErrNotExist) {
		return false
	}
	if err != nil {
		t.Fatal(err)
	}
	return true
}

func TestNewRequiresReclaimPolicy(t *testing.T) {
	env := newJailerEnv(t)
	cfg := env.cfg
	cfg.Reclaim = nil
	if m, err := New(cfg); err == nil || !strings.Contains(err.Error(), "reclaim policy") {
		if m != nil {
			m.Close()
		}
		t.Fatalf("New without a reclaim policy = %v", err)
	}
}

func TestStartRecordsAttemptKindAndOverlay(t *testing.T) {
	jailerNeedBinary(t)
	env := newJailerEnv(t)
	m := env.open(t)
	defer m.Close()
	spec := jailerSpec(t, jailerLaunchID)
	child, _, err := jailerStart(t, m, t.Context(), spec)
	if err != nil {
		t.Fatal(err)
	}
	record := child.Identity()
	overlay, err := fileInode(spec.descriptor(spec.Disks[len(spec.Disks)-1]))
	if err != nil {
		t.Fatal(err)
	}
	if record.Attempt != spec.Attempt || record.Kind != KindVM ||
		record.TargetDev != overlay.Dev || record.TargetIno != overlay.Ino {
		t.Fatalf("record = %+v, want attempt %s, kind vm, target %v", record, spec.Attempt, overlay)
	}
	if stored, ok := m.Record(jailerLaunchID); !ok || stored != record {
		t.Fatalf("stored record %+v differs from %+v", stored, record)
	}
	if own := m.ownership[jailerLaunchID]; own.Exec != "root/usr/bin/sleep" {
		t.Fatalf("staged exec = %q", own.Exec)
	}
}

// TestStartRefusesRetainedRecordWithoutRelease: a second attempt on an id
// with a retained (even dead) record is refused before any reservation or
// reclaim, so the old attempt's resources are never released without the
// caller's settlement.
func TestStartRefusesRetainedRecordWithoutRelease(t *testing.T) {
	jailerNeedBinary(t)
	env := newJailerEnv(t)
	m := env.open(t)
	defer m.Close()
	first := jailerStartedDead(t, m, jailerSpec(t, jailerLaunchID)).Identity()
	before := len(env.stages.reached)
	again := jailerSpec(t, jailerLaunchID)
	child, _, err := jailerStart(t, m, t.Context(), again)
	if child != nil || !errors.Is(err, ErrRetained) {
		t.Fatalf("Start over a retained record = %v, %v; want nil, ErrRetained", child, err)
	}
	if got := env.stages.reached[before:]; len(got) != 0 {
		t.Fatalf("refused Start reached stages %v", got)
	}
	if n := len(env.reclaims()); n != 0 {
		t.Fatalf("refused Start ran %d reclaims", n)
	}
	if current, ok := m.Record(jailerLaunchID); !ok || current != first {
		t.Fatalf("retained record changed: %+v, want %+v", current, first)
	}
}

func TestReleaseAttemptExactGeneration(t *testing.T) {
	jailerNeedBinary(t)
	env := newJailerEnv(t)
	m := env.open(t)
	defer m.Close()
	record := jailerStartedDead(t, m, jailerSpec(t, jailerLaunchID)).Identity()

	if err := m.ReleaseAttempt(t.Context(), Ref{SandboxID: jailerReviewID, Attempt: jailerAttempt()}); err != nil {
		t.Fatalf("absent id = %v, want nil", err)
	}
	other := Ref{SandboxID: jailerLaunchID, Attempt: jailerAttempt()}
	if err := m.ReleaseAttempt(t.Context(), other); !errors.Is(err, ErrSuperseded) {
		t.Fatalf("other attempt = %v, want ErrSuperseded", err)
	}
	if _, err := m.RecordFor(other); !errors.Is(err, ErrSuperseded) {
		t.Fatalf("RecordFor other attempt = %v", err)
	}
	if !jailerRecordFileExists(t, env, jailerLaunchID) || len(env.reclaims()) != 0 {
		t.Fatal("a superseded release touched the record or ran reclaim")
	}
	if err := m.Kill(t.Context(), other); !errors.Is(err, ErrSuperseded) {
		t.Fatalf("Kill of another attempt = %v, want ErrSuperseded", err)
	}
	if got, err := m.RecordFor(record.Ref()); err != nil || got != record {
		t.Fatalf("RecordFor = %+v, %v", got, err)
	}
	if err := m.ReleaseAttempt(t.Context(), record.Ref()); err != nil {
		t.Fatal(err)
	}
	if got := env.reclaims(); len(got) != 1 || got[0] != record {
		t.Fatalf("reclaims = %+v, want exactly the released record", got)
	}
	if _, ok := m.Record(jailerLaunchID); ok || jailerRecordFileExists(t, env, jailerLaunchID) {
		t.Fatal("record survived its release")
	}
	if _, err := m.RecordFor(record.Ref()); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("RecordFor after release = %v", err)
	}
}

// TestReclaimRunsBeforeRemovalOnEveryPath: every API that can free a uid
// runs the policy once, after the death proof and while the record, jail
// and cgroup still exist.
func TestReclaimRunsBeforeRemovalOnEveryPath(t *testing.T) {
	jailerNeedBinary(t)
	paths := []struct {
		name    string
		release func(t *testing.T, m *Manager, env *jailerEnv)
	}{
		{"Child.Release", func(t *testing.T, m *Manager, env *jailerEnv) {
			child := jailerStartedDead(t, m, jailerSpec(t, jailerLaunchID))
			if err := child.Release(t.Context()); err != nil {
				t.Fatal(err)
			}
		}},
		{"ReleaseAttempt", func(t *testing.T, m *Manager, env *jailerEnv) {
			record := jailerStartedDead(t, m, jailerSpec(t, jailerLaunchID)).Identity()
			if err := m.ReleaseAttempt(t.Context(), record.Ref()); err != nil {
				t.Fatal(err)
			}
		}},
		{"Release", func(t *testing.T, m *Manager, env *jailerEnv) {
			jailerStartedDead(t, m, jailerSpec(t, jailerLaunchID))
			if err := m.Release(t.Context(), jailerLaunchID); err != nil {
				t.Fatal(err)
			}
		}},
		{"Reconcile", func(t *testing.T, m *Manager, env *jailerEnv) {
			jailerStartedDead(t, m, jailerSpec(t, jailerLaunchID))
			report, err := m.Reconcile(t.Context())
			if err != nil || !slices.Equal(report.Released, []string{jailerLaunchID}) {
				t.Fatalf("Reconcile = %+v, %v", report, err)
			}
		}},
		{"abandoned prespawn", func(t *testing.T, m *Manager, env *jailerEnv) {
			env.stages.failAt, env.stages.err = stageSpawn, errors.New("injected")
			if child, err := m.Start(t.Context(), jailerSpec(t, jailerLaunchID)); child != nil || err == nil {
				t.Fatalf("Start = %v, %v; want a pre-spawn failure", child, err)
			}
		}},
	}
	for _, tt := range paths {
		t.Run(tt.name, func(t *testing.T) {
			env := newJailerEnv(t)
			var witnessed bool
			env.onReclaim = func(_ context.Context, record Record) error {
				// The record, jail tree and cgroup are still present: the
				// uid cannot have been offered yet.
				witnessed = jailerRecordFileExists(t, env, record.SandboxID) &&
					jailerExists(t, env.cfg.StateDir, filepath.Join(jailsDir, record.SandboxID)) &&
					env.cgroups.has(record.SandboxID)
				return nil
			}
			m := env.open(t)
			defer m.Close()
			tt.release(t, m, env)
			if got := env.reclaims(); len(got) != 1 || got[0].SandboxID != jailerLaunchID {
				t.Fatalf("reclaims = %+v, want one for %s", got, jailerLaunchID)
			}
			if !witnessed {
				t.Fatal("reclaim ran after a removal")
			}
			if _, ok := m.Record(jailerLaunchID); ok || env.cgroups.has(jailerLaunchID) {
				t.Fatal("reservation survived a successful reclaim")
			}
		})
	}
}

// TestReclaimFailureRetainsReservation: a failing policy keeps the record,
// jail, cgroup and uid on every path; a later successful policy releases.
func TestReclaimFailureRetainsReservation(t *testing.T) {
	jailerNeedBinary(t)
	env := newJailerEnv(t)
	reclaimErr := errors.New("injected reclaim failure")
	env.onReclaim = func(context.Context, Record) error { return reclaimErr }
	m := env.open(t)
	defer m.Close()
	child := jailerStartedDead(t, m, jailerSpec(t, jailerLaunchID))
	record := child.Identity()
	for name, release := range map[string]func() error{
		"Child.Release":  func() error { return child.Release(t.Context()) },
		"ReleaseAttempt": func() error { return m.ReleaseAttempt(t.Context(), record.Ref()) },
		"Release":        func() error { return m.Release(t.Context(), jailerLaunchID) },
		"Reconcile": func() error {
			_, err := m.Reconcile(t.Context())
			return err
		},
	} {
		if err := release(); !errors.Is(err, reclaimErr) {
			t.Fatalf("%s = %v, want the reclaim error", name, err)
		}
		if current, ok := m.Record(jailerLaunchID); !ok || current != record ||
			!env.cgroups.has(jailerLaunchID) ||
			!jailerExists(t, env.cfg.StateDir, filepath.Join(jailsDir, jailerLaunchID)) {
			t.Fatalf("%s released part of the reservation", name)
		}
	}
	// The retained uid is not offered to another sandbox.
	other, _, err := jailerStart(t, m, t.Context(), jailerSpec(t, jailerReviewID))
	if err != nil {
		t.Fatal(err)
	}
	if other.Identity().UID == record.UID {
		t.Fatalf("uid %d was reused while its reclaim is unresolved", record.UID)
	}
	env.mu.Lock()
	env.onReclaim = nil
	env.mu.Unlock()
	if err := child.Release(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestChildDetachKeepsReservation(t *testing.T) {
	jailerNeedBinary(t)
	env := newJailerEnv(t)
	m := env.open(t)
	defer m.Close()
	child, owner, err := jailerStart(t, m, t.Context(), jailerSpec(t, jailerLaunchID))
	if err != nil {
		t.Fatal(err)
	}
	record := child.Identity()
	if err := child.Detach(); err != nil {
		t.Fatal(err)
	}
	if err := child.Kill(); !errors.Is(err, errHandleReleased) {
		t.Fatalf("Kill after Detach = %v", err)
	}
	if err := child.Release(t.Context()); !errors.Is(err, errHandleReleased) {
		t.Fatalf("Release after Detach = %v", err)
	}
	if current, ok := m.Record(jailerLaunchID); !ok || current != record || len(env.reclaims()) != 0 {
		t.Fatal("Detach changed the reservation")
	}
	// The durable owner still controls the attempt by identity.
	if err := m.Kill(t.Context(), record.Ref()); err != nil {
		t.Fatal(err)
	}
	owner.waitKilled(t)
	if err := m.ReleaseAttempt(t.Context(), record.Ref()); err != nil {
		t.Fatal(err)
	}
}

func TestChildReleaseOfSupersededRecordIsConflict(t *testing.T) {
	jailerNeedBinary(t)
	env := newJailerEnv(t)
	m := env.open(t)
	defer m.Close()
	child := jailerStartedDead(t, m, jailerSpec(t, jailerLaunchID))
	m.mu.Lock()
	replaced := m.records[jailerLaunchID]
	replaced.Attempt = jailerAttempt()
	m.records[jailerLaunchID] = replaced
	m.mu.Unlock()
	if err := child.Release(t.Context()); !errors.Is(err, ErrSuperseded) {
		t.Fatalf("Release of a superseded attempt = %v", err)
	}
	if current, ok := m.Record(jailerLaunchID); !ok || current != replaced || len(env.reclaims()) != 0 {
		t.Fatal("a superseded Child released or reclaimed the newer record")
	}
	m.mu.Lock()
	m.records[jailerLaunchID] = child.Identity()
	m.mu.Unlock()
}

func TestTargetInodeHasOneOwner(t *testing.T) {
	jailerNeedBinary(t)
	env := newJailerEnv(t)
	m := env.open(t)
	defer m.Close()
	spec := jailerSpec(t, jailerLaunchID)
	if _, _, err := jailerStart(t, m, t.Context(), spec); err != nil {
		t.Fatal(err)
	}
	alias := jailerSpec(t, jailerReviewID)
	overlay := spec.Files[len(spec.Files)-1]
	// Another pathname for the same inode: a hard link made from the fixture
	// pathname in its own directory, so it is on the same filesystem.
	link := filepath.Join(filepath.Dir(overlay.File.Name()), "alias.qcow2")
	if err := os.Link(overlay.File.Name(), link); err != nil {
		t.Fatalf("hard link the overlay fixture: %v", err)
	}
	aliasFile := jailerOpen(t, link, os.O_RDWR)
	if link == overlay.File.Name() {
		t.Fatal("alias pathname equals the original")
	}
	if a, b := jailerStatFile(t, aliasFile), jailerStatFile(t, overlay.File); !os.SameFile(a, b) {
		t.Fatal("alias is not the same inode as the overlay")
	}
	alias.Files[len(alias.Files)-1].File = aliasFile
	if _, _, err := jailerStart(t, m, t.Context(), alias); !errors.Is(err, ErrTargetBusy) {
		t.Fatalf("second owner of the overlay inode = %v, want ErrTargetBusy", err)
	}
}

func TestRecordValidateSchema2(t *testing.T) {
	valid := jailerReviewRecord(jailerReviewID, 50000)
	tests := []struct {
		name   string
		edit   func(*Record)
		reason string
	}{
		{"valid", func(*Record) {}, ""},
		{"schema 1", func(r *Record) { r.SchemaVersion = 1 }, "not migrated"},
		{"missing attempt", func(r *Record) { r.Attempt = "" }, "invalid attempt"},
		{"non canonical attempt", func(r *Record) { r.Attempt = strings.ToUpper(r.Attempt) }, "invalid attempt"},
		{"nil attempt", func(r *Record) { r.Attempt = "00000000-0000-0000-0000-000000000000" }, "invalid attempt"},
		{"unknown kind", func(r *Record) { r.Kind = "other" }, "record id"},
		{"vm without overlay", func(r *Record) { r.TargetIno = 0 }, "overlay identity"},
		{"probe id for vm", func(r *Record) { r.SandboxID = ProbeID }, "record id"},
		{"probe with target", func(r *Record) { r.Kind, r.SandboxID = KindProbe, ProbeID }, "names a target"},
		{"probe", func(r *Record) { r.Kind, r.SandboxID, r.TargetDev, r.TargetIno = KindProbe, ProbeID, 0, 0 }, ""},
		{"sandbox id for probe", func(r *Record) { r.Kind, r.TargetDev, r.TargetIno = KindProbe, 0, 0 }, "record id"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			record := valid
			tt.edit(&record)
			err := record.Validate()
			if tt.reason == "" {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.reason) {
				t.Fatalf("Validate = %v, want %q", err, tt.reason)
			}
		})
	}
}

func TestValidateInventoryRefusesSharedTarget(t *testing.T) {
	a := jailerReviewRecord(jailerReviewID, 50000)
	b := jailerReviewRecord(jailerReviewID2, 50001)
	b.TargetDev, b.TargetIno = a.TargetDev, a.TargetIno
	err := validateInventory(map[string]Record{a.SandboxID: a, b.SandboxID: b}, UIDRange{Min: 50000, Max: 50010})
	if err == nil || !strings.Contains(err.Error(), "share target identity") {
		t.Fatalf("validateInventory = %v", err)
	}
}

// jailerFakeProc serves a live child's real stat (so the start ticks are
// genuine) and fixed confinement files that each case edits. stat, when
// set, replaces the stat read; its argument counts the reads so far.
type jailerFakeProc struct {
	status, mountinfo string
	ns                map[string]inodeIdentity // "pid/ns/mnt" -> identity
	root, exe         inodeIdentity
	statErr           error
	stat              func(read int, real []byte) []byte
	statReads         int
}

func (p *jailerFakeProc) ReadFile(pid, name string) ([]byte, error) {
	switch name {
	case "stat":
		if p.statErr != nil {
			return nil, p.statErr
		}
		raw, err := hostProcFS{}.ReadFile(pid, name)
		if err == nil && p.stat != nil {
			raw = p.stat(p.statReads, raw)
		}
		p.statReads++
		return raw, err
	case "status":
		return []byte(p.status), nil
	case "mountinfo":
		return []byte(p.mountinfo), nil
	}
	return nil, fs.ErrNotExist
}

func (p *jailerFakeProc) Stat(pid, name string) (inodeIdentity, error) {
	switch name {
	case "root":
		return p.root, nil
	case "exe":
		return p.exe, nil
	}
	if identity, ok := p.ns[pid+"/"+name]; ok {
		return identity, nil
	}
	return inodeIdentity{}, fs.ErrNotExist
}

func jailerConfinedStatus(uid uint32) string {
	id := strconv.FormatUint(uint64(uid), 10)
	return "Name:\tqemu\nUid:\t" + id + "\t" + id + "\t" + id + "\t" + id +
		"\nGid:\t" + id + "\t" + id + "\t" + id + "\t" + id +
		"\nGroups:\t\nCapInh:\t0000000000000000\nCapPrm:\t0000000000000000\nCapEff:\t0000000000000000" +
		"\nCapBnd:\t0000000000000000\nCapAmb:\t0000000000000000\nNoNewPrivs:\t1\nSeccomp:\t2\n"
}

func TestVerifyProcess(t *testing.T) {
	jailerNeedBinary(t)
	env := newJailerEnv(t)
	m := env.open(t)
	defer m.Close()
	child, _, err := jailerStart(t, m, t.Context(), jailerSpec(t, jailerLaunchID))
	if err != nil {
		t.Fatal(err)
	}
	record := child.Identity()
	own := m.ownership[jailerLaunchID]
	rootEntry, _ := ownedEntryAt(own, "root")
	execEntry, _ := ownedEntryAt(own, own.Exec)
	pid := strconv.Itoa(record.PID)
	good := func() *jailerFakeProc {
		return &jailerFakeProc{
			status:    jailerConfinedStatus(record.UID),
			mountinfo: "1 0 8:1 / / rw - ext4 /dev/sda1 rw\n40 1 8:1 /jail/root / ro,nosuid - ext4 /dev/sda1 rw\n",
			ns: map[string]inodeIdentity{
				pid + "/ns/mnt": {Dev: 4, Ino: 1}, pid + "/ns/pid": {Dev: 4, Ino: 2}, pid + "/ns/ipc": {Dev: 4, Ino: 3},
				"self/ns/mnt": {Dev: 4, Ino: 11}, "self/ns/pid": {Dev: 4, Ino: 12}, "self/ns/ipc": {Dev: 4, Ino: 13},
			},
			root: rootEntry.identity(),
			exe:  execEntry.identity(),
		}
	}
	tests := []struct {
		name   string
		edit   func(*jailerFakeProc, *Record)
		reason string
	}{
		{"confined", func(*jailerFakeProc, *Record) {}, ""},
		{"uid", func(p *jailerFakeProc, _ *Record) { p.status = jailerConfinedStatus(record.UID + 1) }, "Uid"},
		{"groups", func(p *jailerFakeProc, _ *Record) {
			p.status = strings.Replace(p.status, "Groups:\t\n", "Groups:\t36\n", 1)
		}, "groups"},
		{"bounding caps", func(p *jailerFakeProc, _ *Record) {
			p.status = strings.Replace(p.status, "CapBnd:\t0000000000000000", "CapBnd:\t0000000000000400", 1)
		}, "CapBnd"},
		{"ambient caps", func(p *jailerFakeProc, _ *Record) {
			p.status = strings.Replace(p.status, "CapAmb:\t0000000000000000", "CapAmb:\t0000000000000001", 1)
		}, "CapAmb"},
		{"no_new_privs", func(p *jailerFakeProc, _ *Record) {
			p.status = strings.Replace(p.status, "NoNewPrivs:\t1", "NoNewPrivs:\t0", 1)
		}, "no_new_privs"},
		{"seccomp", func(p *jailerFakeProc, _ *Record) {
			p.status = strings.Replace(p.status, "Seccomp:\t2", "Seccomp:\t0", 1)
		}, "seccomp"},
		{"missing seccomp field", func(p *jailerFakeProc, _ *Record) {
			p.status = strings.Replace(p.status, "Seccomp:\t2\n", "", 1)
		}, "no Seccomp field"},
		{"shared mount ns", func(p *jailerFakeProc, _ *Record) { p.ns[pid+"/ns/mnt"] = p.ns["self/ns/mnt"] }, "ns/mnt"},
		{"shared pid ns", func(p *jailerFakeProc, _ *Record) { p.ns[pid+"/ns/pid"] = p.ns["self/ns/pid"] }, "ns/pid"},
		{"root", func(p *jailerFakeProc, _ *Record) { p.root = inodeIdentity{Dev: 9, Ino: 9} }, "jail root"},
		{"writable root", func(p *jailerFakeProc, _ *Record) {
			p.mountinfo = strings.Replace(p.mountinfo, "/ ro,nosuid", "/ rw,nosuid", 1)
		}, "read-only"},
		{"exe", func(p *jailerFakeProc, _ *Record) { p.exe = inodeIdentity{Dev: 9, Ino: 10} }, "staged binary"},
		{"start ticks", func(_ *jailerFakeProc, r *Record) { r.StartTicks++ }, "differs"},
		{"other attempt", func(_ *jailerFakeProc, r *Record) { r.Attempt = jailerAttempt() }, "another attempt"},
		{"dead", func(p *jailerFakeProc, _ *Record) { p.statErr = fs.ErrNotExist }, "read stat"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			proc := good()
			expected := record
			tt.edit(proc, &expected)
			m.hooks.proc = proc
			err := m.VerifyProcess(t.Context(), expected)
			if tt.reason == "" {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.reason) {
				t.Fatalf("VerifyProcess = %v, want %q", err, tt.reason)
			}
		})
	}
	t.Run("not a member of the owned cgroup", func(t *testing.T) {
		m.hooks.proc = good()
		env.cgroups.mu.Lock()
		env.cgroups.fail["attach"] = errors.New("pid is not a member")
		env.cgroups.mu.Unlock()
		defer func() {
			env.cgroups.mu.Lock()
			delete(env.cgroups.fail, "attach")
			env.cgroups.mu.Unlock()
		}()
		if err := m.VerifyProcess(t.Context(), record); err == nil || !strings.Contains(err.Error(), "not a member") {
			t.Fatalf("VerifyProcess = %v", err)
		}
	})
}

// jailerProbeSpec builds a probe on pinned scratch resources.
func jailerProbeSpec(t *testing.T) ProbeSpec {
	t.Helper()
	dir := t.TempDir()
	if err := os.Mkdir(filepath.Join(dir, "control"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "serial.log"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	return ProbeSpec{
		Attempt:    jailerAttempt(),
		Binary:     jailerTestBinary,
		CPU:        "Haswell-noTSX,+vmx,enforce",
		ControlDir: jailerOpen(t, filepath.Join(dir, "control"), os.O_RDONLY),
		SerialLog:  jailerOpen(t, filepath.Join(dir, "serial.log"), os.O_RDWR),
		Limits: Limits{
			CPUQuotaPercent: 100, MemoryBytes: 256 << 20, Pids: 64,
			IO: IOLimits{Device: BlockDevice{Major: 8, Minor: 0}, ReadBPS: 1 << 20},
		},
	}
}

func TestStartProbeFixedArgvAndRetention(t *testing.T) {
	jailerNeedBinary(t)
	env := newJailerEnv(t)
	m := env.open(t)
	defer m.Close()
	var args []string
	command := m.hooks.command
	m.hooks.command = func(cmd *exec.Cmd) {
		configFD := cmd.ExtraFiles[len(cmd.ExtraFiles)-5]
		var cfg helperConfig
		raw, err := io.ReadAll(configFD)
		if err == nil {
			err = json.Unmarshal(raw, &cfg)
		}
		if _, serr := configFD.Seek(0, io.SeekStart); err == nil {
			err = serr
		}
		if err != nil {
			t.Errorf("read helper config: %v", err)
		}
		args = cfg.Args
		command(cmd)
	}
	spec := jailerProbeSpec(t)
	child, err := m.StartProbe(t.Context(), spec)
	if child == nil {
		t.Fatalf("StartProbe = %v", err)
	}
	owner := jailerOwn(t, child)
	if err != nil {
		t.Fatal(err)
	}
	// An independent literal oracle: never the production probeArgs.
	want := []string{
		"-name", "guest=probe_cpu,process=probe_cpu",
		"-machine", "q35,accel=kvm",
		"-cpu", "Haswell-noTSX,+vmx,enforce",
		"-S",
		"-nodefaults", "-no-user-config", "-no-reboot",
		"-m", "128M",
		"-smp", "1",
		"-chardev", "file,id=serial0,path=/serial.log,append=on",
		"-serial", "chardev:serial0",
		"-qmp", "unix:/control/qmp.sock,server=on,wait=off",
		"-display", "none",
		"-sandbox", "on,obsolete=deny,elevateprivileges=deny,spawn=deny,resourcecontrol=deny",
	}
	if !slices.Equal(args, want) {
		t.Fatalf("probe argv = %q, want %q", args, want)
	}
	for option, count := range map[string]int{"-sandbox": 1, "-S": 1, "-cpu": 1, "-qmp": 1, "-chardev": 1} {
		if got := jailerCount(args, option); got != count {
			t.Fatalf("probe argv has %d %s, want %d", got, option, count)
		}
	}
	if !slices.Contains(args, "-S") || slices.Contains(args, "-kernel") || slices.Contains(args, "-netdev") {
		t.Fatalf("probe argv is not CPU-only and paused: %q", args)
	}
	record := child.Identity()
	if record.SandboxID != ProbeID || record.Kind != KindProbe || record.Attempt != spec.Attempt || record.TargetIno != 0 {
		t.Fatalf("probe record = %+v", record)
	}
	if again, err := m.StartProbe(t.Context(), jailerProbeSpec(t)); again != nil || !errors.Is(err, ErrRetained) {
		t.Fatalf("second probe = %v, %v; want ErrRetained", again, err)
	}
	if err := child.Kill(); err != nil {
		t.Fatal(err)
	}
	owner.waitKilled(t)
	if err := m.ReleaseAttempt(t.Context(), record.Ref()); err != nil {
		t.Fatal(err)
	}
	if got := env.reclaims(); len(got) != 1 || got[0] != record {
		t.Fatalf("probe reclaims = %+v", got)
	}
}

func TestStartProbeRefusesInvalidCPU(t *testing.T) {
	env := newJailerEnv(t)
	m := env.open(t)
	defer m.Close()
	for _, cpu := range []string{"", "help", "host,file=/etc/passwd", "a,,b", "{\"x\":1}"} {
		spec := jailerProbeSpec(t)
		spec.CPU = cpu
		if child, err := m.StartProbe(t.Context(), spec); child != nil || err == nil {
			t.Fatalf("StartProbe(cpu %q) = %v, %v", cpu, child, err)
		}
	}
	if len(m.Records()) != 0 {
		t.Fatal("an invalid probe reserved a uid")
	}
}

// TestValidateArgvIsStartGrammarWithDisabledROM: a recovered argv passes
// only the exact launch grammar, with the NIC option ROM disabled.
func TestValidateArgvIsStartGrammarWithDisabledROM(t *testing.T) {
	spec := jailerSpec(t, jailerLaunchID, jailerQCOW("", 0), jailerQCOW("/base.qcow2", 0))
	if err := ValidateArgv(jailerLaunchID, spec.Args, spec.Disks); err != nil {
		t.Fatalf("valid launch argv refused: %v", err)
	}
	noROM := slices.Clone(spec.Args)
	for i := range noROM {
		noROM[i] = strings.TrimSuffix(noROM[i], ",romfile=")
	}
	if err := ValidateArgv(jailerLaunchID, noROM, spec.Disks); err == nil {
		t.Fatal("an enabled NIC option ROM was accepted")
	}
	extra := append(slices.Clone(spec.Args), "-readconfig", "/etc/shadow")
	if err := ValidateArgv(jailerLaunchID, extra, spec.Disks); err == nil {
		t.Fatal("an unsupported option was accepted")
	}
	if err := ValidateArgv(jailerLaunchID, spec.Args, nil); err == nil {
		t.Fatal("an empty disk chain was accepted")
	}
}
