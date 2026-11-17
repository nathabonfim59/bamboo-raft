package jailer

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// A10 core fix round 1 regressions, adapted from the lead's independent
// review (lead_review_test.go): VerifyProcess and reclaim honor
// cancellation, caller resources never alias across records in any role,
// and the read-only root proof follows mount topology, not line order.

func jailerStatFile(t *testing.T, f *os.File) os.FileInfo {
	t.Helper()
	info, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	return info
}

func jailerCount(args []string, value string) int {
	n := 0
	for _, arg := range args {
		if arg == value {
			n++
		}
	}
	return n
}

// jailerCancelAfterProc cancels ctx right after it serves the named read.
type jailerCancelAfterProc struct {
	procFS
	after  string
	cancel context.CancelFunc
}

func (p jailerCancelAfterProc) ReadFile(pid, name string) ([]byte, error) {
	raw, err := p.procFS.ReadFile(pid, name)
	if name == p.after {
		p.cancel()
	}
	return raw, err
}

func (p jailerCancelAfterProc) Stat(pid, name string) (inodeIdentity, error) {
	identity, err := p.procFS.Stat(pid, name)
	if name == p.after {
		p.cancel()
	}
	return identity, err
}

// jailerConfinedFixture starts a live child and returns its record and a
// proc view that is valid in every respect.
func jailerConfinedFixture(t *testing.T) (*jailerEnv, *Manager, Record, func() *jailerFakeProc) {
	t.Helper()
	jailerNeedBinary(t)
	env := newJailerEnv(t)
	m := env.open(t)
	t.Cleanup(func() { m.Close() })
	child, _, err := jailerStart(t, m, t.Context(), jailerSpec(t, jailerLaunchID))
	if err != nil {
		t.Fatal(err)
	}
	record := child.Identity()
	own := m.ownership[jailerLaunchID]
	root, _ := ownedEntryAt(own, "root")
	exe, _ := ownedEntryAt(own, own.Exec)
	pid := strconv.Itoa(record.PID)
	good := func() *jailerFakeProc {
		return &jailerFakeProc{
			status:    jailerConfinedStatus(record.UID),
			mountinfo: "40 1 8:1 /jail/root / ro,nosuid - ext4 /dev/example rw\n",
			root:      root.identity(),
			exe:       exe.identity(),
			ns: map[string]inodeIdentity{
				pid + "/ns/mnt": {Dev: 4, Ino: 1}, pid + "/ns/pid": {Dev: 4, Ino: 2}, pid + "/ns/ipc": {Dev: 4, Ino: 3},
				"self/ns/mnt": {Dev: 4, Ino: 11}, "self/ns/pid": {Dev: 4, Ino: 12}, "self/ns/ipc": {Dev: 4, Ino: 13},
			},
		}
	}
	return env, m, record, good
}

// TestJailerVerifyHonorsCancellationThroughoutProof: a context canceled
// after any proof read is never success.
func TestJailerVerifyHonorsCancellationThroughoutProof(t *testing.T) {
	env, m, record, good := jailerConfinedFixture(t)
	m.hooks.proc = good()
	if err := m.VerifyProcess(t.Context(), record); err != nil {
		t.Fatalf("valid control: %v", err)
	}
	for _, after := range []string{"stat", "status", "ns/mnt", "root", "mountinfo", "exe"} {
		t.Run(after, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			m.hooks.proc = jailerCancelAfterProc{procFS: good(), after: after, cancel: cancel}
			if err := m.VerifyProcess(ctx, record); !errors.Is(err, context.Canceled) {
				t.Fatalf("VerifyProcess canceled after %s = %v, want Canceled", after, err)
			}
			if current, ok := m.Record(jailerLaunchID); !ok || current != record || len(env.reclaims()) != 0 {
				t.Fatal("a canceled verification changed the reservation")
			}
		})
	}
}

// TestJailerVerifyStartTicksChangeDuringProof: with the expected record
// unchanged, a pid whose start ticks differ before or after the other reads
// is a different process.
func TestJailerVerifyStartTicksChangeDuringProof(t *testing.T) {
	_, m, record, good := jailerConfinedFixture(t)
	changed := func(raw []byte) []byte {
		_, rest, _ := strings.Cut(string(raw), ") ")
		fields := strings.Fields(rest)
		fields[19] = strconv.FormatUint(record.StartTicks+1, 10)
		return []byte(strconv.Itoa(record.PID) + " (sleep) " + strings.Join(fields, " "))
	}
	for name, at := range map[string]int{"before": 0, "after": 1} {
		t.Run(name, func(t *testing.T) {
			proc := good()
			proc.stat = func(read int, raw []byte) []byte {
				if read == at {
					return changed(raw)
				}
				return raw
			}
			m.hooks.proc = proc
			err := m.VerifyProcess(t.Context(), record)
			if err == nil || !strings.Contains(err.Error(), "not the recorded live process "+name) {
				t.Fatalf("VerifyProcess = %v, want a %s tick mismatch", err, name)
			}
			if proc.statReads < at+1 {
				t.Fatalf("only %d stat reads", proc.statReads)
			}
		})
	}
}

// TestJailerVerifyStatusFieldControls: every required status field has an
// independent bad value and a missing-field case.
func TestJailerVerifyStatusFieldControls(t *testing.T) {
	_, m, record, good := jailerConfinedFixture(t)
	id := strconv.FormatUint(uint64(record.UID), 10)
	other := strconv.FormatUint(uint64(record.UID)+7, 10)
	bad := map[string]string{
		"Uid":        "Uid:\t" + id + "\t" + other + "\t" + id + "\t" + id,
		"Gid":        "Gid:\t" + id + "\t" + id + "\t" + id + "\t" + other,
		"Groups":     "Groups:\t" + id,
		"CapInh":     "CapInh:\t0000000000000001",
		"CapPrm":     "CapPrm:\t0000000000200000",
		"CapEff":     "CapEff:\t0000000000200000",
		"CapBnd":     "CapBnd:\t000001ffffffffff",
		"CapAmb":     "CapAmb:\t0000000000001000",
		"NoNewPrivs": "NoNewPrivs:\t0",
		"Seccomp":    "Seccomp:\t1",
	}
	for field, line := range bad {
		for _, missing := range []bool{false, true} {
			name := field + " bad"
			if missing {
				name = field + " missing"
			}
			t.Run(name, func(t *testing.T) {
				var lines []string
				for l := range strings.SplitSeq(jailerConfinedStatus(record.UID), "\n") {
					key, _, _ := strings.Cut(l, ":")
					switch {
					case key != field:
						lines = append(lines, l)
					case !missing:
						lines = append(lines, line)
					}
				}
				proc := good()
				proc.status = strings.Join(lines, "\n")
				m.hooks.proc = proc
				if err := m.VerifyProcess(t.Context(), record); err == nil {
					t.Fatalf("VerifyProcess accepted %s", name)
				}
			})
		}
	}
	t.Run("shared ipc namespace", func(t *testing.T) {
		proc := good()
		pid := strconv.Itoa(record.PID)
		proc.ns[pid+"/ns/ipc"] = proc.ns["self/ns/ipc"]
		m.hooks.proc = proc
		if err := m.VerifyProcess(t.Context(), record); err == nil || !strings.Contains(err.Error(), "ns/ipc") {
			t.Fatalf("VerifyProcess = %v, want an ipc namespace refusal", err)
		}
	})
}

// TestJailerVerifyRefusesReplacedOwnedCgroup: membership counts only in the
// recorded cgroup inode.
func TestJailerVerifyRefusesReplacedOwnedCgroup(t *testing.T) {
	env, m, record, good := jailerConfinedFixture(t)
	m.hooks.proc = good()
	env.cgroups.mu.Lock()
	group := env.cgroups.groups[jailerLaunchID]
	original := group.identity
	group.identity = inodeIdentity{Dev: original.Dev, Ino: original.Ino + 1000}
	env.cgroups.mu.Unlock()
	err := m.VerifyProcess(t.Context(), record)
	env.cgroups.mu.Lock()
	group.identity = original
	env.cgroups.mu.Unlock()
	if err == nil || !strings.Contains(err.Error(), "replaced") {
		t.Fatalf("VerifyProcess with a replaced cgroup = %v", err)
	}
	if err := m.VerifyProcess(t.Context(), record); err != nil {
		t.Fatalf("restored control: %v", err)
	}
}

// TestJailerReclaimCanceledAtBoundary: cancellation at the reclaim boundary
// stops before the policy runs and keeps the complete reservation; a live
// retry then releases.
func TestJailerReclaimCanceledAtBoundary(t *testing.T) {
	jailerNeedBinary(t)
	env := newJailerEnv(t)
	m := env.open(t)
	defer m.Close()
	child := jailerStartedDead(t, m, jailerSpec(t, jailerLaunchID))
	record := child.Identity()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	env.stages.mu.Lock()
	env.stages.on = map[launchStage]func(){stageReclaim: cancel}
	env.stages.mu.Unlock()
	if err := m.ReleaseAttempt(ctx, record.Ref()); !errors.Is(err, context.Canceled) {
		t.Fatalf("ReleaseAttempt = %v, want Canceled", err)
	}
	if n := len(env.reclaims()); n != 0 {
		t.Fatalf("reclaim policy ran %d times after its boundary was canceled", n)
	}
	if current, ok := m.Record(jailerLaunchID); !ok || current != record || !env.cgroups.has(jailerLaunchID) ||
		!jailerExists(t, env.cfg.StateDir, filepath.Join(jailsDir, jailerLaunchID)) {
		t.Fatal("a canceled release removed part of the reservation")
	}
	env.stages.mu.Lock()
	env.stages.on = nil
	env.stages.mu.Unlock()
	if err := m.ReleaseAttempt(t.Context(), record.Ref()); err != nil {
		t.Fatal(err)
	}
	if n := len(env.reclaims()); n != 1 {
		t.Fatalf("retry ran %d reclaims, want 1", n)
	}
}

// TestJailerCallerResourcesNeverAliasAcrossRecords: a new attempt's serial
// log, control directory or overlay may not be another record's overlay,
// log or control directory, in either direction; the refusal changes
// nothing.
func TestJailerCallerResourcesNeverAliasAcrossRecords(t *testing.T) {
	jailerNeedBinary(t)
	overlay := func(s LaunchSpec) *os.File { return s.Files[len(s.Files)-1].File }
	cases := []struct {
		name string
		edit func(first, second *LaunchSpec)
	}{
		{"new log is the old overlay", func(first, second *LaunchSpec) { second.SerialLog = overlay(*first) }},
		{"new overlay is the old log", func(first, second *LaunchSpec) {
			second.Files[len(second.Files)-1].File = first.SerialLog
		}},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			env := newJailerEnv(t)
			m := env.open(t)
			defer m.Close()
			first := jailerSpec(t, jailerLaunchID)
			if _, _, err := jailerStart(t, m, t.Context(), first); err != nil {
				t.Fatal(err)
			}
			kept, _ := m.Record(jailerLaunchID)
			second := jailerSpec(t, jailerReviewID)
			tt.edit(&first, &second)
			stages := len(env.stages.reached)
			env.mu.Lock()
			chowned := len(env.chowned)
			env.mu.Unlock()
			child, _, err := jailerStart(t, m, t.Context(), second)
			if child != nil || err == nil {
				t.Fatalf("aliasing Start = %v, %v; want a refusal", child, err)
			}
			if _, ok := m.Record(jailerReviewID); ok {
				t.Fatal("the refused attempt left a record")
			}
			if current, ok := m.Record(jailerLaunchID); !ok || current != kept {
				t.Fatal("the first reservation changed")
			}
			env.mu.Lock()
			defer env.mu.Unlock()
			if len(env.chowned) != chowned || len(env.stages.reached) != stages {
				t.Fatalf("the refused attempt chowned %v or reached %v", env.chowned[chowned:], env.stages.reached[stages:])
			}
		})
	}
}

func TestJailerInventoryRefusesCrossRoleAliases(t *testing.T) {
	uids := UIDRange{Min: 50000, Max: 50010}
	for name, edit := range map[string]func(a, b *Record){
		"log is another target": func(a, b *Record) { b.LogDev, b.LogIno = a.TargetDev, a.TargetIno },
		"target is another log": func(a, b *Record) { b.TargetDev, b.TargetIno = a.LogDev, a.LogIno },
		"control is another log": func(a, b *Record) {
			b.ControlDev, b.ControlIno = a.LogDev, a.LogIno
		},
		"own log is own target": func(_, b *Record) { b.LogDev, b.LogIno = b.TargetDev, b.TargetIno },
	} {
		t.Run(name, func(t *testing.T) {
			a := jailerReviewRecord(jailerReviewID, 50000)
			b := jailerReviewRecord(jailerReviewID2, 50001)
			edit(&a, &b)
			if err := validateInventory(map[string]Record{a.SandboxID: a, b.SandboxID: b}, uids); err == nil {
				t.Fatalf("validateInventory accepted %s", name)
			}
		})
	}
	a := jailerReviewRecord(jailerReviewID, 50000)
	b := jailerReviewRecord(jailerReviewID2, 50001)
	if err := validateInventory(map[string]Record{a.SandboxID: a, b.SandboxID: b}, uids); err != nil {
		t.Fatalf("distinct records: %v", err)
	}
}

// TestJailerRootProofFollowsMountTopology: the effective root is the mount
// at "/" that no other "/" mount is stacked on, whatever the line order.
func TestJailerRootProofFollowsMountTopology(t *testing.T) {
	rwTop := "20 10 8:1 /jail/root / rw - ext4 /dev/example rw\n"
	roUnder := "10 1 8:1 /jail/root / ro - ext4 /dev/example rw\n"
	roTop := "20 10 8:1 /jail/root / ro,nosuid - ext4 /dev/example rw\n"
	rwUnder := "10 1 8:1 /jail/root / rw - ext4 /dev/example rw\n"
	other := "30 20 0:5 / /proc rw,nosuid - proc proc rw\n"
	for _, tt := range []struct {
		name, raw string
		wantError bool
	}{
		{"writable top first", rwTop + roUnder, true},
		{"writable top last", roUnder + rwTop, true},
		{"readonly top first", roTop + rwUnder, false},
		{"readonly top last", rwUnder + roTop, false},
		{"readonly single", "40 1 8:1 /jail/root / ro - ext4 /dev/example rw\n" + other, false},
		{"writable single", "40 1 8:1 /jail/root / rw - ext4 /dev/example rw\n", true},
		{"no root mount", other, true},
		{"two unrelated tops", "20 1 8:1 / / ro - ext4 a rw\n21 1 8:1 / / ro - ext4 b rw\n", true},
		{"cycle", "20 21 8:1 / / ro - ext4 a rw\n21 20 8:1 / / ro - ext4 b rw\n", true},
		{"duplicate id", "20 1 8:1 / / ro - ext4 a rw\n20 1 8:1 / / ro - ext4 b rw\n", true},
		{"malformed ids", "x 1 8:1 / / ro - ext4 a rw\n", true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			err := checkReadOnlyRoot([]byte(tt.raw))
			if (err != nil) != tt.wantError {
				t.Fatalf("checkReadOnlyRoot = %v, wantError %v", err, tt.wantError)
			}
		})
	}
}
