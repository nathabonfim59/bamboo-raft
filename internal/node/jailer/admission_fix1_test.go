package jailer

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"

	"golang.org/x/sys/unix"
)

// A10 slice4 checkpoint review: the admission journal reuses the accepted
// record publish/remove ownership rules, cleanup proves every entry before
// the first mutation, and the helper's process, namespaces and report are
// proven rather than assumed.

// jailerAdmitFaults fails the nth occurrence of stage (1-based) at the
// admission journal and cleanup boundaries, after running on (if set) at
// that occurrence.
type jailerAdmitFaults struct {
	mu    sync.Mutex
	seen  map[stateFault]int
	stage stateFault
	nth   int
	err   error
	on    func()
}

func (f *jailerAdmitFaults) hook(stage stateFault) error {
	f.mu.Lock()
	if f.seen == nil {
		f.seen = map[stateFault]int{}
	}
	f.seen[stage]++
	hit := stage == f.stage && f.seen[stage] == f.nth
	on := f.on
	f.mu.Unlock()
	if !hit {
		return nil
	}
	if on != nil {
		on()
	}
	return f.err
}

func TestAdmitJournalTempsAreAttemptOwned(t *testing.T) {
	env, m := jailerAdmitEnv(t, jailerAdmitOK)
	spec := jailerAdmitSpec()
	dir := filepath.Join(env.cfg.StateDir, admissionDir)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	// A deterministic name an older writer used and a foreign temp named
	// after this very attempt: neither may be unlinked or adopted.
	for _, name := range []string{".tmp-" + spec.Attempt, spec.Attempt + ".json.tmp-0000"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("foreign "+name), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	before := jailerAdmitTree(t, dir)
	report, err := m.Admit(t.Context(), spec)
	if err != nil {
		t.Fatal(err)
	}
	if after := jailerAdmitTree(t, dir); !maps.Equal(before, after) {
		t.Fatalf("admission changed foreign temporaries\nbefore %v\nafter  %v", before, after)
	}
	if len(report.Preserved) != 2 {
		t.Fatalf("report %+v, want both foreign temporaries preserved", report)
	}
}

func TestAdmitReplacedJournalIsPreserved(t *testing.T) {
	env, m := jailerAdmitEnv(t, jailerAdmitOK)
	spec := jailerAdmitSpec()
	journal := filepath.Join(env.cfg.StateDir, admissionDir, spec.Attempt+".json")
	faults := &jailerAdmitFaults{stage: faultStateRename, nth: 2, on: func() {
		// A foreign writer replaces the published journal between this
		// attempt's first and second publish.
		if err := os.Rename(journal, journal+".moved"); err != nil {
			t.Error(err)
		}
		if err := os.WriteFile(journal, []byte("foreign journal"), 0o600); err != nil {
			t.Error(err)
		}
	}}
	m.hooks.admitState = faults.hook
	if _, err := m.Admit(t.Context(), spec); err == nil {
		t.Fatal("Admit succeeded over a replaced journal")
	}
	raw, err := os.ReadFile(journal)
	if err != nil || string(raw) != "foreign journal" {
		t.Fatalf("the foreign journal was not preserved byte-for-byte: %q %v", raw, err)
	}
}

func TestAdmitJournalFaultsAndRestart(t *testing.T) {
	injected := errors.New("injected journal fault")
	cases := []struct {
		name  string
		stage stateFault
		nth   int
		// retained: the failed attempt keeps its journal (by name).
		retained bool
		// retryFails: a restart's admission must fail loudly.
		retryFails bool
	}{
		{"create of the intent", faultStateCreate, 1, false, false},
		{"write of the intent", faultStateWrite, 1, false, false},
		{"sync of the intent", faultStateSync, 1, false, false},
		{"rename of the intent (pre-publish uncertainty)", faultStateRename, 1, false, false},
		{"parent sync of the intent (post-publish uncertainty)", faultStateParentSync, 1, false, false},
		{"rename of the cgroup identity", faultStateRename, 2, true, true},
		{"journal removal", faultStateRemove, 1, true, false},
		{"barrier after journal removal", faultStateRemoveSync, 1, false, false},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			env, m := jailerAdmitEnv(t, jailerAdmitOK)
			m.hooks.admitState = (&jailerAdmitFaults{stage: tt.stage, nth: tt.nth, err: injected}).hook
			first := jailerAdmitSpec()
			if _, err := m.Admit(t.Context(), first); !errors.Is(err, injected) {
				t.Fatalf("Admit = %v, want the injected fault", err)
			}
			if j, _, _ := jailerAdmitLeft(t, env, first.Attempt); j != tt.retained {
				t.Fatalf("journal retained %v, want %v", j, tt.retained)
			}
			dir := filepath.Join(env.cfg.StateDir, admissionDir)
			before := jailerAdmitTree(t, dir)
			m = jailerAdmitReopen(t, env, m)
			report, err := m.Admit(t.Context(), jailerAdmitSpec())
			if tt.retryFails {
				if err == nil {
					t.Fatal("a restart admitted past a journal whose resources are unproven")
				}
				if after := jailerAdmitTree(t, dir); !maps.Equal(before, after) {
					t.Fatal("a refused restart changed the retained attempt")
				}
				if !env.cgroups.has(admissionID(first.Attempt)) {
					t.Fatal("a refused restart removed the retained cgroup")
				}
				return
			}
			if err != nil {
				t.Fatalf("restart: %v", err)
			}
			if tt.retained && !slices.Contains(report.Removed, first.Attempt) {
				t.Fatalf("restart report %+v does not settle the retained attempt", report)
			}
			if j, s, c := jailerAdmitLeft(t, env, first.Attempt); j || s || c {
				t.Fatalf("after restart journal %v scratch %v cgroup %v", j, s, c)
			}
		})
	}
}

func TestAdmitCleanupProvesEveryEntryFirst(t *testing.T) {
	env, m := jailerAdmitEnv(t, jailerAdmitOK)
	stale := jailerAttempt()
	// The foreign entry is created before mnt, so mnt (ours) comes first in
	// tmpfs directory order.
	jailerStaleAdmissionWith(t, env, stale, jailerStaleOpts{null: true, before: func(scratch string) {
		if err := os.WriteFile(filepath.Join(scratch, "zz-foreign"), []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
	}})
	dir := filepath.Join(env.cfg.StateDir, admissionDir)
	before := jailerAdmitTree(t, dir)
	if _, err := m.Admit(t.Context(), jailerAdmitSpec()); err == nil {
		t.Fatal("Admit settled a journaled attempt holding a foreign entry")
	}
	if after := jailerAdmitTree(t, dir); !maps.Equal(before, after) {
		t.Fatalf("a refused cleanup mutated entries\nbefore %v\nafter  %v", before, after)
	}
}

func TestAdmitReplacedNullIsPreserved(t *testing.T) {
	env, m := jailerAdmitEnv(t, jailerAdmitOK)
	stale := jailerAttempt()
	jailerStaleAdmissionWith(t, env, stale, jailerStaleOpts{null: true})
	null := filepath.Join(env.cfg.StateDir, admissionDir, stale+".d", admissionNullName)
	// Same type, same owner, same name: only the inode differs. The original
	// moves out of the scratch directory (kept alive, so its inode number is
	// not reused), leaving the identity the only difference.
	if err := os.Rename(null, filepath.Join(env.cfg.StateDir, admissionDir, "moved-null")); err != nil {
		t.Fatal(err)
	}
	if err := unix.Mkfifo(null, 0o600); err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(env.cfg.StateDir, admissionDir)
	before := jailerAdmitTree(t, dir)
	if _, err := m.Admit(t.Context(), jailerAdmitSpec()); err == nil {
		t.Fatal("Admit settled a journaled attempt whose null was replaced")
	}
	if after := jailerAdmitTree(t, dir); !maps.Equal(before, after) {
		t.Fatalf("a replaced null's attempt was mutated\nbefore %v\nafter  %v", before, after)
	}
}

func TestAdmitStaleCleanupBarrierRetry(t *testing.T) {
	injected := errors.New("injected barrier fault")
	cases := []struct {
		name  string
		stage stateFault
		group bool
	}{
		{"scratch sync", faultAdmitScratchSync, false},
		{"admission dir sync", faultAdmitDirSync, false},
		{"cgroup removal", "", true},
		{"journal removal", faultStateRemove, false},
		{"barrier after journal removal", faultStateRemoveSync, false},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			env, m := jailerAdmitEnv(t, jailerAdmitOK)
			stale := jailerAttempt()
			jailerStaleAdmissionWith(t, env, stale, jailerStaleOpts{null: true})
			if tt.group {
				env.cgroups.fail["remove"] = injected
			} else {
				m.hooks.admitState = (&jailerAdmitFaults{stage: tt.stage, nth: 1, err: injected}).hook
			}
			if _, err := m.Admit(t.Context(), jailerAdmitSpec()); !errors.Is(err, injected) {
				t.Fatalf("Admit = %v, want the injected barrier fault", err)
			}
			delete(env.cgroups.fail, "remove")
			m = jailerAdmitReopen(t, env, m)
			report, err := m.Admit(t.Context(), jailerAdmitSpec())
			if err != nil {
				t.Fatalf("retry: %v", err)
			}
			journalGone := tt.stage == faultStateRemoveSync
			if !journalGone && !slices.Contains(report.Removed, stale) {
				t.Fatalf("retry report %+v does not settle the stale attempt", report)
			}
			if j, s, c := jailerAdmitLeft(t, env, stale); j || s || c {
				t.Fatalf("after retry journal %v scratch %v cgroup %v", j, s, c)
			}
		})
	}
}

func TestAdmitCurrentCleanupFailureIsNotSuccess(t *testing.T) {
	env, m := jailerAdmitEnv(t, jailerAdmitOK)
	env.cgroups.fail["remove"] = errors.New("injected cgroup removal failure")
	spec := jailerAdmitSpec()
	if _, err := m.Admit(t.Context(), spec); err == nil {
		t.Fatal("Admit reported success although its own cleanup failed")
	}
	if j, _, c := jailerAdmitLeft(t, env, spec.Attempt); !j || !c {
		t.Fatalf("an incomplete cleanup dropped journal %v or cgroup %v", j, c)
	}
}

func TestAdmitUnknownStaleGroupProofIsFatal(t *testing.T) {
	env, m := jailerAdmitEnv(t, jailerAdmitOK)
	stale := jailerAttempt()
	jailerStaleAdmission(t, env, stale, 0)
	// The group under the journaled name is no longer the journaled inode:
	// its emptiness cannot be proven.
	env.cgroups.mu.Lock()
	env.cgroups.groups[admissionID(stale)].identity.Ino += 1000
	env.cgroups.mu.Unlock()
	dir := filepath.Join(env.cfg.StateDir, admissionDir)
	before := jailerAdmitTree(t, dir)
	if _, err := m.Admit(t.Context(), jailerAdmitSpec()); err == nil {
		t.Fatal("Admit continued past a journaled attempt whose group is unproven")
	}
	if after := jailerAdmitTree(t, dir); !maps.Equal(before, after) || !env.cgroups.has(admissionID(stale)) {
		t.Fatal("an unproven stale attempt was changed")
	}
}

func TestAdmitRequiresClonePidfd(t *testing.T) {
	env, m := jailerAdmitEnv(t, jailerAdmitOK)
	command := m.hooks.command
	m.hooks.command = func(cmd *exec.Cmd) {
		command(cmd)
		cmd.SysProcAttr.PidFD = nil // clone3 without CLONE_PIDFD
	}
	opened := 0
	m.hooks.pidfdOpen = func(pid int) (int, error) {
		opened++
		return unix.PidfdOpen(pid, 0)
	}
	spec := jailerAdmitSpec()
	if _, err := m.Admit(t.Context(), spec); err == nil || !strings.Contains(err.Error(), "pidfd") {
		t.Fatalf("Admit = %v, want the missing clone pidfd refused", err)
	}
	if opened != 0 {
		t.Fatal("admission fell back to pidfd_open")
	}
	if j, s, c := jailerAdmitLeft(t, env, spec.Attempt); j || s || c {
		t.Fatalf("a reaped refused helper left journal %v scratch %v cgroup %v", j, s, c)
	}
}

func TestAdmitRequiresFreshNamespaces(t *testing.T) {
	env, m := jailerAdmitEnv(t, jailerAdmitOK)
	m.hooks.proc = nil // the unprivileged fake shares every namespace
	spec := jailerAdmitSpec()
	if _, err := m.Admit(t.Context(), spec); err == nil || !strings.Contains(err.Error(), "namespace") {
		t.Fatalf("Admit = %v, want shared namespaces refused", err)
	}
	if j, s, c := jailerAdmitLeft(t, env, spec.Attempt); j || s || c {
		t.Fatalf("a reaped refused helper left journal %v scratch %v cgroup %v", j, s, c)
	}
}

func TestAdmitReportMustBeComplete(t *testing.T) {
	for _, mode := range []string{jailerAdmitSilent, jailerAdmitTruncated} {
		t.Run(mode, func(t *testing.T) {
			env, m := jailerAdmitEnv(t, mode)
			spec := jailerAdmitSpec()
			if _, err := m.Admit(t.Context(), spec); err == nil {
				t.Fatal("Admit accepted an exit 0 without a complete report")
			}
			if j, s, c := jailerAdmitLeft(t, env, spec.Attempt); j || s || c {
				t.Fatalf("a reaped helper left journal %v scratch %v cgroup %v", j, s, c)
			}
		})
	}
}

func TestAdmitCanceledAfterHelperIsFailure(t *testing.T) {
	cases := map[string]func(m *Manager, cancel context.CancelFunc){
		// The context ends while the sole waiter observes the reap.
		"during the reap": func(m *Manager, cancel context.CancelFunc) {
			m.hooks.admitReaped = func(*processHandle) { cancel() }
		},
		// The context ends after the complete report, while the reported
		// null node is journaled (the sixth journal publish).
		"after the report": func(m *Manager, cancel context.CancelFunc) {
			m.hooks.admitState = (&jailerAdmitFaults{stage: faultStateCreate, nth: 6, on: cancel}).hook
		},
	}
	for name, setup := range cases {
		t.Run(name, func(t *testing.T) {
			env, m := jailerAdmitEnv(t, jailerAdmitOK)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			setup(m, cancel)
			spec := jailerAdmitSpec()
			if _, err := m.Admit(ctx, spec); !errors.Is(err, context.Canceled) {
				t.Fatalf("Admit = %v, want Canceled after the helper completed", err)
			}
			if j, s, c := jailerAdmitLeft(t, env, spec.Attempt); j || s || c {
				t.Fatalf("a canceled settled admission left journal %v scratch %v cgroup %v", j, s, c)
			}
		})
	}
}

func TestAdmitMountProof(t *testing.T) {
	const target = "/state/admission/x.d/mnt"
	line := func(opts, optional string) string {
		return fmt.Sprintf("41 30 0:52 / %s %s %s- tmpfs tmpfs rw\n", target, opts, optional)
	}
	cases := map[string]struct {
		info string
		ok   bool
	}{
		"private and restricted":     {line("rw,nosuid,nodev,noexec,relatime", ""), true},
		"shared":                     {line("rw,nosuid,nodev,noexec", "shared:7 "), false},
		"slave":                      {line("rw,nosuid,nodev,noexec", "master:3 "), false},
		"missing noexec":             {line("rw,nosuid,nodev", ""), false},
		"not mounted":                {"41 30 0:52 / /elsewhere rw,nosuid,nodev,noexec - tmpfs tmpfs rw\n", false},
		"overmounted by a later one": {line("rw,nosuid,nodev,noexec", "") + line("rw", ""), false},
	}
	for name, tt := range cases {
		t.Run(name, func(t *testing.T) {
			err := checkAdmissionMount([]byte(tt.info), target)
			if (err == nil) != tt.ok {
				t.Fatalf("checkAdmissionMount = %v, want ok %v", err, tt.ok)
			}
		})
	}
}
