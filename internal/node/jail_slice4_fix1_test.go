package node

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/nathabonfim59/bamboo-raft/internal/node/jailer"
)

// A10 slice4 fix round 1: probe cancellation and private-handle lifetime,
// and reclaimer roots bound to the validated, durably created inode.

// TestProbeJailedCPUUnprovenExitKeepsOneHandleOwner: when a probe cannot be
// proven dead (post-start failure or graceful-stop failure), the error API
// detaches its private jailer handle exactly once and keeps the reservation
// and scratch; the sole Wait completes later through the owned process, and
// a later probe settles the retained attempt by identity.
func TestProbeJailedCPUUnprovenExitKeepsOneHandleOwner(t *testing.T) {
	cases := map[string]func(f *jailProbeFixture){
		"post-start failure": func(f *jailProbeFixture) {
			f.jail.probeErr = errors.New("injected post-spawn proof error")
		},
		// QMP proofs pass; quit, the grace and the kill all fail to end it.
		"graceful stop failure": func(*jailProbeFixture) {},
	}
	for name, setup := range cases {
		t.Run(name, func(t *testing.T) {
			f := newJailProbeFixture(t)
			f.opts.Grace = 20 * time.Millisecond
			f.jail.killNoop = true
			setup(f)
			if _, err := f.probe(t.Context()); err == nil {
				t.Fatal("probe succeeded with an unproven exit")
			}
			run := f.jail.lastRun()
			if waits, detaches, releases := run.counts(); waits > 1 || detaches != 1 || releases != 0 {
				t.Fatalf("waits %d detaches %d releases %d; want the private handle detached once and nothing released", waits, detaches, releases)
			}
			old, ok := f.jail.Record(jailer.ProbeID)
			if !ok {
				t.Fatal("the unproven probe's reservation was released")
			}
			scratch := f.probeDirEntries(t)
			if len(scratch) == 0 {
				t.Fatal("the unproven probe's scratch was removed")
			}
			// The test kills through the fake's own exec.Cmd (never a raw
			// pid) and joins the production sole Wait.
			if err := run.cmd.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
				t.Fatal(err)
			}
			run.joinReaped(t)
			if waits, detaches, releases := run.counts(); waits != 1 || detaches != 1 || releases != 0 {
				t.Fatalf("after the late reap: waits %d detaches %d releases %d", waits, detaches, releases)
			}
			if rec, ok := f.jail.Record(jailer.ProbeID); !ok || rec != old || !slices.Equal(f.probeDirEntries(t), scratch) {
				t.Fatal("the late reap changed the retained reservation or scratch")
			}
			// A later start settles the retained attempt by identity.
			f.jail.mu.Lock()
			f.jail.killNoop, f.jail.probeErr = false, nil
			f.jail.mu.Unlock()
			if _, err := f.probe(t.Context()); err != nil {
				t.Fatalf("retry: %v", err)
			}
			if _, ok := f.jail.Record(jailer.ProbeID); ok || len(f.probeDirEntries(t)) != 0 {
				t.Fatal("the retry left a reservation or scratch")
			}
		})
	}
}

// TestProbeJailedCPUCompletedProbeIgnoresLaterCancel: a probe that returned
// evidence is unaffected by its context ending afterwards.
func TestProbeJailedCPUCompletedProbeIgnoresLaterCancel(t *testing.T) {
	f := newJailProbeFixture(t)
	ctx, cancel := context.WithCancel(t.Context())
	ev, err := f.probe(ctx)
	cancel()
	if err != nil || ev.Status == "" {
		t.Fatalf("probe = %+v, %v", ev, err)
	}
	if _, ok := f.jail.Record(jailer.ProbeID); ok || len(f.probeDirEntries(t)) != 0 {
		t.Fatal("a completed probe left state")
	}
}

// jailRootOps records the reclaimer's directory barriers and fails or
// intercepts them on demand.
type jailRootOps struct {
	mu      sync.Mutex
	synced  []string
	failAt  int
	onBound func(path string)
}

func (o *jailRootOps) ops() reclaimRootOps {
	return reclaimRootOps{
		syncDir: func(f *os.File) error {
			o.mu.Lock()
			o.synced = append(o.synced, filepath.Clean(f.Name()))
			n, failAt := len(o.synced), o.failAt
			o.mu.Unlock()
			if n == failAt {
				return errors.New("injected directory barrier failure")
			}
			return f.Sync()
		},
		beforeBind: func(path string) {
			if o.onBound != nil {
				o.onBound(path)
			}
		},
	}
}

func (o *jailRootOps) reset(failAt int) []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	got := o.synced
	o.synced, o.failAt = nil, failAt
	return got
}

// TestJailReclaimerRootBarriersRetry: each root's parent barrier runs on
// creation and again on the existing-entry path, so a failed barrier is
// retried by the next construction; the created inode is kept, never
// recreated or removed.
func TestJailReclaimerRootBarriersRetry(t *testing.T) {
	parent := jailShortDir(t)
	data := filepath.Join(parent, "data")
	probe := filepath.Join(data, ProbeDirName)
	for failAt := 1; failAt <= 2; failAt++ {
		ops := &jailRootOps{failAt: failAt}
		if rc, err := newJailReclaimer(t.Context(), data, jailImagesDir(t), ops.ops()); err == nil {
			rc.Close()
			t.Fatalf("construction succeeded although barrier %d failed", failAt)
		}
		before := jailTree(t, parent)
		ops.reset(0)
		rc, err := newJailReclaimer(t.Context(), data, jailImagesDir(t), ops.ops())
		if err != nil {
			t.Fatalf("retry after barrier %d: %v", failAt, err)
		}
		rc.Close()
		if got := ops.reset(0); !slices.Equal(got, []string{parent, data}) {
			t.Fatalf("retry barriers %v, want the data parent and the probe parent", got)
		}
		after := jailTree(t, parent)
		for p, v := range before {
			if after[p] != v {
				t.Fatalf("retry replaced %s", p)
			}
		}
	}
	if _, err := os.Lstat(probe); err != nil {
		t.Fatal(err)
	}
}

// TestJailReclaimerRefusesMissingParent: only the roots themselves are
// created; a missing data parent is refused, nothing is created.
func TestJailReclaimerRefusesMissingParent(t *testing.T) {
	parent := jailShortDir(t)
	data := filepath.Join(parent, "missing", "data")
	if rc, err := NewJailReclaimer(t.Context(), data, jailImagesDir(t)); err == nil {
		rc.Close()
		t.Fatal("a missing data parent was created")
	}
	if _, err := os.Lstat(filepath.Join(parent, "missing")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("a refused construction created a parent")
	}
}

// TestJailReclaimerBindsTheValidatedInode: a root replaced between its
// creation and its binding never gains trust; nothing is removed or
// adopted.
func TestJailReclaimerBindsTheValidatedInode(t *testing.T) {
	cases := map[string]func(t *testing.T, path string){
		"foreign-mode directory": func(t *testing.T, path string) {
			if err := os.Rename(path, path+".orig"); err != nil {
				t.Fatal(err)
			}
			if err := os.Mkdir(path, 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.Chmod(path, 0o777); err != nil {
				t.Fatal(err)
			}
		},
		"symlink to a valid root": func(t *testing.T, path string) {
			target := filepath.Join(jailShortDir(t), "valid")
			if err := os.Mkdir(target, 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.Rename(path, path+".orig"); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(target, path); err != nil {
				t.Fatal(err)
			}
		},
	}
	for _, which := range []string{"data", "probe"} {
		for name, replace := range cases {
			t.Run(which+"/"+name, func(t *testing.T) {
				parent := jailShortDir(t)
				data := filepath.Join(parent, "data")
				want := data
				if which == "probe" {
					want = filepath.Join(data, ProbeDirName)
				}
				ops := &jailRootOps{}
				var replaced bool
				ops.onBound = func(path string) {
					if path == want && !replaced {
						replaced = true
						replace(t, path)
					}
				}
				if rc, err := newJailReclaimer(t.Context(), data, jailImagesDir(t), ops.ops()); err == nil {
					rc.Close()
					t.Fatalf("a replaced %s root was bound", which)
				}
				if !replaced {
					t.Fatal("the replacement hook never ran")
				}
				for _, p := range []string{want, want + ".orig"} {
					if _, err := os.Lstat(p); err != nil {
						t.Fatalf("%s was removed: %v", p, err)
					}
				}
				if name == "foreign-mode directory" {
					if info, err := os.Lstat(want); err != nil || info.Mode().Perm() != 0o777 {
						t.Fatalf("the foreign root was changed: %v %v", info, err)
					}
				}
			})
		}
	}
}

// TestJailReclaimerCancelBetweenRoots: a context ending after the data
// root is bound fails construction with Canceled; nothing is removed and a
// retry completes.
func TestJailReclaimerCancelBetweenRoots(t *testing.T) {
	parent := jailShortDir(t)
	data := filepath.Join(parent, "data")
	ctx, cancel := context.WithCancel(t.Context())
	ops := &jailRootOps{}
	ops.onBound = func(path string) {
		if path == filepath.Join(data, ProbeDirName) {
			cancel()
		}
	}
	if rc, err := newJailReclaimer(ctx, data, jailImagesDir(t), ops.ops()); !errors.Is(err, context.Canceled) {
		if rc != nil {
			rc.Close()
		}
		t.Fatalf("construction = %v, want Canceled", err)
	}
	before := jailTree(t, parent)
	rc, err := newJailReclaimer(t.Context(), data, jailImagesDir(t), (&jailRootOps{}).ops())
	if err != nil {
		t.Fatal(err)
	}
	rc.Close()
	after := jailTree(t, parent)
	for p, v := range before {
		if after[p] != v {
			t.Fatalf("retry replaced %s", p)
		}
	}
}
