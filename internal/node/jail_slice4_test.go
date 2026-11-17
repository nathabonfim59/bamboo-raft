package node

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"syscall"
	"testing"
	"time"
	"uuid"

	"github.com/nathabonfim59/bamboo-raft/internal/node/jailer"
	"github.com/nathabonfim59/bamboo-raft/internal/node/qemu"
	"github.com/nathabonfim59/bamboo-raft/internal/node/qmp"
	"github.com/nathabonfim59/bamboo-raft/internal/node/storage"
)

// A10 slice4: the standalone jail reclaimer (built before the jailer, owning
// its own roots) and the jailed CPU admission probe.

func TestNewJailReclaimerRoots(t *testing.T) {
	t.Run("creates missing roots 0700", func(t *testing.T) {
		data := filepath.Join(jailShortDir(t), "data")
		rc, err := NewJailReclaimer(t.Context(), data, jailImagesDir(t))
		if err != nil {
			t.Fatal(err)
		}
		defer rc.Close()
		for _, p := range []string{data, filepath.Join(data, ProbeDirName)} {
			info, err := os.Lstat(p)
			if err != nil || !info.IsDir() || info.Mode().Perm() != 0o700 {
				t.Fatalf("%s: %v %v", p, info, err)
			}
		}
	})
	t.Run("accepts its own existing roots", func(t *testing.T) {
		data := jailShortDir(t)
		if err := os.Chmod(data, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.Mkdir(filepath.Join(data, ProbeDirName), 0o700); err != nil {
			t.Fatal(err)
		}
		rc, err := NewJailReclaimer(t.Context(), data, jailImagesDir(t))
		if err != nil {
			t.Fatal(err)
		}
		rc.Close()
	})
	refusals := map[string]func(t *testing.T, data string){
		"group-writable data dir": func(t *testing.T, data string) {
			if err := os.Chmod(data, 0o770); err != nil {
				t.Fatal(err)
			}
		},
		"symlinked probe dir": func(t *testing.T, data string) {
			if err := os.Symlink(t.TempDir(), filepath.Join(data, ProbeDirName)); err != nil {
				t.Fatal(err)
			}
		},
		"probe is a file": func(t *testing.T, data string) {
			if err := os.WriteFile(filepath.Join(data, ProbeDirName), nil, 0o600); err != nil {
				t.Fatal(err)
			}
		},
		"probe mode 0755": func(t *testing.T, data string) {
			if err := os.Mkdir(filepath.Join(data, ProbeDirName), 0o755); err != nil {
				t.Fatal(err)
			}
		},
	}
	for name, setup := range refusals {
		t.Run(name, func(t *testing.T) {
			data := jailShortDir(t)
			if err := os.Chmod(data, 0o700); err != nil {
				t.Fatal(err)
			}
			setup(t, data)
			before := jailTree(t, data)
			if rc, err := NewJailReclaimer(t.Context(), data, jailImagesDir(t)); err == nil {
				rc.Close()
				t.Fatalf("NewJailReclaimer accepted %s", name)
			}
			if after := jailTree(t, data); !maps.Equal(before, after) {
				t.Fatal("a refused root was changed")
			}
		})
	}
	t.Run("canceled creates nothing", func(t *testing.T) {
		data := filepath.Join(jailShortDir(t), "data")
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		if _, err := NewJailReclaimer(ctx, data, jailImagesDir(t)); !errors.Is(err, context.Canceled) {
			t.Fatalf("NewJailReclaimer = %v, want Canceled", err)
		}
		if _, err := os.Lstat(data); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("a canceled reclaimer created its root")
		}
	})
}

// jailReclaimerSync records each synced entry together with its observed
// uid, mode and (for directories) entry names at the moment of the sync.
type jailReclaimerSync struct {
	mu   sync.Mutex
	seen []string
}

func (s *jailReclaimerSync) sync(f *os.File) error {
	info, err := f.Stat()
	if err != nil {
		return err
	}
	st := info.Sys().(*syscall.Stat_t)
	state := fmt.Sprintf("%s uid=%d mode=%v", filepath.Base(f.Name()), st.Uid, info.Mode().Perm())
	if info.IsDir() {
		names, _ := os.ReadDir(f.Name())
		state += fmt.Sprintf(" entries=%d", len(names))
	}
	s.mu.Lock()
	s.seen = append(s.seen, state)
	s.mu.Unlock()
	return f.Sync()
}

// TestJailReclaimerVMBarriersObserveRestoredState: each VM entry's sync
// sees its already restored owner and mode, and the control directory's sync
// sees the socket already removed.
func TestJailReclaimerVMBarriersObserveRestoredState(t *testing.T) {
	f, rec := jailReclaimFixture(t)
	control := filepath.Join(f.m.store.VMDir(jailTestID), storage.ControlDirName)
	if err := syscall.Mknod(filepath.Join(control, qemu.QMPFilename), syscall.S_IFSOCK|0o600, 0); err != nil {
		t.Fatal(err)
	}
	jailLoosen(t, control, filepath.Join(f.m.store.VMDir(jailTestID), storage.ConsoleLogName), f.m.store.DiskPath(jailTestID))
	rc, err := NewJailReclaimer(t.Context(), f.m.store.DataDir(), f.m.store.ImageDir())
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()
	witness := &jailReclaimerSync{}
	rc.sync = witness.sync
	if err := rc.Reclaim(t.Context(), rec); err != nil {
		t.Fatal(err)
	}
	uid := os.Geteuid()
	want := []string{
		fmt.Sprintf("control uid=%d mode=-rwx------ entries=0", uid),
		fmt.Sprintf("console.log uid=%d mode=-rw-------", uid),
		fmt.Sprintf("disk.qcow2 uid=%d mode=-rw-------", uid),
	}
	if !slices.Equal(witness.seen, want) {
		t.Fatalf("barriers observed %v, want %v", witness.seen, want)
	}
}

func TestJailReclaimerProbeScratchAndReclaim(t *testing.T) {
	data := jailShortDir(t)
	if err := os.Chmod(data, 0o700); err != nil {
		t.Fatal(err)
	}
	rc, err := NewJailReclaimer(t.Context(), data, jailImagesDir(t))
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()
	attempt := uuid.NewV7().String()
	control, serial, err := rc.ProbeScratch(t.Context(), attempt)
	if err != nil {
		t.Fatal(err)
	}
	defer control.Close()
	defer serial.Close()
	if _, _, err := rc.ProbeScratch(t.Context(), attempt); err == nil {
		t.Fatal("probe scratch of one attempt created twice")
	}
	ci, _ := identityFile(control)
	si, _ := identityFile(serial)
	rec := jailer.Record{SandboxID: jailer.ProbeID, Attempt: attempt, Kind: jailer.KindProbe, UID: uint32(os.Getuid()),
		ControlDev: ci.Dev, ControlIno: ci.Ino, LogDev: si.Dev, LogIno: si.Ino}
	dir := filepath.Join(data, ProbeDirName)
	if err := syscall.Mknod(filepath.Join(dir, attempt+".control", qemu.QMPFilename), syscall.S_IFSOCK|0o600, 0); err != nil {
		t.Fatal(err)
	}
	if err := rc.Reclaim(t.Context(), rec); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{attempt + ".control", attempt + ".serial"} {
		if _, err := os.Lstat(filepath.Join(dir, name)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("%s survived the probe reclaim: %v", name, err)
		}
	}
	if err := rc.Reclaim(t.Context(), rec); err != nil {
		t.Fatalf("probe reclaim is not idempotent: %v", err)
	}
	if err := rc.Reclaim(t.Context(), jailer.Record{SandboxID: jailer.ProbeID, Attempt: attempt, Kind: "tool"}); err == nil {
		t.Fatal("an unknown kind was reclaimed")
	}
}

func TestJailReclaimerProbeReclaimPreservesReplacement(t *testing.T) {
	data := jailShortDir(t)
	if err := os.Chmod(data, 0o700); err != nil {
		t.Fatal(err)
	}
	rc, err := NewJailReclaimer(t.Context(), data, jailImagesDir(t))
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()
	attempt := uuid.NewV7().String()
	control, serial, err := rc.ProbeScratch(t.Context(), attempt)
	if err != nil {
		t.Fatal(err)
	}
	ci, _ := identityFile(control)
	si, _ := identityFile(serial)
	control.Close()
	serial.Close()
	dir := filepath.Join(data, ProbeDirName)
	path := filepath.Join(dir, attempt+".serial")
	if err := os.Rename(path, path+".moved"); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("foreign"), 0o600); err != nil {
		t.Fatal(err)
	}
	before := jailTreeWithContent(t, dir, jailTree(t, dir))
	rec := jailer.Record{SandboxID: jailer.ProbeID, Attempt: attempt, Kind: jailer.KindProbe, UID: uint32(os.Getuid()),
		ControlDev: ci.Dev, ControlIno: ci.Ino, LogDev: si.Dev, LogIno: si.Ino}
	if err := rc.Reclaim(t.Context(), rec); err == nil {
		t.Fatal("a replaced serial log was reclaimed")
	}
	if after := jailTreeWithContent(t, dir, jailTree(t, dir)); !maps.Equal(before, after) {
		t.Fatal("a refused probe reclaim changed the probe directory")
	}
}

// --- jailed CPU probe ------------------------------------------------------

type jailProbeFixture struct {
	jail *fakeJail
	rc   *JailReclaimer
	opts JailProbeOptions
}

func newJailProbeFixture(t *testing.T) *jailProbeFixture {
	t.Helper()
	data := jailShortDir(t)
	if err := os.Chmod(data, 0o700); err != nil {
		t.Fatal(err)
	}
	rc, err := NewJailReclaimer(t.Context(), data, jailImagesDir(t))
	if err != nil {
		t.Fatalf("reclaimer: %v", err)
	}
	t.Cleanup(func() { rc.Close() })
	f := &jailProbeFixture{jail: newFakeJail(t), rc: rc}
	f.jail.reclaim = rc.Reclaim
	f.jail.onStartProbe = func(spec jailer.ProbeSpec) {
		srv := startFakeQMPAt(t, filepath.Join(spec.ControlDir.Name(), qemu.QMPFilename), jailer.ProbeID)
		srv.mu.Lock()
		srv.startPaused = true
		srv.mu.Unlock()
	}
	host := qemu.HostCPU{Vendor: "GenuineIntel", Vmx: true, CPUs: 8}
	option, err := qemu.ResolveCPU(qemu.DefaultCPUModel, qemu.NestedAuto, host)
	if err != nil {
		t.Fatal(err)
	}
	option.Features = []string{"-vmx-apicv-vid"}
	f.opts = JailProbeOptions{
		Binary: "/usr/bin/qemu-system-x86_64", Option: option, Host: host,
		Limits:  jailer.Limits{CPUQuotaPercent: 100, MemoryBytes: 256 << 20, Pids: 64, IO: jailer.IOLimits{Device: jailer.BlockDevice{Major: 8}, ReadBPS: 1}},
		Timeout: 5 * time.Second, Grace: 300 * time.Millisecond,
		dial: func(ctx context.Context, path string, _ int, _ <-chan struct{}) (*qmp.Client, error) {
			return qmp.Dial(ctx, path, quietLog())
		},
	}
	return f
}

func (f *jailProbeFixture) probe(ctx context.Context) (CPUEvidence, error) {
	return probeJailedCPU(ctx, f.jail, f.rc, f.opts)
}

func (f *jailProbeFixture) probeDirEntries(t *testing.T) []string {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(f.rc.dataDirForTest(), ProbeDirName))
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, e := range entries {
		names = append(names, e.Name())
	}
	return names
}

func TestProbeJailedCPUSuccess(t *testing.T) {
	f := newJailProbeFixture(t)
	ev, err := f.probe(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	spec := f.jail.probeSpecs[0]
	if spec.CPU != "Haswell-noTSX,+vmx,-vmx-apicv-vid,enforce" || spec.Binary != f.opts.Binary {
		t.Fatalf("probe spec cpu %q binary %q", spec.CPU, spec.Binary)
	}
	if ev.QEMUVersion != "10.0.0" || ev.Option.String() != spec.CPU || ev.Host != f.opts.Host || ev.Status != "paused" {
		t.Fatalf("evidence %+v", ev)
	}
	run := f.jail.lastRun()
	run.joinReaped(t)
	if waits, _, releases := run.counts(); waits != 1 || releases != 1 {
		t.Fatalf("waits %d releases %d", waits, releases)
	}
	if _, ok := f.jail.Record(jailer.ProbeID); ok {
		t.Fatal("the probe record survived a successful probe")
	}
	if got := f.probeDirEntries(t); len(got) != 0 {
		t.Fatalf("probe scratch left: %v", got)
	}
}

func TestProbeJailedCPUSettlesRetainedProbeByProof(t *testing.T) {
	t.Run("dead retained probe is released first", func(t *testing.T) {
		f := newJailProbeFixture(t)
		old := jailer.Record{SandboxID: jailer.ProbeID, Attempt: uuid.NewV7().String(), Kind: jailer.KindProbe}
		f.jail.records[jailer.ProbeID] = old
		f.jail.reclaim = nil
		if _, err := f.probe(t.Context()); err != nil {
			t.Fatal(err)
		}
		if f.jail.attempts < 1 || len(f.jail.probeSpecs) != 1 {
			t.Fatal("the retained probe was not settled before the new probe")
		}
	})
	t.Run("unprovable retained probe fails startup untouched", func(t *testing.T) {
		f := newJailProbeFixture(t)
		old := jailer.Record{SandboxID: jailer.ProbeID, Attempt: uuid.NewV7().String(), Kind: jailer.KindProbe, PID: 1}
		f.jail.records[jailer.ProbeID] = old
		f.jail.releaseErr = fmt.Errorf("%w: retained probe is alive", jailer.ErrAlive)
		if _, err := f.probe(t.Context()); !errors.Is(err, jailer.ErrAlive) {
			t.Fatalf("probe = %v, want the retained probe's ErrAlive", err)
		}
		if rec, ok := f.jail.Record(jailer.ProbeID); !ok || rec != old || len(f.jail.probeSpecs) != 0 {
			t.Fatal("an unprovable retained probe was released or bypassed")
		}
	})
	t.Run("retained record of another kind is refused", func(t *testing.T) {
		f := newJailProbeFixture(t)
		f.jail.records[jailer.ProbeID] = jailer.Record{SandboxID: jailer.ProbeID, Attempt: uuid.NewV7().String(), Kind: jailer.KindVM}
		if _, err := f.probe(t.Context()); err == nil || f.jail.attempts != 0 || len(f.jail.probeSpecs) != 0 {
			t.Fatalf("probe = %v with a non-probe record under ProbeID; attempts %d", err, f.jail.attempts)
		}
	})
}

func TestProbeJailedCPUFailures(t *testing.T) {
	cases := []struct {
		name        string
		setup       func(f *jailProbeFixture)
		wantRecord  bool
		wantScratch bool
		wantRun     bool
	}{
		{"post-spawn start error", func(f *jailProbeFixture) { f.jail.probeErr = errors.New("injected exec proof failure") }, false, false, true},
		{"qmp peer refused", func(f *jailProbeFixture) {
			f.opts.dial = func(context.Context, string, int, <-chan struct{}) (*qmp.Client, error) {
				return nil, errors.New("qmp peer is another pid")
			}
		}, false, false, true},
		{"release fails after the reap", func(f *jailProbeFixture) {
			f.jail.probeErr = errors.New("injected exec proof failure")
			f.jail.releaseErr = errors.New("injected release failure")
		}, true, true, true},
		{"death unproven", func(f *jailProbeFixture) {
			f.jail.probeErr = errors.New("injected exec proof failure")
			f.jail.killNoop = true
		}, true, true, true},
		{"no spawn, nothing recorded", func(f *jailProbeFixture) { f.jail.noRunErr = errors.New("injected pre-spawn failure") }, false, false, false},
		{"no spawn, record retained", func(f *jailProbeFixture) {
			f.jail.noRunErr = errors.New("injected pre-spawn failure")
			f.jail.retainOnNoRun = true
		}, true, true, false},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			f := newJailProbeFixture(t)
			tt.setup(f)
			if _, err := f.probe(t.Context()); err == nil {
				t.Fatal("probe succeeded")
			}
			_, record := f.jail.Record(jailer.ProbeID)
			if record != tt.wantRecord {
				t.Fatalf("probe record retained %v, want %v", record, tt.wantRecord)
			}
			if scratch := len(f.probeDirEntries(t)) != 0; scratch != tt.wantScratch {
				t.Fatalf("probe scratch retained %v, want %v: %v", scratch, tt.wantScratch, f.probeDirEntries(t))
			}
			run := f.jail.lastRun()
			if (run != nil) != tt.wantRun {
				t.Fatalf("child started %v, want %v", run != nil, tt.wantRun)
			}
			if run != nil {
				_ = run.cmd.Process.Kill()
				run.joinReaped(t)
				if waits, _, _ := run.counts(); waits != 1 {
					t.Fatalf("Wait called %d times", waits)
				}
			}
		})
	}
}

func TestProbeJailedCPUCanceledStartsNothing(t *testing.T) {
	f := newJailProbeFixture(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := f.probe(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("probe = %v, want Canceled", err)
	}
	if len(f.jail.probeSpecs) != 0 || len(f.probeDirEntries(t)) != 0 {
		t.Fatal("a canceled probe started or created scratch")
	}
}
