package storage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json/v2"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
	"uuid"
)

// A10 D1 self-check (rev3 ownership): behavioral tests against a scripted
// imager. The real qemu-img semantics are confirmed only by the lead-owned
// VM gates.

const tcImageID = "v1"

// tcCall is one imager call the self-check made.
type tcCall struct {
	op, path string
	root     ImageRoot
	backing  string // create: BackingName; others: ExpectedBacking
	base     bool
	size     uint64
	// named reports whether the pinned file was, during the call, the
	// inode at its data-root path.
	named bool
}

// tcImager is a scripted Imager: it keeps each fixture's stored name and
// size, and on (when set) intercepts every call before the default.
type tcImager struct {
	dataDir string
	mu      sync.Mutex
	calls   []tcCall
	backing map[string]string
	size    map[string]uint64
	on      func(ctx context.Context, c tcCall) error
	info    func(c tcCall, q QemuInfo) QemuInfo
}

func newTCImager(dataDir string) *tcImager {
	return &tcImager{dataDir: dataDir, backing: map[string]string{}, size: map[string]uint64{}}
}

func (i *tcImager) record(ctx context.Context, op string, ref ImageRef, backing string, size uint64) (tcCall, error) {
	c := tcCall{op: op, path: ref.Path, root: ref.Root, backing: backing, base: ref.Base != nil, size: size}
	if ref.File != nil {
		if pinned, err := ref.File.Stat(); err == nil {
			if named, err := os.Lstat(filepath.Join(i.dataDir, ref.Path)); err == nil {
				c.named = os.SameFile(pinned, named)
			}
		}
	}
	i.mu.Lock()
	i.calls = append(i.calls, c)
	on := i.on
	i.mu.Unlock()
	if on != nil {
		if err := on(ctx, c); err != nil {
			return c, err
		}
	}
	return c, nil
}

func (i *tcImager) CreateOverlay(ctx context.Context, dst ImageRef, size uint64) error {
	if _, err := i.record(ctx, "create", dst, dst.BackingName, size); err != nil {
		return err
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	i.backing[dst.Path], i.size[dst.Path] = dst.BackingName, size
	return nil
}

func (i *tcImager) Grow(ctx context.Context, img ImageRef, size uint64) error {
	if _, err := i.record(ctx, "resize", img, img.ExpectedBacking, size); err != nil {
		return err
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	i.size[img.Path] = size
	return nil
}

func (i *tcImager) Info(ctx context.Context, img ImageRef) (QemuInfo, error) {
	c, err := i.record(ctx, "info", img, img.ExpectedBacking, 0)
	if err != nil {
		return QemuInfo{}, err
	}
	i.mu.Lock()
	q := QemuInfo{Format: "qcow2", VirtualSize: i.size[img.Path], BackingFile: i.backing[img.Path]}
	tweak := i.info
	i.mu.Unlock()
	if tweak != nil {
		q = tweak(c, q)
	}
	return q, nil
}

func (i *tcImager) Check(ctx context.Context, img ImageRef) error {
	_, err := i.record(ctx, "check", img, img.ExpectedBacking, 0)
	return err
}

func (i *tcImager) callsCopy() []tcCall {
	i.mu.Lock()
	defer i.mu.Unlock()
	return slices.Clone(i.calls)
}

// tcEnv is one data root with a verified-looking base and an open Store.
type tcEnv struct {
	dataDir, imageDir string
	store             *Store
	imager            *tcImager
	leases            *toolLeases
	img               Image
	syncs             []string
	pointsSeen        []string
	syncMu            sync.Mutex
}

func newTCEnv(t *testing.T) *tcEnv {
	t.Helper()
	e := &tcEnv{dataDir: t.TempDir(), imageDir: t.TempDir(), leases: &toolLeases{held: map[[2]uint64]bool{}}}
	if err := os.MkdirAll(filepath.Join(e.imageDir, tcImageID), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(e.imageDir, tcImageID, "root.qcow2"), []byte("standalone base"), 0o444); err != nil {
		t.Fatal(err)
	}
	e.imager = newTCImager(e.dataDir)
	e.img = Image{ID: tcImageID, Dir: filepath.Join(e.imageDir, tcImageID), RootVirtualSize: 1 << 30, RootVirtualSizeGiB: 1}
	e.reopen(t)
	return e
}

// reopen closes the Store (a restart) and opens a fresh one with clean
// seams; a sync recorder is always installed.
func (e *tcEnv) reopen(t *testing.T) {
	t.Helper()
	if e.store != nil {
		e.store.Close()
	}
	s, err := Open(Config{DataDir: e.dataDir, ImageDir: e.imageDir, Imager: e.imager, Leases: e.leases})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	e.store = s
	e.imager.mu.Lock()
	e.imager.calls, e.imager.on, e.imager.info = nil, nil, nil
	e.imager.mu.Unlock()
	e.syncMu.Lock()
	e.syncs, e.pointsSeen = nil, nil
	e.syncMu.Unlock()
	s.tc.sync = func(name string, dir *os.File) error {
		e.syncMu.Lock()
		e.syncs = append(e.syncs, name)
		e.syncMu.Unlock()
		return dir.Sync()
	}
}

func (e *tcEnv) parent() string { return filepath.Join(e.dataDir, toolCheckDir) }

func (e *tcEnv) run(t *testing.T) (ToolSelfCheckReport, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	return e.store.ToolSelfCheck(ctx, e.img)
}

func (e *tcEnv) syncsCopy() []string {
	e.syncMu.Lock()
	defer e.syncMu.Unlock()
	return slices.Clone(e.syncs)
}

// tcEntry is one directory entry's observable state.
type tcEntry struct {
	dev, ino uint64
	mode     os.FileMode
	nlink    uint64
	sum      string
	link     string
}

// tcTree snapshots dir's direct entries (nil when dir is absent).
func tcTree(t *testing.T, dir string) map[string]tcEntry {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]tcEntry{}
	for _, d := range entries {
		p := filepath.Join(dir, d.Name())
		info, err := os.Lstat(p)
		if err != nil {
			t.Fatal(err)
		}
		st := info.Sys().(*syscall.Stat_t)
		e := tcEntry{dev: uint64(st.Dev), ino: st.Ino, mode: info.Mode(), nlink: uint64(st.Nlink)}
		switch {
		case info.Mode()&os.ModeSymlink != 0:
			e.link, _ = os.Readlink(p)
		case info.Mode().IsRegular():
			raw, err := os.ReadFile(p)
			if err != nil {
				t.Fatal(err)
			}
			sum := sha256.Sum256(raw)
			e.sum = hex.EncodeToString(sum[:])
		}
		out[d.Name()] = e
	}
	return out
}

func tcIdentity(t *testing.T, p string) Identity {
	t.Helper()
	info, err := os.Lstat(p)
	if err != nil {
		t.Fatal(err)
	}
	st := info.Sys().(*syscall.Stat_t)
	return Identity{Dev: uint64(st.Dev), Ino: st.Ino}
}

// tcValidUUIDv7 reports whether s is a canonical lowercase UUIDv7.
func tcValidUUIDv7(s string) bool {
	u, err := uuid.Parse(s)
	return err == nil && u.String() == s && u[6]>>4 == 7 && u[8]&0xc0 == 0x80
}

// tcWantSteps is the full passing step order.
var tcWantSteps = []string{
	toolCheckStepRecover, toolCheckStepCreate, toolCheckStepInfo, toolCheckStepCheck,
	toolCheckStepResize, toolCheckStepCreateAbsent, toolCheckStepInfoAbsent, toolCheckStepCleanup,
}

// TestToolSelfCheckPasses: a passing run makes exactly the closed calls on
// named fixtures of one fresh attempt, and leaves an empty 0700 parent.
func TestToolSelfCheckPasses(t *testing.T) {
	e := newTCEnv(t)
	report, err := e.run(t)
	if err != nil {
		t.Fatalf("ToolSelfCheck: %v", err)
	}
	if !slices.Equal(report.Steps, tcWantSteps) || report.Image != tcImageID || report.Recovered != 0 {
		t.Fatalf("report = %+v, want steps %v image %s recovered 0", report, tcWantSteps, tcImageID)
	}
	info, err := os.Lstat(e.parent())
	if err != nil || !info.IsDir() || info.Mode().Perm() != 0o700 {
		t.Fatalf("toolcheck parent = %v, %v; want a 0700 directory", info, err)
	}
	if tree := tcTree(t, e.parent()); len(tree) != 0 {
		t.Fatalf("toolcheck not empty after a pass: %v", slices.Sorted(maps.Keys(tree)))
	}
	calls := e.imager.callsCopy()
	if len(calls) != 7 {
		t.Fatalf("imager calls = %+v, want 7", calls)
	}
	overlay := calls[0].path
	attempt, ok := strings.CutPrefix(overlay, toolCheckDir+"/overlay-")
	attempt, ok2 := strings.CutSuffix(attempt, ".qcow2")
	if !ok || !ok2 || !tcValidUUIDv7(attempt) {
		t.Fatalf("overlay fixture path %q is not toolcheck/overlay-<uuidv7>.qcow2", overlay)
	}
	absent := toolCheckDir + "/absent-" + attempt + ".qcow2"
	base := e.store.imageBaseName(tcImageID)
	absentBase := filepath.Join(e.store.DataDir(), toolCheckDir, "absent-base-"+attempt)
	size := e.img.RootVirtualSize
	want := []tcCall{
		{op: "create", path: overlay, root: ImageRootData, backing: base, size: size, named: true},
		{op: "info", path: overlay, root: ImageRootData, backing: base, base: true, named: true},
		{op: "check", path: overlay, root: ImageRootData, backing: base, base: true, named: true},
		{op: "resize", path: overlay, root: ImageRootData, backing: base, base: true, size: size + 1<<20, named: true},
		{op: "info", path: overlay, root: ImageRootData, backing: base, base: true, named: true},
		{op: "create", path: absent, root: ImageRootData, backing: absentBase, size: size, named: true},
		{op: "info", path: absent, root: ImageRootData, backing: absentBase, base: true, named: true},
	}
	if !slices.Equal(calls, want) {
		t.Fatalf("imager calls\n got %+v\nwant %+v", calls, want)
	}
}

// tcAllPoints is the fault-point sequence of a passing run that creates the
// parent.
var tcAllPoints = []string{
	toolCheckPointParentCreated, toolCheckPointParentBarrier, toolCheckPointEntryBarrier,
	toolCheckPointFixturesTmp, toolCheckPointIntentWritten, toolCheckPointIntentLink,
	toolCheckPointIntentLinked, toolCheckPointIntentBarrier,
	toolCheckPointFixtureLink + ":overlay", toolCheckPointFixtureLinked + ":overlay",
	toolCheckPointFixtureLink + ":absent", toolCheckPointFixtureLinked + ":absent",
	toolCheckPointFixturesBarrier,
	"step:" + toolCheckStepCreate, "step:" + toolCheckStepInfo, "step:" + toolCheckStepCheck,
	"step:" + toolCheckStepResize, "step:" + toolCheckStepCreateAbsent, "step:" + toolCheckStepInfoAbsent,
	"step:" + toolCheckStepCleanup, toolCheckPointEntryBarrier,
	toolCheckPointBeforeUnlink + ":overlay", toolCheckPointUnlinked + ":overlay",
	toolCheckPointBeforeUnlink + ":absent", toolCheckPointUnlinked + ":absent",
	toolCheckPointFixturesGone, toolCheckPointBeforeUnlink + ":intent",
	toolCheckPointIntentUnlinked, toolCheckPointFinalBarrier,
}

// TestToolSelfCheckPointOrderAndNamedState pins the publish order: no name
// exists before the intent's exclusive link, the intent is the only name
// until the fixtures are linked, and every fixture is named (and barriered)
// before any tool runs.
func TestToolSelfCheckPointOrderAndNamedState(t *testing.T) {
	e := newTCEnv(t)
	var points []string
	names := map[string][]string{}
	e.store.tc.fault = func(point string) error {
		points = append(points, point)
		if _, ok := names[point]; !ok {
			names[point] = slices.Sorted(maps.Keys(tcTree(t, e.parent())))
		}
		return nil
	}
	if _, err := e.run(t); err != nil {
		t.Fatalf("ToolSelfCheck: %v", err)
	}
	if !slices.Equal(points, tcAllPoints) {
		t.Fatalf("fault points\n got %q\nwant %q", points, tcAllPoints)
	}
	for _, p := range []string{toolCheckPointFixturesTmp, toolCheckPointIntentWritten, toolCheckPointIntentLink} {
		if len(names[p]) != 0 {
			t.Errorf("at %s the parent names %q; no name may exist before the intent's link", p, names[p])
		}
	}
	for _, p := range []string{toolCheckPointIntentLinked, toolCheckPointIntentBarrier, toolCheckPointFixtureLink + ":overlay"} {
		if len(names[p]) != 1 || !strings.HasPrefix(names[p][0], "intent-") || !strings.HasSuffix(names[p][0], ".json") {
			t.Errorf("at %s the parent names %q, want only the intent", p, names[p])
		}
	}
	if got := names["step:"+toolCheckStepCreate]; len(got) != 3 {
		t.Errorf("before the first tool the parent names %q, want the intent and both fixtures", got)
	}
	// The fixtures' barrier precedes the first tool.
	syncs := e.syncsCopy()
	if len(syncs) < 4 || syncs[0] != "." {
		t.Errorf("barriers %q: want the data-root barrier first", syncs)
	}
}

// TestToolSelfCheckParentEEXISTRepaysBarriers: an existing parent still
// pays the data-root barrier and the parent's own barrier before any other
// self-check work, so a crash before either barrier (or a failed final
// unlink barrier) is repaid.
func TestToolSelfCheckParentEEXISTRepaysBarriers(t *testing.T) {
	for _, tc := range []struct {
		name  string
		setup func(t *testing.T, e *tcEnv)
	}{
		{"crash after mkdir", func(t *testing.T, e *tcEnv) {
			e.store.tc.fault = tcAbandonAt(toolCheckPointParentCreated)
			if _, err := e.run(t); !errors.Is(err, errToolCheckAbandon) {
				t.Fatalf("abandoned run = %v", err)
			}
		}},
		{"final barrier failed", func(t *testing.T, e *tcEnv) {
			sync := e.store.tc.sync
			e.store.tc.sync = func(name string, dir *os.File) error {
				if name == toolCheckDir && slices.Contains(e.points(), toolCheckPointIntentUnlinked) {
					return errors.New("injected final barrier failure")
				}
				return sync(name, dir)
			}
			e.trackPoints()
			if _, err := e.run(t); err == nil {
				t.Fatal("a failed final barrier passed")
			}
		}},
		{"empty parent left by a pass", func(t *testing.T, e *tcEnv) {
			if _, err := e.run(t); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := newTCEnv(t)
			tc.setup(t, e)
			e.reopen(t)
			var first []string
			e.store.tc.fault = func(point string) error {
				if len(first) < 2 {
					first = append(first, point+"@"+strings.Join(e.syncsCopy(), ","))
				}
				return nil
			}
			if _, err := e.run(t); err != nil {
				t.Fatalf("restart: %v", err)
			}
			want := []string{toolCheckPointParentBarrier + "@.", toolCheckPointEntryBarrier + "@.," + toolCheckDir}
			if !slices.Equal(first, want) {
				t.Fatalf("restart began %q, want %q (no parent-created; both barriers repaid)", first, want)
			}
		})
	}
}

// points/trackPoints let a sync seam see how far the run got.
func (e *tcEnv) trackPoints() {
	prev := e.store.tc.fault
	e.store.tc.fault = func(p string) error {
		e.syncMu.Lock()
		e.pointsSeen = append(e.pointsSeen, p)
		e.syncMu.Unlock()
		if prev != nil {
			return prev(p)
		}
		return nil
	}
}

func (e *tcEnv) points() []string {
	e.syncMu.Lock()
	defer e.syncMu.Unlock()
	return slices.Clone(e.pointsSeen)
}

func tcAbandonAt(point string) func(string) error {
	fired := false
	return func(p string) error {
		if p == point && !fired {
			fired = true
			return fmt.Errorf("at %s: %w", p, errToolCheckAbandon)
		}
		return nil
	}
}

// tcReadIntent decodes the parent's single intent, if any.
func tcReadIntent(t *testing.T, dir string) (string, *toolCheckIntent) {
	t.Helper()
	var name string
	for n := range tcTree(t, dir) {
		if strings.HasPrefix(n, "intent-") {
			if name != "" {
				t.Fatalf("two intents: %s and %s", name, n)
			}
			name = n
		}
	}
	if name == "" {
		return "", nil
	}
	raw, err := os.ReadFile(filepath.Join(dir, name))
	if err != nil {
		t.Fatal(err)
	}
	var v toolCheckIntent
	if err := json.Unmarshal(raw, &v); err != nil {
		t.Fatalf("intent %s: %v", name, err)
	}
	return name, &v
}

// TestToolSelfCheckCrashPointsRecover: a crash at every publish, link,
// barrier, step and cleanup point leaves only the intent and recorded
// fixtures (or nothing), and the next startup removes exactly those by
// identity and passes.
func TestToolSelfCheckCrashPointsRecover(t *testing.T) {
	for _, point := range tcAllPoints {
		t.Run(point, func(t *testing.T) {
			e := newTCEnv(t)
			e.store.tc.fault = tcAbandonAt(point)
			if _, err := e.run(t); !errors.Is(err, errToolCheckAbandon) {
				t.Fatalf("run with a crash at %s = %v", point, err)
			}
			tree := tcTree(t, e.parent())
			intentName, intent := tcReadIntent(t, e.parent())
			recorded := map[string]Identity{}
			if intent != nil {
				recorded[intentName] = tcIdentity(t, filepath.Join(e.parent(), intentName))
				for _, f := range intent.Fixtures {
					recorded[f.Name] = Identity{Dev: f.Dev, Ino: f.Ino}
				}
			}
			for name, got := range tree {
				if want, ok := recorded[name]; !ok || want != (Identity{Dev: got.dev, Ino: got.ino}) {
					t.Fatalf("after a crash at %s the parent holds unrecorded %s", point, name)
				}
			}
			e.reopen(t)
			report, err := e.run(t)
			if err != nil {
				t.Fatalf("restart after a crash at %s: %v", point, err)
			}
			if report.Recovered != len(tree) {
				t.Errorf("recovered %d, want %d (%v)", report.Recovered, len(tree), slices.Sorted(maps.Keys(tree)))
			}
			if after := tcTree(t, e.parent()); len(after) != 0 {
				t.Fatalf("restart left %v", slices.Sorted(maps.Keys(after)))
			}
		})
	}
}

// TestToolSelfCheckCrashDuringRecovery: recovery is restartable at its own
// unlink and barrier points.
func TestToolSelfCheckCrashDuringRecovery(t *testing.T) {
	for _, point := range []string{
		toolCheckPointUnlinked + ":overlay", toolCheckPointFixturesGone,
		toolCheckPointIntentUnlinked, toolCheckPointFinalBarrier,
	} {
		t.Run(point, func(t *testing.T) {
			e := newTCEnv(t)
			e.store.tc.fault = tcAbandonAt("step:" + toolCheckStepCheck)
			if _, err := e.run(t); !errors.Is(err, errToolCheckAbandon) {
				t.Fatal(err)
			}
			e.reopen(t)
			e.store.tc.fault = tcAbandonAt(point) // reached first in recovery
			if _, err := e.run(t); !errors.Is(err, errToolCheckAbandon) {
				t.Fatalf("recovery crash at %s = %v", point, err)
			}
			if len(e.imager.callsCopy()) != 0 {
				t.Fatal("a tool ran before recovery finished")
			}
			e.reopen(t)
			if _, err := e.run(t); err != nil {
				t.Fatalf("third start: %v", err)
			}
			if after := tcTree(t, e.parent()); len(after) != 0 {
				t.Fatalf("third start left %v", slices.Sorted(maps.Keys(after)))
			}
		})
	}
}

// TestToolSelfCheckPointFailuresCleanUp: an ordinary failure at any point
// after the parent exists fails the check and cleans up exactly.
func TestToolSelfCheckPointFailuresCleanUp(t *testing.T) {
	for _, point := range tcAllPoints {
		if strings.HasPrefix(point, toolCheckPointParentCreated) || strings.HasPrefix(point, toolCheckPointParentBarrier) {
			continue
		}
		cleanupPoint := slices.Index(tcAllPoints, "step:"+toolCheckStepCleanup) <= slices.Index(tcAllPoints, point)
		t.Run(point, func(t *testing.T) {
			e := newTCEnv(t)
			fired := false
			e.store.tc.fault = func(p string) error {
				if p == point && !fired {
					fired = true
					return errors.New("injected failure at " + p)
				}
				return nil
			}
			if _, err := e.run(t); err == nil || !strings.Contains(err.Error(), "injected failure") {
				t.Fatalf("run = %v, want the injected failure", err)
			}
			if cleanupPoint {
				// A failure inside cleanup stops it; the restart finishes.
				e.reopen(t)
				if _, err := e.run(t); err != nil {
					t.Fatalf("restart: %v", err)
				}
			}
			if after := tcTree(t, e.parent()); len(after) != 0 {
				t.Fatalf("failure at %s left %v", point, slices.Sorted(maps.Keys(after)))
			}
		})
	}
}

// TestToolSelfCheckStepRefusals: every semantic mismatch fails the check
// with ErrToolSelfCheck (no fallback) and the fixtures are removed.
func TestToolSelfCheckStepRefusals(t *testing.T) {
	injected := errors.New("injected tool failure")
	nth := func(op string, n int, fail func(ctx context.Context, c tcCall) error) func(context.Context, tcCall) error {
		seen := 0
		return func(ctx context.Context, c tcCall) error {
			if c.op != op {
				return nil
			}
			seen++
			if seen == n {
				return fail(ctx, c)
			}
			return nil
		}
	}
	fail := func(context.Context, tcCall) error { return injected }
	for _, tc := range []struct {
		name string
		on   func(context.Context, tcCall) error
		info func(c tcCall, q QemuInfo) QemuInfo
	}{
		{name: "create fails", on: nth("create", 1, fail)},
		{name: "info fails", on: nth("info", 1, fail)},
		{name: "check fails", on: nth("check", 1, fail)},
		{name: "resize fails", on: nth("resize", 1, fail)},
		{name: "create absent fails", on: nth("create", 2, fail)},
		{name: "info of the absent backing fails (it was opened)", on: nth("info", 3, fail)},
		{name: "info wrong stored name", info: func(c tcCall, q QemuInfo) QemuInfo {
			if strings.Contains(c.path, "overlay-") {
				q.BackingFile = "/elsewhere/root.qcow2"
			}
			return q
		}},
		{name: "info wrong format", info: func(_ tcCall, q QemuInfo) QemuInfo { q.Format = "raw"; return q }},
		{name: "info wrong size", info: func(_ tcCall, q QemuInfo) QemuInfo { q.VirtualSize++; return q }},
		{name: "resize not visible", info: func() func(tcCall, QemuInfo) QemuInfo {
			n := 0
			return func(c tcCall, q QemuInfo) QemuInfo {
				if strings.Contains(c.path, "overlay-") {
					n++
					if n == 2 {
						q.VirtualSize = 1 << 30
					}
				}
				return q
			}
		}()},
		{name: "absent info wrong name", info: func(c tcCall, q QemuInfo) QemuInfo {
			if strings.Contains(c.path, "absent-") {
				q.BackingFile = ""
			}
			return q
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := newTCEnv(t)
			e.imager.on, e.imager.info = tc.on, tc.info
			report, err := e.run(t)
			if !errors.Is(err, ErrToolSelfCheck) {
				t.Fatalf("ToolSelfCheck = %+v, %v; want ErrToolSelfCheck", report, err)
			}
			if after := tcTree(t, e.parent()); len(after) != 0 {
				t.Fatalf("refusal left %v", slices.Sorted(maps.Keys(after)))
			}
		})
	}
}

// TestToolSelfCheckAbsentBaseAppearing: the absent overlay's stored name
// must stay absent; if it appears it is foreign, and nothing is removed.
func TestToolSelfCheckAbsentBaseAppearing(t *testing.T) {
	e := newTCEnv(t)
	e.imager.on = func(_ context.Context, c tcCall) error {
		if c.op == "create" && strings.Contains(c.path, "absent-") {
			return os.WriteFile(c.backing, []byte("planted"), 0o600)
		}
		return nil
	}
	if _, err := e.run(t); !errors.Is(err, ErrToolSelfCheck) {
		t.Fatalf("ToolSelfCheck = %v, want ErrToolSelfCheck", err)
	}
	tree := tcTree(t, e.parent())
	if len(tree) != 4 {
		t.Fatalf("a foreign absent-base must preserve everything; parent holds %v", slices.Sorted(maps.Keys(tree)))
	}
}

// TestToolSelfCheckHeldAndRetained: a fixture a tool still holds, or the
// target of a retained tool, is preserved with its intent and fails the
// check; a timeout never becomes success, and its cleanup (on a bounded
// detached context) still proves no hold.
func TestToolSelfCheckHeldAndRetained(t *testing.T) {
	t.Run("timeout with a live tool", func(t *testing.T) {
		e := newTCEnv(t)
		e.store.tc.timeout = 200 * time.Millisecond
		e.imager.on = func(ctx context.Context, c tcCall) error {
			if c.op == "check" {
				e.leases.hold(nil, filepath.Join(e.dataDir, c.path))
				<-ctx.Done()
				return context.Cause(ctx)
			}
			return nil
		}
		_, err := e.run(t)
		if err == nil || !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("ToolSelfCheck = %v, want a deadline failure", err)
		}
		if tree := tcTree(t, e.parent()); len(tree) != 3 {
			t.Fatalf("a held fixture must keep the intent and both fixtures; parent holds %v", slices.Sorted(maps.Keys(tree)))
		}
	})
	t.Run("timeout without a hold cleans up", func(t *testing.T) {
		e := newTCEnv(t)
		e.store.tc.timeout = 200 * time.Millisecond
		e.imager.on = func(ctx context.Context, c tcCall) error {
			if c.op == "check" {
				<-ctx.Done()
				return context.Cause(ctx)
			}
			return nil
		}
		if _, err := e.run(t); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("ToolSelfCheck = %v, want a deadline failure", err)
		}
		if tree := tcTree(t, e.parent()); len(tree) != 0 {
			t.Fatalf("an unheld timeout left %v", slices.Sorted(maps.Keys(tree)))
		}
	})
	t.Run("canceled after a tool reported success", func(t *testing.T) {
		e := newTCEnv(t)
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		e.imager.on = func(_ context.Context, c tcCall) error {
			if c.op == "info" && strings.Contains(c.path, "absent-") {
				cancel()
			}
			return nil
		}
		if _, err := e.store.ToolSelfCheck(ctx, e.img); !errors.Is(err, context.Canceled) {
			t.Fatalf("ToolSelfCheck = %v, want context.Canceled (never success)", err)
		}
		if tree := tcTree(t, e.parent()); len(tree) != 0 {
			t.Fatalf("cleanup after cancellation left %v", slices.Sorted(maps.Keys(tree)))
		}
	})
	t.Run("canceled at the final cleanup barrier after every tool passed", func(t *testing.T) {
		e := newTCEnv(t)
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		e.trackPoints()
		sync := e.store.tc.sync
		e.store.tc.sync = func(name string, dir *os.File) error {
			if name == toolCheckDir && slices.Contains(e.points(), toolCheckPointIntentUnlinked) {
				cancel()
			}
			return sync(name, dir)
		}
		report, err := e.store.ToolSelfCheck(ctx, e.img)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("ToolSelfCheck = %+v, %v; want context.Canceled, never a passing report", report, err)
		}
		if len(e.imager.callsCopy()) != 7 {
			t.Fatal("the control needs every tool to have passed")
		}
		if tree := tcTree(t, e.parent()); len(tree) != 0 {
			t.Fatalf("the detached cleanup did not complete: %v", slices.Sorted(maps.Keys(tree)))
		}
	})
	t.Run("canceled mid-run runs no further tool", func(t *testing.T) {
		e := newTCEnv(t)
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		e.imager.on = func(_ context.Context, c tcCall) error {
			if c.op == "info" {
				cancel()
			}
			return nil
		}
		report, err := e.store.ToolSelfCheck(ctx, e.img)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("ToolSelfCheck = %v, want context.Canceled", err)
		}
		if want := []string{toolCheckStepRecover, toolCheckStepCreate}; !slices.Equal(report.Steps, want) {
			t.Fatalf("steps %q: the canceled step must not count as passed (want %q)", report.Steps, want)
		}
		if calls := e.imager.callsCopy(); len(calls) != 2 {
			t.Fatalf("tools after cancellation: %+v, want only create and the canceled info", calls)
		}
		if tree := tcTree(t, e.parent()); len(tree) != 0 {
			t.Fatalf("cleanup after cancellation left %v", slices.Sorted(maps.Keys(tree)))
		}
	})
	t.Run("canceled inside a tool runs no later call of its step", func(t *testing.T) {
		e := newTCEnv(t)
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		e.imager.on = func(_ context.Context, c tcCall) error {
			if c.op == "resize" {
				cancel()
			}
			return nil
		}
		if _, err := e.store.ToolSelfCheck(ctx, e.img); !errors.Is(err, context.Canceled) {
			t.Fatalf("ToolSelfCheck = %v, want context.Canceled", err)
		}
		if calls := e.imager.callsCopy(); len(calls) != 4 || calls[3].op != "resize" {
			t.Fatalf("calls after a cancellation inside resize: %+v, want it to be the last", calls)
		}
		if tree := tcTree(t, e.parent()); len(tree) != 0 {
			t.Fatalf("cleanup left %v", slices.Sorted(maps.Keys(tree)))
		}
	})
	t.Run("retained tool", func(t *testing.T) {
		e := newTCEnv(t)
		e.imager.on = func(_ context.Context, c tcCall) error {
			if c.op == "check" {
				return fmt.Errorf("injected: %w", ErrToolRetained)
			}
			return nil
		}
		if _, err := e.run(t); !errors.Is(err, ErrToolRetained) || !errors.Is(err, ErrToolSelfCheck) {
			t.Fatalf("ToolSelfCheck = %v, want ErrToolSelfCheck wrapping ErrToolRetained", err)
		}
		if tree := tcTree(t, e.parent()); len(tree) != 3 {
			t.Fatalf("a retained tool's fixtures must be preserved; parent holds %v", slices.Sorted(maps.Keys(tree)))
		}
	})
}

// tcCrafted is a hand-made earlier attempt: the parent, both fixtures and a
// valid intent naming them.
type tcCrafted struct {
	attempt string
	intent  toolCheckIntent
}

func (c *tcCrafted) intentName() string { return "intent-" + c.attempt + ".json" }

func tcCraft(t *testing.T, e *tcEnv) *tcCrafted {
	t.Helper()
	if err := os.Mkdir(e.parent(), 0o700); err != nil && !errors.Is(err, os.ErrExist) {
		t.Fatal(err)
	}
	c := &tcCrafted{attempt: uuid.NewV7().String()}
	c.intent = toolCheckIntent{Schema: toolCheckSchema, Attempt: c.attempt, Parent: tcIdentity(t, e.parent())}
	for _, kind := range []string{"overlay", "absent"} {
		name := kind + "-" + c.attempt + ".qcow2"
		if err := os.WriteFile(filepath.Join(e.parent(), name), []byte("fixture "+kind), 0o600); err != nil {
			t.Fatal(err)
		}
		id := tcIdentity(t, filepath.Join(e.parent(), name))
		c.intent.Fixtures = append(c.intent.Fixtures, toolCheckFixture{Name: name, Dev: id.Dev, Ino: id.Ino})
	}
	tcWriteIntent(t, e, c.intentName(), c.intent)
	return c
}

func tcWriteIntent(t *testing.T, e *tcEnv, name string, v toolCheckIntent) {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	tcWriteRaw(t, e, name, raw, 0o600)
}

func tcWriteRaw(t *testing.T, e *tcEnv, name string, raw []byte, mode os.FileMode) {
	t.Helper()
	p := filepath.Join(e.parent(), name)
	_ = os.Remove(p)
	if err := os.WriteFile(p, raw, mode); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(p, mode); err != nil {
		t.Fatal(err)
	}
}

// TestToolSelfCheckRecoversCraftedAttempt is the positive control for the
// refusals below: a valid earlier intent and its recorded fixtures are
// removed by identity (an already removed fixture is fine).
func TestToolSelfCheckRecoversCraftedAttempt(t *testing.T) {
	for _, tc := range []struct {
		name      string
		edit      func(t *testing.T, e *tcEnv, c *tcCrafted)
		recovered int
	}{
		{"intent and both fixtures", nil, 3},
		{"one fixture already removed", func(t *testing.T, e *tcEnv, c *tcCrafted) {
			if err := os.Remove(filepath.Join(e.parent(), c.intent.Fixtures[1].Name)); err != nil {
				t.Fatal(err)
			}
		}, 2},
		{"intent only", func(t *testing.T, e *tcEnv, c *tcCrafted) {
			for _, f := range c.intent.Fixtures {
				if err := os.Remove(filepath.Join(e.parent(), f.Name)); err != nil {
					t.Fatal(err)
				}
			}
		}, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := newTCEnv(t)
			c := tcCraft(t, e)
			if tc.edit != nil {
				tc.edit(t, e, c)
			}
			report, err := e.run(t)
			if err != nil {
				t.Fatalf("ToolSelfCheck: %v", err)
			}
			if report.Recovered != tc.recovered {
				t.Fatalf("recovered %d, want %d", report.Recovered, tc.recovered)
			}
			if tree := tcTree(t, e.parent()); len(tree) != 0 {
				t.Fatalf("parent holds %v", slices.Sorted(maps.Keys(tree)))
			}
		})
	}
}

// TestToolSelfCheckRefusesWithoutMutation: every unprovable parent state
// fails the check before any mutation or tool: the parent's entries,
// inodes, modes and contents are unchanged.
func TestToolSelfCheckRefusesWithoutMutation(t *testing.T) {
	other := uuid.NewV7().String()
	for _, tc := range []struct {
		name string
		edit func(t *testing.T, e *tcEnv, c *tcCrafted) // c nil: no crafted attempt
		bare bool
	}{
		{name: "stray file and no intent", bare: true, edit: func(t *testing.T, e *tcEnv, _ *tcCrafted) {
			_ = os.Mkdir(e.parent(), 0o700)
			tcWriteRaw(t, e, "stray", []byte("x"), 0o600)
		}},
		{name: "fixture-named entry and no intent", bare: true, edit: func(t *testing.T, e *tcEnv, _ *tcCrafted) {
			_ = os.Mkdir(e.parent(), 0o700)
			tcWriteRaw(t, e, "overlay-"+other+".qcow2", []byte("x"), 0o600)
		}},
		{name: "foreign entry beside a valid attempt", edit: func(t *testing.T, e *tcEnv, _ *tcCrafted) {
			tcWriteRaw(t, e, "zzz-foreign", []byte("x"), 0o600)
		}},
		{name: "unknown temp-like entry", edit: func(t *testing.T, e *tcEnv, c *tcCrafted) {
			tcWriteRaw(t, e, c.intentName()+".tmp", []byte("{}"), 0o600)
		}},
		{name: "fixture of another attempt", edit: func(t *testing.T, e *tcEnv, _ *tcCrafted) {
			tcWriteRaw(t, e, "absent-"+other+".qcow2", []byte("x"), 0o600)
		}},
		{name: "second intent", edit: func(t *testing.T, e *tcEnv, c *tcCrafted) {
			v := c.intent
			v.Attempt = other
			tcWriteIntent(t, e, "intent-"+other+".json", v)
		}},
		{name: "absent base present", edit: func(t *testing.T, e *tcEnv, c *tcCrafted) {
			tcWriteRaw(t, e, "absent-base-"+c.attempt, []byte("x"), 0o600)
		}},
		{name: "fixture inode replaced at its name", edit: func(t *testing.T, e *tcEnv, c *tcCrafted) {
			p := filepath.Join(e.parent(), c.intent.Fixtures[0].Name)
			if err := os.Rename(p, p+"-keep"); err != nil {
				t.Fatal(err)
			}
			tcWriteRaw(t, e, c.intent.Fixtures[0].Name, []byte("replacement"), 0o600)
			if err := os.Rename(p+"-keep", filepath.Join(e.dataDir, "kept-original")); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "fixture hard-linked elsewhere", edit: func(t *testing.T, e *tcEnv, c *tcCrafted) {
			if err := os.Link(filepath.Join(e.parent(), c.intent.Fixtures[0].Name), filepath.Join(e.dataDir, "second-link")); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "fixture is a symlink", edit: func(t *testing.T, e *tcEnv, c *tcCrafted) {
			p := filepath.Join(e.parent(), c.intent.Fixtures[1].Name)
			if err := os.Remove(p); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink("/etc/hostname", p); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "fixture held by a tool", edit: func(t *testing.T, e *tcEnv, c *tcCrafted) {
			e.leases.hold(t, filepath.Join(e.parent(), c.intent.Fixtures[1].Name))
		}},
		{name: "intent held by a tool", edit: func(t *testing.T, e *tcEnv, c *tcCrafted) {
			e.leases.hold(t, filepath.Join(e.parent(), c.intentName()))
		}},
		{name: "oversized intent", edit: func(t *testing.T, e *tcEnv, c *tcCrafted) {
			raw, _ := json.Marshal(c.intent)
			raw = append(raw, bytes.Repeat([]byte(" "), toolCheckMaxIntent+1-len(raw))...)
			tcWriteRaw(t, e, c.intentName(), raw, 0o600)
		}},
		{name: "intent unknown member", edit: func(t *testing.T, e *tcEnv, c *tcCrafted) {
			raw, _ := json.Marshal(c.intent)
			raw = append([]byte(`{"extra":1,`), raw[1:]...)
			tcWriteRaw(t, e, c.intentName(), raw, 0o600)
		}},
		{name: "intent trailing value", edit: func(t *testing.T, e *tcEnv, c *tcCrafted) {
			raw, _ := json.Marshal(c.intent)
			tcWriteRaw(t, e, c.intentName(), append(raw, []byte(" {}")...), 0o600)
		}},
		{name: "intent duplicate member", edit: func(t *testing.T, e *tcEnv, c *tcCrafted) {
			raw, _ := json.Marshal(c.intent)
			raw = append([]byte(`{"schema":1,`), raw[1:]...)
			tcWriteRaw(t, e, c.intentName(), raw, 0o600)
		}},
		{name: "intent empty", edit: func(t *testing.T, e *tcEnv, c *tcCrafted) {
			tcWriteRaw(t, e, c.intentName(), nil, 0o600)
		}},
		{name: "intent wrong schema", edit: func(t *testing.T, e *tcEnv, c *tcCrafted) {
			v := c.intent
			v.Schema = 2
			tcWriteIntent(t, e, c.intentName(), v)
		}},
		{name: "intent attempt differs from its name", edit: func(t *testing.T, e *tcEnv, c *tcCrafted) {
			v := c.intent
			v.Attempt = other
			tcWriteIntent(t, e, c.intentName(), v)
		}},
		{name: "intent attempt not a uuidv7", edit: func(t *testing.T, e *tcEnv, c *tcCrafted) {
			v4 := uuid.NewV4().String()
			v := c.intent
			v.Attempt = v4
			v.Fixtures = slices.Clone(c.intent.Fixtures)
			for i := range v.Fixtures {
				v.Fixtures[i].Name = strings.Replace(v.Fixtures[i].Name, c.attempt, v4, 1)
				if err := os.Rename(filepath.Join(e.parent(), c.intent.Fixtures[i].Name), filepath.Join(e.parent(), v.Fixtures[i].Name)); err != nil {
					t.Fatal(err)
				}
			}
			if err := os.Remove(filepath.Join(e.parent(), c.intentName())); err != nil {
				t.Fatal(err)
			}
			tcWriteIntent(t, e, "intent-"+v4+".json", v)
		}},
		{name: "intent uppercase attempt", edit: func(t *testing.T, e *tcEnv, c *tcCrafted) {
			v := c.intent
			v.Attempt = strings.ToUpper(c.attempt)
			tcWriteIntent(t, e, c.intentName(), v)
		}},
		{name: "intent wrong parent identity", edit: func(t *testing.T, e *tcEnv, c *tcCrafted) {
			v := c.intent
			v.Parent.Ino++
			tcWriteIntent(t, e, c.intentName(), v)
		}},
		{name: "intent duplicate fixture identity", edit: func(t *testing.T, e *tcEnv, c *tcCrafted) {
			v := c.intent
			v.Fixtures = slices.Clone(v.Fixtures)
			v.Fixtures[1].Dev, v.Fixtures[1].Ino = v.Fixtures[0].Dev, v.Fixtures[0].Ino
			tcWriteIntent(t, e, c.intentName(), v)
		}},
		{name: "intent duplicate fixture identity, second fixture gone", edit: func(t *testing.T, e *tcEnv, c *tcCrafted) {
			if err := os.Remove(filepath.Join(e.parent(), c.intent.Fixtures[1].Name)); err != nil {
				t.Fatal(err)
			}
			v := c.intent
			v.Fixtures = slices.Clone(v.Fixtures)
			v.Fixtures[1].Dev, v.Fixtures[1].Ino = v.Fixtures[0].Dev, v.Fixtures[0].Ino
			tcWriteIntent(t, e, c.intentName(), v)
		}},
		{name: "intent records its own inode as a fixture", edit: func(t *testing.T, e *tcEnv, c *tcCrafted) {
			if err := os.Remove(filepath.Join(e.parent(), c.intent.Fixtures[1].Name)); err != nil {
				t.Fatal(err)
			}
			// Rewrite in place so the intent keeps its inode, then record it.
			p := filepath.Join(e.parent(), c.intentName())
			own := tcIdentity(t, p)
			v := c.intent
			v.Fixtures = slices.Clone(v.Fixtures)
			v.Fixtures[1].Dev, v.Fixtures[1].Ino = own.Dev, own.Ino
			raw, err := json.Marshal(v)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(p, raw, 0o600); err != nil {
				t.Fatal(err)
			}
			if tcIdentity(t, p) != own {
				t.Fatal("the intent changed inode")
			}
		}},
		{name: "intent zero fixture identity", edit: func(t *testing.T, e *tcEnv, c *tcCrafted) {
			v := c.intent
			v.Fixtures = slices.Clone(v.Fixtures)
			v.Fixtures[0].Ino = 0
			tcWriteIntent(t, e, c.intentName(), v)
		}},
		{name: "intent records a foreign name", edit: func(t *testing.T, e *tcEnv, c *tcCrafted) {
			v := c.intent
			v.Fixtures = slices.Clone(v.Fixtures)
			v.Fixtures[0].Name = "../vms/x"
			tcWriteIntent(t, e, c.intentName(), v)
		}},
		{name: "intent one fixture missing from the record", edit: func(t *testing.T, e *tcEnv, c *tcCrafted) {
			v := c.intent
			v.Fixtures = v.Fixtures[:1]
			tcWriteIntent(t, e, c.intentName(), v)
		}},
		{name: "intent mode 0644", edit: func(t *testing.T, e *tcEnv, c *tcCrafted) {
			if err := os.Chmod(filepath.Join(e.parent(), c.intentName()), 0o644); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "intent is a symlink", edit: func(t *testing.T, e *tcEnv, c *tcCrafted) {
			real := filepath.Join(e.dataDir, "real-intent.json")
			if err := os.Rename(filepath.Join(e.parent(), c.intentName()), real); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(real, filepath.Join(e.parent(), c.intentName())); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "intent hard-linked elsewhere", edit: func(t *testing.T, e *tcEnv, c *tcCrafted) {
			if err := os.Link(filepath.Join(e.parent(), c.intentName()), filepath.Join(e.dataDir, "intent-link")); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "intent is a directory", edit: func(t *testing.T, e *tcEnv, c *tcCrafted) {
			if err := os.Remove(filepath.Join(e.parent(), c.intentName())); err != nil {
				t.Fatal(err)
			}
			if err := os.Mkdir(filepath.Join(e.parent(), c.intentName()), 0o700); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "over the entry cap", bare: true, edit: func(t *testing.T, e *tcEnv, _ *tcCrafted) {
			_ = os.Mkdir(e.parent(), 0o700)
			for i := range toolCheckMaxEntries + 1 {
				tcWriteRaw(t, e, fmt.Sprintf("e%02d", i), []byte("x"), 0o600)
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := newTCEnv(t)
			var c *tcCrafted
			if !tc.bare {
				c = tcCraft(t, e)
			}
			tc.edit(t, e, c)
			before := tcTree(t, e.parent())
			beforeData := tcTree(t, e.dataDir)
			report, err := e.run(t)
			if !errors.Is(err, ErrToolSelfCheck) {
				t.Fatalf("ToolSelfCheck = %v, want ErrToolSelfCheck", err)
			}
			if len(report.Steps) != 0 {
				t.Fatalf("steps %q: recovery itself must refuse an unprovable parent", report.Steps)
			}
			if after := tcTree(t, e.parent()); !maps.Equal(before, after) {
				t.Fatalf("a refused parent changed:\nbefore %v\n after %v", before, after)
			}
			if after := tcTree(t, e.dataDir); !maps.Equal(beforeData, after) {
				t.Fatalf("a refused parent changed the data root:\nbefore %v\n after %v", beforeData, after)
			}
			if calls := e.imager.callsCopy(); len(calls) != 0 {
				t.Fatalf("a refused parent ran tools: %+v", calls)
			}
		})
	}
}

// TestToolSelfCheckRefusesBadParent: a parent that is not the node's own
// 0700 directory fails the check and is left alone.
func TestToolSelfCheckRefusesBadParent(t *testing.T) {
	for _, tc := range []struct {
		name string
		make func(t *testing.T, e *tcEnv)
	}{
		{"symlink", func(t *testing.T, e *tcEnv) {
			target := filepath.Join(e.dataDir, "elsewhere")
			if err := os.Mkdir(target, 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(target, e.parent()); err != nil {
				t.Fatal(err)
			}
		}},
		{"mode 0755", func(t *testing.T, e *tcEnv) {
			if err := os.Mkdir(e.parent(), 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.Chmod(e.parent(), 0o755); err != nil {
				t.Fatal(err)
			}
		}},
		{"regular file", func(t *testing.T, e *tcEnv) {
			if err := os.WriteFile(e.parent(), []byte("x"), 0o600); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := newTCEnv(t)
			tc.make(t, e)
			before := tcTree(t, e.dataDir)
			if _, err := e.run(t); !errors.Is(err, ErrToolSelfCheck) {
				t.Fatalf("ToolSelfCheck = %v, want ErrToolSelfCheck", err)
			}
			if after := tcTree(t, e.dataDir); !maps.Equal(before, after) {
				t.Fatalf("a bad parent changed the data root:\nbefore %v\n after %v", before, after)
			}
			if len(e.imager.callsCopy()) != 0 {
				t.Fatal("a bad parent ran tools")
			}
		})
	}
}

// TestToolSelfCheckRefusesRaceBeforeUnlink: a name swapped between the
// validation and its unlink is refused by the re-check and preserved.
func TestToolSelfCheckRefusesRaceBeforeUnlink(t *testing.T) {
	for _, kind := range []string{"overlay", "intent"} {
		t.Run(kind, func(t *testing.T) {
			e := newTCEnv(t)
			c := tcCraft(t, e)
			name := c.intentName()
			if kind == "overlay" {
				name = c.intent.Fixtures[0].Name
			}
			var swapped Identity
			e.store.tc.fault = func(p string) error {
				if p == toolCheckPointBeforeUnlink+":"+kind && swapped == (Identity{}) {
					path := filepath.Join(e.parent(), name)
					if err := os.Remove(path); err != nil {
						return err
					}
					if err := os.WriteFile(path, []byte("swapped in"), 0o600); err != nil {
						return err
					}
					swapped = tcIdentity(t, path)
				}
				return nil
			}
			if _, err := e.run(t); !errors.Is(err, ErrToolSelfCheck) {
				t.Fatalf("ToolSelfCheck = %v, want ErrToolSelfCheck", err)
			}
			if swapped == (Identity{}) {
				t.Fatal("the race seam never ran")
			}
			if got := tcIdentity(t, filepath.Join(e.parent(), name)); got != swapped {
				t.Fatalf("the swapped-in %s was removed or replaced", name)
			}
			if len(e.imager.callsCopy()) != 0 {
				t.Fatal("tools ran after a refused recovery")
			}
		})
	}
}

// TestToolSelfCheckRunsUnderTheStoreLock is the lock positive control: while
// a self-check tool runs, a second Open of the data directory is refused.
func TestToolSelfCheckRunsUnderTheStoreLock(t *testing.T) {
	e := newTCEnv(t)
	var second error
	ran := false
	e.imager.on = func(context.Context, tcCall) error {
		if !ran {
			ran = true
			s, err := Open(Config{DataDir: e.dataDir, ImageDir: e.imageDir, Imager: e.imager})
			if err == nil {
				s.Close()
			}
			second = err
		}
		return nil
	}
	if _, err := e.run(t); err != nil {
		t.Fatal(err)
	}
	if !ran || !errors.Is(second, ErrLocked) {
		t.Fatalf("second Open during the self-check = %v (ran %v), want ErrLocked", second, ran)
	}
	// Negative-to-positive: after Close the lock is free again.
	e.store.Close()
	s, err := Open(Config{DataDir: e.dataDir, ImageDir: e.imageDir, Imager: e.imager})
	if err != nil {
		t.Fatalf("Open after Close: %v", err)
	}
	s.Close()
	e.store = nil
}

// TestToolSelfCheckRefusesPlantBeforePublish: an entry appearing just
// before the intent's exclusive link is caught by the published-state
// re-check; nothing is published and the plant is preserved.
func TestToolSelfCheckRefusesPlantBeforePublish(t *testing.T) {
	for _, point := range []string{toolCheckPointIntentWritten, toolCheckPointIntentLink} {
		t.Run(point, func(t *testing.T) {
			e := newTCEnv(t)
			planted := false
			e.store.tc.fault = func(p string) error {
				if p == point && !planted {
					planted = true
					return os.WriteFile(filepath.Join(e.parent(), "planted"), []byte("x"), 0o600)
				}
				return nil
			}
			if _, err := e.run(t); !errors.Is(err, ErrToolSelfCheck) {
				t.Fatalf("ToolSelfCheck = %v, want ErrToolSelfCheck", err)
			}
			if tree := tcTree(t, e.parent()); len(tree) != 1 || tree["planted"] == (tcEntry{}) {
				t.Fatalf("parent holds %v, want only the preserved plant", slices.Sorted(maps.Keys(tree)))
			}
			if len(e.imager.callsCopy()) != 0 {
				t.Fatal("tools ran after a refused publish")
			}
		})
	}
}

// TestToolSelfCheckCancelAtLinkBoundaries: cancellation witnessed at a link
// boundary publishes no further name, runs no tool, returns Canceled, and
// the detached cleanup removes only what was already published.
func TestToolSelfCheckCancelAtLinkBoundaries(t *testing.T) {
	for _, tc := range []struct {
		point     string
		forbidden []string // points that must not follow the cancellation
	}{
		{toolCheckPointIntentLink, []string{toolCheckPointIntentLinked, toolCheckPointFixtureLinked + ":overlay", toolCheckPointFixtureLinked + ":absent"}},
		{toolCheckPointFixtureLink + ":overlay", []string{toolCheckPointFixtureLinked + ":overlay", toolCheckPointFixtureLinked + ":absent"}},
		{toolCheckPointFixtureLink + ":absent", []string{toolCheckPointFixtureLinked + ":absent", toolCheckPointFixturesBarrier}},
	} {
		t.Run(tc.point, func(t *testing.T) {
			e := newTCEnv(t)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			var after []string
			canceled := false
			var published []string
			e.store.tc.fault = func(p string) error {
				if canceled {
					after = append(after, p)
					return nil
				}
				if p == tc.point {
					canceled = true
					published = slices.Sorted(maps.Keys(tcTree(t, e.parent())))
					cancel()
				}
				return nil
			}
			if _, err := e.store.ToolSelfCheck(ctx, e.img); !errors.Is(err, context.Canceled) {
				t.Fatalf("ToolSelfCheck = %v, want context.Canceled", err)
			}
			if !canceled {
				t.Fatal("the boundary was never reached")
			}
			for _, p := range tc.forbidden {
				if slices.Contains(after, p) {
					t.Fatalf("after cancellation at %s the run reached %s (%q)", tc.point, p, after)
				}
			}
			if calls := e.imager.callsCopy(); len(calls) != 0 {
				t.Fatalf("tools ran after cancellation: %+v", calls)
			}
			if tree := tcTree(t, e.parent()); len(tree) != 0 {
				t.Fatalf("cleanup left %v (published before cancellation: %q)", slices.Sorted(maps.Keys(tree)), published)
			}
		})
	}
}

// TestToolSelfCheckCancelAtEveryBoundary: cancellation witnessed at any
// boundary before the cleanup starts no new allocation, publication or
// tool: no later link point, no further imager call, Canceled, and the
// detached cleanup leaves the parent empty.
func TestToolSelfCheckCancelAtEveryBoundary(t *testing.T) {
	cleanup := slices.Index(tcAllPoints, "step:"+toolCheckStepCleanup)
	for _, point := range tcAllPoints[:cleanup] {
		t.Run(point, func(t *testing.T) {
			e := newTCEnv(t)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			canceled := false
			callsAt := -1
			var after []string
			e.store.tc.fault = func(p string) error {
				if canceled {
					after = append(after, p)
				} else if p == point {
					canceled = true
					callsAt = len(e.imager.callsCopy())
					cancel()
				}
				return nil
			}
			if _, err := e.store.ToolSelfCheck(ctx, e.img); !errors.Is(err, context.Canceled) {
				t.Fatalf("ToolSelfCheck = %v, want context.Canceled", err)
			}
			if !canceled {
				t.Fatal("the boundary was never reached")
			}
			if calls := e.imager.callsCopy(); len(calls) != callsAt {
				t.Fatalf("imager calls after cancellation at %s: %+v", point, calls[callsAt:])
			}
			for _, p := range after {
				if strings.HasSuffix(strings.SplitN(p, ":", 2)[0], "-linked") || p == toolCheckPointFixturesTmp || p == toolCheckPointIntentWritten || strings.HasPrefix(p, "step:") && p != "step:"+toolCheckStepCleanup {
					t.Fatalf("after cancellation at %s the run reached %s (%q)", point, p, after)
				}
			}
			if tree := tcTree(t, e.parent()); len(tree) != 0 {
				t.Fatalf("cleanup left %v", slices.Sorted(maps.Keys(tree)))
			}
		})
	}
}
