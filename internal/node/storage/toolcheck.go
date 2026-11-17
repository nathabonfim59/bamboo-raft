package storage

import (
	"cmp"
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
	"uuid"

	"golang.org/x/sys/unix"
)

// The installed qemu-img self-check (A10 D1): at startup, under the Store
// lock and after earlier tool attempts were settled, the node runs the real
// imager on fixtures it creates over a verified installed base, and fails
// startup on any semantic mismatch (never a fallback).
//
// Ownership (rev3): every fixture and the intent start as unnamed O_TMPFILE
// inodes under the fixed parent toolcheck/; one immutable intent records
// their identities and is published by an exclusive link before any fixture
// gets a name. Recovery and cleanup are one routine: a bounded, complete
// listing is validated in full (entries, inodes, holds) before the first
// mutation, and only a recorded identity, re-checked immediately before the
// unlink, authorizes a removal.

// toolCheckDir is the self-check's fixed parent under the data root.
const toolCheckDir = "toolcheck"

// Self-check bounds.
const (
	toolCheckTimeout        = 60 * time.Second
	toolCheckCleanupTimeout = 10 * time.Second
	// toolCheckMaxEntries caps the parent's listing; more is fatal.
	toolCheckMaxEntries = 8
	// toolCheckMaxIntent caps the intent's size in bytes.
	toolCheckMaxIntent = 4096
	toolCheckSchema    = 1
)

// ErrToolSelfCheck marks a self-check failure: a semantic mismatch of the
// installed qemu-img or fixture state the node cannot prove it owns.
// Startup fails on it and never falls back.
var ErrToolSelfCheck = errors.New("storage: installed qemu-img self-check failed")

// ToolSelfCheckReport is what the self-check showed, for the startup log.
type ToolSelfCheckReport struct {
	// Image is the image id whose verified base was used.
	Image string
	// Recovered is the number of earlier fixture names removed by their
	// recorded identity (fixtures and intent).
	Recovered int
	// Steps are the semantic names that passed, in order.
	Steps []string
	// Elapsed is the self-check's wall time.
	Elapsed time.Duration
}

// toolCheckIntent is the immutable, durable record of one self-check
// attempt: the parent's identity and every fixture's identity, written
// before any fixture has a name.
type toolCheckIntent struct {
	Schema   int                `json:"schema"`
	Attempt  string             `json:"attempt"`
	Parent   Identity           `json:"parent"`
	Fixtures []toolCheckFixture `json:"fixtures"`
}

// toolCheckFixture is one recorded fixture inode and the name it is linked
// at.
type toolCheckFixture struct {
	Name string `json:"name"`
	Dev  uint64 `json:"dev"`
	Ino  uint64 `json:"ino"`
}

// errToolCheckAbandon, returned (wrapped) by a fault hook, makes the
// self-check return at once without cleanup: a simulated crash.
var errToolCheckAbandon = errors.New("storage: self-check abandoned (simulated crash)")

// toolCheckHooks are the self-check's test seams; the zero value is
// production.
type toolCheckHooks struct {
	// fault runs at each named point (toolCheckPoint*). An error wrapping
	// errToolCheckAbandon returns at once; any other error fails that point
	// like an I/O error (with cleanup).
	fault func(point string) error
	// sync is the directory barrier on dir ("." for the data root or
	// toolCheckDir). nil: dir.Sync.
	sync func(name string, dir *os.File) error
	// timeout and cleanupTimeout override the bounds (0: the defaults).
	timeout, cleanupTimeout time.Duration
}

// Self-check fault points, in the order a full run reaches them. Points
// taking a fixture kind are suffixed ":overlay" or ":absent"; step points
// are "step:<name>".
const (
	toolCheckPointParentCreated   = "parent-created"    // after Mkdir, before the data-root barrier
	toolCheckPointParentBarrier   = "parent-barrier"    // after the data-root barrier
	toolCheckPointEntryBarrier    = "entry-barrier"     // after the parent's own barrier at entry
	toolCheckPointFixturesTmp     = "fixtures-tmpfile"  // after both unnamed fixture inodes exist
	toolCheckPointIntentWritten   = "intent-written"    // after the unnamed intent inode is written and synced
	toolCheckPointIntentLink      = "intent-link"       // immediately before the intent's exclusive link
	toolCheckPointIntentLinked    = "intent-linked"     // after the link, before its barrier
	toolCheckPointIntentBarrier   = "intent-barrier"    // after the intent's barrier
	toolCheckPointFixtureLink     = "fixture-link"      // immediately before a fixture's link (":kind")
	toolCheckPointFixtureLinked   = "fixture-linked"    // after a fixture's link (":kind")
	toolCheckPointFixturesBarrier = "fixtures-barrier"  // after the fixtures' barrier, before any tool
	toolCheckPointBeforeUnlink    = "before-unlink"     // after phase 1, before a removal's re-check (":name kind")
	toolCheckPointUnlinked        = "unlinked"          // after a fixture's unlink (":kind")
	toolCheckPointFixturesGone    = "fixtures-unlinked" // after the barrier for the fixture unlinks
	toolCheckPointIntentUnlinked  = "intent-unlinked"   // after the intent's unlink, before the final barrier
	toolCheckPointFinalBarrier    = "final-barrier"     // after the final barrier
)

// Self-check step names (ToolSelfCheckReport.Steps).
const (
	toolCheckStepRecover      = "recover"
	toolCheckStepCreate       = "create-overlay"
	toolCheckStepInfo         = "info-no-backing"
	toolCheckStepCheck        = "check-graph"
	toolCheckStepResize       = "resize-graph"
	toolCheckStepCreateAbsent = "create-absent"
	toolCheckStepInfoAbsent   = "info-absent-backing"
	toolCheckStepCleanup      = "cleanup"
)

// ToolSelfCheck recovers earlier self-check fixtures by recorded identity,
// checks the installed qemu-img semantics through the Store's imager on
// fresh fixtures over img's verified base, and removes exactly those
// fixtures on every outcome it can prove; fixtures a tool still holds (or a
// retained tool's) are preserved and fail the check. The caller holds the
// Store (startup) and has settled earlier tool attempts.
//
// A context that ends is never success: the cleanup then runs on a bounded
// context detached from ctx and still proves that no tool holds a fixture.
func (s *Store) ToolSelfCheck(ctx context.Context, img Image) (ToolSelfCheckReport, error) {
	start := time.Now()
	report := ToolSelfCheckReport{Image: img.ID}
	if !ValidImageID(img.ID) || img.RootVirtualSize == 0 {
		return report, fmt.Errorf("%w: image %q has no verified base", ErrToolSelfCheck, img.ID)
	}
	ctx, cancel := context.WithTimeout(ctx, cmp.Or(s.tc.timeout, toolCheckTimeout))
	defer cancel()
	p, err := s.toolCheckParent(ctx)
	if err != nil {
		return report, toolCheckErr(err)
	}
	defer p.dir.Close()
	n, err := s.toolCheckSweep(ctx, p)
	report.Recovered = n
	if err != nil {
		return report, toolCheckErr(err)
	}
	report.Steps = append(report.Steps, toolCheckStepRecover)

	named, runErr := s.toolCheckRun(ctx, p, img, &report)
	if errors.Is(runErr, errToolCheckAbandon) {
		return report, runErr
	}
	if runErr == nil {
		runErr = s.tcFault("step:" + toolCheckStepCleanup)
		if errors.Is(runErr, errToolCheckAbandon) {
			return report, runErr
		}
	}
	var cleanErr error
	switch {
	case !named:
		// Nothing was published: the unnamed inodes went with their fds.
	case errors.Is(runErr, ErrToolRetained):
		cleanErr = errors.New("storage: self-check: a retained tool keeps its fixture; intent and fixtures preserved")
	default:
		cctx, ccancel := context.WithTimeout(context.WithoutCancel(ctx), cmp.Or(s.tc.cleanupTimeout, toolCheckCleanupTimeout))
		_, cleanErr = s.toolCheckSweep(cctx, p)
		ccancel()
		if errors.Is(cleanErr, errToolCheckAbandon) {
			return report, cleanErr
		}
	}
	if err := errors.Join(runErr, cleanErr); err != nil {
		return report, toolCheckErr(err)
	}
	// Terminal success rechecks the original bound: the cleanup may finish
	// on its detached context, but an ended ctx is never a passing report.
	if err := ctx.Err(); err != nil {
		return report, toolCheckErr(fmt.Errorf("storage: self-check ended: %w", context.Cause(ctx)))
	}
	report.Steps = append(report.Steps, toolCheckStepCleanup)
	report.Elapsed = time.Since(start)
	return report, nil
}

// ctxLive fails once ctx has ended. It runs after every fault boundary and
// before every mutation, publication and tool call: a canceled self-check
// starts nothing new (only the detached cleanup may still remove proven,
// unheld names).
func ctxLive(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("storage: self-check: %w", context.Cause(ctx))
	}
	return nil
}

// toolCheckErr marks err as a self-check failure; a simulated crash passes
// through unchanged.
func toolCheckErr(err error) error {
	if errors.Is(err, errToolCheckAbandon) || errors.Is(err, ErrToolSelfCheck) {
		return err
	}
	return fmt.Errorf("%w: %w", ErrToolSelfCheck, err)
}

func (s *Store) tcFault(point string) error {
	if s.tc.fault == nil {
		return nil
	}
	return s.tc.fault(point)
}

func (s *Store) tcSync(name string, dir *os.File) error {
	if s.tc.sync != nil {
		return s.tc.sync(name, dir)
	}
	return dir.Sync()
}

// toolCheckParentDir is the verified, open fixed parent.
type toolCheckParentDir struct {
	dir *os.File
	id  Identity
}

func (p *toolCheckParentDir) fd() int { return int(p.dir.Fd()) }

// toolCheckParent makes or adopts the fixed parent: it must be a real
// directory owned by the node with mode 0700. The data root's barrier runs
// on both the creating and the existing path (a crash may have skipped it),
// and so does the parent's own barrier (in the sweep).
func (s *Store) toolCheckParent(ctx context.Context) (*toolCheckParentDir, error) {
	if err := ctxLive(ctx); err != nil {
		return nil, err
	}
	created := true
	if err := s.root.Mkdir(toolCheckDir, 0o700); err != nil {
		if !errors.Is(err, fs.ErrExist) {
			return nil, fmt.Errorf("storage: create %s: %w", toolCheckDir, err)
		}
		created = false
	}
	if created {
		if err := s.tcFault(toolCheckPointParentCreated); err != nil {
			return nil, err
		}
	}
	info, err := s.root.Lstat(toolCheckDir)
	if err != nil {
		return nil, fmt.Errorf("storage: %s: %w", toolCheckDir, err)
	}
	if err := toolCheckOwnedDir(info); err != nil {
		return nil, err
	}
	root, err := openChecked(s.root, ".", os.O_RDONLY, true)
	if err != nil {
		return nil, fmt.Errorf("storage: open data root: %w", err)
	}
	err = s.tcSync(".", root)
	root.Close()
	if err != nil {
		return nil, fmt.Errorf("storage: data root barrier for %s: %w", toolCheckDir, err)
	}
	if err := s.tcFault(toolCheckPointParentBarrier); err != nil {
		return nil, err
	}
	dir, err := openChecked(s.root, toolCheckDir, os.O_RDONLY, true)
	if err != nil {
		return nil, fmt.Errorf("storage: open %s: %w", toolCheckDir, err)
	}
	fail := func(err error) (*toolCheckParentDir, error) {
		dir.Close()
		return nil, err
	}
	opened, err := dir.Stat()
	if err != nil {
		return fail(err)
	}
	if err := toolCheckOwnedDir(opened); err != nil {
		return fail(err)
	}
	id, err := identityOf(dir)
	if err != nil {
		return fail(err)
	}
	return &toolCheckParentDir{dir: dir, id: id}, nil
}

// toolCheckOwnedDir requires the node's own 0700 directory (not a symlink).
func toolCheckOwnedDir(info os.FileInfo) error {
	st, ok := info.Sys().(*syscall.Stat_t)
	switch {
	case !info.Mode().IsDir():
		return fmt.Errorf("storage: %s is a symlink or not a directory; preserved for operator cleanup", toolCheckDir)
	case !ok || int(st.Uid) != os.Geteuid():
		return fmt.Errorf("storage: %s is not owned by the node; preserved for operator cleanup", toolCheckDir)
	case info.Mode()&(fs.ModePerm|fs.ModeSetuid|fs.ModeSetgid|fs.ModeSticky) != 0o700:
		return fmt.Errorf("storage: %s has mode %v, want 0700; preserved for operator cleanup", toolCheckDir, info.Mode())
	}
	return nil
}

// The fixture kinds, a closed set; toolCheckKinds gives their intent order.
const (
	toolCheckKindOverlay = "overlay"
	toolCheckKindAbsent  = "absent"
	toolCheckFixtures    = 2
)

// toolCheckKinds returns the fixture kinds in intent order (a fresh value:
// no package-level mutable state).
func toolCheckKinds() [toolCheckFixtures]string {
	return [toolCheckFixtures]string{toolCheckKindOverlay, toolCheckKindAbsent}
}

func toolCheckFixtureName(kind, attempt string) string { return kind + "-" + attempt + ".qcow2" }

func toolCheckIntentName(attempt string) string { return "intent-" + attempt + ".json" }

// validToolCheckAttempt requires a canonical lowercase UUIDv7.
func validToolCheckAttempt(s string) bool {
	u, err := uuid.Parse(s)
	return err == nil && u.String() == s && u[6]>>4 == 7 && u[8]&0xc0 == 0x80
}

func statIdentity(st *unix.Stat_t) Identity { return Identity{Dev: st.Dev, Ino: st.Ino} }

// statAt is fstatat(dirfd, name) without following a final symlink.
func statAt(dirfd int, name string) (unix.Stat_t, error) {
	var st unix.Stat_t
	err := unix.Fstatat(dirfd, name, &st, unix.AT_SYMLINK_NOFOLLOW)
	return st, err
}

// openRegularAt opens the regular file name under dirfd without following
// a symlink and without blocking; the opened inode must be the one named
// before and after the open.
func openRegularAt(dirfd int, name string, flag int) (*os.File, unix.Stat_t, error) {
	rel := path.Join(toolCheckDir, name)
	before, err := statAt(dirfd, name)
	if err != nil {
		return nil, before, fmt.Errorf("storage: %s: %w", rel, err)
	}
	if before.Mode&unix.S_IFMT != unix.S_IFREG {
		return nil, before, fmt.Errorf("storage: %s is a symlink or not a regular file; preserved for operator cleanup", rel)
	}
	fd, err := unix.Openat(dirfd, name, flag|unix.O_NOFOLLOW|unix.O_NONBLOCK|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, before, fmt.Errorf("storage: open %s: %w", rel, err)
	}
	f := os.NewFile(uintptr(fd), rel)
	var opened unix.Stat_t
	err = unix.Fstat(fd, &opened)
	if err == nil {
		var after unix.Stat_t
		if after, err = statAt(dirfd, name); err == nil && (statIdentity(&opened) != statIdentity(&before) || statIdentity(&after) != statIdentity(&opened)) {
			err = errors.New("changed while it was opened")
		}
	}
	if err != nil {
		f.Close()
		return nil, before, fmt.Errorf("storage: %s: %w", rel, err)
	}
	return f, opened, nil
}

// sameParent re-proves that the open parent is still the one named
// toolCheckDir.
func (s *Store) sameParent(p *toolCheckParentDir) error {
	var st unix.Stat_t
	if err := unix.Fstat(p.fd(), &st); err != nil {
		return fmt.Errorf("storage: %s: %w", toolCheckDir, err)
	}
	info, err := s.root.Lstat(toolCheckDir)
	if err != nil {
		return fmt.Errorf("storage: %s: %w", toolCheckDir, err)
	}
	named, ok := info.Sys().(*syscall.Stat_t)
	if !ok || statIdentity(&st) != p.id || (Identity{Dev: uint64(named.Dev), Ino: named.Ino}) != p.id {
		return fmt.Errorf("storage: %s is no longer the verified directory; preserved for operator cleanup", toolCheckDir)
	}
	return nil
}

// listParent reads the parent's complete listing through a fresh
// descriptor of the verified inode; more than toolCheckMaxEntries is fatal.
func (s *Store) listParent(p *toolCheckParentDir) ([]string, error) {
	fd, err := unix.Openat(p.fd(), ".", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, fmt.Errorf("storage: list %s: %w", toolCheckDir, err)
	}
	d := os.NewFile(uintptr(fd), toolCheckDir)
	defer d.Close()
	var st unix.Stat_t
	if err := unix.Fstat(fd, &st); err != nil || statIdentity(&st) != p.id {
		return nil, fmt.Errorf("storage: list %s: not the verified directory (%v)", toolCheckDir, err)
	}
	var names []string
	for {
		batch, err := d.ReadDir(toolCheckMaxEntries + 1)
		for _, e := range batch {
			names = append(names, e.Name())
		}
		if len(names) > toolCheckMaxEntries {
			return nil, fmt.Errorf("storage: %s holds more than %d entries; preserved for operator cleanup", toolCheckDir, toolCheckMaxEntries)
		}
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("storage: list %s: %w", toolCheckDir, err)
		}
	}
	return names, nil
}

// toolCheckEntry is one validated, pinned name the sweep may remove.
type toolCheckEntry struct {
	label, name string
	file        *os.File
	id          Identity
}

// toolCheckPlan is the outcome of the sweep's validation: the intent (nil
// when the parent is empty) and its present fixtures.
type toolCheckPlan struct {
	intent   *toolCheckEntry
	fixtures []*toolCheckEntry
}

func (pl *toolCheckPlan) close() {
	if pl.intent != nil {
		pl.intent.file.Close()
	}
	for _, f := range pl.fixtures {
		f.file.Close()
	}
}

func (s *Store) held(id Identity) bool {
	return s.cfg.Leases != nil && s.cfg.Leases.TargetHeld(id.Dev, id.Ino)
}

// planSweep validates the complete parent without changing it: at most one
// strictly valid intent, nothing but its recorded fixtures beside it (or
// nothing at all without one), every present fixture its recorded inode
// with one link, and nothing a tool holds.
func (s *Store) planSweep(p *toolCheckParentDir) (_ *toolCheckPlan, err error) {
	names, err := s.listParent(p)
	if err != nil {
		return nil, err
	}
	plan := &toolCheckPlan{}
	defer func() {
		if err != nil {
			plan.close()
		}
	}()
	var intentName, attempt string
	for _, n := range names {
		a, ok := strings.CutPrefix(n, "intent-")
		if a, ok2 := strings.CutSuffix(a, ".json"); ok && ok2 && validToolCheckAttempt(a) {
			if intentName != "" {
				return nil, fmt.Errorf("storage: %s holds two intents (%s, %s); preserved for operator cleanup", toolCheckDir, intentName, n)
			}
			intentName, attempt = n, a
		}
	}
	if intentName == "" {
		if len(names) != 0 {
			return nil, fmt.Errorf("storage: %s holds %q without an intent; preserved for operator cleanup", toolCheckDir, names[0])
		}
		return plan, nil
	}
	intent, v, err := s.loadIntent(p, intentName, attempt)
	if err != nil {
		return nil, err
	}
	plan.intent = intent
	recorded := map[string]Identity{}
	for _, f := range v.Fixtures {
		recorded[f.Name] = Identity{Dev: f.Dev, Ino: f.Ino}
	}
	for _, n := range names {
		if n != intentName {
			if _, ok := recorded[n]; !ok {
				return nil, fmt.Errorf("storage: %s holds foreign %q; preserved for operator cleanup", toolCheckDir, n)
			}
		}
	}
	for i, f := range v.Fixtures {
		if _, err := statAt(p.fd(), f.Name); errors.Is(err, unix.ENOENT) {
			continue // an earlier sweep removed it
		}
		file, st, err := openRegularAt(p.fd(), f.Name, os.O_RDONLY)
		if err != nil {
			return nil, err
		}
		entry := &toolCheckEntry{label: toolCheckKinds()[i], name: f.Name, file: file, id: statIdentity(&st)}
		plan.fixtures = append(plan.fixtures, entry)
		switch {
		case entry.id != recorded[f.Name]:
			return nil, fmt.Errorf("storage: %s/%s is not the recorded inode; preserved for operator cleanup", toolCheckDir, f.Name)
		case st.Nlink != 1:
			return nil, fmt.Errorf("storage: %s/%s has %d links; preserved for operator cleanup", toolCheckDir, f.Name, st.Nlink)
		case s.held(entry.id):
			return nil, fmt.Errorf("storage: a jailed tool still holds %s/%s; preserved", toolCheckDir, f.Name)
		}
	}
	return plan, nil
}

// loadIntent opens and strictly decodes the intent name (attempt parsed
// from it).
func (s *Store) loadIntent(p *toolCheckParentDir, name, attempt string) (*toolCheckEntry, *toolCheckIntent, error) {
	bad := func(format string, args ...any) error {
		return fmt.Errorf("storage: %s/%s: %s; preserved for operator cleanup", toolCheckDir, name, fmt.Sprintf(format, args...))
	}
	f, st, err := openRegularAt(p.fd(), name, os.O_RDONLY)
	if err != nil {
		return nil, nil, err
	}
	entry := &toolCheckEntry{label: "intent", name: name, file: f, id: statIdentity(&st)}
	fail := func(err error) (*toolCheckEntry, *toolCheckIntent, error) {
		f.Close()
		return nil, nil, err
	}
	switch {
	case int(st.Uid) != os.Geteuid():
		return fail(bad("not owned by the node"))
	case st.Mode&0o7777 != 0o600:
		return fail(bad("mode %#o, want 0600", st.Mode&0o7777))
	case st.Nlink != 1:
		return fail(bad("%d links", st.Nlink))
	case st.Size <= 0 || st.Size > toolCheckMaxIntent:
		return fail(bad("size %d outside 1..%d", st.Size, toolCheckMaxIntent))
	case s.held(entry.id):
		return fail(bad("a jailed tool holds it"))
	}
	raw, err := io.ReadAll(io.LimitReader(f, toolCheckMaxIntent+1))
	if err != nil {
		return fail(bad("read: %v", err))
	}
	if len(raw) > toolCheckMaxIntent {
		return fail(bad("larger than %d bytes", toolCheckMaxIntent))
	}
	var v toolCheckIntent
	if err := json.Unmarshal(raw, &v, json.RejectUnknownMembers(true)); err != nil {
		return fail(bad("invalid: %v", err))
	}
	if v.Schema != toolCheckSchema || v.Attempt != attempt || v.Parent != p.id || len(v.Fixtures) != toolCheckFixtures {
		return fail(bad("schema, attempt, parent or fixture count does not match"))
	}
	seen := map[Identity]bool{entry.id: true}
	for i, fx := range v.Fixtures {
		id := Identity{Dev: fx.Dev, Ino: fx.Ino}
		if fx.Name != toolCheckFixtureName(toolCheckKinds()[i], attempt) || fx.Ino == 0 || seen[id] {
			return fail(bad("fixture %d is not a distinct recorded %s fixture", i, toolCheckKinds()[i]))
		}
		seen[id] = true
	}
	return entry, &v, nil
}

// toolCheckSweep is recovery and cleanup: the parent's own barrier (repaying
// a failed earlier one), the complete validation, and only then the
// removals, each re-checked immediately before its unlink. It returns the
// number of names removed.
func (s *Store) toolCheckSweep(ctx context.Context, p *toolCheckParentDir) (int, error) {
	if err := s.tcSync(toolCheckDir, p.dir); err != nil {
		return 0, fmt.Errorf("storage: %s barrier: %w", toolCheckDir, err)
	}
	if err := s.tcFault(toolCheckPointEntryBarrier); err != nil {
		return 0, err
	}
	plan, err := s.planSweep(p)
	if err != nil {
		return 0, err
	}
	defer plan.close()
	if plan.intent == nil {
		return 0, nil
	}
	removed := 0
	for _, f := range plan.fixtures {
		if err := s.removeEntry(ctx, p, f); err != nil {
			return removed, err
		}
		removed++
		if err := s.tcFault(toolCheckPointUnlinked + ":" + f.label); err != nil {
			return removed, err
		}
	}
	if err := s.tcSync(toolCheckDir, p.dir); err != nil {
		return removed, fmt.Errorf("storage: %s barrier: %w", toolCheckDir, err)
	}
	if err := s.tcFault(toolCheckPointFixturesGone); err != nil {
		return removed, err
	}
	if err := s.removeEntry(ctx, p, plan.intent); err != nil {
		return removed, err
	}
	removed++
	if err := s.tcFault(toolCheckPointIntentUnlinked); err != nil {
		return removed, err
	}
	if err := s.tcSync(toolCheckDir, p.dir); err != nil {
		return removed, fmt.Errorf("storage: %s barrier: %w", toolCheckDir, err)
	}
	return removed, s.tcFault(toolCheckPointFinalBarrier)
}

// removeEntry unlinks one validated entry after re-proving, immediately
// before the unlink, the parent, the name's inode and that no tool holds it.
func (s *Store) removeEntry(ctx context.Context, p *toolCheckParentDir, e *toolCheckEntry) error {
	if err := s.tcFault(toolCheckPointBeforeUnlink + ":" + e.label); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("storage: remove %s/%s: %w", toolCheckDir, e.name, context.Cause(ctx))
	}
	if err := s.sameParent(p); err != nil {
		return err
	}
	st, err := statAt(p.fd(), e.name)
	if err != nil || statIdentity(&st) != e.id {
		return fmt.Errorf("storage: %s/%s is no longer the validated inode; preserved for operator cleanup", toolCheckDir, e.name)
	}
	if s.held(e.id) {
		return fmt.Errorf("storage: a jailed tool holds %s/%s; preserved", toolCheckDir, e.name)
	}
	if err := unix.Unlinkat(p.fd(), e.name, 0); err != nil {
		return fmt.Errorf("storage: remove %s/%s: %w", toolCheckDir, e.name, err)
	}
	return nil
}

// expectEmpty proves the published state a new attempt starts from: the
// verified parent with no entry at all.
func (s *Store) expectEmpty(p *toolCheckParentDir) error {
	if err := s.sameParent(p); err != nil {
		return err
	}
	names, err := s.listParent(p)
	if err != nil {
		return err
	}
	if len(names) != 0 {
		return fmt.Errorf("storage: %s gained %q before the attempt was published; preserved for operator cleanup", toolCheckDir, names[0])
	}
	return nil
}

// tmpfile makes an unnamed 0600 inode in the parent.
func tmpfile(p *toolCheckParentDir) (*os.File, Identity, error) {
	fd, err := unix.Openat(p.fd(), ".", unix.O_TMPFILE|unix.O_RDWR|unix.O_CLOEXEC, 0o600)
	if err != nil {
		return nil, Identity{}, fmt.Errorf("storage: %s needs O_TMPFILE support (no named fallback): %w", toolCheckDir, err)
	}
	f := os.NewFile(uintptr(fd), toolCheckDir+"/(unnamed)")
	var st unix.Stat_t
	err = unix.Fchmod(fd, 0o600)
	if err == nil {
		err = unix.Fstat(fd, &st)
	}
	if err == nil && (st.Mode&unix.S_IFMT != unix.S_IFREG || st.Nlink != 0) {
		err = errors.New("not an unnamed regular file")
	}
	if err != nil {
		f.Close()
		return nil, Identity{}, fmt.Errorf("storage: %s tmpfile: %w", toolCheckDir, err)
	}
	return f, statIdentity(&st), nil
}

// linkExclusive publishes the unnamed inode f at name: linkat never
// replaces an existing name (EEXIST), and the name must then be f with one
// link. named reports whether the link was made.
func linkExclusive(p *toolCheckParentDir, f *os.File, id Identity, name string) (named bool, err error) {
	if err := unix.Linkat(unix.AT_FDCWD, "/proc/self/fd/"+strconv.Itoa(int(f.Fd())), p.fd(), name, unix.AT_SYMLINK_FOLLOW); err != nil {
		return false, fmt.Errorf("storage: publish %s/%s: %w", toolCheckDir, name, err)
	}
	var st unix.Stat_t
	if err := unix.Fstat(int(f.Fd()), &st); err != nil || st.Nlink != 1 {
		return true, fmt.Errorf("storage: published %s/%s does not have exactly one link (%v)", toolCheckDir, name, err)
	}
	if named, err := statAt(p.fd(), name); err != nil || statIdentity(&named) != id {
		return true, fmt.Errorf("storage: published %s/%s is not its inode", toolCheckDir, name)
	}
	return true, nil
}

// toolCheckRun publishes one attempt and runs its steps. named reports
// whether any name was published (the sweep then cleans up).
func (s *Store) toolCheckRun(ctx context.Context, p *toolCheckParentDir, img Image, report *ToolSelfCheckReport) (named bool, err error) {
	attempt := uuid.NewV7().String()
	if err := s.expectEmpty(p); err != nil {
		return false, err
	}
	var fixtures [toolCheckFixtures]*os.File
	var ids [toolCheckFixtures]Identity
	defer func() {
		for _, f := range fixtures {
			if f != nil {
				f.Close()
			}
		}
	}()
	intent := toolCheckIntent{Schema: toolCheckSchema, Attempt: attempt, Parent: p.id}
	for i, kind := range toolCheckKinds() {
		if err := ctxLive(ctx); err != nil {
			return false, err
		}
		if fixtures[i], ids[i], err = tmpfile(p); err != nil {
			return false, err
		}
		intent.Fixtures = append(intent.Fixtures, toolCheckFixture{Name: toolCheckFixtureName(kind, attempt), Dev: ids[i].Dev, Ino: ids[i].Ino})
	}
	if err := s.tcFault(toolCheckPointFixturesTmp); err != nil {
		return false, err
	}
	if err := ctxLive(ctx); err != nil {
		return false, err
	}
	raw, err := json.Marshal(intent)
	if err != nil {
		return false, err
	}
	if len(raw) > toolCheckMaxIntent {
		return false, fmt.Errorf("storage: self-check intent is %d bytes", len(raw))
	}
	if err := s.expectEmpty(p); err != nil {
		return false, err
	}
	intentFile, intentID, err := tmpfile(p)
	if err != nil {
		return false, err
	}
	defer intentFile.Close()
	if _, err := intentFile.Write(raw); err != nil {
		return false, fmt.Errorf("storage: write self-check intent: %w", err)
	}
	if err := s.cfg.Snapshots.SyncFile(intentFile); err != nil {
		return false, fmt.Errorf("storage: sync self-check intent: %w", err)
	}
	if err := s.tcFault(toolCheckPointIntentWritten); err != nil {
		return false, err
	}
	intentName := toolCheckIntentName(attempt)
	if err := s.tcFault(toolCheckPointIntentLink); err != nil {
		return false, err
	}
	if err := ctxLive(ctx); err != nil {
		return false, err
	}
	if err := s.expectEmpty(p); err != nil {
		return false, err
	}
	named, err = linkExclusive(p, intentFile, intentID, intentName)
	if err != nil {
		return named, err
	}
	if err := s.tcFault(toolCheckPointIntentLinked); err != nil {
		return true, err
	}
	if err := s.tcSync(toolCheckDir, p.dir); err != nil {
		return true, fmt.Errorf("storage: %s barrier: %w", toolCheckDir, err)
	}
	if err := s.tcFault(toolCheckPointIntentBarrier); err != nil {
		return true, err
	}
	for i, kind := range toolCheckKinds() {
		if err := s.tcFault(toolCheckPointFixtureLink + ":" + kind); err != nil {
			return true, err
		}
		if err := ctxLive(ctx); err != nil {
			return true, err
		}
		if err := s.sameParent(p); err != nil {
			return true, err
		}
		if st, err := statAt(p.fd(), intentName); err != nil || statIdentity(&st) != intentID {
			return true, fmt.Errorf("storage: %s/%s is no longer the published intent", toolCheckDir, intentName)
		}
		if _, err := linkExclusive(p, fixtures[i], ids[i], intent.Fixtures[i].Name); err != nil {
			return true, err
		}
		if err := s.tcFault(toolCheckPointFixtureLinked + ":" + kind); err != nil {
			return true, err
		}
	}
	if err := s.tcSync(toolCheckDir, p.dir); err != nil {
		return true, fmt.Errorf("storage: %s barrier: %w", toolCheckDir, err)
	}
	if err := s.tcFault(toolCheckPointFixturesBarrier); err != nil {
		return true, err
	}
	if err := ctxLive(ctx); err != nil {
		return true, err
	}
	return true, s.toolCheckSteps(ctx, p, img, attempt, fixtures, ids, report)
}

// toolCheckSteps runs the semantic checks on the published fixtures.
func (s *Store) toolCheckSteps(ctx context.Context, p *toolCheckParentDir, img Image, attempt string, rw [toolCheckFixtures]*os.File, ids [toolCheckFixtures]Identity, report *ToolSelfCheckReport) error {
	base, err := s.openImageBase(img.ID)
	if err != nil {
		return err
	}
	defer base.Close()
	baseName := s.imageBaseName(img.ID)
	absentName := "absent-base-" + attempt
	absentBacking := filepath.Join(s.dataDir, toolCheckDir, absentName)
	names := [toolCheckFixtures]string{toolCheckFixtureName(toolCheckKinds()[0], attempt), toolCheckFixtureName(toolCheckKinds()[1], attempt)}
	ref := func(i int, f *os.File) ImageRef {
		return ImageRef{File: f, Root: ImageRootData, Path: path.Join(toolCheckDir, names[i]), Format: "qcow2"}
	}
	step := func(name string, run func() error) error {
		if err := s.tcFault("step:" + name); err != nil {
			return err
		}
		if err := ctxLive(ctx); err != nil {
			return err
		}
		if err := run(); err != nil {
			return fmt.Errorf("storage: self-check %s: %w", name, err)
		}
		// A run whose context ended is never success, whatever the tool did.
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("storage: self-check %s: %w", name, context.Cause(ctx))
		}
		report.Steps = append(report.Steps, name)
		return nil
	}
	info := func(i int, backing string, size uint64) error {
		f, st, err := openRegularAt(p.fd(), names[i], os.O_RDONLY)
		if err != nil {
			return err
		}
		defer f.Close()
		if statIdentity(&st) != ids[i] {
			return fmt.Errorf("%w: %s is not its recorded inode", ErrToolSelfCheck, names[i])
		}
		r := ref(i, f)
		r.ExpectedBacking, r.Base = backing, base
		if err := ctxLive(ctx); err != nil {
			return err
		}
		q, err := s.imager.Info(ctx, r)
		if err != nil {
			return err
		}
		if q.Format != "qcow2" || q.VirtualSize != size || q.BackingFile != backing {
			return fmt.Errorf("%w: info reports %s, %d bytes, backing %q; want qcow2, %d bytes, backing %q", ErrToolSelfCheck, q.Format, q.VirtualSize, q.BackingFile, size, backing)
		}
		return nil
	}
	stillAbsent := func() error {
		if _, err := statAt(p.fd(), absentName); !errors.Is(err, unix.ENOENT) {
			return fmt.Errorf("%w: the absent backing %s exists (%v)", ErrToolSelfCheck, absentBacking, err)
		}
		return nil
	}
	size, grown := img.RootVirtualSize, img.RootVirtualSize+1<<20
	if err := step(toolCheckStepCreate, func() error {
		r := ref(0, rw[0])
		r.BackingName = baseName
		return s.imager.CreateOverlay(ctx, r, size)
	}); err != nil {
		return err
	}
	if err := step(toolCheckStepInfo, func() error { return info(0, baseName, size) }); err != nil {
		return err
	}
	if err := step(toolCheckStepCheck, func() error {
		f, st, err := openRegularAt(p.fd(), names[0], os.O_RDONLY)
		if err != nil {
			return err
		}
		defer f.Close()
		if statIdentity(&st) != ids[0] {
			return fmt.Errorf("%w: %s is not its recorded inode", ErrToolSelfCheck, names[0])
		}
		r := ref(0, f)
		r.ExpectedBacking, r.Base = baseName, base
		return s.imager.Check(ctx, r)
	}); err != nil {
		return err
	}
	if err := step(toolCheckStepResize, func() error {
		r := ref(0, rw[0])
		r.ExpectedBacking, r.Base = baseName, base
		if err := s.imager.Grow(ctx, r, grown); err != nil {
			return err
		}
		return info(0, baseName, grown) // info rechecks ctx before its call
	}); err != nil {
		return err
	}
	if err := step(toolCheckStepCreateAbsent, func() error {
		if err := stillAbsent(); err != nil {
			return err
		}
		r := ref(1, rw[1])
		r.BackingName = absentBacking
		if err := s.imager.CreateOverlay(ctx, r, size); err != nil {
			return err
		}
		return stillAbsent()
	}); err != nil {
		return err
	}
	// info opens no backing: a stored name that does not exist must not
	// matter. Were the installed info to open it, this fails closed.
	return step(toolCheckStepInfoAbsent, func() error {
		if err := info(1, absentBacking, size); err != nil {
			return err
		}
		return stillAbsent()
	})
}
