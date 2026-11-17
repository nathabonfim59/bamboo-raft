package jailer

import (
	"bytes"
	"errors"
	"io/fs"
	"log/slog"
	"os"
	"path"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// These are the first review's fail-before regressions, adapted only to the
// changed internal signatures (stateStore.Write returns the written inode,
// prepareCgroup returns the new group's inode, cgroups behind cgroupOps).

const (
	jailerReviewID  = "bx_aaaaaaaa"
	jailerReviewID2 = "bx_bbbbbbbb"
)

func jailerReviewRecord(id string, uid uint32) Record {
	return Record{
		SchemaVersion: recordSchema,
		SandboxID:     id,
		Attempt:       jailerAttempt(),
		Kind:          KindVM,
		UID:           uid,
		ControlDev:    11,
		ControlIno:    100 + uint64(uid),
		LogDev:        12,
		LogIno:        200 + uint64(uid),
		TargetDev:     13,
		TargetIno:     300 + uint64(uid),
		CreatedUnixMs: 1,
	}
}

func jailerReviewConfig(t *testing.T) Config {
	t.Helper()
	base := t.TempDir()
	return Config{
		StateDir:     filepath.Join(base, "state"),
		CgroupParent: filepath.Join(base, "cgroup"),
		UIDs:         UIDRange{Min: 50000, Max: 50010},
		Reclaim:      jailerNoReclaim,
		Log:          slog.New(slog.DiscardHandler),
	}
}

func jailerReviewRoots(t *testing.T) (*os.Root, *os.Root, string) {
	t.Helper()
	cfg := jailerReviewConfig(t)
	if err := os.MkdirAll(cfg.StateDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(cfg.CgroupParent, 0o700); err != nil {
		t.Fatal(err)
	}
	root, err := os.OpenRoot(cfg.StateDir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = root.Close() })
	for _, name := range []string{recordsDir, jailsDir} {
		if err := root.Mkdir(name, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	cgroup, err := os.OpenRoot(cfg.CgroupParent)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cgroup.Close() })
	return root, cgroup, cfg.StateDir
}

// jailerReviewUncertainStates publishes through the real writer, then
// reports an uncertain outcome, as a parent-directory sync failure would.
type jailerReviewUncertainStates struct {
	inner stateStore
	err   error
	wrote *inodeIdentity
}

func (s jailerReviewUncertainStates) Write(root *os.Root, record Record, ownership resourceOwnership, expected inodeIdentity) (inodeIdentity, error) {
	identity, err := s.inner.Write(root, record, ownership, expected)
	if err != nil {
		return identity, err
	}
	*s.wrote = identity
	return identity, s.err
}

func (s jailerReviewUncertainStates) Remove(root *os.Root, id string, expected inodeIdentity) error {
	return s.inner.Remove(root, id, expected)
}

var errJailerReviewSync = errors.New("injected parent sync uncertainty")

func TestReviewUncertainIntentKeepsUIDReservation(t *testing.T) {
	jailerNeedBinary(t)
	env := newJailerEnv(t)
	m := env.open(t)
	defer m.Close()
	var wrote inodeIdentity
	m.state = jailerReviewUncertainStates{inner: fileStates{}, err: errJailerReviewSync, wrote: &wrote}
	child, _, err := jailerStart(t, m, t.Context(), jailerSpec(t, jailerReviewID))
	if !errors.Is(err, errJailerReviewSync) {
		t.Fatalf("Start = %v, want the injected intent uncertainty", err)
	}
	if child != nil {
		t.Fatal("intent failure unexpectedly spawned a child")
	}
	// Exact boundary witness: the intent was published, nothing after it ran.
	if !wrote.valid() {
		t.Fatal("the intent write was not reached")
	}
	if got := env.stages.last(); got != "" {
		t.Fatalf("Start passed the intent boundary to %q", got)
	}
	current, err := statRegularNoFollow(m.root, recordPath(jailerReviewID))
	if err != nil || !current.equal(wrote) {
		t.Fatalf("published intent %v (err %v), want %v", current, err, wrote)
	}
	record, ok := m.Record(jailerReviewID)
	if !ok {
		t.Fatal("uncertain intent write dropped the in-memory uid reservation")
	}
	uid, err := m.allocateUID()
	if err != nil {
		t.Fatal(err)
	}
	if uid == record.UID {
		t.Fatalf("uncertain record uid %d was offered to another launch", uid)
	}
}

func TestReviewStateWritePreservesForeignFixedTemp(t *testing.T) {
	root, _, _ := jailerReviewRoots(t)
	name := recordPath(jailerReviewID) + ".tmp"
	const foreign = "foreign replacement; do not delete"
	if err := root.WriteFile(name, []byte(foreign), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := (fileStates{}).Write(root, jailerReviewRecord(jailerReviewID, 50000), resourceOwnership{}, inodeIdentity{}); err != nil {
		t.Fatal(err)
	}
	raw, err := root.ReadFile(name)
	if errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("Write deleted foreign fixed temp %s", name)
	}
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) != foreign {
		t.Fatalf("foreign temp changed to %q", raw)
	}
}

func TestReviewNewRejectsInvalidInventory(t *testing.T) {
	tests := []struct {
		name    string
		records []Record
		edit    func(*Record)
		reason  string
	}{
		{name: "uid below configured range", records: []Record{jailerReviewRecord(jailerReviewID, 49999)}, reason: "outside configured range"},
		{name: "uid above configured range", records: []Record{jailerReviewRecord(jailerReviewID, 50011)}, reason: "outside configured range"},
		{name: "duplicate uid", records: []Record{
			jailerReviewRecord(jailerReviewID, 50000),
			jailerReviewRecord(jailerReviewID2, 50000),
		}, reason: "share uid"},
		{name: "duplicate control identity", records: []Record{
			jailerReviewRecord(jailerReviewID, 50000),
			jailerReviewRecord(jailerReviewID2, 50001),
		}, edit: func(r *Record) { r.ControlDev, r.ControlIno = 77, 88 }, reason: "share control identity"},
		{name: "duplicate log identity", records: []Record{
			jailerReviewRecord(jailerReviewID, 50000),
			jailerReviewRecord(jailerReviewID2, 50001),
		}, edit: func(r *Record) { r.LogDev, r.LogIno = 99, 111 }, reason: "share log identity"},
		{name: "valid inventory (positive control)", records: []Record{
			jailerReviewRecord(jailerReviewID, 50000),
			jailerReviewRecord(jailerReviewID2, 50010),
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := jailerReviewConfig(t)
			for _, dir := range []string{cfg.CgroupParent, filepath.Join(cfg.StateDir, recordsDir), filepath.Join(cfg.StateDir, jailsDir)} {
				if err := os.MkdirAll(dir, 0o700); err != nil {
					t.Fatal(err)
				}
			}
			const marker = "untouched"
			if err := os.WriteFile(filepath.Join(cfg.CgroupParent, "cgroup.subtree_control"), []byte(marker), 0o600); err != nil {
				t.Fatal(err)
			}
			root, err := os.OpenRoot(cfg.StateDir)
			if err != nil {
				t.Fatal(err)
			}
			for i := range tt.records {
				record := tt.records[i]
				if tt.edit != nil {
					tt.edit(&record)
				}
				if _, err := (fileStates{}).Write(root, record, resourceOwnership{}, inodeIdentity{}); err != nil {
					root.Close()
					t.Fatal(err)
				}
			}
			if err := root.Close(); err != nil {
				t.Fatal(err)
			}
			m, err := New(cfg)
			if tt.reason == "" {
				if err != nil {
					t.Fatalf("valid inventory rejected: %v", err)
				}
				if got := len(m.Records()); got != len(tt.records) {
					t.Fatalf("loaded %d records, want %d", got, len(tt.records))
				}
				m.Close()
				return
			}
			if err == nil {
				m.Close()
				t.Fatal("New accepted invalid persistent inventory")
			}
			if !strings.Contains(err.Error(), tt.reason) {
				t.Fatalf("New error %q does not name %q", err, tt.reason)
			}
			// Inventory is checked before the cgroup parent is opened or mutated.
			entries, err := os.ReadDir(cfg.CgroupParent)
			if err != nil {
				t.Fatal(err)
			}
			raw, err := os.ReadFile(filepath.Join(cfg.CgroupParent, "cgroup.subtree_control"))
			if err != nil || string(raw) != marker || len(entries) != 1 {
				t.Fatalf("cgroup parent changed: %d entries, subtree_control %q (%v)", len(entries), raw, err)
			}
		})
	}
}

func TestReviewNewSerializesStateDirManagers(t *testing.T) {
	cfg := jailerReviewConfig(t)
	if err := os.MkdirAll(cfg.CgroupParent, 0o700); err != nil {
		t.Fatal(err)
	}
	first, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	lockName := filepath.Join(cfg.StateDir, stateLockName)
	before, err := os.Lstat(lockName)
	if err != nil {
		t.Fatal(err)
	}
	second, err := New(cfg)
	if err == nil {
		second.Close()
		first.Close()
		t.Fatal("two independent managers own the same uid allocator")
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	third, err := New(cfg)
	if err != nil {
		t.Fatalf("Close did not hand the state directory to a new owner: %v", err)
	}
	defer third.Close()
	after, err := os.Lstat(lockName)
	if err != nil {
		t.Fatal(err)
	}
	if !os.SameFile(before, after) || !after.Mode().IsRegular() {
		t.Fatal("the permanent lock inode was replaced across Close")
	}
}

func TestReviewReleasePreservesReplacementJail(t *testing.T) {
	tests := []struct {
		name string
		// persisted reports whether the original jail identity was recorded
		// before the replacement appeared.
		persisted bool
	}{
		{name: "unproven identity", persisted: false},
		{name: "persisted identity then replaced", persisted: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			root, cgroup, _ := jailerReviewRoots(t)
			record := jailerReviewRecord(jailerReviewID, 50000)
			base := path.Join(jailsDir, jailerReviewID)
			if err := root.Mkdir(base, 0o700); err != nil {
				t.Fatal(err)
			}
			var own resourceOwnership
			if tt.persisted {
				identity, err := statDirectoryNoFollow(root, base)
				if err != nil {
					t.Fatal(err)
				}
				own.Jail = identity
			}
			stateID, err := (fileStates{}).Write(root, record, own, inodeIdentity{})
			if err != nil {
				t.Fatal(err)
			}
			if err := root.Rename(base, base+".old-attempt"); err != nil {
				t.Fatal(err)
			}
			if err := root.Mkdir(base, 0o700); err != nil {
				t.Fatal(err)
			}
			const marker = "foreign replacement"
			if err := root.WriteFile(path.Join(base, marker), []byte(marker), 0o600); err != nil {
				t.Fatal(err)
			}
			m := &Manager{
				cfg:          Config{UIDs: UIDRange{Min: 50000, Max: 50010}, Reclaim: jailerNoReclaim},
				log:          slog.New(slog.DiscardHandler),
				root:         root,
				cgroups:      cgroupFS{root: cgroup},
				state:        fileStates{},
				records:      map[string]Record{jailerReviewID: record},
				ownership:    map[string]resourceOwnership{jailerReviewID: own},
				stateEntries: map[string]inodeIdentity{jailerReviewID: stateID},
			}
			if err := m.Release(t.Context(), jailerReviewID); err == nil {
				t.Fatal("Release adopted an unproven replacement jail")
			}
			raw, err := root.ReadFile(path.Join(base, marker))
			if err != nil || string(raw) != marker {
				t.Fatalf("replacement jail marker changed: %q, %v", raw, err)
			}
			if _, ok := m.Record(jailerReviewID); !ok {
				t.Fatal("unsafe release dropped the durable reservation")
			}
			if current, err := statRegularNoFollow(root, recordPath(jailerReviewID)); err != nil || !current.equal(stateID) {
				t.Fatalf("durable record changed: %v, %v", current, err)
			}
		})
	}
}

func TestReviewReleasePreservesReplacementCgroup(t *testing.T) {
	root, cgroup, _ := jailerReviewRoots(t)
	name := cgroupName(jailerReviewID)
	if err := cgroup.Mkdir(name, 0o755); err != nil {
		t.Fatal(err)
	}
	original, err := statDirectoryNoFollow(cgroup, name)
	if err != nil {
		t.Fatal(err)
	}
	record := jailerReviewRecord(jailerReviewID, 50000)
	own := resourceOwnership{Cgroup: original}
	stateID, err := (fileStates{}).Write(root, record, own, inodeIdentity{})
	if err != nil {
		t.Fatal(err)
	}
	if err := cgroup.Rename(name, name+".old"); err != nil {
		t.Fatal(err)
	}
	if err := cgroup.Mkdir(name, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := cgroup.WriteFile(path.Join(name, "cgroup.procs"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	m := &Manager{
		cfg: Config{UIDs: UIDRange{Min: 50000, Max: 50010}, Reclaim: jailerNoReclaim}, log: slog.New(slog.DiscardHandler),
		root: root, cgroups: cgroupFS{root: cgroup}, state: fileStates{},
		records:      map[string]Record{jailerReviewID: record},
		ownership:    map[string]resourceOwnership{jailerReviewID: own},
		stateEntries: map[string]inodeIdentity{jailerReviewID: stateID},
	}
	if err := m.Release(t.Context(), jailerReviewID); err == nil {
		t.Fatal("Release adopted a replaced cgroup")
	}
	if _, err := cgroup.Stat(path.Join(name, "cgroup.procs")); err != nil {
		t.Fatalf("replacement cgroup content removed: %v", err)
	}
	if _, ok := m.Record(jailerReviewID); !ok {
		t.Fatal("unsafe release dropped the reservation")
	}
}

func TestReviewOpenVerifiedPidfdRejectsZeroStartTicks(t *testing.T) {
	bootID, err := hostBootID()
	if err != nil {
		t.Fatal(err)
	}
	f, alive, err := openVerifiedPidfd(Record{PID: os.Getpid(), BootID: bootID})
	if f != nil {
		f.Close()
	}
	if err == nil {
		t.Fatalf("incomplete identity was not rejected (alive=%v)", alive)
	}
	// Positive control: the complete identity of this process verifies.
	ticks, err := procStartTicks(os.Getpid())
	if err != nil {
		t.Fatal(err)
	}
	f, alive, err = openVerifiedPidfd(Record{PID: os.Getpid(), BootID: bootID, StartTicks: ticks})
	if err != nil || !alive || f == nil {
		t.Fatalf("complete identity: alive=%v err=%v", alive, err)
	}
	f.Close()
	// A different start time is a different process: not alive, no pidfd.
	f, alive, err = openVerifiedPidfd(Record{PID: os.Getpid(), BootID: bootID, StartTicks: ticks + 1})
	if err != nil || alive || f != nil {
		t.Fatalf("mismatched ticks: alive=%v f=%v err=%v", alive, f, err)
	}
}

func TestReviewLoadRecordsDoesNotBlockOnSpecialFile(t *testing.T) {
	root, _, stateDir := jailerReviewRoots(t)
	name := filepath.Join(stateDir, recordsDir, jailerReviewID+".json")
	if err := unix.Mkfifo(name, 0o600); err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() {
		_, _, _, err := loadRecords(root)
		result <- err
	}()
	select {
	case err := <-result:
		if err == nil {
			t.Fatal("FIFO record was accepted")
		}
		return
	case <-time.After(5 * time.Second):
		t.Error("loadRecords blocked opening a FIFO; want a bounded non-regular-file refusal")
	}
	// Unstick a blocked reader and join it so no goroutine outlives the test.
	if fd, err := unix.Open(name, unix.O_WRONLY|unix.O_NONBLOCK, 0); err == nil {
		unix.Close(fd)
	}
	select {
	case <-result:
	case <-time.After(5 * time.Second):
		t.Fatal("could not release the blocked reader")
	}
}

func TestReviewPrepareCgroupRefusesForeignExistingDirectory(t *testing.T) {
	cgroupDir := t.TempDir()
	cgroup, err := os.OpenRoot(cgroupDir)
	if err != nil {
		t.Fatal(err)
	}
	defer cgroup.Close()
	if err := cgroup.WriteFile("cgroup.controllers", []byte("cpu memory io pids\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := cgroup.WriteFile("cgroup.subtree_control", nil, 0o600); err != nil {
		t.Fatal(err)
	}
	name := cgroupName(jailerReviewID)
	if err := cgroup.Mkdir(name, 0o755); err != nil {
		t.Fatal(err)
	}
	const marker = "do not adopt"
	foreign := map[string][]byte{
		"cpu.max":    []byte("max 100000\n"),
		"memory.max": []byte("max\n"),
		"io.max":     []byte("foreign-io\n"),
		"pids.max":   []byte("max\n"),
		marker:       []byte(marker),
	}
	for file, content := range foreign {
		if err := cgroup.WriteFile(path.Join(name, file), content, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	limits := Limits{CPUQuotaPercent: 100, MemoryBytes: 64 << 20, Pids: 16, IO: IOLimits{
		Device: BlockDevice{Major: 8, Minor: 1}, ReadBPS: 1024,
	}}
	identity, err := prepareCgroup(cgroup, jailerReviewID, limits, nil)
	if err == nil {
		t.Fatal("prepareCgroup adopted and overwrote a foreign existing directory")
	}
	if identity.valid() {
		t.Fatal("prepareCgroup claimed ownership of a foreign directory")
	}
	for file, want := range foreign {
		raw, err := cgroup.ReadFile(path.Join(name, file))
		if err != nil {
			t.Fatalf("foreign cgroup content %s was removed: %v", file, err)
		}
		if !bytes.Equal(raw, want) {
			t.Fatalf("foreign %s changed to %q, want %q", file, raw, want)
		}
	}
}

// TestReviewDiskChainRejectsUnsafeImages replaces the earlier config/qcow
// "sanity" check with real chain rejection through LaunchSpec.validate.
func TestReviewDiskChainRejectsUnsafeImages(t *testing.T) {
	tests := []struct {
		name   string
		disks  [][]byte
		reason string
	}{
		{name: "raw under an overlay", disks: [][]byte{nil, jailerQCOW("base.raw", 0)}, reason: "only a standalone raw base"},
		{name: "dirty overlay", disks: [][]byte{jailerQCOW("", qcowIncompatibleDirty)}, reason: "dirty, corrupt or external-data-file"},
		{name: "corrupt overlay", disks: [][]byte{jailerQCOW("", qcowIncompatibleCorrupt)}, reason: "dirty, corrupt or external-data-file"},
		{name: "external data file", disks: [][]byte{jailerQCOW("", qcowIncompatibleDataFile)}, reason: "dirty, corrupt or external-data-file"},
		{name: "base names a backing file", disks: [][]byte{jailerQCOW("/etc/shadow", 0)}, reason: "explicit chain is incomplete"},
		{name: "overlay without backing", disks: [][]byte{jailerQCOW("", 0), jailerQCOW("", 0)}, reason: "has no backing file"},
		{name: "complete qcow2 chain (positive control)", disks: [][]byte{jailerQCOW("", 0), jailerQCOW("ignored-name", 0)}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			spec := jailerSpec(t, jailerReviewID, tt.disks...)
			_, err := spec.validate()
			if tt.reason == "" {
				if err != nil {
					t.Fatalf("valid chain rejected: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.reason) {
				t.Fatalf("validate = %v, want %q", err, tt.reason)
			}
		})
	}
}
