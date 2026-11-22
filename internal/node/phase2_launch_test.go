package node

import (
	"context"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/nathabonfim59/bamboo-raft/internal/ids"
	"github.com/nathabonfim59/bamboo-raft/internal/node/qemu"
	"github.com/nathabonfim59/bamboo-raft/internal/node/qmp"
	"github.com/nathabonfim59/bamboo-raft/internal/node/storage"
	"github.com/nathabonfim59/bamboo-raft/internal/objstore/memstore"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

// bitmapFakeState models the Phase 2 QMP surface of fakeQMPServer: the
// rootdisk node's dirty bitmaps (QEMU 8.2's add/remove/enable/merge rules),
// a live cut's leftovers (job, fleece node, fd set, export, NBD server), the
// schema reply and the command log. Every access holds fakeQMPServer.mu.
type bitmapFakeState struct {
	// prelaunch reports status "prelaunch" until the first cont (-S).
	prelaunch bool
	bitmaps   []qemu.BlockDirtyBitmap
	// schema is the query-qmp-schema reply; nil answers CommandNotFound.
	schema  []byte
	failCmd string
	job     bool // the cut's backup job bbraft-fleece exists
	fleece  bool // the fleece block node exists
	fdset   bool // the fleece fd set (fdset 3) exists
	export  bool
	nbd     bool
	cmds    []string
}

// handleLocked answers the Phase 2 commands; ok is false for commands the
// rest of the fake answers. Every command is logged.
func (b *bitmapFakeState) handleLocked(cmd string, args jsontext.Value) (result any, qerr string, ok bool) {
	if cmd != "qmp_capabilities" {
		b.cmds = append(b.cmds, cmd)
	}
	if cmd == b.failCmd {
		return nil, "injected failure of " + cmd, true
	}
	var a struct {
		Node     string   `json:"node"`
		Name     string   `json:"name"`
		Target   string   `json:"target"`
		Bitmaps  []string `json:"bitmaps"`
		Persist  bool     `json:"persistent"`
		Gran     uint64   `json:"granularity"`
		NodeName string   `json:"node-name"`
		Device   string   `json:"device"`
		FdsetID  int      `json:"fdset-id"`
	}
	if len(args) > 0 {
		if err := json.Unmarshal(args, &a); err != nil {
			return nil, "bad arguments: " + err.Error(), true
		}
	}
	find := func(name string) int {
		return slices.IndexFunc(b.bitmaps, func(bm qemu.BlockDirtyBitmap) bool { return bm.Name == name })
	}
	switch cmd {
	case "query-qmp-schema":
		if b.schema == nil {
			return nil, "The command query-qmp-schema has not been found", true
		}
		return jsontext.Value(b.schema), "", true
	case "query-named-block-nodes":
		nodes := []map[string]any{
			{"node-name": "base", "dirty-bitmaps": []any{}},
			{"node-name": qemu.RootDiskNode, "dirty-bitmaps": b.bitmaps},
		}
		if b.fleece {
			nodes = append(nodes, map[string]any{"node-name": fleeceNode, "dirty-bitmaps": []any{}})
		}
		return nodes, "", true
	case "block-dirty-bitmap-add":
		if find(a.Name) >= 0 {
			return nil, "Bitmap already exists: " + a.Name, true
		}
		b.bitmaps = append(b.bitmaps, qemu.BlockDirtyBitmap{Name: a.Name, Granularity: a.Gran, Recording: true, Persistent: a.Persist})
		return map[string]any{}, "", true
	case "block-dirty-bitmap-remove":
		i := find(a.Name)
		if i < 0 {
			return nil, "Dirty bitmap '" + a.Name + "' not found", true
		}
		b.bitmaps = slices.Delete(b.bitmaps, i, i+1)
		return map[string]any{}, "", true
	case "block-dirty-bitmap-enable":
		i := find(a.Name)
		if i < 0 {
			return nil, "Dirty bitmap '" + a.Name + "' not found", true
		}
		b.bitmaps[i].Recording = true
		return map[string]any{}, "", true
	case "block-dirty-bitmap-merge":
		t := find(a.Target)
		if t < 0 {
			return nil, "Dirty bitmap '" + a.Target + "' not found", true
		}
		for _, src := range a.Bitmaps {
			s := find(src)
			if s < 0 {
				return nil, "Dirty bitmap '" + src + "' not found", true
			}
			b.bitmaps[t].Count += b.bitmaps[s].Count
		}
		return map[string]any{}, "", true
	case "block-export-del":
		if !b.export {
			return nil, "Export 'bbraft-cut' is not found", true
		}
		b.export = false
		return map[string]any{}, "", true
	case "nbd-server-stop":
		if !b.nbd {
			return nil, "NBD server not running", true
		}
		b.nbd = false
		return map[string]any{}, "", true
	case "query-jobs":
		if b.job {
			return []any{map[string]any{"id": fleeceNode, "type": "backup", "status": "running"}}, "", true
		}
		return []any{}, "", true
	case "block-job-cancel":
		if !b.job || a.Device != fleeceNode {
			return nil, "Block job '" + a.Device + "' not found", true
		}
		b.job = false
		return map[string]any{}, "", true
	case "blockdev-del":
		if !b.fleece || a.NodeName != fleeceNode {
			return nil, "Failed to find node with node-name='" + a.NodeName + "'", true
		}
		b.fleece = false
		return map[string]any{}, "", true
	case "query-fdsets":
		if !b.fdset {
			return []any{}, "", true
		}
		return []any{map[string]any{"fdset-id": 3, "fds": []any{map[string]any{"fd": 54, "opaque": fleeceFDOpaque}}}}, "", true
	case "query-block-exports":
		if b.export {
			return []any{map[string]any{"id": cutExportID, "type": "nbd", "node-name": fleeceNode, "shutting-down": false}}, "", true
		}
		return []any{}, "", true
	case "add-fd", "blockdev-add", "transaction", "nbd-server-start", "block-export-add":
		return b.snapCutLocked(cmd, args)
	case "remove-fd":
		if !b.fdset || a.FdsetID != 3 {
			return nil, "File descriptor named 'fdset-id' not found", true
		}
		b.fdset = false
		return map[string]any{}, "", true
	}
	return nil, "", false
}

func (s *fakeQMPServer) p2Commands() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.p2.cmds)
}

func (s *fakeQMPServer) p2Bitmap(name string) (qemu.BlockDirtyBitmap, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	bm, found, _ := qemu.FindBitmap([]qemu.NamedBlockNode{{NodeName: qemu.RootDiskNode, DirtyBitmaps: s.p2.bitmaps}}, qemu.RootDiskNode, name)
	return bm, found
}

// bitmapHealthy is a bitmap a clean QEMU exit stored and the next launch
// loaded.
func bitmapHealthy() qemu.BlockDirtyBitmap {
	return qemu.BlockDirtyBitmap{Name: qemu.DirtyBitmap, Count: 8 << 20, Granularity: qemu.DirtyBitmapGranularity, Recording: true, Persistent: true}
}

// bitmapSchema reads the qemu package's QEMU 8.2 schema excerpt.
func bitmapSchema(t *testing.T) []byte {
	t.Helper()
	raw, err := os.ReadFile("qemu/testdata/qmp-schema-8.2-phase2.json")
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

// bitmapFakeQMP is a fake QMP server whose schema passes the Phase 2 gate,
// so the bitmap step runs.
func bitmapFakeQMP(t *testing.T, name string) *fakeQMPServer {
	t.Helper()
	srv := startFakeQMP(t, name)
	srv.p2.schema = bitmapSchema(t)
	return srv
}

// bitmapBootHarness boots VMs whose fake process has a QMP connection to
// srv, the way a real paused launch looks to the manager.
func bitmapBootHarness(t *testing.T, srv *fakeQMPServer, configure func(*Config)) *harness {
	t.Helper()
	launcher := &fakeLauncher{}
	launcher.wrapProcess = func(p VMProcess) VMProcess {
		client, err := qmp.Dial(t.Context(), srv.path, quietLog())
		if err != nil {
			t.Errorf("qmp dial: %v", err)
			return p
		}
		return &qmpFakeProcess{fakeProcess: p.(*fakeProcess), client: client}
	}
	return newHarness(t, harnessOptions{launcher: launcher, configure: configure})
}

func phase2StateOf(t *testing.T, h *harness, id string) storage.Phase2State {
	t.Helper()
	st, err := h.mg.store.ReadPhase2(id)
	if err != nil {
		t.Fatalf("ReadPhase2(%s): %v", id, err)
	}
	return st
}

// TestBootPreparesBitmapBeforeCont proves the paused launch's order: the
// bitmap step, then cont, then the guest; and which states force a full
// scan next.
func TestBootPreparesBitmapBeforeCont(t *testing.T) {
	query := "query-named-block-nodes"
	disabled := bitmapHealthy()
	disabled.Recording = false
	inconsistent := qemu.BlockDirtyBitmap{Name: qemu.DirtyBitmap, Granularity: qemu.DirtyBitmapGranularity, Persistent: true, Inconsistent: true}
	tests := []struct {
		name     string
		bitmaps  []qemu.BlockDirtyBitmap
		fullScan bool
		want     []string
	}{
		{"first boot adds the bitmap", nil, true,
			[]string{"query-qmp-schema", query, "block-dirty-bitmap-add", query, "cont", "query-status"}},
		{"clean restart keeps it", []qemu.BlockDirtyBitmap{bitmapHealthy()}, false,
			[]string{"query-qmp-schema", query, "cont", "query-status"}},
		{"killed QEMU's bitmap is replaced", []qemu.BlockDirtyBitmap{inconsistent}, true,
			[]string{"query-qmp-schema", query, "block-dirty-bitmap-remove", "block-dirty-bitmap-add", query, "cont", "query-status"}},
		{"disabled bitmap is enabled", []qemu.BlockDirtyBitmap{disabled}, true,
			[]string{"query-qmp-schema", query, "block-dirty-bitmap-enable", query, "cont", "query-status"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			id := ids.Sandbox()
			srv := bitmapFakeQMP(t, id)
			srv.p2.prelaunch = true
			srv.p2.bitmaps = tt.bitmaps
			h := bitmapBootHarness(t, srv, nil)
			vm := h.create(id, nil)
			if vm.GetState() != nodev1.VMState_VM_STATE_READY {
				t.Fatalf("state %s: %s", vm.GetState(), vm.GetError())
			}
			if got := srv.p2Commands(); !slices.Equal(got, tt.want) {
				t.Fatalf("QMP commands = %q, want %q", got, tt.want)
			}
			if got := srv.contCount(); got != 1 {
				t.Fatalf("cont sent %d times", got)
			}
			bm, found := srv.p2Bitmap(qemu.DirtyBitmap)
			if !found || !bm.Recording || !bm.Persistent || bm.Inconsistent || bm.Granularity != qemu.DirtyBitmapGranularity {
				t.Fatalf("bitmap after boot = %+v (found %t)", bm, found)
			}
			if st := phase2StateOf(t, h, id); st.FullScanNext != tt.fullScan {
				t.Fatalf("FullScanNext = %t, want %t", st.FullScanNext, tt.fullScan)
			}
		})
	}
}

// TestBootBitmapFailureFailsBoot: a bitmap step that fails is a boot
// failure, and the guest is never started.
func TestBootBitmapFailureFailsBoot(t *testing.T) {
	for _, cmd := range []string{"query-named-block-nodes", "block-dirty-bitmap-add", "cont"} {
		t.Run(cmd, func(t *testing.T) {
			id := ids.Sandbox()
			srv := bitmapFakeQMP(t, id)
			srv.p2.prelaunch = true
			if cmd == "cont" {
				srv.contErr = errors.New("injected cont failure")
			} else {
				srv.p2.failCmd = cmd
			}
			h := bitmapBootHarness(t, srv, nil)
			_, err := h.mg.CreateVM(t.Context(), &nodev1.CreateVMRequest{SandboxId: id, Resources: &nodev1.Resources{Vcpus: 2, MemoryMib: 2048, DiskGib: defaultTestBaseGiB}})
			if err == nil {
				t.Fatal("CreateVM succeeded although the bitmap step failed")
			}
			if cmd != "cont" && srv.contCount() != 0 {
				t.Fatal("cont was sent after a failed bitmap step")
			}
		})
	}
}

// TestPhase2GateFromConfig: a known-unavailable QEMU turns the bitmap step
// off (a plain cont, as in Phase 1) and refuses SnapshotVM and from_manifest
// with the reason, after the store check.
func TestPhase2GateFromConfig(t *testing.T) {
	id := ids.Sandbox()
	srv := bitmapFakeQMP(t, id)
	srv.p2.prelaunch = true
	missing := qemu.Phase2Support{QEMUVersion: "7.0.0", Missing: []string{"block-export-add type nbd argument allocation-depth"}}
	h := bitmapBootHarness(t, srv, func(cfg *Config) {
		cfg.Store = memstore.New()
		cfg.Phase2 = &missing
	})
	h.create(id, nil)
	if got, want := srv.p2Commands(), []string{"cont", "query-status"}; !slices.Equal(got, want) {
		t.Fatalf("QMP commands = %q, want only %q", got, want)
	}

	_, err := h.mg.SnapshotVM(t.Context(), &nodev1.SnapshotVMRequest{SandboxId: id, ChainId: "not-a-uuid"})
	if status.Code(err) != codes.FailedPrecondition || status.Convert(err).Message() != missing.Reason() {
		t.Fatalf("SnapshotVM = %v, want FAILED_PRECONDITION %q", err, missing.Reason())
	}
	_, err = h.mg.CreateVM(t.Context(), &nodev1.CreateVMRequest{SandboxId: ids.Sandbox(), FromManifest: "bad key"})
	if status.Code(err) != codes.FailedPrecondition || !strings.Contains(err.Error(), "allocation-depth") {
		t.Fatalf("CreateVM from_manifest = %v, want FAILED_PRECONDITION naming what is missing", err)
	}

	// Store first: without a store the store's message wins.
	h2 := phase2Harness(t, false)
	h2.mg.setPhase2Support(missing)
	_, err = h2.mg.SnapshotVM(t.Context(), &nodev1.SnapshotVMRequest{SandboxId: id})
	if status.Convert(err).Message() != "this node has no object store" {
		t.Fatalf("SnapshotVM without a store = %v", err)
	}
}

// TestPhase2GateLearnedOnFirstLaunch: without a startup result, the first
// QEMU's schema decides once; an unreadable schema is retried next launch.
func TestPhase2GateLearnedOnFirstLaunch(t *testing.T) {
	t.Run("available", func(t *testing.T) {
		id := ids.Sandbox()
		srv := bitmapFakeQMP(t, id)
		srv.p2.schema = bitmapSchema(t)
		h := bitmapBootHarness(t, srv, nil)
		h.create(id, nil)
		s, known := h.mg.phase2Support()
		if !known || !s.Available() {
			t.Fatalf("gate = %+v known %t", s, known)
		}
		h.create(ids.Sandbox(), nil)
		if n := strings.Count(strings.Join(srv.p2Commands(), " "), "query-qmp-schema"); n != 1 {
			t.Fatalf("schema queried %d times, want once", n)
		}
	})
	t.Run("missing", func(t *testing.T) {
		id := ids.Sandbox()
		srv := bitmapFakeQMP(t, id)
		srv.p2.schema = []byte(`[]`)
		h := bitmapBootHarness(t, srv, func(cfg *Config) { cfg.Store = memstore.New() })
		h.create(id, nil)
		s, known := h.mg.phase2Support()
		if !known || s.Available() {
			t.Fatalf("gate = %+v known %t", s, known)
		}
		if slices.Contains(srv.p2Commands(), "block-dirty-bitmap-add") {
			t.Fatal("the bitmap step ran on a QEMU without Phase 2")
		}
		if err := h.mg.phase2Unavailable(); status.Code(err) != codes.FailedPrecondition {
			t.Fatalf("phase2Unavailable = %v", err)
		}
	})
	// An unreadable schema is treated like an unavailable Phase 2 for the
	// launch: a plain cont, so even a QEMU that would fail the bitmap step
	// boots as in Phase 1, and a full scan is recorded because nothing
	// checked the bitmap.
	t.Run("unreadable", func(t *testing.T) {
		id, id2 := ids.Sandbox(), ids.Sandbox()
		srv := bitmapFakeQMP(t, id)
		srv.p2.schema = nil
		srv.p2.prelaunch = true
		srv.p2.failCmd = "query-named-block-nodes"
		h := bitmapBootHarness(t, srv, nil)
		h.create(id, nil)
		h.create(id2, nil)
		if _, known := h.mg.phase2Support(); known {
			t.Fatal("an unreadable schema was cached")
		}
		want := []string{"query-qmp-schema", "cont", "query-status", "query-qmp-schema", "cont", "query-status"}
		if got := srv.p2Commands(); !slices.Equal(got, want) {
			t.Fatalf("QMP commands = %q, want %q", got, want)
		}
		for _, vm := range []string{id, id2} {
			if st := phase2StateOf(t, h, vm); !st.FullScanNext {
				t.Fatalf("%s: an unchecked bitmap did not force a full scan", vm)
			}
		}
		if n := strings.Count(strings.Join(srv.p2Commands(), " "), "query-qmp-schema"); n != 2 {
			t.Fatalf("schema queried %d times, want once per launch", n)
		}
		if err := h.mg.phase2Unavailable(); err != nil {
			t.Fatalf("an unknown gate refused: %v", err)
		}
	})
}

// bitmapPinSelf pins the test process: the fake QMP server runs in it, so
// its socket's peer credential is this pid, as a real QEMU's is its own.
func bitmapPinSelf(t *testing.T) (*pinnedProc, storage.QEMURecord) {
	t.Helper()
	pinned, err := pinProc(os.Getpid())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pinned.close() })
	bootID, err := storage.HostBootID()
	if err != nil {
		t.Fatal(err)
	}
	return pinned, storage.QEMURecord{
		State: storage.QEMURunning, PID: os.Getpid(), BootID: bootID,
		StartTicks: mustProcStartTicks(t, os.Getpid()),
	}
}

// bitmapAdoptMeta creates a VM through the harness (its fake process has
// no QMP) and returns its record pointing at the pinned test process.
func bitmapAdoptMeta(t *testing.T, h *harness, rec storage.QEMURecord) storage.VMMeta {
	t.Helper()
	id := ids.Sandbox()
	h.create(id, nil)
	meta, err := h.mg.store.ReadVM(id)
	if err != nil {
		t.Fatal(err)
	}
	meta.QEMU = &rec
	return meta
}

// TestAdoptionPhase2Step drives verifyQMPAt's adoption path: prelaunch is
// accepted and gets the launch's bitmap step and cont; paused and running
// QEMUs get the bitmap step too (a Phase 1 QEMU after an upgrade has none).
func TestAdoptionPhase2Step(t *testing.T) {
	query := "query-named-block-nodes"
	tests := []struct {
		name      string
		prelaunch bool
		paused    bool
		bitmaps   []qemu.BlockDirtyBitmap
		fullScan  bool
		cont      int
		want      []string
	}{
		{"prelaunch after a crash before cont", true, false, nil, true, 1,
			[]string{"query-name", "query-status", "query-qmp-schema", query, "block-dirty-bitmap-add", query, "cont", "query-status"}},
		{"prelaunch with a stored bitmap", true, false, []qemu.BlockDirtyBitmap{bitmapHealthy()}, false, 1,
			[]string{"query-name", "query-status", "query-qmp-schema", query, "cont", "query-status"}},
		{"running Phase 1 QEMU without a bitmap", false, false, nil, true, 0,
			[]string{"query-name", "query-status", "query-qmp-schema", query, "block-dirty-bitmap-add", query}},
		{"running with its bitmap", false, false, []qemu.BlockDirtyBitmap{bitmapHealthy()}, false, 0,
			[]string{"query-name", "query-status", "query-qmp-schema", query}},
		{"paused is resumed", false, true, []qemu.BlockDirtyBitmap{bitmapHealthy()}, false, 1,
			[]string{"query-name", "query-status", "query-qmp-schema", query, "cont", "query-status"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newHarness(t, harnessOptions{})
			pinned, rec := bitmapPinSelf(t)
			meta := bitmapAdoptMeta(t, h, rec)
			srv := bitmapFakeQMP(t, meta.SandboxID)
			srv.p2.prelaunch = tt.prelaunch
			srv.startPaused = tt.paused
			srv.p2.bitmaps = tt.bitmaps
			client, err := h.mg.verifyQMPAt(t.Context(), pinned, meta, srv.path, true)
			if err != nil {
				t.Fatalf("verifyQMPAt: %v", err)
			}
			client.Close()
			if got := srv.p2Commands(); !slices.Equal(got, tt.want) {
				t.Fatalf("QMP commands = %q, want %q", got, tt.want)
			}
			if got := srv.contCount(); got != tt.cont {
				t.Fatalf("cont sent %d times, want %d", got, tt.cont)
			}
			if st := phase2StateOf(t, h, meta.SandboxID); st.FullScanNext != tt.fullScan {
				t.Fatalf("FullScanNext = %t, want %t", st.FullScanNext, tt.fullScan)
			}
		})
	}
}

// TestAdoptionPhase2Failures: a prelaunch QEMU whose bitmap cannot be made
// healthy is not verified (never started without it); a running one stays
// adoptable with a full scan forced. A snapshot rescue (adopt=false) keeps
// Phase 1's behaviour: no Phase 2 step, prelaunch refused.
func TestAdoptionPhase2Failures(t *testing.T) {
	t.Run("prelaunch", func(t *testing.T) {
		h := newHarness(t, harnessOptions{})
		pinned, rec := bitmapPinSelf(t)
		meta := bitmapAdoptMeta(t, h, rec)
		srv := bitmapFakeQMP(t, meta.SandboxID)
		srv.p2.prelaunch = true
		srv.p2.failCmd = "block-dirty-bitmap-add"
		if _, err := h.mg.verifyQMPAt(t.Context(), pinned, meta, srv.path, true); err == nil {
			t.Fatal("a prelaunch QEMU without its bitmap was verified")
		}
		if srv.contCount() != 0 {
			t.Fatal("cont was sent without the bitmap")
		}
	})
	t.Run("prelaunch with an unreadable schema", func(t *testing.T) {
		h := newHarness(t, harnessOptions{})
		pinned, rec := bitmapPinSelf(t)
		meta := bitmapAdoptMeta(t, h, rec)
		srv := bitmapFakeQMP(t, meta.SandboxID)
		srv.p2.schema = nil
		srv.p2.prelaunch = true
		srv.p2.failCmd = "query-named-block-nodes"
		client, err := h.mg.verifyQMPAt(t.Context(), pinned, meta, srv.path, true)
		if err != nil {
			t.Fatalf("an unknown gate refused adoption: %v", err)
		}
		client.Close()
		if srv.contCount() != 1 {
			t.Fatal("the adopted prelaunch QEMU was not started")
		}
		if st := phase2StateOf(t, h, meta.SandboxID); !st.FullScanNext {
			t.Fatal("an unchecked bitmap did not force a full scan")
		}
	})
	t.Run("running", func(t *testing.T) {
		h := newHarness(t, harnessOptions{})
		pinned, rec := bitmapPinSelf(t)
		meta := bitmapAdoptMeta(t, h, rec)
		srv := bitmapFakeQMP(t, meta.SandboxID)
		srv.p2.failCmd = "query-named-block-nodes"
		client, err := h.mg.verifyQMPAt(t.Context(), pinned, meta, srv.path, true)
		if err != nil {
			t.Fatalf("a running QEMU with a failing bitmap check was refused: %v", err)
		}
		client.Close()
		if st := phase2StateOf(t, h, meta.SandboxID); !st.FullScanNext {
			t.Fatal("a failed bitmap check did not force a full scan")
		}
	})
	t.Run("rescue", func(t *testing.T) {
		h := newHarness(t, harnessOptions{})
		pinned, rec := bitmapPinSelf(t)
		meta := bitmapAdoptMeta(t, h, rec)
		srv := bitmapFakeQMP(t, meta.SandboxID)
		srv.startPaused = true
		client, err := h.mg.verifyQMPAt(t.Context(), pinned, meta, srv.path, false)
		if err != nil {
			t.Fatalf("rescue verify: %v", err)
		}
		client.Close()
		if got, want := srv.p2Commands(), []string{"query-name", "query-status", "cont", "query-status"}; !slices.Equal(got, want) {
			t.Fatalf("rescue QMP commands = %q, want %q", got, want)
		}
		srv2 := bitmapFakeQMP(t, meta.SandboxID)
		srv2.p2.prelaunch = true
		if _, err := h.mg.verifyQMPAt(t.Context(), pinned, meta, srv2.path, false); err == nil {
			t.Fatal("a rescue accepted a prelaunch QEMU")
		}
	})
}

// TestRecoverInterruptedCutLive: a restart that finds the marker and a
// running QEMU tears the cut down, merges bbraft-cut back into bbraft-dirty
// and clears the marker without a full scan; if the merge is impossible the
// next snapshot scans fully.
func TestRecoverInterruptedCutLive(t *testing.T) {
	cut := qemu.BlockDirtyBitmap{Name: qemu.CutBitmap, Count: 12 << 20, Granularity: qemu.DirtyBitmapGranularity}
	teardown := []string{"block-export-del", "query-block-exports", "nbd-server-stop", "query-jobs", "block-job-cancel", "query-jobs", "blockdev-del", "query-fdsets", "remove-fd", "query-named-block-nodes"}
	tests := []struct {
		name     string
		bitmaps  []qemu.BlockDirtyBitmap
		leftover bool
		fullScan bool
		want     []string
	}{
		{"mid-read: everything left over", []qemu.BlockDirtyBitmap{bitmapHealthy(), cut}, true, false,
			append(slices.Clone(teardown), "block-dirty-bitmap-merge", "block-dirty-bitmap-remove")},
		{"before the transaction: nothing to merge", []qemu.BlockDirtyBitmap{bitmapHealthy()}, false, false,
			[]string{"block-export-del", "query-block-exports", "nbd-server-stop", "query-jobs", "blockdev-del", "query-fdsets", "query-named-block-nodes"}},
		{"cut without bbraft-dirty", []qemu.BlockDirtyBitmap{cut}, true, true,
			append(slices.Clone(teardown), "block-dirty-bitmap-remove")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newHarness(t, harnessOptions{})
			id := ids.Sandbox()
			h.create(id, nil)
			if err := h.mg.store.SetSnapshotInProgress(id); err != nil {
				t.Fatal(err)
			}
			fleece := h.mg.store.FleecePath(id)
			if err := os.WriteFile(fleece, []byte("qcow2"), 0o600); err != nil {
				t.Fatal(err)
			}
			srv := bitmapFakeQMP(t, id)
			srv.p2.bitmaps = tt.bitmaps
			srv.p2.job, srv.p2.fleece, srv.p2.fdset, srv.p2.export, srv.p2.nbd = tt.leftover, tt.leftover, tt.leftover, tt.leftover, tt.leftover
			client, err := qmp.Dial(t.Context(), srv.path, quietLog())
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			h.mg.recoverCutLive(t.Context(), client, id)

			if got := srv.p2Commands(); !slices.Equal(got, tt.want) {
				t.Fatalf("QMP commands = %q, want %q", got, tt.want)
			}
			if _, found := srv.p2Bitmap(qemu.CutBitmap); found {
				t.Fatal("bbraft-cut is left over")
			}
			if dirty, found := srv.p2Bitmap(qemu.DirtyBitmap); found && tt.leftover && dirty.Count != bitmapHealthy().Count+cut.Count {
				t.Fatalf("bbraft-dirty count %d after merge, want %d", dirty.Count, bitmapHealthy().Count+cut.Count)
			}
			st := phase2StateOf(t, h, id)
			if st.SnapshotInProgress || st.FullScanNext != tt.fullScan {
				t.Fatalf("state after recovery = %+v, want marker cleared and fullScan %t", st, tt.fullScan)
			}
			if _, err := os.Lstat(fleece); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("fleece file after recovery: %v", err)
			}
		})
	}

	t.Run("no marker: no QMP at all", func(t *testing.T) {
		h := newHarness(t, harnessOptions{})
		id := ids.Sandbox()
		h.create(id, nil)
		srv := bitmapFakeQMP(t, id)
		client, err := qmp.Dial(t.Context(), srv.path, quietLog())
		if err != nil {
			t.Fatal(err)
		}
		defer client.Close()
		h.mg.recoverCutLive(t.Context(), client, id)
		if got := srv.p2Commands(); len(got) != 0 {
			t.Fatalf("QMP commands without a marker = %q", got)
		}
	})
}

// TestRecoverInterruptedCutWithoutQEMU: a restart that finds the marker but
// no QEMU clears it, forces a full scan and removes the cut's scratch (the
// fleece and a stale NBD socket that would block the next jailed launch);
// an unresolved process keeps the marker and still forces a full scan.
func TestRecoverInterruptedCutWithoutQEMU(t *testing.T) {
	t.Run("QEMU gone", func(t *testing.T) {
		id := ids.Sandbox()
		var fleece, sock string
		h := newHarness(t, harnessOptions{
			fallback: pingOnlyGuest{},
			prepare: func(t *testing.T, dataDir, imageDir string) {
				prepareVM(t, dataDir, imageDir, id, 9, 0)
				bitmapMarkInProgress(t, dataDir, imageDir, id)
				dir := filepath.Join(dataDir, "vms", id)
				fleece = filepath.Join(dir, storage.FleeceName)
				if err := os.WriteFile(fleece, []byte("qcow2"), 0o600); err != nil {
					t.Fatal(err)
				}
				if err := os.Mkdir(filepath.Join(dir, storage.ControlDirName), 0o700); err != nil {
					t.Fatal(err)
				}
				sock = bitmapStaleSocket(t, filepath.Join(dir, storage.ControlDirName))
			},
			findProcess: func(storage.VMMeta) (*pinnedProc, error) { return nil, nil },
		})
		st := phase2StateOf(t, h, id)
		if st.SnapshotInProgress || !st.FullScanNext {
			t.Fatalf("state = %+v, want marker cleared and full scan", st)
		}
		for _, p := range []string{fleece, sock} {
			if _, err := os.Lstat(p); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("%s left behind: %v", p, err)
			}
		}
	})
	t.Run("process unresolved", func(t *testing.T) {
		id := ids.Sandbox()
		h := newHarness(t, harnessOptions{
			fallback: pingOnlyGuest{},
			prepare: func(t *testing.T, dataDir, imageDir string) {
				prepareVM(t, dataDir, imageDir, id, 10, 0)
				bitmapMarkInProgress(t, dataDir, imageDir, id)
			},
			findProcess: func(storage.VMMeta) (*pinnedProc, error) { return nil, errors.New("two candidates") },
		})
		st := phase2StateOf(t, h, id)
		if !st.SnapshotInProgress || !st.FullScanNext {
			t.Fatalf("state = %+v, want the marker kept and a full scan", st)
		}
	})
}

func bitmapMarkInProgress(t *testing.T, dataDir, imageDir, id string) {
	t.Helper()
	store, err := storage.Open(storage.Config{DataDir: dataDir, ImageDir: imageDir, Imager: &fakeImager{diskGiB: defaultTestBaseGiB}, Log: quietLog()})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.SetSnapshotInProgress(id); err != nil {
		t.Fatal(err)
	}
}

// bitmapStaleSocket leaves a unix socket file behind in dir, as a QEMU
// killed during an NBD session does.
func bitmapStaleSocket(t *testing.T, dir string) string {
	t.Helper()
	p := filepath.Join(dir, storage.NBDSocketName)
	ln, err := (&net.ListenConfig{}).Listen(context.Background(), "unix", p)
	if err != nil {
		t.Skipf("cannot create a unix socket at %s (path length %d): %v", p, len(p), err)
	}
	ln.(*net.UnixListener).SetUnlinkOnClose(false)
	ln.Close()
	return p
}
