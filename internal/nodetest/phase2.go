package nodetest

import (
	"crypto/rand"
	"io"
	"testing"
	"uuid"

	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	"github.com/nathabonfim59/bamboo-raft/internal/ids"
	"github.com/nathabonfim59/bamboo-raft/internal/objstore"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
	"github.com/nathabonfim59/bamboo-raft/internal/snapformat"
)

// Phase2 selects the Phase 2 case groups. Store is the object store the node
// under test writes to (the same bucket); the suite reads manifests from it
// and writes under no key of its own.
type Phase2 struct {
	Snapshots bool // SnapshotVM, CreateVM from_manifest
	Reset     bool // ResetVM
	Store     objstore.Store
}

func (s *suite) runPhase2(t *testing.T) {
	p := s.opts.Phase2
	if p.Snapshots {
		if p.Store == nil {
			t.Fatal("nodetest: Phase2.Snapshots needs Phase2.Store")
		}
		t.Run("SnapshotRestore", s.testSnapshotRestore)
		t.Run("SnapshotErrors", s.testSnapshotErrors)
		t.Run("RestoreErrors", s.testRestoreErrors)
		if s.opts.Phase1.Stop {
			t.Run("SnapshotStopped", s.testSnapshotStopped)
		}
	}
	if p.Reset {
		t.Run("Reset", s.testReset)
		t.Run("ResetErrors", s.testResetErrors)
	}
}

func newChain() string { return uuid.NewV7().String() }

// putBytes writes data to a guest path through PutFile.
func (s *suite) putBytes(t *testing.T, id, path string, data []byte) {
	t.Helper()
	if _, err := PutFile(s.ctx(t), s.c, id, path, 0o644, data, 1<<20); err != nil {
		t.Fatalf("PutFile(%s): %v", path, err)
	}
}

// sha prints the sha256 of a guest file (relative to /home/user).
func (s *suite) sha(t *testing.T, id, path string) string {
	t.Helper()
	r := s.exec(t, id, "sha256sum "+path+" | cut -d' ' -f1")
	if r.Exit.GetExitCode() != 0 {
		t.Fatalf("sha256sum %s: exit %v, stderr %q", path, r.Exit.GetExitCode(), r.Stderr)
	}
	return r.Stdout
}

func (s *suite) snapshot(t *testing.T, id, chain string, gen uint32) *nodev1.Generation {
	t.Helper()
	g, err := s.c.SnapshotVM(s.ctx(t), &nodev1.SnapshotVMRequest{SandboxId: id, ChainId: chain, Generation: gen})
	if err != nil {
		t.Fatalf("SnapshotVM(%s, %s/%d): %v", id, chain, gen, err)
	}
	if want := snapformat.ManifestKey(chain, gen); g.ManifestKey != want || g.ChainId != chain || g.Generation != gen {
		t.Fatalf("Generation = key %s chain %s gen %d; want %s", g.ManifestKey, g.ChainId, g.Generation, want)
	}
	return g
}

// manifest fetches and decodes a written manifest.
func (s *suite) manifest(t *testing.T, key string) *snapformat.Manifest {
	t.Helper()
	rc, err := s.opts.Phase2.Store.Get(s.ctx(t), key, 0, -1)
	if err != nil {
		t.Fatalf("manifest %s: %v", key, err)
	}
	defer rc.Close()
	b, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}
	m, err := snapformat.DecodeManifest(b)
	if err != nil {
		t.Fatalf("manifest %s: %v", key, err)
	}
	return m
}

func (s *suite) testSnapshotRestore(t *testing.T) {
	src := s.create(t, &nodev1.CreateVMRequest{})
	id := src.SandboxId
	if h := src.GetHydration(); !h.GetHydrated() || h.GetHydratedAtUnixMs() == 0 {
		t.Errorf("a VM not from a manifest has hydration %+v; want hydrated", h)
	}
	big := make([]byte, 9<<20) // spans several chunks
	rand.Read(big)
	s.putBytes(t, id, "p2/big.bin", big)
	s.putBytes(t, id, "p2/note.txt", []byte("generation zero\n"))
	wantBig := s.sha(t, id, "p2/big.bin")

	chain := newChain()
	g0 := s.snapshot(t, id, chain, 0)
	if g0.Image != src.Image || g0.DiskBytes < s.opts.Resources.GetDiskGib()<<30 || g0.Chunks == 0 || g0.ChunksUploaded == 0 || g0.BytesUploaded == 0 {
		t.Fatalf("generation 0 = %+v", g0)
	}
	if g0.ContentBytes < uint64(len(big)) {
		t.Errorf("content_bytes %d < the %d bytes written", g0.ContentBytes, len(big))
	}
	if !g0.Frozen {
		t.Error("a READY VM's default snapshot was not frozen")
	}
	m0 := s.manifest(t, g0.ManifestKey)
	if m0.ChainID != chain || m0.Generation != 0 || m0.Image != src.Image || m0.DiskBytes != g0.DiskBytes || uint64(len(m0.Chunks)) != g0.Chunks {
		t.Fatalf("manifest 0 = chain %s gen %d image %s disk %d chunks %d; generation %+v", m0.ChainID, m0.Generation, m0.Image, m0.DiskBytes, len(m0.Chunks), g0)
	}
	for key := range m0.ChunkKeys() {
		if _, err := s.opts.Phase2.Store.Head(s.ctx(t), key); err != nil {
			t.Fatalf("manifest 0 names %s: %v", key, err)
		}
	}

	// Nothing changed: the next generation uploads less (chunks already in
	// the store are skipped; a real guest still writes a little meanwhile).
	g1 := s.snapshot(t, id, chain, 1)
	if g1.ChunksUploaded >= g0.ChunksUploaded {
		t.Errorf("unchanged disk: generation 1 uploaded %d chunks, generation 0 %d", g1.ChunksUploaded, g0.ChunksUploaded)
	}

	s.putBytes(t, id, "p2/note.txt", []byte("generation two\n"))
	s.putBytes(t, id, "p2/later.txt", []byte("only in two\n"))
	g2 := s.snapshot(t, id, chain, 2)

	// Restore generation 0 under a new id, with a larger disk: generation 0's
	// files, nothing written after it.
	res := &nodev1.Resources{Vcpus: s.opts.Resources.GetVcpus(), MemoryMib: s.opts.Resources.GetMemoryMib(), DiskGib: s.opts.Resources.GetDiskGib() + 2}
	fork0 := s.create(t, &nodev1.CreateVMRequest{FromManifest: g0.ManifestKey, Resources: res})
	if fork0.FromManifest != g0.ManifestKey || fork0.Image != src.Image {
		t.Errorf("restored VM = from_manifest %q image %q", fork0.FromManifest, fork0.Image)
	}
	if h := fork0.GetHydration(); !h.GetHydrated() || h.GetHydratedAtUnixMs() == 0 || h.GetChunksDone() != h.GetChunksTotal() {
		t.Errorf("restored READY VM hydration = %+v; want hydrated (Phase 2a restores are eager)", h)
	}
	if fork0.GetResources().GetDiskGib() != res.DiskGib {
		t.Errorf("restored disk %d GiB, want %d", fork0.GetResources().GetDiskGib(), res.DiskGib)
	}
	if got := s.exec(t, fork0.SandboxId, "cat p2/note.txt").Stdout; got != "generation zero\n" {
		t.Errorf("generation 0 note = %q", got)
	}
	if got := s.sha(t, fork0.SandboxId, "p2/big.bin"); got != wantBig {
		t.Errorf("generation 0 big.bin sha %s, want %s", got, wantBig)
	}
	if r := s.exec(t, fork0.SandboxId, "test -e p2/later.txt"); r.Exit.GetExitCode() == 0 {
		t.Error("generation 0 holds a file written after it")
	}
	// The restored VM is a normal VM: it can be snapshotted itself.
	s.snapshot(t, fork0.SandboxId, newChain(), 0)

	fork2 := s.create(t, &nodev1.CreateVMRequest{FromManifest: g2.ManifestKey})
	if got := s.exec(t, fork2.SandboxId, "cat p2/note.txt p2/later.txt").Stdout; got != "generation two\nonly in two\n" {
		t.Errorf("generation 2 files = %q", got)
	}
	// The source is untouched by its snapshots.
	if got := s.exec(t, id, "cat p2/note.txt").Stdout; got != "generation two\n" {
		t.Errorf("source note after snapshots = %q", got)
	}
}

func (s *suite) testSnapshotErrors(t *testing.T) {
	ctx := s.ctx(t)
	_, err := s.c.SnapshotVM(ctx, &nodev1.SnapshotVMRequest{SandboxId: "bx_unknown0", ChainId: newChain()})
	wantCode(t, err, codes.NotFound)

	id := s.create(t, &nodev1.CreateVMRequest{}).SandboxId
	for _, chain := range []string{"", "not-a-uuid", "../x", "0190C3A4-1B2C-7D3E-8F40-123456789ABC"} {
		_, err := s.c.SnapshotVM(ctx, &nodev1.SnapshotVMRequest{SandboxId: id, ChainId: chain})
		wantCode(t, err, codes.InvalidArgument)
	}
	chain := newChain()
	s.snapshot(t, id, chain, 5)
	_, err = s.c.SnapshotVM(ctx, &nodev1.SnapshotVMRequest{SandboxId: id, ChainId: chain, Generation: 5})
	wantCode(t, err, codes.AlreadyExists)
	// An unfrozen cut works on a READY VM and says so.
	g, err := s.c.SnapshotVM(ctx, &nodev1.SnapshotVMRequest{SandboxId: id, ChainId: chain, Generation: 6, Freeze: new(false)})
	if err != nil {
		t.Fatalf("SnapshotVM freeze=false: %v", err)
	}
	if g.Frozen {
		t.Error("freeze=false reported frozen")
	}
}

func (s *suite) testRestoreErrors(t *testing.T) {
	ctx := s.ctx(t)
	id := s.create(t, &nodev1.CreateVMRequest{}).SandboxId
	g := s.snapshot(t, id, newChain(), 0)
	cases := []struct {
		name string
		req  *nodev1.CreateVMRequest
		code codes.Code
	}{
		{"missing", &nodev1.CreateVMRequest{FromManifest: snapformat.ManifestKey(newChain(), 0)}, codes.NotFound},
		{"bad key", &nodev1.CreateVMRequest{FromManifest: "chunks/abc"}, codes.InvalidArgument},
		{"both sources", &nodev1.CreateVMRequest{FromManifest: g.ManifestKey, FromSnapshot: "snap-x"}, codes.InvalidArgument},
		{"other image", &nodev1.CreateVMRequest{FromManifest: g.ManifestKey, Image: g.Image + "-other"}, codes.InvalidArgument},
		{"smaller disk", &nodev1.CreateVMRequest{FromManifest: g.ManifestKey, Resources: &nodev1.Resources{
			Vcpus: s.opts.Resources.GetVcpus(), MemoryMib: s.opts.Resources.GetMemoryMib(), DiskGib: g.DiskBytes>>30 - 1,
		}}, codes.InvalidArgument},
	}
	for _, c := range cases {
		c.req.SandboxId = newSandboxID()
		if c.req.Resources == nil {
			c.req.Resources = s.opts.Resources
		}
		_, err := s.c.CreateVM(ctx, c.req)
		if err == nil {
			s.c.DeleteVM(ctx, &nodev1.DeleteVMRequest{SandboxId: c.req.SandboxId})
		}
		t.Run(c.name, func(t *testing.T) { wantCode(t, err, c.code) })
		// A refused restore leaves no VM behind.
		if _, err := s.c.GetVM(ctx, &nodev1.GetVMRequest{SandboxId: c.req.SandboxId}); codeOf(err) != codes.NotFound {
			t.Errorf("%s: GetVM after a refused CreateVM = %v, want NOT_FOUND", c.name, err)
		}
	}
	// A corrupt chunk fails the restore instead of booting a wrong disk:
	// covered per implementation (it needs write access to the store).
}

func (s *suite) testSnapshotStopped(t *testing.T) {
	ctx := s.ctx(t)
	id := s.create(t, &nodev1.CreateVMRequest{}).SandboxId
	s.putBytes(t, id, "p2/stopped.txt", []byte("written before stop\n"))
	if _, err := s.c.StopVM(ctx, &nodev1.StopVMRequest{SandboxId: id, GraceSeconds: 10}); err != nil {
		t.Fatalf("StopVM: %v", err)
	}
	g, err := s.c.SnapshotVM(ctx, &nodev1.SnapshotVMRequest{SandboxId: id, ChainId: newChain()})
	if err != nil {
		t.Fatalf("SnapshotVM of a STOPPED VM: %v", err)
	}
	if g.Frozen {
		t.Error("a STOPPED VM's snapshot reported frozen")
	}
	vm, err := s.c.GetVM(ctx, &nodev1.GetVMRequest{SandboxId: id})
	if err != nil || vm.State != nodev1.VMState_VM_STATE_STOPPED {
		t.Fatalf("after SnapshotVM the VM is %v, %v; want STOPPED", vm.GetState(), err)
	}
	restored := s.create(t, &nodev1.CreateVMRequest{FromManifest: g.ManifestKey})
	if got := s.exec(t, restored.SandboxId, "cat p2/stopped.txt").Stdout; got != "written before stop\n" {
		t.Errorf("restored stopped disk: %q", got)
	}
	if _, err := s.c.StartVM(ctx, &nodev1.StartVMRequest{SandboxId: id}); err != nil {
		t.Fatalf("StartVM after an offline snapshot: %v", err)
	}
}

func (s *suite) testReset(t *testing.T) {
	ctx := s.ctx(t)
	vm := s.create(t, &nodev1.CreateVMRequest{})
	id := vm.SandboxId
	s.putBytes(t, id, "p2/kept.txt", []byte("survives reset\n"))
	if s.opts.Phase1.Processes {
		if _, err := s.c.StartProcess(ctx, &nodev1.StartProcessRequest{SandboxId: id, ProcessId: 1, Command: "echo before-reset; sleep 600"}); err != nil {
			t.Fatalf("StartProcess: %v", err)
		}
		waitOutput(t, ctx, s.c, id, 1, "before-reset\n")
	}
	// Make sure the file is on disk before the hardware reset.
	s.exec(t, id, "sync")
	after, err := s.c.ResetVM(ctx, &nodev1.ResetVMRequest{SandboxId: id})
	if err != nil {
		t.Fatalf("ResetVM: %v", err)
	}
	if after.State != nodev1.VMState_VM_STATE_READY || after.StartedAtUnixMs <= vm.StartedAtUnixMs {
		t.Fatalf("after reset: state %s started %d (before %d)", after.State, after.StartedAtUnixMs, vm.StartedAtUnixMs)
	}
	if got := s.exec(t, id, "cat p2/kept.txt").Stdout; got != "survives reset\n" {
		t.Errorf("file after reset = %q", got)
	}
	if s.opts.Phase1.Processes {
		o, err := s.c.GetProcess(ctx, &nodev1.GetProcessRequest{SandboxId: id, ProcessId: 1})
		if err != nil || o.Process.State != nodev1.ProcessState_PROCESS_STATE_LOST {
			t.Fatalf("process after reset = %v, %v; want LOST", o.GetProcess().GetState(), err)
		}
	}
}

func (s *suite) testResetErrors(t *testing.T) {
	ctx := s.ctx(t)
	_, err := s.c.ResetVM(ctx, &nodev1.ResetVMRequest{SandboxId: "bx_unknown0"})
	wantCode(t, err, codes.NotFound)
	if !s.opts.Phase1.Stop {
		return
	}
	id := s.create(t, &nodev1.CreateVMRequest{}).SandboxId
	if _, err := s.c.StopVM(ctx, &nodev1.StopVMRequest{SandboxId: id, GraceSeconds: 5}); err != nil {
		t.Fatalf("StopVM: %v", err)
	}
	_, err = s.c.ResetVM(ctx, &nodev1.ResetVMRequest{SandboxId: id})
	wantCode(t, err, codes.FailedPrecondition)
}

func newSandboxID() string { return ids.Sandbox() }

// codeOf is the gRPC code of err (OK for nil).
func codeOf(err error) codes.Code { return grpcstatus.Code(err) }
