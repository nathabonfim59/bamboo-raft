package node

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"
	"uuid"

	"github.com/nathabonfim59/bamboo-raft/internal/ids"
	"github.com/nathabonfim59/bamboo-raft/internal/objstore"
	"github.com/nathabonfim59/bamboo-raft/internal/objstore/memstore"
	"github.com/nathabonfim59/bamboo-raft/internal/objstore/s3"
	"github.com/nathabonfim59/bamboo-raft/internal/objstore/s3/s3test"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

// crossNodeStore is the one bucket both nodes of a cross-node test use: a
// real Garage when BBRAFT_TEST_S3_GARAGE=1 (the garage binary on PATH, see
// s3test.StartGarage), a memstore otherwise.
func crossNodeStore(t *testing.T) objstore.Store {
	t.Helper()
	if os.Getenv("BBRAFT_TEST_S3_GARAGE") != "1" {
		t.Log("object store: memstore (set BBRAFT_TEST_S3_GARAGE=1 for a real Garage)")
		return memstore.New()
	}
	store, err := s3.New(t.Context(), s3test.StartGarage(t))
	if err != nil {
		t.Fatalf("s3.New against Garage: %v", err)
	}
	t.Log("object store: Garage")
	return store
}

// crossNode is one node of TestKVMCrossNodeRestore: its own data dir and
// Manager, the shared store.
type crossNode struct {
	*snapvmKVM
	name string
}

func newCrossNode(t *testing.T, imageDir, name string, minCID uint32, store objstore.Store) *crossNode {
	t.Helper()
	m := newKVMManagerWith(t, imageDir, func(cfg *Config) {
		cfg.NodeID = "kvm-cross-" + name
		// Both managers share this host's vsock CID space: disjoint ranges,
		// or the two nodes' first VMs would both ask vhost-vsock for CID 3.
		cfg.MinCID, cfg.MaxCID = minCID, minCID+999
		cfg.Store = store
		cfg.ChunkCacheBytes = 1 << 30
	})
	return &crossNode{
		snapvmKVM: &snapvmKVM{m: m, store: &snapvmKVMStore{Store: store}, client: startNodeClient(t, &harness{mg: m})},
		name:      name,
	}
}

// restore creates a VM from a manifest on this node and returns its id.
func (n *crossNode) restore(t *testing.T, manifestKey string) string {
	t.Helper()
	id := ids.Sandbox()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
	defer cancel()
	vm, err := n.m.CreateVM(ctx, &nodev1.CreateVMRequest{
		SandboxId: id, FromManifest: manifestKey,
		Resources: &nodev1.Resources{Vcpus: 2, MemoryMib: 2048, DiskGib: 10},
	})
	if err != nil {
		t.Fatalf("node %s: CreateVM from %s: %v", n.name, manifestKey, err)
	}
	t.Cleanup(func() {
		dctx, dcancel := context.WithTimeout(context.Background(), time.Minute)
		defer dcancel()
		n.m.DeleteVM(dctx, &nodev1.DeleteVMRequest{SandboxId: id})
	})
	if h := vm.GetHydration(); !h.GetHydrated() {
		t.Fatalf("node %s: restored VM %s hydration %+v, want hydrated", n.name, id, h)
	}
	return id
}

// sha is the sha256 of a file under /home/user as the guest sees it.
func (n *crossNode) sha(t *testing.T, id, name string) string {
	t.Helper()
	return strings.Fields(n.exec(t, id, "sha256sum /home/user/"+name))[0]
}

// TestKVMCrossNodeRestore: two nodes (separate data dirs, one object store).
// A snapshots a VM; B restores it from the manifest and reads A's files back;
// B snapshots the restored VM into a NEW chain whose first generation is
// incremental over the restored manifest (it uploads fewer chunks than the
// disk lists, a file B wrote is in it); A restores that chain and sees both
// nodes' files. Needs BBRAFT_TEST_KVM=1, BBRAFT_TEST_IMAGE_DIR, qemu-img and
// debugfs on PATH like the other snapshot tests; BBRAFT_TEST_S3_GARAGE=1
// swaps the in-process memstore for a real Garage:
//
//	BBRAFT_TEST_KVM=1 BBRAFT_TEST_IMAGE_DIR=/path/to/images/out \
//	  BBRAFT_TEST_S3_GARAGE=1 go test ./internal/node -run TestKVMCrossNodeRestore -v -timeout 30m
func TestKVMCrossNodeRestore(t *testing.T) {
	imageDir := requireKVM(t)
	store := crossNodeStore(t)
	a := newCrossNode(t, imageDir, "a", 10000, store)
	b := newCrossNode(t, imageDir, "b", 20000, store)
	if a.m.cfg.DataDir == b.m.cfg.DataDir {
		t.Fatal("both nodes share a data dir")
	}

	// Node A: a VM with data, snapshotted.
	srcA := a.create(t)
	shaA := a.writeFile(t, srcA, "from-a.bin", 12)
	chainA := uuid.NewV7().String()
	g0 := a.snapshot(t, srcA, chainA, 0, nil)
	t.Logf("node a: chain %s gen 0: %d chunks, %d uploaded", chainA, g0.GetChunks(), g0.GetChunksUploaded())

	// Node B: restore it, read A's file back.
	restoredB := b.restore(t, g0.GetManifestKey())
	if got := b.sha(t, restoredB, "from-a.bin"); got != shaA {
		t.Fatalf("node b: from-a.bin sha %s after the restore, node a wrote %s", got, shaA)
	}

	// Node B: write, then snapshot the restored VM into a new chain.
	shaB := b.writeFile(t, restoredB, "from-b.bin", 8)
	chainB := uuid.NewV7().String()
	if chainB == chainA {
		t.Fatal("chain ids collide")
	}
	gB := b.snapshot(t, restoredB, chainB, 0, nil)
	t.Logf("node b: chain %s gen 0: %d chunks, %d uploaded (node a's gen 0 uploaded %d)", chainB, gB.GetChunks(), gB.GetChunksUploaded(), g0.GetChunksUploaded())
	if gB.GetChunksUploaded() >= gB.GetChunks() {
		t.Fatalf("node b's first generation of the new chain uploaded %d of %d chunks: it should be incremental over the restored manifest (chunks A already stored are not uploaded again)",
			gB.GetChunksUploaded(), gB.GetChunks())
	}
	if gB.GetChunksUploaded() >= g0.GetChunksUploaded() {
		t.Fatalf("node b uploaded %d chunks for its new chain, node a's full first generation uploaded %d: nothing was reused", gB.GetChunksUploaded(), g0.GetChunksUploaded())
	}

	// Node A: restore node B's chain, both files are there.
	restoredA := a.restore(t, gB.GetManifestKey())
	for name, want := range map[string]string{"from-a.bin": shaA, "from-b.bin": shaB} {
		if got := a.sha(t, restoredA, name); got != want {
			t.Fatalf("node a: %s sha %s after restoring node b's chain, want %s", name, got, want)
		}
	}
}
