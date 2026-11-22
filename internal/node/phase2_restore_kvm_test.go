package node

import (
	"bytes"
	"context"
	"testing"
	"time"
	"uuid"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/nathabonfim59/bamboo-raft/internal/ids"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
	"github.com/nathabonfim59/bamboo-raft/internal/snapformat"
)

// TestKVMRestoreCorruptChunk restores a real generation after replacing one
// of its chunk objects with bytes that do not match the chunk's hash: the
// CreateVM fails INTERNAL instead of booting a wrong disk, and the VM is a
// failed boot (FAILED, not hydrated) that DeleteVM removes. Same gate as
// the other real-KVM snapshot tests (snapvmKVMSetup):
//
//	BBRAFT_TEST_KVM=1 BBRAFT_TEST_IMAGE_DIR=/path/to/images/out \
//	  go test ./internal/node -run 'TestKVMRestore' -timeout 20m
func TestKVMRestoreCorruptChunk(t *testing.T) {
	k := snapvmKVMSetup(t)
	src := k.create(t)
	k.writeFile(t, src, "restore-corrupt.bin", 8)
	g := k.snapshot(t, src, uuid.NewV7().String(), 0, nil)
	man := k.manifest(t, g.ManifestKey)

	var victim snapformat.Entry
	for _, e := range man.Chunks {
		if !e.Zero {
			victim = e
			break
		}
	}
	if victim.Hash == (snapformat.Hash{}) {
		t.Fatalf("generation %s lists no data chunk", g.ManifestKey)
	}
	key := snapformat.ChunkKey(victim.Hash)
	if err := k.store.Store.Delete(t.Context(), key); err != nil {
		t.Fatalf("delete %s: %v", key, err)
	}
	// A well-formed chunk object, but of other bytes.
	_, bad, err := snapformat.EncodeChunk(bytes.Repeat([]byte{0x5a}, snapformat.ChunkSize))
	if err != nil {
		t.Fatal(err)
	}
	if err := k.store.Store.PutIfAbsent(t.Context(), key, bytes.NewReader(bad), int64(len(bad))); err != nil {
		t.Fatalf("put corrupt %s: %v", key, err)
	}

	id := ids.Sandbox()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		k.m.DeleteVM(ctx, &nodev1.DeleteVMRequest{SandboxId: id})
	})
	_, err = k.m.CreateVM(t.Context(), &nodev1.CreateVMRequest{
		SandboxId: id, FromManifest: g.ManifestKey,
		Resources: &nodev1.Resources{Vcpus: 2, MemoryMib: 2048, DiskGib: 10},
	})
	if status.Code(err) != codes.Internal {
		t.Fatalf("CreateVM from a manifest with a corrupt chunk = %v, want INTERNAL", err)
	}
	vm, err := k.m.GetVM(t.Context(), &nodev1.GetVMRequest{SandboxId: id})
	if err != nil || vm.State != nodev1.VMState_VM_STATE_FAILED || vm.GetHydration().GetHydrated() {
		t.Fatalf("after the failed restore GetVM = %+v, %v; want FAILED and not hydrated", vm, err)
	}
	if _, err := k.m.DeleteVM(t.Context(), &nodev1.DeleteVMRequest{SandboxId: id}); err != nil {
		t.Fatalf("DeleteVM: %v", err)
	}
	if _, err := k.m.GetVM(t.Context(), &nodev1.GetVMRequest{SandboxId: id}); status.Code(err) != codes.NotFound {
		t.Fatalf("GetVM after DeleteVM = %v, want NOT_FOUND", err)
	}
}
