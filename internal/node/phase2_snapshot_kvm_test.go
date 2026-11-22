package node

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"uuid"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/nathabonfim59/bamboo-raft/internal/node/qemu"
	"github.com/nathabonfim59/bamboo-raft/internal/nodetest"
	"github.com/nathabonfim59/bamboo-raft/internal/objstore"
	"github.com/nathabonfim59/bamboo-raft/internal/objstore/memstore"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
	"github.com/nathabonfim59/bamboo-raft/internal/snapformat"
)

// Real-KVM SnapshotVM tests (WP2.4), non-jailed launcher, memstore as the
// object store. Same gate as kvm_test.go, plus qemu-img and debugfs
// (e2fsprogs) on PATH:
//
//	BBRAFT_TEST_KVM=1 BBRAFT_TEST_IMAGE_DIR=/path/to/images/out \
//	  TMPDIR=/path/with/30GiB-free \
//	  go test ./internal/node -run 'TestKVMSnapshot' -timeout 30m
//
// A manifest is checked by assembling it into a raw image (the base image
// converted to raw, every listed chunk written over it) and reading guest
// files out of that whole-disk ext4 with debugfs: the restored view of the
// generation, without the M3 restore path.

// snapvmKVMStore wraps memstore with a switch that fails every chunk put.
type snapvmKVMStore struct {
	objstore.Store
	failPuts atomic.Bool
}

func (s *snapvmKVMStore) PutIfAbsent(ctx context.Context, key string, r io.Reader, size int64) error {
	if s.failPuts.Load() && strings.HasPrefix(key, "chunks/") {
		return errors.New("injected object store outage")
	}
	return s.Store.PutIfAbsent(ctx, key, r, size)
}

type snapvmKVM struct {
	m      *Manager
	store  *snapvmKVMStore
	client nodev1.NodeClient
}

func snapvmKVMSetup(t *testing.T) *snapvmKVM {
	t.Helper()
	imageDir := requireKVM(t)
	for _, tool := range []string{"qemu-img", "debugfs"} {
		if _, err := exec.LookPath(tool); err != nil {
			t.Fatalf("BBRAFT_TEST_KVM=1 needs %s on PATH for the snapshot tests", tool)
		}
	}
	store := &snapvmKVMStore{Store: memstore.New()}
	cpuModel, allowHostCPU := kvmCPUOverride()
	m, err := New(t.Context(), Config{
		NodeID:              "kvm-snapvm",
		DataDir:             t.TempDir(),
		ImageDir:            imageDir,
		CPUModel:            cpuModel,
		AllowHostCPU:        allowHostCPU,
		Capacity:            &nodev1.Resources{Vcpus: 16, MemoryMib: 32 << 10, DiskGib: 200},
		BootTimeout:         2 * time.Minute,
		AllowUserNetworking: true,
		Store:               store,
		Log:                 slog.New(slog.DiscardHandler),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		if err := m.Shutdown(ctx); err != nil {
			t.Errorf("Shutdown: %v", err)
		}
	})
	return &snapvmKVM{m: m, store: store, client: startNodeClient(t, &harness{mg: m})}
}

func (k *snapvmKVM) create(t *testing.T) string {
	t.Helper()
	return bitmapKVMCreate(t, k.m)
}

func (k *snapvmKVM) exec(t *testing.T, id, cmd string) string {
	t.Helper()
	res, err := nodetest.Exec(t.Context(), k.client, &nodev1.ExecRequest{SandboxId: id, Command: cmd, TimeoutSeconds: 300})
	if err != nil || res.Exit.GetExitCode() != 0 {
		t.Fatalf("guest %q: %+v, %v", cmd, res, err)
	}
	return res.Stdout
}

// writeFile writes mib MiB of random data to /home/user/name, flushed, and
// returns its sha256.
func (k *snapvmKVM) writeFile(t *testing.T, id, name string, mib int) string {
	t.Helper()
	out := k.exec(t, id, fmt.Sprintf("dd if=/dev/urandom of=/home/user/%s bs=1M count=%d conv=fsync status=none && sync && sha256sum /home/user/%s", name, mib, name))
	return strings.Fields(out)[0]
}

func (k *snapvmKVM) snapshot(t *testing.T, id, chain string, gen uint32, freeze *bool) *nodev1.Generation {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
	defer cancel()
	g, err := k.m.SnapshotVM(ctx, &nodev1.SnapshotVMRequest{SandboxId: id, ChainId: chain, Generation: gen, Freeze: freeze})
	if err != nil {
		t.Fatalf("SnapshotVM %s gen %d: %v", id, gen, err)
	}
	return g
}

func (k *snapvmKVM) manifest(t *testing.T, key string) *snapformat.Manifest {
	t.Helper()
	rc, err := k.store.Get(t.Context(), key, 0, -1)
	if err != nil {
		t.Fatalf("get %s: %v", key, err)
	}
	defer rc.Close()
	raw, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}
	m, err := snapformat.DecodeManifest(raw)
	if err != nil {
		t.Fatalf("decode %s: %v", key, err)
	}
	return m
}

// assemble builds the raw disk a manifest describes: the base image as raw,
// grown to the manifest's size, every listed chunk written over it.
func (k *snapvmKVM) assemble(t *testing.T, key string) string {
	t.Helper()
	man := k.manifest(t, key)
	raw := filepath.Join(t.TempDir(), "disk.raw")
	base := filepath.Join(k.m.store.ImagePath(man.Image), "root.qcow2")
	if out, err := exec.CommandContext(t.Context(), "qemu-img", "convert", "-f", "qcow2", "-O", "raw", base, raw).CombinedOutput(); err != nil {
		t.Fatalf("qemu-img convert base: %v: %s", err, out)
	}
	f, err := os.OpenFile(raw, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if err := f.Truncate(int64(man.DiskBytes)); err != nil {
		t.Fatal(err)
	}
	zero := make([]byte, snapformat.ChunkSize)
	for _, e := range man.Chunks {
		off := int64(e.Index * snapformat.ChunkSize)
		n := man.ChunkLen(e.Index)
		if e.Zero {
			if _, err := f.WriteAt(zero[:n], off); err != nil {
				t.Fatal(err)
			}
			continue
		}
		obj := k.getObject(t, snapformat.ChunkKey(e.Hash))
		data, err := snapformat.DecodeChunk(e.Hash, obj, nil)
		if err != nil {
			t.Fatalf("chunk %d: %v", e.Index, err)
		}
		if uint64(len(data)) != n {
			t.Fatalf("chunk %d is %d bytes, want %d", e.Index, len(data), n)
		}
		if _, err := f.WriteAt(data, off); err != nil {
			t.Fatal(err)
		}
	}
	return raw
}

func (k *snapvmKVM) getObject(t *testing.T, key string) []byte {
	t.Helper()
	rc, err := k.store.Get(t.Context(), key, 0, -1)
	if err != nil {
		t.Fatalf("get %s: %v", key, err)
	}
	defer rc.Close()
	b, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// readFile reads an absolute guest path out of an assembled raw disk.
func snapvmKVMReadFile(t *testing.T, raw, guestPath string) []byte {
	t.Helper()
	out, err := exec.CommandContext(t.Context(), "debugfs", "-R", "cat "+guestPath, raw).Output()
	if err != nil {
		t.Fatalf("debugfs cat %s: %v", guestPath, err)
	}
	return out
}

func snapvmKVMSHA(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// The guest loop rewrites /home/user/loop as 2048 blocks of 4096 bytes,
// each holding the iteration number right-aligned, written to a temporary
// file, fsynced and renamed over the old one: any consistent cut shows one
// whole iteration.
const snapvmLoopScript = `cd /home/user && rm -f stop && i=0
while [ ! -e stop ]; do
  i=$((i+1))
  printf '%4095s\n' $(seq 2048 | sed "s/.*/$i/") | dd of=loop.tmp conv=fsync status=none
  mv loop.tmp loop
done
echo $i`

// snapvmLoopIteration parses a loop file: every block names the same
// iteration.
func snapvmLoopIteration(t *testing.T, data []byte) int {
	t.Helper()
	if len(data) != 2048*4096 {
		t.Fatalf("loop file is %d bytes, want %d (a torn file)", len(data), 2048*4096)
	}
	first := strings.TrimSpace(string(data[:4096]))
	for b := range 2048 {
		if got := strings.TrimSpace(string(data[b*4096 : (b+1)*4096])); got != first {
			t.Fatalf("loop block %d holds iteration %q, block 0 %q: the cut is not one consistent state", b, got, first)
		}
	}
	n, err := strconv.Atoi(first)
	if err != nil || n < 1 {
		t.Fatalf("loop iteration %q", first)
	}
	return n
}

// TestKVMSnapshotLiveFrozenView: a snapshot of a READY VM while the guest
// overwrites a file in a loop is one consistent frozen view (the loop file
// is one whole iteration, a file written before is intact); generation 1
// of the then-quiet disk uploads fewer chunks and matches the guest's final
// state.
func TestKVMSnapshotLiveFrozenView(t *testing.T) {
	k := snapvmKVMSetup(t)
	id := k.create(t)
	preSHA := k.writeFile(t, id, "pre", 16)

	var wg sync.WaitGroup
	var loopOut string
	var loopErr error
	wg.Go(func() {
		res, err := nodetest.Exec(context.WithoutCancel(t.Context()), k.client, &nodev1.ExecRequest{SandboxId: id, Command: snapvmLoopScript, TimeoutSeconds: 900})
		if err == nil && res.Exit.GetExitCode() != 0 {
			err = fmt.Errorf("loop exit %d: %s", res.Exit.GetExitCode(), res.Stderr)
		}
		if err == nil {
			loopOut = res.Stdout
		}
		loopErr = err
	})
	stopLoop := sync.OnceFunc(func() {
		// Also runs from Cleanup, after t.Context() ended.
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		if _, err := nodetest.Exec(ctx, k.client, &nodev1.ExecRequest{SandboxId: id, Command: "touch /home/user/stop"}); err != nil {
			t.Errorf("stop the guest loop: %v", err)
		}
		wg.Wait()
	})
	t.Cleanup(stopLoop)
	deadline := time.Now().Add(time.Minute)
	for strings.TrimSpace(k.exec(t, id, "test -e /home/user/loop && echo yes || true")) != "yes" {
		if time.Now().After(deadline) {
			t.Fatal("the guest loop never wrote its file")
		}
		time.Sleep(200 * time.Millisecond)
	}

	chain := uuid.NewV7().String()
	g0 := k.snapshot(t, id, chain, 0, nil)
	if !g0.GetFrozen() {
		t.Fatal("gen 0 not frozen")
	}
	stopLoop()
	if loopErr != nil {
		t.Fatal(loopErr)
	}
	raw0 := k.assemble(t, g0.GetManifestKey())
	if got := snapvmKVMSHA(snapvmKVMReadFile(t, raw0, "/home/user/pre")); got != preSHA {
		t.Fatalf("pre in gen 0: sha %s, want %s", got, preSHA)
	}
	cut := snapvmLoopIteration(t, snapvmKVMReadFile(t, raw0, "/home/user/loop"))
	final, err := strconv.Atoi(strings.TrimSpace(loopOut))
	if err != nil {
		t.Fatalf("loop output %q", loopOut)
	}
	t.Logf("gen 0: cut at loop iteration %d of %d; %d chunks, %d uploaded, %d bytes", cut, final, g0.GetChunks(), g0.GetChunksUploaded(), g0.GetBytesUploaded())
	if cut > final {
		t.Fatalf("cut iteration %d after the loop's last %d", cut, final)
	}

	k.exec(t, id, "sync")
	g1 := k.snapshot(t, id, chain, 1, nil)
	t.Logf("gen 1: %d chunks, %d uploaded", g1.GetChunks(), g1.GetChunksUploaded())
	if g1.GetChunksUploaded() >= g0.GetChunksUploaded() {
		t.Fatalf("gen 1 of a quiet disk uploaded %d chunks, gen 0 %d: want fewer", g1.GetChunksUploaded(), g0.GetChunksUploaded())
	}
	raw1 := k.assemble(t, g1.GetManifestKey())
	if got := snapvmLoopIteration(t, snapvmKVMReadFile(t, raw1, "/home/user/loop")); got != final {
		t.Fatalf("gen 1 loop iteration %d, want the final %d", got, final)
	}
	if got := snapvmKVMSHA(snapvmKVMReadFile(t, raw1, "/home/user/pre")); got != preSHA {
		t.Fatalf("pre in gen 1: sha %s, want %s", got, preSHA)
	}
	if st, err := k.m.store.ReadPhase2(id); err != nil || st.SnapshotInProgress || st.FullScanNext {
		t.Fatalf("phase 2 state %+v, %v", st, err)
	}
	bitmapKVMHealthy(t, "after two cuts", bitmapKVMRead(t, k.m, id))
}

// TestKVMSnapshotStoreFailureKeepsChanges: a generation that fails on an
// object store outage merges its cut back, so the next (incremental)
// generation still holds a file written before the failed one.
func TestKVMSnapshotStoreFailureKeepsChanges(t *testing.T) {
	k := snapvmKVMSetup(t)
	id := k.create(t)
	chain := uuid.NewV7().String()
	k.snapshot(t, id, chain, 0, nil)
	sha := k.writeFile(t, id, "a", 8)

	k.store.failPuts.Store(true)
	_, err := k.m.SnapshotVM(t.Context(), &nodev1.SnapshotVMRequest{SandboxId: id, ChainId: chain, Generation: 1})
	k.store.failPuts.Store(false)
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("SnapshotVM during an outage: %v, want UNAVAILABLE", err)
	}
	st, err := k.m.store.ReadPhase2(id)
	if err != nil || st.SnapshotInProgress || st.FullScanNext {
		t.Fatalf("after the failure: %+v, %v (want the cut merged back, no full scan)", st, err)
	}
	if bm := bitmapKVMRead(t, k.m, id); bm.Count < 8<<20 {
		t.Fatalf("bbraft-dirty counts %d after the merge back, want the 8 MiB write", bm.Count)
	}
	g2 := k.snapshot(t, id, chain, 2, nil)
	raw := k.assemble(t, g2.GetManifestKey())
	if got := snapvmKVMSHA(snapvmKVMReadFile(t, raw, "/home/user/a")); got != sha {
		t.Fatalf("file written before the failed generation: sha %s in gen 2, want %s", got, sha)
	}
}

// TestKVMSnapshotDisabledBitmapRegression is condition 1's regression: a
// bbraft-dirty stored disabled is re-enabled at StartVM, so a write after
// the start is in the next incremental manifest. The test clears the full
// scan flag StartVM sets, so the generation relies on the bitmap alone.
func TestKVMSnapshotDisabledBitmapRegression(t *testing.T) {
	k := snapvmKVMSetup(t)
	id := k.create(t)
	chain := uuid.NewV7().String()
	k.snapshot(t, id, chain, 0, nil)
	bitmapKVMStop(t, k.m, id)
	out, err := exec.CommandContext(t.Context(), "qemu-img", "bitmap", "--disable", "-f", "qcow2", k.m.store.DiskPath(id), qemu.DirtyBitmap).CombinedOutput()
	if err != nil {
		t.Fatalf("qemu-img bitmap --disable: %v: %s", err, out)
	}
	bitmapKVMStart(t, k.m, id)
	if err := k.m.store.ClearFullScanNext(id); err != nil {
		t.Fatal(err)
	}
	sha := k.writeFile(t, id, "b", 8)
	g1 := k.snapshot(t, id, chain, 1, nil)
	raw := k.assemble(t, g1.GetManifestKey())
	if got := snapvmKVMSHA(snapvmKVMReadFile(t, raw, "/home/user/b")); got != sha {
		t.Fatalf("write after a re-enabled bitmap: sha %s in the manifest, want %s", got, sha)
	}
}

// TestKVMSnapshotStoppedThenStart: a STOPPED VM's kept disk is snapshotted
// offline (frozen false, still STOPPED, a file written before the stop in
// the manifest), then StartVM boots it with a healthy bitmap, and the next
// live generation is incremental and holds a new write.
func TestKVMSnapshotStoppedThenStart(t *testing.T) {
	k := snapvmKVMSetup(t)
	id := k.create(t)
	chain := uuid.NewV7().String()
	k.snapshot(t, id, chain, 0, nil)
	shaC := k.writeFile(t, id, "c", 8)
	bitmapKVMStop(t, k.m, id)

	g1 := k.snapshot(t, id, chain, 1, nil)
	if g1.GetFrozen() {
		t.Fatal("a STOPPED snapshot reported frozen")
	}
	vm, err := k.m.GetVM(t.Context(), &nodev1.GetVMRequest{SandboxId: id})
	if err != nil || vm.GetState() != nodev1.VMState_VM_STATE_STOPPED {
		t.Fatalf("after the STOPPED snapshot: %v, %v", vm.GetState(), err)
	}
	if got := snapvmKVMSHA(snapvmKVMReadFile(t, k.assemble(t, g1.GetManifestKey()), "/home/user/c")); got != shaC {
		t.Fatalf("c in the STOPPED generation: sha %s, want %s", got, shaC)
	}
	if flags := bitmapKVMStoredFlags(t, k.m, id); !strings.Contains(strings.Join(flags, ","), "auto") {
		t.Fatalf("after the session the stored bitmap flags are %q, want auto (stored cleanly)", flags)
	}

	bitmapKVMStart(t, k.m, id)
	bitmapKVMHealthy(t, "start after a STOPPED snapshot", bitmapKVMRead(t, k.m, id))
	if got := strings.Fields(k.exec(t, id, "sha256sum /home/user/c"))[0]; got != shaC {
		t.Fatalf("c after StartVM: %s, want %s", got, shaC)
	}
	if bitmapKVMFullScan(t, k.m, id) {
		t.Fatal("a clean session forced a full scan")
	}
	shaD := k.writeFile(t, id, "d", 8)
	g2 := k.snapshot(t, id, chain, 2, nil)
	t.Logf("gen 2: %d chunks, %d uploaded (gen 1 lists %d)", g2.GetChunks(), g2.GetChunksUploaded(), g1.GetChunks())
	raw := k.assemble(t, g2.GetManifestKey())
	for name, want := range map[string]string{"c": shaC, "d": shaD} {
		if got := snapvmKVMSHA(snapvmKVMReadFile(t, raw, "/home/user/"+name)); got != want {
			t.Fatalf("%s in gen 2: sha %s, want %s", name, got, want)
		}
	}
}

// TestKVMSnapshotConformance runs nodetest's Phase 2 snapshot and restore
// groups against the real node (TestKVMConformanceDirect runs them with the
// rest of the suite).
func TestKVMSnapshotConformance(t *testing.T) {
	k := snapvmKVMSetup(t)
	nodetest.Run(t, k.client, nodetest.Options{
		BootTimeout: 2 * time.Minute,
		FenceWatch:  2 * time.Minute,
		Phase1:      nodetest.Phase1{Stop: true},
		Phase2:      nodetest.Phase2{Snapshots: true, Store: k.store},
	})
}
