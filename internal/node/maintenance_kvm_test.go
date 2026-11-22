package node

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"

	"github.com/nathabonfim59/bamboo-raft/internal/node/nbd"
	"github.com/nathabonfim59/bamboo-raft/internal/node/storage"
	"github.com/nathabonfim59/bamboo-raft/internal/nodetest"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

// Maintenance sessions on real QEMU (non-jailed launcher). They need:
//
//	BBRAFT_TEST_KVM=1 BBRAFT_TEST_IMAGE_DIR=/path/to/images/out \
//	  go test ./internal/node -run 'TestKVMMaintenance' -timeout 20m

// maintKVMStopped creates and stops one VM on a real-KVM manager.
func maintKVMStopped(t *testing.T, m *Manager, id string, diskGiB uint64) *vm {
	t.Helper()
	if _, err := m.CreateVM(t.Context(), &nodev1.CreateVMRequest{SandboxId: id, Resources: &nodev1.Resources{Vcpus: 2, MemoryMib: 2048, DiskGib: diskGiB}}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { m.DeleteVM(context.Background(), &nodev1.DeleteVMRequest{SandboxId: id}) })
	if _, err := m.StopVM(t.Context(), &nodev1.StopVMRequest{SandboxId: id, GraceSeconds: 30}); err != nil {
		t.Fatalf("StopVM: %v", err)
	}
	v, err := m.lookup(id)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

// TestKVMMaintenanceRestoreWrite drives a paused maintenance QEMU of a
// STOPPED VM: a writable NBD export of rootdisk, a pattern written with the
// node's NBD client near the end of the disk, a clean quit, then StartVM:
// the guest reads the pattern raw from /dev/vda.
func TestKVMMaintenanceRestoreWrite(t *testing.T) {
	imageDir := requireKVM(t)
	m := newKVMManager(t, imageDir)
	client := startNodeClient(t, &harness{mg: m})
	const id = "bx_mantkv22"
	const diskGiB = 10
	v := maintKVMStopped(t, m, id, diskGiB)

	pattern := bytes.Repeat([]byte("bbraft-maint-"), (1<<20)/13+1)[:1<<20]
	const offset = diskGiB<<30 - 8<<20 // unused space at the end of the fs
	var seen []string
	err := m.maintenance(t.Context(), v, func(ctx context.Context, s *maintSession) error {
		var status struct {
			Status string `json:"status"`
		}
		if err := s.QMP().Execute(ctx, "query-status", nil, &status); err != nil {
			return err
		}
		if status.Status != "prelaunch" {
			return fmt.Errorf("maintenance QEMU status %q, want prelaunch (never continued)", status.Status)
		}
		var blocks []struct {
			Device string `json:"device"`
		}
		if err := s.QMP().Execute(ctx, "query-block", nil, &blocks); err != nil {
			return err
		}
		for _, b := range blocks {
			seen = append(seen, b.Device)
		}
		if _, err := s.StartNBD(ctx); err != nil {
			return err
		}
		export := map[string]any{"type": "nbd", "id": "bbraft-restore", "node-name": "rootdisk", "name": "restore", "writable": true}
		if err := s.QMP().Execute(ctx, "block-export-add", export, nil); err != nil {
			return fmt.Errorf("block-export-add: %w", err)
		}
		c, err := s.DialNBD(ctx, nbd.Options{Export: "restore"})
		if err != nil {
			return err
		}
		if c.Size() != diskGiB<<30 {
			return fmt.Errorf("export size %d, want %d", c.Size(), uint64(diskGiB)<<30)
		}
		if err := c.Write(ctx, offset, pattern); err != nil {
			return fmt.Errorf("nbd write: %w", err)
		}
		if err := c.Flush(ctx); err != nil {
			return fmt.Errorf("nbd flush: %w", err)
		}
		if err := c.Close(); err != nil {
			return err
		}
		return s.StopNBD(ctx)
	})
	if err != nil {
		t.Fatalf("maintenance: %v", err)
	}
	if slices.ContainsFunc(seen, func(d string) bool { return d != "" }) {
		t.Fatalf("a maintenance QEMU has a block device attached: %q", seen)
	}
	if _, err := os.Lstat(filepath.Join(m.store.VMDir(id), storage.NBDSocketName)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("nbd.sock after the session: %v", err)
	}
	if _, err := m.StartVM(t.Context(), &nodev1.StartVMRequest{SandboxId: id}); err != nil {
		t.Fatalf("StartVM: %v", err)
	}
	cmd := fmt.Sprintf("sudo dd if=/dev/vda bs=1M skip=%d count=1 status=none | sha256sum", offset>>20)
	res, err := nodetest.Exec(t.Context(), client, &nodev1.ExecRequest{SandboxId: id, Command: cmd})
	if err != nil || res.Exit.GetExitCode() != 0 {
		t.Fatalf("Exec(%q): %+v, %v", cmd, res, err)
	}
	sum := sha256.Sum256(pattern)
	if got, want := strings.Fields(res.Stdout), hex.EncodeToString(sum[:]); len(got) == 0 || got[0] != want {
		t.Fatalf("guest sees %q at offset %d, want sha256 %s", res.Stdout, offset, want)
	}
}

// TestKVMMaintenanceDeleteFences deletes a VM while its maintenance session
// runs: DeleteVM returns promptly, the session ends ABORTED, and no QEMU,
// socket or VM directory is left.
func TestKVMMaintenanceDeleteFences(t *testing.T) {
	imageDir := requireKVM(t)
	m := newKVMManager(t, imageDir)
	const id = "bx_mantkv33"
	v := maintKVMStopped(t, m, id, 10)
	entered := make(chan *maintSession, 1)
	done := make(chan error, 1)
	go func() {
		done <- m.maintenance(context.Background(), v, func(ctx context.Context, s *maintSession) error {
			if _, err := s.StartNBD(ctx); err != nil {
				return err
			}
			entered <- s
			<-ctx.Done()
			return ctx.Err()
		})
	}()
	var s *maintSession
	select {
	case s = <-entered:
	case err := <-done:
		t.Fatalf("maintenance ended before the fence: %v", err)
	case <-time.After(time.Minute):
		t.Fatal("maintenance session did not start")
	}
	pid := s.PID()
	start := time.Now()
	if _, err := m.DeleteVM(t.Context(), &nodev1.DeleteVMRequest{SandboxId: id}); err != nil {
		t.Fatalf("DeleteVM: %v", err)
	}
	if took := time.Since(start); took > 30*time.Second {
		t.Fatalf("DeleteVM took %s with a session running", took)
	}
	wantCode(t, <-done, codes.Aborted)
	if _, err := os.Stat(fmt.Sprintf("/proc/%d", pid)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("maintenance QEMU pid %d still exists: %v", pid, err)
	}
	if _, err := os.Stat(m.store.VMDir(id)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("VM directory after DeleteVM: %v", err)
	}
}
