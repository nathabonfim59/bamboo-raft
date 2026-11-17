package qemu

import (
	"context"
	"fmt"

	"github.com/nathabonfim59/bamboo-raft/internal/node/qmp"
)

// Pause stops guest execution through client and verifies the resulting state
// before a caller copies the disk. It waits for the stop command reply (not the
// asynchronous STOP event), requires strict paused status, and requires the
// inserted rootdisk BlockInfo to carry top-level io-status=ok.
//
// QEMU's qmp_stop ignores the return value of its final bdrv_flush_all(), so
// this is a strong precondition, not a durability proof. Snapshot code must
// still run qemu-img Check against the staged copy and validate its contents.
func Pause(ctx context.Context, client *qmp.Client, id string) error {
	if client == nil {
		return fmt.Errorf("qemu: %s: no QMP client", id)
	}
	if err := client.Execute(ctx, "stop", nil, nil); err != nil {
		return fmt.Errorf("qemu: %s: stop: %w", id, err)
	}

	var status queryStatus
	if err := client.Execute(ctx, "query-status", nil, &status); err != nil {
		return fmt.Errorf("qemu: %s: query-status after stop: %w", id, err)
	}
	if status.Status == "" {
		return fmt.Errorf("qemu: %s: query-status returned no status after stop", id)
	}
	if status.Running || status.Status != "paused" {
		return fmt.Errorf("qemu: %s: stop replied but status is %q (running=%t)", id, status.Status, status.Running)
	}

	var blocks []queryBlockEntry
	if err := client.Execute(ctx, "query-block", nil, &blocks); err != nil {
		return fmt.Errorf("qemu: %s: query-block after stop: %w", id, err)
	}
	diskSeen := false
	for _, b := range blocks {
		if b.Inserted == nil || b.Inserted.NodeName != rootDiskNode {
			continue
		}
		diskSeen = true
		if b.IOStatus != "ok" {
			return fmt.Errorf("qemu: %s: root disk %q io-status %q after stop", id, b.Device, b.IOStatus)
		}
	}
	if !diskSeen {
		return fmt.Errorf("qemu: %s: query-block has no inserted %q device after stop", id, rootDiskNode)
	}
	return nil
}

// Resume continues guest execution through client and verifies strict running
// status after the cont reply.
func Resume(ctx context.Context, client *qmp.Client, id string) error {
	if client == nil {
		return fmt.Errorf("qemu: %s: no QMP client", id)
	}
	if err := client.Execute(ctx, "cont", nil, nil); err != nil {
		return fmt.Errorf("qemu: %s: cont: %w", id, err)
	}
	var status queryStatus
	if err := client.Execute(ctx, "query-status", nil, &status); err != nil {
		return fmt.Errorf("qemu: %s: query-status after cont: %w", id, err)
	}
	if status.Status == "" {
		return fmt.Errorf("qemu: %s: query-status returned no status after cont", id)
	}
	if !status.Running || status.Status != "running" {
		return fmt.Errorf("qemu: %s: cont replied but status is %q (running=%t)", id, status.Status, status.Running)
	}
	return nil
}
