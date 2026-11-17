package node

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"syscall"

	"github.com/nathabonfim59/bamboo-raft/internal/node/jailer"
	"github.com/nathabonfim59/bamboo-raft/internal/node/storage"
)

// D1: the node's adapters between storage's jailed image tools and the
// jailer.

// DefaultJailToolBinary is the trusted qemu-img the jailer stages.
const DefaultJailToolBinary = "/usr/bin/qemu-img"

// jailToolMemory bounds one image tool (memory.max, and RLIMIT_AS).
const jailToolMemory = 1 << 30

// jailToolRunner runs storage's tool requests as jailed tool attempts.
type jailToolRunner struct {
	m      *jailer.Manager
	binary string
	io     jailer.IOLimits
}

// RunTool implements storage.ToolRunner.
func (r jailToolRunner) RunTool(ctx context.Context, req storage.ToolRequest) (storage.ToolOutcome, error) {
	spec := jailer.ToolSpec{
		Attempt: req.Attempt, Binary: r.binary, Op: jailer.ToolOp(req.Op), Driver: req.Driver,
		Target: req.Target.File, TargetRoot: jailer.TargetRoot(req.Target.Root), TargetPath: req.Target.Path,
		TargetAccess: jailer.TargetAccess(req.Access), Base: req.Target.Base, BackingName: req.Target.BackingName,
		Size: req.Size, Timeout: jailer.MaxToolTimeout,
		Limits: jailer.ToolLimits{CPUQuotaPercent: 100, MemoryBytes: jailToolMemory, Pids: 64, IO: r.io},
		Stdout: req.Stdout, Stderr: req.Stderr,
	}
	if req.Op != "create" {
		spec.BackingName = ""
	}
	res, err := r.m.RunTool(ctx, spec)
	if errors.Is(err, jailer.ErrToolRetained) {
		err = fmt.Errorf("%w: %w", storage.ErrToolRetained, err)
	}
	return storage.ToolOutcome{ExitCode: res.ExitCode}, err
}

// TargetHeld implements storage.TargetLeases.
func (r jailToolRunner) TargetHeld(dev, ino uint64) bool {
	_, held := r.m.TargetHolder(dev, ino)
	return held
}

// jailLeases exposes a jailRunner's ownership index to storage.
type jailLeases struct{ jail jailRunner }

func (l jailLeases) TargetHeld(dev, ino uint64) bool {
	_, held := l.jail.TargetHolder(dev, ino)
	return held
}

// diskHeldByTool fails when a jailed image tool still holds id's disk: the
// caller keeps the disk, network and reservation until it is settled. The
// VM's own jail is settled before this check, so any holder is foreign.
func (m *Manager) diskHeldByTool(id string) error {
	if m.jail == nil {
		return nil
	}
	info, err := os.Lstat(m.store.DiskPath(id))
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("node: %s: inspect disk: %w", id, err)
	}
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return fmt.Errorf("node: %s: disk has no inode identity", id)
	}
	if ref, held := m.jail.TargetHolder(uint64(st.Dev), st.Ino); held {
		return fmt.Errorf("node: %s: disk is held by %s attempt %s: %w", id, ref.SandboxID, ref.Attempt, storage.ErrToolRetained)
	}
	return nil
}
