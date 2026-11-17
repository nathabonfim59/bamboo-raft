package jailer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
)

// ProbeSpec describes the node's CPU admission probe: one paused (-S),
// CPU-only QEMU with no kernel, disk, network or vsock, jailed exactly like
// a VM. Its argv is fixed by StartProbe; only the CPU option is the caller's,
// and it must pass the same validCPU rule as a VM's -cpu value.
type ProbeSpec struct {
	// Attempt is the caller-chosen immutable generation identity (uuid).
	Attempt string
	// Binary is the trusted host QEMU, staged like a VM's.
	Binary string
	// CPU is the resolved enforced CPU option, for example
	// "Haswell-noTSX,+vmx,enforce".
	CPU string
	// ControlDir and SerialLog are pinned per-attempt scratch resources with
	// the same rules as LaunchSpec's.
	ControlDir *os.File
	SerialLog  *os.File
	Limits     Limits
	Stdout     io.Writer
	Stderr     io.Writer
}

// ProbeMemoryMiB is the probe machine's fixed memory size.
const ProbeMemoryMiB = 128

// probeArgs is the complete fixed probe argv (without argv0).
func probeArgs(cpu string) []string {
	return []string{
		"-name", "guest=" + ProbeID + ",process=" + ProbeID,
		"-machine", argMachine,
		"-cpu", cpu,
		"-S",
		"-nodefaults", "-no-user-config", "-no-reboot",
		"-m", fmt.Sprintf("%dM", ProbeMemoryMiB),
		"-smp", "1",
		"-chardev", argChardev,
		"-serial", argSerial,
		"-qmp", argQMP,
		"-display", argDisplay,
		"-sandbox", SandboxPolicy,
	}
}

// launchSpec converts a validated probe into the internal launch form: no
// inherited descriptors and no disks, so neither device roles nor ownership
// of an overlay apply.
func (p ProbeSpec) launchSpec() LaunchSpec {
	return LaunchSpec{
		SandboxID:  ProbeID,
		Attempt:    p.Attempt,
		Binary:     p.Binary,
		Args:       probeArgs(p.CPU),
		ControlDir: p.ControlDir,
		SerialLog:  p.SerialLog,
		Limits:     p.Limits,
		Stdout:     p.Stdout,
		Stderr:     p.Stderr,
	}
}

func (p ProbeSpec) validate() error {
	if !validAttempt(p.Attempt) {
		return fmt.Errorf("jailer: invalid probe attempt %q", p.Attempt)
	}
	if err := cleanAbsolute("QEMU binary", p.Binary); err != nil {
		return err
	}
	if _, err := runtimeTarget(p.Binary); err != nil {
		return err
	}
	if !validCPU(p.CPU) {
		return fmt.Errorf("jailer: unsupported probe -cpu value %q", p.CPU)
	}
	spec := p.launchSpec()
	if spec.ControlDir == nil || spec.SerialLog == nil {
		return errors.New("jailer: pinned probe control directory and serial log are required")
	}
	info, err := spec.ControlDir.Stat()
	if err != nil {
		return fmt.Errorf("jailer: probe control directory: %w", err)
	}
	if !info.IsDir() {
		return errors.New("jailer: probe control descriptor is not a directory")
	}
	info, err = spec.SerialLog.Stat()
	if err != nil {
		return fmt.Errorf("jailer: probe serial log: %w", err)
	}
	if !info.Mode().IsRegular() {
		return errors.New("jailer: probe serial log is not a regular file")
	}
	if err := spec.checkAliases(); err != nil {
		return err
	}
	if err := spec.Limits.Validate(); err != nil {
		return err
	}
	return validateControlEntries(spec.ControlDir)
}

// StartProbe launches the CPU admission probe under ProbeID with the same
// durable reservation, birth containment, credential drop, chroot and exec
// proof as Start. Its argv is fixed (probeArgs); there is no caller-supplied
// option. A retained probe record returns ErrRetained: the caller settles it
// with ReleaseAttempt first. The caller owns the sole Cmd.Wait and Release,
// exactly as for Start.
func (m *Manager) StartProbe(ctx context.Context, spec ProbeSpec) (*Child, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.root == nil {
		return nil, ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := spec.validate(); err != nil {
		return nil, err
	}
	return m.launchLocked(ctx, spec.launchSpec(), KindProbe, nil)
}
