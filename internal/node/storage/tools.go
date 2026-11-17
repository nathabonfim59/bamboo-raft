package storage

import (
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
	"uuid"
)

// D1: storage's view of jailed image tools. The production Imager runs
// every qemu-img operation as a jailed tool attempt through a ToolRunner
// (the node adapter wraps the jailer); the store never runs guest-influenced
// qemu-img as root.

// ErrToolRetained marks a tool failure after which the tool's target is
// still held (its exit or settlement is unproven). Every rollback that would
// remove the target or a directory containing it keeps them instead, in the
// same run and after a restart.
var ErrToolRetained = errors.New("storage: image tool retained its target")

// TargetLeases reports whether a jailed process holds an inode. Removal
// paths consult it before removing a tree that may contain a held target.
type TargetLeases interface {
	TargetHeld(dev, ino uint64) bool
}

// ImageRoot names the confined root an image file lives under.
type ImageRoot string

// Image roots.
const (
	ImageRootData   ImageRoot = "data"
	ImageRootImages ImageRoot = "images"
)

// ImageAccess is the access a tool run needs to its target (see the jailer's
// TargetAccess): shared-ro for verified bases, group-ro for reads of
// overlays and copies, owner-rw for writes.
type ImageAccess string

// Image accesses.
const (
	ImageShared  ImageAccess = "shared-ro"
	ImageGroupRO ImageAccess = "group-ro"
	ImageOwnerRW ImageAccess = "owner-rw"
)

// ImageRef is one pinned image file a tool operates on.
type ImageRef struct {
	// File is pinned no-follow: O_RDONLY for reads, O_RDWR for writes.
	File *os.File
	Root ImageRoot
	// Path is clean and relative to Root.
	Path string
	// Format is the explicit driver ("qcow2" or "raw"); never probed.
	Format string
	// BackingName is the exact backing name a create stores (with -u);
	// create only.
	BackingName string
	// ExpectedBacking is the authority for every other operation: the exact
	// stored backing name the Store expects, from its own verified image
	// record and never from the header (empty: standalone). The header is
	// compared with it before and after the tool runs; an overlay also needs
	// Base, which must itself be standalone.
	ExpectedBacking string
	// Base is the verified standalone base of an overlay, opened read-only;
	// the explicit graph names it and the stored name is never followed.
	Base *os.File
}

// ToolRequest is one jailed qemu-img run.
type ToolRequest struct {
	Attempt string
	Op      string // info|check|resize|create
	Driver  string // qcow2|raw
	Target  ImageRef
	Access  ImageAccess
	Size    uint64
	Stdout  io.Writer
	Stderr  io.Writer
}

// ToolOutcome is a completed run; success requires ExitCode 0.
type ToolOutcome struct {
	ExitCode int
}

// ToolRunner runs jailed tools. A run whose exit or settlement is unproven
// returns an error wrapping ErrToolRetained.
type ToolRunner interface {
	RunTool(ctx context.Context, req ToolRequest) (ToolOutcome, error)
	TargetLeases
}

// Tool output bounds, enforced at collection.
const (
	maxToolStdout = 1 << 20
	maxToolStderr = 512
)

var errOutputLimit = errors.New("tool output exceeds its bound")

// boundedBuffer collects at most limit bytes; a write past it fails and is
// remembered, so the tool's output copy stops at the bound.
type boundedBuffer struct {
	limit int
	buf   []byte
	over  bool
}

func (b *boundedBuffer) Write(p []byte) (int, error) {
	if b.over || len(b.buf)+len(p) > b.limit {
		b.over = true
		return 0, errOutputLimit
	}
	b.buf = append(b.buf, p...)
	return len(p), nil
}

// tailBuffer keeps only the last limit bytes it was given.
type tailBuffer struct {
	limit int
	buf   []byte
}

func (b *tailBuffer) Write(p []byte) (int, error) {
	n := len(p)
	if len(p) >= b.limit {
		b.buf = append(b.buf[:0], p[len(p)-b.limit:]...)
		return n, nil
	}
	if over := len(b.buf) + len(p) - b.limit; over > 0 {
		b.buf = append(b.buf[:0], b.buf[over:]...)
	}
	b.buf = append(b.buf, p...)
	return n, nil
}

// JailedImager runs each qemu-img operation through a ToolRunner with a
// fixed argv and a closed graph, after a Go header precheck against the
// Store's expected backing and with a header postcheck after the run.
type JailedImager struct {
	tools ToolRunner
}

// NewJailedImager returns the production image tool runner.
func NewJailedImager(tools ToolRunner) *JailedImager { return &JailedImager{tools: tools} }

// imageAccess derives the access an operation needs: image bases are only
// ever read shared-ro; reads of data files are group-ro, writes owner-rw.
func imageAccess(ref ImageRef, writes bool) (ImageAccess, error) {
	switch {
	case ref.Root == ImageRootImages && !writes:
		return ImageShared, nil
	case ref.Root == ImageRootImages:
		return "", errors.New("storage: an image base is never written")
	case ref.Root != ImageRootData:
		return "", fmt.Errorf("storage: unknown image root %q", ref.Root)
	case writes:
		return ImageOwnerRW, nil
	default:
		return ImageGroupRO, nil
	}
}

// checkRef validates a reference before any tool runs.
func checkRef(ref ImageRef, op string) error {
	if ref.File == nil {
		return errors.New("storage: image reference has no pinned file")
	}
	if ref.Path == "" || path.IsAbs(ref.Path) || path.Clean(ref.Path) != ref.Path || strings.HasPrefix(ref.Path, "../") {
		return fmt.Errorf("storage: unsafe image path %q", ref.Path)
	}
	switch ref.Format {
	case "qcow2":
	case "raw":
		if op != "info" && op != "check" {
			return fmt.Errorf("storage: %s of a raw image is not supported", op)
		}
		if ref.ExpectedBacking != "" || ref.Base != nil {
			return errors.New("storage: a raw image has no backing")
		}
	default:
		// The format is always explicit: qemu-img never probes it.
		return fmt.Errorf("storage: unsupported image format %q", ref.Format)
	}
	return nil
}

// checkBacking proves the closed graph from headers, before (and again
// after) a tool runs: the target's stored backing is exactly the Store's
// expected name, and an overlay's pinned base is itself standalone.
func checkBacking(ref ImageRef, expected string) error {
	if ref.Format != "qcow2" {
		return nil
	}
	header, err := readQCOWHeader(ref.File)
	if err != nil {
		return fmt.Errorf("storage: %s: %w", ref.Path, err)
	}
	if header.backing != expected {
		return fmt.Errorf("storage: %s: stored backing %q is not the expected %q", ref.Path, header.backing, expected)
	}
	if expected == "" {
		if ref.Base != nil {
			return fmt.Errorf("storage: %s: a standalone image takes no base", ref.Path)
		}
		return nil
	}
	if ref.Base == nil {
		return fmt.Errorf("storage: %s: an overlay needs its pinned base", ref.Path)
	}
	base, err := readQCOWHeader(ref.Base)
	if err != nil {
		return fmt.Errorf("storage: %s base: %w", ref.Path, err)
	}
	if base.backing != "" {
		return fmt.Errorf("storage: %s: the base has its own backing %q", ref.Path, base.backing)
	}
	return nil
}

// run executes one tool operation with bounded output and both header
// checks, returning the collected stdout.
func (j *JailedImager) run(ctx context.Context, op string, ref ImageRef, size uint64, expected string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("storage: qemu-img %s %s: %w", op, ref.Path, context.Cause(ctx))
	}
	writes := op == "resize" || op == "create"
	if err := checkRef(ref, op); err != nil {
		return nil, err
	}
	access, err := imageAccess(ref, writes)
	if err != nil {
		return nil, err
	}
	if op == "create" {
		info, err := ref.File.Stat()
		if err != nil {
			return nil, fmt.Errorf("storage: create %s: %w", ref.Path, err)
		}
		if info.Size() != 0 || ref.BackingName == "" || ref.Base != nil {
			return nil, fmt.Errorf("storage: create %s needs a new empty file and an exact backing name", ref.Path)
		}
	} else if err := checkBacking(ref, expected); err != nil {
		return nil, err
	}
	stdout := &boundedBuffer{limit: maxToolStdout}
	stderr := &tailBuffer{limit: maxToolStderr}
	target := ref
	if op == "info" {
		// The Store pinned the base and proved it standalone (checkBacking),
		// but qemu-img info always opens without a backing: it gets the
		// target alone.
		target.Base = nil
	}
	out, err := j.tools.RunTool(ctx, ToolRequest{
		Attempt: uuid.NewV7().String(), Op: op, Driver: ref.Format, Target: target, Access: access, Size: size,
		Stdout: stdout, Stderr: stderr,
	})
	if err != nil {
		return nil, fmt.Errorf("storage: qemu-img %s %s: %w%s", op, ref.Path, err, errSuffix(string(stderr.buf)))
	}
	if stdout.over {
		return nil, fmt.Errorf("storage: qemu-img %s %s: output exceeds %d bytes", op, ref.Path, maxToolStdout)
	}
	if out.ExitCode != 0 {
		return nil, fmt.Errorf("storage: qemu-img %s %s: exit %d%s", op, ref.Path, out.ExitCode, errSuffix(string(stderr.buf)))
	}
	if op == "create" {
		// A create stores exactly its backing name and opens no base.
		header, err := readQCOWHeader(ref.File)
		if err != nil {
			return nil, fmt.Errorf("storage: created %s: %w", ref.Path, err)
		}
		if header.backing != ref.BackingName {
			return nil, fmt.Errorf("storage: created %s stores backing %q, not %q", ref.Path, header.backing, ref.BackingName)
		}
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("storage: qemu-img %s %s: %w", op, ref.Path, context.Cause(ctx))
		}
		return stdout.buf, nil
	}
	if err := checkBacking(ref, expected); err != nil {
		return nil, fmt.Errorf("storage: header changed during qemu-img %s: %w", op, err)
	}
	// A run whose context ended is never success, whatever the tool did.
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("storage: qemu-img %s %s: %w", op, ref.Path, context.Cause(ctx))
	}
	return stdout.buf, nil
}

// CreateOverlay creates dst (owner-rw), a new empty pinned file, as a qcow2
// overlay storing exactly dst.BackingName with -u: the base is not opened.
func (j *JailedImager) CreateOverlay(ctx context.Context, dst ImageRef, virtualSize uint64) error {
	if dst.Format != "qcow2" {
		return fmt.Errorf("storage: create %s: format %q, want qcow2", dst.Path, dst.Format)
	}
	_, err := j.run(ctx, "create", dst, virtualSize, "")
	return err
}

// Grow grows img (owner-rw) to virtualSize through the closed graph.
func (j *JailedImager) Grow(ctx context.Context, img ImageRef, virtualSize uint64) error {
	_, err := j.run(ctx, "resize", img, virtualSize, img.ExpectedBacking)
	return err
}

// Info inspects img with a strictly parsed, bounded JSON report.
func (j *JailedImager) Info(ctx context.Context, img ImageRef) (QemuInfo, error) {
	out, err := j.run(ctx, "info", img, 0, img.ExpectedBacking)
	if err != nil {
		return QemuInfo{}, err
	}
	var v struct {
		Format      string `json:"format"`
		VirtualSize uint64 `json:"virtual-size"`
		Backing     string `json:"backing-filename"`
	}
	if err := json.Unmarshal(out, &v); err != nil {
		return QemuInfo{}, fmt.Errorf("storage: qemu-img info %s: invalid json output: %w", img.Path, err)
	}
	switch {
	case v.Format != img.Format:
		return QemuInfo{}, fmt.Errorf("storage: qemu-img info %s: format %q, want %q", img.Path, v.Format, img.Format)
	case v.VirtualSize == 0:
		return QemuInfo{}, fmt.Errorf("storage: qemu-img info %s: zero virtual size", img.Path)
	case v.Backing != img.ExpectedBacking:
		return QemuInfo{}, fmt.Errorf("storage: qemu-img info %s: backing %q, want %q", img.Path, v.Backing, img.ExpectedBacking)
	}
	return QemuInfo{Format: v.Format, VirtualSize: v.VirtualSize, BackingFile: img.ExpectedBacking}, nil
}

// Check checks img and requires a clean, strictly parsed report.
func (j *JailedImager) Check(ctx context.Context, img ImageRef) error {
	out, err := j.run(ctx, "check", img, 0, img.ExpectedBacking)
	if err != nil {
		return err
	}
	var v struct {
		CheckErrors *int64 `json:"check-errors"`
	}
	if err := json.Unmarshal(out, &v); err != nil {
		return fmt.Errorf("storage: qemu-img check %s: invalid json output: %w", img.Path, err)
	}
	if v.CheckErrors != nil && *v.CheckErrors != 0 {
		return fmt.Errorf("storage: qemu-img check %s: %d errors", img.Path, *v.CheckErrors)
	}
	return nil
}
