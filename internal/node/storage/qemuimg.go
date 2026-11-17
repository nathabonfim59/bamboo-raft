package storage

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

// Imager creates and inspects qcow2 files through pinned references. The
// production imager runs every operation as a jailed tool attempt
// (JailedImager); tests inject a recorder.
type Imager interface {
	// CreateOverlay makes dst, a new empty pinned file, a qcow2 overlay
	// storing exactly dst.BackingName, with the given virtual size.
	CreateOverlay(ctx context.Context, dst ImageRef, virtualSize uint64) error
	// Grow grows img's virtual size to virtualSize bytes. Only growing is
	// supported; shrinking is refused by the caller first.
	Grow(ctx context.Context, img ImageRef, virtualSize uint64) error
	// Info reports img's format, virtual size and its expected backing
	// (empty when standalone).
	Info(ctx context.Context, img ImageRef) (QemuInfo, error)
	// Check runs a qcow2 consistency check.
	Check(ctx context.Context, img ImageRef) error
}

// QemuInfo is what the node needs to know about one qcow2 file.
type QemuInfo struct {
	Format      string
	VirtualSize uint64
	BackingFile string
}

// QemuImg runs the host qemu-img directly, for the explicit dev/test
// (unjailed) node only, with the same fixed argv and closed graph as the
// jailed tools. It refuses to run as root: guest-influenced qemu-img never
// runs with root privileges.
type QemuImg struct{}

// commandTimeout bounds every qemu-img invocation; these are local metadata
// operations and must never hang a boot.
const commandTimeout = 2 * time.Minute

func (QemuImg) imager() *JailedImager { return NewJailedImager(localToolRunner{}) }

// CreateOverlay implements [Imager].
func (q QemuImg) CreateOverlay(ctx context.Context, dst ImageRef, virtualSize uint64) error {
	return q.imager().CreateOverlay(ctx, dst, virtualSize)
}

// Grow implements [Imager].
func (q QemuImg) Grow(ctx context.Context, img ImageRef, virtualSize uint64) error {
	return q.imager().Grow(ctx, img, virtualSize)
}

// Info implements [Imager].
func (q QemuImg) Info(ctx context.Context, img ImageRef) (QemuInfo, error) {
	return q.imager().Info(ctx, img)
}

// Check implements [Imager].
func (q QemuImg) Check(ctx context.Context, img ImageRef) error {
	return q.imager().Check(ctx, img)
}

// localToolRunner runs qemu-img as the current, non-root user.
type localToolRunner struct{}

func (localToolRunner) TargetHeld(uint64, uint64) bool { return false }

func (localToolRunner) RunTool(ctx context.Context, req ToolRequest) (ToolOutcome, error) {
	if os.Geteuid() == 0 {
		return ToolOutcome{}, errors.New("storage: refusing to run qemu-img as root without the jailer")
	}
	ctx, cancel := context.WithTimeout(ctx, commandTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, "qemu-img", toolArgv(req)...)
	cmd.ExtraFiles = toolFiles(req)
	cmd.Stdout, cmd.Stderr = req.Stdout, req.Stderr
	err := cmd.Run()
	if exitErr, ok := errors.AsType[*exec.ExitError](err); ok && exitErr.Exited() {
		return ToolOutcome{ExitCode: exitErr.ExitCode()}, nil
	}
	return ToolOutcome{}, err
}

// toolArgv is the fixed argv of req: the closed graph names the target at
// fd 3 and, for an overlay's check or resize, its pinned base at fd 4; info
// opens no backing and names the target alone.
func toolArgv(req ToolRequest) []string {
	graph := "driver=" + req.Driver + ",file.driver=file,file.filename=/proc/self/fd/3"
	if toolOpensBase(req) {
		graph += ",backing.driver=qcow2,backing.file.driver=file,backing.file.filename=/proc/self/fd/4,backing.read-only=on"
	}
	size := strconv.FormatUint(req.Size, 10)
	switch req.Op {
	case "info", "check":
		return []string{req.Op, "--output=json", "--image-opts", graph}
	case "resize":
		return []string{"resize", "--image-opts", graph, size}
	default:
		return []string{"create", "-f", "qcow2", "-u", "-F", "qcow2", "-b", req.Target.BackingName, "/proc/self/fd/3", size}
	}
}

// errSuffix trims qemu-img diagnostics. qemu-img prints paths and metadata
// errors, never guest data or credentials.
func errSuffix(stderr string) string {
	s := strings.TrimSpace(stderr)
	if s == "" {
		return ""
	}
	const max = 512
	if len(s) > max {
		s = s[len(s)-max:]
	}
	return ": " + s
}

// toolOpensBase reports whether req's graph names its pinned base at fd 4:
// check and resize of an overlay open a backing; info never does.
func toolOpensBase(req ToolRequest) bool {
	return req.Target.Base != nil && req.Op != "info"
}

// toolFiles is req's descriptors from fd 3: the target and, when the graph
// names it, the pinned base at fd 4.
func toolFiles(req ToolRequest) []*os.File {
	files := []*os.File{req.Target.File}
	if toolOpensBase(req) {
		files = append(files, req.Target.Base)
	}
	return files
}
