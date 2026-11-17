package server_test

// The end-to-end node wrapper. It delegates every ordinary request to
// nodefake, but it must not run a CLI-generated guest-absolute push script on
// the host: nodefake is not a chroot, so `/tmp/bbraft-push-*.tar` inside a
// command string would name the host's /tmp. Only the exact internal push
// extraction and cleanup scripts are recognised, validated and emulated with a
// confined os.Root. Every emulated entry is recorded so the test can inspect
// the real uploaded tar and the extracted result.

import (
	"archive/tar"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/nathabonfim59/bamboo-raft/internal/nodefake"
	"github.com/nathabonfim59/bamboo-raft/internal/nodelink"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

// e2eNodeCounts is a snapshot of the node RPC counters.
type e2eNodeCounts struct {
	CreateVM       int
	DeleteVM       int
	Exec           int
	Extract        int
	Cleanup        int
	Delegated      int
	Rejected       int
	PutFile        int
	SaveSnapshot   int
	DeleteSnapshot int
	Info           int
}

// e2eCreateVMRecord is one CreateVM request reduced to its safe fields.
type e2eCreateVMRecord struct {
	sandboxID    string
	fromSnapshot string
	vcpus        uint32
	memoryMiB    uint64
	diskGiB      uint64
}

// e2eExecRecord is one Exec request. command is the exact fixture command and
// contains no secret.
type e2eExecRecord struct {
	sandboxID string
	command   string
	cwd       string
	timeout   uint32
	emulated  bool
}

// e2ePutFileRecord is one uploaded file header.
type e2ePutFileRecord struct {
	sandboxID string
	path      string
	size      int64
}

// e2eTarEntry is one entry of the actual uploaded tar.
type e2eTarEntry struct {
	name     string
	typeflag byte
	mode     int64
	size     int64
	linkname string
}

// e2eExtractRecord is one emulated internal push extraction.
type e2eExtractRecord struct {
	archivePath string
	destination string
	timeout     uint32
	entries     []e2eTarEntry
}

// e2eNode wraps nodefake with request counters and the confined push
// emulation.
type e2eNode struct {
	*nodefake.Node
	root string

	mu            sync.Mutex
	counters      e2eNodeCounts
	createRecords []e2eCreateVMRecord
	execs         []e2eExecRecord
	putFiles      []e2ePutFileRecord
	extracts      []e2eExtractRecord
	violationList []string
}

// newE2ENode wraps a nodefake rooted at root.
func newE2ENode(nodeID, root string) *e2eNode {
	return &e2eNode{Node: nodefake.New(nodeID, root), root: root}
}

// Register registers the wrapper, not the embedded fake, so the overrides are
// the methods the gateway calls.
func (n *e2eNode) Register(s *grpc.Server) { nodev1.RegisterNodeServer(s, n) }

// CreateVM records the request before delegating.
func (n *e2eNode) CreateVM(ctx context.Context, req *nodev1.CreateVMRequest) (*nodev1.VM, error) {
	n.mu.Lock()
	n.counters.CreateVM++
	n.createRecords = append(n.createRecords, e2eCreateVMRecord{
		sandboxID:    req.GetSandboxId(),
		fromSnapshot: req.GetFromSnapshot(),
		vcpus:        req.GetResources().GetVcpus(),
		memoryMiB:    req.GetResources().GetMemoryMib(),
		diskGiB:      req.GetResources().GetDiskGib(),
	})
	n.mu.Unlock()
	return n.Node.CreateVM(ctx, req)
}

// DeleteVM counts the request before delegating.
func (n *e2eNode) DeleteVM(ctx context.Context, req *nodev1.DeleteVMRequest) (*nodev1.DeleteVMResponse, error) {
	n.mu.Lock()
	n.counters.DeleteVM++
	n.mu.Unlock()
	return n.Node.DeleteVM(ctx, req)
}

// Info counts inventory polls before delegating.
func (n *e2eNode) Info(ctx context.Context, req *nodev1.InfoRequest) (*nodev1.InfoResponse, error) {
	n.mu.Lock()
	n.counters.Info++
	n.mu.Unlock()
	return n.Node.Info(ctx, req)
}

// SaveSnapshot counts the request before delegating.
func (n *e2eNode) SaveSnapshot(ctx context.Context, req *nodev1.SaveSnapshotRequest) (*nodev1.Snapshot, error) {
	n.mu.Lock()
	n.counters.SaveSnapshot++
	n.mu.Unlock()
	return n.Node.SaveSnapshot(ctx, req)
}

// DeleteSnapshot counts the request before delegating.
func (n *e2eNode) DeleteSnapshot(ctx context.Context, req *nodev1.DeleteSnapshotRequest) (*nodev1.DeleteSnapshotResponse, error) {
	n.mu.Lock()
	n.counters.DeleteSnapshot++
	n.mu.Unlock()
	return n.Node.DeleteSnapshot(ctx, req)
}

// PutFile wraps the client stream so the header path and byte count are
// recorded, then delegates the actual write to nodefake.
func (n *e2eNode) PutFile(stream grpc.ClientStreamingServer[nodev1.PutFileRequest, nodev1.PutFileResponse]) error {
	n.mu.Lock()
	n.counters.PutFile++
	n.mu.Unlock()
	return n.Node.PutFile(&e2ePutFileStream{ClientStreamingServer: stream, node: n})
}

// e2ePutFileStream records one upload while nodefake consumes it.
type e2ePutFileStream struct {
	grpc.ClientStreamingServer[nodev1.PutFileRequest, nodev1.PutFileResponse]
	node   *e2eNode
	header *nodev1.PutFileRequest_Header
	bytes  int64
}

// Recv records the header and data size.
func (s *e2ePutFileStream) Recv() (*nodev1.PutFileRequest, error) {
	msg, err := s.ClientStreamingServer.Recv()
	if msg != nil {
		if hdr := msg.GetHeader(); hdr != nil {
			s.header = hdr
		}
		s.bytes += int64(len(msg.GetData()))
	}
	return msg, err
}

// SendAndClose records the completed upload, then closes the stream.
func (s *e2ePutFileStream) SendAndClose(resp *nodev1.PutFileResponse) error {
	if s.header != nil {
		s.node.mu.Lock()
		s.node.putFiles = append(s.node.putFiles, e2ePutFileRecord{
			sandboxID: s.header.GetSandboxId(),
			path:      s.header.GetPath(),
			size:      s.bytes,
		})
		s.node.mu.Unlock()
	}
	return s.ClientStreamingServer.SendAndClose(resp)
}

// The exact normal fixture commands the test sends. This is a test boundary,
// not a shell parser: anything else is rejected before it can reach host bash.
const (
	e2eExecCode7Command   = "printf out; printf err >&2; exit 7"
	e2eExecTimeoutCommand = "sleep 5"
	e2eRestoreCatCommand  = "cat target/marker.txt"
	e2eRunNonzeroCommand  = `basename "$PWD"; cat marker.txt; exit 7`
	e2eRunHumanCommand    = `basename "$PWD"; cat ../marker.txt`
	e2eTestVMArgvCommand  = `'bash' '-lc' 'printf out; printf err >&2; exit 7'`
	e2eSignalCommand      = "echo $$ > started.pid\nsh -c 'echo $$ > child.pid; sleep 30' &\necho $! > child.pid\nwait"
	// e2eResumeProbeCommand is the exact literal sent by the CLI resume
	// readiness probe. It is deliberately narrow: no other new command gains
	// host execution through this fixture.
	e2eResumeProbeCommand = "true"
)

// e2eAllowedFixtureCommand is the explicit allowlist of ordinary commands that
// may run on nodefake. Everything else is refused.
func e2eAllowedFixtureCommand(command string) bool {
	switch command {
	case e2eExecCode7Command, e2eExecTimeoutCommand, e2eRestoreCatCommand,
		e2eRunNonzeroCommand, e2eRunHumanCommand, e2eTestVMArgvCommand, e2eSignalCommand,
		e2eResumeProbeCommand:
		return true
	default:
		return false
	}
}

// e2eLooksLikeInternalPush reports a command that is probably the generated
// internal push script but did not match its exact validated form. It must be
// refused, never delegated to host bash.
func e2eLooksLikeInternalPush(command string) bool {
	return strings.Contains(command, "/tmp/bbraft-push-") ||
		strings.HasPrefix(command, "tmp=") ||
		strings.Contains(command, "trap cleanup EXIT")
}

// Exec records every command. Only the exact internal push extraction and
// cleanup scripts are emulated, only the explicit fixture allowlist delegates
// to nodefake, and everything else is refused.
func (n *e2eNode) Exec(req *nodev1.ExecRequest, stream grpc.ServerStreamingServer[nodev1.ExecEvent]) error {
	archive, destination, isExtract := e2eParsePushExtract(req.GetCommand())
	cleanupArchive, isCleanup := e2eParsePushCleanup(req.GetCommand())

	n.mu.Lock()
	n.counters.Exec++
	n.execs = append(n.execs, e2eExecRecord{
		sandboxID: req.GetSandboxId(),
		command:   req.GetCommand(),
		cwd:       req.GetCwd(),
		timeout:   req.GetTimeoutSeconds(),
		emulated:  isExtract || isCleanup,
	})
	switch {
	case isExtract:
		n.counters.Extract++
	case isCleanup:
		n.counters.Cleanup++
	case e2eAllowedFixtureCommand(req.GetCommand()):
		n.counters.Delegated++
	default:
		n.counters.Rejected++
	}
	n.mu.Unlock()

	switch {
	case isExtract:
		return n.emulateExtract(req, archive, destination, stream)
	case isCleanup:
		return n.emulateCleanup(req, cleanupArchive, stream)
	case e2eLooksLikeInternalPush(req.GetCommand()):
		n.recordViolation("unrecognised internal push command")
		return status.Error(codes.InvalidArgument, "test node: unrecognised internal push command")
	case e2eAllowedFixtureCommand(req.GetCommand()):
		return n.Node.Exec(req, stream)
	default:
		n.recordViolation("unapproved fixture command")
		return status.Error(codes.InvalidArgument, "test node: command is not an approved fixture command")
	}
}

// e2eParsePushExtract recognises only the exact script built by
// cli.pushExtractCommand. A false result never delegates a push-shaped command
// to nodefake: Exec rejects it unless it is an allowlisted fixture command.
func e2eParsePushExtract(command string) (archive, destination string, ok bool) {
	lines := strings.Split(command, "\n")
	if len(lines) != 5 {
		return "", "", false
	}
	archive, ok = e2eParseSingleQuotedAssignment(lines[0], "tmp=")
	if !ok {
		return "", "", false
	}
	destination, ok = e2eParseSingleQuotedAssignment(lines[1], "dst=")
	if !ok {
		return "", "", false
	}
	if lines[2] != `cleanup() { rm -f -- "$tmp"; }` ||
		lines[3] != "trap cleanup EXIT" ||
		lines[4] != `mkdir -p -- "$dst" && tar -xf "$tmp" -C "$dst"` {
		return "", "", false
	}
	if !strings.HasPrefix(archive, "/tmp/bbraft-push-") || !strings.HasSuffix(archive, ".tar") {
		return "", "", false
	}
	if !strings.HasPrefix(destination, "/home/user/") || path.Clean(destination) != destination {
		return "", "", false
	}
	return archive, destination, true
}

// e2eParseSingleQuotedAssignment parses `prefix='value'` with the cli's
// execShellJoin quoting. The fixture paths contain no embedded single quote.
func e2eParseSingleQuotedAssignment(line, prefix string) (string, bool) {
	value, ok := strings.CutPrefix(line, prefix)
	if !ok || len(value) < 2 || value[0] != '\'' || value[len(value)-1] != '\'' {
		return "", false
	}
	inner := value[1 : len(value)-1]
	if strings.Contains(inner, "'") || inner == "" {
		return "", false
	}
	return inner, true
}

// e2eParsePushCleanup recognises the best-effort `rm -f --` cleanup script.
func e2eParsePushCleanup(command string) (archive string, ok bool) {
	const prefix = "rm -f -- '"
	if !strings.HasPrefix(command, prefix) || !strings.HasSuffix(command, "'") {
		return "", false
	}
	archive = strings.TrimSuffix(strings.TrimPrefix(command, prefix), "'")
	if strings.Contains(archive, "'") ||
		!strings.HasPrefix(archive, "/tmp/bbraft-push-") ||
		!strings.HasSuffix(archive, ".tar") {
		return "", false
	}
	return archive, true
}

// emulateExtract reads the actual uploaded tar from the fake's confined VM
// root, records its entries, extracts it under the requested guest destination
// with os.Root, removes the tar exactly as the EXIT trap would, and sends the
// started/exit frames nodefake would have sent.
func (n *e2eNode) emulateExtract(req *nodev1.ExecRequest, archive, destination string, stream grpc.ServerStreamingServer[nodev1.ExecEvent]) error {
	record := e2eExtractRecord{
		archivePath: archive,
		destination: destination,
		timeout:     req.GetTimeoutSeconds(),
	}
	code, err := n.extractArchive(req.GetSandboxId(), archive, destination, &record)
	n.mu.Lock()
	n.extracts = append(n.extracts, record)
	if err != nil {
		n.violationList = append(n.violationList, err.Error())
	}
	n.mu.Unlock()
	if sendErr := e2eSendStarted(stream); sendErr != nil {
		return sendErr
	}
	return e2eSendExit(stream, code)
}

// extractArchive is the confined tar extraction. It never follows a tar entry
// outside the VM root and never creates a symlink to an absolute target.
func (n *e2eNode) extractArchive(sandboxID, archive, destination string, record *e2eExtractRecord) (int32, error) {
	if sandboxID == "" {
		return 1, errors.New("push emulation: sandbox id is missing")
	}
	if record.timeout != 600 {
		return 1, fmt.Errorf("push emulation: extraction timeout is %d, want 600", record.timeout)
	}
	vmDir := filepath.Join(n.root, "vms", sandboxID)
	root, err := os.OpenRoot(vmDir)
	if err != nil {
		return 1, errors.New("push emulation: vm root is not readable")
	}
	defer root.Close()

	archiveRel := filepath.Join("tmp", path.Base(archive))
	file, err := root.Open(archiveRel)
	if err != nil {
		return 1, errors.New("push emulation: uploaded archive is missing")
	}
	defer file.Close()

	destRel := strings.TrimPrefix(destination, "/")
	if err := root.MkdirAll(destRel, 0o755); err != nil {
		return 1, errors.New("push emulation: destination is not creatable")
	}

	reader := tar.NewReader(file)
	for {
		header, err := reader.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return 1, errors.New("push emulation: uploaded tar is malformed")
		}
		name := path.Clean(header.Name)
		if name == "" || name == "." || path.IsAbs(name) || name == ".." || strings.HasPrefix(name, "../") {
			return 1, errors.New("push emulation: tar entry escapes the destination")
		}
		rel := filepath.FromSlash(name)
		record.entries = append(record.entries, e2eTarEntry{
			name:     name,
			typeflag: header.Typeflag,
			mode:     header.Mode,
			size:     header.Size,
			linkname: header.Linkname,
		})
		target := filepath.Join(destRel, rel)
		mode := fs.FileMode(header.Mode) & fs.ModePerm
		switch header.Typeflag {
		case tar.TypeDir:
			if mode == 0 {
				mode = 0o755
			}
			if err := root.MkdirAll(target, mode); err != nil {
				return 1, errors.New("push emulation: tar directory is not creatable")
			}
		case tar.TypeReg:
			if mode == 0 {
				mode = 0o644
			}
			if err := root.MkdirAll(filepath.Dir(target), 0o755); err != nil {
				return 1, errors.New("push emulation: tar parent is not creatable")
			}
			out, err := root.OpenFile(target, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, mode)
			if err != nil {
				return 1, errors.New("push emulation: tar file is not creatable")
			}
			_, copyErr := io.CopyN(out, reader, header.Size)
			chmodErr := out.Chmod(mode)
			closeErr := out.Close()
			if copyErr != nil || chmodErr != nil || closeErr != nil {
				return 1, errors.New("push emulation: tar file could not be written")
			}
		case tar.TypeSymlink:
			if path.IsAbs(header.Linkname) {
				return 1, errors.New("push emulation: absolute symlink target is refused")
			}
			if err := root.MkdirAll(filepath.Dir(target), 0o755); err != nil {
				return 1, errors.New("push emulation: symlink parent is not creatable")
			}
			if err := root.Symlink(header.Linkname, target); err != nil {
				return 1, errors.New("push emulation: symlink is not creatable")
			}
		default:
			return 1, fmt.Errorf("push emulation: unsupported tar entry type %d", header.Typeflag)
		}
	}
	if err := root.Remove(archiveRel); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return 1, errors.New("push emulation: archive cleanup failed")
	}
	return 0, nil
}

// emulateCleanup removes the confined archive and reports success, matching
// `rm -f`.
func (n *e2eNode) emulateCleanup(req *nodev1.ExecRequest, archive string, stream grpc.ServerStreamingServer[nodev1.ExecEvent]) error {
	code := int32(0)
	if req.GetSandboxId() == "" {
		code = 1
		n.recordViolation("push cleanup: sandbox id is missing")
	} else {
		vmDir := filepath.Join(n.root, "vms", req.GetSandboxId())
		root, err := os.OpenRoot(vmDir)
		if err != nil {
			code = 1
			n.recordViolation("push cleanup: vm root is not readable")
		} else {
			err = root.Remove(filepath.Join("tmp", path.Base(archive)))
			root.Close()
			if err != nil && !errors.Is(err, fs.ErrNotExist) {
				code = 1
				n.recordViolation("push cleanup: archive removal failed")
			}
		}
	}
	if err := e2eSendStarted(stream); err != nil {
		return err
	}
	return e2eSendExit(stream, code)
}

// recordViolation records one safe emulation failure.
func (n *e2eNode) recordViolation(reason string) {
	n.mu.Lock()
	n.violationList = append(n.violationList, reason)
	n.mu.Unlock()
}

// e2eSendStarted sends the started frame every exec stream starts with.
func e2eSendStarted(stream grpc.ServerStreamingServer[nodev1.ExecEvent]) error {
	return stream.Send(&nodev1.ExecEvent{Event: &nodev1.ExecEvent_Started_{
		Started: &nodev1.ExecEvent_Started{Pid: 1},
	}})
}

// e2eSendExit sends a terminal exit frame with the given status.
func e2eSendExit(stream grpc.ServerStreamingServer[nodev1.ExecEvent], code int32) error {
	return stream.Send(&nodev1.ExecEvent{Event: &nodev1.ExecEvent_Exit_{
		Exit: &nodev1.ExecEvent_Exit{ExitCode: new(code)},
	}})
}

// counts returns a copy of the RPC counters.
func (n *e2eNode) counts() e2eNodeCounts {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.counters
}

// createVMs returns a copy of the recorded CreateVM requests.
func (n *e2eNode) createVMs() []e2eCreateVMRecord {
	n.mu.Lock()
	defer n.mu.Unlock()
	return append([]e2eCreateVMRecord(nil), n.createRecords...)
}

// lastCreateVM returns the newest CreateVM record.
func (n *e2eNode) lastCreateVM() (e2eCreateVMRecord, bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if len(n.createRecords) == 0 {
		return e2eCreateVMRecord{}, false
	}
	return n.createRecords[len(n.createRecords)-1], true
}

// findExec returns the first Exec whose command contains want.
func (n *e2eNode) findExec(want string) (e2eExecRecord, bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for _, record := range n.execs {
		if strings.Contains(record.command, want) {
			return record, true
		}
	}
	return e2eExecRecord{}, false
}

// lastPutFile returns the newest uploaded file record.
func (n *e2eNode) lastPutFile() (e2ePutFileRecord, bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if len(n.putFiles) == 0 {
		return e2ePutFileRecord{}, false
	}
	return n.putFiles[len(n.putFiles)-1], true
}

// lastExtract returns the newest emulated extraction record.
func (n *e2eNode) lastExtract() (e2eExtractRecord, bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if len(n.extracts) == 0 {
		return e2eExtractRecord{}, false
	}
	return n.extracts[len(n.extracts)-1], true
}

// violations returns a copy of the safe emulation failures.
func (n *e2eNode) violations() []string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return append([]string(nil), n.violationList...)
}

// e2eStartNode runs the wrapper behind the real nodelink transport, dialing
// the actual daemon's nodehub listener.
func e2eStartNode(t *testing.T, f *daemonProcessFixture) *e2eNode {
	t.Helper()
	node := newE2ENode(f.nodeID, f.nodeRoot)
	server := grpc.NewServer(nodelink.ServerOptions()...)
	node.Register(server)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = nodelink.Serve(ctx, nodelink.DialConfig{
			Addr:       f.nodeAddr,
			NodeID:     f.nodeID,
			Token:      f.nodeToken,
			Log:        slog.New(slog.DiscardHandler),
			MinBackoff: 20 * time.Millisecond,
			MaxBackoff: 100 * time.Millisecond,
		}, server)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			server.Stop()
			<-done
		}
	})
	return node
}

// phaseExitExecStream is a minimal server stream for the node fixture test.
type phaseExitExecStream struct {
	grpc.ServerStream
	ctx    context.Context
	events []*nodev1.ExecEvent
}

// Context returns the request context.
func (s *phaseExitExecStream) Context() context.Context { return s.ctx }

// Send records one event.
func (s *phaseExitExecStream) Send(event *nodev1.ExecEvent) error {
	s.events = append(s.events, event)
	return nil
}

// TestPhaseExitNodeAllowlist pins the exact true command the resume readiness
// probe sends. It must delegate with exit 0, while an ordinary non-allowlisted
// command remains refused before reaching host bash.
func TestPhaseExitNodeAllowlist(t *testing.T) {
	node := newE2ENode("node-allowlist", t.TempDir())
	if _, err := node.CreateVM(t.Context(), &nodev1.CreateVMRequest{
		SandboxId: "bx_allowlist",
		Image:     nodefake.Image,
		Resources: &nodev1.Resources{Vcpus: 1, MemoryMib: 512, DiskGib: 1},
	}); err != nil {
		t.Fatalf("create fixture VM: %v", err)
	}

	stream := &phaseExitExecStream{ctx: t.Context()}
	if err := node.Exec(&nodev1.ExecRequest{
		SandboxId:      "bx_allowlist",
		Command:        "true",
		TimeoutSeconds: 5,
	}, stream); err != nil {
		t.Fatalf("exact true command was refused: %v", err)
	}
	if len(stream.events) < 2 || stream.events[0].GetStarted() == nil {
		t.Fatal("true command did not emit a started event")
	}
	exit := stream.events[len(stream.events)-1].GetExit()
	if exit == nil || exit.ExitCode == nil || *exit.ExitCode != 0 || exit.TimedOut {
		t.Fatal("true command did not exit successfully")
	}
	if got := node.counts(); got.Delegated != 1 || got.Rejected != 0 {
		t.Fatalf("true command counters = delegated %d rejected %d", got.Delegated, got.Rejected)
	}

	refused := &phaseExitExecStream{ctx: t.Context()}
	if err := node.Exec(&nodev1.ExecRequest{
		SandboxId:      "bx_allowlist",
		Command:        "printf not-allowed",
		TimeoutSeconds: 5,
	}, refused); err == nil {
		t.Fatal("non-allowlisted command was not refused")
	}
	if len(refused.events) != 0 {
		t.Fatal("refused command emitted events")
	}
	if got := node.counts(); got.Delegated != 1 || got.Rejected != 1 {
		t.Fatalf("refused command counters = delegated %d rejected %d", got.Delegated, got.Rejected)
	}
}
