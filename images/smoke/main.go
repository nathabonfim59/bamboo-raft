// Command smoke is the host-side acceptance client for images/smoke.sh.
//
// It dials the guest agent over AF_VSOCK (the CID QEMU assigned) and checks
// the A1.2 contract: Ping, Exec echo/uid/HOME/cwd/env, PutFile + read back +
// ownership, and the in-guest toolchain/root-growth/rootless-Podman script.
// It never prints the command environment or any credential.
package main

import (
	"bytes"
	"cmp"
	"context"
	"crypto/sha256"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/mdlayher/vsock"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/nathabonfim59/bamboo-raft/internal/node/qemu"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/guestv1"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

const maxOutput = 1 << 20 // 1 MiB is plenty for smoke commands.

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "smoke: FAIL: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	cid := flag.Uint("cid", 42, "guest vsock CID assigned to QEMU")
	port := flag.Uint("port", 10789, "guest vsock port")
	timeout := flag.Duration("timeout", 240*time.Second, "whole-run timeout")
	bootStart := flag.Int64("boot-start-unix-ms", 0, "QEMU start time in Unix milliseconds, for readiness timing")
	expectNested := flag.String("expect-nested", "", "expected guest nested feature: vmx, svm or none")
	bootLabel := flag.String("boot-label", "", "smoke boot label (1 or 2) for cross-boot checks; empty disables them")
	poweroff := flag.Bool("poweroff", false, "sync and power the guest off after the checks (clean two-boot runs)")
	expectRootGiB := flag.Uint("expect-root-gib", 0, "expected root block-device size in GiB (0 disables the lower bound)")
	resolveOnly := flag.Bool("resolve-cpu", false, "resolve the shared CPU policy, print it and exit (no QEMU, no vsock)")
	probeOnly := flag.Bool("probe-cpu", false, "launch a CPU-only paused QEMU, prove it via QMP and exit (no vsock)")
	cpuModel := flag.String("cpu-model", "", "fixed CPU model (default the node's qemu.DefaultCPUModel)")
	cpuModelTestHost := flag.Bool("cpu-model-test-host", false, "internal test-only opt-in: allow -cpu-model host via qemu.ResolveTestCPU (smoke/probe only; never set in production)")
	cpuNested := flag.String("cpu-nested", "auto", "nested-virtualization mode: auto, on or off")
	cpuinfo := flag.String("cpuinfo", "", "host cpuinfo file to resolve against (default /proc/cpuinfo; for tests)")
	qemuBinary := flag.String("qemu", cmp.Or(os.Getenv("QEMU_SYSTEM_X86_64"), qemu.DefaultBinary), "qemu-system-x86_64 binary for -probe-cpu")
	kvmDevice := flag.String("kvm-device", qemu.KVMDevice, "KVM device path the probe must open read-write")
	probeDir := flag.String("probe-dir", "", "probe work directory for the QMP socket (default a fresh temp dir)")
	probeTimeout := flag.Duration("probe-timeout", 20*time.Second, "bounds the probe's QMP wait and queries")
	probeGrace := flag.Duration("probe-grace", 5*time.Second, "grace between QMP quit and SIGKILL in the probe")
	flag.Parse()
	if *cpuModelTestHost && *cpuModel != "host" {
		return errors.New("-cpu-model-test-host is an internal test opt-in and requires -cpu-model host")
	}
	if *resolveOnly {
		return resolveCPUMode(*cpuModel, *cpuNested, *cpuinfo, *cpuModelTestHost)
	}
	if *probeOnly {
		return probeCPUMode(probeConfig{
			binary:           *qemuBinary,
			cpuinfo:          *cpuinfo,
			cpuModel:         *cpuModel,
			cpuNested:        *cpuNested,
			cpuModelTestHost: *cpuModelTestHost,
			kvmDevice:        *kvmDevice,
			dir:              *probeDir,
			timeout:          *probeTimeout,
			grace:            *probeGrace,
		})
	}
	if err := validateVsock(*cid, *port); err != nil {
		return err
	}
	switch *expectNested {
	case "", "none", "vmx", "svm":
	default:
		return fmt.Errorf("invalid -expect-nested %q: use vmx, svm or none", *expectNested)
	}

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	conn, err := grpc.NewClient(
		"passthrough:///bbraft-guest",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return vsock.DialContext(ctx, uint32(*cid), uint32(*port), nil)
		}),
	)
	if err != nil {
		return fmt.Errorf("create guest client: %w", err)
	}
	defer conn.Close()
	client := guestv1.NewGuestClient(conn)

	// WaitForReady lets the RPC survive the gap between QEMU start and the
	// guest unit listening; the caller's timeout bounds it.
	ping, err := client.Ping(ctx, &guestv1.PingRequest{}, grpc.WaitForReady(true))
	if err != nil {
		return fmt.Errorf("ping guest (cid %d port %d): %w", *cid, *port, err)
	}
	if ping.GetVersion() == "" {
		return fmt.Errorf("ping returned an empty version")
	}
	if *bootStart > 0 {
		fmt.Printf("PASS ping version=%s boot_ready_ms=%d\n", ping.GetVersion(), time.Now().UnixMilli()-*bootStart)
	} else {
		fmt.Printf("PASS ping version=%s\n", ping.GetVersion())
	}

	if err := checkEcho(ctx, client); err != nil {
		return err
	}
	if err := checkIdentity(ctx, client); err != nil {
		return err
	}
	if err := checkCwdAndEnv(ctx, client); err != nil {
		return err
	}
	if err := checkPutFile(ctx, client); err != nil {
		return err
	}
	if err := checkTimeout(ctx, client); err != nil {
		return err
	}
	if err := checkCancel(ctx, client); err != nil {
		return err
	}
	if err := checkToolchain(ctx, client, *expectNested, *bootLabel, *expectRootGiB); err != nil {
		return err
	}
	if *poweroff {
		if err := powerOffGuest(ctx, client); err != nil {
			return err
		}
	}
	fmt.Println("smoke: all checks passed")
	return nil
}

// powerOffGuest flushes the guest filesystem and requests a clean poweroff.
// Both commands must complete with exit status 0; only a transport-level
// error during the shutdown request itself is tolerated, because the VM may go
// away before the final stream frame arrives. A completed nonzero response is
// always a failure, and the caller still waits for QEMU to exit with status 0
// before the next boot.
func powerOffGuest(ctx context.Context, client execClient) error {
	res, err := execGuest(ctx, client, &guestv1.ExecRequest{
		Command:        "sync",
		TimeoutSeconds: 30,
	})
	if err := mustExitZero("sync before poweroff", res, err); err != nil {
		return err
	}
	res, err = execGuest(ctx, client, &guestv1.ExecRequest{
		Command:        "sudo -n systemctl poweroff --no-block",
		TimeoutSeconds: 15,
	})
	if err != nil {
		if !shutdownTransportError(err) {
			return fmt.Errorf("poweroff request: %w", err)
		}
		fmt.Printf("smoke: poweroff request lost the stream during shutdown (expected): %v\n", err)
	} else if err := mustExitZero("poweroff request", res, err); err != nil {
		return err
	}
	fmt.Println("PASS guest synced and requested poweroff")
	return nil
}

// shutdownTransportError reports whether err is a gRPC transport failure that
// can legitimately replace the final frame while the guest powers off. Plain
// (non-status) errors are protocol violations and stay fatal.
func shutdownTransportError(err error) bool {
	st, ok := status.FromError(err)
	if !ok {
		return false
	}
	switch st.Code() {
	case codes.Canceled, codes.Unavailable, codes.Unknown, codes.Internal, codes.DeadlineExceeded:
		return true
	}
	return false
}

func validateVsock(cid, port uint) error {
	// Linux reserves CID 0 (hypervisor), 1 (local) and 2 (host); 0xffffffff
	// is VMADDR_CID_ANY, which QEMU rejects as a guest CID.
	if cid < 3 || cid == 0xffffffff {
		return fmt.Errorf("invalid vsock cid %d: use 3..4294967294 (0xffffffff is reserved)", cid)
	}
	if port == 0 || port > 0xffffffff {
		return fmt.Errorf("invalid vsock port %d: use 1..4294967295", port)
	}
	return nil
}

type execResult struct {
	stdout string
	stderr string
	exit   *nodev1.ExecEvent_Exit
}

func (r *execResult) exitOK() bool {
	return r.exit != nil &&
		r.exit.ExitCode != nil &&
		*r.exit.ExitCode == 0 &&
		r.exit.GetSignal() == "" &&
		!r.exit.GetTimedOut() &&
		!r.exit.GetOomKilled()
}

// execClient is the single method execGuest needs. guestv1.GuestClient
// satisfies it; tests use a scripted stream.
type execClient interface {
	Exec(ctx context.Context, in *guestv1.ExecRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[nodev1.ExecEvent], error)
}

// execGuest enforces the nodev1 Exec frame contract strictly: exactly one
// started(pid>0), stdout/stderr only between started and exit, exactly one
// final exit, and no unknown or nil frames. A protocol violation cancels the
// per-command context so the guest kills the process group.
func execGuest(ctx context.Context, client execClient, req *guestv1.ExecRequest) (*execResult, error) {
	execCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := client.Exec(execCtx, req)
	if err != nil {
		return nil, fmt.Errorf("exec %q: %w", req.GetCommand(), err)
	}
	var stdout, stderr bytes.Buffer
	var result execResult
	started := false
	exited := false
	for {
		event, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("exec %q stream: %w", req.GetCommand(), err)
		}
		if event == nil {
			return nil, fmt.Errorf("exec %q: nil stream frame", req.GetCommand())
		}
		if startedEvent := event.GetStarted(); startedEvent != nil {
			if exited {
				return nil, fmt.Errorf("exec %q: started frame after exit", req.GetCommand())
			}
			if started {
				return nil, fmt.Errorf("exec %q: duplicate started frame", req.GetCommand())
			}
			if startedEvent.GetPid() <= 0 {
				return nil, fmt.Errorf("exec %q: started frame has pid %d", req.GetCommand(), startedEvent.GetPid())
			}
			started = true
			continue
		}
		if exited {
			return nil, fmt.Errorf("exec %q: frame after exit", req.GetCommand())
		}
		switch {
		case event.GetStdout() != nil:
			if !started {
				return nil, fmt.Errorf("exec %q: stdout before started", req.GetCommand())
			}
			if stdout.Len()+len(event.GetStdout()) > maxOutput {
				return nil, fmt.Errorf("exec %q stdout exceeds %d bytes", req.GetCommand(), maxOutput)
			}
			stdout.Write(event.GetStdout())
		case event.GetStderr() != nil:
			if !started {
				return nil, fmt.Errorf("exec %q: stderr before started", req.GetCommand())
			}
			if stderr.Len()+len(event.GetStderr()) > maxOutput {
				return nil, fmt.Errorf("exec %q stderr exceeds %d bytes", req.GetCommand(), maxOutput)
			}
			stderr.Write(event.GetStderr())
		case event.GetExit() != nil:
			if !started {
				return nil, fmt.Errorf("exec %q: exit before started", req.GetCommand())
			}
			result.exit = event.GetExit()
			exited = true
		default:
			return nil, fmt.Errorf("exec %q: unknown stream frame %T", req.GetCommand(), event.Event)
		}
	}
	if !started {
		return nil, fmt.Errorf("exec %q stream ended without a started frame", req.GetCommand())
	}
	if !exited {
		return nil, fmt.Errorf("exec %q stream ended without an exit event", req.GetCommand())
	}
	result.stdout = stdout.String()
	result.stderr = stderr.String()
	return &result, nil
}

func mustExitZero(name string, res *execResult, err error) error {
	if err != nil {
		return err
	}
	if !res.exitOK() {
		code := "<nil>"
		if res.exit != nil && res.exit.ExitCode != nil {
			code = fmt.Sprint(*res.exit.ExitCode)
		}
		return fmt.Errorf("%s did not exit successfully: code=%s signal=%q timed_out=%v oom_killed=%v\nstdout:\n%s\nstderr:\n%s",
			name, code, res.exit.GetSignal(), res.exit.GetTimedOut(), res.exit.GetOomKilled(),
			res.stdout, res.stderr)
	}
	return nil
}

func checkEcho(ctx context.Context, client guestv1.GuestClient) error {
	res, err := execGuest(ctx, client, &guestv1.ExecRequest{
		Command:        `printf 'smoke-echo\n'`,
		TimeoutSeconds: 30,
	})
	if err := mustExitZero("echo", res, err); err != nil {
		return err
	}
	if res.stdout != "smoke-echo\n" {
		return fmt.Errorf("echo stdout is %q", res.stdout)
	}
	fmt.Println("PASS exec echo")
	return nil
}

func checkIdentity(ctx context.Context, client guestv1.GuestClient) error {
	res, err := execGuest(ctx, client, &guestv1.ExecRequest{
		Command:        `printf 'uid=%s gid=%s home=%s\n' "$(id -u)" "$(id -g)" "$HOME"`,
		TimeoutSeconds: 30,
	})
	if err := mustExitZero("identity", res, err); err != nil {
		return err
	}
	got := strings.TrimSpace(res.stdout)
	if got != "uid=1000 gid=1000 home=/home/user" {
		return fmt.Errorf("identity is %q, want uid=1000 gid=1000 home=/home/user", got)
	}
	fmt.Println("PASS exec uid/HOME")
	return nil
}

func checkCwdAndEnv(ctx context.Context, client guestv1.GuestClient) error {
	res, err := execGuest(ctx, client, &guestv1.ExecRequest{
		Command:        `printf 'cwd=%s env=%s\n' "$PWD" "$BBRAFT_SMOKE"`,
		Cwd:            "/tmp",
		Env:            map[string]string{"BBRAFT_SMOKE": "ok"},
		TimeoutSeconds: 30,
	})
	if err := mustExitZero("cwd/env", res, err); err != nil {
		return err
	}
	if strings.TrimSpace(res.stdout) != "cwd=/tmp env=ok" {
		return fmt.Errorf("cwd/env output is %q", res.stdout)
	}
	fmt.Println("PASS exec cwd/override env")
	return nil
}

const childReadyToken = "bbraft-smoke-child-ready"

// childSetupCommand is the exact shell setup used by both timeout and cancel.
// It spawns a background child that writes its completion marker after four
// seconds, records the child PID, writes the non-empty ready token to a file,
// and (when emitReadyLine) prints the same token on stdout before sleeping.
func childSetupCommand(base string, emitReadyLine bool) string {
	command := fmt.Sprintf(
		`rm -f %[1]s.done %[1]s.pid %[1]s.ready; (sleep 4; : > %[1]s.done) & echo $! > %[1]s.pid; printf '%%s\n' %[2]s > %[1]s.ready;`,
		base, childReadyToken)
	if emitReadyLine {
		return command + fmt.Sprintf(" printf '%%s\\n' %s; sleep 30", childReadyToken)
	}
	return command + " sleep 30"
}

// containsExactLine reports whether text contains line as a complete line.
// Prefix chatter ending in the token is not accepted; a trailing partial
// line also waits for its newline.
func containsExactLine(text, line string) bool {
	start := 0
	for i := 0; i < len(text); i++ {
		if text[i] != '\n' {
			continue
		}
		if text[start:i] == line {
			return true
		}
		start = i + 1
	}
	// A trailing partial line (without its newline) is not yet a match.
	return false
}

func checkTimeout(ctx context.Context, client guestv1.GuestClient) error {
	base := "/tmp/bbraft-smoke-timeout"
	res, err := execGuest(ctx, client, &guestv1.ExecRequest{
		Command:        childSetupCommand(base, false),
		TimeoutSeconds: 3,
	})
	if err != nil {
		return err
	}
	if !res.exit.GetTimedOut() {
		return fmt.Errorf("timeout exec did not report timed_out (exit=%+v)", res.exit)
	}
	if err := checkGroupGone(ctx, client, base, "timeout"); err != nil {
		return err
	}
	fmt.Println("PASS exec timeout kills whole process group")
	return nil
}

func checkCancel(ctx context.Context, client guestv1.GuestClient) error {
	base := "/tmp/bbraft-smoke-cancel"
	cancelCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	stream, err := client.Exec(cancelCtx, &guestv1.ExecRequest{
		Command:        childSetupCommand(base, true),
		TimeoutSeconds: 0,
	})
	if err != nil {
		return fmt.Errorf("cancel exec: %w", err)
	}
	var stdout strings.Builder
	sawReady := false
	for !sawReady {
		event, recvErr := stream.Recv()
		if errors.Is(recvErr, io.EOF) {
			return fmt.Errorf("cancel exec ended before the child-ready marker")
		}
		if recvErr != nil {
			return fmt.Errorf("cancel exec before cancel: %w", recvErr)
		}
		if event == nil {
			return fmt.Errorf("cancel exec: nil frame")
		}
		if chunk := event.GetStdout(); len(chunk) > 0 {
			if stdout.Len()+len(chunk) > maxOutput {
				return fmt.Errorf("cancel exec stdout exceeds %d bytes", maxOutput)
			}
			stdout.Write(chunk)
			// Only an exact marker line counts; shell-profile chatter,
			// prefixes or a partial line ending in the token do not.
			if containsExactLine(stdout.String(), childReadyToken) {
				sawReady = true
			}
		}
	}
	cancel()
	var recvErr error
	for {
		if _, recvErr = stream.Recv(); recvErr != nil {
			break
		}
	}
	if code := status.Code(recvErr); code != codes.Canceled {
		return fmt.Errorf("cancel exec stream ended with %v (code %s), want Canceled", recvErr, code)
	}
	if err := checkGroupGone(ctx, client, base, "cancel"); err != nil {
		return err
	}
	fmt.Println("PASS exec RPC cancel kills whole process group")
	return nil
}

// groupCheckCommand validates the setup artifacts before probing liveness.
// waitSeconds is a parameter so tests can run it without the six-second wait.
func groupCheckCommand(base string, waitSeconds int) string {
	return fmt.Sprintf(
		`if [ ! -s %[1]s.ready ]; then echo missing-ready; exit 0; fi
if [ ! -s %[1]s.pid ]; then echo missing-pid; exit 0; fi
pid=$(cat %[1]s.pid)
case "$pid" in "" | 0* | *[!0-9]*) echo bad-pid; exit 0 ;; esac
sleep %[2]d
if [ -e %[1]s.done ]; then echo done
elif kill -0 "$pid" 2>/dev/null; then echo alive
else echo gone
fi`, base, waitSeconds)
}

// checkGroupGone requires the child-ready marker and a canonical numeric PID
// from the setup step, waits long enough for a surviving child to write its
// completion marker, then reports dead vs. alive. A missing or malformed
// setup is a failure, never "gone".
func checkGroupGone(ctx context.Context, client execClient, base, label string) error {
	res, err := execGuest(ctx, client, &guestv1.ExecRequest{
		Command:        groupCheckCommand(base, 6),
		TimeoutSeconds: 20,
	})
	if err := mustExitZero(label+" group check", res, err); err != nil {
		return err
	}
	switch strings.TrimSpace(res.stdout) {
	case "gone":
		return nil
	case "done":
		return fmt.Errorf("%s: background child survived long enough to write its completion marker", label)
	case "alive":
		return fmt.Errorf("%s: background child is still alive", label)
	case "missing-ready":
		return fmt.Errorf("%s: child-ready marker is missing; setup did not complete", label)
	case "missing-pid":
		return fmt.Errorf("%s: PID file is missing; setup did not complete", label)
	case "bad-pid":
		return fmt.Errorf("%s: PID file does not contain a canonical positive PID", label)
	default:
		return fmt.Errorf("%s: unexpected group check output %q", label, res.stdout)
	}
}

func checkPutFile(ctx context.Context, client guestv1.GuestClient) error {
	path := "/home/user/bbraft-smoke/upload.bin"
	payload := bytes.Repeat([]byte("bamboo-raft-smoke\n"), 2048) // 32 KiB
	sum := sha256.Sum256(payload)

	stream, err := client.PutFile(ctx)
	if err != nil {
		return fmt.Errorf("putfile: %w", err)
	}
	if err := stream.Send(&guestv1.PutFileRequest{
		Part: &guestv1.PutFileRequest_Header_{
			Header: &guestv1.PutFileRequest_Header{Path: path, Mode: 0o640},
		},
	}); err != nil {
		return fmt.Errorf("putfile header: %w", err)
	}
	// Two data messages exercise the chunked stream.
	for _, chunk := range [][]byte{payload[:len(payload)/2], payload[len(payload)/2:]} {
		if err := stream.Send(&guestv1.PutFileRequest{
			Part: &guestv1.PutFileRequest_Data{Data: chunk},
		}); err != nil {
			return fmt.Errorf("putfile data: %w", err)
		}
	}
	resp, err := stream.CloseAndRecv()
	if err != nil {
		return fmt.Errorf("putfile close: %w", err)
	}
	if resp.GetPath() != path || resp.GetSize() != int64(len(payload)) {
		return fmt.Errorf("putfile response path=%q size=%d, want %q %d", resp.GetPath(), resp.GetSize(), path, len(payload))
	}
	fmt.Printf("PASS putfile %s (%d bytes)\n", path, resp.GetSize())

	check, err := execGuest(ctx, client, &guestv1.ExecRequest{
		Command:        fmt.Sprintf(`stat -c '%%u:%%g %%a' %s && sha256sum %s`, path, path),
		TimeoutSeconds: 30,
	})
	if err := mustExitZero("putfile read", check, err); err != nil {
		return err
	}
	lines := strings.Split(strings.TrimSpace(check.stdout), "\n")
	if len(lines) != 2 {
		return fmt.Errorf("putfile read output is %q", check.stdout)
	}
	if strings.TrimSpace(lines[0]) != "1000:1000 640" {
		return fmt.Errorf("putfile ownership/mode is %q, want 1000:1000 640", lines[0])
	}
	if want := fmt.Sprintf("%x  %s", sum, path); strings.TrimSpace(lines[1]) != want {
		return fmt.Errorf("putfile sha256 line is %q, want %q", lines[1], want)
	}
	fmt.Println("PASS putfile ownership/content")
	return nil
}

func checkToolchain(ctx context.Context, client guestv1.GuestClient, expectNested, bootLabel string, expectRootGiB uint) error {
	env := map[string]string{"BBRAFT_EXPECT_NESTED": expectNested}
	if expectNested == "" {
		env["BBRAFT_EXPECT_NESTED"] = "none"
	}
	if bootLabel != "" {
		env["BBRAFT_SMOKE_BOOT"] = bootLabel
	}
	if expectRootGiB > 0 {
		env["BBRAFT_EXPECT_ROOT_GIB"] = strconv.FormatUint(uint64(expectRootGiB), 10)
	}
	res, err := execGuest(ctx, client, &guestv1.ExecRequest{
		Command:        `/usr/local/bin/bbraft-toolchain-check`,
		Env:            env,
		TimeoutSeconds: 180,
	})
	if err := mustExitZero("toolchain", res, err); err != nil {
		return err
	}
	if !strings.Contains(res.stdout, "all checks passed") {
		return fmt.Errorf("toolchain check did not report success:\n%s", res.stdout)
	}
	fmt.Println(res.stdout)
	return nil
}
