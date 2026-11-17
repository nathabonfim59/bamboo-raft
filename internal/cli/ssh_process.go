package cli

import (
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/nathabonfim59/bamboo-raft/internal/ids"
)

// sshDefaultPort is the pinned SSH port when no endpoint forwards one.
const sshDefaultPort = 22

// ProcessCommand is one external program invocation. Args excludes Path. Stdin,
// Stdout and Stderr are passed straight through, never through a shell.
type ProcessCommand struct {
	Path   string
	Args   []string
	Stdin  io.Reader
	Stdout io.Writer
	Stderr io.Writer
	// Foreground gives the child's own process group the controlling terminal.
	// Every child always gets its own process group for cancellation; this flag
	// only decides whether that group is handed the tty.
	Foreground bool
}

// ProcessRunner runs one external program and returns its exit status. A
// context cancellation returns the context error; a start failure returns a
// non-nil error. Implementations kill the whole child process group.
type ProcessRunner func(ctx context.Context, command ProcessCommand) (int, error)

// sshAccess is everything one ssh or scp invocation needs after the API and
// key setup succeeded.
type sshAccess struct {
	sandboxID      string
	target         sshTarget
	privateKeyPath string
	knownHostsPath string
}

// sshInvocation builds the ssh argv. remoteCommand is always at most one
// argument: OpenSSH joins its remote argv with spaces, so the CLI quotes argv
// itself and passes exactly one string. An empty remoteCommand opens a session.
func sshInvocation(access sshAccess, tty bool, remoteCommand string, stdin io.Reader, stdout, stderr io.Writer) ProcessCommand {
	args := make([]string, 0, 16)
	if tty {
		args = append(args, "-t")
	}
	args = append(args, "-i", access.privateKeyPath)
	args = append(args, sshPortArgs("-p", access.target.Port)...)
	args = append(args, sshPinArgs(access.knownHostsPath, access.sandboxID)...)
	args = append(args, sshDestination(access.target))
	if remoteCommand != "" {
		args = append(args, remoteCommand)
	}
	return ProcessCommand{
		Path:       "ssh",
		Args:       args,
		Stdin:      stdin,
		Stdout:     stdout,
		Stderr:     stderr,
		Foreground: processForeground(stdin),
	}
}

// sshPinArgs returns the ssh host-key pinning options. The known-hosts path is
// config-quoted inside one argv element. Global known-hosts, DNS and host-key
// updates are disabled so only the authenticated per-invocation pin is trusted
// and nothing can add or replace entries in it.
func sshPinArgs(knownHostsPath, sandboxID string) []string {
	return []string{
		"-o", "IdentitiesOnly=yes",
		"-o", "StrictHostKeyChecking=yes",
		"-o", "UserKnownHostsFile=" + sshConfigValue(knownHostsPath),
		"-o", "GlobalKnownHostsFile=/dev/null",
		"-o", "HostKeyAlias=" + sandboxID,
		"-o", "CheckHostIP=no",
		"-o", "VerifyHostKeyDNS=no",
		"-o", "UpdateHostKeys=no",
	}
}

// sshPortArgs returns the port flag pair unless the pinned default is used.
// ssh uses -p and scp uses capital -P.
func sshPortArgs(flag string, port int) []string {
	if port == sshDefaultPort {
		return nil
	}
	return []string{flag, strconv.Itoa(port)}
}

// sshDestination renders user@host, bracketing IPv6 literals.
func sshDestination(target sshTarget) string {
	return target.User + "@" + sshHostLiteral(target.Host)
}

// sshHostLiteral brackets a host that is an IPv6 literal.
func sshHostLiteral(host string) string {
	if strings.Contains(host, ":") {
		return "[" + host + "]"
	}
	return host
}

// scpRemote renders user@host:path for a remote SCP operand.
func scpRemote(target sshTarget, remotePath string) string {
	return sshDestination(target) + ":" + remotePath
}

// processForeground reports whether the child owns an interactive terminal.
// True means its own process group is handed the terminal; false leaves that
// group in the background. The child always gets its own group either way.
func processForeground(stdin io.Reader) bool {
	file, ok := stdin.(*os.File)
	return ok && isTerminalFile(file)
}

// scpPlan is a parsed `bbraft scp` command: exactly one remote operand.
type scpPlan struct {
	recursive   bool
	remoteIndex int
	remoteID    string
	remotePath  string
	args        []string
}

// parseSCPPlan finds the single bx_ID:path operand and validates every local
// operand. It never rewrites the remote operand yet: the user and host come
// from the sshKey response.
func parseSCPPlan(args []string, recursive bool) (scpPlan, error) {
	if len(args) < 2 {
		return scpPlan{}, &UsageError{Err: errors.New("scp needs at least one source and one destination")}
	}
	plan := scpPlan{recursive: recursive, remoteIndex: -1, args: args}
	for i, arg := range args {
		if strings.ContainsRune(arg, '\x00') {
			return scpPlan{}, &UsageError{Err: errors.New("scp path contains a NUL byte")}
		}
		if arg == "" {
			return scpPlan{}, &UsageError{Err: errors.New("scp path is empty")}
		}
		id, path, ok := parseSCPRemote(arg)
		if !ok {
			if _, err := localSCPPath(arg); err != nil {
				return scpPlan{}, err
			}
			continue
		}
		if plan.remoteIndex >= 0 {
			return scpPlan{}, &UsageError{Err: errors.New("scp accepts only one remote operand")}
		}
		plan.remoteIndex = i
		plan.remoteID = id
		plan.remotePath = path
	}
	if plan.remoteIndex < 0 {
		return scpPlan{}, &UsageError{Err: errors.New("scp needs one remote operand in the form bx_ID:/path")}
	}
	return plan, nil
}

// parseSCPRemote splits bx_ID:/path. A colon on a local path is not treated as
// remote unless the left side is a syntactically valid sandbox id.
func parseSCPRemote(arg string) (string, string, bool) {
	id, path, ok := strings.Cut(arg, ":")
	if !ok || !ids.ValidSandbox(id) {
		return "", "", false
	}
	return id, path, true
}

// localSCPPath makes a local operand unambiguous to scp. OpenSSH treats any
// pathname containing ':' as a host specifier; the man page documents that
// absolute paths and explicit relative paths (./…) avoid that. A leading dash
// is prefixed so scp cannot read it as an option.
func localSCPPath(arg string) (string, error) {
	if strings.ContainsRune(arg, '\x00') {
		return "", &UsageError{Err: errors.New("scp path contains a NUL byte")}
	}
	if filepath.IsAbs(arg) {
		return arg, nil
	}
	needsPrefix := strings.HasPrefix(arg, "-") || strings.Contains(arg, ":")
	if needsPrefix && !strings.HasPrefix(arg, "./") {
		return "./" + arg, nil
	}
	return arg, nil
}

// scpInvocation builds the scp argv with the destination last. Remote operands
// become user@host:path; local operands are made unambiguous.
func scpInvocation(access sshAccess, plan scpPlan, stdin io.Reader, stdout, stderr io.Writer) ProcessCommand {
	args := make([]string, 0, len(plan.args)+12)
	if plan.recursive {
		args = append(args, "-r")
	}
	args = append(args, "-i", access.privateKeyPath)
	args = append(args, sshPortArgs("-P", access.target.Port)...)
	args = append(args, sshPinArgs(access.knownHostsPath, access.sandboxID)...)
	for i, arg := range plan.args {
		if i == plan.remoteIndex {
			args = append(args, scpRemote(access.target, plan.remotePath))
			continue
		}
		// parseSCPPlan already validated every local operand.
		local, err := localSCPPath(arg)
		if err != nil {
			continue
		}
		args = append(args, local)
	}
	return ProcessCommand{
		Path:       "scp",
		Args:       args,
		Stdin:      stdin,
		Stdout:     stdout,
		Stderr:     stderr,
		Foreground: processForeground(stdin),
	}
}

// processStatus maps a finished child to its exit status.
func processStatus(ctx context.Context, waitErr error) (int, error) {
	if waitErr == nil {
		return 0, nil
	}
	var exitErr *exec.ExitError
	if errors.As(waitErr, &exitErr) {
		if ctx.Err() != nil {
			return exitErr.ExitCode(), ctx.Err()
		}
		return exitErr.ExitCode(), nil
	}
	if ctx.Err() != nil {
		return 0, ctx.Err()
	}
	return 0, &CLIError{Code: "process_start_failed", Message: "wait for the external program failed"}
}

// processStartFailure maps a start failure without echoing paths or arguments.
func processStartFailure(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if errors.Is(err, exec.ErrNotFound) || errors.Is(err, os.ErrNotExist) {
		return &CLIError{Code: "process_start_failed", Message: "the external program is not installed"}
	}
	return &CLIError{Code: "process_start_failed", Message: "cannot start the external program"}
}
