// Command bbraft-guest is the agent inside every bamboo-raft VM. It
// serves the guest protocol over AF_VSOCK (fixed port 10789) to
// bbraft-node: exec as the unprivileged sandbox user, confined file
// uploads, and freeze/thaw for snapshots. It runs as root and must never
// hold platform credentials.
package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc"

	"github.com/mdlayher/vsock"

	"github.com/nathabonfim59/bamboo-raft/internal/guest"
	"github.com/nathabonfim59/bamboo-raft/internal/version"
)

func main() {
	// A credential-dropped file helper is this same binary re-executed by the
	// running agent; it serves one file operation on stdin/stdout as the
	// sandbox user and never starts the vsock service or touches the freezer.
	if guest.FileHelperRequested() {
		os.Exit(guest.FileHelperMain())
	}
	if err := run(os.Args[1:], os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, "bbraft-guest:", err)
		os.Exit(1)
	}
}

func run(args []string, stderr io.Writer) error {
	return runWithDeps(args, stderr, guest.RecoverFrozenRoot)
}

// runWithDeps injects root recovery per call; tests never invoke the real /.
func runWithDeps(args []string, stderr io.Writer, recoverRoot func(context.Context, time.Duration) error) error {
	// Parsing writes into memory: on a normal startup (including a malformed
	// command line) no diagnostic reaches a potentially frozen root until the
	// previous agent's freeze has been recovered. Help is not a startup and
	// stays usable without issuing privileged ioctls.
	var parseOutput bytes.Buffer
	cfg, parseErr := parseFlags(args, &parseOutput)
	if errors.Is(parseErr, flag.ErrHelp) {
		_, err := stderr.Write(parseOutput.Bytes())
		return err
	}
	// Signals must be caught before recovery: a service stop during a slow
	// FITHAW retry must not take the default terminating action and kill the
	// only watchdog that can rescue the frozen root. Recovery itself stays on
	// Background until the thaw is proven, then a queued shutdown is honored
	// before any diagnostic write or service startup.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := recoverRoot(context.Background(), time.Second); err != nil {
		return fmt.Errorf("recover frozen root: %w", err)
	}
	select {
	case <-ctx.Done():
		return nil
	default:
	}
	if parseErr != nil {
		_, writeErr := stderr.Write(parseOutput.Bytes())
		return errors.Join(parseErr, writeErr)
	}
	log := slog.New(slog.NewTextHandler(stderr, nil))
	log.Info("starting bbraft-guest",
		"version", version.Version,
		"vsock_port", cfg.port,
		"home", cfg.home,
	)

	svc, err := guest.New(guest.Config{
		Log:           log,
		Version:       version.Version,
		UID:           cfg.uid,
		GID:           cfg.gid,
		HomeRoot:      cfg.home,
		TmpRoot:       cfg.tmp,
		DefaultLease:  cfg.freezeLease,
		WatchdogRetry: cfg.watchdogRetry,
	})
	if err != nil {
		return fmt.Errorf("build service: %w", err)
	}

	ln, err := listenVsock(uint32(cfg.cid), uint32(cfg.port))
	if err != nil {
		shutdownThaw(svc, log)
		return fmt.Errorf("listen on vsock port %d: %w", cfg.port, err)
	}
	srv := grpc.NewServer()
	svc.Register(srv)
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(ln) }()

	select {
	case err := <-serveErr:
		shutdownThaw(svc, log)
		return fmt.Errorf("serve: %w", err)
	case <-ctx.Done():
	}
	// Thaw before anything logs or drains: on a frozen filesystem both
	// could block forever, and an exiting agent must never leave the
	// guest frozen.
	shutdownThaw(svc, log)
	log.Info("shutting down")
	stopServe(srv, log)
	return nil
}

// shutdownThaw thaws the guest before any other shutdown step, logging or
// draining: on a frozen filesystem logging can block forever, and an
// exiting agent must never leave the guest frozen. It deliberately takes no
// context: a deadline that expires while the guest is frozen would let this
// process exit and kill the in-process watchdog that is supposed to rescue
// the guest. Shutdown with a context that cannot expire only returns after a
// successful thaw (or after the thawed freezer failed to close), so this
// helper can never log or return while the guest is still frozen.
func shutdownThaw(svc *guest.Service, log *slog.Logger) {
	if err := svc.Shutdown(context.Background()); err != nil {
		// The guest thawed but cleanup failed; the watchdog is no longer
		// needed. Logging is safe now and the caller may exit.
		log.Error("shutdown cleanup failed after thaw", "err", err)
	}
}

// stopServe stops the gRPC server gracefully with a hard fallback, so a
// stuck stream cannot hold the shutdown hostage.
func stopServe(srv *grpc.Server, log *slog.Logger) {
	done := make(chan struct{})
	go func() {
		srv.GracefulStop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		log.Warn("graceful stop timed out; stopping hard")
		srv.Stop()
		<-done
	}
}

// listenVsock listens on the vsock port, letting the kernel pick the
// context ID unless an explicit one is configured.
func listenVsock(cid, port uint32) (net.Listener, error) {
	if cid == 0 {
		return vsock.Listen(port, nil)
	}
	return vsock.ListenContextID(cid, port, nil)
}

// flags holds the parsed command line.
type flags struct {
	cid           uint64
	port          uint64
	uid           int
	gid           int
	home          string
	tmp           string
	freezeLease   time.Duration
	watchdogRetry time.Duration
}

// parseFlags fills flags from command-line flags with BBRAFT_* environment
// fallback (dots and dashes become underscores, upper-cased).
func parseFlags(args []string, out io.Writer) (*flags, error) {
	fs := flag.NewFlagSet("bbraft-guest", flag.ContinueOnError)
	fs.SetOutput(out)

	var f flags
	var err error
	fs.Uint64Var(&f.cid, "guest.vsock-cid", 0,
		"vsock CID to listen on; 0 picks this VM's CID (env BBRAFT_GUEST_VSOCK_CID)")
	fs.Uint64Var(&f.port, "guest.vsock-port", 10789,
		"vsock port to listen on (env BBRAFT_GUEST_VSOCK_PORT)")
	fs.IntVar(&f.uid, "guest.uid", 1000,
		"sandbox user uid for exec and uploads (env BBRAFT_GUEST_UID)")
	fs.IntVar(&f.gid, "guest.gid", 1000,
		"sandbox user gid for exec and uploads (env BBRAFT_GUEST_GID)")
	fs.StringVar(&f.home, "guest.home", "/home/user",
		"home directory: confinement root and HOME (env BBRAFT_GUEST_HOME)")
	fs.StringVar(&f.tmp, "guest.tmp", "/tmp",
		"tmp directory /tmp paths map to (env BBRAFT_GUEST_TMP)")
	fs.DurationVar(&f.freezeLease, "guest.freeze-lease", 2*time.Minute,
		"default freeze lease when the node requests none (env BBRAFT_GUEST_FREEZE_LEASE)")
	fs.DurationVar(&f.watchdogRetry, "guest.watchdog-retry", 5*time.Second,
		"delay between watchdog thaw attempts (env BBRAFT_GUEST_WATCHDOG_RETRY)")
	if err := fs.Parse(args); err != nil {
		return nil, err
	}
	if fs.NArg() > 0 {
		return nil, fmt.Errorf("unexpected argument %q", fs.Arg(0))
	}

	// Flags are authoritative over the BBRAFT_* fallback: parse first, then
	// fill only the flags the command line did not set. A malformed value
	// for an unset flag is a configuration error (never a silent fallback),
	// and the error names the variable without echoing its value.
	explicit := make(map[string]bool)
	fs.Visit(func(fl *flag.Flag) { explicit[fl.Name] = true })
	if !explicit["guest.vsock-cid"] {
		if f.cid, err = envUint64("guest.vsock-cid", 0); err != nil {
			return nil, err
		}
	}
	if !explicit["guest.vsock-port"] {
		if f.port, err = envUint64("guest.vsock-port", 10789); err != nil {
			return nil, err
		}
	}
	if !explicit["guest.uid"] {
		if f.uid, err = envInt("guest.uid", 1000); err != nil {
			return nil, err
		}
	}
	if !explicit["guest.gid"] {
		if f.gid, err = envInt("guest.gid", 1000); err != nil {
			return nil, err
		}
	}
	if !explicit["guest.home"] {
		f.home = envString("guest.home", "/home/user")
	}
	if !explicit["guest.tmp"] {
		f.tmp = envString("guest.tmp", "/tmp")
	}
	if !explicit["guest.freeze-lease"] {
		if f.freezeLease, err = envDuration("guest.freeze-lease", 2*time.Minute); err != nil {
			return nil, err
		}
	}
	if !explicit["guest.watchdog-retry"] {
		if f.watchdogRetry, err = envDuration("guest.watchdog-retry", 5*time.Second); err != nil {
			return nil, err
		}
	}
	if f.port == 0 || f.port > 0xFFFFFFFF {
		return nil, errors.New("-guest.vsock-port must be between 1 and 4294967295")
	}
	if f.cid > 0xFFFFFFFF {
		return nil, errors.New("-guest.vsock-cid is not a 32-bit cid")
	}
	if f.uid < 1 || f.uid > 4294967294 || f.gid < 1 || f.gid > 4294967294 {
		return nil, errors.New("-guest.uid and -guest.gid must be between 1 and 4294967294")
	}
	if f.freezeLease <= 0 || f.watchdogRetry <= 0 {
		return nil, errors.New("-guest.freeze-lease and -guest.watchdog-retry must be positive")
	}
	return &f, nil
}

// envName maps a flag name to its BBRAFT_* variable.
func envName(flagName string) string {
	rep := strings.NewReplacer(".", "_", "-", "_")
	return "BBRAFT_" + strings.ToUpper(rep.Replace(flagName))
}

func envString(name, def string) string {
	if v, ok := os.LookupEnv(envName(name)); ok {
		return v
	}
	return def
}

func envUint64(name string, def uint64) (uint64, error) {
	v, ok := os.LookupEnv(envName(name))
	if !ok {
		return def, nil
	}
	n, err := strconv.ParseUint(strings.TrimSpace(v), 10, 64)
	if err != nil {
		// Never echo the value: it may be secret-adjacent and the message
		// only needs to name the misconfigured variable.
		return 0, fmt.Errorf("%s is not a valid unsigned integer", envName(name))
	}
	return n, nil
}

func envInt(name string, def int) (int, error) {
	v, ok := os.LookupEnv(envName(name))
	if !ok {
		return def, nil
	}
	n, err := strconv.Atoi(strings.TrimSpace(v))
	if err != nil {
		return 0, fmt.Errorf("%s is not a valid integer", envName(name))
	}
	return n, nil
}

func envDuration(name string, def time.Duration) (time.Duration, error) {
	v, ok := os.LookupEnv(envName(name))
	if !ok {
		return def, nil
	}
	d, err := time.ParseDuration(strings.TrimSpace(v))
	if err != nil {
		return 0, fmt.Errorf("%s is not a valid duration", envName(name))
	}
	return d, nil
}
