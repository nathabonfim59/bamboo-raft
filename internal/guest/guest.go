// Package guest implements the guest agent service: exec,
// file operations, SSH key installation and freeze/thaw inside a sandbox VM,
// served over AF_VSOCK to bbraft-node. The service mirrors the Exec and
// PutFile semantics of the frozen nodev1 contract, reusing its message types.
//
// The agent runs as root. Exec drops to the unprivileged sandbox user for
// every command. Every file operation runs as that user too: when the agent
// is not already the configured sandbox uid/gid it executes a one-shot,
// credential-dropped helper process (see [FileHelperMain]) that performs the
// operation, so the kernel enforces mode bits, access ACLs, sticky
// directories and ownership rather than user-space emulation. In either path
// all filesystem access is confined with [os.Root] to the configured home
// and tmp roots, which also rejects symlink escapes. If the sandbox user
// cannot open one of those roots as a directory (a search-only 0111 root),
// file RPCs return INVALID_ARGUMENT naming the logical root while Exec keeps
// working for known paths.
package guest

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"google.golang.org/grpc"

	"github.com/nathabonfim59/bamboo-raft/internal/proto/guestv1"
)

// Spec path prefixes as the v1 API sees them. Relative paths resolve
// under the home prefix; every path must end up under one of the two.
const (
	homeSpec = "/home/user"
	tmpSpec  = "/tmp"
)

// maxID is the largest acceptable uid/gid: 0 is root and 4294967295 is
// setuid(2)'s "no change" sentinel; neither may name the sandbox user.
const maxID = 4294967294

// Config configures a Service. The zero value is not usable: fill in at
// least Log; the rest have defaults for the standard guest layout.
type Config struct {
	// Log receives structured diagnostics. nil: logs are discarded.
	Log *slog.Logger
	// Version is reported by Ping. Default "dev".
	Version string

	// UID and GID are the sandbox user's ids used as the exec credential
	// and the owner of uploaded files, each in 1..4294967294. Default
	// 1000/1000. The account name is resolved from the system user
	// database for this uid; a uid without an account is a configuration
	// error.
	UID int
	GID int
	// GroupFile is the group(5) file the user's supplementary groups are
	// resolved from, on every credential build. Default /etc/group. An
	// unreadable or malformed file fails the credential rather than
	// silently dropping the user's membership; the field exists so tests
	// can inject a deterministic database.
	GroupFile string
	// NoSetGroups keeps the agent's supplementary groups instead of
	// installing the resolved ones. Only for the agent running
	// unprivileged (tests, smoke runs outside a VM): adopting an explicit
	// group list needs CAP_SETGID. The real agent installs the resolved
	// list and passes none of root's groups on.
	NoSetGroups bool

	// ForceFileHelper runs every file operation in the credential-dropped
	// helper even when the agent already runs as the configured sandbox
	// user. It is a test seam for exercising the helper protocol as an
	// unprivileged process; production never needs it.
	ForceFileHelper bool

	// HomeRoot is the directory relative paths resolve under and the
	// value of HOME. Default /home/user.
	HomeRoot string
	// TmpRoot is the directory /tmp paths map to. Default /tmp.
	TmpRoot string

	// DefaultLease is the freeze lease used when a FreezeRequest omits
	// it. It must be positive and no larger than maxLease. Default 2m.
	DefaultLease time.Duration
	// WatchdogRetry is how long the freeze watchdog waits between thaw
	// attempts when a thaw fails. Default 5s.
	WatchdogRetry time.Duration

	// Freezer freezes and thaws the guest root filesystem. nil: the real
	// FIFREEZE/FITHAW ioctl freezer (requires CAP_SYS_ADMIN).
	Freezer Freezer
	// SyncDir makes a directory entry durable after PutFile's rename.
	// nil: fsync the directory. Tests inject failures; a failed sync is
	// reported to the uploader.
	SyncDir func(*os.File) error
	// SyncFile makes a file durable after process state writes (process
	// metadata, final records, the log sync before them). nil: fsync the
	// file. Tests inject failures; a failed sync before the final process
	// record leaves it undurable, so a restart recovers the process LOST.
	SyncFile func(*os.File) error

	// SSHHostKeyFile is the guest's ed25519 host public key that AddSSHKey
	// publishes, in OpenSSH format. The image generates it fresh on every
	// boot in tmpfs before sshd starts, so it never reaches the image or a
	// snapshot. Default /run/ssh/ssh_host_ed25519_key.pub.
	SSHHostKeyFile string
}

// Service implements guestv1.GuestServer. Construct with [New].
type Service struct {
	guestv1.UnimplementedGuestServer

	log     *slog.Logger
	version string

	uid, gid    int
	noSetGroups bool
	groupFile   string // group(5) file the user's groups are resolved from
	homeDir     string // configured home root, also HOME and cwd base for execs
	homeRoot    *os.Root
	tmpRoot     *os.Root

	files      fileOps
	ssh        sshInstaller // serializes authorized_keys updates; host key path
	freezer    *freezerState
	syncDir    func(*os.File) error
	syncFile   func(*os.File) error
	closeRoots func() error

	// Detached-process registry and state, owned by process.go. procs maps
	// server-chosen process ids to their live or recovered entries;
	// procDir is the pinned no-follow handle on <home>/.bbraft/processes.
	// groupScan and killProve are per-service seams for the kill machinery
	// (the /proc group scan and the SIGKILL proof bound): supervisors
	// deliberately outlive RPCs and Shutdown, so test injection lives on
	// the service, never in shared package state. procFS is the seam table
	// for the process-state filesystem and signaling calls; nil fields use
	// the real syscall.
	procMu        sync.Mutex
	procs         map[uint32]*detachedProc
	procDir       *os.File
	procDirClosed bool
	groupScan     func(pgid int, includeZombies bool) bool
	killProve     time.Duration
	preSignal     func()
	procFS        procFS
}

// New builds a Service and opens the confinement roots. Call
// [Service.Shutdown] when done.
func New(cfg Config) (*Service, error) {
	if cfg.Log == nil {
		cfg.Log = slog.New(slog.DiscardHandler)
	}
	if cfg.Version == "" {
		cfg.Version = "dev"
	}
	if cfg.UID == 0 {
		cfg.UID = 1000
	}
	if cfg.GID == 0 {
		cfg.GID = 1000
	}
	for _, id := range []struct {
		name string
		val  int
	}{{"UID", cfg.UID}, {"GID", cfg.GID}} {
		if id.val < 1 || id.val > maxID {
			return nil, fmt.Errorf("guest: %s %d out of range 1..%d", id.name, id.val, maxID)
		}
	}
	if cfg.HomeRoot == "" {
		cfg.HomeRoot = homeSpec
	}
	if cfg.TmpRoot == "" {
		cfg.TmpRoot = tmpSpec
	}
	if cfg.DefaultLease <= 0 {
		cfg.DefaultLease = 2 * time.Minute
	}
	if cfg.DefaultLease > maxLease {
		return nil, fmt.Errorf("guest: DefaultLease %s exceeds the maximum %s", cfg.DefaultLease, maxLease)
	}
	if cfg.WatchdogRetry <= 0 {
		cfg.WatchdogRetry = 5 * time.Second
	}
	if cfg.GroupFile == "" {
		cfg.GroupFile = "/etc/group"
	}
	if cfg.SSHHostKeyFile == "" {
		cfg.SSHHostKeyFile = defaultSSHHostKeyFile
	}
	homeRoot, err := os.OpenRoot(cfg.HomeRoot)
	if err != nil {
		return nil, err
	}
	tmpRoot, err := os.OpenRoot(cfg.TmpRoot)
	if err != nil {
		homeRoot.Close()
		return nil, err
	}
	if cfg.Freezer == nil {
		cfg.Freezer, err = newIoctlFreezer("/")
		if err != nil {
			homeRoot.Close()
			tmpRoot.Close()
			return nil, err
		}
	}
	if cfg.SyncDir == nil {
		cfg.SyncDir = func(d *os.File) error { return d.Sync() }
	}
	if cfg.SyncFile == nil {
		cfg.SyncFile = func(f *os.File) error { return f.Sync() }
	}
	var files fileOps
	svc := &Service{
		log:         cfg.Log,
		version:     cfg.Version,
		uid:         cfg.UID,
		gid:         cfg.GID,
		noSetGroups: cfg.NoSetGroups,
		groupFile:   cfg.GroupFile,
		homeDir:     cfg.HomeRoot,
		homeRoot:    homeRoot,
		tmpRoot:     tmpRoot,
		ssh:         sshInstaller{gate: newSSHGate(), hostKeyFile: cfg.SSHHostKeyFile},
		freezer:     newFreezerState(cfg.Freezer, cfg.DefaultLease, cfg.WatchdogRetry, cfg.Log),
		syncDir:     cfg.SyncDir,
		syncFile:    cfg.SyncFile,
		procs:       map[uint32]*detachedProc{},
		closeRoots: sync.OnceValue(func() error {
			return errors.Join(homeRoot.Close(), tmpRoot.Close())
		}),
	}
	if cfg.ForceFileHelper || os.Geteuid() != cfg.UID || os.Getegid() != cfg.GID {
		// The agent is not the sandbox user (production root): every file
		// operation runs in a credential-dropped helper so the kernel
		// enforces the user's access. Tests can force this path even when
		// the process already owns the files.
		files, err = newHelperFiles(homeRoot, tmpRoot, cfg.UID, cfg.GID, svc.credential)
		if err != nil {
			homeRoot.Close()
			tmpRoot.Close()
			return nil, err
		}
	} else {
		files = newFileServer(homeRoot, tmpRoot, cfg.SyncDir, cfg.Log)
	}
	svc.files = files
	// Rebuild the detached-process registry from the durable records:
	// unfinished processes recover as LOST, finished ones stay EXITED.
	svc.initProcesses()
	return svc, nil
}

// Register adds the service to a gRPC server.
func (s *Service) Register(g *grpc.Server) { guestv1.RegisterGuestServer(g, s) }

// Shutdown thaws the guest if it is still frozen — an exiting agent must
// never leave it frozen — retrying failed thaws on the watchdog cadence
// until ctx ends, and then closes the confinement roots. Detached
// processes are deliberately left running with their supervisor goroutines
// alive: they are independent of the service's lifetime, and their exit
// records must still land durably. If Shutdown gives up with the guest
// still frozen, the watchdog stays armed so the guest thaws by itself, and
// it returns an error; the caller must keep the process alive after that
// error, or the watchdog dies with it and the guest stays frozen. Passing
// context.Background() makes Shutdown wait for the thaw instead.
func (s *Service) Shutdown(ctx context.Context) error {
	thawErr := s.freezer.shutdown(ctx)
	if !s.freezer.isClosed() {
		// The thaw failed: keep the confinement roots and the freezer open
		// so the watchdog's retries and outstanding handlers stay valid.
		// Closing them here would break the very recovery path that keeps
		// the guest from staying frozen.
		return thawErr
	}
	closeProcState := sync.OnceValue(func() error {
		// Outstanding supervisors keep their own per-process handles; this
		// releases only the shared directory pin.
		s.procMu.Lock()
		defer s.procMu.Unlock()
		s.procDirClosed = true
		if s.procDir == nil {
			return nil
		}
		err := s.procDir.Close()
		s.procDir = nil
		return err
	})
	return errors.Join(thawErr, s.closeRoots(), closeProcState())
}

// Ping answers the guest's readiness probe. It touches no filesystem and
// writes no logs, so it keeps answering while the root filesystem is
// frozen.
func (s *Service) Ping(context.Context, *guestv1.PingRequest) (*guestv1.PingResponse, error) {
	return &guestv1.PingResponse{Version: s.version}, nil
}

// freezerState is the in-memory freeze bookkeeping. Nothing here touches
// the filesystem: the guest must keep serving (Thaw above all) while its
// root filesystem is frozen, and the handlers log nothing between a
// successful freeze and a successful thaw, so nothing can block on the
// frozen filesystem.
type freezerState struct {
	log           *slog.Logger
	freezer       Freezer
	defaultLease  time.Duration
	watchdogRetry time.Duration

	mu         sync.Mutex
	frozen     bool
	closing    bool // Shutdown began; new freezes are refused
	closed     bool // Shutdown thawed and closed; no new freeze may start
	leaseToken string
	leaseEnd   time.Time
	watchdog   *time.Timer
	thawFails  int
	closeErr   error
	gen        uint64 // lease generation; stale watchdog callbacks no-op
}
