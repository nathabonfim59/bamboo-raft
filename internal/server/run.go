package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/nathabonfim59/bamboo-raft/internal/api"
	"github.com/nathabonfim59/bamboo-raft/internal/db"
	"github.com/nathabonfim59/bamboo-raft/internal/objstore"
	"github.com/nathabonfim59/bamboo-raft/internal/objstore/s3"
	"github.com/nathabonfim59/bamboo-raft/internal/server/config"
)

// DefaultShutdownTimeout bounds the api role's HTTP graceful shutdown. After
// it, the server is closed so a slow request body cannot hang process exit.
const DefaultShutdownTimeout = 5 * time.Second

// Listeners are optional pre-bound listeners for tests. A nil field makes
// its role bind the configured address. Providing one never bypasses role
// startup or TLS wrapping.
type Listeners struct {
	API     net.Listener
	Node    net.Listener
	Gateway net.Listener
}

// closeFor closes only the listeners the selected roles own. An api-only
// process never closes a nodehub listener, and vice versa.
func (l Listeners) closeFor(roles config.Roles) {
	if roles.Has(config.RoleAPI) && l.API != nil {
		_ = l.API.Close()
	}
	if roles.Has(config.RoleNodeHub) {
		if l.Node != nil {
			_ = l.Node.Close()
		}
		if l.Gateway != nil {
			_ = l.Gateway.Close()
		}
	}
}

// roleFailure is the safe boundary error of one failed role. It deliberately
// carries only the role and a fixed classification, never the driver or node
// cause.
type roleFailure struct {
	role   string
	reason string
}

// Error implements error.
func (e *roleFailure) Error() string { return "server: " + e.role + " failed: " + e.reason }

// StoreOpener returns one role's own open, migrated store. Production leaves
// it nil and each role opens a private pool from config with the install
// master key and migrates under the accepted locks. Tests may supply their
// own independent stores; the seam still calls the opener once per role.
type StoreOpener func(ctx context.Context, role config.Role) (*db.Store, error)

// UploadDeps are the exact wiring contract of the separately reviewed upload
// package: its constructor takes these values and returns its two handlers.
type UploadDeps struct {
	Lookup     api.CommandTargetLookup
	Clients    api.NodeClientFactory
	SigningKey []byte
	BaseURL    string
	Logger     *slog.Logger
}

// RegisterUploads optionally replaces the default upload routes. It receives
// the root mux and the bearer middleware: the signed-URL receiver must be
// registered without bearer, while the signed-URL issuer uses it. Nil
// installs the real api.NewUploadsHandlers routes.
type RegisterUploads func(mux *http.ServeMux, bearer func(http.Handler) http.Handler, deps UploadDeps) error

// APIHooks are the route packages added to the api role outside this
// package.
type APIHooks struct {
	RegisterUploads RegisterUploads
}

// Options configure [Run].
type Options struct {
	// Logger receives operational logs. Nil discards them.
	Logger *slog.Logger
	// Listeners optionally pre-bind the three role listeners for tests.
	Listeners Listeners
	// StoreOpener optionally supplies role stores for tests.
	StoreOpener StoreOpener
	// ObjectStore optionally supplies the installation's object store,
	// bypassing the S3 settings in cfg.ObjectStore (tests inject a
	// memstore). Nil uses the configured bucket; when that is nil too the
	// server runs in Phase 1 mode: no snapshot uploads, local stop/resume.
	ObjectStore objstore.Store
	// APIHooks optionally register accepted route packages.
	APIHooks APIHooks
	// InventoryInterval overrides the nodehub inventory poll interval.
	// Zero uses nodegateway.DefaultInventoryInterval. It is a test seam;
	// production keeps the default.
	InventoryInterval time.Duration
	// ShutdownTimeout bounds HTTP graceful shutdown. Zero uses
	// DefaultShutdownTimeout.
	ShutdownTimeout time.Duration
}

// Run starts every selected role until ctx ends or a role fails. A startup or
// runtime failure cancels and joins the other roles. When ctx ends normally
// Run returns nil after closing listeners, joining workers and bounding HTTP
// shutdown.
func Run(ctx context.Context, cfg *config.Config, options Options) error {
	if cfg == nil {
		return errors.New("server: Run needs a config")
	}
	logger := options.Logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	shutdownTimeout := options.ShutdownTimeout
	if shutdownTimeout <= 0 {
		shutdownTimeout = DefaultShutdownTimeout
	}

	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)

	var wg sync.WaitGroup
	start := func(name string, run func(context.Context) error) {
		wg.Go(func() {
			if err := run(ctx); err != nil && ctx.Err() == nil {
				// Keep the actual cause out of the boundary: driver and
				// node errors can carry DSNs, tokens or request material.
				cancel(&roleFailure{role: name, reason: safeErrorName(err)})
			}
		})
	}

	defer options.Listeners.closeFor(cfg.Roles)

	objects, err := openObjectStore(ctx, cfg, options.ObjectStore)
	if err != nil {
		return err
	}

	openStore := options.storeOpener(cfg)
	if cfg.Roles.Has(config.RoleAPI) {
		start("api", func(ctx context.Context) error {
			return runAPI(ctx, cfg, logger, options, openStore, shutdownTimeout, objects != nil)
		})
	}
	if cfg.Roles.Has(config.RoleLifecycle) {
		start("lifecycle", func(ctx context.Context) error {
			return runLifecycle(ctx, cfg, logger, openStore, objects)
		})
	}
	if cfg.Roles.Has(config.RoleNodeHub) {
		start("nodehub", func(ctx context.Context) error {
			return runNodeHub(ctx, cfg, logger, options, openStore)
		})
	}

	wg.Wait()
	cause := context.Cause(ctx)
	switch {
	case cause == nil, errors.Is(cause, context.Canceled):
		return nil
	default:
		return cause
	}
}

// storeOpener returns the configured test seam or the real per-role opener.
func (o Options) storeOpener(cfg *config.Config) StoreOpener {
	if o.StoreOpener != nil {
		return o.StoreOpener
	}
	return func(ctx context.Context, role config.Role) (*db.Store, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		secrets := db.Secrets{MasterKey: cfg.Secrets.MasterKey}
		store, err := db.Open(ctx, cfg.Database.URL, secrets)
		if err != nil {
			return nil, err
		}
		if err := store.Migrate(ctx); err != nil {
			store.Close()
			return nil, err
		}
		return store, nil
	}
}

// openObjectStore resolves the installation's object store: an injected store
// wins, no configured bucket means Phase 1 mode (nil), and otherwise it
// connects to the configured S3 bucket. A failed connection fails start-up;
// the error never carries credentials.
func openObjectStore(ctx context.Context, cfg *config.Config, injected objstore.Store) (objstore.Store, error) {
	if injected != nil {
		return injected, nil
	}
	if cfg == nil || cfg.ObjectStore == nil {
		return nil, nil
	}
	store, err := s3.New(ctx, *cfg.ObjectStore)
	if err != nil {
		return nil, fmt.Errorf("server: object store: %w", err)
	}
	return store, nil
}
