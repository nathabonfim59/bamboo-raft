package server

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"sync"
	"time"

	"google.golang.org/grpc"

	"github.com/nathabonfim59/bamboo-raft/internal/api"
	"github.com/nathabonfim59/bamboo-raft/internal/apierr"
	"github.com/nathabonfim59/bamboo-raft/internal/auth"
	"github.com/nathabonfim59/bamboo-raft/internal/lifecycle"
	snapshotsplugin "github.com/nathabonfim59/bamboo-raft/internal/lifecycle/snapshots"
	"github.com/nathabonfim59/bamboo-raft/internal/nodelink"
	"github.com/nathabonfim59/bamboo-raft/internal/scheduler"
	"github.com/nathabonfim59/bamboo-raft/internal/server/config"
	"github.com/nathabonfim59/bamboo-raft/internal/server/nodegateway"
	"github.com/nathabonfim59/bamboo-raft/internal/version"
)

// HTTP server bounds. There is deliberately no ReadTimeout or WriteTimeout:
// streamed commands and uploads may take as long as both sides allow. Header
// size and idle time are still bounded.
const (
	httpReadHeaderTimeout = 10 * time.Second
	httpIdleTimeout       = 120 * time.Second
	httpMaxHeaderBytes    = 1 << 20
)

// runAPI starts the public HTTP role. It owns one store handle, the auth
// bootstrap and the bearer stack; it reaches nodes only through the internal
// gateway client factory.
func runAPI(ctx context.Context, cfg *config.Config, logger *slog.Logger, options Options, openStore StoreOpener, shutdownTimeout time.Duration) error {
	store, err := openStore(ctx, config.RoleAPI)
	if err != nil {
		return err
	}
	defer store.Close()

	keys, err := auth.LoadKeysFileWithPrefix(ctx, cfg.Auth.KeysFile, cfg.Auth.KeyPrefix)
	if err != nil {
		return err
	}
	mapping, err := bootstrapAuth(ctx, store, keys, cfg.Auth.KeyPrefix, logger)
	if err != nil {
		return err
	}
	gatewayTLS, err := gatewayClientTLS(ctx, &cfg.Node)
	if err != nil {
		return err
	}
	factory := nodegateway.NewClientFactory(cfg.Node.GatewayDial, cfg.Node.GatewayToken, gatewayTLS)
	placer, err := scheduler.New(scheduler.Options{
		Store:   store,
		Dial:    gatewaySchedulerDial(factory),
		Log:     logger,
		Restore: snapshotRestoreResolver{store: store},
	})
	if err != nil {
		return err
	}
	core, err := api.NewCoreHandlers(api.CoreOptions{
		Store:     store,
		Admission: apiAdmission{placer: placer},
		Logger:    logger,
	})
	if err != nil {
		return err
	}
	lifecycleRoutes, err := api.NewLifecycleHandlers(api.LifecycleOptions{
		Store:  store,
		Resume: apiResumeAdmission{scheduler: placer},
		Logger: logger,
	})
	if err != nil {
		return err
	}
	saved, err := api.NewSavedSnapshotHandlers(api.SavedSnapshotOptions{Store: store, Logger: logger})
	if err != nil {
		return err
	}
	lookup := commandTargets{store: store}
	clients := gatewayAPICommands(factory)
	commandOptions := api.CommandsOptions{
		Lookup:           lookup,
		Clients:          clients,
		Store:            store,
		Logger:           logger,
		StreamMaxSeconds: cfg.Commands.StreamMaxSeconds,
	}
	commands, err := api.NewCommandsHandler(commandOptions)
	if err != nil {
		return err
	}
	status, err := api.NewCommandStatusHandler(commandOptions)
	if err != nil {
		return err
	}
	attach, err := api.NewCommandAttachHandler(api.CommandAttachOptions{CommandsOptions: commandOptions})
	if err != nil {
		return err
	}
	stop, err := api.NewCommandStopHandler(commandOptions)
	if err != nil {
		return err
	}
	files, err := api.NewFilesHandlers(api.FilesOptions{
		Lookup:  lookup,
		Clients: clients,
		Logger:  logger,
	})
	if err != nil {
		return err
	}
	ssh, err := api.NewSSHKeyHandler(api.SSHKeyOptions{
		Store:   store,
		Clients: clients,
		Logger:  logger,
	})
	if err != nil {
		return err
	}
	info, err := api.NewInfoHandler(api.InfoOptions{
		Version:          version.Version,
		StreamMaxSeconds: cfg.Commands.StreamMaxSeconds,
		Detached:         true,
	})
	if err != nil {
		return err
	}
	uploadKey, err := DeriveUploadSigningKey(cfg.Secrets.MasterKey)
	if err != nil {
		return err
	}
	downloadKey, err := DeriveDownloadSigningKey(cfg.Secrets.MasterKey)
	if err != nil {
		return err
	}

	protected := http.NewServeMux()
	api.Register(protected, core)
	api.Register(protected, lifecycleRoutes)
	api.RegisterSavedSnapshots(protected, saved)
	protected.HandleFunc("POST "+api.CommandsPath, commands.ServeHTTP)
	protected.HandleFunc("GET "+api.ProcessStatusPath, status.ServeHTTP)
	protected.HandleFunc("GET "+api.CommandAttachPath, attach.ServeHTTP)
	protected.HandleFunc("DELETE "+api.ProcessStatusPath, stop.ServeHTTP)
	api.RegisterFiles(protected, files)
	protected.Handle("POST "+api.PathSandboxSSHKey, ssh)

	bearer := func(next http.Handler) http.Handler {
		return keys.Middleware(mapping.Middleware(next))
	}

	root := http.NewServeMux()
	root.HandleFunc("GET "+HealthPath, health)
	root.Handle("/api/v1/", bearer(safeDefaults(protected)))
	root.Handle("GET "+api.PathInfo, bearer(info))
	if options.APIHooks.RegisterUploads != nil {
		if err := options.APIHooks.RegisterUploads(root, bearer, UploadDeps{
			Lookup:     lookup,
			Clients:    clients,
			SigningKey: uploadKey[:],
			BaseURL:    cfg.Server.PublicURL,
			Logger:     logger,
		}); err != nil {
			return err
		}
	} else {
		uploads, err := api.NewUploadsHandlers(api.UploadsOptions{
			Lookup:     lookup,
			Clients:    clients,
			SigningKey: uploadKey[:],
			BaseURL:    cfg.Server.PublicURL,
			Logger:     logger,
		})
		if err != nil {
			return err
		}
		root.Handle("POST "+api.UploadsPath, bearer(uploads.Issue))
		root.Handle("PUT "+api.UploadReceiverPath, uploads.Receive)
	}
	downloads, err := api.NewDownloadsHandlers(api.DownloadsOptions{
		Lookup:     lookup,
		Clients:    clients,
		SigningKey: downloadKey[:],
		BaseURL:    cfg.Server.PublicURL,
		Logger:     logger,
	})
	if err != nil {
		return err
	}
	root.Handle("POST "+api.DownloadsPath, bearer(downloads.Issue))
	root.Handle("GET "+api.DownloadReceiverPath, downloads.Receive)

	tracker := newRequestTracker()
	handler := tracker.wrap(apierr.RequestID(api.NoStore(safeDefaults(root))))
	requestCtx, cancelRequests := context.WithCancel(ctx)
	defer cancelRequests()
	listener, err := roleListener(ctx, options.Listeners.API, cfg.Server.Listen)
	if err != nil {
		return err
	}
	if cfg.Server.TLSCertFile != "" {
		listener, err = tlsListener(listener, cfg.Server.TLSCertFile, cfg.Server.TLSKeyFile)
		if err != nil {
			return err
		}
	}
	return serveHTTP(ctx, logger, &http.Server{
		Handler:           handler,
		ReadHeaderTimeout: httpReadHeaderTimeout,
		IdleTimeout:       httpIdleTimeout,
		MaxHeaderBytes:    httpMaxHeaderBytes,
		BaseContext:       func(net.Listener) context.Context { return requestCtx },
		ErrorLog:          slog.NewLogLogger(logger.Handler(), slog.LevelWarn),
	}, listener, tracker, cancelRequests, shutdownTimeout)
}

// runLifecycle starts the durable job workers with a private store and a
// gateway dialer. Handlers are fixed at construction: the saved-snapshot
// plugin supplies its map and the snapshot reconciler runs beside the workers
// under the same role context.
func runLifecycle(ctx context.Context, cfg *config.Config, logger *slog.Logger, openStore StoreOpener) error {
	store, err := openStore(ctx, config.RoleLifecycle)
	if err != nil {
		return err
	}
	defer store.Close()

	gatewayTLS, err := gatewayClientTLS(ctx, &cfg.Node)
	if err != nil {
		return err
	}
	factory := nodegateway.NewClientFactory(cfg.Node.GatewayDial, cfg.Node.GatewayToken, gatewayTLS)
	dial := gatewayLifecycleDial(factory)
	plugin, err := snapshotsplugin.New(snapshotsplugin.Options{
		Store:  store,
		Dial:   dial,
		Logger: logger,
	})
	if err != nil {
		return err
	}
	reconciler, err := snapshotsplugin.NewReconciler(snapshotsplugin.ReconcilerOptions{
		Store:  store,
		Dial:   dial,
		Logger: logger,
	})
	if err != nil {
		return err
	}
	worker, err := lifecycle.New(lifecycle.Options{
		Store:            store,
		Dial:             dial,
		Logger:           logger,
		RetryBase:        seconds(cfg.Lifecycle.RetrySeconds),
		JobPoll:          seconds(cfg.Lifecycle.JobPollSeconds),
		ReaperInterval:   seconds(cfg.Lifecycle.ReaperSeconds),
		StoppedRetention: cfg.Lifecycle.StoppedRetentionOption(),
		Handlers:         plugin.Handlers(),
	})
	if err != nil {
		return err
	}

	runCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	errCh := make(chan error, 2)
	go func() { errCh <- worker.Run(runCtx) }()
	go func() { errCh <- reconciler.Run(runCtx) }()
	var first error
	for range 2 {
		if err := <-errCh; err != nil && first == nil {
			first = err
			cancel(err)
		}
	}
	return first
}

// runNodeHub starts the node link listener, the authenticated internal
// gateway and the inventory poller. The hub exists only in this role.
func runNodeHub(ctx context.Context, cfg *config.Config, logger *slog.Logger, options Options, openStore StoreOpener) error {
	store, err := openStore(ctx, config.RoleNodeHub)
	if err != nil {
		return err
	}
	defer store.Close()

	tokens, err := auth.LoadNodeTokensFile(ctx, cfg.Node.TokensFile)
	if err != nil {
		return err
	}
	hub := nodelink.NewHub(tokens.Authenticator(), logger)
	gateway, err := nodegateway.New(hub, cfg.Node.GatewayToken)
	if err != nil {
		return err
	}
	nodeListener, err := roleListener(ctx, options.Listeners.Node, cfg.Node.Listen)
	if err != nil {
		return err
	}
	defer nodeListener.Close()
	if cfg.Node.TLSCertFile != "" {
		nodeListener, err = tlsListener(nodeListener, cfg.Node.TLSCertFile, cfg.Node.TLSKeyFile)
		if err != nil {
			return err
		}
	}
	gatewayListener, err := roleListener(ctx, options.Listeners.Gateway, cfg.Node.GatewayListen)
	if err != nil {
		return err
	}
	defer gatewayListener.Close()
	var gatewayOptions []grpc.ServerOption
	if cfg.Node.GatewayTLSCertFile != "" {
		creds, err := nodegateway.ServerCredentials(ctx, cfg.Node.GatewayTLSCertFile, cfg.Node.GatewayTLSKeyFile)
		if err != nil {
			return err
		}
		gatewayOptions = append(gatewayOptions, grpc.Creds(creds))
	}
	inventory, err := nodegateway.NewInventory(hub, inventoryWriter{store: store, now: time.Now}, logger)
	if err != nil {
		return err
	}
	if options.InventoryInterval > 0 {
		inventory.Interval = options.InventoryInterval
	}

	runCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	errCh := make(chan error, 3)
	run := func(task func(context.Context) error) {
		go func() { errCh <- task(runCtx) }()
	}
	run(func(ctx context.Context) error { return hub.Serve(ctx, nodeListener) })
	run(func(ctx context.Context) error { return gateway.Serve(ctx, gatewayListener, gatewayOptions...) })
	run(inventory.Run)

	var first error
	for range 3 {
		if err := <-errCh; err != nil && first == nil {
			first = err
			cancel(err)
		}
	}
	return first
}

// roleListener uses a provided test listener or binds the configured
// address with the run context.
func roleListener(ctx context.Context, provided net.Listener, addr string) (net.Listener, error) {
	if provided != nil {
		return provided, nil
	}
	var lc net.ListenConfig
	return lc.Listen(ctx, "tcp", addr)
}

// tlsListener wraps ln with the certificate pair. A malformed pair closes
// ln so startup failure leaks no socket.
func tlsListener(ln net.Listener, certFile, keyFile string) (net.Listener, error) {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		ln.Close()
		return nil, fmt.Errorf("server: load TLS pair: %w", err)
	}
	return tls.NewListener(ln, &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	}), nil
}

// gatewayClientTLS builds the api/lifecycle trust for the internal gateway.
// Nil means a plaintext loopback gateway. A configured pair always verifies
// the certificate; InsecureSkipVerify is never set.
func gatewayClientTLS(ctx context.Context, node *config.Node) (*tls.Config, error) {
	if node.GatewayTLSCertFile == "" {
		return nil, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	conf := &tls.Config{MinVersion: tls.VersionTLS12}
	if node.GatewayCACertFile != "" {
		pem, err := os.ReadFile(node.GatewayCACertFile)
		if err != nil {
			return nil, fmt.Errorf("server: gateway CA: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, errors.New("server: gateway CA file has no certificates")
		}
		conf.RootCAs = pool
	}
	if node.GatewayServerName != "" {
		conf.ServerName = node.GatewayServerName
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return conf, nil
}

// requestTracker counts live HTTP handlers so shutdown joins them instead of
// trusting Serve's return (Close does not join ServeHTTP goroutines). stop
// gates new entries before the wait begins, so no Add races with Wait.
type requestTracker struct {
	mu     sync.Mutex
	active int
	closed bool
	idle   chan struct{} // non-nil after stop; closed when active reaches 0
}

func newRequestTracker() *requestTracker { return &requestTracker{} }

// wrap counts every request for the whole handler lifetime.
func (t *requestTracker) wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !t.enter() {
			http.Error(w, "server is shutting down", http.StatusServiceUnavailable)
			return
		}
		defer t.leave()
		next.ServeHTTP(w, r)
	})
}

func (t *requestTracker) enter() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return false
	}
	t.active++
	return true
}

func (t *requestTracker) leave() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.active--
	if t.closed && t.active == 0 && t.idle != nil {
		close(t.idle)
		t.idle = nil
	}
}

// stop refuses new requests and returns a channel closed when every tracked
// handler has returned.
func (t *requestTracker) stop() <-chan struct{} {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.closed = true
	if t.active == 0 {
		done := make(chan struct{})
		close(done)
		return done
	}
	t.idle = make(chan struct{})
	return t.idle
}

// serveHTTP serves until ctx ends or the listener fails, then stops new
// requests, bounds graceful shutdown, force-closes stragglers and waits for
// every tracked handler before returning. A graceful timeout only triggers
// the force close; it never abandons handlers.
func serveHTTP(ctx context.Context, logger *slog.Logger, srv *http.Server, ln net.Listener, tracker *requestTracker, cancelRequests context.CancelFunc, shutdownTimeout time.Duration) error {
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(ln) }()
	select {
	case err := <-serveErr:
		// The listener failed without a shutdown request: cancel every
		// request context, refuse new requests, close active connections
		// and join their handlers.
		idle := tracker.stop()
		cancelRequests()
		_ = srv.Close()
		<-idle
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
		idle := tracker.stop()
		cancelRequests()
		shutdownCtx, stop := context.WithTimeout(context.WithoutCancel(ctx), shutdownTimeout)
		err := srv.Shutdown(shutdownCtx)
		stop()
		if err != nil {
			logger.Warn("api: graceful shutdown timed out; closing", "error", safeErrorName(err))
			_ = srv.Close()
		}
		// Join every handler unconditionally. Production handlers observe
		// the canceled request context or the force-closed body; a handler
		// that does not is a bug, never a reason to close its store under
		// it.
		<-idle
		if err := <-serveErr; err != nil && !errors.Is(err, http.ErrServerClosed) && !errors.Is(err, net.ErrClosed) {
			return err
		}
		return nil
	}
}

// seconds converts a positive configured interval to a duration.
func seconds(n int) time.Duration { return time.Duration(n) * time.Second }

// safeDefaults converts the ServeMux's unmatched-path and wrong-method
// answers into the API error envelope. Matched routes pass through
// untouched, so streaming and handlers that already write envelopes keep
// full control.
func safeDefaults(mux *http.ServeMux) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handler, pattern := mux.Handler(r)
		if pattern != "" {
			mux.ServeHTTP(w, r)
			return
		}
		buffered := &bufferedResponse{header: make(http.Header)}
		handler.ServeHTTP(buffered, r)
		switch buffered.status {
		case http.StatusNotFound:
			apierr.WriteError(w, r, apierr.New(http.StatusNotFound, apierr.CodeNotFound, "not found"))
		case http.StatusMethodNotAllowed:
			if allow := buffered.header.Get("Allow"); allow != "" {
				w.Header().Set("Allow", allow)
			}
			apierr.WriteError(w, r, apierr.New(http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed"))
		default:
			buffered.flush(w)
		}
	})
}

// bufferedResponse captures the small default error answers so they can be
// replaced by an envelope.
type bufferedResponse struct {
	header http.Header
	status int
	body   []byte
}

func (b *bufferedResponse) Header() http.Header { return b.header }

func (b *bufferedResponse) WriteHeader(status int) {
	if b.status == 0 {
		b.status = status
	}
}

func (b *bufferedResponse) Write(p []byte) (int, error) {
	if b.status == 0 {
		b.status = http.StatusOK
	}
	b.body = append(b.body, p...)
	return len(p), nil
}

// flush copies the captured response to the real writer.
func (b *bufferedResponse) flush(w http.ResponseWriter) {
	for key, values := range b.header {
		for _, value := range values {
			w.Header().Add(key, value)
		}
	}
	status := b.status
	if status == 0 {
		status = http.StatusOK
	}
	w.WriteHeader(status)
	_, _ = w.Write(b.body)
}
