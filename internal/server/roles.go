package server

import (
	"cmp"
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
	"github.com/nathabonfim59/bamboo-raft/internal/gc"
	"github.com/nathabonfim59/bamboo-raft/internal/lifecycle"
	snapshotsplugin "github.com/nathabonfim59/bamboo-raft/internal/lifecycle/snapshots"
	"github.com/nathabonfim59/bamboo-raft/internal/nodelink"
	"github.com/nathabonfim59/bamboo-raft/internal/objstore"
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
//
// objectStore reports whether an object store is configured; it enables
// resume on another node and fork (both need a generation), and its absence
// makes the fork handler answer 409 snapshots_disabled.
func runAPI(ctx context.Context, cfg *config.Config, logger *slog.Logger, options Options, openStore StoreOpener, shutdownTimeout time.Duration, objectStore bool) error {
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
	schedOpts := scheduler.Options{
		Store:   store,
		Dial:    gatewaySchedulerDial(factory),
		Log:     logger,
		Restore: snapshotRestoreResolver{store: store},
	}
	// with an object store, an archived sandbox whose disk node
	// cannot take it resumes on another node from its last generation.
	if objectStore {
		schedOpts.ResumeElsewhere = store
	}
	placer, err := scheduler.New(schedOpts)
	if err != nil {
		return err
	}
	core, err := api.NewCoreHandlers(api.CoreOptions{
		Store:     store,
		Admission: apiAdmission{placer: placer, batches: placer},
		Batch:     apiAdmission{placer: placer, batches: placer},
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
	snapshotShares, err := api.NewSnapshotShareHandlers(api.SnapshotShareOptions{Store: store, Logger: logger})
	if err != nil {
		return err
	}
	snapshotListings, err := api.NewGenerationHandlers(api.GenerationOptions{Store: store, Logger: logger})
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
	recoverSandbox, err := api.NewRecoverHandler(api.RecoverOptions{
		Store:   store,
		Clients: clients,
		Logger:  logger,
	})
	if err != nil {
		return err
	}
	// A fork is admitted through placement and the store; without an object
	// store the handler refuses it (409 snapshots_disabled).
	forkSandbox, err := api.NewForkHandler(api.ForkOptions{
		Store:       store,
		Admission:   apiForkAdmission{placer: placer, store: store},
		ObjectStore: objectStore,
		Logger:      logger,
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
	api.Register(protected, api.Handlers{RecoverSandbox: recoverSandbox})
	api.Register(protected, api.Handlers{ForkSandbox: forkSandbox})
	api.RegisterSavedSnapshots(protected, saved)
	api.RegisterSnapshotShares(protected, snapshotShares)
	api.Register(protected, snapshotListings)
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
func runLifecycle(ctx context.Context, cfg *config.Config, logger *slog.Logger, openStore StoreOpener, objects objstore.Store) error {
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
	// The plugin exists before the worker (it supplies the handler map); a
	// generation-backed save reaches the worker's CutGeneration through this
	// late-bound adapter. Without an object store the node-local path stays.
	cutter := &snapshotCutter{}
	pluginOptions := snapshotsplugin.Options{
		Store:  store,
		Dial:   dial,
		Logger: logger,
	}
	if objects != nil {
		pluginOptions.Cutter = cutter
	}
	plugin, err := snapshotsplugin.New(pluginOptions)
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
	// the provision job re-places a manifest restore refused by a
	// node without an object store, through the same gateway factory.
	placer, err := scheduler.New(scheduler.Options{
		Store: store,
		Dial:  gatewaySchedulerDial(factory),
		Log:   logger,
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
		DataRetention:    cfg.Lifecycle.DataRetentionOption(),
		Handlers:         plugin.Handlers(),
		ObjectStore:      objects,
		SnapshotInterval: seconds(cfg.Lifecycle.SnapshotIntervalSeconds),
		SnapshotTimeout:  seconds(cfg.Lifecycle.SnapshotTimeoutSeconds),
		Placer:           placer,
		NodeLostAfter:    seconds(cfg.Lifecycle.NodeLostSeconds),
		DiskLowPercent:   cfg.Lifecycle.DiskLowPercent,
	})
	if err != nil {
		return err
	}
	cutter.worker = worker

	// The chunk collector runs only with an object store: without one no
	// generation exists to collect.
	runners := 2
	var collector *gc.Collector
	if objects != nil {
		collector, err = gc.New(gc.Options{
			Store:           store,
			Objects:         objects,
			Logger:          logger,
			Interval:        seconds(cmp.Or(cfg.GC.IntervalSeconds, config.DefaultGCIntervalSeconds)),
			Grace:           seconds(cmp.Or(cfg.GC.GraceSeconds, config.DefaultGCGraceSeconds)),
			History:         seconds(cmp.Or(cfg.GC.HistorySeconds, config.DefaultGCHistorySeconds)),
			Batch:           cmp.Or(cfg.GC.Batch, config.DefaultGCBatch),
			SnapshotTimeout: seconds(cfg.Lifecycle.SnapshotTimeoutSeconds),
		})
		if err != nil {
			return err
		}
		runners++
	}

	runCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	errCh := make(chan error, runners)
	go func() { errCh <- worker.Run(runCtx) }()
	go func() { errCh <- reconciler.Run(runCtx) }()
	if collector != nil {
		go func() { errCh <- collector.Run(runCtx) }()
	}
	var first error
	for range runners {
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
