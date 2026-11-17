// Command bbraft-server runs the bamboo-raft control plane. With -roles it
// can run every role in one process or exactly one role per process; all
// deployments need PostgreSQL and the internal gateway credential.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/nathabonfim59/bamboo-raft/internal/server"
	"github.com/nathabonfim59/bamboo-raft/internal/server/config"
	"github.com/nathabonfim59/bamboo-raft/internal/version"
)

// usage is the short help printed for -h/-help. Every flag has a BBRAFT_*
// environment fallback; docs/server.md lists them with defaults.
const usage = `usage: bbraft-server [flags]

Control plane for bamboo-raft. Flags take BBRAFT_* environment fallbacks;
secrets come from files or the environment, never flags.
See docs/server.md for the full list.

  -roles all|api|lifecycle|nodehub   roles to run (default all)
  -server.listen host:port           public API listener
  -server.public-url URL             trusted base URL for signed links
  -node.listen host:port             node link listener
  -node.gateway-listen host:port     internal gateway listener
  -node.gateway-dial addr            internal gateway dial address
  -database.url-file path            PostgreSQL connection URL file
  -auth.keys-file path               API key file
  -node.tokens-file path             node token digest file
  -node.gateway-token-file path      internal gateway credential file
  -secrets.master-key-file path      installation master key
`

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "bbraft-server: %v\n", err)
		os.Exit(1)
	}
}

// run loads the configuration and serves until SIGINT/SIGTERM or a role
// failure. Errors never include secrets because config and roles keep them
// out of error text.
func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg, err := config.Load(ctx, os.Args[1:], os.LookupEnv)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			fmt.Fprint(os.Stdout, usage)
			return nil
		}
		return err
	}

	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	logger.Info("bbraft-server starting", "version", version.Version, "roles", cfg.Roles.String())
	if err := server.Run(ctx, cfg, server.Options{Logger: logger}); err != nil {
		return err
	}
	logger.Info("bbraft-server stopped")
	return nil
}
