package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/spf13/cobra"

	"github.com/nathabonfim59/bamboo-raft/internal/api"
)

// maxAPIKeyBytes bounds the key read from stdin. Real keys are far shorter;
// the bound keeps a mistaken pipe from exhausting memory.
const maxAPIKeyBytes = 8 << 10

// loginEvent is the JSON object printed by login. It never contains the key.
type loginEvent struct {
	Event  string     `json:"event"`
	APIURL string     `json:"apiUrl"`
	User   api.MeUser `json:"user"`
}

// newLoginCommand builds `bbraft login --key-stdin`.
func newLoginCommand(a *App) *cobra.Command {
	var keyStdin bool
	cmd := &cobra.Command{
		Use:   "login",
		Short: "Sign in with an API key read from standard input",
		Args:  cobra.NoArgs,
		RunE: a.runE(func(ctx context.Context, _ []string) error {
			if !keyStdin {
				return &UsageError{Err: errors.New("login reads the API key from standard input: pass --key-stdin")}
			}
			key, err := readAPIKey(ctx, a.opts.Stdin)
			if err != nil {
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					return err
				}
				return &UsageError{Err: err}
			}
			cfg, err := a.resolveConfig(ctx)
			if err != nil {
				return err
			}
			cfg.APIKey = key
			client, err := NewClient(cfg.APIURL, key, a.opts.HTTPClient)
			if err != nil {
				return err
			}
			me, err := client.Me(ctx)
			if err != nil {
				return err
			}
			dir, err := a.configDir()
			if err != nil {
				return err
			}
			if err := SaveConfig(ctx, dir, cfg); err != nil {
				return err
			}
			if a.out.JSON {
				return a.out.JSONLine(loginEvent{Event: "logged_in", APIURL: cfg.APIURL, User: me.User})
			}
			name := me.User.Login
			if name == "" {
				name = me.User.DisplayName
			}
			if name == "" {
				name = me.User.ID
			}
			return a.out.Humanf("logged in as %s (%s)", name, cfg.APIURL)
		}),
	}
	cmd.Flags().BoolVar(&keyStdin, "key-stdin", false, "read the API key from standard input")
	return cmd
}

// readAPIKey reads one key from r. Surrounding whitespace from a here-string
// or echo is trimmed; whitespace inside the key is refused because it is
// never part of a valid token. readWithContext closes and joins a closable
// reader when the context ends, so Ctrl-C does not wait on a blocked stdin.
func readAPIKey(ctx context.Context, r io.Reader) (string, error) {
	var raw []byte
	err := readWithContext(ctx, r, func(reader io.Reader) error {
		var readErr error
		raw, readErr = io.ReadAll(io.LimitReader(reader, maxAPIKeyBytes+1))
		return readErr
	})
	if err != nil {
		if ctx.Err() != nil {
			return "", ctx.Err()
		}
		return "", errors.New("read api key failed")
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if len(raw) > maxAPIKeyBytes {
		return "", fmt.Errorf("api key is longer than %d bytes", maxAPIKeyBytes)
	}
	key := strings.TrimSpace(string(raw))
	if key == "" {
		return "", errors.New("api key from standard input is empty")
	}
	if strings.ContainsAny(key, " \t\r\n") {
		return "", errors.New("api key from standard input contains whitespace")
	}
	return key, nil
}
