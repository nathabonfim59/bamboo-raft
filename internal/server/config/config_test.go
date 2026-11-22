package config

import (
	"context"
	"crypto/subtle"
	"errors"
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// testEnv builds the injectable environment from a map.
func testEnv(m map[string]string) func(string) (string, bool) {
	return func(k string) (string, bool) {
		v, ok := m[k]
		return v, ok
	}
}

func noEnv(string) (string, bool) { return "", false }

// fixture writes the files a valid config needs and returns their paths.
type fixture struct {
	dir        string
	keysFile   string
	tokensFile string
	dbURLFile  string
	gwFile     string
	certFile   string
	keyFile    string
}

func newFixture(t *testing.T) fixture {
	t.Helper()
	dir := t.TempDir()
	f := fixture{dir: dir}
	write := func(name, content string) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	f.keysFile = write("keys.json", `{"keys":[{"owner":"a","key":"k"}]}`)
	f.tokensFile = write("tokens.json", `{"nodes":[{"id":"n","tokenSha256":"`+strings.Repeat("ab", 32)+`"}]}`)
	f.dbURLFile = write("db.url", "postgres://localhost/bbraft\n")
	f.gwFile = write("gw.token", "gateway-shared-secret\n")
	f.certFile = write("cert.pem", "not a real cert; config only checks existence")
	f.keyFile = write("key.pem", "not a real key")
	return f
}

// fileArgs returns the flags pointing at the fixture files, without a
// secrets master key source.
func (f fixture) fileArgs() []string {
	return []string{
		"-auth.keys-file", f.keysFile,
		"-node.tokens-file", f.tokensFile,
		"-database.url-file", f.dbURLFile,
		"-node.gateway-token-file", f.gwFile,
	}
}

// baseArgs returns the flags every valid all-roles Load needs. It is a
// fresh literal with no spare capacity: the malformed table appends to
// it for several cases before running any of them.
func (f fixture) baseArgs() []string {
	return []string{
		"-auth.keys-file", f.keysFile,
		"-node.tokens-file", f.tokensFile,
		"-database.url-file", f.dbURLFile,
		"-node.gateway-token-file", f.gwFile,
		"-secrets.allow-ephemeral",
	}
}

func TestLoadPublicURLErrorKeepsSecrets(t *testing.T) {
	f := newFixture(t)
	const sentinel = "SENTINELurlSecret"
	for _, raw := range []string{
		"https://user:" + sentinel + "@example.test",
		"https://example.test/?token=" + sentinel,
		"https://example.test/#" + sentinel,
	} {
		_, err := Load(t.Context(), append(f.baseArgs(), "-server.public-url", raw), noEnv)
		if err == nil {
			t.Fatal("accepted a credential-bearing public URL")
		}
		if strings.Contains(err.Error(), sentinel) || strings.Contains(err.Error(), raw) {
			t.Fatal("error leaks the rejected URL")
		}
	}
}

// Keep the existing secret-dump guards:
func TestLoadDefaults(t *testing.T) {
	f := newFixture(t)
	c, err := Load(t.Context(), f.baseArgs(), noEnv)
	if err != nil {
		t.Fatal(err)
	}
	if c.Server.Listen != "127.0.0.1:7700" {
		t.Fatalf("server.listen = %q", c.Server.Listen)
	}
	if c.Node.Listen != "127.0.0.1:7701" {
		t.Fatalf("node.listen = %q", c.Node.Listen)
	}
	if c.Node.GatewayListen != "127.0.0.1:7702" {
		t.Fatalf("node.gateway-listen = %q", c.Node.GatewayListen)
	}
	if c.Node.GatewayDial != "127.0.0.1:7702" {
		t.Fatalf("node.gateway-dial = %q", c.Node.GatewayDial)
	}
	if c.Server.PublicURL != "http://127.0.0.1:7700" {
		t.Fatalf("server.public-url = %q", c.Server.PublicURL)
	}
	if c.Commands.StreamMaxSeconds != 7200 {
		t.Fatalf("stream-max = %d", c.Commands.StreamMaxSeconds)
	}
	if c.Auth.KeyPrefix != "bmb_" {
		t.Fatalf("key-prefix = %q", c.Auth.KeyPrefix)
	}
	if !c.Roles.Has(RoleAPI) || !c.Roles.Has(RoleLifecycle) || !c.Roles.Has(RoleNodeHub) {
		t.Fatalf("roles = %v", c.Roles)
	}
	if subtle.ConstantTimeCompare([]byte(c.Database.URL), []byte("postgres://localhost/bbraft")) != 1 {
		t.Fatal("database URL mismatch")
	}
	if subtle.ConstantTimeCompare([]byte(c.Node.GatewayToken), []byte("gateway-shared-secret")) != 1 {
		t.Fatal("gateway token mismatch")
	}
	if c.Lifecycle.RetrySeconds != 5 || c.Lifecycle.ReaperSeconds != 60 || c.Lifecycle.JobPollSeconds != 15 {
		t.Fatalf("lifecycle = %+v", c.Lifecycle)
	}
	if c.Lifecycle.StoppedRetentionSeconds != 604800 {
		t.Fatalf("stopped-retention default = %d", c.Lifecycle.StoppedRetentionSeconds)
	}
	if opt := c.Lifecycle.StoppedRetentionOption(); opt == nil || *opt != 7*24*time.Hour {
		t.Fatalf("stopped-retention option = %v", opt)
	}
	if c.Lifecycle.DataRetentionSeconds != 1209600 {
		t.Fatalf("data-retention default = %d", c.Lifecycle.DataRetentionSeconds)
	}
	if opt := c.Lifecycle.DataRetentionOption(); opt == nil || *opt != 14*24*time.Hour {
		t.Fatalf("data-retention option = %v", opt)
	}
}

// TestLoadDataRetentionDefaults checks the two retention windows together:
// without an object store today's seven-day stopped-disk deletion stays, with
// one the row is kept and only the data expires, so row deletion is off unless
// the operator asked for it explicitly.
func TestLoadDataRetentionDefaults(t *testing.T) {
	f := newFixture(t)
	secretFile := s3TestSecretFile(t, "s3-secret\n")
	withStore := func(t *testing.T, extra ...string) *Config {
		t.Helper()
		c, err := Load(t.Context(), append(append(f.baseArgs(), s3TestFlags(secretFile)...), extra...), noEnv)
		if err != nil {
			t.Fatal(err)
		}
		return c
	}

	plain, err := Load(t.Context(), f.baseArgs(), noEnv)
	if err != nil {
		t.Fatal(err)
	}
	if plain.ObjectStore != nil || plain.Lifecycle.StoppedRetentionSeconds != 604800 {
		t.Fatalf("phase 1 defaults: store=%v stopped=%d", plain.ObjectStore, plain.Lifecycle.StoppedRetentionSeconds)
	}
	if opt := plain.Lifecycle.DataRetentionOption(); opt == nil || *opt != 14*24*time.Hour {
		t.Fatalf("phase 1 data-retention option = %v", opt)
	}

	stored := withStore(t)
	if stored.ObjectStore == nil {
		t.Fatal("object store not configured")
	}
	if stored.Lifecycle.StoppedRetentionSeconds != 0 {
		t.Fatalf("stopped-retention with an object store = %d, want 0 (off)", stored.Lifecycle.StoppedRetentionSeconds)
	}
	if opt := stored.Lifecycle.DataRetentionOption(); opt == nil || *opt != 14*24*time.Hour {
		t.Fatalf("data-retention option with an object store = %v", opt)
	}

	if got := withStore(t, "-lifecycle.stopped-retention-seconds", "3600").Lifecycle.StoppedRetentionSeconds; got != 3600 {
		t.Fatalf("explicit stopped-retention with an object store = %d", got)
	}
	env := testEnv(map[string]string{
		"BBRAFT_AUTH_KEYS_FILE":                      f.keysFile,
		"BBRAFT_NODE_TOKENS_FILE":                    f.tokensFile,
		"BBRAFT_DATABASE_URL_FILE":                   f.dbURLFile,
		"BBRAFT_NODE_GATEWAY_TOKEN_FILE":             f.gwFile,
		"BBRAFT_SECRETS_ALLOW_EPHEMERAL":             "true",
		"BBRAFT_LIFECYCLE_STOPPED_RETENTION_SECONDS": "7200",
		"BBRAFT_LIFECYCLE_DATA_RETENTION_SECONDS":    "60",
		"BBRAFT_S3_ENDPOINT":                         "http://s3.example.test:3900",
		"BBRAFT_S3_BUCKET":                           "bbraft",
		"BBRAFT_S3_ACCESS_KEY_ID":                    "GKEXAMPLE",
		"BBRAFT_S3_SECRET_ACCESS_KEY":                "env-s3-secret",
	})
	fromEnv, err := Load(t.Context(), nil, env)
	if err != nil {
		t.Fatal(err)
	}
	if fromEnv.Lifecycle.StoppedRetentionSeconds != 7200 || fromEnv.Lifecycle.DataRetentionSeconds != 60 {
		t.Fatalf("explicit retention from env: stopped=%d data=%d",
			fromEnv.Lifecycle.StoppedRetentionSeconds, fromEnv.Lifecycle.DataRetentionSeconds)
	}

	off := withStore(t, "-lifecycle.data-retention-seconds", "0")
	if opt := off.Lifecycle.DataRetentionOption(); opt == nil || *opt != 0 {
		t.Fatalf("disabled data-retention option = %v", opt)
	}
}

func TestLoadEnvFallback(t *testing.T) {
	f := newFixture(t)
	env := testEnv(map[string]string{
		"BBRAFT_AUTH_KEYS_FILE":                      f.keysFile,
		"BBRAFT_NODE_TOKENS_FILE":                    f.tokensFile,
		"BBRAFT_DATABASE_URL_FILE":                   f.dbURLFile,
		"BBRAFT_NODE_GATEWAY_TOKEN_FILE":             f.gwFile,
		"BBRAFT_NODE_LISTEN":                         "10.0.0.5:9999",
		"BBRAFT_NODE_GATEWAY_DIAL":                   "127.0.0.1:9702",
		"BBRAFT_SERVER_PUBLIC_URL":                   "https://bbraft.example.test",
		"BBRAFT_COMMANDS_STREAM_MAX_SECONDS":         "9000",
		"BBRAFT_ROLES":                               "api",
		"BBRAFT_SECRETS_ALLOW_EPHEMERAL":             "true",
		"BBRAFT_LIFECYCLE_STOPPED_RETENTION_SECONDS": "0",
	})
	c, err := Load(t.Context(), nil, env)
	if err != nil {
		t.Fatal(err)
	}
	if c.Lifecycle.StoppedRetentionSeconds != 0 {
		t.Fatalf("stopped-retention from env = %d, want 0 (never)", c.Lifecycle.StoppedRetentionSeconds)
	}
	if opt := c.Lifecycle.StoppedRetentionOption(); opt == nil || *opt != 0 {
		t.Fatalf("disabled stopped-retention option = %v", opt)
	}
	if c.Node.Listen != "10.0.0.5:9999" {
		t.Fatalf("node.listen = %q", c.Node.Listen)
	}
	if c.Node.GatewayDial != "127.0.0.1:9702" || c.Server.PublicURL != "https://bbraft.example.test" {
		t.Fatal("new listener settings did not come from the environment")
	}
	if c.Commands.StreamMaxSeconds != 9000 {
		t.Fatalf("stream-max = %d", c.Commands.StreamMaxSeconds)
	}
	if c.Roles.Has(RoleNodeHub) || !c.Roles.Has(RoleAPI) {
		t.Fatalf("roles = %v", c.Roles)
	}
	// The flag still wins over the environment.
	c, err = Load(t.Context(), []string{
		"-node.listen", "127.0.0.1:7701",
		"-lifecycle.stopped-retention-seconds", "3600",
	}, env)
	if err != nil {
		t.Fatal(err)
	}
	if c.Node.Listen != "127.0.0.1:7701" {
		t.Fatalf("flag did not override env: %q", c.Node.Listen)
	}
	if c.Lifecycle.StoppedRetentionSeconds != 3600 {
		t.Fatalf("stopped-retention flag override = %d", c.Lifecycle.StoppedRetentionSeconds)
	}
	if opt := c.Lifecycle.StoppedRetentionOption(); opt == nil || *opt != time.Hour {
		t.Fatalf("stopped-retention option after flag = %v", opt)
	}
	// A flag also wins for the new absolute/trusted settings.
	c, err = Load(t.Context(), []string{
		"-server.public-url", "http://flag.example.test",
		"-node.gateway-dial", "127.0.0.1:9703",
	}, env)
	if err != nil {
		t.Fatal(err)
	}
	if c.Server.PublicURL != "http://flag.example.test" || c.Node.GatewayDial != "127.0.0.1:9703" {
		t.Fatal("flags did not override public URL/gateway dial env")
	}
}

func TestLoadSecretFromEnv(t *testing.T) {
	f := newFixture(t)
	env := testEnv(map[string]string{
		EnvDatabaseURL:  "postgres://env/bbraft",
		EnvGatewayToken: "env-token",
	})
	c, err := Load(t.Context(), []string{
		"-auth.keys-file", f.keysFile,
		"-node.tokens-file", f.tokensFile,
		"-secrets.allow-ephemeral",
	}, env)
	if err != nil {
		t.Fatal(err)
	}
	if subtle.ConstantTimeCompare([]byte(c.Database.URL), []byte("postgres://env/bbraft")) != 1 {
		t.Fatal("database URL mismatch")
	}
	if subtle.ConstantTimeCompare([]byte(c.Node.GatewayToken), []byte("env-token")) != 1 {
		t.Fatal("gateway token mismatch")
	}
}

func TestLoadMalformed(t *testing.T) {
	f := newFixture(t)
	good := f.baseArgs()
	tests := []struct {
		name string
		args []string
		env  map[string]string
		want string // substring of the error
	}{
		{"unknown flag", append(good, "-bogus"), nil, "bogus"},
		{"positional arg", append(good, "extra"), nil, "unexpected arguments"},
		{"bad roles", append(good, "-roles", "bogus"), nil, "unknown role"},
		{"roles all mixed", append(good, "-roles", "all,api"), nil, "cannot mix"},
		{"duplicate role", append(good, "-roles", "api,api"), nil, "duplicate"},
		{"empty roles", append(good, "-roles", " "), nil, "empty"},
		{"bad listen", append(good, "-node.listen", "noport"), nil, "node.listen"},
		{"bad port", append(good, "-node.listen", "127.0.0.1:notaport"), nil, "port"},
		{"port zero", append(good, "-node.listen", "127.0.0.1:0"), nil, "port"},
		{"hostname host", append(good, "-node.listen", "example.com:7701"), nil, "IP literal"},
		{"int env", good, map[string]string{"BBRAFT_COMMANDS_STREAM_MAX_SECONDS": "many"}, "not an integer"},
		{"zero stream max", append(good, "-commands.stream-max-seconds", "0"), nil, "stream-max"},
		{"bad interval", append(good, "-lifecycle.reaper-seconds", "-3"), nil, "reaper"},
		{"negative stopped retention", append(good, "-lifecycle.stopped-retention-seconds", "-1"), nil, "stopped-retention"},
		{"stopped retention overflow", append(good, "-lifecycle.stopped-retention-seconds", "9223372037"), nil, "stopped-retention"},
		{"stopped retention not int", good, map[string]string{"BBRAFT_LIFECYCLE_STOPPED_RETENTION_SECONDS": "week"}, "not an integer"},
		{"negative data retention", append(good, "-lifecycle.data-retention-seconds", "-1"), nil, "data-retention"},
		{"data retention overflow", append(good, "-lifecycle.data-retention-seconds", "9223372037"), nil, "data-retention"},
		{"data retention not int", good, map[string]string{"BBRAFT_LIFECYCLE_DATA_RETENTION_SECONDS": "fortnight"}, "not an integer"},
		{"missing keys file", []string{"-roles", "api", "-database.url-file", f.dbURLFile, "-node.gateway-token-file", f.gwFile, "-secrets.allow-ephemeral"}, nil, "keys-file"},
		{"missing tokens file", []string{"-roles", "nodehub", "-database.url-file", f.dbURLFile, "-node.gateway-token-file", f.gwFile, "-secrets.allow-ephemeral"}, nil, "tokens-file"},
		{"missing database", []string{"-roles", "lifecycle", "-node.gateway-token-file", f.gwFile, "-secrets.allow-ephemeral"}, nil, "database"},
		{"nodehub needs database", []string{"-roles", "nodehub", "-node.tokens-file", f.tokensFile, "-node.gateway-token-file", f.gwFile, "-secrets.allow-ephemeral"}, nil, "database"},
		{"missing gateway token", []string{"-roles", "api", "-auth.keys-file", f.keysFile, "-database.url-file", f.dbURLFile, "-secrets.allow-ephemeral"}, nil, "gateway"},
		{"no secrets key", f.fileArgs(), nil, "secrets"},
		{"db conflict", append(good, "-database.url-file", f.dbURLFile), map[string]string{EnvDatabaseURL: "postgres://x"}, "both set"},
		{"gw conflict", good, map[string]string{EnvGatewayToken: "tok"}, "both set"},
		{"empty db env", []string{"-roles", "api", "-auth.keys-file", f.keysFile, "-node.gateway-token-file", f.gwFile, "-secrets.allow-ephemeral"}, map[string]string{EnvDatabaseURL: ""}, "empty"},
		{"missing file path", append(good, "-auth.keys-file", filepath.Join(f.dir, "gone.json")), nil, "gone.json"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			env := noEnv
			if tc.env != nil {
				env = testEnv(tc.env)
			}
			_, err := Load(t.Context(), tc.args, env)
			if err == nil {
				t.Fatalf("%s: malformed input accepted", tc.name)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("%s: error does not mention %q", tc.name, tc.want)
			}
		})
	}
}

func TestLoadNodeListenPrivacy(t *testing.T) {
	f := newFixture(t)
	tests := []struct {
		name    string
		listen  string
		tls     bool
		wantErr bool
	}{
		{"loopback", "127.0.0.1:7701", false, false},
		{"localhost", "localhost:7701", false, false},
		{"ipv6 loopback", "[::1]:7701", false, false},
		{"private 10", "10.1.2.3:7701", false, false},
		{"private 192", "192.168.1.5:7701", false, false},
		{"link local", "169.254.1.1:7701", false, false},
		{"wildcard", ":7701", false, true},
		{"public ip", "203.0.113.7:7701", false, true},
		{"public ip tls", "203.0.113.7:7701", true, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			args := append(f.baseArgs(), "-node.listen", tc.listen)
			if tc.tls {
				args = append(args, "-node.tls-cert-file", f.certFile, "-node.tls-key-file", f.keyFile)
			}
			_, err := Load(t.Context(), args, noEnv)
			if tc.wantErr && err == nil {
				t.Fatalf("plaintext %s accepted", tc.listen)
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("%s refused: %v", tc.listen, err)
			}
		})
	}
}

func TestLoadGatewayTLSRule(t *testing.T) {
	f := newFixture(t)
	args := f.baseArgs()

	// Private is off loopback: TLS required.
	_, err := Load(t.Context(), append(args, "-node.gateway-listen", "10.0.0.5:7702"), noEnv)
	if err == nil || !strings.Contains(err.Error(), "TLS") {
		t.Fatalf("off-loopback plaintext gateway: %v", err)
	}
	// Loopback is fine.
	if _, err := Load(t.Context(), append(args, "-node.gateway-listen", "127.0.0.1:7702"), noEnv); err != nil {
		t.Fatal(err)
	}
	// With TLS the private bind is allowed.
	_, err = Load(t.Context(), append(args,
		"-node.gateway-listen", "10.0.0.5:7702",
		"-node.gateway-tls-cert-file", f.certFile,
		"-node.gateway-tls-key-file", f.keyFile), noEnv)
	if err != nil {
		t.Fatalf("tls gateway refused: %v", err)
	}
}

func TestLoadTLSPairs(t *testing.T) {
	f := newFixture(t)
	for _, prefix := range []string{"server.", "node.", "node.gateway-"} {
		_, err := Load(t.Context(), append(f.baseArgs(),
			"-"+prefix+"tls-cert-file", f.certFile), noEnv)
		if err == nil || !strings.Contains(err.Error(), "together") {
			t.Fatalf("%stls-cert-file without key: %v", prefix, err)
		}
		_, err = Load(t.Context(), append(f.baseArgs(),
			"-"+prefix+"tls-key-file", f.keyFile), noEnv)
		if err == nil {
			t.Fatalf("%stls-key-file without cert accepted", prefix)
		}
	}
}

func TestLoadEnvErrorOverriddenByFlag(t *testing.T) {
	f := newFixture(t)
	env := testEnv(map[string]string{
		"BBRAFT_COMMANDS_STREAM_MAX_SECONDS": "bogus",
	})
	// A malformed env value only matters when the flag did not override it.
	args := append(f.baseArgs(), "-commands.stream-max-seconds", "7200")
	c, err := Load(t.Context(), args, env)
	if err != nil {
		t.Fatalf("explicit flag lost to malformed env: %v", err)
	}
	if c.Commands.StreamMaxSeconds != 7200 {
		t.Fatalf("stream-max = %d", c.Commands.StreamMaxSeconds)
	}
	// Without the flag the same env is still an error.
	if _, err := Load(t.Context(), f.baseArgs(), env); err == nil {
		t.Fatal("malformed env without flag override accepted")
	}
}

func TestLoadCanceled(t *testing.T) {
	f := newFixture(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := Load(ctx, f.baseArgs(), noEnv); err == nil {
		t.Fatal("accepted canceled context")
	}
}

func TestLoadHelp(t *testing.T) {
	if _, err := Load(t.Context(), []string{"-h"}, noEnv); !errors.Is(err, flag.ErrHelp) {
		t.Fatalf("err = %v, want flag.ErrHelp", err)
	}
}

func TestParseRoles(t *testing.T) {
	r, err := ParseRoles("all")
	if err != nil || !r.Has(RoleAPI) || !r.Has(RoleLifecycle) || !r.Has(RoleNodeHub) {
		t.Fatalf("all = %v, %v", r, err)
	}
	r, err = ParseRoles("api, lifecycle")
	if err != nil || !r.Has(RoleAPI) || !r.Has(RoleLifecycle) || r.Has(RoleNodeHub) {
		t.Fatalf("api,lifecycle = %v, %v", r, err)
	}
	if _, err := ParseRoles(""); err == nil {
		t.Fatal("empty accepted")
	}
	if _, err := ParseRoles("bogus"); err == nil {
		t.Fatal("bogus accepted")
	}
	if got := r.String(); got != "api,lifecycle" {
		t.Fatalf("String = %q", got)
	}
}

func TestLoadPublicURLAndGatewayDial(t *testing.T) {
	f := newFixture(t)
	c, err := Load(t.Context(), append(f.baseArgs(),
		"-server.public-url", "https://bbraft.example.test/base",
		"-node.gateway-dial", "gateway.internal:9702",
		"-node.gateway-tls-cert-file", f.certFile,
		"-node.gateway-tls-key-file", f.keyFile,
		"-node.gateway-ca-file", f.certFile,
		"-node.gateway-server-name", "gateway.internal",
	), noEnv)
	if err != nil {
		t.Fatal(err)
	}
	if c.Server.PublicURL != "https://bbraft.example.test/base" {
		t.Fatalf("public url = %q", c.Server.PublicURL)
	}
	if c.Node.GatewayDial != "gateway.internal:9702" {
		t.Fatalf("gateway dial = %q", c.Node.GatewayDial)
	}
	if c.Node.GatewayCACertFile != f.certFile || c.Node.GatewayServerName != "gateway.internal" {
		t.Fatalf("gateway trust = %q / %q", c.Node.GatewayCACertFile, c.Node.GatewayServerName)
	}

	// The public URL scheme follows the configured TLS pair by default.
	c, err = Load(t.Context(), append(f.baseArgs(),
		"-server.tls-cert-file", f.certFile,
		"-server.tls-key-file", f.keyFile,
	), noEnv)
	if err != nil {
		t.Fatal(err)
	}
	if c.Server.PublicURL != "https://127.0.0.1:7700" {
		t.Fatalf("TLS public url = %q", c.Server.PublicURL)
	}

	// A wildcard API bind has no trusted host: it needs an explicit public
	// URL instead of advertising 0.0.0.0. The gateway dial still defaults
	// to loopback for a wildcard gateway bind.
	if _, err := Load(t.Context(), append(f.baseArgs(),
		"-server.listen", "0.0.0.0:7700",
		"-node.gateway-listen", "0.0.0.0:7702",
		"-node.gateway-tls-cert-file", f.certFile,
		"-node.gateway-tls-key-file", f.keyFile,
	), noEnv); err == nil || !strings.Contains(err.Error(), "public-url") {
		t.Fatalf("wildcard API bind without public URL: %v", err)
	}
	c, err = Load(t.Context(), append(f.baseArgs(),
		"-server.listen", "0.0.0.0:7700",
		"-server.public-url", "https://bbraft.example.test",
		"-node.gateway-listen", "0.0.0.0:7702",
		"-node.gateway-tls-cert-file", f.certFile,
		"-node.gateway-tls-key-file", f.keyFile,
	), noEnv)
	if err != nil {
		t.Fatal(err)
	}
	if c.Server.PublicURL != "https://bbraft.example.test" || c.Node.GatewayDial != "127.0.0.1:7702" {
		t.Fatalf("wildcard defaults = %q / %q", c.Server.PublicURL, c.Node.GatewayDial)
	}
}

func TestLoadPublicURLAndGatewayDialMalformed(t *testing.T) {
	f := newFixture(t)
	good := f.baseArgs()
	tests := []struct {
		name string
		args []string
		want string
	}{
		{"relative public url", append(good, "-server.public-url", "/api"), "scheme"},
		{"public url without host", append(good, "-server.public-url", "http:/path"), "host"},
		{"ftp public url", append(good, "-server.public-url", "ftp://example.test"), "http"},
		{"public url credentials", append(good, "-server.public-url", "https://u:p@example.test"), "credentials"},
		{"public url query", append(good, "-server.public-url", "https://example.test?x=1"), "query"},
		{"public url empty hostname", append(good, "-server.public-url", "https://:443"), "host"},
		{"empty dial host", append(good, "-node.gateway-dial", ":7702"), "empty"},
		{"dial port zero", append(good, "-node.gateway-dial", "127.0.0.1:0"), "port"},
		{"dial port plus", append(good, "-node.gateway-dial", "127.0.0.1:+7702"), "port"},
		{"plaintext off-loopback dial", append(good, "-node.gateway-dial", "10.0.0.5:7702"), "loopback"},
		{"ca without gateway tls", append(good, "-node.gateway-ca-file", f.certFile), "TLS"},
		{"server name without gateway tls", append(good, "-node.gateway-server-name", "gw"), "TLS"},
		{"server name newline", append(good,
			"-node.gateway-tls-cert-file", f.certFile,
			"-node.gateway-tls-key-file", f.keyFile,
			"-node.gateway-server-name", "gw\nname"), "whitespace"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Load(t.Context(), tc.args, noEnv)
			if err == nil {
				t.Fatalf("%s: malformed input accepted", tc.name)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("%s: error does not mention %q", tc.name, tc.want)
			}
		})
	}
}

// TestLoadNodeLost covers -lifecycle.node-lost-seconds: the default, the
// environment fallback, zero to disable, and the refused values (below two
// inventory polls, negative, too large).
func TestLoadNodeLost(t *testing.T) {
	f := newFixture(t)
	for _, tc := range []struct {
		name    string
		args    []string
		env     map[string]string
		want    int
		wantErr bool
	}{
		{name: "default", want: DefaultNodeLostSeconds},
		{name: "flag", args: []string{"-lifecycle.node-lost-seconds", "300"}, want: 300},
		{name: "env", env: map[string]string{"BBRAFT_LIFECYCLE_NODE_LOST_SECONDS": "90"}, want: 90},
		{name: "disabled", args: []string{"-lifecycle.node-lost-seconds", "0"}, want: 0},
		{name: "minimum", args: []string{"-lifecycle.node-lost-seconds", "60"}, want: 60},
		{name: "below two polls", args: []string{"-lifecycle.node-lost-seconds", "59"}, wantErr: true},
		{name: "negative", args: []string{"-lifecycle.node-lost-seconds", "-1"}, wantErr: true},
		{name: "overflow", args: []string{"-lifecycle.node-lost-seconds", "9223372037"}, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			env := noEnv
			if tc.env != nil {
				env = testEnv(tc.env)
			}
			c, err := Load(t.Context(), append(f.baseArgs(), tc.args...), env)
			if tc.wantErr {
				if err == nil || !strings.Contains(err.Error(), "node-lost-seconds") {
					t.Fatalf("Load = %v, want a node-lost-seconds error", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if c.Lifecycle.NodeLostSeconds != tc.want {
				t.Fatalf("node-lost-seconds = %d, want %d", c.Lifecycle.NodeLostSeconds, tc.want)
			}
		})
	}
}

// TestLoadDiskLowPercent covers -lifecycle.disk-low-percent: the default,
// the environment fallback, zero to disable, and the refused values
// (negative, above 90).
func TestLoadDiskLowPercent(t *testing.T) {
	f := newFixture(t)
	for _, tc := range []struct {
		name    string
		args    []string
		env     map[string]string
		want    int
		wantErr bool
	}{
		{name: "default", want: DefaultDiskLowPercent},
		{name: "flag", args: []string{"-lifecycle.disk-low-percent", "25"}, want: 25},
		{name: "env", env: map[string]string{"BBRAFT_LIFECYCLE_DISK_LOW_PERCENT": "5"}, want: 5},
		{name: "disabled", args: []string{"-lifecycle.disk-low-percent", "0"}, want: 0},
		{name: "maximum", args: []string{"-lifecycle.disk-low-percent", "90"}, want: 90},
		{name: "above maximum", args: []string{"-lifecycle.disk-low-percent", "91"}, wantErr: true},
		{name: "negative", args: []string{"-lifecycle.disk-low-percent", "-1"}, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			env := noEnv
			if tc.env != nil {
				env = testEnv(tc.env)
			}
			c, err := Load(t.Context(), append(f.baseArgs(), tc.args...), env)
			if tc.wantErr {
				if err == nil || !strings.Contains(err.Error(), "disk-low-percent") {
					t.Fatalf("Load = %v, want a disk-low-percent error", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if c.Lifecycle.DiskLowPercent != tc.want {
				t.Fatalf("disk-low-percent = %d, want %d", c.Lifecycle.DiskLowPercent, tc.want)
			}
		})
	}
}

// TestLoadGC covers the chunk collector's flags: defaults, environment
// fallback and refused values (a zero grace would delete chunks an attempt has
// just uploaded).
func TestLoadGC(t *testing.T) {
	f := newFixture(t)
	c, err := Load(t.Context(), f.baseArgs(), noEnv)
	if err != nil {
		t.Fatal(err)
	}
	if want := (GC{IntervalSeconds: 21600, GraceSeconds: 86400, HistorySeconds: 86400, Batch: 1000}); c.GC != want {
		t.Fatalf("defaults = %+v, want %+v", c.GC, want)
	}
	c, err = Load(t.Context(), append(f.baseArgs(), "-gc.batch", "50"), testEnv(map[string]string{"BBRAFT_GC_GRACE_SECONDS": "3600"}))
	if err != nil {
		t.Fatal(err)
	}
	if c.GC.Batch != 50 || c.GC.GraceSeconds != 3600 {
		t.Fatalf("flag and env = %+v", c.GC)
	}
	for _, tc := range []struct {
		flag, value string
	}{
		{"gc.interval-seconds", "0"}, {"gc.grace-seconds", "0"}, {"gc.history-seconds", "-1"},
		{"gc.batch", "0"}, {"gc.grace-seconds", "9223372037"},
	} {
		_, err := Load(t.Context(), append(f.baseArgs(), "-"+tc.flag, tc.value), noEnv)
		if err == nil || !strings.Contains(err.Error(), tc.flag) {
			t.Errorf("-%s %s: Load = %v, want an error naming the flag", tc.flag, tc.value, err)
		}
	}
}
