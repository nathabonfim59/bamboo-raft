// Package config parses bbraft-server's flags and their BBRAFT_*
// environment fallbacks (AGENTS.md "Configuration"): a flag -a.b-c reads
// BBRAFT_A_B_C when the flag is not given. Secrets come only from files or
// the environment, never from flags, and never appear in errors.
//
// Flag names, defaults and rules are the contracts other packages consume:
// internal/server/run starts listeners, internal/auth loads the credential
// files named here, and internal/lifecycle reads the intervals.
package config

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"net"
	"net/netip"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
	"unicode"
)

// Config is the validated server configuration.
type Config struct {
	Roles     Roles
	Server    Server
	Database  Database
	Auth      Auth
	Node      Node
	Commands  Commands
	Lifecycle Lifecycle
	Secrets   Secrets
}

// Server holds the public HTTP API listener.
type Server struct {
	// Listen is host:port, default 127.0.0.1:7700 (-server.listen).
	Listen string
	// TLSCertFile and TLSKeyFile (-server.tls-cert-file/-key-file) enable
	// HTTPS; both or neither.
	TLSCertFile string
	TLSKeyFile  string
	// PublicURL is the trusted absolute base URL clients use to reach this
	// server (-server.public-url). Signed upload URLs are built from it;
	// request Host and proxy headers are never trusted. It is required when
	// Server.Listen binds a wildcard address; otherwise empty resolves to
	// the scheme implied by the TLS pair and the listen host:port.
	PublicURL string
}

// Database holds the PostgreSQL connection. URL is a secret: it never goes
// to flags, errors or logs.
type Database struct {
	// URL is the resolved postgres:// connection string, from
	// -database.url-file (or BBRAFT_DATABASE_URL_FILE) contents or the
	// BBRAFT_DATABASE_URL environment value.
	URL string
	// URLFile names the file URL was read from, empty when it came from
	// the environment. Kept for diagnostics only.
	URLFile string
}

// Auth holds the API key configuration.
type Auth struct {
	// KeysFile is the JSON file of configured keys (-auth.keys-file), the
	// shape auth.LoadKeysFile reads.
	KeysFile string
	// KeyPrefix is the required API key prefix (-auth.key-prefix),
	// default "bmb_".
	KeyPrefix string
}

// Node holds the hub listener nodes dial, its credential file and the
// internal node gateway.
type Node struct {
	// Listen is where nodes dial in, default 127.0.0.1:7701 (-node.listen).
	Listen string
	// TLSCertFile and TLSKeyFile secure the listener. Without them a
	// plaintext bind is allowed only on loopback or private addresses
	// (Phase 0.5 rule).
	TLSCertFile string
	TLSKeyFile  string
	// TokensFile is the JSON file of per-node token digests
	// (-node.tokens-file), the shape auth.LoadNodeTokensFile reads.
	TokensFile string
	// GatewayListen is the internal node gateway address, default
	// 127.0.0.1:7702 (-node.gateway-listen). Off loopback it requires TLS.
	GatewayListen      string
	GatewayTLSCertFile string
	GatewayTLSKeyFile  string
	// GatewayDial is the address the api and lifecycle roles dial to reach
	// the gateway (-node.gateway-dial). Empty uses GatewayListen, with a
	// wildcard bind mapped to 127.0.0.1, so a process that binds elsewhere
	// still has a trusted dial target.
	GatewayDial string
	// GatewayCACertFile is a PEM bundle of CAs that verify the gateway's
	// certificate (-node.gateway-ca-file). Optional: without it the system
	// roots are used, and a private CA is expected to be installed there.
	GatewayCACertFile string
	// GatewayServerName overrides the verified certificate name
	// (-node.gateway-server-name). Optional; empty verifies the dial host.
	GatewayServerName string
	// GatewayTokenFile names the file holding the shared server credential
	// (-node.gateway-token-file); BBRAFT_NODE_GATEWAY_TOKEN supplies it
	// directly. GatewayToken is the resolved secret.
	GatewayTokenFile string
	GatewayToken     string
}

// Commands holds command execution limits.
type Commands struct {
	// StreamMaxSeconds caps streamed command timeouts, the approved
	// Phase 0.5 difference: default 7200 instead of the API's 600
	// (-commands.stream-max-seconds).
	StreamMaxSeconds int
}

// Lifecycle holds the worker intervals.
type Lifecycle struct {
	// RetrySeconds is the base job retry backoff (-lifecycle.retry-seconds).
	RetrySeconds int
	// ReaperSeconds is the TTL expiry scan interval (-lifecycle.reaper-seconds).
	ReaperSeconds int
	// JobPollSeconds is the LISTEN/NOTIFY fallback poll
	// (-lifecycle.job-poll-seconds).
	JobPollSeconds int
	// StoppedRetentionSeconds is how long an archived sandbox keeps its
	// retained disk before the reaper deletes the sandbox through the normal
	// deletion path (-lifecycle.stopped-retention-seconds). The default is
	// seven days; zero disables retention deletion.
	StoppedRetentionSeconds int
}

// Defaults, overridable by flags and BBRAFT_* environment.
const (
	DefaultRoles            = RolesAll
	DefaultServerListen     = "127.0.0.1:7700"
	DefaultKeyPrefix        = "bmb_"
	DefaultNodeListen       = "127.0.0.1:7701"
	DefaultGatewayListen    = "127.0.0.1:7702"
	DefaultStreamMaxSeconds = 7200
	DefaultRetrySeconds     = 5
	DefaultReaperSeconds    = 60
	DefaultJobPollSeconds   = 15
	// DefaultStoppedRetentionSeconds is the stopped-disk retention: seven
	// days.
	DefaultStoppedRetentionSeconds = 604800
)

// Env names of secrets that have no flag: they arrive through the
// environment directly, and conflict with their file source when both are
// set.
const (
	EnvDatabaseURL      = "BBRAFT_DATABASE_URL"
	EnvGatewayToken     = "BBRAFT_NODE_GATEWAY_TOKEN"
	EnvSecretsMasterKey = "BBRAFT_SECRETS_MASTER_KEY"
)

// envName maps a flag name to its environment fallback: dots and dashes
// become underscores, upper-cased, under BBRAFT_.
func envName(name string) string {
	return "BBRAFT_" + strings.ToUpper(strings.NewReplacer(".", "_", "-", "_").Replace(name))
}

// Load parses args (the command line, without argv[0]) with env fallbacks
// (os.LookupEnv works directly) into a validated Config. Unknown flags and
// positional arguments are errors; -h/-help returns flag.ErrHelp. Secrets
// are resolved from their file or environment source, so the returned
// Config holds Database.URL, Node.GatewayToken and Secrets.MasterKey —
// keep them out of logs.
func Load(ctx context.Context, args []string, env func(string) (string, bool)) (*Config, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if env == nil {
		env = os.LookupEnv
	}
	fs := flag.NewFlagSet("bbraft-server", flag.ContinueOnError)
	fs.SetOutput(io.Discard)

	str := func(name, def string) *string {
		if v, ok := env(envName(name)); ok {
			def = v
		}
		return fs.String(name, def, "")
	}
	envErrs := map[string]error{}
	num := func(name string, def int) *int {
		if v, ok := env(envName(name)); ok {
			n, err := strconv.Atoi(v)
			if err != nil {
				envErrs[name] = fmt.Errorf("config: %s: %q is not an integer", envName(name), v)
			} else {
				def = n
			}
		}
		return fs.Int(name, def, "")
	}
	boolean := func(name string, def bool) *bool {
		if v, ok := env(envName(name)); ok {
			b, err := strconv.ParseBool(v)
			if err != nil {
				envErrs[name] = fmt.Errorf("config: %s: %q is not a boolean", envName(name), v)
			} else {
				def = b
			}
		}
		return fs.Bool(name, def, "")
	}

	roles := str("roles", DefaultRoles)
	serverListen := str("server.listen", DefaultServerListen)
	serverCert := str("server.tls-cert-file", "")
	serverKey := str("server.tls-key-file", "")
	serverPublicURL := str("server.public-url", "")
	dbURLFile := str("database.url-file", "")
	keysFile := str("auth.keys-file", "")
	keyPrefix := str("auth.key-prefix", DefaultKeyPrefix)
	nodeListen := str("node.listen", DefaultNodeListen)
	nodeCert := str("node.tls-cert-file", "")
	nodeKey := str("node.tls-key-file", "")
	tokensFile := str("node.tokens-file", "")
	gwListen := str("node.gateway-listen", DefaultGatewayListen)
	gwCert := str("node.gateway-tls-cert-file", "")
	gwKey := str("node.gateway-tls-key-file", "")
	gwDial := str("node.gateway-dial", "")
	gwCA := str("node.gateway-ca-file", "")
	gwServerName := str("node.gateway-server-name", "")
	gwTokenFile := str("node.gateway-token-file", "")
	streamMax := num("commands.stream-max-seconds", DefaultStreamMaxSeconds)
	retry := num("lifecycle.retry-seconds", DefaultRetrySeconds)
	reaper := num("lifecycle.reaper-seconds", DefaultReaperSeconds)
	jobPoll := num("lifecycle.job-poll-seconds", DefaultJobPollSeconds)
	stoppedRetention := num("lifecycle.stopped-retention-seconds", DefaultStoppedRetentionSeconds)
	masterKeyFile := str("secrets.master-key-file", "")
	allowEphemeral := boolean("secrets.allow-ephemeral", false)

	if err := fs.Parse(args); err != nil {
		return nil, err
	}
	if fs.NArg() > 0 {
		return nil, fmt.Errorf("config: unexpected arguments: %s", strings.Join(fs.Args(), " "))
	}
	// A malformed environment fallback is only an error when the flag did
	// not override it: explicit flags win.
	set := map[string]bool{}
	fs.Visit(func(f *flag.Flag) { set[f.Name] = true })
	for name, err := range envErrs {
		if !set[name] {
			return nil, err
		}
	}

	parsed, err := ParseRoles(*roles)
	if err != nil {
		return nil, err
	}
	c := &Config{
		Roles: parsed,
		Server: Server{
			Listen:      *serverListen,
			TLSCertFile: *serverCert,
			TLSKeyFile:  *serverKey,
			PublicURL:   *serverPublicURL,
		},
		Database: Database{URLFile: *dbURLFile},
		Auth: Auth{
			KeysFile:  *keysFile,
			KeyPrefix: *keyPrefix,
		},
		Node: Node{
			Listen:             *nodeListen,
			TLSCertFile:        *nodeCert,
			TLSKeyFile:         *nodeKey,
			TokensFile:         *tokensFile,
			GatewayListen:      *gwListen,
			GatewayTLSCertFile: *gwCert,
			GatewayTLSKeyFile:  *gwKey,
			GatewayDial:        *gwDial,
			GatewayCACertFile:  *gwCA,
			GatewayServerName:  *gwServerName,
			GatewayTokenFile:   *gwTokenFile,
		},
		Commands: Commands{StreamMaxSeconds: *streamMax},
		Lifecycle: Lifecycle{
			RetrySeconds:            *retry,
			ReaperSeconds:           *reaper,
			JobPollSeconds:          *jobPoll,
			StoppedRetentionSeconds: *stoppedRetention,
		},
		Secrets: Secrets{
			MasterKeyFile:  *masterKeyFile,
			AllowEphemeral: *allowEphemeral,
		},
	}
	if err := c.resolve(ctx, env); err != nil {
		return nil, err
	}
	if err := c.Validate(); err != nil {
		return nil, err
	}
	return c, nil
}

// resolve fills the secret fields from their file or environment source
// and checks that every named file exists.
func (c *Config) resolve(ctx context.Context, env func(string) (string, bool)) error {
	url, err := secret("database.url", c.Database.URLFile, EnvDatabaseURL, env)
	if err != nil {
		return err
	}
	c.Database.URL = url

	token, err := secret("node.gateway-token", c.Node.GatewayTokenFile, EnvGatewayToken, env)
	if err != nil {
		return err
	}
	c.Node.GatewayToken = token

	if err := c.resolveSecrets(ctx, env); err != nil {
		return err
	}
	c.resolveEndpoints()

	for _, p := range []string{
		c.Database.URLFile, c.Auth.KeysFile, c.Node.TokensFile,
		c.Node.GatewayTokenFile, c.Node.GatewayCACertFile,
		c.Server.TLSCertFile, c.Server.TLSKeyFile,
		c.Node.TLSCertFile, c.Node.TLSKeyFile,
		c.Node.GatewayTLSCertFile, c.Node.GatewayTLSKeyFile,
	} {
		if p == "" {
			continue
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := checkFile(p); err != nil {
			return err
		}
	}
	return nil
}

// resolveEndpoints fills the trusted absolute URLs from the listener when
// they were not configured explicitly. A wildcard API bind has no trusted
// public host, so it keeps PublicURL empty and Validate requires an explicit
// value instead of advertising 0.0.0.0 or an empty host.
func (c *Config) resolveEndpoints() {
	if c.Node.GatewayDial == "" {
		c.Node.GatewayDial = dialHost(c.Node.GatewayListen)
	}
	if c.Server.PublicURL != "" || wildcardListen(c.Server.Listen) {
		return
	}
	scheme := "http"
	if c.Server.TLSCertFile != "" {
		scheme = "https"
	}
	c.Server.PublicURL = scheme + "://" + c.Server.Listen
}

// wildcardListen reports a bind that has no stable public host.
func wildcardListen(listen string) bool {
	host, _, err := net.SplitHostPort(listen)
	if err != nil {
		return false // Validate reports the malformed address
	}
	switch host {
	case "", "0.0.0.0", "::":
		return true
	default:
		return false
	}
}

// dialHost turns a listen address into a dialable default: a wildcard bind
// (empty host, 0.0.0.0 or ::) becomes loopback, and a hostname is kept.
func dialHost(listen string) string {
	host, port, err := net.SplitHostPort(listen)
	if err != nil {
		return listen // Validate reports the malformed address
	}
	switch host {
	case "", "0.0.0.0", "::":
		host = "127.0.0.1"
	}
	return net.JoinHostPort(host, port)
}

// secret resolves one secret from a file (path may itself come from the
// flag's env fallback) or a direct environment value. Setting both is a
// conflict error, and the value never appears in it.
func secret(name, file, directEnv string, env func(string) (string, bool)) (string, error) {
	direct, hasDirect := env(directEnv)
	if file != "" && hasDirect {
		return "", fmt.Errorf("config: %s: file and %s both set; pick one", name, directEnv)
	}
	if hasDirect {
		if direct == "" {
			return "", fmt.Errorf("config: %s is empty", directEnv)
		}
		return direct, nil
	}
	if file == "" {
		return "", nil
	}
	raw, err := os.ReadFile(file)
	if err != nil {
		return "", fmt.Errorf("config: %s: read file: %w", name, err)
	}
	v := strings.TrimSpace(string(raw))
	if v == "" {
		return "", fmt.Errorf("config: %s: file is empty", name)
	}
	return v, nil
}

func checkFile(path string) error {
	st, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("config: %s: %w", path, err)
	}
	if !st.Mode().IsRegular() {
		return fmt.Errorf("config: %s: not a regular file", path)
	}
	return nil
}

// Validate checks the assembled Config; Load already ran it.
func (c *Config) Validate() error {
	if len(c.Roles) == 0 {
		return errors.New("config: no roles selected")
	}
	if err := listenAddr("server.listen", c.Server.Listen); err != nil {
		return err
	}
	if err := tlsPair("server", c.Server.TLSCertFile, c.Server.TLSKeyFile); err != nil {
		return err
	}
	if err := listenAddr("node.listen", c.Node.Listen); err != nil {
		return err
	}
	if err := tlsPair("node", c.Node.TLSCertFile, c.Node.TLSKeyFile); err != nil {
		return err
	}
	if err := listenAddr("node.gateway-listen", c.Node.GatewayListen); err != nil {
		return err
	}
	if err := tlsPair("node.gateway", c.Node.GatewayTLSCertFile, c.Node.GatewayTLSKeyFile); err != nil {
		return err
	}
	if c.Roles.Has(RoleAPI) && c.Server.PublicURL == "" {
		return errors.New("config: server.public-url is required when server.listen binds a wildcard address")
	}
	if c.Server.PublicURL != "" {
		if err := publicURL("server.public-url", c.Server.PublicURL); err != nil {
			return err
		}
	}
	if err := dialAddr("node.gateway-dial", c.Node.GatewayDial); err != nil {
		return err
	}
	if (c.Node.GatewayCACertFile != "" || c.Node.GatewayServerName != "") && c.Node.GatewayTLSCertFile == "" {
		return errors.New("config: node.gateway-ca-file and node.gateway-server-name need the gateway TLS pair")
	}
	if c.Node.GatewayServerName != "" && strings.IndexFunc(c.Node.GatewayServerName, func(r rune) bool { return unicode.IsSpace(r) || r == 0 }) >= 0 {
		return errors.New("config: node.gateway-server-name must be nonempty without whitespace")
	}

	// Phase 0.5 transport rules: plaintext node binds only on loopback or
	// private addresses; the internal gateway needs TLS off loopback.
	if c.Node.TLSCertFile == "" {
		public, err := publicHost("node.listen", c.Node.Listen)
		if err != nil {
			return err
		}
		if public {
			return errors.New("config: node.listen: plaintext binds are limited to loopback or private addresses; set -node.tls-cert-file/-node.tls-key-file")
		}
	}
	if c.Node.GatewayTLSCertFile == "" {
		loopback, err := loopbackHost("node.gateway-listen", c.Node.GatewayListen)
		if err != nil {
			return err
		}
		if !loopback {
			return errors.New("config: node.gateway-listen: TLS is required off loopback; set -node.gateway-tls-cert-file/-node.gateway-tls-key-file")
		}
		if !loopbackDial(c.Node.GatewayDial) {
			return errors.New("config: node.gateway-dial: a plaintext gateway is limited to loopback; set the gateway TLS pair")
		}
	}

	if c.Roles.Has(RoleAPI) {
		if c.Auth.KeysFile == "" {
			return errors.New("config: role api needs -auth.keys-file or BBRAFT_AUTH_KEYS_FILE")
		}
	}
	if c.Roles.Has(RoleNodeHub) && c.Node.TokensFile == "" {
		return errors.New("config: role nodehub needs -node.tokens-file or BBRAFT_NODE_TOKENS_FILE")
	}
	// Every role touches PostgreSQL: api and lifecycle for state and jobs,
	// nodehub for the node inventory writer.
	if c.Database.URL == "" {
		return fmt.Errorf("config: a database URL is required: -database.url-file, BBRAFT_DATABASE_URL_FILE or %s", EnvDatabaseURL)
	}
	if c.Node.GatewayToken == "" {
		return fmt.Errorf("config: the internal node gateway needs -node.gateway-token-file, BBRAFT_NODE_GATEWAY_TOKEN_FILE or %s", EnvGatewayToken)
	}

	if c.Auth.KeyPrefix == "" || strings.ContainsAny(c.Auth.KeyPrefix, " \t") {
		return errors.New("config: auth.key-prefix must be nonempty without spaces")
	}
	if c.Commands.StreamMaxSeconds < 1 {
		return errors.New("config: commands.stream-max-seconds must be at least 1")
	}
	for _, iv := range []struct {
		name string
		n    int
	}{
		{"lifecycle.retry-seconds", c.Lifecycle.RetrySeconds},
		{"lifecycle.reaper-seconds", c.Lifecycle.ReaperSeconds},
		{"lifecycle.job-poll-seconds", c.Lifecycle.JobPollSeconds},
	} {
		if iv.n < 1 {
			return fmt.Errorf("config: %s must be at least 1", iv.name)
		}
	}
	if c.Lifecycle.StoppedRetentionSeconds < 0 {
		return errors.New("config: lifecycle.stopped-retention-seconds must not be negative")
	}
	if c.Lifecycle.StoppedRetentionSeconds > maxRetentionSeconds {
		return errors.New("config: lifecycle.stopped-retention-seconds is too large")
	}
	return nil
}

// maxRetentionSeconds is the largest stopped-retention that fits a
// time.Duration when converted from seconds.
const maxRetentionSeconds = int(math.MaxInt64 / int64(time.Second))

// StoppedRetentionOption converts the configured stopped-retention into the
// lifecycle worker's optional duration. The config always carries an explicit
// value (the flag default), so the result is never nil: the default becomes
// seven days and an explicit zero disables retention deletion.
func (l Lifecycle) StoppedRetentionOption() *time.Duration {
	return new(time.Duration(l.StoppedRetentionSeconds) * time.Second)
}

// listenAddr checks a host:port value: a numeric port and a host that is
// empty (wildcard), "localhost" or an IP literal.
func listenAddr(flag, addr string) error {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("config: %s: %q: %w", flag, addr, err)
	}
	n, err := strconv.Atoi(port)
	if err != nil || n < 1 || n > 65535 || !asciiDigits(port) {
		return fmt.Errorf("config: %s: %q: port must be 1-65535", flag, addr)
	}
	switch host {
	case "", "localhost":
		return nil
	}
	if _, err := netip.ParseAddr(host); err != nil {
		return fmt.Errorf("config: %s: %q: host must be an IP literal or localhost", flag, addr)
	}
	return nil
}

// tlsPair checks cert and key are set together or not at all.
func tlsPair(name, cert, key string) error {
	if (cert == "") != (key == "") {
		return fmt.Errorf("config: %s.tls-cert-file and %s.tls-key-file must be set together", name, name)
	}
	return nil
}

// publicHost reports whether the listen host exposes the socket beyond
// loopback/private space (wildcard and public IPs included).
func publicHost(flag, addr string) (bool, error) {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return false, fmt.Errorf("config: %s: %q: %w", flag, addr, err)
	}
	if host == "localhost" {
		return false, nil
	}
	if host == "" {
		return true, nil // wildcard binds every interface
	}
	ip, err := netip.ParseAddr(host)
	if err != nil {
		return false, fmt.Errorf("config: %s: %q: host must be an IP literal or localhost", flag, addr)
	}
	return !ip.IsLoopback() && !ip.IsPrivate() && !ip.IsLinkLocalUnicast(), nil
}

// publicURL checks the trusted absolute base URL used for signed links. The
// server never derives it from request Host or proxy headers. Diagnostics
// name the source only: a rejected URL may carry credentials or a query, so
// its raw form is never echoed.
func publicURL(flag, raw string) error {
	u, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("config: %s: not a valid URL", flag)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("config: %s: scheme must be http or https", flag)
	}
	if u.Hostname() == "" {
		return fmt.Errorf("config: %s: URL needs a host", flag)
	}
	if u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return fmt.Errorf("config: %s: URL must not carry credentials, a query or a fragment", flag)
	}
	return nil
}

// asciiDigits reports whether s is nonempty ASCII decimal digits only.
func asciiDigits(s string) bool {
	if s == "" {
		return false
	}
	for i := range len(s) {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

// dialAddr checks a client dial target: host:port with a nonempty host and an
// ASCII decimal port. Unlike a listen address it may name a host.
func dialAddr(flag, addr string) error {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("config: %s: %q: %w", flag, addr, err)
	}
	if host == "" {
		return fmt.Errorf("config: %s: %q: host must not be empty", flag, addr)
	}
	if !asciiDigits(port) {
		return fmt.Errorf("config: %s: %q: port must be 1-65535", flag, addr)
	}
	n, err := strconv.Atoi(port)
	if err != nil || n < 1 || n > 65535 {
		return fmt.Errorf("config: %s: %q: port must be 1-65535", flag, addr)
	}
	return nil
}

// loopbackDial reports whether a plaintext dial target stays on loopback.
func loopbackDial(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return false
	}
	if host == "localhost" {
		return true
	}
	ip, err := netip.ParseAddr(host)
	return err == nil && ip.IsLoopback()
}

// loopbackHost reports whether the listen host is loopback only.
func loopbackHost(flag, addr string) (bool, error) {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return false, fmt.Errorf("config: %s: %q: %w", flag, addr, err)
	}
	if host == "localhost" {
		return true, nil
	}
	ip, err := netip.ParseAddr(host)
	if err != nil {
		return false, fmt.Errorf("config: %s: %q: host must be an IP literal or localhost", flag, addr)
	}
	return ip.IsLoopback(), nil
}
