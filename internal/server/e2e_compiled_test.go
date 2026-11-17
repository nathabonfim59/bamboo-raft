package server_test

// End-to-end coverage for the compiled bbraft CLI and server. The parent test
// is a PostgreSQL integration test: it skips unless BBRAFT_TEST_DATABASE_URL
// is set and BBRAFT_TEST_REQUIRE_ALL=1 turns a missing database
// into a failure. Real binaries are built and executed only after that gate.
//
// The daemon runs every role with real TCP listeners and the real node
// gateway. The node is nodefake behind the real nodelink transport; the test
// wrapper emulates only the exact internal push extraction/cleanup script and
// delegates every ordinary command to nodefake. Two bounded reverse proxies
// capture wire bodies for the v1/extension validators: one is the
// CLI's API URL, the other is the cross-origin signed upload receiver. No
// test StoreOpener or route hook is used.

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"runtime"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"github.com/nathabonfim59/bamboo-raft/internal/api"
	"github.com/nathabonfim59/bamboo-raft/internal/compat"
	"github.com/nathabonfim59/bamboo-raft/internal/db"
	"github.com/nathabonfim59/bamboo-raft/internal/db/dbtest"
)

// Proxy capture bounds. The fixture bodies are tiny; the bounds keep a broken
// client or server from growing test memory.
const (
	e2eRequestCaptureLimit  = 64 << 10
	e2eResponseCaptureLimit = 4 << 20
)

// e2eDoc identifies the OpenAPI document that owns an operation.
type e2eDoc int

const (
	e2eDocNone e2eDoc = iota
	e2eDocAPI
	e2eDocExtension
)

// e2eExchange is one recorded proxy request/response. It stores safe fields
// only: no Authorization value, cookie value, request URL or signed token.
type e2eExchange struct {
	method           string
	opID             string
	doc              e2eDoc
	signed           bool
	pathClass        string
	targetID         string
	status           int
	header           http.Header
	stream           bool
	reqBody          []byte
	reqTruncated     bool
	respBody         []byte
	respTruncated    bool
	hasAuthorization bool
	hasCookie        bool
	confirmDelete    string
}

// e2eProxy is a bounded recording reverse proxy.
type e2eProxy struct {
	server  *httptest.Server
	proxy   *httputil.ReverseProxy
	target  *url.URL
	secrets *daemonProcessSecrets

	mu           sync.Mutex
	exchanges    []e2eExchange
	signedTokens []string
}

// e2eProxyOptions selects test-local wire compatibility behavior. The default
// proxy forwards the server exactly.
type e2eProxyOptions struct {
	// omitInfoDetached removes the optional commands.detached member from
	// /api/bbraft/info before the CLI sees it. The legacy harness uses this
	// to represent an old server while the compiled phase-exit harness sees
	// the real capability.
	omitInfoDetached bool
}

// newE2EProxy builds one recording reverse proxy to target.
func newE2EProxy(t *testing.T, target string, secrets *daemonProcessSecrets) *e2eProxy {
	t.Helper()
	return newE2EProxyWithOptions(t, target, secrets, e2eProxyOptions{})
}

// newE2EProxyWithOptions builds a recording proxy with test-local options.
func newE2EProxyWithOptions(t *testing.T, target string, secrets *daemonProcessSecrets, options e2eProxyOptions) *e2eProxy {
	t.Helper()
	targetURL, err := url.Parse(target)
	if err != nil {
		t.Fatal("proxy target is malformed")
	}
	p := &e2eProxy{target: targetURL, secrets: secrets}
	proxy := httputil.NewSingleHostReverseProxy(targetURL)
	// Stream NDJSON instead of buffering the whole command response.
	proxy.FlushInterval = -1
	proxy.ErrorHandler = func(w http.ResponseWriter, _ *http.Request, _ error) {
		// Never echo the URL or transport error: a signed URL may be in it.
		w.WriteHeader(http.StatusBadGateway)
	}
	if options.omitInfoDetached {
		proxy.ModifyResponse = func(response *http.Response) error {
			if response.StatusCode != http.StatusOK || response.Request == nil ||
				response.Request.URL.Path != "/api/bbraft/info" {
				return nil
			}
			raw, err := io.ReadAll(io.LimitReader(response.Body, 64<<10))
			if err != nil {
				return err
			}
			response.Body.Close()
			var members map[string]any
			if err := json.Unmarshal(raw, &members); err != nil {
				return err
			}
			if commands, ok := members["commands"].(map[string]any); ok {
				delete(commands, "detached")
			}
			body, err := json.Marshal(members)
			if err != nil {
				return err
			}
			response.Body = io.NopCloser(bytes.NewReader(body))
			response.ContentLength = int64(len(body))
			response.Header.Set("Content-Length", strconv.Itoa(len(body)))
			return nil
		}
	}
	p.proxy = proxy
	p.server = httptest.NewServer(http.HandlerFunc(p.serveHTTP))
	t.Cleanup(p.server.Close)
	return p
}

// url returns the proxy base URL.
func (p *e2eProxy) url() string { return p.server.URL }

// serveHTTP records safe request fields and tees the bodies while forwarding.
func (p *e2eProxy) serveHTTP(w http.ResponseWriter, r *http.Request) {
	opID, doc, signed, class, targetID := e2eOperation(r.Method, r.URL.Path)
	rec := e2eExchange{
		method:           r.Method,
		opID:             opID,
		doc:              doc,
		signed:           signed,
		pathClass:        class,
		targetID:         targetID,
		hasAuthorization: r.Header.Get("Authorization") != "",
		hasCookie:        r.Header.Get("Cookie") != "",
		confirmDelete:    r.Header.Get("X-Ascii-Confirm-Delete"),
	}
	var reqTee *e2eTeeBody
	if signed {
		// The token is a bearer-equivalent secret. Remember it only for
		// output sweeping, never in a printable field.
		if token := path.Base(r.URL.Path); token != "" && token != "." && token != "/" {
			p.mu.Lock()
			p.signedTokens = append(p.signedTokens, token)
			p.mu.Unlock()
		}
	} else if r.Body != nil && r.Method != http.MethodGet && r.Method != http.MethodHead &&
		r.Method != http.MethodDelete && r.Method != http.MethodOptions {
		reqTee = &e2eTeeBody{ReadCloser: r.Body, limit: e2eRequestCaptureLimit}
		r.Body = reqTee
	}
	rw := &e2eResponseWriter{ResponseWriter: w, limit: e2eResponseCaptureLimit}
	// Finalize in a defer: ReverseProxy.ServeHTTP can panic with
	// http.ErrAbortHandler when a client closes a streaming response (the
	// SIGINT case). The exchange must still be recorded; the panic is not
	// recovered, so normal net/http abort handling is preserved.
	defer func() {
		if reqTee != nil {
			rec.reqBody = reqTee.buf.Bytes()
			rec.reqTruncated = reqTee.truncated
		}
		rec.status = rw.status
		if rec.status == 0 {
			rec.status = http.StatusOK
		}
		rec.header = rw.header
		if rec.header == nil {
			rec.header = http.Header{}
		}
		mediaType, _, err := mime.ParseMediaType(rec.header.Get("Content-Type"))
		rec.stream = err == nil && strings.EqualFold(mediaType, compat.NDJSONMediaType)
		rec.respBody = rw.buf.Bytes()
		rec.respTruncated = rw.truncated
		p.mu.Lock()
		p.exchanges = append(p.exchanges, rec)
		p.mu.Unlock()
	}()
	p.proxy.ServeHTTP(rw, r)
}

// e2eTeeBody captures a bounded copy of a request body while it is forwarded.
type e2eTeeBody struct {
	io.ReadCloser
	buf       bytes.Buffer
	limit     int
	truncated bool
}

// Read implements io.Reader.
func (b *e2eTeeBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	if n > 0 {
		if b.buf.Len() < b.limit {
			room := b.limit - b.buf.Len()
			if n > room {
				b.truncated = true
				b.buf.Write(p[:room])
			} else {
				b.buf.Write(p[:n])
			}
		} else {
			b.truncated = true
		}
	}
	return n, err
}

// e2eResponseWriter captures a bounded copy of a response while it is
// forwarded, preserving streaming through Flush.
type e2eResponseWriter struct {
	http.ResponseWriter
	status    int
	header    http.Header
	buf       bytes.Buffer
	limit     int
	truncated bool
}

// WriteHeader records the first status and header, then forwards.
func (w *e2eResponseWriter) WriteHeader(status int) {
	if w.status == 0 {
		w.status = status
		w.header = w.ResponseWriter.Header().Clone()
	}
	w.ResponseWriter.WriteHeader(status)
}

// Write records the body bytes, then forwards.
func (w *e2eResponseWriter) Write(p []byte) (int, error) {
	if w.status == 0 {
		w.WriteHeader(http.StatusOK)
	}
	if w.buf.Len() < w.limit {
		room := w.limit - w.buf.Len()
		if len(p) > room {
			w.truncated = true
			w.buf.Write(p[:room])
		} else {
			w.buf.Write(p)
		}
	} else {
		w.truncated = true
	}
	return w.ResponseWriter.Write(p)
}

// Flush implements http.Flusher.
func (w *e2eResponseWriter) Flush() {
	if flusher, ok := w.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}

// Unwrap exposes the underlying writer to http.ResponseController.
func (w *e2eResponseWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

// e2eOperation classifies one request. An API path that is not classified is
// reported as unclassified and fails validation instead of being skipped.
func e2eOperation(method, p string) (opID string, doc e2eDoc, signed bool, class, targetID string) {
	switch {
	case method == http.MethodGet && p == "/api/bbraft/info":
		return "bbraftInfo", e2eDocExtension, false, "extension-info", ""
	case method == http.MethodPut && strings.HasPrefix(p, "/api/bbraft/uploads/"):
		return "bbraftUploadReceiver", e2eDocExtension, true, "signed-upload", ""
	case method == http.MethodGet && strings.HasPrefix(p, "/api/bbraft/downloads/"):
		return "bbraftDownloadReceiver", e2eDocExtension, true, "signed-download", ""
	case method == http.MethodGet && p == "/api/v1/me":
		return "me", e2eDocAPI, false, "api", ""
	case p == "/api/v1/sandboxes":
		switch method {
		case http.MethodGet:
			return "sandboxes", e2eDocAPI, false, "api", ""
		case http.MethodPost:
			return "create", e2eDocAPI, false, "api", ""
		}
	case strings.HasPrefix(p, "/api/v1/sandboxes/"):
		parts := strings.Split(strings.TrimPrefix(p, "/api/v1/sandboxes/"), "/")
		id := parts[0]
		if id != "" {
			switch {
			case len(parts) == 1 && method == http.MethodGet:
				return "get", e2eDocAPI, false, "api", id
			case len(parts) == 1 && method == http.MethodPatch:
				return "update", e2eDocAPI, false, "api", id
			case len(parts) == 1 && method == http.MethodDelete:
				return "deleteSandbox", e2eDocAPI, false, "api", id
			case len(parts) == 2 && parts[1] == "commands" && method == http.MethodPost:
				return "command", e2eDocAPI, false, "api", id
			case len(parts) == 2 && parts[1] == "stop" && method == http.MethodPost:
				return "stop", e2eDocAPI, false, "api", id
			case len(parts) == 2 && parts[1] == "resume" && method == http.MethodPost:
				return "resume", e2eDocAPI, false, "api", id
			case len(parts) == 2 && parts[1] == "sshkey" && method == http.MethodPost:
				return "sshKey", e2eDocAPI, false, "api", id
			case len(parts) == 3 && parts[1] == "stops" && method == http.MethodGet:
				return "getStop", e2eDocAPI, false, "api", id
			case len(parts) == 3 && parts[1] == "commands" && method == http.MethodGet:
				return "commandStatus", e2eDocAPI, false, "api", id
			case len(parts) == 3 && parts[1] == "commands" && method == http.MethodDelete:
				return "stopCommand", e2eDocAPI, false, "api", id
			case len(parts) == 4 && parts[1] == "commands" && parts[3] == "stream" && method == http.MethodGet:
				return "attachCommand", e2eDocAPI, false, "api", id
			case len(parts) == 2 && parts[1] == "files" && method == http.MethodGet:
				return "readFile", e2eDocAPI, false, "api", id
			case len(parts) == 2 && parts[1] == "files" && method == http.MethodPut:
				return "writeFile", e2eDocAPI, false, "api", id
			case len(parts) == 2 && parts[1] == "files" && method == http.MethodDelete:
				return "deleteFile", e2eDocAPI, false, "api", id
			case len(parts) == 3 && parts[1] == "files" && method == http.MethodGet:
				switch parts[2] {
				case "list":
					return "listFiles", e2eDocAPI, false, "api", id
				case "info":
					return "getFileInfo", e2eDocAPI, false, "api", id
				}
			case len(parts) == 3 && parts[1] == "files" && method == http.MethodPost:
				switch parts[2] {
				case "upload-url":
					return "createUploadUrl", e2eDocAPI, false, "api", id
				case "download-url":
					return "createDownloadUrl", e2eDocAPI, false, "api", id
				case "folder":
					return "createFolder", e2eDocAPI, false, "api", id
				case "move":
					return "moveFile", e2eDocAPI, false, "api", id
				case "permissions":
					return "setFilePermissions", e2eDocAPI, false, "api", id
				}
			}
		}
	case strings.HasPrefix(p, "/api/v1/deletion-operations/"):
		if method == http.MethodGet {
			return "getDeletionOperation", e2eDocAPI, false, "api",
				strings.TrimPrefix(p, "/api/v1/deletion-operations/")
		}
	case p == "/api/v1/saved-snapshots":
		switch method {
		case http.MethodGet:
			return "listSavedSnapshots", e2eDocAPI, false, "api", ""
		case http.MethodPost:
			return "saveSavedSnapshot", e2eDocAPI, false, "api", ""
		}
	case strings.HasPrefix(p, "/api/v1/saved-snapshots/"):
		name := strings.TrimPrefix(p, "/api/v1/saved-snapshots/")
		if name != "" {
			switch method {
			case http.MethodGet:
				return "getSavedSnapshot", e2eDocAPI, false, "api", name
			case http.MethodDelete:
				return "deleteSavedSnapshot", e2eDocAPI, false, "api", name
			}
		}
	}
	if strings.HasPrefix(p, "/api/v1/") || strings.HasPrefix(p, "/api/bbraft/") {
		return "", e2eDocNone, false, "unclassified", ""
	}
	return "", e2eDocNone, false, "other", ""
}

// byOp returns a copy of the exchanges for one operation.
func (p *e2eProxy) byOp(opID string) []e2eExchange {
	p.mu.Lock()
	defer p.mu.Unlock()
	var out []e2eExchange
	for _, exchange := range p.exchanges {
		if exchange.opID == opID {
			out = append(out, exchange)
		}
	}
	return out
}

// last returns the newest exchange for one operation.
func (p *e2eProxy) last(opID string) (e2eExchange, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for i := len(p.exchanges) - 1; i >= 0; i-- {
		if p.exchanges[i].opID == opID {
			return p.exchanges[i], true
		}
	}
	return e2eExchange{}, false
}

// count returns the number of exchanges for one operation.
func (p *e2eProxy) count(opID string) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	count := 0
	for _, exchange := range p.exchanges {
		if exchange.opID == opID {
			count++
		}
	}
	return count
}

// tokens returns a copy of the observed signed upload tokens.
func (p *e2eProxy) tokens() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.signedTokens...)
}

// tokenHit reports whether data contains one observed signed token.
func (p *e2eProxy) tokenHit(data []byte) (string, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, token := range p.signedTokens {
		if bytes.Contains(data, []byte(token)) {
			return token, true
		}
	}
	return "", false
}

// validate checks every captured response against its document and enforces
// the transport assertions that must never be relaxed.
func (p *e2eProxy) validate(t *testing.T, v1, extension *compat.Validator) {
	t.Helper()
	p.mu.Lock()
	exchanges := append([]e2eExchange(nil), p.exchanges...)
	p.mu.Unlock()
	for _, exchange := range exchanges {
		if exchange.pathClass == "unclassified" {
			t.Errorf("proxy saw an unclassified API request (%s)", exchange.method)
			continue
		}
		if exchange.opID == "" {
			continue
		}
		// Sweep captured bodies for platform credentials/DSN/master/env
		// sentinels before any validator or diagnostic can echo them. The
		// legitimate upload-URL response may carry a signed URL; its token is
		// not in this list and is swept separately from CLI output/logs.
		if secret, hit := p.secrets.hit(p.secrets.logs, exchange.reqBody); hit {
			t.Errorf("%s request leaked %s", exchange.opID, secret)
			continue
		}
		if secret, hit := p.secrets.hit(p.secrets.bodies, exchange.reqBody); hit {
			t.Errorf("%s request leaked %s", exchange.opID, secret)
			continue
		}
		if secret, hit := p.secrets.hit(p.secrets.logs, exchange.respBody); hit {
			t.Errorf("%s response leaked %s", exchange.opID, secret)
			continue
		}
		if secret, hit := p.secrets.hit(p.secrets.bodies, exchange.respBody); hit {
			t.Errorf("%s response leaked %s", exchange.opID, secret)
			continue
		}
		if exchange.reqTruncated {
			t.Errorf("%s request exceeded the proxy capture bound", exchange.opID)
		}
		if exchange.respTruncated {
			t.Errorf("%s response exceeded the proxy capture bound", exchange.opID)
			continue
		}
		var validators []*compat.Validator
		switch exchange.doc {
		case e2eDocAPI:
			validators = []*compat.Validator{v1}
		case e2eDocExtension:
			validators = []*compat.Validator{extension}
		}
		for _, validator := range validators {
			var err error
			if exchange.stream {
				err = validator.NDJSON(exchange.opID, exchange.status, exchange.header, exchange.respBody)
			} else {
				err = validator.Response(exchange.opID, exchange.status, exchange.header, exchange.respBody)
			}
			if err != nil {
				// Validator errors can echo response values; report fields only.
				t.Errorf("%s response %d failed wire validation", exchange.opID, exchange.status)
			}
		}
		if (exchange.doc == e2eDocAPI || exchange.opID == "bbraftInfo") && !exchange.hasAuthorization {
			t.Errorf("%s request had no bearer", exchange.opID)
		}
		if exchange.signed && (exchange.hasAuthorization || exchange.hasCookie) {
			t.Errorf("%s request carried a bearer or cookie", exchange.opID)
		}
		if exchange.opID == "deleteSandbox" && exchange.confirmDelete != exchange.targetID {
			t.Errorf("deleteSandbox confirmation header did not match its target")
		}
	}
}

// e2eCLI runs the real bbraft binary with an isolated XDG_CONFIG_HOME.
type e2eCLI struct {
	binary     string
	apiURL     string
	configHome string
	env        []string
	secrets    *daemonProcessSecrets
	signed     *e2eProxy
}

// e2eNewCLI builds a CLI runner. The environment carries BBRAFT_API_URL and
// XDG_CONFIG_HOME but deliberately never BBRAFT_API_KEY after login.
func e2eNewCLI(t *testing.T, binary, apiURL string, secrets *daemonProcessSecrets, signed *e2eProxy) *e2eCLI {
	t.Helper()
	configHome := t.TempDir()
	return &e2eCLI{
		binary:     binary,
		apiURL:     apiURL,
		configHome: configHome,
		env:        e2eProcessEnv(configHome, apiURL),
		secrets:    secrets,
		signed:     signed,
	}
}

// e2eProcessEnv strips inherited credentials and sets the isolated config
// directory and explicit API URL. HOME is deliberately preserved.
func e2eProcessEnv(configHome, apiURL string) []string {
	base := daemonProcessEnv()
	env := make([]string, 0, len(base)+2)
	for _, entry := range base {
		name, _, _ := strings.Cut(entry, "=")
		switch name {
		case "XDG_CONFIG_HOME", "BBRAFT_API_URL", "BBRAFT_API_KEY":
			continue
		}
		env = append(env, entry)
	}
	return append(env, "XDG_CONFIG_HOME="+configHome, "BBRAFT_API_URL="+apiURL)
}

// e2eBuffer is a race-safe child output sink.
type e2eBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

// Write implements io.Writer.
func (b *e2eBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

// Bytes returns a copy of the output.
func (b *e2eBuffer) Bytes() []byte {
	b.mu.Lock()
	defer b.mu.Unlock()
	return bytes.Clone(b.buf.Bytes())
}

// Process lifetimes. Every started CLI process has a local deadline, a
// process-group SIGTERM/SIGKILL path and an unconditional Wait join after a
// force kill. A timeout fails the test safely instead of hanging the package.
const (
	e2eCommandTimeout = 5 * time.Minute
	e2eSignalTimeout  = 30 * time.Second
	e2eTermGrace      = 5 * time.Second
	e2ePTYReadTimeout = 10 * time.Second
)

// e2eCmd owns one started CLI process and its process group.
type e2eCmd struct {
	label  string
	cmd    *exec.Cmd
	cancel context.CancelFunc
	stdout *e2eBuffer
	stderr *e2eBuffer
	done   chan struct{}
}

// e2eStartCmd starts one CLI process and registers its fatal-path cleanup.
func e2eStartCmd(t *testing.T, label, binary, dir string, env []string, stdin io.Reader, args ...string) *e2eCmd {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	cmd := exec.CommandContext(ctx, binary, args...)
	cmd.Dir = dir
	cmd.Env = env
	cmd.Stdin = stdin
	var stdout, stderr e2eBuffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error { return e2eSignalGroup(cmd, syscall.SIGTERM) }
	cmd.WaitDelay = e2eTermGrace
	if err := cmd.Start(); err != nil {
		cancel()
		t.Fatalf("%s: start failed", label)
	}
	running := &e2eCmd{label: label, cmd: cmd, cancel: cancel, stdout: &stdout, stderr: &stderr, done: make(chan struct{})}
	go func() {
		_ = cmd.Wait()
		close(running.done)
	}()
	t.Cleanup(func() { running.stop(t) })
	return running
}

// e2eSignalGroup signals the whole process group, tolerating an already gone
// group.
func e2eSignalGroup(cmd *exec.Cmd, sig syscall.Signal) error {
	if cmd.Process == nil {
		return nil
	}
	err := syscall.Kill(-cmd.Process.Pid, sig)
	if errors.Is(err, syscall.ESRCH) {
		return nil
	}
	return err
}

// exited reports whether Wait returned.
func (c *e2eCmd) exited() bool {
	select {
	case <-c.done:
		return true
	default:
		return false
	}
}

// signal sends one signal to the CLI process.
func (c *e2eCmd) signal(sig os.Signal) error { return c.cmd.Process.Signal(sig) }

// stop cancels, then kills the whole group and joins Wait unconditionally.
func (c *e2eCmd) stop(t *testing.T) {
	t.Helper()
	if c.exited() {
		return
	}
	c.cancel()
	select {
	case <-c.done:
		return
	case <-time.After(e2eTermGrace):
	}
	_ = e2eSignalGroup(c.cmd, syscall.SIGKILL)
	// After force-kill there is no second timer: the process must be reaped.
	<-c.done
}

// wait joins the CLI within a local deadline and returns its output and code.
func (c *e2eCmd) wait(t *testing.T, timeout time.Duration) ([]byte, []byte, int) {
	t.Helper()
	select {
	case <-c.done:
	case <-time.After(timeout):
		c.stop(t)
		t.Fatalf("%s: process did not exit within %s", c.label, timeout)
	}
	code := -1
	if c.cmd.ProcessState != nil {
		code = c.cmd.ProcessState.ExitCode()
	}
	if code < 0 {
		t.Fatalf("%s: process did not exit normally", c.label)
	}
	return c.stdout.Bytes(), c.stderr.Bytes(), code
}

// start launches the CLI without waiting; the caller owns the wait deadline.
func (c *e2eCLI) start(t *testing.T, label, dir string, args ...string) *e2eCmd {
	t.Helper()
	return e2eStartCmd(t, label, c.binary, dir, c.env, nil, args...)
}

// run executes the CLI to completion within the local deadline.
func (c *e2eCLI) run(t *testing.T, label, dir string, stdin io.Reader, args ...string) ([]byte, []byte, int) {
	t.Helper()
	command := e2eStartCmd(t, label, c.binary, dir, c.env, stdin, args...)
	return command.wait(t, e2eCommandTimeout)
}

// checkSecrets rejects any CLI output carrying a credential or signed token.
func (c *e2eCLI) checkSecrets(t *testing.T, label string, outputs ...[]byte) {
	t.Helper()
	for _, data := range outputs {
		if secret, hit := c.secrets.hit(c.secrets.logs, data); hit {
			t.Fatalf("%s output leaked %s", label, secret)
		}
		if secret, hit := c.secrets.hit(c.secrets.bodies, data); hit {
			t.Fatalf("%s output leaked %s", label, secret)
		}
		if c.signed != nil {
			if _, hit := c.signed.tokenHit(data); hit {
				t.Fatalf("%s output leaked a signed upload token", label)
			}
		}
	}
}

// runOK requires exit 0.
func (c *e2eCLI) runOK(t *testing.T, label, dir string, args ...string) []byte {
	t.Helper()
	stdout, stderr, code := c.run(t, label, dir, nil, args...)
	c.checkSecrets(t, label, stdout, stderr)
	if code != 0 {
		t.Fatalf("%s: exit %d", label, code)
	}
	return stdout
}

// runExit requires one specific nonzero exit.
func (c *e2eCLI) runExit(t *testing.T, label, dir string, want int, args ...string) ([]byte, []byte) {
	t.Helper()
	stdout, stderr, code := c.run(t, label, dir, nil, args...)
	c.checkSecrets(t, label, stdout, stderr)
	if code != want {
		t.Fatalf("%s: exit %d, want %d", label, code, want)
	}
	return stdout, stderr
}

// login proves key-stdin persistence and that no raw key reaches output.
func (c *e2eCLI) login(t *testing.T, key string) {
	t.Helper()
	stdout, stderr, code := c.run(t, "login", t.TempDir(), strings.NewReader(key+"\n"), "login", "--key-stdin")
	c.checkSecrets(t, "login", stdout, stderr)
	if code != 0 {
		t.Fatalf("login: exit %d", code)
	}
	if bytes.Contains(stdout, []byte(key)) || bytes.Contains(stderr, []byte(key)) {
		t.Fatal("login printed the api key")
	}
	var event e2eLoginEvent
	e2eDecode(t, "login", stdout, &event)
	if event.Event != "logged_in" || event.APIURL != c.apiURL || event.User.ID == "" {
		t.Fatal("login event does not describe the authenticated user")
	}
	configPath := filepath.Join(c.configHome, "bbraft", "config.json")
	stat, err := os.Stat(configPath)
	if err != nil || stat.Mode().Perm() != 0o600 {
		t.Fatal("login did not persist a 0600 config file")
	}
	dirStat, err := os.Stat(filepath.Dir(configPath))
	if err != nil || dirStat.Mode().Perm() != 0o700 {
		t.Fatal("login did not persist a 0700 config directory")
	}
	raw, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatal("login config is not readable")
	}
	var saved struct {
		APIURL string `json:"apiUrl"`
		APIKey string `json:"apiKey"`
	}
	e2eDecode(t, "login config", raw, &saved)
	wantKey := sha256.Sum256([]byte(key))
	gotKey := sha256.Sum256([]byte(saved.APIKey))
	if saved.APIURL != c.apiURL || subtle.ConstantTimeCompare(gotKey[:], wantKey[:]) != 1 {
		t.Fatal("persisted login config differs")
	}
}

// list returns the raw sandbox list envelope.
func (c *e2eCLI) list(t *testing.T) api.SandboxListResponse {
	t.Helper()
	raw := c.runOK(t, "list", t.TempDir(), "list", "--json", "--all")
	var response api.SandboxListResponse
	e2eDecode(t, "list", raw, &response)
	if !response.OK || response.Type != api.TypeSandboxList {
		t.Fatal("list envelope mismatch")
	}
	return response
}

// info returns the raw sandbox info envelope.
func (c *e2eCLI) info(t *testing.T, id string) api.SandboxInfoResponse {
	t.Helper()
	raw := c.runOK(t, "info", t.TempDir(), "info", "--json", id)
	var response api.SandboxInfoResponse
	e2eDecode(t, "info", raw, &response)
	if !response.OK || response.Type != api.TypeSandboxInfo {
		t.Fatal("info envelope mismatch")
	}
	return response
}

// infoExpectError requires the 404-safe error event.
func (c *e2eCLI) infoExpectError(t *testing.T, id string) e2eErrorEvent {
	t.Helper()
	stdout, _ := c.runExit(t, "info error", t.TempDir(), 1, "info", "--json", id)
	return e2eErrorEventFrom(t, "info error", stdout)
}

// newSandbox creates and waits for a sandbox, parsing exactly one ready event.
func (c *e2eCLI) newSandbox(t *testing.T, extra ...string) string {
	t.Helper()
	args := append([]string{"new", "--json"}, extra...)
	raw := c.runOK(t, "new", t.TempDir(), args...)
	var ready []string
	created := ""
	for _, line := range bytes.Split(raw, []byte{'\n'}) {
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		var event e2eEvent
		e2eDecode(t, "new", line, &event)
		switch event.Event {
		case "created":
			created = event.ID
		case "ready":
			ready = append(ready, event.ID)
		}
	}
	if len(ready) != 1 || ready[0] == "" || created != ready[0] {
		t.Fatalf("new emitted %d ready events", len(ready))
	}
	return ready[0]
}

// newExpectError requires a create failure and returns its safe error event.
func (c *e2eCLI) newExpectError(t *testing.T, extra ...string) e2eErrorEvent {
	t.Helper()
	args := append([]string{"new", "--json"}, extra...)
	stdout, _ := c.runExit(t, "new error", t.TempDir(), 1, args...)
	return e2eErrorEventFrom(t, "new error", stdout)
}

// snapshot saves a name and returns the ready artifact id.
func (c *e2eCLI) snapshot(t *testing.T, id, name string) string {
	t.Helper()
	raw := c.runOK(t, "snapshot", t.TempDir(), "snapshot", id, name, "--json")
	var artifact string
	saving := false
	for _, line := range bytes.Split(raw, []byte{'\n'}) {
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		var event e2eSnapshotEvent
		e2eDecode(t, "snapshot", line, &event)
		switch event.Event {
		case "saving":
			saving = true
		case "ready":
			if event.Status != api.SavedSnapshotReady || !strings.HasPrefix(event.SnapshotID, "snap-") {
				t.Fatal("snapshot ready event has no artifact id")
			}
			artifact = event.SnapshotID
		}
	}
	if !saving || artifact == "" {
		t.Fatal("snapshot did not report saving then ready")
	}
	return artifact
}

// push copies one directory and returns the pushed event.
func (c *e2eCLI) push(t *testing.T, id, source, to string) e2ePushEvent {
	t.Helper()
	raw := c.runOK(t, "push", t.TempDir(), "push", id, source, "--to", to, "--json")
	var event e2ePushEvent
	e2eDecode(t, "push", raw, &event)
	if event.Event != "pushed" || event.SandboxID != id {
		t.Fatal("push event does not describe the pushed sandbox")
	}
	return event
}

// execFrames runs an explicit JSON streamed command.
func (c *e2eCLI) execFrames(t *testing.T, id, timeout, command string) ([]api.CommandStreamFrame, int) {
	t.Helper()
	args := []string{"exec", "--json", "--stream", "--timeout", timeout, id, command}
	stdout, stderr, code := c.run(t, "exec", t.TempDir(), nil, args...)
	c.checkSecrets(t, "exec", stdout, stderr)
	return e2eParseCommandFrames(t, "exec", stdout), code
}

// execArgvFrames runs the exact agent-harness argv form with explicit JSON streaming.
func (c *e2eCLI) execArgvFrames(t *testing.T, id, timeout string, argv ...string) ([]api.CommandStreamFrame, int) {
	t.Helper()
	args := []string{"exec", "--json", "--stream", "--timeout", timeout, id, "--"}
	args = append(args, argv...)
	stdout, stderr, code := c.run(t, "exec argv", t.TempDir(), nil, args...)
	c.checkSecrets(t, "exec argv", stdout, stderr)
	return e2eParseCommandFrames(t, "exec argv", stdout), code
}

// e2eParseCommandFrames decodes one JSON stream frame per nonempty line.
func e2eParseCommandFrames(t *testing.T, label string, data []byte) []api.CommandStreamFrame {
	t.Helper()
	var frames []api.CommandStreamFrame
	for _, line := range bytes.Split(data, []byte{'\n'}) {
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		var frame api.CommandStreamFrame
		e2eDecode(t, label, line, &frame)
		frames = append(frames, frame)
	}
	return frames
}

// e2eHasStartedFrame reports whether data contains a complete parsed started
// frame. It is used as a running-process barrier, so a partial line is ignored.
func e2eHasStartedFrame(data []byte) bool {
	for _, line := range bytes.Split(data, []byte{'\n'}) {
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		var frame api.CommandStreamFrame
		if json.Unmarshal(line, &frame) != nil {
			continue
		}
		if frame.Type == api.CommandFrameStarted {
			return true
		}
	}
	return false
}

// e2eKillOwnedGroup kills the nodefake-owned remote process group on fatal
// paths. It only acts while a recorded PID still has its original start time
// and belongs to the shell's group, so a reused PID or unrelated group is
// never touched.
func e2eKillOwnedGroup(shellPID, childPID int, shellStart, childStart uint64) {
	if shellPID <= 0 {
		return
	}
	owned := false
	for _, process := range []struct {
		pid   int
		start uint64
	}{{shellPID, shellStart}, {childPID, childStart}} {
		if process.pid <= 0 || process.start == 0 {
			continue
		}
		if start, ok := e2eProcessStartTime(process.pid); ok && start == process.start {
			if pgid, err := syscall.Getpgid(process.pid); err == nil && pgid == shellPID {
				owned = true
				break
			}
		}
	}
	if owned {
		_ = syscall.Kill(-shellPID, syscall.SIGKILL)
	}
}

// e2eProcessStartTime returns the kernel start time of pid from
// /proc/<pid>/stat, which distinguishes a live process from a later reuse of
// the same PID.
func e2eProcessStartTime(pid int) (uint64, bool) {
	if pid <= 0 {
		return 0, false
	}
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return 0, false
	}
	end := bytes.LastIndexByte(data, ')')
	if end < 0 || end+2 >= len(data) {
		return 0, false
	}
	fields := strings.Fields(string(data[end+2:]))
	// After the comm field, fields[19] is starttime (field 22 overall).
	if len(fields) <= 19 {
		return 0, false
	}
	start, err := strconv.ParseUint(fields[19], 10, 64)
	if err != nil {
		return 0, false
	}
	return start, true
}

// e2eProcessNotRunning treats a missing process or a zombie as stopped.
func e2eProcessNotRunning(pid int) bool {
	if pid <= 0 {
		return true
	}
	if err := syscall.Kill(pid, 0); err != nil {
		return errors.Is(err, syscall.ESRCH)
	}
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return errors.Is(err, os.ErrNotExist)
	}
	// The comm field may contain spaces and parentheses; state follows the
	// last ')'.
	end := bytes.LastIndexByte(data, ')')
	if end < 0 || end+2 >= len(data) {
		return false
	}
	state := data[end+2]
	return state == 'Z' || state == 'X'
}

// execSync runs one synchronous JSON command.
func (c *e2eCLI) execSync(t *testing.T, id, command string) api.CommandResponse {
	t.Helper()
	raw := c.runOK(t, "exec sync", t.TempDir(), "exec", "--json", id, command)
	var response api.CommandResponse
	e2eDecode(t, "exec sync", raw, &response)
	if !response.OK || response.Type != api.TypeCommandFinished {
		t.Fatal("exec sync envelope mismatch")
	}
	return response
}

// deleteWait deletes a sandbox and requires the completed operation.
func (c *e2eCLI) deleteWait(t *testing.T, id string) string {
	t.Helper()
	raw := c.runOK(t, "delete", t.TempDir(), "delete", id, "--yes", "--wait", "--json")
	var response api.DeletionOperationResponse
	e2eDecode(t, "delete", raw, &response)
	if !response.OK || response.Type != api.TypeDeletionOperation ||
		response.Operation.Status != api.DeletionCompleted ||
		response.Operation.Kind != api.DeletionKindSandbox ||
		response.Operation.TargetID != id || response.Operation.CompletedAt == nil {
		t.Fatal("delete did not return its completed operation")
	}
	return response.Operation.ID
}

// runJSON runs a temporary-sandbox command in explicit JSON mode.
func (c *e2eCLI) runJSON(t *testing.T, dir string, extra ...string) ([]api.CommandStreamFrame, e2eRunSummary, []e2eErrorEvent, int) {
	t.Helper()
	args := append([]string{"run", "--json"}, extra...)
	stdout, stderr, code := c.run(t, "run", dir, nil, args...)
	c.checkSecrets(t, "run", stdout, stderr)
	frames, summary, errs := e2eParseRunOutput(t, stdout)
	return frames, summary, errs, code
}

// e2ePTY owns one CLI process whose stdout is a raw pseudo-terminal. The PTY
// reader and the process are joined on every path, including a timeout.
type e2ePTY struct {
	label    string
	cmd      *exec.Cmd
	cancel   context.CancelFunc
	master   *os.File
	stderr   *e2eBuffer
	stdout   bytes.Buffer
	readDone chan struct{}
	done     chan struct{}
}

// e2eStartPTY starts the CLI with stdout on a raw pseudo-terminal.
func e2eStartPTY(t *testing.T, label, binary, dir string, env []string, args ...string) *e2ePTY {
	t.Helper()
	master, slave := e2eOpenPTY(t)
	ctx, cancel := context.WithCancel(t.Context())
	cmd := exec.CommandContext(ctx, binary, args...)
	cmd.Dir = dir
	cmd.Env = env
	cmd.Stdout = slave
	var stderr e2eBuffer
	cmd.Stderr = &stderr
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error { return e2eSignalGroup(cmd, syscall.SIGTERM) }
	cmd.WaitDelay = e2eTermGrace
	if err := cmd.Start(); err != nil {
		slave.Close()
		master.Close()
		cancel()
		t.Fatalf("%s: start failed", label)
	}
	slave.Close()
	pty := &e2ePTY{
		label:    label,
		cmd:      cmd,
		cancel:   cancel,
		master:   master,
		stderr:   &stderr,
		readDone: make(chan struct{}),
		done:     make(chan struct{}),
	}
	go func() {
		_ = cmd.Wait()
		close(pty.done)
	}()
	go func() {
		defer close(pty.readDone)
		buf := make([]byte, 32<<10)
		for {
			n, err := pty.master.Read(buf)
			if n > 0 {
				pty.stdout.Write(buf[:n])
			}
			if err != nil {
				return
			}
		}
	}()
	t.Cleanup(func() { pty.stop(t) })
	return pty
}

// exited reports whether Wait returned.
func (p *e2ePTY) exited() bool {
	select {
	case <-p.done:
		return true
	default:
		return false
	}
}

// stop kills and reaps the process group, then closes and joins the PTY read.
func (p *e2ePTY) stop(t *testing.T) {
	t.Helper()
	if !p.exited() {
		p.cancel()
		select {
		case <-p.done:
		case <-time.After(e2eTermGrace):
			_ = e2eSignalGroup(p.cmd, syscall.SIGKILL)
			// After force-kill there is no second timer.
			<-p.done
		}
	}
	p.master.Close()
	<-p.readDone
}

// wait joins the process within a local deadline and then joins the reader.
func (p *e2ePTY) wait(t *testing.T, timeout time.Duration) ([]byte, []byte, int) {
	t.Helper()
	select {
	case <-p.done:
	case <-time.After(timeout):
		p.stop(t)
		t.Fatalf("%s: process did not exit within %s", p.label, timeout)
	}
	select {
	case <-p.readDone:
	case <-time.After(e2ePTYReadTimeout):
		p.master.Close()
		<-p.readDone
	}
	code := -1
	if p.cmd.ProcessState != nil {
		code = p.cmd.ProcessState.ExitCode()
	}
	if code < 0 {
		t.Fatalf("%s: process did not exit normally", p.label)
	}
	return p.stdout.Bytes(), p.stderr.Bytes(), code
}

// runTTY runs the CLI with stdout on a pseudo-terminal, which is the only way
// to select exec's raw output mode when the parent's stdout is a pipe.
func (c *e2eCLI) runTTY(t *testing.T, label string, args ...string) ([]byte, []byte, int) {
	t.Helper()
	pty := e2eStartPTY(t, label, c.binary, t.TempDir(), c.env, args...)
	stdout, stderr, code := pty.wait(t, e2eCommandTimeout)
	c.checkSecrets(t, label, stdout, stderr)
	return stdout, stderr, code
}

// e2eOpenPTY opens a raw pseudo-terminal pair without a new dependency.
func e2eOpenPTY(t *testing.T) (*os.File, *os.File) {
	t.Helper()
	master, err := os.OpenFile("/dev/ptmx", os.O_RDWR|syscall.O_NOCTTY, 0)
	if err != nil {
		t.Fatal("pty: open master failed")
	}
	if err := unix.IoctlSetPointerInt(int(master.Fd()), unix.TIOCSPTLCK, 0); err != nil {
		master.Close()
		t.Fatal("pty: unlock failed")
	}
	number, err := unix.IoctlGetInt(int(master.Fd()), unix.TIOCGPTN)
	if err != nil {
		master.Close()
		t.Fatal("pty: number failed")
	}
	slave, err := os.OpenFile(fmt.Sprintf("/dev/pts/%d", number), os.O_RDWR|syscall.O_NOCTTY, 0)
	if err != nil {
		master.Close()
		t.Fatal("pty: open slave failed")
	}
	termios, err := unix.IoctlGetTermios(int(slave.Fd()), unix.TCGETS)
	if err != nil {
		master.Close()
		slave.Close()
		t.Fatal("pty: get termios failed")
	}
	termios.Lflag &^= unix.ECHO | unix.ICANON | unix.ISIG
	termios.Iflag &^= unix.ICRNL | unix.IXON
	termios.Oflag &^= unix.OPOST
	if err := unix.IoctlSetTermios(int(slave.Fd()), unix.TCSETS, termios); err != nil {
		master.Close()
		slave.Close()
		t.Fatal("pty: set termios failed")
	}
	return master, slave
}

// e2eDecode decodes one JSON value without echoing the decoder's text.
func e2eDecode(t *testing.T, label string, data []byte, out any) {
	t.Helper()
	if err := json.Unmarshal(data, out); err != nil {
		t.Fatalf("%s: malformed JSON", label)
	}
}

// e2eErrorEventFrom returns the first error event in JSON Lines output.
func e2eErrorEventFrom(t *testing.T, label string, data []byte) e2eErrorEvent {
	t.Helper()
	for _, line := range bytes.Split(data, []byte{'\n'}) {
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		var event e2eErrorEvent
		e2eDecode(t, label, line, &event)
		if event.Event == "error" {
			return event
		}
	}
	t.Fatalf("%s: no error event", label)
	return e2eErrorEvent{}
}

// e2eParseRunOutput requires exactly one terminal summary and no output after
// it, then returns the frames, summary and any error events.
func e2eParseRunOutput(t *testing.T, data []byte) ([]api.CommandStreamFrame, e2eRunSummary, []e2eErrorEvent) {
	t.Helper()
	var frames []api.CommandStreamFrame
	var summary e2eRunSummary
	var errs []e2eErrorEvent
	summaries := 0
	terminal := false
	for _, line := range bytes.Split(data, []byte{'\n'}) {
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		if terminal {
			t.Fatal("run: output after the terminal summary")
		}
		var head struct {
			Event string `json:"event"`
		}
		e2eDecode(t, "run", line, &head)
		switch head.Event {
		case "summary":
			summaries++
			if summaries > 1 {
				t.Fatal("run: duplicate terminal summary")
			}
			e2eDecode(t, "run summary", line, &summary)
			terminal = true
		case "error":
			var event e2eErrorEvent
			e2eDecode(t, "run error", line, &event)
			errs = append(errs, event)
		default:
			var frame api.CommandStreamFrame
			e2eDecode(t, "run frame", line, &frame)
			frames = append(frames, frame)
		}
	}
	if summaries != 1 {
		t.Fatal("run: expected exactly one terminal summary")
	}
	return frames, summary, errs
}

// e2eBuildCLI builds the real CLI once, after the service/spec gate.
func e2eBuildCLI(t *testing.T, root string) string {
	t.Helper()
	binary := filepath.Join(t.TempDir(), "bbraft")
	ctx, cancel := context.WithTimeout(t.Context(), daemonProcessBuildTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, "go", "build", "-buildvcs=false", "-trimpath", "-o", binary, "./cmd/bbraft")
	cmd.Dir = root
	cmd.Env = daemonProcessEnv()
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard
	if err := cmd.Run(); err != nil {
		if ctx.Err() != nil {
			t.Fatal("bbraft build timed out")
		}
		t.Fatal("bbraft build failed")
	}
	stat, err := os.Stat(binary)
	if err != nil || !stat.Mode().IsRegular() {
		t.Fatal("bbraft build produced no binary")
	}
	return binary
}

// e2eSetArg replaces one flag value, or appends the pair when absent.
func e2eSetArg(args []string, name, value string) []string {
	out := append([]string(nil), args...)
	for i := 0; i+1 < len(out); i++ {
		if out[i] == name {
			out[i+1] = value
			return out
		}
	}
	return append(out, name, value)
}

// e2eEvent is one new event.
type e2eEvent struct {
	Event string `json:"event"`
	ID    string `json:"id"`
	State string `json:"state"`
}

// e2eLoginEvent is login's single event.
type e2eLoginEvent struct {
	Event  string `json:"event"`
	APIURL string `json:"apiUrl"`
	User   struct {
		ID string `json:"id"`
	} `json:"user"`
}

// e2ePushEvent is push's single event.
type e2ePushEvent struct {
	Event     string `json:"event"`
	SandboxID string `json:"sandboxId"`
	Directory string `json:"directory"`
	Files     int    `json:"files"`
	Bytes     int64  `json:"bytes"`
}

// e2eSnapshotEvent is one snapshot event.
type e2eSnapshotEvent struct {
	Event      string                  `json:"event"`
	Name       string                  `json:"name"`
	Status     api.SavedSnapshotStatus `json:"status"`
	SnapshotID string                  `json:"snapshotId"`
}

// e2eRunSummary is run's terminal summary.
type e2eRunSummary struct {
	Event          string  `json:"event"`
	ExitCode       int     `json:"exitCode"`
	ElapsedSeconds int64   `json:"elapsedSeconds"`
	Dollars        float64 `json:"dollars"`
	SandboxType    string  `json:"sandboxType"`
	Deleted        bool    `json:"deleted"`
}

// e2eErrorEvent is the safe JSON error line.
type e2eErrorEvent struct {
	Event  string `json:"event"`
	Error  string `json:"error"`
	Code   string `json:"code"`
	Status int    `json:"status"`
}

// e2eCreateBody is the subset of create requests the test asserts.
type e2eCreateBody struct {
	Type       *string `json:"type,omitzero"`
	TTLSeconds *int    `json:"ttlSeconds,omitzero"`
	From       *string `json:"from,omitzero"`
}

// e2eCommandBody is the subset of command requests the test asserts.
type e2eCommandBody struct {
	Command        string  `json:"command"`
	Cwd            *string `json:"cwd,omitzero"`
	TimeoutSeconds *int    `json:"timeoutSeconds,omitzero"`
	Stream         *bool   `json:"stream,omitzero"`
}

// e2eUploadBody is the upload-URL request body.
type e2eUploadBody struct {
	Path       string `json:"path"`
	TTLSeconds *int   `json:"ttlSeconds,omitzero"`
}

// e2ePushFixture is the >5MiB push source and its expected entries.
type e2ePushFixture struct {
	dir     string
	big     []byte
	entries []string
	marker  string
	files   int
}

// e2eMakePushDir writes the >5MiB push fixture with executable, symlink and
// unusual names.
func e2eMakePushDir(t *testing.T) e2ePushFixture {
	t.Helper()
	dir := t.TempDir()
	big := bytes.Repeat([]byte("bamboo-raft-e2e-"), (5<<20)/len("bamboo-raft-e2e-")+100)
	files := []struct {
		name string
		data []byte
		mode os.FileMode
	}{
		{"big.bin", big, 0o644},
		{"run.sh", []byte("#!/bin/sh\necho e2e\n"), 0o755},
		{"space name.txt", []byte("space"), 0o644},
		{"quote's.txt", []byte("quote"), 0o644},
		{"dollar$name.txt", []byte("dollar"), 0o644},
		{"-leading-dash.txt", []byte("dash"), 0o644},
		{"üñïçøde.txt", []byte("unicode"), 0o644},
		{"new\nline.txt", []byte("newline"), 0o644},
		{"marker.txt", []byte("e2e-push-marker"), 0o644},
	}
	entries := make([]string, 0, len(files)+1)
	for _, file := range files {
		full := filepath.Join(dir, file.name)
		if err := os.WriteFile(full, file.data, file.mode); err != nil {
			t.Fatal("push fixture write failed")
		}
		if err := os.Chmod(full, file.mode); err != nil {
			t.Fatal("push fixture chmod failed")
		}
		entries = append(entries, file.name)
	}
	if err := os.Symlink("big.bin", filepath.Join(dir, "link")); err != nil {
		t.Fatal("push fixture symlink failed")
	}
	entries = append(entries, "link")
	sort.Strings(entries)
	return e2ePushFixture{
		dir:     dir,
		big:     big,
		entries: entries,
		marker:  "e2e-push-marker",
		files:   len(entries),
	}
}

// e2eMakeSmallDir writes a one-file directory for push and run fixtures.
func e2eMakeSmallDir(t *testing.T, name, content string) string {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o644); err != nil {
		t.Fatal("fixture file write failed")
	}
	return dir
}

// e2eHarness owns the fixture, proxies, CLI runners and node wrapper.
type e2eHarness struct {
	fixture       *daemonProcessFixture
	node          *e2eNode
	apiProxy      *e2eProxy
	signedProxy   *e2eProxy
	cli           *e2eCLI
	otherCLI      *e2eCLI
	v1            *compat.Validator
	extension     *compat.Validator
	metadataReads int
}

// TestE2ECompiledCLI drives the real CLI against the real compiled daemon.
func TestE2ECompiledCLI(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("compiled CLI/server E2E tests use /proc, process groups, signals and a PTY")
	}
	// Gate before any build: no database means a skip, and REQUIRE_ALL means a
	// failure. The v1 API document is loaded before any process starts.
	dbtest.PoolConfig(t)
	v1, extension := daemonProcessValidators(t)
	root := compat.Root(t)

	serverBinary := daemonProcessBuild(t, root)
	cliBinary := e2eBuildCLI(t, root)

	f := newDaemonProcessFixture(t, serverBinary, root, v1, extension)
	account, err := f.inspection.EnsureAccount(t.Context(), f.owner)
	if err != nil {
		t.Fatal("owner account setup failed")
	}
	f.ownerID = account.ID

	// This harness is the explicit old-server compatibility path: its info
	// response omits commands.detached so the CLI keeps the legacy streamed
	// command behavior the assertions below describe. The phase-exit harness
	// reads the real response and covers the detached capability.
	apiProxy := newE2EProxyWithOptions(t, "http://"+f.apiAddr, f.secrets, e2eProxyOptions{omitInfoDetached: true})
	signedProxy := newE2EProxy(t, "http://"+f.apiAddr, f.secrets)

	args := e2eSetArg(f.args("all", f.files.dbAll), "-server.public-url", signedProxy.url())
	f.startProcess(t, "all", args)
	f.waitAPI(t)
	f.waitDBBackend(t, f.allApp)
	node := e2eStartNode(t, f)
	f.waitNodeFresh(t, time.Time{})

	cli := e2eNewCLI(t, cliBinary, apiProxy.url(), f.secrets, signedProxy)
	otherCLI := e2eNewCLI(t, cliBinary, apiProxy.url(), f.secrets, signedProxy)
	h := &e2eHarness{
		fixture:     f,
		node:        node,
		apiProxy:    apiProxy,
		signedProxy: signedProxy,
		cli:         cli,
		otherCLI:    otherCLI,
		v1:          v1,
		extension:   extension,
	}
	// Registered after the daemon/node and before the CLI processes: LIFO
	// means the CLI processes stop first, then this validation runs, then the
	// node/daemon stop and the proxies close.
	t.Cleanup(func() { h.validateWire(t) })
	h.runFlow(t)
}

// runFlow is the ordered end-to-end scenario.
func (h *e2eHarness) runFlow(t *testing.T) {
	h.loginAndScope(t)
	base := h.createBase(t)
	h.checkInfoList(t, base)
	h.checkForeign(t, base)
	h.execRaw(t, base)
	h.execJSONStream(t, base)
	h.execTestVMArgv(t, base)
	h.execTimeout(t, base)

	pushFixture := e2eMakePushDir(t)
	h.pushBig(t, base, pushFixture)
	h.pushRelative(t, base, e2eMakeSmallDir(t, "small.txt", "small-push"))

	snap1 := h.snapshot(t, base, "base")
	snap2 := h.snapshot(t, base, "base")
	h.checkReplacement(t, base, snap1, snap2)
	h.checkShrink(t, base)

	restoredInherit := h.restore(t, snap2, "", false)
	restoredLarge := h.restore(t, snap2, "large", true)
	restoredXlarge := h.restore(t, snap2, "xlarge", true)
	h.deleteWait(t, restoredInherit)
	h.deleteWait(t, restoredLarge)
	h.deleteWait(t, restoredXlarge)

	// Source VM deletion must be durably complete before a later restore.
	h.deleteWait(t, base)
	restoredAfterDelete := h.restore(t, snap2, "large", true)
	response := h.cli.execSync(t, restoredAfterDelete, e2eRestoreCatCommand)
	if response.Stdout != pushFixture.marker {
		t.Fatal("restored file content differs")
	}
	h.deleteWait(t, restoredAfterDelete)

	h.runNonzero(t)
	h.runSignal(t)
	h.runHuman(t)
	h.checkUnsupported(t)

	h.fixture.waitFor(t, 15*time.Second, "node reservations released", func() bool {
		node, err := h.fixture.inspection.Node(t.Context(), h.fixture.nodeID)
		return err == nil && node.Reserved == (db.Resources{})
	})
}

// loginAndScope proves key-stdin persistence and owner scoping.
func (h *e2eHarness) loginAndScope(t *testing.T) {
	h.cli.login(t, h.fixture.apiKey)
	if list := h.cli.list(t); len(list.Sandboxes) != 0 {
		t.Fatalf("initial list has %d sandboxes", len(list.Sandboxes))
	}
	h.otherCLI.login(t, h.fixture.otherKey)
	if list := h.otherCLI.list(t); len(list.Sandboxes) != 0 {
		t.Fatalf("other owner list has %d sandboxes", len(list.Sandboxes))
	}
	mainDigest := sha256.Sum256([]byte(h.fixture.apiKey))
	mainKey, err := h.fixture.inspection.APIKeyBySHA256(t.Context(), mainDigest)
	if err != nil {
		t.Fatal("main api key row is missing")
	}
	otherDigest := sha256.Sum256([]byte(h.fixture.otherKey))
	otherKey, err := h.fixture.inspection.APIKeyBySHA256(t.Context(), otherDigest)
	if err != nil {
		t.Fatal("other api key row is missing")
	}
	if mainKey.AccountID != h.fixture.ownerID || otherKey.AccountID == mainKey.AccountID {
		t.Fatal("configured keys are not owner-scoped")
	}
	if mainKey.LastUsedAt == nil || otherKey.LastUsedAt == nil {
		t.Fatal("api key last-used was not persisted")
	}
}

// createBase creates the large source sandbox used by every flow.
func (h *e2eHarness) createBase(t *testing.T) string {
	id := h.cli.newSandbox(t, "--type", "large", "--ttl", "600", "--name", "e2e-base")
	body := h.lastCreate(t)
	if body.Type == nil || *body.Type != "large" || body.TTLSeconds == nil || *body.TTLSeconds != 600 {
		t.Fatal("base create request is not large/600")
	}
	h.fixture.waitJobDone(t, id, db.JobProvision)
	sandbox := h.sandbox(t, id)
	if sandbox.State != db.StateReady || sandbox.MachineType != "large" || sandbox.Reserved.DiskGiB != 111 {
		t.Fatalf("base sandbox state/type/disk = %s/%s/%d", sandbox.State, sandbox.MachineType, sandbox.Reserved.DiskGiB)
	}
	if sandbox.NodeID == nil || *sandbox.NodeID != h.fixture.nodeID {
		t.Fatal("base sandbox is not on the test node")
	}
	if counts := h.node.counts(); counts.CreateVM < 1 {
		t.Fatalf("base create node calls = %d, want at least 1", counts.CreateVM)
	}
	return id
}

// checkInfoList exercises list and info through the real CLI.
func (h *e2eHarness) checkInfoList(t *testing.T, id string) {
	info := h.cli.info(t, id)
	if info.Sandbox.ID != id || info.Sandbox.State != api.StateReady || info.Sandbox.Type != "large" {
		t.Fatal("info does not describe the ready large sandbox")
	}
	found := false
	for _, sandbox := range h.cli.list(t).Sandboxes {
		if sandbox.ID == id {
			found = true
			if sandbox.Type != "large" {
				t.Fatal("list sandbox type differs")
			}
		}
	}
	if !found {
		t.Fatal("list does not contain the base sandbox")
	}
}

// checkForeign proves another owner cannot read the base sandbox.
func (h *e2eHarness) checkForeign(t *testing.T, id string) {
	event := h.otherCLI.infoExpectError(t, id)
	if event.Code != "not_found" || event.Status != http.StatusNotFound {
		t.Fatalf("foreign info error = %q/%d", event.Code, event.Status)
	}
}

// execRaw exercises exec's raw streaming mode through a PTY.
func (h *e2eHarness) execRaw(t *testing.T, id string) {
	before := h.node.counts().Exec
	stdout, stderr, code := h.cli.runTTY(t, "exec raw", "exec", "--stream", "--timeout", "5", id,
		e2eExecCode7Command)
	if code != 7 {
		t.Fatalf("raw exec exit = %d, want 7", code)
	}
	if !bytes.Contains(stdout, []byte("out")) {
		t.Fatal("raw exec stdout is missing output")
	}
	if !bytes.Contains(stderr, []byte("err")) {
		t.Fatal("raw exec stderr is missing output")
	}
	if h.node.counts().Exec <= before {
		t.Fatal("raw exec did not reach the node")
	}
	command := h.lastCommand(t)
	if command.TimeoutSeconds == nil || *command.TimeoutSeconds != 5 ||
		command.Stream == nil || !*command.Stream {
		t.Fatal("raw exec request protocol differs")
	}
}

// execJSONStream exercises explicit JSON streaming and the timeout-0 metadata
// discovery exactly once.
func (h *e2eHarness) execJSONStream(t *testing.T, id string) {
	h.metadataReads++
	frames, code := h.cli.execFrames(t, id, "0", e2eExecCode7Command)
	if code != 7 {
		t.Fatalf("json exec exit = %d, want 7", code)
	}
	h.checkExitFrame(t, frames, 7, false)
	h.checkFrameOutput(t, frames, "out")
	command := h.lastCommand(t)
	if command.TimeoutSeconds == nil || *command.TimeoutSeconds != 901 ||
		command.Stream == nil || !*command.Stream {
		t.Fatal("timeout-0 exec did not send the discovered stream cap")
	}
}

// execTestVMArgv drives the exact agent-harness argv form:
// exec ID --timeout 0 --stream --json -- bash -lc CMD.
func (h *e2eHarness) execTestVMArgv(t *testing.T, id string) {
	h.metadataReads++
	frames, code := h.cli.execArgvFrames(t, id, "0", "bash", "-lc", e2eExecCode7Command)
	if code != 7 {
		t.Fatalf("agent-harness argv exec exit = %d, want 7", code)
	}
	h.checkExitFrame(t, frames, 7, false)
	h.checkFrameOutput(t, frames, "out")
	command := h.lastCommand(t)
	if command.TimeoutSeconds == nil || *command.TimeoutSeconds != 901 ||
		command.Stream == nil || !*command.Stream {
		t.Fatal("agent-harness argv exec did not send the discovered stream cap")
	}
}

// execTimeout exercises the remote timeout exit status 124.
func (h *e2eHarness) execTimeout(t *testing.T, id string) {
	frames, code := h.cli.execFrames(t, id, "1", e2eExecTimeoutCommand)
	if code != 124 {
		t.Fatalf("timeout exec exit = %d, want 124", code)
	}
	h.checkExitFrame(t, frames, 124, true)
	command := h.lastCommand(t)
	if command.TimeoutSeconds == nil || *command.TimeoutSeconds != 1 {
		t.Fatal("timeout exec request differs")
	}
}

// checkExitFrame validates the terminal frame of a streamed command.
func (h *e2eHarness) checkExitFrame(t *testing.T, frames []api.CommandStreamFrame, want int, timedOut bool) {
	if len(frames) == 0 || frames[0].Type != api.CommandFrameStarted {
		t.Fatal("command stream does not start with started")
	}
	last := frames[len(frames)-1]
	if last.Type != api.CommandFrameExit {
		t.Fatal("command stream has no terminal exit frame")
	}
	if !last.Success.Present || last.Success.Value == nil || *last.Success.Value != (want == 0) {
		t.Fatal("exit frame success flag differs")
	}
	if !last.TimedOut.Present || last.TimedOut.Value == nil || *last.TimedOut.Value != timedOut {
		t.Fatal("exit frame timedOut flag differs")
	}
	if timedOut {
		if !last.ExitCode.Present || last.ExitCode.Value != nil {
			t.Fatal("timeout exit frame must carry a null exitCode")
		}
		return
	}
	if !last.ExitCode.Present || last.ExitCode.Value == nil || *last.ExitCode.Value != want {
		t.Fatal("exit frame exitCode differs")
	}
}

// checkFrameOutput requires one stdout substring across the stream frames.
func (h *e2eHarness) checkFrameOutput(t *testing.T, frames []api.CommandStreamFrame, want string) {
	var output strings.Builder
	for _, frame := range frames {
		if frame.Type == api.CommandFrameStdout {
			output.WriteString(frame.Data)
		}
	}
	if !strings.Contains(output.String(), want) {
		t.Fatalf("command output is missing %q", want)
	}
}

// pushBig exercises the >5MiB cross-origin signed upload and confined tar
// extraction, including --to.
func (h *e2eHarness) pushBig(t *testing.T, id string, fixture e2ePushFixture) {
	beforePut := h.node.counts().PutFile
	beforeExtract := h.node.counts().Extract
	event := h.cli.push(t, id, fixture.dir, "/home/user/target")
	if event.Directory != "/home/user/target" || event.Files != fixture.files || event.Bytes <= 5<<20 {
		t.Fatalf("push event dir/files/bytes = %q/%d/%d", event.Directory, event.Files, event.Bytes)
	}
	counts := h.node.counts()
	if counts.PutFile != beforePut+1 || counts.Extract != beforeExtract+1 {
		t.Fatal("push did not reach the node exactly once")
	}
	put, ok := h.node.lastPutFile()
	if !ok || put.size <= 5<<20 || !strings.HasPrefix(put.path, "/tmp/bbraft-push-") {
		t.Fatal("uploaded archive record is missing or too small")
	}
	extract, ok := h.node.lastExtract()
	if !ok || extract.destination != "/home/user/target" || extract.timeout != 600 {
		t.Fatal("push extraction protocol differs")
	}
	h.checkTarEntries(t, extract, fixture)
	h.checkExtracted(t, h.vmDir(id), "/home/user/target", fixture)
	archiveRel := filepath.Join("tmp", path.Base(extract.archivePath))
	if _, err := os.Stat(filepath.Join(h.vmDir(id), archiveRel)); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("uploaded archive was not removed")
	}
	upload := h.lastUploadURL(t)
	if !strings.HasPrefix(upload.Path, "/tmp/bbraft-push-") || !strings.HasSuffix(upload.Path, ".tar") ||
		upload.TTLSeconds == nil || *upload.TTLSeconds != 3600 {
		t.Fatal("upload URL request is not a push archive")
	}
	command := h.lastCommand(t)
	if command.TimeoutSeconds == nil || *command.TimeoutSeconds != 600 ||
		(command.Stream != nil && *command.Stream) || !strings.Contains(command.Command, "tar -xf") {
		t.Fatal("push extraction command protocol differs")
	}
}

// pushRelative exercises a relative --to destination.
func (h *e2eHarness) pushRelative(t *testing.T, id, source string) {
	event := h.cli.push(t, id, source, "nested/relative")
	if event.Directory != "/home/user/nested/relative" || event.Files != 1 {
		t.Fatalf("relative push event = %q/%d", event.Directory, event.Files)
	}
	extract, ok := h.node.lastExtract()
	if !ok || extract.destination != "/home/user/nested/relative" {
		t.Fatal("relative push extraction destination differs")
	}
	data, err := os.ReadFile(filepath.Join(h.vmDir(id), "home/user/nested/relative/small.txt"))
	if err != nil || string(data) != "small-push" {
		t.Fatal("relative push file is missing or differs")
	}
}

// checkTarEntries inspects the actual uploaded tar recorded by the wrapper.
func (h *e2eHarness) checkTarEntries(t *testing.T, extract e2eExtractRecord, fixture e2ePushFixture) {
	var got []string
	for _, entry := range extract.entries {
		if entry.typeflag == tar.TypeReg || entry.typeflag == tar.TypeSymlink {
			got = append(got, entry.name)
		}
	}
	sort.Strings(got)
	want := append([]string(nil), fixture.entries...)
	sort.Strings(want)
	if !slices.Equal(got, want) {
		t.Fatalf("uploaded tar entries = %d, want %d", len(got), len(want))
	}
	for _, entry := range extract.entries {
		switch entry.name {
		case "big.bin":
			if entry.typeflag != tar.TypeReg || entry.size != int64(len(fixture.big)) {
				t.Fatal("uploaded big.bin header differs")
			}
		case "run.sh":
			if entry.typeflag != tar.TypeReg || entry.mode&0o777 != 0o755 {
				t.Fatal("uploaded run.sh mode differs")
			}
		case "link":
			if entry.typeflag != tar.TypeSymlink || entry.linkname != "big.bin" {
				t.Fatal("uploaded symlink header differs")
			}
		}
	}
}

// checkExtracted inspects the confined extraction result.
func (h *e2eHarness) checkExtracted(t *testing.T, vmDir, destination string, fixture e2ePushFixture) {
	base := filepath.Join(vmDir, filepath.FromSlash(strings.TrimPrefix(destination, "/")))
	data, err := os.ReadFile(filepath.Join(base, "big.bin"))
	if err != nil || !bytes.Equal(data, fixture.big) {
		t.Fatal("pushed big.bin differs")
	}
	stat, err := os.Stat(filepath.Join(base, "run.sh"))
	if err != nil || stat.Mode().Perm() != 0o755 {
		t.Fatal("pushed run.sh mode differs")
	}
	target, err := os.Readlink(filepath.Join(base, "link"))
	if err != nil || target != "big.bin" {
		t.Fatal("pushed symlink differs")
	}
	data, err = os.ReadFile(filepath.Join(base, "marker.txt"))
	if err != nil || string(data) != fixture.marker {
		t.Fatal("pushed marker differs")
	}
	for _, name := range fixture.entries {
		if name == "link" {
			continue
		}
		if _, err := os.Stat(filepath.Join(base, name)); err != nil {
			t.Fatalf("pushed entry %q is missing", name)
		}
	}
}

// snapshot saves a name and checks the durable ready generation.
func (h *e2eHarness) snapshot(t *testing.T, id, name string) string {
	artifact := h.cli.snapshot(t, id, name)
	h.fixture.waitJobDone(t, id, db.JobSaveSnapshot)
	row, ok, err := h.fixture.inspection.SavedSnapshotByArtifact(t.Context(), artifact)
	if err != nil || !ok {
		t.Fatal("saved snapshot row is missing")
	}
	if row.Status != db.SnapshotReady || !row.IsCurrent || row.MachineType != "large" || row.DiskGiB != 111 {
		t.Fatal("saved snapshot is not the current ready large generation")
	}
	if row.SizeBytes == nil || *row.SizeBytes < 0 {
		t.Fatal("saved snapshot has no valid size")
	}
	if row.NodeID == nil || *row.NodeID != h.fixture.nodeID {
		t.Fatal("saved snapshot is on the wrong node")
	}
	return artifact
}

// checkReplacement proves a same-name save changes the artifact and retires the
// old generation.
func (h *e2eHarness) checkReplacement(t *testing.T, base, first, second string) {
	if first == second {
		t.Fatal("same-name save reused the artifact id")
	}
	oldRow, ok, err := h.fixture.inspection.SavedSnapshotByArtifact(t.Context(), first)
	if err != nil || !ok || oldRow.RetiredAt == nil {
		t.Fatal("old snapshot generation was not retired")
	}
	newRow, ok, err := h.fixture.inspection.SavedSnapshotByArtifact(t.Context(), second)
	if err != nil || !ok || newRow.Status != db.SnapshotReady || !newRow.IsCurrent {
		t.Fatal("replacement snapshot is not current and ready")
	}
	if newRow.Generation <= oldRow.Generation {
		t.Fatal("replacement generation did not advance")
	}
	h.fixture.waitFor(t, 20*time.Second, "old snapshot purged", func() bool {
		row, ok, err := h.fixture.inspection.SavedSnapshotByArtifact(t.Context(), first)
		return err == nil && ok && row.PurgedAt != nil
	})
	h.fixture.waitJobDone(t, base, db.JobDeleteSnapshot)
	if counts := h.node.counts(); counts.SaveSnapshot < 2 || counts.DeleteSnapshot < 1 {
		t.Fatalf("node snapshot calls = save %d delete %d", counts.SaveSnapshot, counts.DeleteSnapshot)
	}
}

// checkShrink proves an explicit smaller type is a 400 with no side effects.
func (h *e2eHarness) checkShrink(t *testing.T, base string) {
	beforeVMs := h.node.counts().CreateVM
	beforeSandboxes := h.sandboxCount(t)
	event := h.cli.newExpectError(t, "--from", "base", "--type", "small")
	if event.Code != "invalid_json" || event.Status != http.StatusBadRequest {
		t.Fatalf("shrink error = %q/%d", event.Code, event.Status)
	}
	body, exchange := h.lastCreateExchange(t)
	if body.From == nil || *body.From != "base" || body.Type == nil || *body.Type != "small" {
		t.Fatal("shrink create request did not name base/small")
	}
	if exchange.status != http.StatusBadRequest {
		t.Fatal("shrink create did not return 400")
	}
	if h.node.counts().CreateVM != beforeVMs || h.sandboxCount(t) != beforeSandboxes {
		t.Fatal("rejected shrink created a VM or sandbox row")
	}
}

// restore creates from base and checks type inheritance, explicit growth and
// the released restore pin.
func (h *e2eHarness) restore(t *testing.T, artifact, wantType string, explicit bool) string {
	args := []string{"--from", "base"}
	if explicit {
		args = append(args, "--type", wantType)
	}
	id := h.cli.newSandbox(t, args...)
	body := h.lastCreate(t)
	if body.From == nil || *body.From != "base" {
		t.Fatal("restore create request did not name base")
	}
	if explicit {
		if body.Type == nil || *body.Type != wantType {
			t.Fatal("restore create request did not carry the explicit type")
		}
	} else if body.Type != nil {
		t.Fatal("omitted-type restore sent a type")
	}
	machine := "large"
	if explicit {
		machine = wantType
	}
	var wantVCPU uint32
	var wantMemory, wantDisk uint64
	switch machine {
	case "large":
		wantVCPU, wantMemory, wantDisk = 8, 16<<10, 111
	case "xlarge":
		wantVCPU, wantMemory, wantDisk = 16, 32<<10, 251
	default:
		t.Fatalf("unexpected restore type %q", machine)
	}
	h.fixture.waitJobDone(t, id, db.JobProvision)
	sandbox := h.sandbox(t, id)
	if sandbox.MachineType != machine || sandbox.Reserved.VCPUs != wantVCPU ||
		sandbox.Reserved.MemoryMiB != wantMemory || sandbox.Reserved.DiskGiB != wantDisk {
		t.Fatal("restored sandbox shape differs")
	}
	if sandbox.FromSnapshotID != artifact || sandbox.SnapshotPin {
		t.Fatal("restored sandbox does not record the released snapshot source")
	}
	record, ok := h.node.lastCreateVM()
	if !ok || record.fromSnapshot != artifact || record.diskGiB != wantDisk ||
		record.vcpus != wantVCPU || record.memoryMiB != wantMemory {
		t.Fatal("restore CreateVM request differs")
	}
	row, ok, err := h.fixture.inspection.SavedSnapshotByArtifact(t.Context(), artifact)
	if err != nil || !ok || row.Pins != 0 {
		t.Fatal("restore pin was not released")
	}
	return id
}

// deleteWait deletes a sandbox and checks the durable completed operation.
func (h *e2eHarness) deleteWait(t *testing.T, id string) {
	before := h.node.counts().DeleteVM
	operationID := h.cli.deleteWait(t, id)
	h.fixture.waitJobDone(t, id, db.JobDeleteVM)
	h.fixture.waitFor(t, 10*time.Second, "vm directory removed", func() bool {
		_, err := os.Stat(h.vmDir(id))
		return errors.Is(err, os.ErrNotExist)
	})
	row, err := h.fixture.inspection.SandboxAny(t.Context(), h.fixture.ownerID, id)
	if err != nil || row.DeletedAt == nil {
		t.Fatal("deleted sandbox row is still live")
	}
	operation, err := h.fixture.inspection.DeletionOperation(t.Context(), h.fixture.ownerID, operationID)
	if err != nil || operation.Status != db.DeletionCompleted {
		t.Fatal("deletion operation row is not completed")
	}
	if h.node.counts().DeleteVM <= before {
		t.Fatal("deletion did not reach the node")
	}
}

// runNonzero runs a pushed command with a remote exit and checks cleanup.
func (h *e2eHarness) runNonzero(t *testing.T) {
	dir := e2eMakeSmallDir(t, "marker.txt", "run-marker")
	base := filepath.Base(dir)
	beforeCreate := len(h.node.createVMs())
	h.metadataReads++
	frames, summary, errs, code := h.cli.runJSON(t, dir, "--timeout", "0", "--push", ".",
		"--", e2eRunNonzeroCommand)
	if len(errs) != 0 {
		t.Fatal("run nonzero emitted an error event")
	}
	if code != 7 || summary.ExitCode != 7 || !summary.Deleted {
		t.Fatalf("run nonzero exit/summary = %d/%d/%v", code, summary.ExitCode, summary.Deleted)
	}
	if summary.Dollars != 0 || summary.ElapsedSeconds < 0 || summary.SandboxType != "default" {
		t.Fatal("run summary fields differ")
	}
	h.checkFrameOutput(t, frames, base)
	h.checkFrameOutput(t, frames, "run-marker")
	h.checkExitFrame(t, frames, 7, false)
	body := h.lastCreate(t)
	if body.TTLSeconds == nil || *body.TTLSeconds != 4621 || body.Type != nil {
		t.Fatal("run create request does not use the fixed TTL policy")
	}
	command := h.lastCommand(t)
	if command.Cwd == nil || *command.Cwd != "/home/user/"+base ||
		command.TimeoutSeconds == nil || *command.TimeoutSeconds != 901 ||
		command.Stream == nil || !*command.Stream {
		t.Fatal("run command request differs")
	}
	created := h.node.createVMs()
	if len(created) < beforeCreate+1 {
		t.Fatal("run did not create a sandbox")
	}
	h.waitRunDeleted(t, created[len(created)-1].sandboxID)
}

// runSignal interrupts a running pushed command and checks cleanup.
func (h *e2eHarness) runSignal(t *testing.T) {
	dir := e2eMakeSmallDir(t, "marker.txt", "signal-marker")
	base := filepath.Base(dir)
	h.metadataReads++
	running := h.cli.start(t, "run signal", dir, "run", "--json", "--timeout", "0", "--push", ".",
		"--", e2eSignalCommand)
	var sandboxID string
	shellPID, childPID := 0, 0
	var shellStart, childStart uint64
	h.fixture.waitFor(t, 30*time.Second, "run signal command started", func() bool {
		record, ok := h.node.findExec("started.pid")
		if !ok {
			return false
		}
		dir := filepath.Join(h.vmDir(record.sandboxID), "home/user", base)
		started, err := os.ReadFile(filepath.Join(dir, "started.pid"))
		if err != nil {
			return false
		}
		child, err := os.ReadFile(filepath.Join(dir, "child.pid"))
		if err != nil {
			return false
		}
		shell, shellErr := strconv.Atoi(strings.TrimSpace(string(started)))
		childValue, childErr := strconv.Atoi(strings.TrimSpace(string(child)))
		if shellErr != nil || childErr != nil || shell <= 0 || childValue <= 0 {
			return false
		}
		shellStart, ok = e2eProcessStartTime(shell)
		if !ok {
			return false
		}
		childStart, ok = e2eProcessStartTime(childValue)
		if !ok {
			return false
		}
		sandboxID = record.sandboxID
		shellPID, childPID = shell, childValue
		return true
	})
	// Fatal-path cleanup for the nodefake-owned remote group. The recorded
	// start times and group check keep it from touching a reused PID or an
	// unrelated group.
	t.Cleanup(func() { e2eKillOwnedGroup(shellPID, childPID, shellStart, childStart) })
	// Require both the remote markers and a parsed started frame in the real
	// running CLI stdout before interrupting. A marker can exist before the
	// first gRPC started event reaches HTTP; cancelling in that window would
	// leave a legitimate empty response and make strict validation flaky.
	h.fixture.waitFor(t, 30*time.Second, "run signal stream started", func() bool {
		return e2eHasStartedFrame(running.stdout.Bytes())
	})
	if err := running.signal(os.Interrupt); err != nil {
		t.Fatal("run signal: SIGINT could not be delivered")
	}
	stdout, stderr, code := running.wait(t, e2eSignalTimeout)
	h.cli.checkSecrets(t, "run signal", stdout, stderr)
	if code != 130 {
		t.Fatalf("run signal exit = %d, want 130", code)
	}
	_, summary, errs := e2eParseRunOutput(t, stdout)
	if len(errs) != 0 || summary.ExitCode != 130 || !summary.Deleted {
		t.Fatalf("run signal summary = %d/%v", summary.ExitCode, summary.Deleted)
	}
	h.fixture.waitFor(t, 10*time.Second, "remote shell and child stopped", func() bool {
		return e2eProcessNotRunning(shellPID) && e2eProcessNotRunning(childPID)
	})
	h.waitRunDeleted(t, sandboxID)
}

// runHuman runs a successful human-mode command with an explicit cwd and checks
// the raw output, stderr summary and durable deletion.
func (h *e2eHarness) runHuman(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "sub"), 0o755); err != nil {
		t.Fatal("human run fixture directory failed")
	}
	if err := os.WriteFile(filepath.Join(dir, "marker.txt"), []byte("human-marker"), 0o644); err != nil {
		t.Fatal("human run fixture file failed")
	}
	base := filepath.Base(dir)
	beforeCreate := len(h.node.createVMs())
	h.metadataReads++
	stdout, stderr, code := h.cli.run(t, "run human", dir, nil, "run", "--timeout", "0", "--push", ".",
		"--cwd", "/home/user/"+base+"/sub", "--", e2eRunHumanCommand)
	h.cli.checkSecrets(t, "run human", stdout, stderr)
	if code != 0 {
		t.Fatalf("human run exit = %d, want 0", code)
	}
	if !strings.Contains(string(stdout), "sub") || !strings.Contains(string(stdout), "human-marker") {
		t.Fatal("human run output differs")
	}
	if !strings.Contains(string(stderr), "exit 0") || !strings.Contains(string(stderr), "deleted") {
		t.Fatal("human run summary differs")
	}
	body := h.lastCreate(t)
	if body.TTLSeconds == nil || *body.TTLSeconds != 4621 || body.Type != nil {
		t.Fatal("human run create request does not use the fixed TTL policy")
	}
	command := h.lastCommand(t)
	if command.Cwd == nil || *command.Cwd != "/home/user/"+base+"/sub" ||
		command.TimeoutSeconds == nil || *command.TimeoutSeconds != 901 ||
		command.Stream == nil || !*command.Stream {
		t.Fatal("human run command request differs")
	}
	created := h.node.createVMs()
	if len(created) < beforeCreate+1 {
		t.Fatal("human run did not create a sandbox")
	}
	runID := created[len(created)-1].sandboxID
	if !strings.Contains(string(stderr), runID) {
		t.Fatal("human run summary does not name its sandbox")
	}
	h.waitRunDeleted(t, runID)
}

// checkUnsupported proves unsupported run flags fail before any request and
// that legacy servers refuse --keep before create.
func (h *e2eHarness) checkUnsupported(t *testing.T) {
	beforeVMs := len(h.node.createVMs())
	beforeCreates := h.apiProxy.count("create")
	cases := [][]string{
		{"--personal"},
		{"--environment", "x"},
		{"--no-env"},
	}
	for _, extra := range cases {
		args := append([]string{"run", "--json"}, extra...)
		args = append(args, "--", "echo", "unsupported")
		stdout, stderr, code := h.cli.run(t, "run unsupported", t.TempDir(), nil, args...)
		h.cli.checkSecrets(t, "run unsupported", stdout, stderr)
		if code != 2 {
			t.Fatalf("unsupported run exit = %d, want 2", code)
		}
		if !bytes.Contains(stderr, []byte("not supported")) {
			t.Fatal("unsupported run did not explain the refusal")
		}
	}

	// --keep is valid syntax, but an old server has no sandbox TTL renewal.
	// It must fail after the one metadata read and before create.
	h.metadataReads++
	stdout, stderr, code := h.cli.run(t, "legacy keep", t.TempDir(), nil,
		"run", "--json", "--keep", "--", "echo", "keep")
	h.cli.checkSecrets(t, "legacy keep", stdout, stderr)
	if code != 1 {
		t.Fatalf("legacy keep exit = %d, want 1", code)
	}
	event := e2eErrorEventFrom(t, "legacy keep", stdout)
	if event.Code != "keep_unsupported" || !strings.Contains(event.Error, "TTL renewal") {
		t.Fatal("legacy keep did not explain the missing TTL renewal")
	}
	if len(h.node.createVMs()) != beforeVMs || h.apiProxy.count("create") != beforeCreates {
		t.Fatal("unsupported or legacy keep run had a create side effect")
	}
}

// waitRunDeleted waits for the run's durable deletion and VM removal.
func (h *e2eHarness) waitRunDeleted(t *testing.T, id string) {
	h.fixture.waitJobDone(t, id, db.JobDeleteVM)
	row, err := h.fixture.inspection.SandboxAny(t.Context(), h.fixture.ownerID, id)
	if err != nil || row.DeletedAt == nil {
		t.Fatal("run sandbox row is not deleted")
	}
	h.fixture.waitFor(t, 10*time.Second, "run vm directory removed", func() bool {
		_, err := os.Stat(h.vmDir(id))
		return errors.Is(err, os.ErrNotExist)
	})
	h.fixture.waitFor(t, 10*time.Second, "run deletion completed", func() bool {
		var status string
		err := h.fixture.inspection.Pool().QueryRow(t.Context(), `
			SELECT status FROM deletion_operations
			WHERE owner_id = $1 AND target_id = $2 AND kind = 'sandbox'
			ORDER BY requested_at DESC LIMIT 1`, h.fixture.ownerID, id).Scan(&status)
		return err == nil && status == string(db.DeletionCompleted)
	})
}

// lastCreate returns the newest create request body.
func (h *e2eHarness) lastCreate(t *testing.T) e2eCreateBody {
	t.Helper()
	_, exchange := h.lastCreateExchange(t)
	var body e2eCreateBody
	e2eDecode(t, "create request", exchange.reqBody, &body)
	return body
}

// lastCreateExchange returns the newest create request body and exchange.
func (h *e2eHarness) lastCreateExchange(t *testing.T) (e2eCreateBody, e2eExchange) {
	t.Helper()
	exchange, ok := h.apiProxy.last("create")
	if !ok {
		t.Fatal("no create request was captured")
	}
	var body e2eCreateBody
	e2eDecode(t, "create request", exchange.reqBody, &body)
	return body, exchange
}

// lastCommand returns the newest command request body.
func (h *e2eHarness) lastCommand(t *testing.T) e2eCommandBody {
	t.Helper()
	exchange, ok := h.apiProxy.last("command")
	if !ok {
		t.Fatal("no command request was captured")
	}
	var body e2eCommandBody
	e2eDecode(t, "command request", exchange.reqBody, &body)
	return body
}

// lastUploadURL returns the newest upload-URL request body.
func (h *e2eHarness) lastUploadURL(t *testing.T) e2eUploadBody {
	t.Helper()
	exchange, ok := h.apiProxy.last("createUploadUrl")
	if !ok {
		t.Fatal("no upload URL request was captured")
	}
	var body e2eUploadBody
	e2eDecode(t, "upload URL request", exchange.reqBody, &body)
	return body
}

// sandbox returns one owner-scoped live sandbox row.
func (h *e2eHarness) sandbox(t *testing.T, id string) db.Sandbox {
	t.Helper()
	sandbox, err := h.fixture.inspection.Sandbox(t.Context(), h.fixture.ownerID, id)
	if err != nil {
		t.Fatalf("sandbox row %s is missing", id)
	}
	return sandbox
}

// sandboxCount returns the number of sandbox rows for the test owner.
func (h *e2eHarness) sandboxCount(t *testing.T) int {
	t.Helper()
	var count int
	if err := h.fixture.inspection.Pool().QueryRow(t.Context(),
		`SELECT count(*) FROM sandboxes WHERE owner_id = $1`, h.fixture.ownerID).Scan(&count); err != nil {
		t.Fatal("sandbox count query failed")
	}
	return count
}

// vmDir returns the fake node's directory for one sandbox.
func (h *e2eHarness) vmDir(id string) string {
	return filepath.Join(h.fixture.nodeRoot, "vms", id)
}

// validateWire checks every captured body and the required request counts.
func (h *e2eHarness) validateWire(t *testing.T) {
	t.Helper()
	h.apiProxy.validate(t, h.v1, h.extension)
	h.signedProxy.validate(t, h.v1, h.extension)
	for _, token := range h.signedProxy.tokens() {
		h.fixture.secrets.addLog("signed upload token", token)
	}
	for _, reason := range h.node.violations() {
		t.Errorf("test node: %s", reason)
	}
	if t.Failed() {
		return
	}
	if got := h.apiProxy.count("bbraftInfo"); got != h.metadataReads {
		t.Errorf("metadata requests = %d, want %d (one per metadata read)", got, h.metadataReads)
	}
	if got := h.signedProxy.count("bbraftUploadReceiver"); got != 5 {
		t.Errorf("signed uploads = %d, want 5", got)
	}
	if got := h.apiProxy.count("bbraftUploadReceiver"); got != 0 {
		t.Errorf("API proxy saw %d signed uploads, want 0", got)
	}
	if got := h.apiProxy.count("createUploadUrl"); got != 5 {
		t.Errorf("upload URL requests = %d, want 5", got)
	}
	if got := h.apiProxy.count("saveSavedSnapshot"); got != 2 {
		t.Errorf("snapshot saves = %d, want 2", got)
	}
	if got := h.apiProxy.count("create"); got != 9 {
		t.Errorf("create requests = %d, want 9", got)
	}
	if got := h.apiProxy.count("deleteSandbox"); got != 8 {
		t.Errorf("delete requests = %d, want 8", got)
	}
	// Eight delegated user commands (raw, JSON, exact agent-harness argv, timeout,
	// restored cat, three runs) plus five internal push extractions.
	if got := h.apiProxy.count("command"); got != 13 {
		t.Errorf("command requests = %d, want 13 (8 delegated + 5 extraction)", got)
	}
	if got := h.apiProxy.count("me"); got < 2 {
		t.Errorf("me requests = %d, want at least 2", got)
	}
	if got := h.apiProxy.count("sandboxes"); got < 2 {
		t.Errorf("list requests = %d, want at least 2", got)
	}
	if got := h.apiProxy.count("getDeletionOperation"); got < 8 {
		t.Errorf("deletion polls = %d, want at least 8", got)
	}
	if got := h.apiProxy.count("getSavedSnapshot"); got < 2 {
		t.Errorf("snapshot polls = %d, want at least 2", got)
	}
	for _, exchange := range h.apiProxy.byOp("createUploadUrl") {
		if exchange.status != http.StatusOK {
			continue
		}
		var response api.FileURLResponse
		if err := json.Unmarshal(exchange.respBody, &response); err != nil {
			t.Errorf("upload URL response is malformed")
			continue
		}
		if !strings.HasPrefix(response.URL, h.signedProxy.url()+"/api/bbraft/uploads/") {
			t.Errorf("upload URL did not use the cross-origin signed receiver")
		}
		if response.Method != api.FileMethodPut || !response.ExpiresAt.After(time.Now()) {
			t.Errorf("upload URL method/expiry differs")
		}
		var request e2eUploadBody
		if err := json.Unmarshal(exchange.reqBody, &request); err != nil {
			t.Errorf("upload URL request is malformed")
			continue
		}
		if request.Path != response.Path || !strings.HasPrefix(request.Path, "/tmp/bbraft-push-") ||
			!strings.HasSuffix(request.Path, ".tar") {
			t.Errorf("upload URL request/response path differs")
		}
	}
	counts := h.node.counts()
	if counts.CreateVM < 8 || counts.DeleteVM < 8 {
		t.Errorf("node VM calls = create %d delete %d, want at least 8/8", counts.CreateVM, counts.DeleteVM)
	}
	if counts.Extract != 5 || counts.Cleanup != 0 || counts.Delegated != 8 || counts.Rejected != 0 {
		t.Errorf("node exec calls = extract %d cleanup %d delegated %d rejected %d, want 5/0/8/0",
			counts.Extract, counts.Cleanup, counts.Delegated, counts.Rejected)
	}
	if counts.PutFile != 5 || counts.SaveSnapshot < 2 || counts.DeleteSnapshot < 1 {
		t.Errorf("node file/snapshot calls = put %d save %d delete %d",
			counts.PutFile, counts.SaveSnapshot, counts.DeleteSnapshot)
	}
}
