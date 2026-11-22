// Package s3 is the objstore.Store for S3-compatible services: Garage (the
// default), Cloudflare R2, AWS S3 and others, chosen by configuration only
// (PLAN.md §5.9). It uses only core S3 operations: PUT (HEAD-checked, with
// If-None-Match: * where the service supports it), GET with Range, HEAD,
// DELETE, ListObjectsV2 and presigned GET/PUT.
//
// There is deliberately no multipart upload: every object bamboo-raft writes
// is one snapshot chunk (at most a few MiB compressed) or one manifest (at
// most 128 MiB), and a single PUT reaches 5 GiB. Signing is standard-library
// SigV4; the only dependency is net/http.
//
// PutIfAbsent checks HEAD before it sends the conditional PUT. Garage, our
// default service, ignores If-None-Match rather than rejecting it ("No
// conditional writes" in its known issues), so the conditional header alone
// could silently overwrite an existing key; the HEAD makes an existing key
// ErrExists on every service, while the header still closes the race on
// services that implement conditional writes. The objstore contract allows
// two concurrent writers of an immutable key to both succeed on services
// without them.
package s3

import (
	"bytes"
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"iter"
	"log/slog"
	"math"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/nathabonfim59/bamboo-raft/internal/objstore"
)

const (
	// service is the SigV4 service name.
	service = "s3"
	// maxAttempts bounds one idempotent operation.
	maxAttempts = 3
	// maxErrorBody bounds how much of an error response we read.
	maxErrorBody = 64 << 10
	// defaultRetryBase is the first backoff step; it doubles per attempt.
	defaultRetryBase = 100 * time.Millisecond
	// maxPresignExpiry is S3's limit for presigned URLs.
	maxPresignExpiry = 7 * 24 * time.Hour
)

// ErrInvalidArgument reports an argument the S3 API cannot express.
var ErrInvalidArgument = errors.New("s3: invalid argument")

// Config selects one bucket on one service. It comes from the BBRAFT_S3_*
// variables (endpoint, region, bucket, path style, access key id, secret
// access key or a file holding it, location hint).
type Config struct {
	// Endpoint is "http(s)://host[:port]"; empty means AWS S3.
	Endpoint string
	// Region: Garage's configured region (e.g. "garage"), "auto" for R2, the
	// bucket's region on AWS.
	Region string
	Bucket string
	// PathStyle addresses the bucket in the path (Garage and most
	// self-hosted services) instead of a virtual host.
	PathStyle bool
	// Credentials. Never logged or included in errors.
	AccessKeyID     string
	SecretAccessKey string
	// LocationHint is applied when bbraft creates the bucket (R2 only).
	LocationHint string
	// CreateBucket creates the bucket if it does not exist.
	CreateBucket bool
}

// redacted replaces values that must never reach a log or an error.
const redacted = "[REDACTED]"

// String describes the configuration with the secret redacted; it is safe to
// log.
func (c Config) String() string {
	return fmt.Sprintf("s3.Config{endpoint:%q region:%q bucket:%q path_style:%t access_key_id:%q create_bucket:%t location_hint:%q secret_access_key:%q}",
		c.Endpoint, c.Region, c.Bucket, c.PathStyle, c.AccessKeyID, c.CreateBucket, c.LocationHint, redacted)
}

// LogValue implements slog.LogValuer with the secret access key redacted.
func (c Config) LogValue() slog.Value {
	return slog.GroupValue(
		slog.String("endpoint", c.Endpoint),
		slog.String("region", c.Region),
		slog.String("bucket", c.Bucket),
		slog.Bool("path_style", c.PathStyle),
		slog.String("access_key_id", c.AccessKeyID),
		slog.Bool("create_bucket", c.CreateBucket),
		slog.String("location_hint", c.LocationHint),
		slog.String("secret_access_key", redacted),
	)
}

// Store is an objstore.Store on one S3 bucket.
type Store struct {
	cfg         Config
	client      *http.Client
	endpoint    url.URL
	virtualHost bool

	// now and retryBase are overridable in tests.
	now       func() time.Time
	retryBase time.Duration

	// conditionalUnsupported records that the service rejected
	// If-None-Match: * at least once; later puts use HEAD then PUT.
	conditionalUnsupported atomic.Bool
}

// Config returns the configuration the store was created with, credentials
// included; its String and LogValue redact the secret.
func (s *Store) Config() Config { return s.cfg }

var _ objstore.Store = (*Store)(nil)

// New checks the configuration, connects and (with CreateBucket) creates
// the bucket.
func New(ctx context.Context, cfg Config) (*Store, error) {
	s, err := newStore(cfg, nil)
	if err != nil {
		return nil, err
	}
	if cfg.CreateBucket {
		if err := s.createBucket(ctx); err != nil {
			return nil, err
		}
	}
	if err := s.probeBucket(ctx); err != nil {
		return nil, err
	}
	return s, nil
}

// newStore validates cfg and builds the store without any network access.
// client nil means the default S3 client.
func newStore(cfg Config, client *http.Client) (*Store, error) {
	endpoint, err := normalizeConfig(&cfg)
	if err != nil {
		return nil, err
	}
	virtualHost := !cfg.PathStyle
	if virtualHost {
		if _, err := netip.ParseAddr(endpoint.Hostname()); err == nil {
			return nil, fmt.Errorf("s3: endpoint %s is an IP address; set s3.path-style", endpoint.Host)
		}
	}
	if client == nil {
		client = defaultHTTPClient()
	}
	return &Store{
		cfg:         cfg,
		client:      client,
		endpoint:    *endpoint,
		virtualHost: virtualHost,
		now:         time.Now,
		retryBase:   defaultRetryBase,
	}, nil
}

// normalizeConfig validates cfg, fills the AWS defaults and returns the
// parsed endpoint. cfg.Endpoint and cfg.Region are updated to the effective
// values.
func normalizeConfig(cfg *Config) (*url.URL, error) {
	if cfg.Bucket == "" {
		return nil, errors.New("s3: Config.Bucket is required")
	}
	if !validBucketName(cfg.Bucket) {
		return nil, fmt.Errorf("s3: bucket name %q is not a valid S3 bucket name", cfg.Bucket)
	}
	if cfg.AccessKeyID == "" {
		return nil, errors.New("s3: Config.AccessKeyID is required")
	}
	if cfg.SecretAccessKey == "" {
		return nil, errors.New("s3: Config.SecretAccessKey is required")
	}
	if cfg.Region == "" {
		if cfg.Endpoint != "" {
			return nil, errors.New("s3: Config.Region is required for a custom endpoint")
		}
		cfg.Region = "us-east-1" // the AWS default endpoint
	}
	raw := cfg.Endpoint
	if raw == "" {
		raw = "https://s3." + cfg.Region + ".amazonaws.com"
	}
	u, err := url.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("s3: endpoint %q: %w", cfg.Endpoint, err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return nil, fmt.Errorf("s3: endpoint %q: scheme must be http or https", cfg.Endpoint)
	}
	if u.Host == "" {
		return nil, fmt.Errorf("s3: endpoint %q has no host", cfg.Endpoint)
	}
	if u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return nil, fmt.Errorf("s3: endpoint %q must not carry credentials, a query or a fragment", cfg.Endpoint)
	}
	u.Path = strings.TrimRight(u.Path, "/")
	cfg.Endpoint = u.Scheme + "://" + u.Host + u.Path
	return u, nil
}

// validBucketName enforces the S3 bucket naming rules every service shares.
func validBucketName(name string) bool {
	if len(name) < 3 || len(name) > 63 {
		return false
	}
	if !lowerAlphaNumeric(name[0]) || !lowerAlphaNumeric(name[len(name)-1]) {
		return false
	}
	for i := range len(name) {
		c := name[i]
		if !(c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '-' || c == '.') {
			return false
		}
	}
	if strings.Contains(name, "..") || strings.HasPrefix(name, "xn--") ||
		strings.HasSuffix(name, "-s3alias") || strings.HasSuffix(name, "--ol-s3") {
		return false
	}
	if _, err := netip.ParseAddr(name); err == nil {
		return false
	}
	return true
}

func lowerAlphaNumeric(c byte) bool {
	return c >= 'a' && c <= 'z' || c >= '0' && c <= '9'
}

// defaultHTTPClient is one client per Store: persistent connections, no
// transparent gzip (it would corrupt Range and Content-Length), bounded
// connect and response-header time, no overall timeout because Get streams.
func defaultHTTPClient() *http.Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.DisableCompression = true
	transport.MaxIdleConnsPerHost = 8
	transport.ResponseHeaderTimeout = 60 * time.Second
	transport.DialContext = (&net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}).DialContext
	return &http.Client{Transport: transport}
}

// urlFor builds the URL of key ("" names the bucket itself). Path-style puts
// the bucket first in the path; virtual-host addressing prefixes the host.
func (s *Store) urlFor(key string) *url.URL {
	u := s.endpoint
	prefix := u.Path
	path := prefix
	if s.virtualHost {
		if key != "" {
			path += "/" + key
		}
		u.Host = bucketHost(s.cfg.Bucket, u.Host)
	} else {
		path += "/" + s.cfg.Bucket
		if key != "" {
			path += "/" + key
		}
	}
	if path == "" {
		path = "/"
	}
	u.Path = path
	u.RawPath = uriEncode(path, false)
	u.RawQuery = ""
	return &u
}

// bucketHost prepends the bucket to a host, keeping an explicit port.
func bucketHost(bucket, host string) string {
	if h, port, err := net.SplitHostPort(host); err == nil {
		return bucket + "." + net.JoinHostPort(h, port)
	}
	return bucket + "." + host
}

// newRequest builds a request for key. query, when set, is encoded the SigV4
// way (space as %20, sorted names) so the wire form and the signature agree.
func (s *Store) newRequest(ctx context.Context, method, key string, query url.Values) (*http.Request, error) {
	u := s.urlFor(key)
	if len(query) > 0 {
		u.RawQuery = canonicalQuery(query)
	}
	req, err := http.NewRequestWithContext(ctx, method, u.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("s3: build %s request: %w", strings.ToLower(method), err)
	}
	return req, nil
}

// sign sets the payload hash header S3 requires on every signed request and
// signs req.
func (s *Store) sign(req *http.Request, payloadHash string) {
	req.Header.Set("X-Amz-Content-Sha256", payloadHash)
	signV4(req, credentials{accessKeyID: s.cfg.AccessKeyID, secretAccessKey: s.cfg.SecretAccessKey},
		s.cfg.Region, service, payloadHash, s.now())
}

// do signs req and sends it, retrying idempotent requests.
func (s *Store) do(ctx context.Context, req *http.Request, payloadHash string, replay bool) (*http.Response, error) {
	s.sign(req, payloadHash)
	return s.doRetry(ctx, req, replay)
}

// doRetry sends req up to maxAttempts times on retryable failures (5xx, 429
// and connection errors), backing off and respecting ctx. When replay is true
// the body is re-created from req.GetBody on each attempt.
func (s *Store) doRetry(ctx context.Context, req *http.Request, replay bool) (*http.Response, error) {
	for attempt := 1; ; attempt++ {
		r := req.Clone(ctx)
		if replay && req.GetBody != nil {
			body, err := req.GetBody()
			if err != nil {
				return nil, fmt.Errorf("s3: replay request body: %w", err)
			}
			r.Body = body
		}
		resp, err := s.client.Do(r)
		if err == nil && !retryableStatus(resp.StatusCode) {
			return resp, nil
		}
		if ctx.Err() != nil {
			if resp != nil {
				drainAndClose(resp)
			}
			return nil, ctx.Err()
		}
		if attempt == maxAttempts {
			if err != nil {
				return nil, err
			}
			return resp, nil
		}
		if resp != nil {
			drainAndClose(resp)
		}
		if err := s.sleepRetry(ctx, attempt); err != nil {
			return nil, err
		}
	}
}

// retryableStatus reports a transient HTTP status.
func retryableStatus(status int) bool {
	switch status {
	case http.StatusTooManyRequests, http.StatusInternalServerError, http.StatusBadGateway,
		http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		return true
	default:
		return false
	}
}

// sleepRetry backs off before the next attempt.
func (s *Store) sleepRetry(ctx context.Context, attempt int) error {
	delay := s.retryBase
	for i := 1; i < attempt; i++ {
		delay *= 2
	}
	delay = min(delay, 2*time.Second)
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// drainAndClose consumes a bounded amount of a response we will not use, so
// the connection can be reused, then closes it.
func drainAndClose(resp *http.Response) {
	if resp.Body != nil {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxErrorBody))
		_ = resp.Body.Close()
	}
}

// apiError is a non-2xx S3 response. Its message never contains credentials or
// signatures.
type apiError struct {
	Op         string
	Key        string
	StatusCode int
	Code       string
	Message    string
	RequestID  string
}

func (e *apiError) Error() string {
	var b strings.Builder
	b.WriteString("s3: ")
	b.WriteString(e.Op)
	if e.Key != "" {
		fmt.Fprintf(&b, " %q", e.Key)
	}
	b.WriteString(": ")
	if e.Code != "" {
		b.WriteString(e.Code)
	} else {
		fmt.Fprintf(&b, "HTTP %d", e.StatusCode)
	}
	if e.Message != "" {
		b.WriteString(": ")
		b.WriteString(e.Message)
	}
	if e.RequestID != "" {
		b.WriteString(" (request ")
		b.WriteString(e.RequestID)
		b.WriteByte(')')
	}
	return b.String()
}

// errorResponse is the S3 error document.
type errorResponse struct {
	XMLName   xml.Name `xml:"Error"`
	Code      string   `xml:"Code"`
	Message   string   `xml:"Message"`
	RequestID string   `xml:"RequestId"`
}

// apiErrorFrom reads the bounded error document of resp.
func (s *Store) apiErrorFrom(op, key string, resp *http.Response) error {
	e := &apiError{Op: op, Key: key, StatusCode: resp.StatusCode}
	if resp.Body != nil {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrorBody))
		var er errorResponse
		if err := xml.Unmarshal(b, &er); err == nil {
			e.Code = er.Code
			e.Message = truncate(er.Message, 512)
			e.RequestID = truncate(er.RequestID, 128)
		}
	}
	return e
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

// PutIfAbsent implements objstore.Store. It checks HEAD first: an existing
// key is ErrExists even on a service that ignores If-None-Match (Garage), and
// the conditional PUT then guards the race on services that implement it.
func (s *Store) PutIfAbsent(ctx context.Context, key string, r io.Reader, size int64) error {
	if err := objstore.CheckKey(key); err != nil {
		return err
	}
	if r == nil {
		return fmt.Errorf("s3: put %q: nil reader", key)
	}
	if size < 0 {
		return fmt.Errorf("s3: put %q: negative size %d", key, size)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if _, err := s.Head(ctx, key); err == nil {
		return fmt.Errorf("s3: put %q: %w", key, objstore.ErrExists)
	} else if !errors.Is(err, objstore.ErrNotFound) {
		return err
	}

	if size == 0 {
		// The transport never reads a declared-empty body, so the extra-byte
		// check has to happen here.
		var probe [1]byte
		n, err := r.Read(probe[:])
		if n > 0 {
			return fmt.Errorf("s3: put %q: body has more than the declared 0 bytes", key)
		}
		if err != nil && !errors.Is(err, io.EOF) {
			return fmt.Errorf("s3: put %q: read body: %w", key, err)
		}
		r = http.NoBody
	}

	start, seekable := int64(0), false
	seeker, ok := r.(io.Seeker)
	if ok {
		seekable = true
		if p, err := seeker.Seek(0, io.SeekCurrent); err == nil {
			start = p
		}
	}

	if s.conditionalUnsupported.Load() {
		return s.putPlain(ctx, key, r, size)
	}
	unsupported, err := s.putConditional(ctx, key, r, size)
	if !unsupported {
		return err
	}
	s.conditionalUnsupported.Store(true)
	if seekable {
		if _, err := seeker.Seek(start, io.SeekStart); err != nil {
			return fmt.Errorf("s3: put %q: rewind after a rejected If-None-Match: %w", key, err)
		}
	} else if size > 0 {
		return fmt.Errorf("s3: put %q: service rejected If-None-Match and the body cannot be replayed", key)
	}
	return s.putPlain(ctx, key, r, size)
}

// putConditional attempts the conditional PUT. unsupported is true when the
// service rejected If-None-Match: * itself (not the write).
func (s *Store) putConditional(ctx context.Context, key string, body io.Reader, size int64) (unsupported bool, err error) {
	resp, err := s.sendPut(ctx, key, body, size, true)
	if err != nil {
		return false, fmt.Errorf("s3: put %q: %w", key, err)
	}
	defer resp.Body.Close()
	switch {
	case resp.StatusCode/100 == 2:
		return false, nil
	case resp.StatusCode == http.StatusPreconditionFailed:
		return false, fmt.Errorf("s3: put %q: %w", key, objstore.ErrExists)
	case resp.StatusCode == http.StatusConflict:
		return false, fmt.Errorf("s3: put %q: %w", key, objstore.ErrExists)
	case resp.StatusCode == http.StatusNotImplemented:
		return true, nil
	}
	e := s.apiErrorFrom("put", key, resp)
	if ae, ok := errors.AsType[*apiError](e); ok && ae.StatusCode == http.StatusBadRequest && conditionalRejected(ae) {
		return true, nil
	}
	return false, e
}

// conditionalRejected reports a 400 that names the If-None-Match header or
// the NotImplemented code: the service does not implement conditional PUTs.
func conditionalRejected(e *apiError) bool {
	if e.Code == "NotImplemented" {
		return true
	}
	return strings.Contains(strings.ToLower(e.Message), "if-none-match") ||
		strings.Contains(strings.ToLower(e.Code), "notimplemented")
}

// putPlain writes without a conditional header, for services that reject
// If-None-Match. When the store got here, a HEAD has already shown the key is
// absent; the objstore contract allows a concurrent writer to win the race on
// such services.
func (s *Store) putPlain(ctx context.Context, key string, body io.Reader, size int64) error {
	resp, err := s.sendPut(ctx, key, body, size, false)
	if err != nil {
		return fmt.Errorf("s3: put %q: %w", key, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 == 2 {
		return nil
	}
	return s.apiErrorFrom("put", key, resp)
}

// sendPut sends one PUT with an exactly-sized streamed body. A PUT is only
// retried while no body byte has been read, because a partly sent body cannot
// be replayed.
func (s *Store) sendPut(ctx context.Context, key string, body io.Reader, size int64, conditional bool) (*http.Response, error) {
	req, err := s.newRequest(ctx, http.MethodPut, key, nil)
	if err != nil {
		return nil, err
	}
	if conditional {
		req.Header.Set("If-None-Match", "*")
	}
	req.ContentLength = size
	if size == 0 {
		// A non-nil body with ContentLength 0 makes net/http fall back to
		// chunked transfer encoding; an empty object must be a plain PUT.
		req.Body = http.NoBody
	}
	s.sign(req, unsignedPayload)

	for attempt := 1; ; attempt++ {
		er := &exactReader{r: body, want: size}
		r := req.Clone(ctx)
		if size == 0 {
			r.Body = http.NoBody
		} else {
			r.Body = io.NopCloser(er)
		}
		resp, err := s.client.Do(r)
		if err == nil || er.n > 0 || ctx.Err() != nil || attempt == maxAttempts {
			if err != nil {
				return nil, err
			}
			return resp, nil
		}
		if resp != nil {
			drainAndClose(resp)
		}
		if err := s.sleepRetry(ctx, attempt); err != nil {
			return nil, err
		}
	}
}

// exactReader streams exactly want bytes and fails on short or long bodies.
// At the boundary it probes one byte ahead, so an oversized body aborts the
// request instead of being truncated silently.
type exactReader struct {
	r    io.Reader
	want int64
	n    int64
	err  error
}

func (e *exactReader) Read(p []byte) (int, error) {
	if e.err != nil {
		return 0, e.err
	}
	if e.want == 0 {
		e.err = io.EOF
		return 0, io.EOF
	}
	remaining := e.want - e.n
	if int64(len(p)) < remaining {
		return e.read(p)
	}

	// Tail: read the rest, then look one byte past the declared size.
	p = p[:remaining]
	m, err := e.read(p)
	if err != nil || e.n < e.want {
		return m, err
	}
	var extra [1]byte
	k, perr := e.r.Read(extra[:])
	switch {
	case k > 0:
		e.err = fmt.Errorf("s3: body has more than the declared %d bytes", e.want)
		return 0, e.err
	case perr == nil, errors.Is(perr, io.EOF):
		e.err = io.EOF
	default:
		e.err = perr
		return m, e.err
	}
	return m, nil
}

// read is the unbounded part of Read: it accounts bytes and turns a short
// body into an error.
func (e *exactReader) read(p []byte) (int, error) {
	m, err := e.r.Read(p)
	e.n += int64(m)
	if errors.Is(err, io.EOF) && e.n < e.want {
		e.err = fmt.Errorf("s3: body ended after %d of %d bytes", e.n, e.want)
		return m, e.err
	}
	if err != nil && !errors.Is(err, io.EOF) {
		e.err = err
		return m, err
	}
	e.err = nil
	return m, nil
}

// Get implements objstore.Store.
func (s *Store) Get(ctx context.Context, key string, offset, length int64) (rc io.ReadCloser, err error) {
	if err := objstore.CheckKey(key); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if offset < 0 {
		return nil, fmt.Errorf("s3: get %q: negative offset %d", key, offset)
	}
	if length == 0 {
		info, err := s.Head(ctx, key)
		if err != nil {
			return nil, err
		}
		if offset > 0 && offset >= info.Size {
			return nil, fmt.Errorf("s3: get %q: %w", key, objstore.ErrInvalidRange)
		}
		return io.NopCloser(strings.NewReader("")), nil
	}

	req, err := s.newRequest(ctx, http.MethodGet, key, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept-Encoding", "identity")
	switch {
	case length < 0 && offset > 0:
		req.Header.Set("Range", "bytes="+strconv.FormatInt(offset, 10)+"-")
	case length > 0:
		end := int64(math.MaxInt64)
		if offset <= math.MaxInt64-(length-1) {
			end = offset + length - 1
		}
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", offset, end))
	}

	resp, err := s.do(ctx, req, emptyPayloadHash, false)
	if err != nil {
		return nil, fmt.Errorf("s3: get %q: %w", key, err)
	}
	switch {
	case resp.StatusCode/100 == 2:
	case resp.StatusCode == http.StatusNotFound:
		resp.Body.Close()
		return nil, fmt.Errorf("s3: get %q: %w", key, objstore.ErrNotFound)
	case resp.StatusCode == http.StatusRequestedRangeNotSatisfiable:
		// A range starting at 0 on an empty object is unsatisfiable to the
		// service but is an empty read by contract; a 416 at offset 0 can
		// only get here that way. A non-zero offset stays ErrInvalidRange.
		if offset == 0 {
			info, herr := s.Head(ctx, key)
			if herr == nil && info.Size == 0 {
				resp.Body.Close()
				return io.NopCloser(strings.NewReader("")), nil
			}
		}
		resp.Body.Close()
		return nil, fmt.Errorf("s3: get %q: %w", key, objstore.ErrInvalidRange)
	default:
		defer resp.Body.Close()
		return nil, s.apiErrorFrom("get", key, resp)
	}

	body := io.ReadCloser(resp.Body)
	if offset > 0 && resp.StatusCode == http.StatusOK {
		// The service ignored Range: skip the prefix ourselves, and refuse an
		// offset at or past the end the way the contract says.
		if resp.ContentLength >= 0 && offset >= resp.ContentLength {
			resp.Body.Close()
			return nil, fmt.Errorf("s3: get %q: %w", key, objstore.ErrInvalidRange)
		}
		if _, err := io.CopyN(io.Discard, body, offset); err != nil {
			resp.Body.Close()
			if errors.Is(err, io.EOF) {
				return nil, fmt.Errorf("s3: get %q: %w", key, objstore.ErrInvalidRange)
			}
			return nil, fmt.Errorf("s3: get %q: skip %d bytes: %w", key, offset, err)
		}
	}
	if length > 0 {
		body = limitedReadCloser{Reader: io.LimitReader(body, length), Closer: body}
	}
	return body, nil
}

// limitedReadCloser cuts a stream at a byte count but still closes the
// underlying response body.
type limitedReadCloser struct {
	io.Reader
	io.Closer
}

// Head implements objstore.Store.
func (s *Store) Head(ctx context.Context, key string) (objstore.Info, error) {
	if err := objstore.CheckKey(key); err != nil {
		return objstore.Info{}, err
	}
	if err := ctx.Err(); err != nil {
		return objstore.Info{}, err
	}
	req, err := s.newRequest(ctx, http.MethodHead, key, nil)
	if err != nil {
		return objstore.Info{}, err
	}
	resp, err := s.do(ctx, req, emptyPayloadHash, false)
	if err != nil {
		return objstore.Info{}, fmt.Errorf("s3: head %q: %w", key, err)
	}
	defer resp.Body.Close()
	switch {
	case resp.StatusCode/100 == 2:
		if resp.ContentLength < 0 {
			return objstore.Info{}, fmt.Errorf("s3: head %q: response has no Content-Length", key)
		}
		info := objstore.Info{Key: key, Size: resp.ContentLength}
		if lm := resp.Header.Get("Last-Modified"); lm != "" {
			if t, err := http.ParseTime(lm); err == nil {
				info.Modified = t
			}
		}
		return info, nil
	case resp.StatusCode == http.StatusNotFound:
		return objstore.Info{}, fmt.Errorf("s3: head %q: %w", key, objstore.ErrNotFound)
	default:
		return objstore.Info{}, s.apiErrorFrom("head", key, resp)
	}
}

// Delete implements objstore.Store.
func (s *Store) Delete(ctx context.Context, key string) error {
	if err := objstore.CheckKey(key); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	req, err := s.newRequest(ctx, http.MethodDelete, key, nil)
	if err != nil {
		return err
	}
	resp, err := s.do(ctx, req, emptyPayloadHash, false)
	if err != nil {
		return fmt.Errorf("s3: delete %q: %w", key, err)
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusNoContent, http.StatusOK, http.StatusNotFound:
		return nil
	default:
		return s.apiErrorFrom("delete", key, resp)
	}
}

// Presign implements objstore.Store.
func (s *Store) Presign(ctx context.Context, method, key string, expires time.Duration) (string, error) {
	if err := objstore.CheckKey(key); err != nil {
		return "", err
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if err := checkPresignMethod(method); err != nil {
		return "", err
	}
	if expires < time.Second || expires > maxPresignExpiry {
		return "", fmt.Errorf("%w: expiry %v outside 1s..7d", ErrInvalidArgument, expires)
	}
	u := s.urlFor(key)
	presignV4(u, method, credentials{accessKeyID: s.cfg.AccessKeyID, secretAccessKey: s.cfg.SecretAccessKey},
		s.cfg.Region, service, unsignedPayload, s.now(), expires)
	return u.String(), nil
}

// createBucket implements Config.CreateBucket. Repeating it is safe: an
// already-owned bucket maps to success, so the request may be replayed.
func (s *Store) createBucket(ctx context.Context) error {
	var body []byte
	if s.cfg.LocationHint != "" {
		b, err := xml.Marshal(createBucketConfiguration{LocationConstraint: s.cfg.LocationHint})
		if err != nil {
			return fmt.Errorf("s3: create bucket %q: %w", s.cfg.Bucket, err)
		}
		body = b
	}
	req, err := s.newRequest(ctx, http.MethodPut, "", nil)
	if err != nil {
		return err
	}
	req.ContentLength = int64(len(body))
	if len(body) == 0 {
		req.Body = http.NoBody
	} else {
		req.Body = io.NopCloser(bytes.NewReader(body))
		req.GetBody = func() (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(body)), nil }
	}
	if len(body) > 0 {
		req.Header.Set("Content-Type", "application/xml")
	}
	resp, err := s.do(ctx, req, sha256Hex(body), true)
	if err != nil {
		return fmt.Errorf("s3: create bucket %q: %w", s.cfg.Bucket, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 == 2 {
		return nil
	}
	if resp.StatusCode == http.StatusConflict {
		e := s.apiErrorFrom("create bucket", "", resp)
		if ae, ok := errors.AsType[*apiError](e); ok && ae.Code == "BucketAlreadyOwnedByYou" {
			return nil
		}
		return e
	}
	return s.apiErrorFrom("create bucket", "", resp)
}

// createBucketConfiguration carries the R2 location hint.
type createBucketConfiguration struct {
	XMLName            xml.Name `xml:"CreateBucketConfiguration"`
	LocationConstraint string   `xml:"LocationConstraint"`
}

// probeBucket checks that the bucket exists and we may use it.
func (s *Store) probeBucket(ctx context.Context) error {
	req, err := s.newRequest(ctx, http.MethodHead, "", nil)
	if err != nil {
		return err
	}
	resp, err := s.do(ctx, req, emptyPayloadHash, false)
	if err != nil {
		return fmt.Errorf("s3: probe bucket %q: %w", s.cfg.Bucket, err)
	}
	defer resp.Body.Close()
	switch {
	case resp.StatusCode/100 == 2:
		return nil
	case resp.StatusCode == http.StatusNotFound:
		return fmt.Errorf("s3: bucket %q does not exist; create it or set s3.create-bucket", s.cfg.Bucket)
	case resp.StatusCode == http.StatusForbidden:
		return fmt.Errorf("s3: bucket %q: access denied; check the access key and its bucket permissions", s.cfg.Bucket)
	case resp.StatusCode == http.StatusMovedPermanently:
		region := resp.Header.Get("X-Amz-Bucket-Region")
		if region != "" {
			return fmt.Errorf("s3: bucket %q is in region %q; set s3.region to it", s.cfg.Bucket, region)
		}
	}
	return s.apiErrorFrom("probe bucket", "", resp)
}

// List implements objstore.Store.
func (s *Store) List(ctx context.Context, prefix string) iter.Seq2[objstore.Info, error] {
	return s.listObjects(ctx, prefix)
}
