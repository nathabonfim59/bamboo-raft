package api

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/nathabonfim59/bamboo-raft/internal/apierr"
	"github.com/nathabonfim59/bamboo-raft/internal/auth"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

// Download route patterns. The issuance route is the v1 API path; the
// receiver is an /api/bbraft extension because the signed URL is served by
// this server rather than by the guest itself.
const (
	// DownloadsPath is the v1 API path of POST download-url issuance. It
	// carries no HTTP method; the wiring registers "POST "+DownloadsPath.
	DownloadsPath = "/api/v1/sandboxes/{sandboxId}/files/download-url"
	// DownloadReceiverPath is the /api/bbraft extension the signed URL points
	// at. The wiring registers "GET "+DownloadReceiverPath without the bearer
	// middleware.
	DownloadReceiverPath = "/api/bbraft/downloads/{token}"
	// DownloadReceiverPrefix is the absolute path prefix of receiver URLs.
	DownloadReceiverPrefix = "/api/bbraft/downloads"
	// DownloadTokenParam is the receiver path parameter name.
	DownloadTokenParam = "token"
)

// Download lifetime bounds from the pinned FileUrlRequest schema.
const (
	// DefaultDownloadTTLSeconds is the TTL when the request omits ttlSeconds.
	DefaultDownloadTTLSeconds = 900
	// MinDownloadTTLSeconds is the shortest accepted TTL.
	MinDownloadTTLSeconds = 1
	// MaxDownloadTTLSeconds is the longest accepted TTL and the maximum
	// lifetime a receiver accepts from a signed token.
	MaxDownloadTTLSeconds = 3600
)

const (
	// defaultDownloadWriteDeadline bounds one Write or Flush to the HTTP
	// client. A stalled reader cannot pin a handler or a node stream
	// indefinitely. Options may lower it for tests.
	defaultDownloadWriteDeadline = 30 * time.Second

	// maxDownloadChunkBytes is the largest data message accepted from a
	// node. The contract allows at most 1 MiB per message, and the handler
	// writes one chunk before receiving the next, so memory stays bounded.
	maxDownloadChunkBytes = 1 << 20

	// downloadTokenVersion is the only token payload version understood.
	downloadTokenVersion = 1
	// downloadTokenPurpose is the fixed purpose claim. It is redundant with
	// the HKDF key and HMAC domain separation on purpose: dropping either
	// defense still cannot turn a download token into an upload token.
	downloadTokenPurpose = "download"
	// downloadTokenDomain is prepended to the signed payload bytes.
	downloadTokenDomain = "bbraft/download-url/v1"
	// downloadNonceBytes is the random nonce length in a token.
	downloadNonceBytes = 16
	// maxDownloadPathBytes bounds one canonical guest path. Issuance and
	// verification share it.
	maxDownloadPathBytes = 4096
	// maxDownloadTokenOwnerBytes and maxDownloadTokenSandboxBytes bound the
	// identity carried by a token.
	maxDownloadTokenOwnerBytes   = 256
	maxDownloadTokenSandboxBytes = 256
	// maxDownloadTokenPayloadBytes bounds the decoded payload JSON. The worst
	// case is every path/owner/sandbox byte JSON-escaped as \u00XX (six bytes
	// per input byte), plus fixed field overhead.
	maxDownloadTokenPayloadBytes = 6*(maxDownloadPathBytes+maxDownloadTokenOwnerBytes+maxDownloadTokenSandboxBytes) + 1024
	// maxDownloadTokenLength bounds a token before any decoding. It covers the
	// base64 expansion of the largest payload plus the signature.
	maxDownloadTokenLength = 4*((maxDownloadTokenPayloadBytes+2)/3) + 128
	// minDownloadSigningKeyBytes is the minimum installation key length. A
	// shorter HMAC key is not an adequate installation secret.
	minDownloadSigningKeyBytes = 32
)

// TypeFileDownloadURL is the pinned success envelope discriminator of a
// signed download URL.
const TypeFileDownloadURL = "file.download_url"

// NewFileDownloadURLResponse builds a download-URL envelope. The URL is
// secret-bearing until it expires.
func NewFileDownloadURLResponse(rawURL, path string, expiresAt time.Time) FileURLResponse {
	return FileURLResponse{
		SuccessBase: success(TypeFileDownloadURL),
		URL:         rawURL,
		Method:      FileMethodGet,
		Path:        path,
		ExpiresAt:   expiresAt,
	}
}

// Sentinel token failures. They are fixed and carry no token material; the
// receiver answers every failure with the same 404 so it never reveals which
// check failed.
var (
	errDownloadTokenMalformed = errors.New("api: download token is malformed")
	errDownloadTokenSignature = errors.New("api: download token signature does not match")
	errDownloadTokenExpired   = errors.New("api: download token has expired")
)

// DownloadsOptions are the dependencies of the download issuance and receiver
// handlers.
type DownloadsOptions struct {
	// Lookup resolves the owner-scoped sandbox target. Required. It is the
	// same accepted interface the command and upload handlers consume.
	Lookup CommandTargetLookup
	// Clients returns a client for the target's node. Required. It is the only
	// seam into the node gateway.
	Clients NodeClientFactory
	// SigningKey is the installation-scoped HMAC key. Required. It must be the
	// same stable secret in every API process so a token issued by one
	// instance redeems at another. It is an independent secret from the upload
	// key; DeriveDownloadSigningKey supplies it in production.
	SigningKey []byte
	// BaseURL is the trusted absolute public base URL of this server, e.g.
	// "https://api.example.test" or "https://api.example.test/bbraft". Required.
	// Only its scheme, host and path prefix are used; query and fragment are
	// refused. Proxy headers and the request Host are never consulted.
	BaseURL string
	// Logger receives operational failures. Nil discards them.
	Logger *slog.Logger
	// Now is the clock used for issuance and redemption. Nil means
	// time.Now. It is a test seam; production callers leave it nil.
	Now func() time.Time
	// WriteDeadline bounds one Write or Flush to the HTTP client. Zero uses
	// defaultDownloadWriteDeadline; a negative value is rejected. It is a test
	// seam; production callers leave it zero.
	WriteDeadline time.Duration
}

// DownloadsHandlers are the two download routes. Nothing here registers them:
// server wiring chooses the mux and the middleware, exactly as it does for the
// core Handlers struct.
type DownloadsHandlers struct {
	// Issue serves POST DownloadsPath and returns the pinned file.download_url
	// envelope. It requires bearer identity and is registered behind it.
	Issue http.HandlerFunc
	// Receive serves GET DownloadReceiverPath and streams nodev1.ReadFile to
	// the HTTP client. It requires no Authorization header.
	Receive http.HandlerFunc
}

// downloadsService is the shared state behind both handlers. The signing key is
// immutable after construction, and the service holds no per-request or
// process-global state.
type downloadsService struct {
	lookup        CommandTargetLookup
	clients       NodeClientFactory
	key           []byte
	base          *url.URL
	logger        *slog.Logger
	now           func() time.Time
	writeDeadline time.Duration
}

// NewDownloadsHandlers validates opts and returns the two download handlers.
// The caller registers Issue for "POST "+DownloadsPath behind the bearer
// middleware and Receive for "GET "+DownloadReceiverPath without it.
func NewDownloadsHandlers(opts DownloadsOptions) (DownloadsHandlers, error) {
	if opts.Lookup == nil {
		return DownloadsHandlers{}, errors.New("api: download handler needs a target lookup")
	}
	if opts.Clients == nil {
		return DownloadsHandlers{}, errors.New("api: download handler needs a node-client factory")
	}
	if len(opts.SigningKey) < minDownloadSigningKeyBytes {
		return DownloadsHandlers{}, fmt.Errorf("api: download handler needs a signing key of at least %d bytes", minDownloadSigningKeyBytes)
	}
	base, err := uploadBaseURL(opts.BaseURL)
	if err != nil {
		return DownloadsHandlers{}, err
	}
	if opts.WriteDeadline < 0 {
		return DownloadsHandlers{}, errors.New("api: download handler write deadline must not be negative")
	}
	logger := opts.Logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	now := opts.Now
	if now == nil {
		now = time.Now
	}
	writeDeadline := opts.WriteDeadline
	if writeDeadline == 0 {
		writeDeadline = defaultDownloadWriteDeadline
	}
	service := &downloadsService{
		lookup:        opts.Lookup,
		clients:       opts.Clients,
		key:           bytes.Clone(opts.SigningKey),
		base:          base,
		logger:        logger,
		now:           now,
		writeDeadline: writeDeadline,
	}
	return DownloadsHandlers{
		Issue:   service.issue,
		Receive: service.receive,
	}, nil
}

// issue implements POST /api/v1/sandboxes/{sandboxId}/files/download-url.
func (s *downloadsService) issue(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	if r.Method != http.MethodPost {
		// The wiring only registers POST; this guard keeps a manually
		// dispatched handler from treating another method as issuance.
		apierr.WriteError(w, r, downloadMethodNotAllowed())
		return
	}
	identity, ok := auth.IdentityFrom(r.Context())
	if !ok || identity.Owner == "" {
		apierr.WriteError(w, r, downloadUnauthorized())
		return
	}
	sandboxID := r.PathValue(ParamSandboxID)
	if sandboxID == "" {
		apierr.WriteError(w, r, downloadNotFound())
		return
	}
	if apiErr := requireUploadJSONMedia(r); apiErr != nil {
		apierr.WriteError(w, r, apiErr)
		return
	}
	request, apiErr := decodeUploadRequest(r)
	if apiErr != nil {
		apierr.WriteError(w, r, apiErr)
		return
	}
	canonical, err := canonicalUploadPath(request.Path)
	if err != nil {
		apierr.WriteError(w, r, downloadInvalidPath())
		return
	}
	if r.Context().Err() != nil {
		// The request was canceled before the lookup. Do not touch the store
		// or mint a token.
		return
	}
	_, apiErr = s.resolveTarget(r.Context(), identity.Owner, sandboxID)
	if r.Context().Err() != nil {
		// The caller left while the lookup ran. Do not mint a token or write
		// a response, not even a lookup error.
		return
	}
	if apiErr != nil {
		apierr.WriteError(w, r, apiErr)
		return
	}
	ttl := DefaultDownloadTTLSeconds
	if request.TTLSeconds != nil {
		ttl = *request.TTLSeconds
	}
	token, expiresAt, err := s.signDownloadToken(identity.Owner, sandboxID, canonical, ttl)
	if err != nil {
		s.logger.ErrorContext(r.Context(), "api: sign download token failed", "sandbox_id", sandboxID, "error", downloadErrorName(err))
		apierr.WriteError(w, r, downloadInternal())
		return
	}
	if r.Context().Err() != nil {
		return
	}
	// The node is contacted only at redemption; the issuance lookup still
	// proves the sandbox is live and owned.
	apierr.Write(w, http.StatusOK, TypeFileDownloadURL, NewFileDownloadURLResponse(s.downloadURL(token), canonical, expiresAt))
}

// receive implements GET /api/bbraft/downloads/{token}. It needs no
// Authorization header: the signature is the credential, and it authorizes
// exactly one owner, sandbox and canonical path until it expires.
func (s *downloadsService) receive(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	if r.Method != http.MethodGet {
		// The wiring only registers GET (net/http also routes HEAD here). This
		// defensive guard answers a token used through another method with the
		// same not_found as the upload receiver, so a token is never useful
		// through another method.
		apierr.WriteError(w, r, downloadMethodNotAllowed())
		return
	}
	payload, err := s.verifyDownloadToken(r.PathValue(DownloadTokenParam))
	if err != nil {
		s.logger.WarnContext(r.Context(), "api: download token refused", "reason", downloadTokenReason(err))
		apierr.WriteError(w, r, downloadNotFound())
		return
	}
	if r.Context().Err() != nil {
		// The request was canceled before the owner-scoped lookup. Do not
		// touch the store or the node.
		return
	}
	target, apiErr := s.resolveTarget(r.Context(), payload.Owner, payload.SandboxID)
	if r.Context().Err() != nil {
		return
	}
	if apiErr != nil {
		apierr.WriteError(w, r, apiErr)
		return
	}
	client, closeClient, apiErr := s.dial(target.NodeID)
	if r.Context().Err() != nil {
		// The caller left during the factory call. Close the node client
		// without starting an RPC or writing a response, not even a factory
		// error.
		if closeClient != nil {
			s.closeClient(closeClient)
		}
		return
	}
	if apiErr != nil {
		apierr.WriteError(w, r, apiErr)
		return
	}
	defer s.closeClient(closeClient)
	s.streamFile(w, r, client, payload)
}

// downloadTokenPayload is the signed token body. It never carries an API key,
// node credential or installation master key.
type downloadTokenPayload struct {
	Version   int    `json:"v"`
	Purpose   string `json:"u"`
	Owner     string `json:"o"`
	SandboxID string `json:"s"`
	Path      string `json:"p"`
	ExpiresAt int64  `json:"e"`
	Nonce     string `json:"n"`
}

// signDownloadToken builds a token for one owner, sandbox and canonical path.
// The expiry is truncated to whole seconds so the returned time and the signed
// claim are identical on the wire.
func (s *downloadsService) signDownloadToken(owner, sandboxID, canonicalPath string, ttlSeconds int) (string, time.Time, error) {
	expiresAt := s.now().UTC().Truncate(time.Second).Add(time.Duration(ttlSeconds) * time.Second)
	nonce, err := downloadNonce()
	if err != nil {
		return "", time.Time{}, err
	}
	payload := downloadTokenPayload{
		Version:   downloadTokenVersion,
		Purpose:   downloadTokenPurpose,
		Owner:     owner,
		SandboxID: sandboxID,
		Path:      canonicalPath,
		ExpiresAt: expiresAt.Unix(),
		Nonce:     nonce,
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("marshal download token: %w", err)
	}
	signature := s.sign(raw)
	token := base64.RawURLEncoding.EncodeToString(raw) + "." + base64.RawURLEncoding.EncodeToString(signature)
	return token, expiresAt, nil
}

// downloadNonce returns a fresh base64url nonce.
func downloadNonce() (string, error) {
	var raw [downloadNonceBytes]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", fmt.Errorf("generate download token nonce: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(raw[:]), nil
}

// sign returns HMAC-SHA256 of the download domain, a NUL separator and raw
// under the download key. The domain and separate key keep an upload token
// from ever verifying here, and the purpose claim is a third defense.
func (s *downloadsService) sign(raw []byte) []byte {
	mac := hmac.New(sha256.New, s.key)
	mac.Write([]byte(downloadTokenDomain))
	mac.Write([]byte{0})
	mac.Write(raw)
	return mac.Sum(nil)
}

// verifyDownloadToken parses one token and returns its payload. Every failure
// is a fixed sentinel; no error contains the token or a decoder message.
func (s *downloadsService) verifyDownloadToken(raw string) (downloadTokenPayload, error) {
	var payload downloadTokenPayload
	if raw == "" || len(raw) > maxDownloadTokenLength {
		return payload, errDownloadTokenMalformed
	}
	encodedPayload, encodedSignature, ok := strings.Cut(raw, ".")
	if !ok || encodedPayload == "" || encodedSignature == "" || strings.Contains(encodedSignature, ".") {
		return payload, errDownloadTokenMalformed
	}
	payloadBytes, err := base64.RawURLEncoding.DecodeString(encodedPayload)
	if err != nil || base64.RawURLEncoding.EncodeToString(payloadBytes) != encodedPayload {
		return payload, errDownloadTokenMalformed
	}
	if len(payloadBytes) > maxDownloadTokenPayloadBytes {
		return payload, errDownloadTokenMalformed
	}
	signature, err := base64.RawURLEncoding.DecodeString(encodedSignature)
	if err != nil || base64.RawURLEncoding.EncodeToString(signature) != encodedSignature || len(signature) != sha256.Size {
		return payload, errDownloadTokenMalformed
	}
	if !hmac.Equal(signature, s.sign(payloadBytes)) {
		return payload, errDownloadTokenSignature
	}
	if err := decodeDownloadTokenPayload(payloadBytes, &payload); err != nil {
		return payload, errDownloadTokenMalformed
	}
	canonical, err := json.Marshal(payload)
	if err != nil || !bytes.Equal(canonical, payloadBytes) {
		return payload, errDownloadTokenMalformed
	}
	if payload.Version != downloadTokenVersion || payload.Purpose != downloadTokenPurpose {
		return payload, errDownloadTokenMalformed
	}
	if payload.Owner == "" || len(payload.Owner) > maxDownloadTokenOwnerBytes ||
		payload.SandboxID == "" || len(payload.SandboxID) > maxDownloadTokenSandboxBytes ||
		len(payload.Path) > maxDownloadPathBytes {
		return payload, errDownloadTokenMalformed
	}
	canonicalPath, err := canonicalUploadPath(payload.Path)
	if err != nil || canonicalPath != payload.Path {
		return payload, errDownloadTokenMalformed
	}
	nonce, err := base64.RawURLEncoding.DecodeString(payload.Nonce)
	if err != nil || len(nonce) != downloadNonceBytes || base64.RawURLEncoding.EncodeToString(nonce) != payload.Nonce {
		return payload, errDownloadTokenMalformed
	}
	if payload.ExpiresAt <= 0 {
		return payload, errDownloadTokenMalformed
	}
	now := s.now().UTC()
	expiresAt := time.Unix(payload.ExpiresAt, 0).UTC()
	if !expiresAt.After(now) {
		return payload, errDownloadTokenExpired
	}
	if expiresAt.Sub(now) > MaxDownloadTTLSeconds*time.Second {
		return payload, errDownloadTokenMalformed
	}
	return payload, nil
}

// decodeDownloadTokenPayload decodes exactly one payload object. It refuses
// unknown members and trailing values, and never exposes decoder text.
func decodeDownloadTokenPayload(raw []byte, payload *downloadTokenPayload) error {
	dec := jsontext.NewDecoder(bytes.NewReader(raw))
	if err := json.UnmarshalDecode(dec, payload, json.RejectUnknownMembers(true)); err != nil {
		return errDownloadTokenMalformed
	}
	var extra any
	if err := json.UnmarshalDecode(dec, &extra); !errors.Is(err, io.EOF) {
		return errDownloadTokenMalformed
	}
	return nil
}

// downloadTokenReason classifies a verification failure for logs only.
func downloadTokenReason(err error) string {
	switch {
	case errors.Is(err, errDownloadTokenSignature):
		return "signature"
	case errors.Is(err, errDownloadTokenExpired):
		return "expired"
	case errors.Is(err, errDownloadTokenMalformed):
		return "malformed"
	default:
		return "invalid"
	}
}

// downloadURL returns the absolute same-server URL of one token. It uses only
// the validated base URL; the request Host and proxy headers are irrelevant.
func (s *downloadsService) downloadURL(token string) string {
	u := s.base.Clone()
	u.Path = strings.TrimSuffix(u.Path, "/") + DownloadReceiverPrefix + "/" + token
	u.RawPath = ""
	u.RawQuery = ""
	u.Fragment = ""
	return u.String()
}

// resolveTarget performs the explicit owner-scoped lookup and requires a live,
// ready-ish target with a node. It is called on issuance and again at
// redemption, so a deletion or state change between the two is observed.
// Nothing dials the node before this lookup has verified the signed owner.
func (s *downloadsService) resolveTarget(ctx context.Context, owner, sandboxID string) (CommandTarget, *apierr.Error) {
	target, err := s.lookup.TargetLookup(ctx, owner, sandboxID)
	if err != nil {
		if errors.Is(err, ErrCommandTargetNotFound) {
			return CommandTarget{}, downloadNotFound()
		}
		// Missing is signalled only by ErrCommandTargetNotFound. Anything else
		// is an internal fault and must never turn a transient failure into an
		// indistinguishable 404. Classify by the fixed Go type or gRPC code
		// only; arbitrary lookup text and codes may echo query text or a
		// credential.
		s.logger.ErrorContext(ctx, "api: download target lookup failed",
			"sandbox_id", sandboxID, "error", downloadErrorName(err))
		return CommandTarget{}, downloadInternal()
	}
	if downloadTargetReady(target.State) && target.NodeID != "" {
		return target, nil
	}
	if downloadTargetReady(target.State) {
		s.logger.ErrorContext(ctx, "api: ready sandbox has no node", "sandbox_id", sandboxID, "state", target.State)
		return CommandTarget{}, downloadInternal()
	}
	return CommandTarget{}, downloadNotReady(target.State)
}

// downloadTargetReady reports whether a sandbox state accepts downloads.
func downloadTargetReady(state SandboxState) bool {
	switch state {
	case StateReady, StateIdle, StateRunning:
		return true
	default:
		return false
	}
}

// dial returns the node client of target and releases it when done.
func (s *downloadsService) dial(nodeID string) (nodev1.NodeClient, func() error, *apierr.Error) {
	client, closeClient, err := s.clients(nodeID)
	if err != nil {
		s.logger.Error("api: download node client", "node_id", nodeID, "error", downloadErrorName(err))
		return nil, nil, downloadDirectFailed()
	}
	if client == nil {
		s.logger.Error("api: download node client is nil", "node_id", nodeID)
		if closeClient != nil {
			if err := closeClient(); err != nil {
				s.logger.Debug("api: close download node client", "error", downloadErrorName(err))
			}
		}
		return nil, nil, downloadInternal()
	}
	if closeClient == nil {
		closeClient = func() error { return nil }
	}
	return client, closeClient, nil
}

// closeClient releases a node client, logging a failure without failing the
// response.
func (s *downloadsService) closeClient(closeClient func() error) {
	if err := closeClient(); err != nil {
		s.logger.Debug("api: close download node client", "error", downloadErrorName(err))
	}
}

// streamFile opens nodev1.ReadFile and validates the first message before any
// HTTP header is written. A failure then is a normal JSON error. Once headers
// are sent, a failure aborts the connection instead of appending JSON to the
// file bytes.
func (s *downloadsService) streamFile(w http.ResponseWriter, r *http.Request, client nodev1.NodeClient, payload downloadTokenPayload) {
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	stream, err := client.ReadFile(ctx, &nodev1.ReadFileRequest{
		SandboxId: payload.SandboxID,
		Path:      payload.Path,
		MaxBytes:  0,
	})
	if err != nil {
		s.writeNodeError(w, r, err)
		return
	}
	if r.Context().Err() != nil {
		// The caller left while ReadFile was opening. Do not Recv or write.
		return
	}
	if stream == nil {
		// A nil stream with a nil error is a protocol failure, not a reason
		// to dereference the interface.
		s.logger.WarnContext(r.Context(), "api: download node returned a nil stream", "sandbox_id", payload.SandboxID)
		apierr.WriteError(w, r, downloadDirectFailed())
		return
	}
	first, err := stream.Recv()
	if err != nil {
		s.writeNodeError(w, r, err)
		return
	}
	if r.Context().Err() != nil {
		// The caller left after the node answered but before any header was
		// sent. Do not start the body or write a response.
		return
	}
	info, ok := downloadFirstInfo(first, payload.Path)
	if !ok {
		s.logger.WarnContext(r.Context(), "api: download node sent an invalid first message", "sandbox_id", payload.SandboxID)
		apierr.WriteError(w, r, downloadDirectFailed())
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Disposition", downloadContentDisposition(info.GetPath()))
	w.Header().Set("X-Content-Type-Options", "nosniff")
	// Deliberately no Content-Length: the file may change between the node's
	// info message and the read, so the initial size is not a total the server
	// can honestly claim.
	w.WriteHeader(http.StatusOK)
	s.streamBody(w, r, cancel, stream, payload)
}

// downloadFirstInfo validates the first ReadFile message. The node must send
// one info for the signed canonical path describing a regular file. A negative
// size or permission bits outside 0o7777 are malformed metadata; a zero or
// negative modification time is legitimate and is not checked.
func downloadFirstInfo(message *nodev1.ReadFileResponse, signedPath string) (*nodev1.FileInfo, bool) {
	if message == nil {
		return nil, false
	}
	// Type-switch rather than GetInfo: a typed-nil oneof wrapper would panic
	// in the generated getter.
	part, ok := message.GetPart().(*nodev1.ReadFileResponse_Info)
	if !ok || part == nil || part.Info == nil {
		return nil, false
	}
	info := part.Info
	if info.GetPath() != signedPath ||
		info.GetName() != path.Base(signedPath) ||
		info.GetType() != nodev1.FileType_FILE_TYPE_REGULAR ||
		info.GetSize() < 0 ||
		info.GetMode() > 0o7777 {
		return nil, false
	}
	return info, true
}

// streamBody writes data chunks one at a time. Each chunk is written and
// flushed before the next Recv, so a slow client applies backpressure to the
// node instead of filling a buffer. The only successful end is io.EOF.
func (s *downloadsService) streamBody(w http.ResponseWriter, r *http.Request, cancel context.CancelFunc, stream nodev1.Node_ReadFileClient, payload downloadTokenPayload) {
	for {
		if r.Context().Err() != nil {
			// Headers are already sent. A server-side request cancellation
			// (role shutdown, for example) is not a clean end of file for the
			// client, so abort rather than returning a plausible success.
			s.abortStream(w, r, cancel, "canceled")
		}
		message, err := stream.Recv()
		if r.Context().Err() != nil {
			// The cancellation may have raced the Recv result, including an
			// EOF. Never let that look like a clean, complete download.
			s.abortStream(w, r, cancel, "canceled")
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				return
			}
			s.logger.WarnContext(r.Context(), "api: download node stream failed",
				"sandbox_id", payload.SandboxID, "error", downloadErrorName(err))
			s.abortStream(w, r, cancel, "node")
		}
		data, ok := downloadChunkData(message)
		if !ok {
			s.logger.WarnContext(r.Context(), "api: download node sent an invalid stream message", "sandbox_id", payload.SandboxID)
			s.abortStream(w, r, cancel, "protocol")
		}
		if len(data) == 0 {
			continue
		}
		if err := s.writeChunk(w, data); err != nil {
			s.logger.DebugContext(r.Context(), "api: download client write failed",
				"sandbox_id", payload.SandboxID, "error", downloadErrorName(err))
			s.abortStream(w, r, cancel, "write")
		}
	}
}

// downloadChunkData validates one post-header message: one data part of at
// most the contract's 1 MiB chunk. Repeated info, an unset part and oversized
// chunks are protocol failures.
func downloadChunkData(message *nodev1.ReadFileResponse) ([]byte, bool) {
	if message == nil {
		return nil, false
	}
	part, ok := message.GetPart().(*nodev1.ReadFileResponse_Data)
	if !ok || part == nil || len(part.Data) > maxDownloadChunkBytes {
		return nil, false
	}
	return part.Data, true
}

// writeChunk bounds both operations separately: a fresh deadline before the
// Write and another before the Flush, so neither a stalled socket write nor a
// stalled flush can pin the handler. The deadline is always cleared before
// returning to the node Recv: HTTP/2 can otherwise reset the stream when a
// deadline expires while the handler is merely waiting for the node, which
// would exceed the intended per-operation bound. ErrNotSupported is tolerated
// for writers that cannot set deadlines or flush (httptest recorders). Any
// other failure, including a short nil-error Write, stops the stream.
func (s *downloadsService) writeChunk(w http.ResponseWriter, data []byte) (err error) {
	controller := http.NewResponseController(w)
	defer func() {
		clearErr := controller.SetWriteDeadline(time.Time{})
		if clearErr != nil && !errors.Is(clearErr, http.ErrNotSupported) && err == nil {
			err = clearErr
		}
	}()
	if err = s.setWriteDeadline(controller); err != nil {
		return err
	}
	n, writeErr := w.Write(data)
	if writeErr != nil {
		return writeErr
	}
	if n != len(data) {
		return io.ErrShortWrite
	}
	if err = s.setWriteDeadline(controller); err != nil {
		return err
	}
	if flushErr := controller.Flush(); flushErr != nil && !errors.Is(flushErr, http.ErrNotSupported) {
		return flushErr
	}
	return nil
}

// setWriteDeadline installs a fresh deadline on the response connection.
func (s *downloadsService) setWriteDeadline(controller *http.ResponseController) error {
	if err := controller.SetWriteDeadline(time.Now().Add(s.writeDeadline)); err != nil && !errors.Is(err, http.ErrNotSupported) {
		return err
	}
	return nil
}

// abortStream cancels the node RPC and aborts the HTTP connection with
// net/http's standard control signal. Headers are already sent, so a JSON
// error cannot be appended; aborting makes truncation visible to the client
// instead of looking like a clean end of file. The handler never recovers it.
func (s *downloadsService) abortStream(w http.ResponseWriter, r *http.Request, cancel context.CancelFunc, reason string) {
	s.logger.DebugContext(r.Context(), "api: download stream aborted", "reason", reason)
	cancel()
	panic(http.ErrAbortHandler)
}

// downloadContentDisposition builds a safe attachment header from the signed
// path's last element. An invalid parameter (for example a newline) falls back
// to a bare attachment rather than writing an unattributable header.
func downloadContentDisposition(canonicalPath string) string {
	value := mime.FormatMediaType("attachment", map[string]string{"filename": path.Base(canonicalPath)})
	if value == "" {
		return "attachment"
	}
	return value
}

// writeNodeError maps a failure before any header was sent. It suppresses the
// answer when the caller is already gone.
func (s *downloadsService) writeNodeError(w http.ResponseWriter, r *http.Request, err error) {
	if r.Context().Err() != nil {
		return
	}
	s.logger.WarnContext(r.Context(), "api: download node call failed", "error", downloadErrorName(err))
	apierr.WriteError(w, r, downloadNodeError(err))
}

// downloadNodeError maps a nodev1.ReadFile failure to the fixed file error
// table. ResourceExhausted is deliberately not 503 out_of_capacity: a file
// read does not request VM capacity, and the file table has no capacity code.
func downloadNodeError(err error) *apierr.Error {
	st, ok := status.FromError(err)
	if !ok {
		return downloadDirectFailed()
	}
	switch st.Code() {
	case codes.NotFound:
		return downloadNotFound()
	case codes.InvalidArgument:
		return downloadInvalidPath()
	case codes.FailedPrecondition:
		return downloadNotReady("")
	default:
		return downloadDirectFailed()
	}
}

// downloadErrorName classifies an error for logs without echoing its text.
// Node RPC text can contain a path or file content, lookup text can contain
// credentials, and a token or request URI is never safe: only the gRPC code or
// Go type is returned.
func downloadErrorName(err error) string {
	if st, ok := status.FromError(err); ok {
		return "grpc/" + st.Code().String()
	}
	switch {
	case errors.Is(err, ErrCommandTargetNotFound):
		return "not_found"
	case errors.Is(err, context.Canceled):
		return "canceled"
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline_exceeded"
	}
	return fmt.Sprintf("%T", err)
}

// downloadUnauthorized is the pinned missing-identity answer.
func downloadUnauthorized() *apierr.Error {
	return apierr.New(http.StatusUnauthorized, apierr.CodeUnauthorized, "unauthorized")
}

// downloadNotFound is the owner-scoped missing-sandbox/token answer.
func downloadNotFound() *apierr.Error {
	return apierr.New(http.StatusNotFound, apierr.CodeNotFound, "not found")
}

// downloadMethodNotAllowed refuses a token used with another HTTP method. The
// route is registered for GET only; this guard is defensive and mirrors the
// upload receiver's fixed not_found answer.
func downloadMethodNotAllowed() *apierr.Error {
	return apierr.New(http.StatusMethodNotAllowed, apierr.CodeNotFound, "not found")
}

// downloadNotReady is the pinned state conflict for a sandbox that cannot
// serve a download.
func downloadNotReady(state SandboxState) *apierr.Error {
	apiErr := apierr.New(http.StatusConflict, apierr.CodeSandboxNotReady, "sandbox is not ready").Retryable(false)
	if state != "" {
		apiErr.WithDetail("state", string(state))
	}
	return apiErr
}

// downloadInvalidPath is the pinned bad-path answer.
func downloadInvalidPath() *apierr.Error {
	return apierr.New(http.StatusBadRequest, apierr.CodeInvalidPath,
		"path must resolve under /home/user or /tmp")
}

// downloadDirectFailed is the ambiguous node failure. A file read is
// idempotent, so retrying the same download is safe.
func downloadDirectFailed() *apierr.Error {
	return apierr.New(http.StatusBadGateway, apierr.CodeSandboxDirectFailed,
		"the sandbox machine could not complete the call").Retryable(true)
}

// downloadInternal is the safe 500 for failures that are not the node's answer.
func downloadInternal() *apierr.Error {
	return apierr.New(http.StatusInternalServerError, apierr.CodeInternal, "internal server error")
}
