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
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/nathabonfim59/bamboo-raft/internal/apierr"
	"github.com/nathabonfim59/bamboo-raft/internal/auth"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

// Upload route patterns. The issuance route is the v1 API path; the
// receiver is an /api/bbraft extension because the signed URL is served by
// this server rather than by the guest itself.
const (
	// UploadsPath is the v1 API path of POST upload-url issuance. It
	// carries no HTTP method; the wiring registers "POST "+UploadsPath.
	UploadsPath = "/api/v1/sandboxes/{sandboxId}/files/upload-url"
	// UploadReceiverPath is the /api/bbraft extension the signed URL points
	// at. The wiring registers "PUT "+UploadReceiverPath.
	UploadReceiverPath = "/api/bbraft/uploads/{token}"
	// UploadReceiverPrefix is the absolute path prefix of receiver URLs.
	UploadReceiverPrefix = "/api/bbraft/uploads"
	// UploadTokenParam is the receiver path parameter name.
	UploadTokenParam = "token"
)

// Upload lifetime bounds from the pinned FileUrlRequest schema.
const (
	// DefaultUploadTTLSeconds is the TTL when the request omits ttlSeconds.
	DefaultUploadTTLSeconds = 900
	// MinUploadTTLSeconds is the shortest accepted TTL.
	MinUploadTTLSeconds = 1
	// MaxUploadTTLSeconds is the longest accepted TTL and the maximum
	// lifetime a receiver accepts from a signed token.
	MaxUploadTTLSeconds = 3600
)

const (
	// uploadChunkSize bounds one data message sent to nodev1.PutFile. The
	// proto allows up to 1 MiB per message, and the handler never buffers
	// more than one chunk at a time.
	uploadChunkSize = 1 << 20

	// uploadTokenVersion is the only token payload version understood.
	uploadTokenVersion = 1
	// uploadNonceBytes is the random nonce length in a token.
	uploadNonceBytes = 16
	// maxUploadPathBytes bounds one canonical guest path. Issuance and
	// verification share it.
	maxUploadPathBytes = 4096
	// maxUploadTokenOwnerBytes and maxUploadTokenSandboxBytes bound the
	// identity carried by a token.
	maxUploadTokenOwnerBytes   = 256
	maxUploadTokenSandboxBytes = 256
	// maxUploadTokenPayloadBytes bounds the decoded payload JSON. The
	// worst case is every path/owner/sandbox byte JSON-escaped as \u00XX
	// (six bytes per input byte), plus fixed field overhead.
	maxUploadTokenPayloadBytes = 6*(maxUploadPathBytes+maxUploadTokenOwnerBytes+maxUploadTokenSandboxBytes) + 1024
	// maxUploadTokenLength bounds a token before any decoding. It covers
	// the base64 expansion of the largest payload plus the signature.
	maxUploadTokenLength = 4*((maxUploadTokenPayloadBytes+2)/3) + 128
	// minUploadSigningKeyBytes is the minimum installation key length. A
	// shorter HMAC key is not an adequate installation secret.
	minUploadSigningKeyBytes = 32
	// uploadCapacityRetrySeconds is the retry hint on a capacity refusal.
	uploadCapacityRetrySeconds = 5
	// uploadSendGrace bounds waiting for a node's final status after a
	// client Send failure. On expiry the RPC is cancelled and the receive
	// is joined unconditionally; it is never used for normal completion.
	uploadSendGrace = 5 * time.Second
)

// TypeFileUploaded is the /api/bbraft extension discriminator of a completed
// upload. It is deliberately not part of the pinned /api/v1 document.
const TypeFileUploaded = "file.uploaded"

// UploadedFile is the receiver's success body. The envelope adds ok and the
// file.uploaded type; these fields are extension-only.
type UploadedFile struct {
	Path string `json:"path"`
	Size int64  `json:"size"`
}

// Sentinel token failures. They are fixed and carry no token material; the
// receiver answers every failure with the same 404 so it never reveals which
// check failed.
var (
	errUploadTokenMalformed = errors.New("api: upload token is malformed")
	errUploadTokenSignature = errors.New("api: upload token signature does not match")
	errUploadTokenExpired   = errors.New("api: upload token has expired")
)

// UploadsOptions are the dependencies of the upload issuance and receiver
// handlers.
type UploadsOptions struct {
	// Lookup resolves the owner-scoped sandbox target. Required. It is the
	// same accepted interface the command handler consumes.
	Lookup CommandTargetLookup
	// Clients returns a client for the target's node. Required. It is the
	// only seam into the node gateway.
	Clients NodeClientFactory
	// SigningKey is the installation-scoped HMAC key. Required. It must be
	// the same stable secret in every API process so a token issued by one
	// instance redeems at another. A random key of at least 32 bytes is
	// recommended.
	SigningKey []byte
	// BaseURL is the trusted absolute public base URL of this server, e.g.
	// "https://api.example.test". Required. Only its scheme, host and path
	// prefix are used; query and fragment are refused. Proxy headers and
	// the request Host are never consulted.
	BaseURL string
	// Logger receives operational failures. Nil discards them.
	Logger *slog.Logger
	// Now is the clock used for issuance and redemption. Nil means
	// time.Now. It is a test seam; production callers leave it nil.
	Now func() time.Time
}

// UploadsHandlers are the two upload routes. Nothing here registers them:
// server wiring chooses the mux and the middleware, exactly as it does for
// the core Handlers struct.
type UploadsHandlers struct {
	// Issue serves POST UploadsPath and returns the pinned file.upload_url
	// envelope.
	Issue http.HandlerFunc
	// Receive serves PUT UploadReceiverPath and streams the request body to
	// the target node. It requires no Authorization header.
	Receive http.HandlerFunc
}

// uploadsService is the shared state behind both handlers. The signing key is
// immutable after construction, and the service holds no per-request or
// process-global state.
type uploadsService struct {
	lookup  CommandTargetLookup
	clients NodeClientFactory
	key     []byte
	base    *url.URL
	logger  *slog.Logger
	now     func() time.Time
}

// NewUploadsHandlers validates opts and returns the two upload handlers. The
// caller registers Issue for "POST "+UploadsPath and Receive for
// "PUT "+UploadReceiverPath.
func NewUploadsHandlers(opts UploadsOptions) (UploadsHandlers, error) {
	if opts.Lookup == nil {
		return UploadsHandlers{}, errors.New("api: upload handler needs a target lookup")
	}
	if opts.Clients == nil {
		return UploadsHandlers{}, errors.New("api: upload handler needs a node-client factory")
	}
	if len(opts.SigningKey) < minUploadSigningKeyBytes {
		return UploadsHandlers{}, fmt.Errorf("api: upload handler needs a signing key of at least %d bytes", minUploadSigningKeyBytes)
	}
	base, err := uploadBaseURL(opts.BaseURL)
	if err != nil {
		return UploadsHandlers{}, err
	}
	logger := opts.Logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	now := opts.Now
	if now == nil {
		now = time.Now
	}
	service := &uploadsService{
		lookup:  opts.Lookup,
		clients: opts.Clients,
		key:     bytes.Clone(opts.SigningKey),
		base:    base,
		logger:  logger,
		now:     now,
	}
	return UploadsHandlers{
		Issue:   service.issue,
		Receive: service.receive,
	}, nil
}

// uploadBaseURL parses and validates the trusted base URL. It refuses
// anything that is not an absolute http(s) URL with a host, so a handler can
// never build a signed URL from an unchecked request Host or proxy header.
func uploadBaseURL(raw string) (*url.URL, error) {
	base, err := url.Parse(raw)
	if err != nil {
		return nil, errors.New("api: upload base URL is not a valid URL")
	}
	if base.Scheme != "http" && base.Scheme != "https" {
		return nil, errors.New("api: upload base URL must use http or https")
	}
	if base.Host == "" || base.Hostname() == "" {
		return nil, errors.New("api: upload base URL must be absolute")
	}
	if base.User != nil || base.Opaque != "" || base.RawQuery != "" || base.Fragment != "" {
		return nil, errors.New("api: upload base URL must not carry credentials, query or fragment")
	}
	return base.Clone(), nil
}

// issue implements POST /api/v1/sandboxes/{sandboxId}/files/upload-url.
func (s *uploadsService) issue(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	if r.Method != http.MethodPost {
		// The wiring only registers POST; this guard keeps a manually
		// dispatched handler from treating another method as issuance.
		apierr.WriteError(w, r, uploadMethodNotAllowed())
		return
	}
	identity, ok := auth.IdentityFrom(r.Context())
	if !ok || identity.Owner == "" {
		apierr.WriteError(w, r, uploadUnauthorized())
		return
	}
	sandboxID := r.PathValue(ParamSandboxID)
	if sandboxID == "" {
		apierr.WriteError(w, r, uploadNotFound())
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
		apierr.WriteError(w, r, uploadInvalidPath())
		return
	}
	if _, apiErr := s.resolveTarget(r.Context(), identity.Owner, sandboxID); apiErr != nil {
		apierr.WriteError(w, r, apiErr)
		return
	}
	ttl := DefaultUploadTTLSeconds
	if request.TTLSeconds != nil {
		ttl = *request.TTLSeconds
	}
	token, expiresAt, err := s.signUploadToken(identity.Owner, sandboxID, canonical, ttl)
	if err != nil {
		s.logger.ErrorContext(r.Context(), "api: sign upload token failed", "sandbox_id", sandboxID, "error", uploadErrorName(err))
		apierr.WriteError(w, r, uploadInternal())
		return
	}
	// The node is contacted only at redemption; the issuance lookup still
	// proves the sandbox is live and owned.
	apierr.Write(w, http.StatusOK, TypeFileUploadURL, NewFileUploadURLResponse(s.uploadURL(token), canonical, expiresAt))
}

// receive implements PUT /api/bbraft/uploads/{token}. It needs no
// Authorization header: the signature is the credential, and it authorizes
// exactly one owner, sandbox and canonical path until it expires.
func (s *uploadsService) receive(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPut {
		// The wiring only registers PUT, so this is a defensive guard for a
		// caller that dispatches the handler manually. Reuse the not_found
		// answer so a token is never useful through another method.
		apierr.WriteError(w, r, uploadMethodNotAllowed())
		return
	}
	payload, err := s.verifyUploadToken(r.PathValue(UploadTokenParam))
	if err != nil {
		s.logger.WarnContext(r.Context(), "api: upload token refused", "reason", uploadTokenReason(err))
		apierr.WriteError(w, r, uploadTokenNotFound())
		return
	}
	target, apiErr := s.resolveTarget(r.Context(), payload.Owner, payload.SandboxID)
	if apiErr != nil {
		apierr.WriteError(w, r, apiErr)
		return
	}
	client, closeClient, apiErr := s.dial(target.NodeID)
	if apiErr != nil {
		apierr.WriteError(w, r, apiErr)
		return
	}
	defer s.closeClient(closeClient)
	s.streamToNode(w, r, client, payload)
}

// uploadTokenPayload is the signed token body. It never carries an API key,
// node credential or installation master key.
type uploadTokenPayload struct {
	Version   int    `json:"v"`
	Owner     string `json:"o"`
	SandboxID string `json:"s"`
	Path      string `json:"p"`
	ExpiresAt int64  `json:"e"`
	Nonce     string `json:"n"`
}

// signUploadToken builds a token for one owner, sandbox and canonical path.
// The expiry is truncated to whole seconds so the returned time and the
// signed claim are identical on the wire.
func (s *uploadsService) signUploadToken(owner, sandboxID, canonicalPath string, ttlSeconds int) (string, time.Time, error) {
	expiresAt := s.now().UTC().Truncate(time.Second).Add(time.Duration(ttlSeconds) * time.Second)
	nonce, err := uploadNonce()
	if err != nil {
		return "", time.Time{}, err
	}
	payload := uploadTokenPayload{
		Version:   uploadTokenVersion,
		Owner:     owner,
		SandboxID: sandboxID,
		Path:      canonicalPath,
		ExpiresAt: expiresAt.Unix(),
		Nonce:     nonce,
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("marshal upload token: %w", err)
	}
	signature := s.sign(raw)
	token := base64.RawURLEncoding.EncodeToString(raw) + "." + base64.RawURLEncoding.EncodeToString(signature)
	return token, expiresAt, nil
}

// uploadNonce returns a fresh base64url nonce.
func uploadNonce() (string, error) {
	var raw [uploadNonceBytes]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", fmt.Errorf("generate upload token nonce: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(raw[:]), nil
}

// sign returns the HMAC-SHA256 of raw under the installation key.
func (s *uploadsService) sign(raw []byte) []byte {
	mac := hmac.New(sha256.New, s.key)
	mac.Write(raw)
	return mac.Sum(nil)
}

// verifyUploadToken parses one token and returns its payload. Every failure is
// a fixed sentinel; no error contains the token or a decoder message.
func (s *uploadsService) verifyUploadToken(raw string) (uploadTokenPayload, error) {
	var payload uploadTokenPayload
	if raw == "" || len(raw) > maxUploadTokenLength {
		return payload, errUploadTokenMalformed
	}
	encodedPayload, encodedSignature, ok := strings.Cut(raw, ".")
	if !ok || encodedPayload == "" || encodedSignature == "" || strings.Contains(encodedSignature, ".") {
		return payload, errUploadTokenMalformed
	}
	payloadBytes, err := base64.RawURLEncoding.DecodeString(encodedPayload)
	if err != nil || base64.RawURLEncoding.EncodeToString(payloadBytes) != encodedPayload {
		return payload, errUploadTokenMalformed
	}
	if len(payloadBytes) > maxUploadTokenPayloadBytes {
		return payload, errUploadTokenMalformed
	}
	signature, err := base64.RawURLEncoding.DecodeString(encodedSignature)
	if err != nil || base64.RawURLEncoding.EncodeToString(signature) != encodedSignature || len(signature) != sha256.Size {
		return payload, errUploadTokenMalformed
	}
	if !hmac.Equal(signature, s.sign(payloadBytes)) {
		return payload, errUploadTokenSignature
	}
	if err := decodeUploadTokenPayload(payloadBytes, &payload); err != nil {
		return payload, errUploadTokenMalformed
	}
	canonical, err := json.Marshal(payload)
	if err != nil || !bytes.Equal(canonical, payloadBytes) {
		return payload, errUploadTokenMalformed
	}
	if payload.Version != uploadTokenVersion {
		return payload, errUploadTokenMalformed
	}
	if payload.Owner == "" || len(payload.Owner) > maxUploadTokenOwnerBytes ||
		payload.SandboxID == "" || len(payload.SandboxID) > maxUploadTokenSandboxBytes ||
		len(payload.Path) > maxUploadPathBytes {
		return payload, errUploadTokenMalformed
	}
	canonicalPath, err := canonicalUploadPath(payload.Path)
	if err != nil || canonicalPath != payload.Path {
		return payload, errUploadTokenMalformed
	}
	nonce, err := base64.RawURLEncoding.DecodeString(payload.Nonce)
	if err != nil || len(nonce) != uploadNonceBytes || base64.RawURLEncoding.EncodeToString(nonce) != payload.Nonce {
		return payload, errUploadTokenMalformed
	}
	if payload.ExpiresAt <= 0 {
		return payload, errUploadTokenMalformed
	}
	now := s.now().UTC()
	expiresAt := time.Unix(payload.ExpiresAt, 0).UTC()
	if !expiresAt.After(now) {
		return payload, errUploadTokenExpired
	}
	if expiresAt.Sub(now) > MaxUploadTTLSeconds*time.Second {
		return payload, errUploadTokenMalformed
	}
	return payload, nil
}

// decodeUploadTokenPayload decodes exactly one payload object. It refuses
// unknown members and trailing values, and never exposes decoder text.
func decodeUploadTokenPayload(raw []byte, payload *uploadTokenPayload) error {
	dec := jsontext.NewDecoder(bytes.NewReader(raw))
	if err := json.UnmarshalDecode(dec, payload, json.RejectUnknownMembers(true)); err != nil {
		return errUploadTokenMalformed
	}
	var extra any
	if err := json.UnmarshalDecode(dec, &extra); !errors.Is(err, io.EOF) {
		return errUploadTokenMalformed
	}
	return nil
}

// uploadTokenReason classifies a verification failure for logs only.
func uploadTokenReason(err error) string {
	switch {
	case errors.Is(err, errUploadTokenSignature):
		return "signature"
	case errors.Is(err, errUploadTokenExpired):
		return "expired"
	case errors.Is(err, errUploadTokenMalformed):
		return "malformed"
	default:
		return "invalid"
	}
}

// uploadURL returns the absolute same-server URL of one token. It uses only
// the validated base URL; the request Host and proxy headers are irrelevant.
func (s *uploadsService) uploadURL(token string) string {
	u := s.base.Clone()
	u.Path = strings.TrimSuffix(u.Path, "/") + UploadReceiverPrefix + "/" + token
	u.RawPath = ""
	u.RawQuery = ""
	u.Fragment = ""
	return u.String()
}

// resolveTarget performs the explicit owner-scoped lookup and requires a live,
// ready-ish target with a node. It is called on issuance and again at
// redemption, so a deletion or state change between the two is observed.
func (s *uploadsService) resolveTarget(ctx context.Context, owner, sandboxID string) (CommandTarget, *apierr.Error) {
	target, err := s.lookup.TargetLookup(ctx, owner, sandboxID)
	if err != nil {
		if errors.Is(err, ErrCommandTargetNotFound) {
			return CommandTarget{}, uploadNotFound()
		}
		// Missing is signalled only by ErrCommandTargetNotFound. Anything
		// else is an internal fault and must never turn a transient
		// failure into an indistinguishable 404. Classify by the fixed Go
		// type or gRPC code only; arbitrary lookup text and codes may echo
		// query text or a credential.
		s.logger.ErrorContext(ctx, "api: upload target lookup failed",
			"sandbox_id", sandboxID, "error", uploadErrorName(err))
		return CommandTarget{}, uploadInternal()
	}
	if uploadTargetReady(target.State) && target.NodeID != "" {
		return target, nil
	}
	if uploadTargetReady(target.State) {
		s.logger.ErrorContext(ctx, "api: ready sandbox has no node", "sandbox_id", sandboxID, "state", target.State)
	}
	return CommandTarget{}, uploadNotReady(target.State)
}

// uploadTargetReady reports whether a sandbox state accepts uploads.
func uploadTargetReady(state SandboxState) bool {
	switch state {
	case StateReady, StateIdle, StateRunning:
		return true
	default:
		return false
	}
}

// dial returns the node client of target and releases it when done.
func (s *uploadsService) dial(nodeID string) (nodev1.NodeClient, func() error, *apierr.Error) {
	client, closeClient, err := s.clients(nodeID)
	if err != nil {
		s.logger.Error("api: upload node client", "node_id", nodeID, "error", uploadErrorName(err))
		return nil, nil, uploadDirectFailed()
	}
	if client == nil {
		s.logger.Error("api: upload node client is nil", "node_id", nodeID)
		if closeClient != nil {
			if err := closeClient(); err != nil {
				s.logger.Debug("api: close upload node client", "error", uploadErrorName(err))
			}
		}
		return nil, nil, uploadInternal()
	}
	if closeClient == nil {
		closeClient = func() error { return nil }
	}
	return client, closeClient, nil
}

// closeClient releases a node client, logging a failure without failing the
// response.
func (s *uploadsService) closeClient(closeClient func() error) {
	if err := closeClient(); err != nil {
		s.logger.Debug("api: close upload node client", "error", uploadErrorName(err))
	}
}

// uploadPumpResult is the raw-body pump's outcome.
type uploadPumpResult struct {
	size    int64
	err     error
	bodyErr bool
}

// uploadReceiveResult is the final gRPC receive's outcome.
type uploadReceiveResult struct {
	response *nodev1.PutFileResponse
	err      error
}

// streamToNode sends the header and then the raw request body to
// nodev1.PutFile in bounded chunks. It never buffers the whole body and never
// writes a temp file.
//
// The body pump and the final gRPC receive run concurrently. A client-streaming
// Send often returns nil before the peer's rejection is visible, so the receive
// side observes the status while the pump is still reading. When the node fails
// first, the HTTP body is closed to interrupt the read, both sides are joined,
// and the node's real status wins. CloseSend is called only after the pump has
// finished, so send and final receive never race on closing the send side.
func (s *uploadsService) streamToNode(w http.ResponseWriter, r *http.Request, client nodev1.NodeClient, payload uploadTokenPayload) {
	body := r.Body
	if body == nil {
		body = http.NoBody
	}
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	stream, err := client.PutFile(ctx)
	if err != nil {
		s.writeNodeError(w, r, err)
		return
	}

	recvCh := make(chan uploadReceiveResult, 1)
	go func() {
		var response nodev1.PutFileResponse
		if err := stream.RecvMsg(&response); err != nil {
			recvCh <- uploadReceiveResult{err: err}
			return
		}
		recvCh <- uploadReceiveResult{response: &response}
	}()
	pumpCh := make(chan uploadPumpResult, 1)
	go func() {
		pumpCh <- pumpUploadBody(body, stream, payload.SandboxID, payload.Path)
	}()

	select {
	case pump := <-pumpCh:
		s.finishAfterUploadPump(w, r, payload, stream, cancel, recvCh, pump)
	case recv := <-recvCh:
		// Capture a prior caller cancellation before our own body
		// interruption can cancel the request context; a real early node
		// status must not be written after the caller already left.
		callerGone := r.Context().Err() != nil
		cancel()
		interruptUploadBody(w, body)
		// A valid net/http Request.Body.Close unblocks the concurrent
		// Read, so the pump is joined unconditionally.
		<-pumpCh
		_ = body.Close()
		if recv.err == nil {
			if callerGone {
				return
			}
			// The node answered before the body ended. Never report a
			// successful upload of a partial body.
			s.logger.WarnContext(r.Context(), "api: upload node returned before the body completed", "sandbox_id", payload.SandboxID)
			apierr.WriteError(w, r, uploadDirectFailed())
			return
		}
		if callerGone {
			return
		}
		// The captured callerGone is authoritative here: our own
		// SetReadDeadline may have canceled r.Context even though the
		// caller is still waiting. Map every real node status, including
		// Canceled and DeadlineExceeded, to its forced 502/4xx answer.
		s.writeNodeErrorForced(w, r, recv.err)
	case <-r.Context().Done():
		// This is a genuine caller cancellation: our interrupt runs only
		// after the recv branch is selected. Suppress the response.
		cancel()
		interruptUploadBody(w, body)
		// Join both workers unconditionally; Close and context
		// cancellation must unblock a valid body Read and gRPC RecvMsg.
		<-pumpCh
		_ = body.Close()
		<-recvCh
	}
}

// interruptUploadBody stops a blocked request-body Read. Setting the
// connection read deadline to now is the net/http way to interrupt it without
// waiting for the client; when the writer cannot set deadlines (httptest
// recorders, custom readers), closing the body is the fallback.
func interruptUploadBody(w http.ResponseWriter, body io.ReadCloser) {
	if err := http.NewResponseController(w).SetReadDeadline(time.Now()); err == nil {
		return
	}
	_ = body.Close()
}

// finishAfterUploadPump resolves the response after the body pump finished. A
// completed body closes the send side and waits for the final receive; a local
// read failure cancels and joins the RPC; a Send failure waits for the node's
// real status before falling back to the Send error.
func (s *uploadsService) finishAfterUploadPump(w http.ResponseWriter, r *http.Request, payload uploadTokenPayload, stream grpc.ClientStreamingClient[nodev1.PutFileRequest, nodev1.PutFileResponse], cancel context.CancelFunc, recvCh <-chan uploadReceiveResult, pump uploadPumpResult) {
	if pump.err == nil {
		// The body is complete; EOF lets the node send its response. A
		// legitimate slow commit (for example a large fsync) may take as
		// long as it needs; only caller cancellation bounds this wait.
		_ = stream.CloseSend()
		select {
		case recv := <-recvCh:
			if recv.err != nil {
				s.writeNodeError(w, r, recv.err)
				return
			}
			if recv.response == nil {
				s.logger.WarnContext(r.Context(), "api: upload node returned no response", "sandbox_id", payload.SandboxID)
				apierr.WriteError(w, r, uploadDirectFailed())
				return
			}
			s.finishUpload(w, r, payload, pump.size, recv.response)
		case <-r.Context().Done():
			// Genuine caller cancellation. Cancel and join the receive
			// side; suppress the response.
			cancel()
			<-recvCh
		}
		return
	}
	if pump.bodyErr {
		// The HTTP body failed, not the node. Cancel and join the receive
		// side; a real status that arrived first is still authoritative.
		cancel()
		recv := <-recvCh
		if r.Context().Err() != nil {
			return
		}
		if recv.err != nil && !uploadCanceled(recv.err) {
			s.writeNodeErrorForced(w, r, recv.err)
			return
		}
		s.logger.WarnContext(r.Context(), "api: upload body read failed", "error", uploadErrorName(pump.err))
		apierr.WriteError(w, r, uploadBodyFailed())
		return
	}
	// A Send failed. The receive side is already observing the final status;
	// give it a bounded grace to return the real status before cancelling,
	// then cancel and join it unconditionally.
	var recv uploadReceiveResult
	select {
	case recv = <-recvCh:
	case <-time.After(uploadSendGrace):
		cancel()
		recv = <-recvCh
	}
	if recv.err != nil {
		if uploadCanceled(recv.err) {
			s.writeNodeError(w, r, pump.err)
			return
		}
		if r.Context().Err() != nil {
			return
		}
		s.writeNodeErrorForced(w, r, recv.err)
		return
	}
	// The node claimed success even though the client failed to send the
	// whole body. Treat it as a protocol failure, never a success.
	s.logger.WarnContext(r.Context(), "api: upload node returned success before the body completed", "sandbox_id", payload.SandboxID)
	apierr.WriteError(w, r, uploadDirectFailed())
}

// pumpUploadBody sends the header and then body in bounded chunks until EOF or
// the first read/send failure. It never closes the send side; the caller does
// that only after this function returns.
func pumpUploadBody(body io.Reader, stream grpc.ClientStreamingClient[nodev1.PutFileRequest, nodev1.PutFileResponse], sandboxID, path string) uploadPumpResult {
	header := &nodev1.PutFileRequest{
		Part: &nodev1.PutFileRequest_Header_{Header: &nodev1.PutFileRequest_Header{
			SandboxId: sandboxID,
			Path:      path,
		}},
	}
	if err := stream.Send(header); err != nil {
		return uploadPumpResult{err: err}
	}
	buffer := make([]byte, uploadChunkSize)
	var size int64
	for {
		n, readErr := body.Read(buffer)
		if n > 0 {
			size += int64(n)
			// One chunk is in flight at a time, so gRPC backpressure
			// bounds memory. Clone because the send may outlive this
			// iteration in a test client.
			chunk := bytes.Clone(buffer[:n])
			if err := stream.Send(&nodev1.PutFileRequest{
				Part: &nodev1.PutFileRequest_Data{Data: chunk},
			}); err != nil {
				return uploadPumpResult{size: size, err: err}
			}
		}
		if readErr == nil {
			continue
		}
		if errors.Is(readErr, io.EOF) {
			return uploadPumpResult{size: size}
		}
		return uploadPumpResult{size: size, err: readErr, bodyErr: true}
	}
}

// finishUpload validates the node's final response and writes the extension
// success body.
func (s *uploadsService) finishUpload(w http.ResponseWriter, r *http.Request, payload uploadTokenPayload, size int64, response *nodev1.PutFileResponse) {
	if response.GetPath() != payload.Path || response.GetSize() != size {
		s.logger.WarnContext(r.Context(), "api: upload node result does not match the request",
			"sandbox_id", payload.SandboxID, "path_ok", response.GetPath() == payload.Path, "size_ok", response.GetSize() == size)
		apierr.WriteError(w, r, uploadDirectFailed())
		return
	}
	apierr.Write(w, http.StatusOK, TypeFileUploaded, UploadedFile{Path: response.GetPath(), Size: response.GetSize()})
}

// uploadCanceled reports whether err is a cancellation, including a gRPC
// status Canceled or DeadlineExceeded. errors.Is alone only matches the
// context sentinels, not the status errors a real client receives.
func uploadCanceled(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	st, ok := status.FromError(err)
	if !ok {
		return false
	}
	return st.Code() == codes.Canceled || st.Code() == codes.DeadlineExceeded
}

// writeNodeError maps a node failure to a safe fixed error, unless the caller
// is already gone.
func (s *uploadsService) writeNodeError(w http.ResponseWriter, r *http.Request, err error) {
	if r.Context().Err() != nil {
		return
	}
	s.writeNodeErrorForced(w, r, err)
}

// writeNodeErrorForced maps a node failure even when the request context is
// already canceled. It is used when we canceled our own body read after the
// node returned a real status, so the waiting client still gets that status.
func (s *uploadsService) writeNodeErrorForced(w http.ResponseWriter, r *http.Request, err error) {
	s.logger.WarnContext(r.Context(), "api: upload node call failed", "error", uploadErrorName(err))
	apiErr := apierr.NodeMap{Retryable: true}.Map(err)
	if st, ok := status.FromError(err); ok && st.Code() == codes.ResourceExhausted {
		apiErr.RetryAfterSeconds(uploadCapacityRetrySeconds)
	}
	apierr.WriteError(w, r, apiErr)
}

// uploadErrorName classifies an error for logs without echoing its text. Node
// RPC text can contain a path or file content, and lookup text can contain
// credentials; only the gRPC code or Go type is safe.
func uploadErrorName(err error) string {
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

// uploadRequestSyntax is the strict member shape. Raw members distinguish an
// omitted ttlSeconds from an explicit null, which is invalid.
type uploadRequestSyntax struct {
	Path       jsontext.Value `json:"path,omitzero"`
	TTLSeconds jsontext.Value `json:"ttlSeconds,omitzero"`
}

// decodeUploadRequest parses and validates the issuance body. Unknown members,
// trailing values, empty bodies and oversized bodies are invalid_json; a path
// that is not a string is invalid_json; a present ttlSeconds that is null, not
// an integer or out of range is invalid_timeout.
func decodeUploadRequest(r *http.Request) (FileURLRequest, *apierr.Error) {
	var request FileURLRequest
	raw, err := readUploadJSONBody(r)
	if err != nil {
		return request, uploadInvalidJSON("request body is not valid JSON")
	}
	var syntax uploadRequestSyntax
	if err := json.Unmarshal(raw, &syntax, json.RejectUnknownMembers(true)); err != nil {
		return request, uploadInvalidJSON("request body is not valid JSON")
	}
	pathValue := bytes.TrimSpace(syntax.Path)
	switch {
	case len(pathValue) == 0 || bytes.Equal(pathValue, []byte("null")):
		return request, uploadInvalidJSON("path is required")
	case !isJSONString(pathValue):
		return request, uploadInvalidJSON("path must be a string")
	}
	if apiErr := validateUploadTTL(syntax.TTLSeconds); apiErr != nil {
		return request, apiErr
	}
	if err := DecodeJSON(r, &request); err != nil {
		return request, uploadInvalidJSON("request body is not valid JSON")
	}
	return request, nil
}

// validateUploadTTL enforces the pinned integer TTL bounds. An omitted member
// is valid and takes the default; an explicit null is not.
func validateUploadTTL(raw jsontext.Value) *apierr.Error {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 {
		return nil
	}
	if bytes.Equal(raw, []byte("null")) {
		return uploadInvalidTimeout()
	}
	switch raw[0] {
	case '"', 't', 'f', '[', '{':
		return uploadInvalidTimeout()
	}
	if bytes.ContainsAny(raw, ".eE") {
		return uploadInvalidTimeout()
	}
	value, err := strconv.ParseInt(string(raw), 10, 64)
	if err != nil || value < MinUploadTTLSeconds || value > MaxUploadTTLSeconds {
		return uploadInvalidTimeout()
	}
	return nil
}

// readUploadJSONBody reads the bounded issuance body and restores it for the
// DTO decode. It mirrors the command reader but stays in the uploads file.
func readUploadJSONBody(r *http.Request) ([]byte, error) {
	if r.Body == nil {
		return nil, ErrEmptyBody
	}
	raw, err := io.ReadAll(io.LimitReader(r.Body, MaxJSONBodyBytes+1))
	if err != nil {
		return nil, err
	}
	if len(raw) > MaxJSONBodyBytes {
		return nil, ErrBodyTooLarge
	}
	r.Body = io.NopCloser(bytes.NewReader(raw))
	return raw, nil
}

// requireUploadJSONMedia rejects a declared non-JSON media type. A missing
// Content-Type is accepted because the body is still decoded strictly.
func requireUploadJSONMedia(r *http.Request) *apierr.Error {
	raw := r.Header.Get("Content-Type")
	if raw == "" {
		return nil
	}
	mediaType, _, err := mime.ParseMediaType(raw)
	if err != nil || !strings.EqualFold(mediaType, "application/json") {
		return uploadInvalidJSON("Content-Type must be application/json")
	}
	return nil
}

// canonicalUploadPath resolves an API file path to its canonical absolute
// guest path, relative to /home/user, confined under /home/user or /tmp.
// NUL bytes, directory roots and traversal escapes are refused.
func canonicalUploadPath(raw string) (string, error) {
	if raw == "" || strings.ContainsRune(raw, 0) {
		return "", errUploadInvalidPath
	}
	if !path.IsAbs(raw) {
		raw = "/home/user/" + raw
	}
	cleaned := path.Clean(raw)
	// The bound applies to the canonical path the token carries, after a
	// relative path has been resolved against /home/user and cleaned.
	if len(cleaned) > maxUploadPathBytes {
		return "", errUploadInvalidPath
	}
	for _, base := range []string{"/home/user", "/tmp"} {
		if cleaned == base {
			return "", errUploadInvalidPath
		}
		if strings.HasPrefix(cleaned, base+"/") {
			return cleaned, nil
		}
	}
	return "", errUploadInvalidPath
}

var errUploadInvalidPath = errors.New("api: path is outside /home/user and /tmp")

// uploadUnauthorized is the pinned missing-identity answer.
func uploadUnauthorized() *apierr.Error {
	return apierr.New(http.StatusUnauthorized, apierr.CodeUnauthorized, "unauthorized")
}

// uploadNotFound is the owner-scoped missing-sandbox/token answer.
func uploadNotFound() *apierr.Error {
	return apierr.New(http.StatusNotFound, apierr.CodeNotFound, "not found")
}

// uploadTokenNotFound is the fixed receiver answer for any invalid token. It
// never says which check failed.
func uploadTokenNotFound() *apierr.Error {
	return apierr.New(http.StatusNotFound, apierr.CodeNotFound, "not found")
}

// uploadMethodNotAllowed refuses a token used with another HTTP method. The
// route is registered for PUT only; this guard is defensive.
func uploadMethodNotAllowed() *apierr.Error {
	return apierr.New(http.StatusMethodNotAllowed, apierr.CodeNotFound, "not found")
}

// uploadNotReady is the pinned state conflict for a sandbox that cannot accept
// an upload.
func uploadNotReady(state SandboxState) *apierr.Error {
	apiErr := apierr.New(http.StatusConflict, apierr.CodeSandboxNotReady, "sandbox is not ready").Retryable(false)
	if state != "" {
		apiErr.WithDetail("state", string(state))
	}
	return apiErr
}

// uploadDirectFailed is the ambiguous node failure. A file upload is
// idempotent for the same bytes, so retrying the same request is safe.
func uploadDirectFailed() *apierr.Error {
	return apierr.New(http.StatusBadGateway, apierr.CodeSandboxDirectFailed,
		"the sandbox machine could not complete the call").Retryable(true)
}

// uploadInternal is the safe 500 for failures that are not the node's answer.
func uploadInternal() *apierr.Error {
	return apierr.New(http.StatusInternalServerError, apierr.CodeInternal, "internal server error")
}

// uploadBodyFailed is the safe 400 for a request body that could not be read.
// The body is raw bytes, so invalid_json is the closest fixed bad-request code.
func uploadBodyFailed() *apierr.Error {
	return apierr.New(http.StatusBadRequest, apierr.CodeInvalidJSON, "request body could not be read")
}

// uploadInvalidJSON is the pinned bad-body error.
func uploadInvalidJSON(message string) *apierr.Error {
	return apierr.New(http.StatusBadRequest, apierr.CodeInvalidJSON, message)
}

// uploadInvalidPath is the pinned out-of-roots path error.
func uploadInvalidPath() *apierr.Error {
	return apierr.New(http.StatusBadRequest, apierr.CodeInvalidPath,
		"path must resolve under /home/user or /tmp")
}

// uploadInvalidTimeout is the pinned bad-TTL error.
func uploadInvalidTimeout() *apierr.Error {
	return apierr.New(http.StatusBadRequest, apierr.CodeInvalidTimeout,
		"ttlSeconds must be an integer from 1 to 3600")
}
