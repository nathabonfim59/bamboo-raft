package api_test

import (
	"bytes"
	"context"
	"crypto/hmac"
	"encoding/base64"
	"encoding/json/v2"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/nathabonfim59/bamboo-raft/internal/api"
	"github.com/nathabonfim59/bamboo-raft/internal/apierr"
	"github.com/nathabonfim59/bamboo-raft/internal/auth"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

// downloadTestScriptedStream serves the given messages and then terminal. A
// nil terminal is io.EOF.
func downloadTestScriptedStream(messages []*nodev1.ReadFileResponse, terminal error) *downloadTestStream {
	var index int
	return &downloadTestStream{
		recv: func() (*nodev1.ReadFileResponse, error) {
			if index < len(messages) {
				message := messages[index]
				index++
				return message, nil
			}
			if terminal == nil {
				return nil, io.EOF
			}
			return nil, terminal
		},
	}
}

// downloadTestSuccessStream returns one info and EOF for a normal empty file.
func downloadTestSuccessStream(filePath string) *downloadTestStream {
	return downloadTestScriptedStream([]*nodev1.ReadFileResponse{downloadTestInfoMessage(filePath, 0)}, nil)
}

func downloadTestBaseToken(t *testing.T, clock *downloadTestClock) (string, downloadTestTokenPayload) {
	t.Helper()
	issuer := downloadTestHandlers(t, downloadTestReadyLookup(), downloadTestRejectingFactory(t).factory(), clock)
	token, _ := downloadTestIssueToken(t, issuer, downloadTestOwner, downloadTestSandboxID, "x")
	return token, downloadTestDecodeToken(t, token)
}

func downloadTestTokenCaseRejects(t *testing.T, handler http.HandlerFunc, lookup *downloadTestLookup, factory *downloadTestFactoryCounter, token string) {
	t.Helper()
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/api/bbraft/downloads/"+token, nil)
	request.SetPathValue(api.DownloadTokenParam, token)
	handler(recorder, request)
	downloadTestTargets(t, recorder, http.StatusNotFound, apierr.CodeNotFound)
	downloadTestAssertNoToken(t, recorder, token)
	if lookup.count() != 0 {
		t.Error("a rejected token reached the owner lookup")
	}
	if factory.calls.Load() != 0 {
		t.Error("a rejected token dialed the node")
	}
}

func TestDownloadTokenRejections(t *testing.T) {
	clock := newDownloadTestClock()
	baseToken, base := downloadTestBaseToken(t, clock)
	payloadBytes, signature := downloadTestTokenParts(t, baseToken)
	// Pin the HMAC domain used by the production verifier: the signed
	// payload is exactly the canonical JSON prefixed by the download domain
	// and a NUL byte. The crafted-token helpers below rely on this.
	if !hmac.Equal(signature, downloadTestSignPayload(payloadBytes, downloadTestKey)) {
		t.Fatal("issued download token does not use the expected domain-separated HMAC")
	}

	canonical := func(payload downloadTestTokenPayload) []byte {
		return downloadTestObject(t, payload)
	}
	mutated := func(mutate func(*downloadTestTokenPayload)) string {
		payload := base
		mutate(&payload)
		return downloadTestTokenFromRaw(canonical(payload))
	}
	rawWith := func(mutate func([]byte) []byte) string {
		return downloadTestTokenFromRaw(mutate(bytes.Clone(canonical(base))))
	}
	flippedSignature := bytes.Clone(signature)
	flippedSignature[len(flippedSignature)-1] ^= 1

	tests := []struct {
		name  string
		token string
	}{
		{name: "empty", token: ""},
		{name: "too long", token: strings.Repeat("a", 40000)},
		{name: "no separator", token: "abcdef"},
		{name: "empty payload", token: "." + base64.RawURLEncoding.EncodeToString(signature)},
		{name: "empty signature", token: base64.RawURLEncoding.EncodeToString(payloadBytes) + "."},
		{name: "extra separator", token: baseToken + ".x"},
		{name: "bad payload base64", token: "!!!!." + base64.RawURLEncoding.EncodeToString(signature)},
		{name: "bad signature base64", token: base64.RawURLEncoding.EncodeToString(payloadBytes) + ".!!!!"},
		{name: "padding in payload", token: base64.RawURLEncoding.EncodeToString(payloadBytes) + "=." + base64.RawURLEncoding.EncodeToString(signature)},
		{name: "padding in signature", token: base64.RawURLEncoding.EncodeToString(payloadBytes) + "." + base64.RawURLEncoding.EncodeToString(signature) + "="},
		{name: "short signature", token: base64.RawURLEncoding.EncodeToString(payloadBytes) + "." + base64.RawURLEncoding.EncodeToString(signature[:16])},
		{name: "wrong signature", token: base64.RawURLEncoding.EncodeToString(payloadBytes) + "." + base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{0x00}, 32))},
		{name: "small signature difference", token: base64.RawURLEncoding.EncodeToString(payloadBytes) + "." + base64.RawURLEncoding.EncodeToString(flippedSignature)},
		{name: "wrong key", token: downloadTestTokenWithKey(canonical(base), []byte("another-installation-signing-key-0123456789"))},
		{name: "noncanonical whitespace", token: rawWith(func(raw []byte) []byte { return append([]byte(" "), raw...) })},
		{name: "noncanonical reordered", token: downloadTestTokenFromRaw(fmt.Appendf(nil,
			`{"u":%q,"v":%d,"o":%q,"s":%q,"p":%q,"e":%d,"n":%q}`,
			base.Purpose, base.Version, base.Owner, base.SandboxID, base.Path, base.ExpiresAt, base.Nonce))},
		{name: "duplicate member", token: downloadTestTokenFromRaw(bytes.Replace(canonical(base), []byte(`"v":1`), []byte(`"v":1,"v":1`), 1))},
		{name: "unknown member", token: rawWith(func(raw []byte) []byte { return append(raw[:len(raw)-1], []byte(`,"x":1}`)...) })},
		{name: "trailing value", token: rawWith(func(raw []byte) []byte { return append(raw, []byte(" {}")...) })},
		{name: "version", token: mutated(func(payload *downloadTestTokenPayload) { payload.Version = 2 })},
		{name: "purpose upload", token: mutated(func(payload *downloadTestTokenPayload) { payload.Purpose = "upload" })},
		{name: "purpose empty", token: mutated(func(payload *downloadTestTokenPayload) { payload.Purpose = "" })},
		{name: "purpose uppercase", token: mutated(func(payload *downloadTestTokenPayload) { payload.Purpose = "DOWNLOAD" })},
		{name: "empty owner", token: mutated(func(payload *downloadTestTokenPayload) { payload.Owner = "" })},
		{name: "long owner", token: mutated(func(payload *downloadTestTokenPayload) { payload.Owner = strings.Repeat("a", 257) })},
		{name: "empty sandbox", token: mutated(func(payload *downloadTestTokenPayload) { payload.SandboxID = "" })},
		{name: "long sandbox", token: mutated(func(payload *downloadTestTokenPayload) { payload.SandboxID = strings.Repeat("a", 257) })},
		{name: "relative path", token: mutated(func(payload *downloadTestTokenPayload) { payload.Path = "relative.txt" })},
		{name: "nul path", token: mutated(func(payload *downloadTestTokenPayload) { payload.Path = "/home/user/a\x00b" })},
		{name: "outside path", token: mutated(func(payload *downloadTestTokenPayload) { payload.Path = "/etc/passwd" })},
		{name: "directory root path", token: mutated(func(payload *downloadTestTokenPayload) { payload.Path = "/home/user" })},
		{name: "noncanonical path", token: mutated(func(payload *downloadTestTokenPayload) { payload.Path = "/home/user/a/../b" })},
		{name: "long path", token: mutated(func(payload *downloadTestTokenPayload) {
			payload.Path = "/home/user/" + strings.Repeat("a", 4096)
		})},
		{name: "empty nonce", token: mutated(func(payload *downloadTestTokenPayload) { payload.Nonce = "" })},
		{name: "short nonce", token: mutated(func(payload *downloadTestTokenPayload) { payload.Nonce = "AAAA" })},
		{name: "padded nonce", token: mutated(func(payload *downloadTestTokenPayload) { payload.Nonce += "=" })},
		{name: "expiry zero", token: mutated(func(payload *downloadTestTokenPayload) { payload.ExpiresAt = 0 })},
		{name: "expiry past", token: mutated(func(payload *downloadTestTokenPayload) { payload.ExpiresAt = clock.Now().Add(-time.Second).Unix() })},
		{name: "expiry too far", token: mutated(func(payload *downloadTestTokenPayload) {
			payload.ExpiresAt = clock.Now().Add((api.MaxDownloadTTLSeconds + 1) * time.Second).Unix()
		})},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			testClock := newDownloadTestClock()
			lookup := downloadTestReadyLookup()
			factory := downloadTestRejectingFactory(t)
			handlers := downloadTestHandlers(t, lookup, factory.factory(), testClock)
			downloadTestTokenCaseRejects(t, handlers.Receive, lookup, factory, test.token)
		})
	}

	t.Run("expired", func(t *testing.T) {
		testClock := newDownloadTestClock()
		issuer := downloadTestHandlers(t, downloadTestReadyLookup(), downloadTestRejectingFactory(t).factory(), testClock)
		token, _ := downloadTestIssueTokenTTL(t, issuer, downloadTestOwner, downloadTestSandboxID, "x", 1, downloadTestBaseURL)
		testClock.Advance(2 * time.Second)
		lookup := downloadTestReadyLookup()
		factory := downloadTestRejectingFactory(t)
		handlers := downloadTestHandlers(t, lookup, factory.factory(), testClock)
		downloadTestTokenCaseRejects(t, handlers.Receive, lookup, factory, token)
	})
}

func TestDownloadTokenTamperedSegments(t *testing.T) {
	clock := newDownloadTestClock()
	token, _ := downloadTestBaseToken(t, clock)
	payload, signature := downloadTestTokenParts(t, token)
	encodedPayload := base64.RawURLEncoding.EncodeToString(payload)
	encodedSignature := base64.RawURLEncoding.EncodeToString(signature)

	tests := []struct {
		name  string
		token string
	}{
		{"payload bit", downloadTestFlipMiddle(encodedPayload) + "." + encodedSignature},
		{"signature bit", encodedPayload + "." + downloadTestFlipMiddle(encodedSignature)},
		{"truncated signature", encodedPayload + "." + encodedSignature[:len(encodedSignature)-1]},
		{"extra payload char", encodedPayload + "A." + encodedSignature},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			lookup := downloadTestReadyLookup()
			factory := downloadTestRejectingFactory(t)
			handlers := downloadTestHandlers(t, lookup, factory.factory(), clock)
			downloadTestTokenCaseRejects(t, handlers.Receive, lookup, factory, test.token)
		})
	}
}

func TestDownloadTokenExpiryBoundary(t *testing.T) {
	clock := newDownloadTestClock()
	issuer := downloadTestHandlers(t, downloadTestReadyLookup(), downloadTestRejectingFactory(t).factory(), clock)
	token, _ := downloadTestIssueTokenTTL(t, issuer, downloadTestOwner, downloadTestSandboxID, "x", 1, downloadTestBaseURL)

	clock.Advance(time.Second - time.Millisecond)
	stream := downloadTestSuccessStream("/home/user/x")
	handlers := downloadTestHandlers(t, downloadTestReadyLookup(), downloadTestFactory(&downloadTestNodeClient{readFile: func(ctx context.Context, _ *nodev1.ReadFileRequest) (nodev1.Node_ReadFileClient, error) {
		stream.ctx = ctx
		return stream, nil
	}}), clock)
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/api/bbraft/downloads/"+token, nil)
	request.SetPathValue(api.DownloadTokenParam, token)
	handlers.Receive(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", recorder.Code)
	}

	clock.Advance(time.Millisecond)
	recorder = httptest.NewRecorder()
	request = httptest.NewRequest(http.MethodGet, "/api/bbraft/downloads/"+token, nil)
	request.SetPathValue(api.DownloadTokenParam, token)
	handlers.Receive(recorder, request)
	downloadTestTargets(t, recorder, http.StatusNotFound, apierr.CodeNotFound)
}

// TestDownloadTokenCrossPurposeSameKey proves the purpose claim and HMAC
// domain separate the two token kinds even when both handler constructors are
// given the identical signing key. The key derivation is not the only defense.
func TestDownloadTokenCrossPurposeSameKey(t *testing.T) {
	clock := newDownloadTestClock()
	lookup := downloadTestReadyLookup()
	factory := downloadTestRejectingFactory(t)

	uploads, err := api.NewUploadsHandlers(api.UploadsOptions{
		Lookup:     lookup,
		Clients:    factory.factory(),
		SigningKey: downloadTestKey,
		BaseURL:    downloadTestBaseURL,
		Logger:     downloadTestLogger(),
		Now:        clock.Now,
	})
	if err != nil {
		t.Fatalf("NewUploadsHandlers: %v", err)
	}
	downloads := downloadTestHandlers(t, lookup, factory.factory(), clock)

	downloadToken, _ := downloadTestIssueToken(t, downloads, downloadTestOwner, downloadTestSandboxID, "x")
	uploadToken := downloadTestIssueUploadToken(t, uploads, downloadTestOwner, downloadTestSandboxID, "x")

	// A download token must never authorize an upload.
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPut, "/api/bbraft/uploads/"+downloadToken, strings.NewReader("data"))
	request.SetPathValue(api.UploadTokenParam, downloadToken)
	uploads.Receive(recorder, request)
	downloadTestTargets(t, recorder, http.StatusNotFound, apierr.CodeNotFound)
	downloadTestAssertNoToken(t, recorder, downloadToken)

	// An upload token must never authorize a download.
	recorder = httptest.NewRecorder()
	request = httptest.NewRequest(http.MethodGet, "/api/bbraft/downloads/"+uploadToken, nil)
	request.SetPathValue(api.DownloadTokenParam, uploadToken)
	downloads.Receive(recorder, request)
	downloadTestTargets(t, recorder, http.StatusNotFound, apierr.CodeNotFound)
	downloadTestAssertNoToken(t, recorder, uploadToken)

	if lookup.count() != 2 { // only the two issuance lookups
		t.Errorf("lookup calls = %d, want 2 (rejections must not look up)", lookup.count())
	}
	if factory.calls.Load() != 0 {
		t.Fatal("a cross-purpose rejection dialed the node")
	}
}

func downloadTestIssueUploadToken(t *testing.T, handlers api.UploadsHandlers, owner, sandboxID, filePath string) string {
	t.Helper()
	mux := http.NewServeMux()
	mux.Handle("POST "+api.UploadsPath, handlers.Issue)
	request := httptest.NewRequest(http.MethodPost, "/api/v1/sandboxes/"+sandboxID+"/files/upload-url",
		strings.NewReader(`{"path":"`+filePath+`"}`))
	request.Header.Set("Content-Type", "application/json")
	request = request.WithContext(withDownloadTestIdentity(request, owner))
	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("upload issuance status = %d", recorder.Code)
	}
	var response api.FileURLResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode upload URL response failed (%T)", err)
	}
	prefix := downloadTestBaseURL + api.UploadReceiverPrefix + "/"
	token, ok := strings.CutPrefix(response.URL, prefix)
	if !ok || token == "" {
		t.Fatal("upload URL is not under the upload receiver prefix")
	}
	return token
}

func withDownloadTestIdentity(r *http.Request, owner string) context.Context {
	return auth.WithIdentity(r.Context(), auth.Identity{Owner: owner, KeyID: "key_download_test"})
}

// TestDownloadTokenKeyClone proves the constructor keeps its own key copy. It
// mutates the caller's slice and then issues a token: if the issuer had kept
// the mutable reference, the token would be signed with the mutated key and a
// replica holding the original key would reject it.
func TestDownloadTokenKeyClone(t *testing.T) {
	clock := newDownloadTestClock()
	original := bytes.Clone(downloadTestKey)
	callerKey := bytes.Clone(downloadTestKey)
	issuer := downloadTestHandlers(t, downloadTestReadyLookup(), downloadTestRejectingFactory(t).factory(), clock,
		func(o *api.DownloadsOptions) { o.SigningKey = callerKey })

	// Mutate before issuing so the token proves which key the issuer used.
	callerKey[0] ^= 0xff
	token, _ := downloadTestIssueToken(t, issuer, downloadTestOwner, downloadTestSandboxID, "/home/user/clone.txt")

	stream := downloadTestSuccessStream("/home/user/clone.txt")
	replica := downloadTestHandlers(t, downloadTestReadyLookup(), downloadTestFactory(&downloadTestNodeClient{
		readFile: func(ctx context.Context, _ *nodev1.ReadFileRequest) (nodev1.Node_ReadFileClient, error) {
			stream.ctx = ctx
			return stream, nil
		},
	}), clock, func(o *api.DownloadsOptions) { o.SigningKey = bytes.Clone(original) })
	recorder := downloadTestReceive(t, replica, http.MethodGet, token, "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("replica status = %d, want 200; issuer did not clone the signing key", recorder.Code)
	}
}

// TestDownloadTokenReplica proves a separate handler instance with the same
// installation key redeems a token with the signed owner and sandbox.
func TestDownloadTokenReplica(t *testing.T) {
	clock := newDownloadTestClock()
	issuer := downloadTestHandlers(t, downloadTestReadyLookup(), downloadTestRejectingFactory(t).factory(), clock,
		func(o *api.DownloadsOptions) { o.SigningKey = bytes.Clone(downloadTestKey) })
	token, _ := downloadTestIssueToken(t, issuer, downloadTestOwner, downloadTestSandboxID, "/home/user/x")

	receiverLookup := downloadTestReadyLookup()
	stream := downloadTestSuccessStream("/home/user/x")
	receiver := downloadTestHandlers(t, receiverLookup, downloadTestFactory(&downloadTestNodeClient{
		readFile: func(ctx context.Context, _ *nodev1.ReadFileRequest) (nodev1.Node_ReadFileClient, error) {
			stream.ctx = ctx
			return stream, nil
		},
	}), clock, func(o *api.DownloadsOptions) { o.SigningKey = bytes.Clone(downloadTestKey) })
	recorder := downloadTestReceive(t, receiver, http.MethodGet, token, "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("replica status = %d, want 200", recorder.Code)
	}
	if calls := receiverLookup.snapshot(); len(calls) != 1 ||
		calls[0].Owner != downloadTestOwner || calls[0].SandboxID != downloadTestSandboxID {
		t.Errorf("replica lookup calls = %+v", calls)
	}
}

func TestDownloadTokenNoSecretsInLogs(t *testing.T) {
	var buffer bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buffer, nil))
	clock := newDownloadTestClock()
	issuer := downloadTestHandlers(t, downloadTestReadyLookup(), downloadTestRejectingFactory(t).factory(), clock,
		func(o *api.DownloadsOptions) { o.Logger = logger })
	token, _ := downloadTestIssueToken(t, issuer, downloadTestOwner, downloadTestSandboxID, "secret.txt")
	payload, signature := downloadTestTokenParts(t, token)
	tampered := base64.RawURLEncoding.EncodeToString(payload) + "." + downloadTestFlipMiddle(base64.RawURLEncoding.EncodeToString(signature))

	receiver := downloadTestHandlers(t, downloadTestReadyLookup(), downloadTestRejectingFactory(t).factory(), clock,
		func(o *api.DownloadsOptions) { o.Logger = logger })
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/api/bbraft/downloads/"+tampered+"?path=/etc/passwd", nil)
	request.SetPathValue(api.DownloadTokenParam, tampered)
	receiver.Receive(recorder, request)
	downloadTestTargets(t, recorder, http.StatusNotFound, apierr.CodeNotFound)

	logged := buffer.String()
	for _, secret := range []string{token, tampered, "/api/bbraft/downloads/", "signature does not match"} {
		if strings.Contains(logged, secret) {
			t.Fatalf("log leaked token material or request URI")
		}
	}
	if !strings.Contains(logged, "malformed") && !strings.Contains(logged, "signature") {
		t.Fatal("log did not record a safe failure class")
	}
}
