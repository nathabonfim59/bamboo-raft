package api_test

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json/v2"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/nathabonfim59/bamboo-raft/internal/api"
	"github.com/nathabonfim59/bamboo-raft/internal/apierr"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

func uploadsTestSignPayloadWithKey(payload, key []byte) []byte {
	mac := hmac.New(sha256.New, key)
	mac.Write(payload)
	return mac.Sum(nil)
}

func uploadsTestTokenWithKey(payload, key []byte) string {
	return uploadsTestTokenWithSignature(payload, uploadsTestSignPayloadWithKey(payload, key))
}

func uploadsTestTokenWithSignature(payload, signature []byte) string {
	return base64.RawURLEncoding.EncodeToString(payload) + "." +
		base64.RawURLEncoding.EncodeToString(signature)
}

// uploadsTestBaseToken issues a token for a normal target and returns it with
// its decoded payload. The caller's clock is used for expiry.
func uploadsTestBaseToken(t *testing.T, clock *uploadsTestClock) (string, uploadsTestTokenPayload) {
	t.Helper()
	issuerLookup := uploadsTestReadyLookup()
	issuer := uploadsTestHandlers(t, issuerLookup, uploadsTestFactory(&uploadsTestNodeClient{}), clock)
	token := uploadsTestIssueToken(t, issuer, uploadsTestOwner, uploadsTestSandboxID, "x")
	return token, uploadsTestDecodeToken(t, token)
}

// uploadsTestRejectingFactory fails if a malformed token ever reaches the
// node.
func uploadsTestRejectingFactory() api.NodeClientFactory {
	return func(string) (nodev1.NodeClient, func() error, error) {
		return nil, nil, errors.New("token should have been rejected before dialing")
	}
}

func uploadsTestFlipMiddle(value string) string {
	if value == "" {
		return "A"
	}
	middle := len(value) / 2
	replacement := byte('A')
	if value[middle] == 'A' {
		replacement = 'B'
	}
	return value[:middle] + string(replacement) + value[middle+1:]
}

func uploadsTestTokenCaseRejects(t *testing.T, handler http.HandlerFunc, lookup *uploadsTestLookup, token string) {
	t.Helper()
	recorder := uploadsTestReceive(t, api.UploadsHandlers{Receive: handler}, http.MethodPut, token, strings.NewReader("data"))
	uploadsTestTargets(t, recorder, http.StatusNotFound, apierr.CodeNotFound)
	uploadsTestAssertNoToken(t, recorder, token)
	if lookup.count() != 0 {
		t.Error("a rejected token reached the owner lookup")
	}
}

func TestUploadTokenRejections(t *testing.T) {
	clock := newUploadsTestClock()
	baseToken, base := uploadsTestBaseToken(t, clock)
	payloadBytes, signature := uploadsTestTokenParts(t, baseToken)

	canonical := func(payload uploadsTestTokenPayload) []byte {
		raw, err := json.Marshal(payload)
		if err != nil {
			t.Fatalf("marshal token payload: %v", err)
		}
		return raw
	}
	mutated := func(mutate func(*uploadsTestTokenPayload)) string {
		payload := base
		mutate(&payload)
		return uploadsTestTokenFromRaw(canonical(payload))
	}
	rawWith := func(mutate func([]byte) []byte) string {
		return uploadsTestTokenFromRaw(mutate(bytes.Clone(canonical(base))))
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
		{name: "small signature difference", token: uploadsTestTokenWithSignature(payloadBytes, flippedSignature)},
		{name: "wrong key", token: uploadsTestTokenWithKey(canonical(base), []byte("another-installation-signing-key-0123456789"))},
		{name: "noncanonical whitespace", token: rawWith(func(raw []byte) []byte { return append([]byte(" "), raw...) })},
		{name: "noncanonical reordered", token: uploadsTestTokenFromRaw(fmt.Appendf(nil,
			`{"o":%q,"v":%d,"s":%q,"p":%q,"e":%d,"n":%q}`,
			base.Owner, base.Version, base.SandboxID, base.Path, base.ExpiresAt, base.Nonce))},
		{name: "duplicate member", token: uploadsTestTokenFromRaw(bytes.Replace(canonical(base), []byte(`"v":1`), []byte(`"v":1,"v":1`), 1))},
		{name: "unknown member", token: rawWith(func(raw []byte) []byte { return append(raw[:len(raw)-1], []byte(`,"x":1}`)...) })},
		{name: "trailing value", token: rawWith(func(raw []byte) []byte { return append(raw, []byte(" {}")...) })},
		{name: "version", token: mutated(func(payload *uploadsTestTokenPayload) { payload.Version = 2 })},
		{name: "empty owner", token: mutated(func(payload *uploadsTestTokenPayload) { payload.Owner = "" })},
		{name: "long owner", token: mutated(func(payload *uploadsTestTokenPayload) { payload.Owner = strings.Repeat("a", 257) })},
		{name: "empty sandbox", token: mutated(func(payload *uploadsTestTokenPayload) { payload.SandboxID = "" })},
		{name: "long sandbox", token: mutated(func(payload *uploadsTestTokenPayload) { payload.SandboxID = strings.Repeat("a", 257) })},
		{name: "relative path", token: mutated(func(payload *uploadsTestTokenPayload) { payload.Path = "relative.txt" })},
		{name: "nul path", token: mutated(func(payload *uploadsTestTokenPayload) { payload.Path = "/home/user/a\x00b" })},
		{name: "outside path", token: mutated(func(payload *uploadsTestTokenPayload) { payload.Path = "/etc/passwd" })},
		{name: "directory root path", token: mutated(func(payload *uploadsTestTokenPayload) { payload.Path = "/home/user" })},
		{name: "noncanonical path", token: mutated(func(payload *uploadsTestTokenPayload) { payload.Path = "/home/user/a/../b" })},
		{name: "long path", token: mutated(func(payload *uploadsTestTokenPayload) {
			payload.Path = "/home/user/" + strings.Repeat("a", 4096)
		})},
		{name: "empty nonce", token: mutated(func(payload *uploadsTestTokenPayload) { payload.Nonce = "" })},
		{name: "short nonce", token: mutated(func(payload *uploadsTestTokenPayload) { payload.Nonce = "AAAA" })},
		{name: "padded nonce", token: mutated(func(payload *uploadsTestTokenPayload) { payload.Nonce += "=" })},
		{name: "expiry zero", token: mutated(func(payload *uploadsTestTokenPayload) { payload.ExpiresAt = 0 })},
		{name: "expiry past", token: mutated(func(payload *uploadsTestTokenPayload) { payload.ExpiresAt = clock.Now().Add(-time.Second).Unix() })},
		{name: "expiry too far", token: mutated(func(payload *uploadsTestTokenPayload) {
			payload.ExpiresAt = clock.Now().Add((api.MaxUploadTTLSeconds + 1) * time.Second).Unix()
		})},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			testClock := newUploadsTestClock()
			lookup := uploadsTestReadyLookup()
			handlers := uploadsTestHandlers(t, lookup, uploadsTestRejectingFactory(), testClock)
			uploadsTestTokenCaseRejects(t, handlers.Receive, lookup, test.token)
		})
	}

	t.Run("expired", func(t *testing.T) {
		testClock := newUploadsTestClock()
		issuer := uploadsTestHandlers(t, uploadsTestReadyLookup(), uploadsTestRejectingFactory(), testClock)
		token := uploadsTestIssueTokenTTL(t, issuer, uploadsTestOwner, uploadsTestSandboxID, "x", 1)
		testClock.Advance(2 * time.Second)
		lookup := uploadsTestReadyLookup()
		handlers := uploadsTestHandlers(t, lookup, uploadsTestRejectingFactory(), testClock)
		uploadsTestTokenCaseRejects(t, handlers.Receive, lookup, token)
	})
}

func TestUploadTokenTamperedSegments(t *testing.T) {
	clock := newUploadsTestClock()
	token, _ := uploadsTestBaseToken(t, clock)
	payload, signature := uploadsTestTokenParts(t, token)
	encodedPayload := base64.RawURLEncoding.EncodeToString(payload)
	encodedSignature := base64.RawURLEncoding.EncodeToString(signature)

	tests := []struct {
		name  string
		token string
	}{
		{"payload bit", uploadsTestFlipMiddle(encodedPayload) + "." + encodedSignature},
		{"signature bit", encodedPayload + "." + uploadsTestFlipMiddle(encodedSignature)},
		{"truncated signature", encodedPayload + "." + encodedSignature[:len(encodedSignature)-1]},
		{"extra payload char", encodedPayload + "A." + encodedSignature},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			lookup := uploadsTestReadyLookup()
			handlers := uploadsTestHandlers(t, lookup, uploadsTestRejectingFactory(), clock)
			uploadsTestTokenCaseRejects(t, handlers.Receive, lookup, test.token)
		})
	}
}

func TestUploadTokenExpiryBoundary(t *testing.T) {
	clock := newUploadsTestClock()
	stream := uploadsTestNewStream()
	stream.recv = func() (*nodev1.PutFileResponse, error) {
		return &nodev1.PutFileResponse{Path: "/home/user/x", Size: 0}, nil
	}
	lookup := uploadsTestReadyLookup()
	handlers := uploadsTestHandlers(t, lookup, uploadsTestStreamFactory(stream), clock)
	token := uploadsTestIssueTokenTTL(t, handlers, uploadsTestOwner, uploadsTestSandboxID, "x", 1)

	clock.Advance(time.Second - time.Millisecond)
	recorder := uploadsTestReceive(t, handlers, http.MethodPut, token, strings.NewReader(""))
	uploadsTestDecodeUploaded(t, recorder)

	clock.Advance(time.Millisecond)
	recorder = uploadsTestReceive(t, handlers, http.MethodPut, token, strings.NewReader(""))
	uploadsTestTargets(t, recorder, http.StatusNotFound, apierr.CodeNotFound)
	uploadsTestAssertNoToken(t, recorder, token)
}

func TestUploadTokenDoesNotEchoInErrors(t *testing.T) {
	clock := newUploadsTestClock()
	token, _ := uploadsTestBaseToken(t, clock)
	payload, signature := uploadsTestTokenParts(t, token)
	tampered := base64.RawURLEncoding.EncodeToString(payload) + "." + uploadsTestFlipMiddle(base64.RawURLEncoding.EncodeToString(signature))
	lookup := uploadsTestReadyLookup()
	handlers := uploadsTestHandlers(t, lookup, uploadsTestRejectingFactory(), clock)
	recorder := uploadsTestReceive(t, handlers, http.MethodPut, tampered, strings.NewReader("data"))
	uploadsTestTargets(t, recorder, http.StatusNotFound, apierr.CodeNotFound)
	uploadsTestAssertNoToken(t, recorder, tampered)
	if bytes.Contains(recorder.Body.Bytes(), []byte("signature")) || bytes.Contains(recorder.Body.Bytes(), []byte("malformed")) {
		t.Fatal("receiver leaked a token verification class in the error body")
	}
}

func TestUploadTokenSignatureVerificationIsConstantTime(t *testing.T) {
	// This is a negative test: a signature that differs from the valid one
	// only in its final byte must be refused exactly like any other bad
	// signature. The implementation uses hmac.Equal, which compares in
	// constant time; the test proves the refusal path.
	clock := newUploadsTestClock()
	token, _ := uploadsTestBaseToken(t, clock)
	payload, signature := uploadsTestTokenParts(t, token)
	almost := bytes.Clone(signature)
	almost[len(almost)-1] ^= 1
	crafted := base64.RawURLEncoding.EncodeToString(payload) + "." + base64.RawURLEncoding.EncodeToString(almost)
	lookup := uploadsTestReadyLookup()
	handlers := uploadsTestHandlers(t, lookup, uploadsTestRejectingFactory(), clock)
	uploadsTestTokenCaseRejects(t, handlers.Receive, lookup, crafted)
}
