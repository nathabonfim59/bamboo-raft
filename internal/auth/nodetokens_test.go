package auth

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/nathabonfim59/bamboo-raft/internal/nodelink"
)

const (
	nodeID    = "node-example"
	nodeToken = "node-secret-token-0123456789"
)

var nodeDigest = hex.EncodeToString(func() []byte { d := sha256.Sum256([]byte(nodeToken)); return d[:] }())

func writeTokensFile(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "tokens.json")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLoadNodeTokensFile(t *testing.T) {
	other := strings.Repeat("ab", 32)
	path := writeTokensFile(t, `{"nodes":[
		{"id":"`+nodeID+`","tokenSha256":"`+nodeDigest+`"},
		{"id":"node-two","tokenSha256":"`+other+`"}
	]}`)
	tokens, err := LoadNodeTokensFile(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	if !tokens.Verify(nodeID, nodeToken) {
		t.Fatal("valid token rejected")
	}
	if tokens.Verify(nodeID, "wrong-token") {
		t.Fatal("wrong token accepted")
	}
	if tokens.Verify("unknown-node", nodeToken) {
		t.Fatal("unknown node accepted")
	}
	if tokens.Verify(nodeID, "") {
		t.Fatal("empty token accepted")
	}
}

func TestLoadNodeTokensFileMalformed(t *testing.T) {
	tests := []struct {
		name    string
		content string
	}{
		{"not json", `nope`},
		{"empty file", ``},
		{"no nodes", `{"nodes":[]}`},
		{"missing id", `{"nodes":[{"tokenSha256":"` + nodeDigest + `"}]}`},
		{"duplicate id", `{"nodes":[{"id":"a","tokenSha256":"` + nodeDigest + `"},{"id":"a","tokenSha256":"` + nodeDigest + `"}]}`},
		{"short digest", `{"nodes":[{"id":"a","tokenSha256":"` + nodeDigest[:62] + `"}]}`},
		{"long digest", `{"nodes":[{"id":"a","tokenSha256":"` + nodeDigest + "00" + `"}]}`},
		{"uppercase digest", `{"nodes":[{"id":"a","tokenSha256":"` + strings.ToUpper(nodeDigest) + `"}]}`},
		{"non hex", `{"nodes":[{"id":"a","tokenSha256":"` + strings.Repeat("zz", 32) + `"}]}`},
		{"missing digest", `{"nodes":[{"id":"a"}]}`},
		{"unknown member", `{"nodes":[{"id":"a","tokenSha256":"` + nodeDigest + `","token":"raw"}]}`},
		{"unknown top member", `{"nodes":[{"id":"a","tokenSha256":"` + nodeDigest + `"}],"x":1}`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := LoadNodeTokensFile(t.Context(), writeTokensFile(t, tc.content)); err == nil {
				t.Fatalf("accepted %s", tc.content)
			}
		})
	}
}

func TestLoadNodeTokensFileErrorKeepsSecrets(t *testing.T) {
	// A raw token mistakenly placed in the file must not echo in errors.
	const sentinel = "SENTINEL-raw-node-token"
	path := writeTokensFile(t, `{"nodes":[{"id":"a","tokenSha256":"`+sentinel+`"}]}`)
	_, err := LoadNodeTokensFile(t.Context(), path)
	if err == nil {
		t.Fatal("accepted malformed digest")
	}
	if strings.Contains(err.Error(), sentinel) {
		t.Fatalf("error leaks credential: %v", err)
	}
}

func TestLoadNodeTokensFileCanceled(t *testing.T) {
	path := writeTokensFile(t, `{"nodes":[{"id":"a","tokenSha256":"`+nodeDigest+`"}]}`)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := LoadNodeTokensFile(ctx, path); err == nil {
		t.Fatal("accepted canceled context")
	}
}

func TestNodeTokensAuthenticator(t *testing.T) {
	path := writeTokensFile(t, `{"nodes":[{"id":"`+nodeID+`","tokenSha256":"`+nodeDigest+`"}]}`)
	tokens, err := LoadNodeTokensFile(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	auth := tokens.Authenticator()
	if err := auth(t.Context(), nodelink.Hello{NodeID: nodeID, Token: nodeToken}); err != nil {
		t.Fatalf("valid hello refused: %v", err)
	}
	err = auth(t.Context(), nodelink.Hello{NodeID: nodeID, Token: "wrong"})
	if err == nil {
		t.Fatal("bad token accepted")
	}
	// The refusal is generic: it must not say id vs token, nor echo either.
	if strings.Contains(err.Error(), nodeID) || strings.Contains(err.Error(), "wrong") || strings.Contains(err.Error(), "token") {
		t.Fatalf("refusal leaks detail: %v", err)
	}
	err = auth(t.Context(), nodelink.Hello{NodeID: "ghost", Token: nodeToken})
	if err == nil {
		t.Fatal("unknown node accepted")
	}
	if strings.Contains(err.Error(), "ghost") {
		t.Fatalf("refusal leaks detail: %v", err)
	}
}
