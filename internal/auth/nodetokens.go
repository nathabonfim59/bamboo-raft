package auth

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/nathabonfim59/bamboo-raft/internal/nodelink"
)

// tokensFile is the JSON shape of -node.tokens-file, per the approved
// cross-part default:
//
//	{"nodes":[{"id":"node-example","tokenSha256":"<64 lowercase hex>"}]}
type tokensFile struct {
	Nodes []struct {
		ID          string `json:"id"`
		TokenSHA256 string `json:"tokenSha256"`
	} `json:"nodes"`
}

// NodeTokens holds the SHA-256 digests of the tokens that let a node join
// the hub. The raw token travels in the nodelink hello, gets hashed here
// and is compared in constant time; it is never stored or logged.
type NodeTokens struct {
	digests map[string][sha256.Size]byte
}

// LoadNodeTokensFile reads a node tokens file into NodeTokens. Node ids
// must be unique and nonempty; tokenSha256 is exactly 64 lowercase hex
// characters (a 32-byte digest). The file needs at least one node.
func LoadNodeTokensFile(ctx context.Context, path string) (*NodeTokens, error) {
	raw, err := readCredFile(ctx, path)
	if err != nil {
		return nil, err
	}
	var f tokensFile
	if err := unmarshalCred(path, "node tokens", raw, &f); err != nil {
		return nil, err
	}
	if len(f.Nodes) == 0 {
		return nil, fmt.Errorf("auth: %s: no nodes configured", path)
	}
	digests := make(map[string][sha256.Size]byte, len(f.Nodes))
	for i, n := range f.Nodes {
		if n.ID == "" {
			return nil, fmt.Errorf("auth: %s: node %d has no id", path, i)
		}
		if _, dup := digests[n.ID]; dup {
			return nil, fmt.Errorf("auth: %s: duplicate node id %q", path, n.ID)
		}
		if len(n.TokenSHA256) != 2*sha256.Size || !lowerHex(n.TokenSHA256) {
			return nil, fmt.Errorf("auth: %s: node %q: tokenSha256 must be 64 lowercase hex characters", path, n.ID)
		}
		digest, err := hex.DecodeString(n.TokenSHA256)
		if err != nil {
			return nil, fmt.Errorf("auth: %s: node %q: tokenSha256: %w", path, n.ID, err)
		}
		digests[n.ID] = [sha256.Size]byte(digest)
	}
	return &NodeTokens{digests: digests}, nil
}

func lowerHex(s string) bool {
	for i := range len(s) {
		c := s[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

// Verify reports whether token is the configured token of nodeID. The
// comparison always runs: an unknown node compares against a zero digest
// and only then does Verify combine the outcomes, so the id cannot be
// probed by timing.
func (t *NodeTokens) Verify(nodeID, token string) bool {
	want, ok := t.digests[nodeID]
	if !ok {
		want = [sha256.Size]byte{}
	}
	got := sha256.Sum256([]byte(token))
	eq := subtle.ConstantTimeCompare(want[:], got[:])
	return ok && eq == 1
}

// Authenticator adapts NodeTokens to nodelink.Authenticator. The refusal
// is deliberately generic: it goes back to the node and must not reveal
// whether the id or the token failed.
func (t *NodeTokens) Authenticator() nodelink.Authenticator {
	return func(_ context.Context, h nodelink.Hello) error {
		if t.Verify(h.NodeID, h.Token) {
			return nil
		}
		return errors.New("authentication failed")
	}
}
