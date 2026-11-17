package auth

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/nathabonfim59/bamboo-raft/internal/apierr"
)

// KeyConfig is one configured API key. Raw is hashed at load time and is
// never retained by Keys.
type KeyConfig struct {
	// ID names the record. Empty picks the owner id.
	ID string
	// Owner is the account id every sandbox of this key belongs to.
	Owner string
	// Raw is the bearer token as configured.
	Raw string
}

// KeyInfo is the safe metadata of one loaded key record: it carries no key
// material, only identifiers and usage data.
type KeyInfo struct {
	ID         string
	Owner      string
	LastUsedAt time.Time
}

// KeyRecord is a configured key in its storable form: the SHA-256 digest,
// never the raw key. It is for trusted consumers only — server startup
// persists it into the api_keys table; it must never reach the wire.
type KeyRecord struct {
	ID     string
	Owner  string
	Digest [sha256.Size]byte
}

// keyEntry is a loaded key: only the digest survives.
type keyEntry struct {
	id       string
	owner    string
	digest   [sha256.Size]byte
	lastUsed atomic.Int64 // unix nanos of the last successful Authenticate
}

// keysFile is the JSON shape of -auth.keys-file:
//
//	{"keys":[{"id":"main","owner":"<account id>","key":"<raw key>"}]}
type keysFile struct {
	Keys []struct {
		ID    string `json:"id"`
		Owner string `json:"owner"`
		Key   string `json:"key"`
	} `json:"keys"`
}

// Keys holds the configured API keys as SHA-256 digests and authenticates
// bearer tokens for them. The optional public prefix is the configured
// auth.key-prefix: a token without it never reaches a digest comparison.
type Keys struct {
	records []keyEntry
	prefix  string
}

// LoadKeysFile reads a keys file (keysFile JSON) into Keys.
func LoadKeysFile(ctx context.Context, path string) (*Keys, error) {
	return loadKeysFile(ctx, path, "")
}

// LoadKeysFileWithPrefix reads a keys file like [LoadKeysFile] and requires
// every configured raw key and every presented bearer token to start with
// prefix. The configured value is checked before hashing, and a refusal
// names only the record index — never the key or its material. An empty
// prefix disables the check, matching [LoadKeysFile].
func LoadKeysFileWithPrefix(ctx context.Context, path, prefix string) (*Keys, error) {
	return loadKeysFile(ctx, path, prefix)
}

// loadKeysFile is the shared loader. prefix is public key syntax, not secret
// material.
func loadKeysFile(ctx context.Context, path, prefix string) (*Keys, error) {
	raw, err := readCredFile(ctx, path)
	if err != nil {
		return nil, err
	}
	var f keysFile
	if err := unmarshalCred(path, "keys", raw, &f); err != nil {
		return nil, err
	}
	configs := make([]KeyConfig, 0, len(f.Keys))
	for _, k := range f.Keys {
		configs = append(configs, KeyConfig{ID: k.ID, Owner: k.Owner, Raw: k.Key})
	}
	ks, err := newKeys(configs, prefix)
	if err != nil {
		return nil, fmt.Errorf("auth: %s: %w", path, err)
	}
	return ks, nil
}

// NewKeys builds Keys from configured records. Each Raw is hashed
// immediately; nothing in Keys keeps it. Records need a nonempty owner and
// key; ids and keys must be unique.
func NewKeys(configs []KeyConfig) (*Keys, error) {
	return newKeys(configs, "")
}

// newKeys is the shared constructor. A nonempty prefix must match every
// configured raw key before any digest is computed.
func newKeys(configs []KeyConfig, prefix string) (*Keys, error) {
	if len(configs) == 0 {
		return nil, fmt.Errorf("auth: no api keys configured")
	}
	seenIDs := map[string]bool{}
	seenKeys := map[[sha256.Size]byte]bool{}
	records := make([]keyEntry, 0, len(configs))
	for i, c := range configs {
		if c.Owner == "" {
			return nil, fmt.Errorf("auth: key %d has no owner", i)
		}
		if c.Raw == "" {
			return nil, fmt.Errorf("auth: key %d has no key", i)
		}
		if prefix != "" && !strings.HasPrefix(c.Raw, prefix) {
			// Fixed index only: the raw key and prefix never reach an
			// error string.
			return nil, fmt.Errorf("auth: key %d does not use the configured key prefix", i)
		}
		id := c.ID
		if id == "" {
			id = c.Owner
		}
		if seenIDs[id] {
			return nil, fmt.Errorf("auth: duplicate key id %q", id)
		}
		seenIDs[id] = true
		digest := sha256.Sum256([]byte(c.Raw))
		if seenKeys[digest] {
			return nil, fmt.Errorf("auth: duplicate key %q", id)
		}
		seenKeys[digest] = true
		records = append(records, keyEntry{id: id, owner: c.Owner, digest: digest})
	}
	return &Keys{records: records, prefix: prefix}, nil
}

// Authenticate reports the identity a raw bearer token belongs to. A token
// without the configured public prefix is refused before any digest work;
// the comparison is then constant time over fixed-size digests.
func (k *Keys) Authenticate(token string) (Identity, bool) {
	if k.prefix != "" && !strings.HasPrefix(token, k.prefix) {
		return Identity{}, false
	}
	digest := sha256.Sum256([]byte(token))
	for i := range k.records {
		rec := &k.records[i]
		if subtle.ConstantTimeCompare(rec.digest[:], digest[:]) == 1 {
			rec.lastUsed.Store(time.Now().UnixNano())
			return Identity{Owner: rec.owner, KeyID: rec.id}, true
		}
	}
	return Identity{}, false
}

// KeyInfo returns the safe metadata of every record, in file order.
func (k *Keys) KeyInfo() []KeyInfo {
	infos := make([]KeyInfo, 0, len(k.records))
	for i := range k.records {
		rec := &k.records[i]
		var used time.Time
		if n := rec.lastUsed.Load(); n != 0 {
			used = time.Unix(0, n)
		}
		infos = append(infos, KeyInfo{ID: rec.id, Owner: rec.owner, LastUsedAt: used})
	}
	return infos
}

// Records returns every configured key in storable form, for persisting
// the api_keys table at startup. See KeyRecord for the trust contract.
func (k *Keys) Records() []KeyRecord {
	recs := make([]KeyRecord, 0, len(k.records))
	for i := range k.records {
		rec := &k.records[i]
		recs = append(recs, KeyRecord{ID: rec.id, Owner: rec.owner, Digest: rec.digest})
	}
	return recs
}

// Middleware requires Authorization: Bearer <key> on every request and
// stores the key's Identity in the request context. Missing or unknown
// credentials answer 401 unauthorized; the message never says which.
func (k *Keys) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token, ok := bearerToken(r.Header.Get("Authorization"))
		if !ok {
			unauthorized(w, r)
			return
		}
		id, ok := k.Authenticate(token)
		if !ok {
			unauthorized(w, r)
			return
		}
		next.ServeHTTP(w, r.WithContext(WithIdentity(r.Context(), id)))
	})
}

func bearerToken(header string) (string, bool) {
	scheme, rest, ok := strings.Cut(header, " ")
	if !ok || !strings.EqualFold(scheme, "bearer") {
		return "", false
	}
	token := strings.TrimSpace(rest)
	return token, token != ""
}

func unauthorized(w http.ResponseWriter, r *http.Request) {
	apierr.WriteError(w, r, apierr.New(http.StatusUnauthorized, apierr.CodeUnauthorized, "Unauthorized"))
}
