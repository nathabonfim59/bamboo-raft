package auth

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json/v2"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const testRawKey = "bmb_testkey0123456789abcdef"

func writeTempFile(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "keys.json")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLoadKeysFileWithPrefix(t *testing.T) {
	path := writeTempFile(t, `{"keys":[
		{"id":"main","owner":"acct_1","key":"`+testRawKey+`"},
		{"id":"second","owner":"acct_2","key":"bmb_otherkey9876543210"}
	]}`)
	ks, err := LoadKeysFileWithPrefix(t.Context(), path, "bmb_")
	if err != nil {
		t.Fatal(err)
	}
	if id, ok := ks.Authenticate(testRawKey); !ok || id.KeyID != "main" {
		t.Fatalf("valid prefixed key: %+v, %v", id, ok)
	}
	// The public prefix is enforced before digest comparison.
	for _, bad := range []string{"", "otherkey9876543210", "BMB_" + testRawKey[4:]} {
		if _, ok := ks.Authenticate(bad); ok {
			t.Fatalf("accepted an unprefixed or wrong-prefix token")
		}
	}
	// LoadKeysFile keeps the accepted no-prefix behavior.
	plain, err := LoadKeysFile(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := plain.Authenticate("bmb_otherkey9876543210"); !ok {
		t.Fatal("no-prefix loader rejected the exact configured key")
	}
}

func TestLoadKeysFileWithPrefixRejectsConfiguredKey(t *testing.T) {
	const secret = "not-prefixed-SENTINEL-value"
	path := writeTempFile(t, `{"keys":[
		{"id":"ok","owner":"acct_1","key":"bmb_goodkey"},
		{"id":"bad","owner":"acct_2","key":"`+secret+`"}
	]}`)
	_, err := LoadKeysFileWithPrefix(t.Context(), path, "bmb_")
	if err == nil {
		t.Fatal("accepted a configured key without the prefix")
	}
	if !strings.Contains(err.Error(), "key 1") {
		t.Fatal("error does not name the record index")
	}
	if strings.Contains(err.Error(), secret) || strings.Contains(err.Error(), "bmb_") {
		t.Fatal("error leaks key material")
	}
}

func TestLoadKeysFile(t *testing.T) {
	path := writeTempFile(t, `{"keys":[
		{"id":"main","owner":"acct_1","key":"`+testRawKey+`"},
		{"id":"second","owner":"acct_2","key":"bmb_otherkey9876543210"}
	]}`)
	ks, err := LoadKeysFile(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	id, ok := ks.Authenticate(testRawKey)
	if !ok {
		t.Fatal("valid key rejected")
	}
	if id.Owner != "acct_1" || id.KeyID != "main" {
		t.Fatalf("identity = %+v", id)
	}
	if _, ok := ks.Authenticate("bmb_wrong"); ok {
		t.Fatal("invalid key accepted")
	}
}

func TestLoadKeysFileMalformed(t *testing.T) {
	tests := []struct {
		name    string
		content string
	}{
		{"not json", `this is not json`},
		{"truncated", `{"keys":[{"id":"a","owner":"o","key":"k"`},
		{"wrong shape", `{"keys":"yes"}`},
		{"empty file", ``},
		{"no keys", `{"keys":[]}`},
		{"missing owner", `{"keys":[{"key":"k"}]}`},
		{"missing key", `{"keys":[{"owner":"o"}]}`},
		{"duplicate id", `{"keys":[{"id":"a","owner":"o1","key":"k1"},{"id":"a","owner":"o2","key":"k2"}]}`},
		{"duplicate key", `{"keys":[{"owner":"o1","key":"same"},{"owner":"o2","key":"same"}]}`},
		{"unknown member", `{"keys":[{"owner":"o","key":"k","secret_extra":"x"}]}`},
		{"unknown top member", `{"keys":[],"other":1}`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := LoadKeysFile(t.Context(), writeTempFile(t, tc.content)); err == nil {
				t.Fatalf("%s: malformed key file accepted", tc.name)
			}
		})
	}
}

func TestLoadKeysFileErrorKeepsSecrets(t *testing.T) {
	// A malformed file holding a credential must not echo it in the error.
	const secret = "bmb_SENTINELsecretvalue"
	path := writeTempFile(t, `{"keys":[{"owner":"o","key":"`+secret+`"}] trailing`)
	_, err := LoadKeysFile(t.Context(), path)
	if err == nil {
		t.Fatal("accepted malformed file")
	}
	if strings.Contains(err.Error(), secret) {
		t.Fatalf("error leaks credential: %v", err)
	}
}

func TestLoadKeysFileMissing(t *testing.T) {
	if _, err := LoadKeysFile(t.Context(), filepath.Join(t.TempDir(), "none.json")); err == nil {
		t.Fatal("accepted missing file")
	}
}

func TestLoadKeysFileCanceled(t *testing.T) {
	path := writeTempFile(t, `{"keys":[{"owner":"o","key":"k"}]}`)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := LoadKeysFile(ctx, path); err == nil {
		t.Fatal("accepted canceled context")
	}
}

func TestNewKeysValidation(t *testing.T) {
	if _, err := NewKeys(nil); err == nil {
		t.Fatal("no keys accepted")
	}
	if _, err := NewKeys([]KeyConfig{{Owner: "o"}}); err == nil {
		t.Fatal("empty raw key accepted")
	}
	if _, err := NewKeys([]KeyConfig{{Raw: "k"}}); err == nil {
		t.Fatal("empty owner accepted")
	}
	// An empty id defaults to the owner id.
	ks, err := NewKeys([]KeyConfig{{Owner: "acct", Raw: "k"}})
	if err != nil {
		t.Fatal(err)
	}
	id, _ := ks.Authenticate("k")
	if id.KeyID != "acct" {
		t.Fatalf("default key id = %q", id.KeyID)
	}
}

func TestAuthenticateConstantShape(t *testing.T) {
	ks, err := NewKeys([]KeyConfig{
		{ID: "a", Owner: "acct_a", Raw: testRawKey},
		{ID: "b", Owner: "acct_b", Raw: "bmb_second"},
	})
	if err != nil {
		t.Fatal(err)
	}
	// Prefixes and near-misses fail.
	for _, bad := range []string{"", "bmb_", testRawKey[:len(testRawKey)-1] + "x", "bmb_second2"} {
		if _, ok := ks.Authenticate(bad); ok {
			t.Fatal("accepted a bad token")
		}
	}
}

func TestKeyInfoAndRecords(t *testing.T) {
	ks, err := NewKeys([]KeyConfig{{ID: "main", Owner: "acct_1", Raw: testRawKey}})
	if err != nil {
		t.Fatal(err)
	}
	if got := ks.KeyInfo(); len(got) != 1 || got[0].ID != "main" || got[0].Owner != "acct_1" || !got[0].LastUsedAt.IsZero() {
		t.Fatalf("KeyInfo = %+v", got)
	}
	// LastUsedAt updates on a successful authenticate; it is safe metadata.
	if _, ok := ks.Authenticate(testRawKey); !ok {
		t.Fatal("key rejected")
	}
	if ks.KeyInfo()[0].LastUsedAt.IsZero() {
		t.Fatal("LastUsedAt not updated")
	}

	// Records carries the digest for durable api_keys rows, never the raw key.
	recs := ks.Records()
	if len(recs) != 1 || recs[0].ID != "main" || recs[0].Owner != "acct_1" {
		t.Fatalf("Records = %+v", recs)
	}
	want := sha256.Sum256([]byte(testRawKey))
	if subtle.ConstantTimeCompare(recs[0].Digest[:], want[:]) != 1 {
		t.Fatal("digest is not the SHA-256 of the raw key")
	}
	out, _ := json.Marshal(ks)
	if strings.Contains(string(out), testRawKey) {
		t.Fatal("raw key reachable through marshaling")
	}
}

func TestMiddleware(t *testing.T) {
	ks, err := NewKeys([]KeyConfig{{ID: "main", Owner: "acct_1", Raw: testRawKey}})
	if err != nil {
		t.Fatal(err)
	}
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id, ok := IdentityFrom(r.Context())
		if !ok {
			t.Error("no identity in context")
			return
		}
		if id.Owner != "acct_1" || id.KeyID != "main" {
			t.Errorf("identity = %+v", id)
		}
		w.WriteHeader(http.StatusNoContent)
	})
	h := ks.Middleware(next)

	tests := []struct {
		name   string
		header string
		want   int
	}{
		{"no header", "", http.StatusUnauthorized},
		{"basic scheme", "Basic abc", http.StatusUnauthorized},
		{"empty bearer", "Bearer ", http.StatusUnauthorized},
		{"wrong key", "Bearer bmb_wrong", http.StatusUnauthorized},
		{"valid", "Bearer " + testRawKey, http.StatusNoContent},
		{"lowercase scheme", "bearer " + testRawKey, http.StatusNoContent},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodGet, "/api/v1/sandboxes", nil)
			if tc.header != "" {
				r.Header.Set("Authorization", tc.header)
			}
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, r)
			if rec.Code != tc.want {
				t.Fatalf("status = %d, want %d", rec.Code, tc.want)
			}
			if tc.want == http.StatusUnauthorized {
				var body struct {
					OK    bool   `json:"ok"`
					Type  string `json:"type"`
					Code  string `json:"code"`
					ReqID string `json:"requestId"`
				}
				if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
					t.Fatalf("401 body not JSON: %v", err)
				}
				if body.OK || body.Type != "sandbox.error" || body.Code != "unauthorized" {
					t.Fatalf("401 body = %s", rec.Body.String())
				}
				if !strings.HasPrefix(body.ReqID, "req_") {
					t.Fatalf("401 requestId = %q", body.ReqID)
				}
				// The refusal never names the key or the failure reason.
				if strings.Contains(rec.Body.String(), testRawKey) {
					t.Fatal("401 echoes the presented key")
				}
			}
		})
	}
}

func TestIdentityRoundTrip(t *testing.T) {
	ctx := WithIdentity(t.Context(), Identity{Owner: "acct", KeyID: "k"})
	id, ok := IdentityFrom(ctx)
	if !ok || id.Owner != "acct" || id.KeyID != "k" {
		t.Fatalf("identity = %+v, %v", id, ok)
	}
	if _, ok := IdentityFrom(t.Context()); ok {
		t.Fatal("identity from bare context")
	}
}
