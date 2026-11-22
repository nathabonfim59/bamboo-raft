// AWS Signature Version 4 for S3: the Authorization header on every request
// and the query-string form used by presigned URLs.
package s3

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"maps"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"
)

const (
	// amzDateFormat is the timestamp format of X-Amz-Date.
	amzDateFormat = "20060102T150405Z"
	// emptyPayloadHash is the SHA-256 of an empty body.
	emptyPayloadHash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	// unsignedPayload tells the service not to verify the body hash; used for
	// streamed PUTs and presigned URLs, as S3 documents.
	unsignedPayload = "UNSIGNED-PAYLOAD"
	// signAlgorithm is the only SigV4 variant we produce.
	signAlgorithm = "AWS4-HMAC-SHA256"
)

// credentials are the only secret material the signer sees.
type credentials struct {
	accessKeyID     string
	secretAccessKey string
}

// signV4 signs req with the SigV4 Authorization header. It adds X-Amz-Date
// and signs the Host header, every x-amz-* header already present, and the
// request headers S3 understands that we set ourselves (Range, Content-Type,
// If-None-Match), so none of them can be altered in transit.
func signV4(req *http.Request, creds credentials, region, service, payloadHash string, now time.Time) {
	now = now.UTC()
	req.Header.Set("X-Amz-Date", now.Format(amzDateFormat))

	canonical := map[string]string{"host": canonicalHost(req.URL)}
	for _, name := range [...]string{
		"content-type", "if-none-match", "range",
		"x-amz-content-sha256", "x-amz-date", "x-amz-security-token",
	} {
		if v := req.Header.Get(name); v != "" {
			canonical[name] = canonicalHeaderValue(v)
		}
	}
	names := slices.Sorted(maps.Keys(canonical))
	var canonicalHeaders strings.Builder
	for _, name := range names {
		canonicalHeaders.WriteString(name)
		canonicalHeaders.WriteByte(':')
		canonicalHeaders.WriteString(canonical[name])
		canonicalHeaders.WriteByte('\n')
	}
	signedHeaders := strings.Join(names, ";")
	scope := credentialScope(now, region, service)

	request := strings.Join([]string{
		req.Method,
		canonicalURI(req.URL),
		canonicalQuery(req.URL.Query()),
		canonicalHeaders.String(),
		signedHeaders,
		payloadHash,
	}, "\n")
	sig := signatureV4(creds.secretAccessKey, now, region, service, request)
	req.Header.Set("Authorization", signAlgorithm+" Credential="+creds.accessKeyID+"/"+scope+
		", SignedHeaders="+signedHeaders+", Signature="+sig)
}

// presignV4 turns u into a presigned URL for method. payloadHash is
// unsignedPayload for S3 objects; the generic SigV4 test vectors use the
// empty-body hash.
func presignV4(u *url.URL, method string, creds credentials, region, service, payloadHash string, now time.Time, expires time.Duration) {
	now = now.UTC()
	scope := credentialScope(now, region, service)
	q := u.Query()
	q.Set("X-Amz-Algorithm", signAlgorithm)
	q.Set("X-Amz-Credential", creds.accessKeyID+"/"+scope)
	q.Set("X-Amz-Date", now.Format(amzDateFormat))
	q.Set("X-Amz-Expires", strconv.FormatInt(int64(expires/time.Second), 10))
	q.Set("X-Amz-SignedHeaders", "host")

	request := strings.Join([]string{
		method,
		canonicalURI(u),
		canonicalQuery(q),
		"host:" + canonicalHost(u) + "\n",
		"host",
		payloadHash,
	}, "\n")
	sig := signatureV4(creds.secretAccessKey, now, region, service, request)
	q.Set("X-Amz-Signature", sig)
	u.RawQuery = canonicalQuery(q)
}

// credentialScope is the date/region/service/terminator scope of a signature.
func credentialScope(now time.Time, region, service string) string {
	return now.Format("20060102") + "/" + region + "/" + service + "/aws4_request"
}

// signatureV4 computes the hex HMAC chain over the canonical request.
func signatureV4(secret string, now time.Time, region, service, canonicalRequest string) string {
	date := now.UTC().Format("20060102")
	stringToSign := strings.Join([]string{
		signAlgorithm,
		now.UTC().Format(amzDateFormat),
		credentialScope(now, region, service),
		sha256Hex([]byte(canonicalRequest)),
	}, "\n")

	key := hmacSHA256([]byte("AWS4"+secret), date)
	key = hmacSHA256(key, region)
	key = hmacSHA256(key, service)
	key = hmacSHA256(key, "aws4_request")
	return hex.EncodeToString(hmacSHA256(key, stringToSign))
}

func hmacSHA256(key []byte, data string) []byte {
	h := hmac.New(sha256.New, key)
	h.Write([]byte(data))
	return h.Sum(nil)
}

func sha256Hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// canonicalHost is the Host header value SigV4 signs; Go sends URL.Host
// verbatim, including any explicit port.
func canonicalHost(u *url.URL) string {
	return u.Host
}

// canonicalURI is the SigV4-encoded request path: every byte except the
// unreserved characters and "/" is percent-encoded.
func canonicalURI(u *url.URL) string {
	p := uriEncode(u.Path, false)
	if p == "" {
		return "/"
	}
	return p
}

// canonicalHeaderValue trims and collapses whitespace as SigV4 requires.
func canonicalHeaderValue(v string) string { return strings.Join(strings.Fields(v), " ") }

// canonicalQuery builds the SigV4 canonical query string: percent-encoded
// names and values, sorted by name then value, joined with "&".
func canonicalQuery(q url.Values) string {
	type pair struct{ k, v string }
	pairs := make([]pair, 0, len(q))
	for name, values := range q {
		k := uriEncode(name, true)
		for _, v := range values {
			pairs = append(pairs, pair{k, uriEncode(v, true)})
		}
	}
	slices.SortFunc(pairs, func(a, b pair) int {
		if a.k != b.k {
			return strings.Compare(a.k, b.k)
		}
		return strings.Compare(a.v, b.v)
	})
	var b strings.Builder
	for i, p := range pairs {
		if i > 0 {
			b.WriteByte('&')
		}
		b.WriteString(p.k)
		b.WriteByte('=')
		b.WriteString(p.v)
	}
	return b.String()
}

// uriEncode percent-encodes every byte outside the SigV4 unreserved set
// (A-Z a-z 0-9 - . _ ~), upper-case hex. Slashes are kept when encodeSlash is
// false.
func uriEncode(s string, encodeSlash bool) string {
	const hexDigits = "0123456789ABCDEF"
	var b strings.Builder
	b.Grow(len(s))
	for i := range len(s) {
		c := s[i]
		switch {
		case c >= 'A' && c <= 'Z', c >= 'a' && c <= 'z', c >= '0' && c <= '9', c == '-', c == '.', c == '_', c == '~':
			b.WriteByte(c)
		case c == '/' && !encodeSlash:
			b.WriteByte(c)
		default:
			b.WriteByte('%')
			b.WriteByte(hexDigits[c>>4])
			b.WriteByte(hexDigits[c&0xf])
		}
	}
	return b.String()
}

// checkPresignMethod reports whether Presign may sign method.
func checkPresignMethod(method string) error {
	switch method {
	case http.MethodGet, http.MethodPut:
		return nil
	default:
		return fmt.Errorf("%w: method %q (only GET and PUT)", ErrInvalidArgument, method)
	}
}
