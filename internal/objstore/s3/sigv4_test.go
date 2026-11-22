package s3

import (
	"net/http"
	"net/url"
	"testing"
	"time"
)

// awsTestCredentials are the published example keys of the AWS SigV4
// documentation. They are public test data, not a secret.
var awsTestCredentials = credentials{
	accessKeyID:     "AKIAIOSFODNN7EXAMPLE",
	secretAccessKey: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
}

func mustParseTime(t *testing.T, layout, value string) time.Time {
	t.Helper()
	tm, err := time.Parse(layout, value)
	if err != nil {
		t.Fatalf("parse %q: %v", value, err)
	}
	return tm
}

// TestSignV4S3DocsExample is the "GET Object" example of the S3 SigV4
// documentation, whose signature is published.
func TestSignV4S3DocsExample(t *testing.T) {
	req, err := http.NewRequest(http.MethodGet, "https://examplebucket.s3.amazonaws.com/test.txt", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Range", "bytes=0-9")
	req.Header.Set("X-Amz-Content-Sha256", emptyPayloadHash)
	now := mustParseTime(t, amzDateFormat, "20130524T000000Z")

	signV4(req, awsTestCredentials, "us-east-1", "s3", emptyPayloadHash, now)

	want := "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, " +
		"SignedHeaders=host;range;x-amz-content-sha256;x-amz-date, " +
		"Signature=f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41"
	if got := req.Header.Get("Authorization"); got != want {
		t.Fatalf("Authorization = %q, want %q", got, want)
	}
	if got := req.Header.Get("X-Amz-Date"); got != "20130524T000000Z" {
		t.Errorf("X-Amz-Date = %q", got)
	}
}

// TestSignV4AWSSuiteHeader is the get-vanilla vector of the AWS SigV4 test
// suite (header form).
func TestSignV4AWSSuiteHeader(t *testing.T) {
	req, err := http.NewRequest(http.MethodGet, "https://example.amazonaws.com/", nil)
	if err != nil {
		t.Fatal(err)
	}
	creds := credentials{
		accessKeyID:     "AKIDEXAMPLE",
		secretAccessKey: "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
	}
	now := mustParseTime(t, amzDateFormat, "20150830T123600Z")

	signV4(req, creds, "us-east-1", "service", emptyPayloadHash, now)

	want := "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/service/aws4_request, " +
		"SignedHeaders=host;x-amz-date, " +
		"Signature=5fa00fa31553b73ebf1942676e86291e8372ff2a2260956d9b8aae1d763fbf31"
	if got := req.Header.Get("Authorization"); got != want {
		t.Fatalf("Authorization = %q, want %q", got, want)
	}
}

// TestPresignV4AWSSuiteQuery is the get-vanilla vector of the AWS SigV4 test
// suite (query-string form).
func TestPresignV4AWSSuiteQuery(t *testing.T) {
	u, err := url.Parse("https://example.amazonaws.com/")
	if err != nil {
		t.Fatal(err)
	}
	creds := credentials{
		accessKeyID:     "AKIDEXAMPLE",
		secretAccessKey: "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
	}
	now := mustParseTime(t, amzDateFormat, "20150830T123600Z")

	presignV4(u, http.MethodGet, creds, "us-east-1", "service", emptyPayloadHash, now, time.Hour)

	q := u.Query()
	if got, want := q.Get("X-Amz-Signature"), "e93c787ed7f371d5c6b165c1b38ede9550f4dce4144713e844b25b7192d3865d"; got != want {
		t.Fatalf("X-Amz-Signature = %q, want %q", got, want)
	}
	if got := q.Get("X-Amz-Credential"); got != "AKIDEXAMPLE/20150830/us-east-1/service/aws4_request" {
		t.Errorf("X-Amz-Credential = %q", got)
	}
	if got := q.Get("X-Amz-Expires"); got != "3600" {
		t.Errorf("X-Amz-Expires = %q", got)
	}
	if got := q.Get("X-Amz-SignedHeaders"); got != "host" {
		t.Errorf("X-Amz-SignedHeaders = %q", got)
	}
}

func TestURIEncode(t *testing.T) {
	tests := []struct {
		in          string
		encodeSlash bool
		want        string
	}{
		{"simple", false, "simple"},
		{"abcXYZ019-._~", false, "abcXYZ019-._~"},
		{"a b", false, "a%20b"},
		{"a+b", false, "a%2Bb"},
		{"a/b", false, "a/b"},
		{"a/b", true, "a%2Fb"},
		{"a:b?c=d&e", false, "a%3Ab%3Fc%3Dd%26e"},
		{"%", false, "%25"},
		{"é", false, "%C3%A9"},
		{"", false, ""},
	}
	for _, tt := range tests {
		if got := uriEncode(tt.in, tt.encodeSlash); got != tt.want {
			t.Errorf("uriEncode(%q, %t) = %q, want %q", tt.in, tt.encodeSlash, got, tt.want)
		}
	}
}

func TestCanonicalQuery(t *testing.T) {
	q := url.Values{
		"b":         {"2", "1"},
		"a":         {"x y"},
		"prefix":    {"chunks/"},
		"A":         {"upper"},
		"list-type": {"2"},
	}
	want := "A=upper&a=x%20y&b=1&b=2&list-type=2&prefix=chunks%2F"
	if got := canonicalQuery(q); got != want {
		t.Fatalf("canonicalQuery = %q, want %q", got, want)
	}
}

func TestCanonicalHost(t *testing.T) {
	tests := []struct {
		raw  string
		want string
	}{
		{"https://examplebucket.s3.amazonaws.com/test.txt", "examplebucket.s3.amazonaws.com"},
		{"https://127.0.0.1:3900/bucket/key", "127.0.0.1:3900"},
		{"http://127.0.0.1:80/bucket", "127.0.0.1:80"},
		{"https://127.0.0.1:443/bucket", "127.0.0.1:443"},
	}
	for _, tt := range tests {
		u, err := url.Parse(tt.raw)
		if err != nil {
			t.Fatal(err)
		}
		if got := canonicalHost(u); got != tt.want {
			t.Errorf("canonicalHost(%q) = %q, want %q", tt.raw, got, tt.want)
		}
	}
}
