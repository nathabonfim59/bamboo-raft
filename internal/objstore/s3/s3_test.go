package s3

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nathabonfim59/bamboo-raft/internal/objstore"
	"github.com/nathabonfim59/bamboo-raft/internal/objstore/objstoretest"
)

const (
	fakeBucket = "testbucket"
	fakeRegion = "test-region"
	fakeAKID   = "AKIDEXAMPLE"
	fakeSecret = "secret-is-not-logged-0123456789"
)

// fakeObject is one stored object.
type fakeObject struct {
	data     []byte
	modified time.Time
}

// fakeS3 is a small path-style S3 server for the object operations the store
// uses. It does not verify signatures; tests assert the Authorization header
// shape separately.
type fakeS3 struct {
	mu        sync.Mutex
	bucket    string
	objects   map[string]fakeObject
	pageSize  int
	failHeads int
	failGets  int
	failPuts  int

	// rejectConditionalStatus, when non-zero, answers every conditional PUT
	// with this status and an error naming If-None-Match.
	rejectConditionalStatus int
	// ignoreConditional models Garage: the header is accepted and ignored, so
	// a conditional PUT overwrites.
	ignoreConditional bool
	conditionalPuts   int
	plainPuts         int
	headAttempts      int
	getAttempts       int
	listAttempts      int
	lastAuth          string
	lastContentHash   string
	ignoreRange       bool
}

func newFakeS3() *fakeS3 {
	return &fakeS3{bucket: fakeBucket, objects: map[string]fakeObject{}}
}

func fakeXMLError(w http.ResponseWriter, r *http.Request, status int, code, message string) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	if r.Method == http.MethodHead {
		return
	}
	fmt.Fprintf(w, "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>%s</Code><Message>%s</Message><RequestId>req-1</RequestId></Error>", code, message)
}

func (f *fakeS3) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	f.lastAuth = r.Header.Get("Authorization")
	f.lastContentHash = r.Header.Get("X-Amz-Content-Sha256")
	f.mu.Unlock()

	// Every request must be signed: the Authorization header for our own
	// calls, the query for presigned ones.
	presigned := r.URL.Query().Get("X-Amz-Signature") != ""
	if !presigned {
		if auth := r.Header.Get("Authorization"); !strings.HasPrefix(auth, "AWS4-HMAC-SHA256 Credential="+fakeAKID+"/") {
			fakeXMLError(w, r, http.StatusForbidden, "AccessDenied", "request is not signed")
			return
		}
		hash := r.Header.Get("X-Amz-Content-Sha256")
		want := emptyPayloadHash
		if r.Method == http.MethodPut {
			want = unsignedPayload
		}
		if hash != want {
			fakeXMLError(w, r, http.StatusForbidden, "AccessDenied", "payload hash header missing or wrong")
			return
		}
	}

	prefix := "/" + f.bucket
	if !strings.HasPrefix(r.URL.Path, prefix) {
		fakeXMLError(w, r, http.StatusNotFound, "NoSuchBucket", "no such bucket")
		return
	}
	key := strings.TrimPrefix(strings.TrimPrefix(r.URL.Path, prefix), "/")
	if key == "" {
		f.serveBucket(w, r)
		return
	}
	switch r.Method {
	case http.MethodPut:
		f.servePut(w, r, key)
	case http.MethodGet:
		if r.URL.Query().Get("list-type") != "" {
			f.serveList(w, r)
			return
		}
		f.serveGet(w, r, key)
	case http.MethodHead:
		f.serveHead(w, r, key)
	case http.MethodDelete:
		f.mu.Lock()
		delete(f.objects, key)
		f.mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	default:
		fakeXMLError(w, r, http.StatusMethodNotAllowed, "MethodNotAllowed", "method not allowed")
	}
}

func (f *fakeS3) serveBucket(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodHead:
		w.Header().Set("Content-Length", "0")
		w.WriteHeader(http.StatusOK)
	case http.MethodPut:
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusOK)
	case http.MethodGet:
		if r.URL.Query().Get("list-type") != "" {
			f.serveList(w, r)
			return
		}
		fakeXMLError(w, r, http.StatusMethodNotAllowed, "MethodNotAllowed", "method not allowed")
	default:
		fakeXMLError(w, r, http.StatusMethodNotAllowed, "MethodNotAllowed", "method not allowed")
	}
}

func (f *fakeS3) servePut(w http.ResponseWriter, r *http.Request, key string) {
	if len(r.TransferEncoding) != 0 {
		fakeXMLError(w, r, http.StatusBadRequest, "NotSupported", "chunked transfer is not supported")
		return
	}
	f.mu.Lock()
	if r.Header.Get("If-None-Match") != "" {
		f.conditionalPuts++
	} else {
		f.plainPuts++
	}
	reject := f.rejectConditionalStatus
	ignoreConditional := f.ignoreConditional
	fail := f.failPuts > 0
	if fail {
		f.failPuts--
	}
	_, exists := f.objects[key]
	f.mu.Unlock()

	if fail {
		fakeXMLError(w, r, http.StatusServiceUnavailable, "ServiceUnavailable", "try again")
		return
	}
	if reject != 0 && r.Header.Get("If-None-Match") == "*" {
		fakeXMLError(w, r, reject, "InvalidRequest", "this service does not support the If-None-Match header")
		return
	}
	if r.Header.Get("If-None-Match") == "*" && exists && !ignoreConditional {
		fakeXMLError(w, r, http.StatusPreconditionFailed, "PreconditionFailed", "the object already exists")
		return
	}
	data, err := io.ReadAll(r.Body)
	if err != nil {
		fakeXMLError(w, r, http.StatusBadRequest, "IncompleteBody", "the body could not be read")
		return
	}
	if r.ContentLength >= 0 && int64(len(data)) != r.ContentLength {
		fakeXMLError(w, r, http.StatusBadRequest, "IncompleteBody", "body length mismatch")
		return
	}
	f.mu.Lock()
	f.objects[key] = fakeObject{data: data, modified: time.Now()}
	f.mu.Unlock()
	w.WriteHeader(http.StatusOK)
}

func (f *fakeS3) serveGet(w http.ResponseWriter, r *http.Request, key string) {
	f.mu.Lock()
	f.getAttempts++
	fail := f.failGets > 0
	if fail {
		f.failGets--
	}
	ignoreRange := f.ignoreRange
	obj, ok := f.objects[key]
	f.mu.Unlock()

	if fail {
		fakeXMLError(w, r, http.StatusServiceUnavailable, "ServiceUnavailable", "try again")
		return
	}
	if !ok {
		fakeXMLError(w, r, http.StatusNotFound, "NoSuchKey", "the key does not exist")
		return
	}
	body := obj.data
	if spec, ok := strings.CutPrefix(r.Header.Get("Range"), "bytes="); ok && spec != "" && !ignoreRange {
		start, end, err := parseFakeRange(spec, int64(len(body)))
		if err != nil {
			w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", len(body)))
			fakeXMLError(w, r, http.StatusRequestedRangeNotSatisfiable, "InvalidRange", "the range is not satisfiable")
			return
		}
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(body)))
		body = body[start : end+1]
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(body)
		return
	}
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	w.Header().Set("Last-Modified", obj.modified.UTC().Format(http.TimeFormat))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body)
}

// parseFakeRange parses "start-" and "start-end".
func parseFakeRange(spec string, size int64) (start, end int64, err error) {
	first, last, ok := strings.Cut(spec, "-")
	if !ok {
		return 0, 0, errors.New("bad range")
	}
	start, err = strconv.ParseInt(first, 10, 64)
	if err != nil || start < 0 || start >= size {
		return 0, 0, errors.New("bad start")
	}
	if last == "" {
		return start, size - 1, nil
	}
	end, err = strconv.ParseInt(last, 10, 64)
	if err != nil || end < start {
		return 0, 0, errors.New("bad end")
	}
	return start, min(end, size-1), nil
}

func (f *fakeS3) serveHead(w http.ResponseWriter, r *http.Request, key string) {
	f.mu.Lock()
	f.headAttempts++
	fail := f.failHeads > 0
	if fail {
		f.failHeads--
	}
	obj, ok := f.objects[key]
	f.mu.Unlock()

	if fail {
		fakeXMLError(w, r, http.StatusServiceUnavailable, "ServiceUnavailable", "try again")
		return
	}
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Length", strconv.Itoa(len(obj.data)))
	w.Header().Set("Last-Modified", obj.modified.UTC().Format(http.TimeFormat))
	w.WriteHeader(http.StatusOK)
}

// fakeListResult is a ListObjectsV2 response document.
type fakeListResult struct {
	XMLName               xml.Name        `xml:"ListBucketResult"`
	Name                  string          `xml:"Name"`
	Prefix                string          `xml:"Prefix"`
	KeyCount              int             `xml:"KeyCount"`
	MaxKeys               int             `xml:"MaxKeys"`
	IsTruncated           bool            `xml:"IsTruncated"`
	Contents              []fakeListEntry `xml:"Contents"`
	NextContinuationToken string          `xml:"NextContinuationToken,omitempty"`
}

type fakeListEntry struct {
	Key          string `xml:"Key"`
	LastModified string `xml:"LastModified"`
	Size         int64  `xml:"Size"`
}

func (f *fakeS3) serveList(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	f.listAttempts++
	keys := make([]string, 0, len(f.objects))
	for key := range f.objects {
		keys = append(keys, key)
	}
	objects := make(map[string]fakeObject, len(f.objects))
	for key, obj := range f.objects {
		objects[key] = obj
	}
	pageSize := f.pageSize
	f.mu.Unlock()

	slices.Sort(keys)
	prefix := r.URL.Query().Get("prefix")
	filtered := keys[:0]
	for _, key := range keys {
		if strings.HasPrefix(key, prefix) {
			filtered = append(filtered, key)
		}
	}
	start := 0
	if token := r.URL.Query().Get("continuation-token"); token != "" {
		n, err := strconv.Atoi(token)
		if err != nil {
			fakeXMLError(w, r, http.StatusBadRequest, "InvalidArgument", "bad continuation token")
			return
		}
		start = n
	}
	if pageSize <= 0 {
		pageSize = 1000
	}
	slices.Sort(filtered)
	end := min(start+pageSize, len(filtered))
	if start > len(filtered) {
		start = len(filtered)
	}
	result := fakeListResult{
		Name:        f.bucket,
		Prefix:      prefix,
		KeyCount:    end - start,
		MaxKeys:     pageSize,
		IsTruncated: end < len(filtered),
	}
	for _, key := range filtered[start:end] {
		obj := objects[key]
		entry := fakeListEntry{
			Key:          key,
			LastModified: obj.modified.UTC().Format(time.RFC3339Nano),
			Size:         int64(len(obj.data)),
		}
		if r.URL.Query().Get("encoding-type") == "url" {
			entry.Key = url.QueryEscape(key)
		}
		result.Contents = append(result.Contents, entry)
	}
	if result.IsTruncated {
		result.NextContinuationToken = strconv.Itoa(end)
	}
	body, err := xml.Marshal(result)
	if err != nil {
		fakeXMLError(w, r, http.StatusInternalServerError, "InternalError", "marshal")
		return
	}
	w.Header().Set("Content-Type", "application/xml")
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body)
}

// newFakeStore starts a fake server with one store configured for it.
func newFakeStore(t *testing.T) (*Store, *fakeS3) {
	t.Helper()
	fake := newFakeS3()
	srv := httptest.NewServer(fake)
	t.Cleanup(srv.Close)
	s, err := New(t.Context(), Config{
		Endpoint:        srv.URL,
		Region:          fakeRegion,
		Bucket:          fakeBucket,
		PathStyle:       true,
		AccessKeyID:     fakeAKID,
		SecretAccessKey: fakeSecret,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	s.retryBase = time.Millisecond
	return s, fake
}

// TestStoreConformanceFake runs the whole objstore conformance suite against
// the store on the fake server.
func TestStoreConformanceFake(t *testing.T) {
	store, _ := newFakeStore(t)
	objstoretest.Run(t, store, objstoretest.Options{Presign: true, LargeSize: 1 << 20})
}

func TestPutExactLength(t *testing.T) {
	tests := []struct {
		name    string
		body    string
		size    int64
		wantErr string
		stored  string
	}{
		{name: "exact", body: "abcdef", size: 6, stored: "abcdef"},
		{name: "short", body: "abc", size: 6, wantErr: "ended after"},
		{name: "long", body: "abcdef", size: 3, wantErr: "more than the declared"},
		{name: "zero with data", body: "x", size: 0, wantErr: "more than the declared 0"},
		{name: "zero empty", body: "", size: 0, stored: ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store, _ := newFakeStore(t)
			key := "k"
			err := store.PutIfAbsent(t.Context(), key, strings.NewReader(tt.body), tt.size)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("PutIfAbsent = %v, want an error containing %q", err, tt.wantErr)
				}
				if _, err := store.Head(t.Context(), key); !errors.Is(err, objstore.ErrNotFound) {
					t.Fatalf("failed put left an object: Head = %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("PutIfAbsent: %v", err)
			}
			rc, err := store.Get(t.Context(), key, 0, -1)
			if err != nil {
				t.Fatal(err)
			}
			defer rc.Close()
			got, err := io.ReadAll(rc)
			if err != nil || string(got) != tt.stored {
				t.Fatalf("Get = %q, %v; want %q", got, err, tt.stored)
			}
		})
	}
}

func TestPutIfAbsentExists(t *testing.T) {
	store, _ := newFakeStore(t)
	ctx := t.Context()
	if err := store.PutIfAbsent(ctx, "k", strings.NewReader("first"), 5); err != nil {
		t.Fatal(err)
	}
	err := store.PutIfAbsent(ctx, "k", strings.NewReader("second"), 6)
	if !errors.Is(err, objstore.ErrExists) {
		t.Fatalf("second PutIfAbsent = %v, want ErrExists", err)
	}
	rc, err := store.Get(ctx, "k", 0, -1)
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()
	got, _ := io.ReadAll(rc)
	if string(got) != "first" {
		t.Fatalf("object overwritten: %q", got)
	}
}

// TestPutIfAbsentOnServiceIgnoringConditional models Garage, which accepts
// If-None-Match and ignores it: HEAD-first must still report ErrExists and
// leave the first object untouched.
func TestPutIfAbsentOnServiceIgnoringConditional(t *testing.T) {
	store, fake := newFakeStore(t)
	fake.ignoreConditional = true
	ctx := t.Context()
	if err := store.PutIfAbsent(ctx, "k", strings.NewReader("first"), 5); err != nil {
		t.Fatalf("first PutIfAbsent: %v", err)
	}
	err := store.PutIfAbsent(ctx, "k", strings.NewReader("second"), 6)
	if !errors.Is(err, objstore.ErrExists) {
		t.Fatalf("second PutIfAbsent = %v, want ErrExists", err)
	}
	rc, err := store.Get(ctx, "k", 0, -1)
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()
	got, _ := io.ReadAll(rc)
	if string(got) != "first" {
		t.Fatalf("object overwritten: %q", got)
	}
	fake.mu.Lock()
	conditional, plain := fake.conditionalPuts, fake.plainPuts
	fake.mu.Unlock()
	if conditional != 1 || plain != 0 {
		t.Fatalf("conditional puts = %d, plain puts = %d; want 1 and 0", conditional, plain)
	}
	if store.conditionalUnsupported.Load() {
		t.Fatal("store marked conditional writes unsupported although the service accepted the header")
	}
}

func TestConditionalFallback(t *testing.T) {
	for _, status := range []int{http.StatusBadRequest, http.StatusNotImplemented} {
		t.Run(strconv.Itoa(status), func(t *testing.T) {
			store, fake := newFakeStore(t)
			fake.rejectConditionalStatus = status
			ctx := t.Context()

			if err := store.PutIfAbsent(ctx, "a", strings.NewReader("one"), 3); err != nil {
				t.Fatalf("PutIfAbsent with fallback: %v", err)
			}
			if !store.conditionalUnsupported.Load() {
				t.Fatal("store did not remember the rejected conditional write")
			}
			// A second put uses HEAD then a plain PUT.
			if err := store.PutIfAbsent(ctx, "b", strings.NewReader("two"), 3); err != nil {
				t.Fatalf("second PutIfAbsent: %v", err)
			}
			fake.mu.Lock()
			conditional, plain := fake.conditionalPuts, fake.plainPuts
			fake.mu.Unlock()
			if conditional != 1 || plain != 2 {
				t.Fatalf("conditional puts = %d, plain puts = %d; want 1 and 2", conditional, plain)
			}
			// An existing key still reports ErrExists.
			if err := store.PutIfAbsent(ctx, "a", strings.NewReader("xx"), 2); !errors.Is(err, objstore.ErrExists) {
				t.Fatalf("put over an existing key = %v, want ErrExists", err)
			}
			rc, err := store.Get(ctx, "a", 0, -1)
			if err != nil {
				t.Fatal(err)
			}
			defer rc.Close()
			got, _ := io.ReadAll(rc)
			if string(got) != "one" {
				t.Fatalf("content = %q, want one", got)
			}
		})
	}
}

func TestConditionalFallbackNonReplayable(t *testing.T) {
	store, fake := newFakeStore(t)
	fake.rejectConditionalStatus = http.StatusBadRequest
	body := io.NopCloser(strings.NewReader("abcdef"))
	err := store.PutIfAbsent(t.Context(), "k", body, 6)
	if err == nil || !strings.Contains(err.Error(), "cannot be replayed") {
		t.Fatalf("PutIfAbsent = %v, want a replay error", err)
	}
	if _, err := store.Head(t.Context(), "k"); !errors.Is(err, objstore.ErrNotFound) {
		t.Fatalf("failed put left an object: Head = %v", err)
	}
}

func TestPutNotRetriedAfterBody(t *testing.T) {
	store, fake := newFakeStore(t)
	fake.failPuts = 2
	err := store.PutIfAbsent(t.Context(), "k", strings.NewReader("abc"), 3)
	if err == nil {
		t.Fatal("PutIfAbsent succeeded against a failing server")
	}
	fake.mu.Lock()
	attempts := fake.conditionalPuts
	fake.mu.Unlock()
	if attempts != 1 {
		t.Fatalf("PUT attempts = %d, want 1 (a sent body is not replayed)", attempts)
	}
	if _, err := store.Head(t.Context(), "k"); !errors.Is(err, objstore.ErrNotFound) {
		t.Fatalf("failed put left an object: Head = %v", err)
	}
}

func TestRetryIdempotent(t *testing.T) {
	store, fake := newFakeStore(t)
	fake.failHeads = 2
	if _, err := store.Head(t.Context(), "missing"); !errors.Is(err, objstore.ErrNotFound) {
		t.Fatalf("Head = %v, want ErrNotFound after retries", err)
	}
	fake.mu.Lock()
	attempts := fake.headAttempts
	fake.mu.Unlock()
	if attempts != 3 {
		t.Fatalf("HEAD attempts = %d, want 3", attempts)
	}

	fake2 := newFakeS3()
	srv := httptest.NewServer(fake2)
	t.Cleanup(srv.Close)
	store2, err := New(t.Context(), Config{
		Endpoint: srv.URL, Region: fakeRegion, Bucket: fakeBucket, PathStyle: true,
		AccessKeyID: fakeAKID, SecretAccessKey: fakeSecret,
	})
	if err != nil {
		t.Fatal(err)
	}
	store2.retryBase = time.Millisecond
	fake2.failHeads = 5
	if _, err := store2.Head(t.Context(), "missing"); err == nil {
		t.Fatal("Head succeeded against a permanently failing server")
	}
	fake2.mu.Lock()
	attempts = fake2.headAttempts
	fake2.mu.Unlock()
	if attempts != maxAttempts {
		t.Fatalf("HEAD attempts = %d, want %d", attempts, maxAttempts)
	}
}

// TestGetEmptyObjectRange pins the contract edge: any range at offset 0 on an
// empty object is an empty read, never ErrInvalidRange; a non-zero offset is
// still invalid.
func TestGetEmptyObjectRange(t *testing.T) {
	store, _ := newFakeStore(t)
	ctx := t.Context()
	if err := store.PutIfAbsent(ctx, "empty", strings.NewReader(""), 0); err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct {
		off, n int64
	}{
		{0, 5},
		{0, 1},
		{0, -1},
		{0, 0},
	} {
		rc, err := store.Get(ctx, "empty", tt.off, tt.n)
		if err != nil {
			t.Fatalf("Get(empty, %d, %d) = %v, want an empty reader", tt.off, tt.n, err)
		}
		got, err := io.ReadAll(rc)
		rc.Close()
		if err != nil || len(got) != 0 {
			t.Fatalf("Get(empty, %d, %d) = %q, %v; want empty", tt.off, tt.n, got, err)
		}
	}
	for _, tt := range []struct{ off, n int64 }{{1, 1}, {1, -1}, {5, 10}} {
		if _, err := store.Get(ctx, "empty", tt.off, tt.n); !errors.Is(err, objstore.ErrInvalidRange) {
			t.Errorf("Get(empty, %d, %d) = %v, want ErrInvalidRange", tt.off, tt.n, err)
		}
	}
}

func TestGetZeroLength(t *testing.T) {
	store, _ := newFakeStore(t)
	ctx := t.Context()
	if err := store.PutIfAbsent(ctx, "k", strings.NewReader("abc"), 3); err != nil {
		t.Fatal(err)
	}
	rc, err := store.Get(ctx, "k", 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	got, _ := io.ReadAll(rc)
	rc.Close()
	if len(got) != 0 {
		t.Fatalf("Get(0, 0) = %q, want empty", got)
	}
	if _, err := store.Get(ctx, "k", 3, 0); !errors.Is(err, objstore.ErrInvalidRange) {
		t.Fatalf("Get(3, 0) = %v, want ErrInvalidRange", err)
	}
	if _, err := store.Get(ctx, "missing", 0, 0); !errors.Is(err, objstore.ErrNotFound) {
		t.Fatalf("Get(missing, 0, 0) = %v, want ErrNotFound", err)
	}
}

func TestListPaginationAndEncoding(t *testing.T) {
	store, fake := newFakeStore(t)
	fake.pageSize = 2
	ctx := t.Context()
	want := []string{
		"list/a b",
		"list/b/1",
		"list/b/2",
		"list/c",
		"list/é",
	}
	for i, key := range want {
		if err := store.PutIfAbsent(ctx, key, strings.NewReader(strconv.Itoa(i)), 1); err != nil {
			t.Fatalf("put %q: %v", key, err)
		}
	}
	var got []string
	for info, err := range store.List(ctx, "list/") {
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, info.Key)
		if info.Size != 1 {
			t.Errorf("size of %q = %d, want 1", info.Key, info.Size)
		}
		if info.Modified.IsZero() {
			t.Errorf("modified of %q is zero", info.Key)
		}
	}
	if !slices.Equal(got, want) {
		t.Fatalf("List = %q, want %q", got, want)
	}
	fake.mu.Lock()
	pages := fake.listAttempts
	fake.mu.Unlock()
	if pages != 3 {
		t.Fatalf("List pages = %d, want 3", pages)
	}

	// Breaking out stops fetching pages.
	fake.mu.Lock()
	fake.listAttempts = 0
	fake.mu.Unlock()
	n := 0
	for _, err := range store.List(ctx, "list/") {
		if err != nil {
			t.Fatal(err)
		}
		n++
		break
	}
	fake.mu.Lock()
	pages = fake.listAttempts
	fake.mu.Unlock()
	if n != 1 || pages != 1 {
		t.Fatalf("after break: entries %d, pages %d; want 1 and 1", n, pages)
	}
}

func TestListEmptyPrefix(t *testing.T) {
	store, _ := newFakeStore(t)
	ctx := t.Context()
	for _, key := range []string{"a", "b/c"} {
		if err := store.PutIfAbsent(ctx, key, strings.NewReader("x"), 1); err != nil {
			t.Fatal(err)
		}
	}
	var got []string
	for info, err := range store.List(ctx, "") {
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, info.Key)
	}
	if !slices.Equal(got, []string{"a", "b/c"}) {
		t.Fatalf("List(\"\") = %q", got)
	}
}

func TestDeleteMissing(t *testing.T) {
	store, _ := newFakeStore(t)
	if err := store.Delete(t.Context(), "missing"); err != nil {
		t.Fatalf("Delete(missing) = %v, want nil", err)
	}
}

// TestGetServiceIgnoresRange covers the fallback where a service answers a
// ranged GET with the whole object.
func TestGetServiceIgnoresRange(t *testing.T) {
	store, fake := newFakeStore(t)
	fake.ignoreRange = true
	ctx := t.Context()
	if err := store.PutIfAbsent(ctx, "k", strings.NewReader("0123456789"), 10); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		off, n int64
		want   string
	}{
		{0, 3, "012"},
		{4, 2, "45"},
		{7, -1, "789"},
		{8, 100, "89"},
	}
	for _, tt := range tests {
		rc, err := store.Get(ctx, "k", tt.off, tt.n)
		if err != nil {
			t.Fatalf("Get(%d, %d): %v", tt.off, tt.n, err)
		}
		got, err := io.ReadAll(rc)
		rc.Close()
		if err != nil || string(got) != tt.want {
			t.Errorf("Get(%d, %d) = %q, %v; want %q", tt.off, tt.n, got, err, tt.want)
		}
	}
	if _, err := store.Get(ctx, "k", 10, 1); !errors.Is(err, objstore.ErrInvalidRange) {
		t.Errorf("Get at the end = %v, want ErrInvalidRange", err)
	}
}

func TestListErrors(t *testing.T) {
	tests := []struct {
		name    string
		handler http.HandlerFunc
		wantErr string
	}{
		{
			name: "forbidden",
			handler: func(w http.ResponseWriter, r *http.Request) {
				fakeXMLError(w, r, http.StatusForbidden, "AccessDenied", "denied")
			},
			wantErr: "AccessDenied",
		},
		{
			name: "malformed xml",
			handler: func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.WriteString(w, "<ListBucketResult><Contents>")
			},
			wantErr: "decode response",
		},
		{
			name: "truncated without token",
			handler: func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.WriteString(w, `<?xml version="1.0"?><ListBucketResult><IsTruncated>true</IsTruncated></ListBucketResult>`)
			},
			wantErr: "without a continuation token",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := httptest.NewServer(tt.handler)
			t.Cleanup(srv.Close)
			store, err := newStore(Config{
				Endpoint: srv.URL, Region: fakeRegion, Bucket: fakeBucket, PathStyle: true,
				AccessKeyID: fakeAKID, SecretAccessKey: fakeSecret,
			}, srv.Client())
			if err != nil {
				t.Fatal(err)
			}
			var errs []error
			for _, err := range store.List(t.Context(), "p/") {
				errs = append(errs, err)
			}
			if len(errs) != 1 || errs[0] == nil || !strings.Contains(errs[0].Error(), tt.wantErr) {
				t.Fatalf("List errors = %v, want one containing %q", errs, tt.wantErr)
			}
		})
	}
}

func TestCreateBucket(t *testing.T) {
	tests := []struct {
		name      string
		putStatus int
		putCode   string
		wantErr   string
	}{
		{name: "created", putStatus: http.StatusOK},
		{name: "already owned", putStatus: http.StatusConflict, putCode: "BucketAlreadyOwnedByYou"},
		{name: "already exists", putStatus: http.StatusConflict, putCode: "BucketAlreadyExists", wantErr: "BucketAlreadyExists"},
		{name: "forbidden", putStatus: http.StatusForbidden, putCode: "AccessDenied", wantErr: "AccessDenied"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var bodyLen int
			var chunked bool
			handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodHead {
					w.WriteHeader(http.StatusOK)
					return
				}
				if r.Method == http.MethodPut {
					b, _ := io.ReadAll(r.Body)
					bodyLen = len(b)
					chunked = len(r.TransferEncoding) != 0
					if tt.putStatus/100 != 2 {
						fakeXMLError(w, r, tt.putStatus, tt.putCode, "bucket problem")
						return
					}
					w.WriteHeader(tt.putStatus)
					return
				}
				w.WriteHeader(http.StatusNotFound)
			})
			srv := httptest.NewServer(handler)
			t.Cleanup(srv.Close)
			_, err := New(t.Context(), Config{
				Endpoint: srv.URL, Region: fakeRegion, Bucket: "b-created", PathStyle: true,
				AccessKeyID: fakeAKID, SecretAccessKey: fakeSecret, CreateBucket: true,
			})
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("New = %v, want an error containing %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			if bodyLen != 0 || chunked {
				t.Fatalf("create bucket body %d bytes, chunked %t; want an empty, non-chunked PUT", bodyLen, chunked)
			}
		})
	}
}

func TestRetryStopsOnCancel(t *testing.T) {
	store, fake := newFakeStore(t)
	fake.failHeads = 5
	store.retryBase = 50 * time.Millisecond
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
	defer cancel()
	if _, err := store.Head(ctx, "k"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Head = %v, want context.DeadlineExceeded", err)
	}
	fake.mu.Lock()
	attempts := fake.headAttempts
	fake.mu.Unlock()
	if attempts != 1 {
		t.Fatalf("HEAD attempts = %d, want 1 (backoff stopped at the deadline)", attempts)
	}
}

func TestPresignValidation(t *testing.T) {
	store, _ := newFakeStore(t)
	ctx := t.Context()
	u, err := store.Presign(ctx, http.MethodGet, "k", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := url.Parse(u)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"X-Amz-Algorithm", "X-Amz-Credential", "X-Amz-Date", "X-Amz-Expires", "X-Amz-Signature", "X-Amz-SignedHeaders"} {
		if parsed.Query().Get(name) == "" {
			t.Errorf("presigned URL lacks %s: %s", name, u)
		}
	}
	if strings.Contains(u, fakeSecret) {
		t.Error("presigned URL contains the secret")
	}
	for _, tt := range []struct {
		method  string
		expires time.Duration
		wantErr error
	}{
		{http.MethodDelete, time.Minute, ErrInvalidArgument},
		{http.MethodGet, time.Millisecond, ErrInvalidArgument},
		{http.MethodGet, 8 * 24 * time.Hour, ErrInvalidArgument},
	} {
		if _, err := store.Presign(ctx, tt.method, "k", tt.expires); !errors.Is(err, tt.wantErr) {
			t.Errorf("Presign(%s, %v) = %v, want %v", tt.method, tt.expires, err, tt.wantErr)
		}
	}
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := store.Presign(cctx, http.MethodGet, "k", time.Minute); err == nil {
		t.Error("Presign on a cancelled context succeeded")
	}
}

func TestNewCreateBucket(t *testing.T) {
	fake := newFakeS3()
	fake.bucket = "b-created"
	var putBody []byte
	var mu sync.Mutex
	wrapped := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut && strings.TrimSuffix(r.URL.Path, "/") == "/b-created" {
			body, _ := io.ReadAll(r.Body)
			mu.Lock()
			putBody = body
			mu.Unlock()
			w.WriteHeader(http.StatusOK)
			return
		}
		fake.ServeHTTP(w, r)
	})
	srv := httptest.NewUnstartedServer(wrapped)
	srv.Start()
	t.Cleanup(srv.Close)

	store, err := New(t.Context(), Config{
		Endpoint: srv.URL, Region: fakeRegion, Bucket: "b-created", PathStyle: true,
		AccessKeyID: fakeAKID, SecretAccessKey: fakeSecret,
		CreateBucket: true, LocationHint: "auto",
	})
	if err != nil {
		t.Fatalf("New with CreateBucket: %v", err)
	}
	mu.Lock()
	body := string(putBody)
	mu.Unlock()
	if !strings.Contains(body, "<LocationConstraint>auto</LocationConstraint>") {
		t.Fatalf("create bucket body = %q", body)
	}
	if store.Config().Bucket != "b-created" {
		t.Fatalf("Config().Bucket = %q", store.Config().Bucket)
	}
}

func TestNewErrors(t *testing.T) {
	newWithHandler := func(t *testing.T, h http.HandlerFunc, cfg Config) (*Store, error) {
		t.Helper()
		srv := httptest.NewServer(h)
		t.Cleanup(srv.Close)
		cfg.Endpoint = srv.URL
		cfg.Region = fakeRegion
		cfg.Bucket = fakeBucket
		cfg.PathStyle = true
		cfg.AccessKeyID = fakeAKID
		cfg.SecretAccessKey = fakeSecret
		return New(t.Context(), cfg)
	}
	tests := []struct {
		name    string
		handler http.HandlerFunc
		wantErr string
	}{
		{
			name: "bucket missing",
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusNotFound)
			},
			wantErr: "does not exist",
		},
		{
			name: "access denied",
			handler: func(w http.ResponseWriter, r *http.Request) {
				fakeXMLError(w, r, http.StatusForbidden, "AccessDenied", "denied")
			},
			wantErr: "access denied",
		},
		{
			name: "region hint",
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("X-Amz-Bucket-Region", "eu-west-1")
				w.WriteHeader(http.StatusMovedPermanently)
			},
			wantErr: `region "eu-west-1"`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := newWithHandler(t, tt.handler, Config{}); err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("New = %v, want an error containing %q", err, tt.wantErr)
			}
		})
	}
}

func TestNewValidation(t *testing.T) {
	base := Config{
		Endpoint: "http://127.0.0.1:3900", Region: "garage", Bucket: "testbucket", PathStyle: true,
		AccessKeyID: fakeAKID, SecretAccessKey: fakeSecret,
	}
	tests := []struct {
		name    string
		mutate  func(*Config)
		wantErr string
	}{
		{name: "ok", mutate: func(*Config) {}},
		{name: "no bucket", mutate: func(c *Config) { c.Bucket = "" }, wantErr: "Bucket is required"},
		{name: "short bucket", mutate: func(c *Config) { c.Bucket = "ab" }, wantErr: "not a valid S3 bucket"},
		{name: "uppercase bucket", mutate: func(c *Config) { c.Bucket = "MyBucket" }, wantErr: "not a valid S3 bucket"},
		{name: "double dot bucket", mutate: func(c *Config) { c.Bucket = "a..b" }, wantErr: "not a valid S3 bucket"},
		{name: "ip bucket", mutate: func(c *Config) { c.Bucket = "192.168.0.1" }, wantErr: "not a valid S3 bucket"},
		{name: "no access key", mutate: func(c *Config) { c.AccessKeyID = "" }, wantErr: "AccessKeyID is required"},
		{name: "no secret", mutate: func(c *Config) { c.SecretAccessKey = "" }, wantErr: "SecretAccessKey is required"},
		{name: "no region with endpoint", mutate: func(c *Config) { c.Region = "" }, wantErr: "Region is required"},
		{name: "bad scheme", mutate: func(c *Config) { c.Endpoint = "ftp://example.com" }, wantErr: "scheme must be http or https"},
		{name: "no host", mutate: func(c *Config) { c.Endpoint = "http://" }, wantErr: "no host"},
		{name: "credentials in url", mutate: func(c *Config) { c.Endpoint = "http://user:pass@example.com" }, wantErr: "must not carry credentials"},
		{name: "query in url", mutate: func(c *Config) { c.Endpoint = "http://example.com/?x=1" }, wantErr: "must not carry credentials"},
		{name: "ip endpoint needs path style", mutate: func(c *Config) { c.PathStyle = false }, wantErr: "set s3.path-style"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := base
			tt.mutate(&cfg)
			_, err := newStore(cfg, nil)
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("newStore: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("newStore = %v, want an error containing %q", err, tt.wantErr)
			}
		})
	}
}

func TestAWSDefaultEndpoint(t *testing.T) {
	store, err := newStore(Config{
		Region: "eu-west-1", Bucket: "testbucket",
		AccessKeyID: fakeAKID, SecretAccessKey: fakeSecret,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	u := store.urlFor("k")
	if got, want := u.String(), "https://testbucket.s3.eu-west-1.amazonaws.com/k"; got != want {
		t.Fatalf("URL = %q, want %q", got, want)
	}

	store, err = newStore(Config{
		Bucket: "testbucket", AccessKeyID: fakeAKID, SecretAccessKey: fakeSecret,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := store.Config().Region; got != "us-east-1" {
		t.Fatalf("default region = %q, want us-east-1", got)
	}
}

func TestURLAddressing(t *testing.T) {
	tests := []struct {
		name      string
		endpoint  string
		pathStyle bool
		key       string
		want      string
	}{
		{"path style", "http://127.0.0.1:3900", true, "a/b", "http://127.0.0.1:3900/testbucket/a/b"},
		{"path style prefix", "http://example.com/s3/", true, "a", "http://example.com/s3/testbucket/a"},
		{"path style bucket", "http://example.com", true, "", "http://example.com/testbucket"},
		{"virtual host", "https://s3.example.com", false, "a/b", "https://testbucket.s3.example.com/a/b"},
		{"virtual host port", "https://s3.example.com:8443", false, "a", "https://testbucket.s3.example.com:8443/a"},
		{"virtual host bucket", "https://s3.example.com", false, "", "https://testbucket.s3.example.com/"},
		{"key with spaces and plus", "http://example.com", true, "a b+c/d", "http://example.com/testbucket/a%20b%2Bc/d"},
		{"key with query characters", "http://example.com", true, "a?b#c", "http://example.com/testbucket/a%3Fb%23c"},
		{"unicode key", "http://example.com", true, "é", "http://example.com/testbucket/%C3%A9"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store, err := newStore(Config{
				Endpoint: tt.endpoint, Region: fakeRegion, Bucket: "testbucket", PathStyle: tt.pathStyle,
				AccessKeyID: fakeAKID, SecretAccessKey: fakeSecret,
			}, nil)
			if err != nil {
				t.Fatal(err)
			}
			if got := store.urlFor(tt.key).String(); got != tt.want {
				t.Fatalf("URL = %q, want %q", got, tt.want)
			}
		})
	}
}

// TestSecretNeverInErrors walks the error paths and asserts neither the
// secret nor the Authorization header leaks into an error string.
func TestSecretNeverInErrors(t *testing.T) {
	store, fake := newFakeStore(t)
	ctx := t.Context()
	if err := store.PutIfAbsent(ctx, "k", strings.NewReader("v"), 1); err != nil {
		t.Fatal(err)
	}

	store2, fake2 := newFakeStore(t)
	fake2.failHeads = 5
	fake2.failGets = 5
	fake2.failPuts = 5

	var errs []error
	_, err := store.Get(ctx, "missing", 0, -1)
	errs = append(errs, err)
	_, err = store.Head(ctx, "missing")
	errs = append(errs, err)
	errs = append(errs, store.PutIfAbsent(ctx, "k", strings.NewReader("x"), 1))
	_, err = store2.Head(ctx, "x")
	errs = append(errs, err)
	_, err = store2.Get(ctx, "x", 0, -1)
	errs = append(errs, err)
	errs = append(errs, store2.PutIfAbsent(ctx, "x", strings.NewReader("x"), 1))

	fake.mu.Lock()
	auth := fake.lastAuth
	fake.mu.Unlock()
	if auth == "" {
		t.Fatal("fake server saw no Authorization header")
	}
	seen := 0
	for _, err := range errs {
		if err == nil {
			continue
		}
		seen++
		msg := fmt.Sprint(err)
		if strings.Contains(msg, fakeSecret) {
			t.Errorf("error contains the secret: %s", msg)
		}
		if strings.Contains(msg, auth) {
			t.Errorf("error contains the Authorization header: %s", msg)
		}
	}
	if seen < 4 {
		t.Fatalf("only %d error paths fired, want at least 4", seen)
	}
}
