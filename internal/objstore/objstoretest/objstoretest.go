// Package objstoretest is the conformance suite every objstore.Store passes.
//
// Run it against a store that is empty or holds only keys outside a
// per-run random prefix; the suite writes only under that prefix and
// deletes what it wrote.
package objstoretest

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nathabonfim59/bamboo-raft/internal/objstore"
)

// Options tune the suite for one implementation.
type Options struct {
	// Presign: the store signs URLs; the suite then fetches and puts
	// through them with Client (default http.DefaultClient).
	Presign bool
	Client  *http.Client
	// LargeSize is the size of the large-object case. Default 9 MiB (crosses
	// typical multipart thresholds' first part).
	LargeSize int
}

// Run runs every case against store.
func Run(t *testing.T, store objstore.Store, opts Options) {
	t.Helper()
	if opts.LargeSize == 0 {
		opts.LargeSize = 9 << 20
	}
	if opts.Client == nil {
		opts.Client = http.DefaultClient
	}
	prefix := "objstoretest/" + rand.Text() + "/"
	s := &suite{store: store, opts: opts, prefix: prefix}
	t.Cleanup(func() { s.cleanup(t) })

	t.Run("PutGet", s.testPutGet)
	t.Run("PutIfAbsentExists", s.testPutIfAbsentExists)
	t.Run("PutEmpty", s.testPutEmpty)
	t.Run("PutShortReader", s.testPutShortReader)
	t.Run("PutLarge", s.testPutLarge)
	t.Run("GetRange", s.testGetRange)
	t.Run("NotFound", s.testNotFound)
	t.Run("Head", s.testHead)
	t.Run("Delete", s.testDelete)
	t.Run("List", s.testList)
	t.Run("ListBreak", s.testListBreak)
	t.Run("InvalidKeys", s.testInvalidKeys)
	t.Run("ConcurrentPuts", s.testConcurrentPuts)
	t.Run("Cancelled", s.testCancelled)
	if opts.Presign {
		t.Run("Presign", s.testPresign)
	} else {
		t.Run("PresignUnsupported", s.testPresignUnsupported)
	}
}

type suite struct {
	store  objstore.Store
	opts   Options
	prefix string
}

func (s *suite) key(parts ...string) string { return s.prefix + strings.Join(parts, "/") }

func (s *suite) put(t *testing.T, key string, data []byte) {
	t.Helper()
	if err := s.store.PutIfAbsent(t.Context(), key, bytes.NewReader(data), int64(len(data))); err != nil {
		t.Fatalf("PutIfAbsent(%s): %v", key, err)
	}
}

func (s *suite) get(t *testing.T, key string, off, n int64) []byte {
	t.Helper()
	rc, err := s.store.Get(t.Context(), key, off, n)
	if err != nil {
		t.Fatalf("Get(%s, %d, %d): %v", key, off, n, err)
	}
	defer rc.Close()
	b, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("Get(%s) read: %v", key, err)
	}
	return b
}

func (s *suite) cleanup(t *testing.T) {
	ctx := t.Context()
	if ctx.Err() != nil {
		return // test context already done; nothing we can do
	}
	for info, err := range s.store.List(ctx, s.prefix) {
		if err != nil {
			t.Logf("cleanup list: %v", err)
			return
		}
		if err := s.store.Delete(ctx, info.Key); err != nil {
			t.Logf("cleanup delete %s: %v", info.Key, err)
		}
	}
}

func randBytes(n int) []byte {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return b
}

func (s *suite) testPutGet(t *testing.T) {
	k := s.key("putget")
	data := []byte("hello, object store")
	s.put(t, k, data)
	if got := s.get(t, k, 0, -1); !bytes.Equal(got, data) {
		t.Fatalf("Get = %q, want %q", got, data)
	}
}

func (s *suite) testPutIfAbsentExists(t *testing.T) {
	k := s.key("exists")
	s.put(t, k, []byte("first"))
	err := s.store.PutIfAbsent(t.Context(), k, strings.NewReader("second"), 6)
	if !errors.Is(err, objstore.ErrExists) {
		t.Fatalf("second PutIfAbsent = %v, want ErrExists", err)
	}
	if got := s.get(t, k, 0, -1); string(got) != "first" {
		t.Fatalf("object overwritten: %q", got)
	}
}

func (s *suite) testPutEmpty(t *testing.T) {
	k := s.key("empty")
	s.put(t, k, nil)
	if got := s.get(t, k, 0, -1); len(got) != 0 {
		t.Fatalf("Get = %d bytes, want 0", len(got))
	}
	info, err := s.store.Head(t.Context(), k)
	if err != nil || info.Size != 0 {
		t.Fatalf("Head = %+v, %v; want size 0", info, err)
	}
}

func (s *suite) testPutShortReader(t *testing.T) {
	k := s.key("short")
	err := s.store.PutIfAbsent(t.Context(), k, strings.NewReader("abc"), 10)
	if err == nil {
		t.Fatal("PutIfAbsent with a short reader succeeded")
	}
	if _, err := s.store.Head(t.Context(), k); !errors.Is(err, objstore.ErrNotFound) {
		t.Fatalf("after a failed put, Head = %v, want ErrNotFound", err)
	}
}

func (s *suite) testPutLarge(t *testing.T) {
	k := s.key("large")
	data := randBytes(s.opts.LargeSize)
	s.put(t, k, data)
	if got := s.get(t, k, 0, -1); !bytes.Equal(got, data) {
		t.Fatalf("large object differs (got %d bytes)", len(got))
	}
}

func (s *suite) testGetRange(t *testing.T) {
	k := s.key("range")
	data := []byte("0123456789")
	s.put(t, k, data)
	cases := []struct {
		off, n int64
		want   string
	}{
		{0, 3, "012"},
		{4, 2, "45"},
		{7, -1, "789"},
		{8, 100, "89"},
		{9, 1, "9"},
	}
	for _, c := range cases {
		if got := s.get(t, k, c.off, c.n); string(got) != c.want {
			t.Errorf("Get(%d, %d) = %q, want %q", c.off, c.n, got, c.want)
		}
	}
	if _, err := s.store.Get(t.Context(), k, 10, 1); !errors.Is(err, objstore.ErrInvalidRange) {
		t.Errorf("Get at the end = %v, want ErrInvalidRange", err)
	}
}

func (s *suite) testNotFound(t *testing.T) {
	k := s.key("missing")
	if _, err := s.store.Get(t.Context(), k, 0, -1); !errors.Is(err, objstore.ErrNotFound) {
		t.Errorf("Get = %v, want ErrNotFound", err)
	}
	if _, err := s.store.Head(t.Context(), k); !errors.Is(err, objstore.ErrNotFound) {
		t.Errorf("Head = %v, want ErrNotFound", err)
	}
}

func (s *suite) testHead(t *testing.T) {
	k := s.key("head")
	before := time.Now().Add(-5 * time.Minute)
	s.put(t, k, []byte("12345"))
	info, err := s.store.Head(t.Context(), k)
	if err != nil {
		t.Fatal(err)
	}
	if info.Key != k || info.Size != 5 {
		t.Errorf("Head = %+v, want key %s size 5", info, k)
	}
	if info.Modified.Before(before) || info.Modified.After(time.Now().Add(5*time.Minute)) {
		t.Errorf("Head modified = %v, not near now", info.Modified)
	}
}

func (s *suite) testDelete(t *testing.T) {
	k := s.key("delete")
	s.put(t, k, []byte("x"))
	if err := s.store.Delete(t.Context(), k); err != nil {
		t.Fatal(err)
	}
	if _, err := s.store.Head(t.Context(), k); !errors.Is(err, objstore.ErrNotFound) {
		t.Fatalf("after Delete, Head = %v", err)
	}
	if err := s.store.Delete(t.Context(), k); err != nil {
		t.Fatalf("Delete of a missing key = %v, want nil", err)
	}
	// A deleted key can be written again.
	s.put(t, k, []byte("y"))
}

func (s *suite) testList(t *testing.T) {
	want := []string{s.key("list", "a"), s.key("list", "b", "1"), s.key("list", "b", "2"), s.key("list", "c")}
	for _, k := range []int{2, 0, 3, 1} {
		s.put(t, want[k], []byte(fmt.Sprint(k)))
	}
	s.put(t, s.key("listx"), []byte("outside")) // shares the string prefix "list" but not "list/"
	var got []string
	for info, err := range s.store.List(t.Context(), s.key("list")+"/") {
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, info.Key)
		if info.Size != 1 {
			t.Errorf("List size of %s = %d, want 1", info.Key, info.Size)
		}
	}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("List = %v, want %v", got, want)
	}
	var sub []string
	for info, err := range s.store.List(t.Context(), s.key("list", "b")+"/") {
		if err != nil {
			t.Fatal(err)
		}
		sub = append(sub, info.Key)
	}
	if len(sub) != 2 {
		t.Fatalf("List of a sub-prefix = %v", sub)
	}
}

func (s *suite) testListBreak(t *testing.T) {
	for i := range 5 {
		s.put(t, s.key("brk", fmt.Sprint(i)), []byte("x"))
	}
	n := 0
	for _, err := range s.store.List(t.Context(), s.key("brk")+"/") {
		if err != nil {
			t.Fatal(err)
		}
		n++
		if n == 2 {
			break
		}
	}
	if n != 2 {
		t.Fatalf("iterated %d after break", n)
	}
}

func (s *suite) testInvalidKeys(t *testing.T) {
	for _, k := range []string{"", "/abs", "a//b", "a/../b", "a/./b", "trailing/", "nul\x00", "back\\slash", strings.Repeat("k", objstore.MaxKeyLen+1)} {
		if err := s.store.PutIfAbsent(t.Context(), k, strings.NewReader("x"), 1); !isKeyError(err) {
			t.Errorf("PutIfAbsent(%q) = %v, want *KeyError", k, err)
		}
		if _, err := s.store.Get(t.Context(), k, 0, -1); !isKeyError(err) {
			t.Errorf("Get(%q) = %v, want *KeyError", k, err)
		}
		if _, err := s.store.Head(t.Context(), k); !isKeyError(err) {
			t.Errorf("Head(%q) = %v, want *KeyError", k, err)
		}
		if err := s.store.Delete(t.Context(), k); !isKeyError(err) {
			t.Errorf("Delete(%q) = %v, want *KeyError", k, err)
		}
	}
}

func (s *suite) testConcurrentPuts(t *testing.T) {
	var wg sync.WaitGroup
	errs := make([]error, 16)
	for i := range errs {
		wg.Go(func() {
			k := s.key("conc", fmt.Sprint(i))
			d := randBytes(64 << 10)
			if err := s.store.PutIfAbsent(t.Context(), k, bytes.NewReader(d), int64(len(d))); err != nil {
				errs[i] = err
				return
			}
			rc, err := s.store.Get(t.Context(), k, 0, -1)
			if err != nil {
				errs[i] = err
				return
			}
			defer rc.Close()
			got, err := io.ReadAll(rc)
			if err == nil && !bytes.Equal(got, d) {
				err = fmt.Errorf("%s: content differs", k)
			}
			errs[i] = err
		})
	}
	wg.Wait()
	if err := errors.Join(errs...); err != nil {
		t.Fatal(err)
	}
}

func (s *suite) testCancelled(t *testing.T) {
	ctx := t.Context()
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	k := s.key("cancelled")
	if err := s.store.PutIfAbsent(cctx, k, strings.NewReader("x"), 1); err == nil {
		t.Error("PutIfAbsent on a cancelled context succeeded")
	}
	if _, err := s.store.Head(ctx, k); !errors.Is(err, objstore.ErrNotFound) {
		t.Errorf("cancelled put left an object: Head = %v", err)
	}
}

func (s *suite) testPresign(t *testing.T) {
	k := s.key("presign")
	put, err := s.store.Presign(t.Context(), http.MethodPut, k, 5*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	data := []byte("signed upload")
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPut, put, bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	req.ContentLength = int64(len(data))
	resp, err := s.opts.Client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		t.Fatalf("presigned PUT: %s", resp.Status)
	}
	get, err := s.store.Presign(t.Context(), http.MethodGet, k, 5*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	req, err = http.NewRequestWithContext(t.Context(), http.MethodGet, get, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err = s.opts.Client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	got, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK || !bytes.Equal(got, data) {
		t.Fatalf("presigned GET: %s %q", resp.Status, got)
	}
	if _, err := s.store.Presign(t.Context(), http.MethodDelete, k, time.Minute); err == nil {
		t.Error("Presign(DELETE) succeeded; only GET and PUT are allowed")
	}
}

func (s *suite) testPresignUnsupported(t *testing.T) {
	if _, err := s.store.Presign(t.Context(), http.MethodGet, s.key("p"), time.Minute); !errors.Is(err, objstore.ErrPresignUnsupported) {
		t.Fatalf("Presign = %v, want ErrPresignUnsupported", err)
	}
}

func isKeyError(err error) bool {
	_, ok := errors.AsType[*objstore.KeyError](err)
	return ok
}
