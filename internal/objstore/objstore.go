// Package objstore is the object store contract (PLAN.md §5.9): the portable
// subset of S3 that bamboo-raft relies on, so Garage, Cloudflare R2, AWS S3
// and other S3-compatible services work through configuration only.
//
// Integrity, retention and garbage collection are bamboo-raft's own: keys are
// immutable (written once, never overwritten), contents are verified by the
// caller (content-addressed chunks), and nothing here depends on ETags,
// server-side checksums, tagging, versioning or lifecycle rules.
//
// Implementations: memstore (in-process, for tests) and s3 (any S3 service).
// Every implementation passes objstoretest.Run.
package objstore

import (
	"context"
	"errors"
	"io"
	"iter"
	"net/http"
	"time"
)

// Errors every implementation returns (wrapped is fine; compare with
// errors.Is).
var (
	// ErrNotFound: the key does not exist.
	ErrNotFound = errors.New("objstore: object not found")
	// ErrExists: PutIfAbsent found the key already written.
	ErrExists = errors.New("objstore: object already exists")
	// ErrInvalidRange: Get with a range that starts at or past the end of a
	// non-empty object.
	ErrInvalidRange = errors.New("objstore: invalid range")
)

// Store is one bucket. Keys are non-empty, at most 1024 bytes, without a
// leading "/", and never contain "//", "\\", a NUL or other control byte, or
// a "." or ".." element (ValidKey). An invalid key is an error before any
// request is made.
//
// All methods are safe for concurrent use.
type Store interface {
	// PutIfAbsent writes the object if the key does not exist yet, and
	// returns ErrExists (writing nothing) if it does. size is the exact
	// length of r; reading fewer or more bytes is an error and leaves no
	// object behind. Once PutIfAbsent returns nil, Get and Head see the
	// object (read-after-write).
	//
	// Two concurrent PutIfAbsent calls for one key may both succeed on a
	// service without conditional writes; callers only put immutable,
	// content-addressed or uniquely named objects, so either result is the
	// same object.
	PutIfAbsent(ctx context.Context, key string, r io.Reader, size int64) error

	// Get opens the object, or a byte range of it: offset is the first
	// byte, length the count, length < 0 means to the end. A range past the
	// end is cut short; a range starting at or past the end of a non-empty
	// object is ErrInvalidRange. The caller closes the reader.
	Get(ctx context.Context, key string, offset, length int64) (io.ReadCloser, error)

	// Head returns the object's size and modification time, or ErrNotFound.
	Head(ctx context.Context, key string) (Info, error)

	// Delete removes the object. Deleting a missing key is not an error.
	Delete(ctx context.Context, key string) error

	// List yields the objects whose keys start with prefix, in ascending
	// key order. Breaking out of the loop stops listing. An error is yielded
	// once, as the last pair.
	List(ctx context.Context, prefix string) iter.Seq2[Info, error]

	// Presign returns a URL that performs method (http.MethodGet or
	// http.MethodPut) on the key without credentials until it expires.
	// Implementations that cannot sign return ErrPresignUnsupported.
	Presign(ctx context.Context, method, key string, expires time.Duration) (string, error)
}

// ErrPresignUnsupported: the store cannot hand out signed URLs (memstore);
// callers stream the object themselves.
var ErrPresignUnsupported = errors.New("objstore: presigned URLs unsupported")

// Info describes one object.
type Info struct {
	Key      string
	Size     int64
	Modified time.Time
}

// PresignMethods are the methods Presign accepts.
var PresignMethods = []string{http.MethodGet, http.MethodPut}

// MaxKeyLen is the longest key a Store accepts.
const MaxKeyLen = 1024

// ValidKey reports whether key is acceptable to every Store.
func ValidKey(key string) bool {
	if key == "" || len(key) > MaxKeyLen || key[0] == '/' {
		return false
	}
	start := 0
	for i := 0; i <= len(key); i++ {
		if i < len(key) {
			c := key[i]
			if c < 0x20 || c == 0x7f || c == '\\' {
				return false
			}
			if c != '/' {
				continue
			}
		}
		switch key[start:i] {
		case "", ".", "..":
			// An empty element is "//" or a trailing "/" (a directory
			// marker, never a valid object key here).
			return false
		}
		start = i + 1
	}
	return true
}

// KeyError is returned for an invalid key.
type KeyError struct{ Key string }

func (e *KeyError) Error() string { return "objstore: invalid key " + quoteKey(e.Key) }

func quoteKey(k string) string {
	if len(k) > 80 {
		k = k[:80] + "..."
	}
	return `"` + k + `"`
}

// CheckKey returns a *KeyError for an invalid key, nil otherwise.
// Implementations call it first in every method that takes a key.
func CheckKey(key string) error {
	if !ValidKey(key) {
		return &KeyError{Key: key}
	}
	return nil
}
