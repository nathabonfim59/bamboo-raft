// Package memstore is an in-process objstore.Store for tests. It keeps every
// object in memory and cannot presign URLs.
package memstore

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"iter"
	"maps"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/nathabonfim59/bamboo-raft/internal/objstore"
)

// Store is an in-memory bucket. The zero value is not usable; call New.
type Store struct {
	mu   sync.Mutex
	objs map[string]object
	now  func() time.Time
}

type object struct {
	data     []byte
	modified time.Time
}

// New returns an empty store.
func New() *Store { return &Store{objs: map[string]object{}, now: time.Now} }

var _ objstore.Store = (*Store)(nil)

// PutIfAbsent implements objstore.Store.
func (s *Store) PutIfAbsent(ctx context.Context, key string, r io.Reader, size int64) error {
	if err := objstore.CheckKey(key); err != nil {
		return err
	}
	if size < 0 {
		return fmt.Errorf("memstore: put %s: negative size %d", key, size)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	_, exists := s.objs[key]
	s.mu.Unlock()
	if exists {
		return objstore.ErrExists
	}
	var buf bytes.Buffer
	n, err := io.Copy(&buf, io.LimitReader(r, size+1))
	if err != nil {
		return fmt.Errorf("memstore: put %s: %w", key, err)
	}
	if n != size {
		return fmt.Errorf("memstore: put %s: read %d bytes, want %d", key, n, size)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.objs[key]; exists {
		return objstore.ErrExists
	}
	s.objs[key] = object{data: buf.Bytes(), modified: s.now()}
	return nil
}

// Get implements objstore.Store.
func (s *Store) Get(ctx context.Context, key string, offset, length int64) (io.ReadCloser, error) {
	if err := objstore.CheckKey(key); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if offset < 0 {
		return nil, fmt.Errorf("memstore: get %s: negative offset", key)
	}
	s.mu.Lock()
	o, ok := s.objs[key]
	s.mu.Unlock()
	if !ok {
		return nil, objstore.ErrNotFound
	}
	size := int64(len(o.data))
	if offset > 0 && offset >= size {
		return nil, objstore.ErrInvalidRange
	}
	end := size
	if length >= 0 {
		end = min(size, offset+length)
	}
	return io.NopCloser(bytes.NewReader(o.data[offset:end])), nil
}

// Head implements objstore.Store.
func (s *Store) Head(ctx context.Context, key string) (objstore.Info, error) {
	if err := objstore.CheckKey(key); err != nil {
		return objstore.Info{}, err
	}
	if err := ctx.Err(); err != nil {
		return objstore.Info{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	o, ok := s.objs[key]
	if !ok {
		return objstore.Info{}, objstore.ErrNotFound
	}
	return objstore.Info{Key: key, Size: int64(len(o.data)), Modified: o.modified}, nil
}

// Delete implements objstore.Store.
func (s *Store) Delete(ctx context.Context, key string) error {
	if err := objstore.CheckKey(key); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.objs, key)
	return nil
}

// List implements objstore.Store. It lists a snapshot of the keys taken when
// the loop starts.
func (s *Store) List(ctx context.Context, prefix string) iter.Seq2[objstore.Info, error] {
	return func(yield func(objstore.Info, error) bool) {
		s.mu.Lock()
		var infos []objstore.Info
		for _, k := range slices.Sorted(maps.Keys(s.objs)) {
			if strings.HasPrefix(k, prefix) {
				o := s.objs[k]
				infos = append(infos, objstore.Info{Key: k, Size: int64(len(o.data)), Modified: o.modified})
			}
		}
		s.mu.Unlock()
		for _, info := range infos {
			if err := ctx.Err(); err != nil {
				yield(objstore.Info{}, err)
				return
			}
			if !yield(info, nil) {
				return
			}
		}
	}
}

// Presign implements objstore.Store: memstore cannot sign.
func (s *Store) Presign(context.Context, string, string, time.Duration) (string, error) {
	return "", objstore.ErrPresignUnsupported
}

// SetClock replaces the clock used for modification times (tests of GC grace
// periods). Not safe to call concurrently with writes.
func (s *Store) SetClock(now func() time.Time) { s.now = now }

// Len returns the number of objects (tests).
func (s *Store) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.objs)
}
