// Package snapformat is the on-store format of disk snapshots (PLAN.md §5.2,
// decision 21): chunk and manifest object keys, the Manifest type, and how
// chunks and manifests are encoded. Nodes write it; the server reads it (sizes,
// fork, retention, chunk GC). Nothing else invents a key or a manifest field.
//
// A manifest describes a sandbox's WHOLE virtual disk over a base image, not a
// delta: every ChunkSize-aligned chunk the sandbox's overlay holds is listed
// with the SHA-256 of its uncompressed bytes (or as a zero chunk); a chunk
// that is not listed reads from the base image. Restoring needs only the
// manifest, the base image and the chunks it names. Increments come from
// content addressing: a chunk already in the store is never uploaded again.
//
// Object keys (immutable, written once with PutIfAbsent):
//
//	chunks/<sha256 hex of the uncompressed chunk>      zstd frame of the chunk
//	manifests/<chain id>/<generation>.json.zst         zstd frame of the JSON manifest
package snapformat

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json/v2"
	"errors"
	"fmt"
	"iter"
	"strconv"
	"strings"
	"time"
	"uuid"

	"github.com/klauspost/compress/zstd"
)

// ChunkSize is the size of every chunk but possibly the disk's last one.
const ChunkSize = 4 << 20

// Version is the manifest format version this package writes and reads.
const Version = 1

// Limits that keep a corrupt or hostile manifest from exhausting memory.
const (
	// MaxDiskBytes bounds Manifest.DiskBytes (4 TiB).
	MaxDiskBytes = 4 << 40
	// MaxManifestBytes bounds a decoded (decompressed) manifest.
	MaxManifestBytes = 128 << 20
)

const (
	chunkPrefix    = "chunks/"
	manifestPrefix = "manifests/"
	manifestSuffix = ".json.zst"
)

// ChunkPrefix is the key prefix of every chunk (for listing in GC).
const ChunkPrefix = chunkPrefix

// ManifestPrefix is the key prefix of every manifest.
const ManifestPrefix = manifestPrefix

// Hash is the SHA-256 of a chunk's uncompressed bytes.
type Hash [sha256.Size]byte

// String is the lowercase hex form used in keys and manifests.
func (h Hash) String() string { return hex.EncodeToString(h[:]) }

// ParseHash parses 64 lowercase hex digits.
func ParseHash(s string) (Hash, error) {
	var h Hash
	if len(s) != 2*len(h) || strings.ToLower(s) != s {
		return h, fmt.Errorf("snapformat: bad chunk hash %q", s)
	}
	if _, err := hex.Decode(h[:], []byte(s)); err != nil {
		return h, fmt.Errorf("snapformat: bad chunk hash %q", s)
	}
	return h, nil
}

// MarshalText implements encoding.TextMarshaler.
func (h Hash) MarshalText() ([]byte, error) { return []byte(h.String()), nil }

// UnmarshalText implements encoding.TextUnmarshaler.
func (h *Hash) UnmarshalText(b []byte) error {
	v, err := ParseHash(string(b))
	if err != nil {
		return err
	}
	*h = v
	return nil
}

// ChunkKey is the object key of a chunk.
func ChunkKey(h Hash) string { return chunkPrefix + h.String() }

// ParseChunkKey returns the hash a chunk key names.
func ParseChunkKey(key string) (Hash, error) {
	rest, ok := strings.CutPrefix(key, chunkPrefix)
	if !ok {
		return Hash{}, fmt.Errorf("snapformat: %q is not a chunk key", key)
	}
	return ParseHash(rest)
}

// ManifestKey is the object key of a generation's manifest. chainID must be a
// canonical (lowercase, hyphenated) UUID; ManifestKey panics otherwise, as
// callers create chain ids with uuid and check foreign ones with
// ValidChainID first.
func ManifestKey(chainID string, generation uint32) string {
	if !ValidChainID(chainID) {
		panic(fmt.Sprintf("snapformat: ManifestKey with bad chain id %q", chainID))
	}
	return manifestPrefix + chainID + "/" + strconv.FormatUint(uint64(generation), 10) + manifestSuffix
}

// ParseManifestKey returns the chain id and generation a manifest key names.
func ParseManifestKey(key string) (chainID string, generation uint32, err error) {
	rest, ok := strings.CutPrefix(key, manifestPrefix)
	if !ok {
		return "", 0, fmt.Errorf("snapformat: %q is not a manifest key", key)
	}
	chain, file, ok := strings.Cut(rest, "/")
	if !ok || !ValidChainID(chain) {
		return "", 0, fmt.Errorf("snapformat: bad manifest key %q", key)
	}
	num, ok := strings.CutSuffix(file, manifestSuffix)
	if !ok || num == "" || (len(num) > 1 && num[0] == '0') {
		return "", 0, fmt.Errorf("snapformat: bad manifest key %q", key)
	}
	g, err := strconv.ParseUint(num, 10, 32)
	if err != nil {
		return "", 0, fmt.Errorf("snapformat: bad manifest key %q", key)
	}
	return chain, uint32(g), nil
}

// ValidChainID reports whether s is a canonical UUID string.
func ValidChainID(s string) bool {
	u, err := uuid.Parse(s)
	return err == nil && u.String() == s
}

// Entry is one chunk of the disk: chunk Index covers bytes
// [Index*ChunkSize, min((Index+1)*ChunkSize, DiskBytes)).
type Entry struct {
	Index uint64 `json:"i"`
	// Hash of the chunk's uncompressed bytes; zero value when Zero.
	Hash Hash `json:"h,omitzero"`
	// Zero: the chunk reads as zeroes (discarded or written as zeroes) and
	// has no object.
	Zero bool `json:"z,omitzero"`
}

// Manifest describes one generation of a sandbox's disk.
type Manifest struct {
	Version    int    `json:"version"`
	ChainID    string `json:"chainId"`
	Generation uint32 `json:"generation"`
	// SandboxID is the sandbox whose disk this is (informational; forks
	// restore it under another id).
	SandboxID string `json:"sandboxId"`
	// Image is the base image id the disk sits on; ImageSHA256 pins it.
	Image       string `json:"image"`
	ImageSHA256 string `json:"imageSha256,omitempty"`
	// DiskBytes is the virtual disk size.
	DiskBytes uint64 `json:"diskBytes"`
	ChunkSize uint32 `json:"chunkSize"`
	// CreatedAt is the cut (freeze) time.
	CreatedAt time.Time `json:"createdAt"`
	// Chunks sorted by Index, each index at most once.
	Chunks []Entry `json:"chunks"`
}

// NumChunks is the number of chunk positions of the disk.
func (m *Manifest) NumChunks() uint64 { return (m.DiskBytes + ChunkSize - 1) / ChunkSize }

// ChunkLen is the uncompressed length of chunk index i.
func (m *Manifest) ChunkLen(i uint64) uint64 {
	start := i * ChunkSize
	return min(ChunkSize, m.DiskBytes-start)
}

// Validate checks the manifest's invariants.
func (m *Manifest) Validate() error {
	switch {
	case m.Version != Version:
		return fmt.Errorf("snapformat: manifest version %d, want %d", m.Version, Version)
	case !ValidChainID(m.ChainID):
		return fmt.Errorf("snapformat: bad chain id %q", m.ChainID)
	case m.Image == "":
		return errors.New("snapformat: manifest without image")
	case m.DiskBytes == 0 || m.DiskBytes > MaxDiskBytes:
		return fmt.Errorf("snapformat: disk size %d out of range", m.DiskBytes)
	case m.ChunkSize != ChunkSize:
		return fmt.Errorf("snapformat: chunk size %d, want %d", m.ChunkSize, ChunkSize)
	case m.CreatedAt.IsZero():
		return errors.New("snapformat: manifest without createdAt")
	}
	n := m.NumChunks()
	for k, e := range m.Chunks {
		if e.Index >= n {
			return fmt.Errorf("snapformat: chunk index %d past the disk's %d chunks", e.Index, n)
		}
		if k > 0 && e.Index <= m.Chunks[k-1].Index {
			return fmt.Errorf("snapformat: chunk index %d out of order or repeated", e.Index)
		}
		if e.Zero != (e.Hash == Hash{}) {
			return fmt.Errorf("snapformat: chunk %d must have exactly one of hash and zero", e.Index)
		}
	}
	return nil
}

// ChunkKeys yields the distinct chunk keys the manifest references, in
// manifest order (GC marks them).
func (m *Manifest) ChunkKeys() iter.Seq[string] {
	return func(yield func(string) bool) {
		seen := make(map[Hash]struct{}, len(m.Chunks))
		for _, e := range m.Chunks {
			if e.Zero {
				continue
			}
			if _, ok := seen[e.Hash]; ok {
				continue
			}
			seen[e.Hash] = struct{}{}
			if !yield(ChunkKey(e.Hash)) {
				return
			}
		}
	}
}

// ContentBytes is the uncompressed size of the listed non-zero chunks: the
// sandbox's own data restored by this manifest (base image excluded).
func (m *Manifest) ContentBytes() uint64 {
	var n uint64
	for _, e := range m.Chunks {
		if !e.Zero {
			n += m.ChunkLen(e.Index)
		}
	}
	return n
}

// Lookup returns the entry for chunk index i, if listed.
func (m *Manifest) Lookup(i uint64) (Entry, bool) {
	lo, hi := 0, len(m.Chunks)
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if m.Chunks[mid].Index < i {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	if lo < len(m.Chunks) && m.Chunks[lo].Index == i {
		return m.Chunks[lo], true
	}
	return Entry{}, false
}

// The coders are safe for concurrent EncodeAll/DecodeAll. Each decoder caps
// the decoded size, so a hostile frame cannot exhaust memory.
var (
	encoder, _         = zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))
	manifestDecoder, _ = zstd.NewReader(nil, zstd.WithDecoderConcurrency(0), zstd.WithDecoderMaxMemory(MaxManifestBytes))
	chunkDecoder, _    = zstd.NewReader(nil, zstd.WithDecoderConcurrency(0), zstd.WithDecoderMaxMemory(ChunkSize))
)

// EncodeManifest validates m and returns its object bytes.
func EncodeManifest(m *Manifest) ([]byte, error) {
	if err := m.Validate(); err != nil {
		return nil, err
	}
	raw, err := json.Marshal(m)
	if err != nil {
		return nil, fmt.Errorf("snapformat: encode manifest: %w", err)
	}
	return encoder.EncodeAll(raw, nil), nil
}

// DecodeManifest parses and validates manifest object bytes.
func DecodeManifest(b []byte) (*Manifest, error) {
	raw, err := manifestDecoder.DecodeAll(b, nil)
	if err != nil {
		return nil, fmt.Errorf("snapformat: decode manifest: %w", err)
	}
	var m Manifest
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, fmt.Errorf("snapformat: decode manifest: %w", err)
	}
	if err := m.Validate(); err != nil {
		return nil, err
	}
	return &m, nil
}

// HashChunk returns the hash of uncompressed chunk bytes.
func HashChunk(data []byte) Hash { return sha256.Sum256(data) }

// IsZero reports whether data is all zeroes (such chunks go in the manifest
// as Zero entries, never as objects).
func IsZero(data []byte) bool {
	for len(data) >= 8 {
		if data[0]|data[1]|data[2]|data[3]|data[4]|data[5]|data[6]|data[7] != 0 {
			return false
		}
		data = data[8:]
	}
	for _, c := range data {
		if c != 0 {
			return false
		}
	}
	return true
}

// EncodeChunk returns a chunk's hash and its object bytes. data is at most
// ChunkSize bytes.
func EncodeChunk(data []byte) (Hash, []byte, error) {
	if len(data) == 0 || len(data) > ChunkSize {
		return Hash{}, nil, fmt.Errorf("snapformat: chunk of %d bytes", len(data))
	}
	return HashChunk(data), encoder.EncodeAll(data, nil), nil
}

// DecodeChunk decompresses chunk object bytes into dst[:0] and verifies them
// against want. A mismatch is ErrCorruptChunk.
func DecodeChunk(want Hash, obj []byte, dst []byte) ([]byte, error) {
	data, err := chunkDecoder.DecodeAll(obj, dst[:0])
	if err != nil {
		return nil, fmt.Errorf("%w %s: %v", ErrCorruptChunk, want, err)
	}
	if len(data) == 0 || len(data) > ChunkSize || HashChunk(data) != want {
		return nil, fmt.Errorf("%w %s: content does not match its hash", ErrCorruptChunk, want)
	}
	return data, nil
}

// ErrCorruptChunk: a chunk object does not decode to bytes with its hash.
var ErrCorruptChunk = errors.New("snapformat: corrupt chunk")
