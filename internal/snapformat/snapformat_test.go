package snapformat_test

import (
	"bytes"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"

	"github.com/nathabonfim59/bamboo-raft/internal/snapformat"
)

const chain = "0190c3a4-1b2c-7d3e-8f40-123456789abc"

func manifest(entries ...snapformat.Entry) *snapformat.Manifest {
	return &snapformat.Manifest{
		Version:    snapformat.Version,
		ChainID:    chain,
		Generation: 2,
		SandboxID:  "bx_abcdefgh",
		Image:      "ubuntu-24.04-v1",
		DiskBytes:  10 << 30,
		ChunkSize:  snapformat.ChunkSize,
		CreatedAt:  time.Date(2026, 11, 21, 12, 0, 0, 0, time.UTC),
		Chunks:     entries,
	}
}

func TestManifestRoundTrip(t *testing.T) {
	h1 := snapformat.HashChunk([]byte("one"))
	h2 := snapformat.HashChunk([]byte("two"))
	m := manifest(
		snapformat.Entry{Index: 0, Hash: h1},
		snapformat.Entry{Index: 3, Zero: true},
		snapformat.Entry{Index: 7, Hash: h2},
		snapformat.Entry{Index: 9, Hash: h1},
	)
	b, err := snapformat.EncodeManifest(m)
	if err != nil {
		t.Fatal(err)
	}
	got, err := snapformat.DecodeManifest(b)
	if err != nil {
		t.Fatal(err)
	}
	if !got.CreatedAt.Equal(m.CreatedAt) || got.ChainID != chain || !slices.Equal(got.Chunks, m.Chunks) {
		t.Fatalf("round trip differs:\n got %+v\nwant %+v", got, m)
	}
	keys := slices.Collect(got.ChunkKeys())
	if want := []string{snapformat.ChunkKey(h1), snapformat.ChunkKey(h2)}; !slices.Equal(keys, want) {
		t.Fatalf("ChunkKeys = %v, want %v", keys, want)
	}
	if got.ContentBytes() != 3*snapformat.ChunkSize {
		t.Fatalf("ContentBytes = %d", got.ContentBytes())
	}
	if e, ok := got.Lookup(7); !ok || e.Hash != h2 {
		t.Fatalf("Lookup(7) = %+v, %v", e, ok)
	}
	if _, ok := got.Lookup(5); ok {
		t.Fatal("Lookup(5) found an unlisted chunk")
	}
}

func TestManifestValidate(t *testing.T) {
	h := snapformat.HashChunk([]byte("x"))
	cases := map[string]func(m *snapformat.Manifest){
		"version":     func(m *snapformat.Manifest) { m.Version = 2 },
		"chain":       func(m *snapformat.Manifest) { m.ChainID = "not-a-uuid" },
		"chain upper": func(m *snapformat.Manifest) { m.ChainID = strings.ToUpper(chain) },
		"image":       func(m *snapformat.Manifest) { m.Image = "" },
		"disk zero":   func(m *snapformat.Manifest) { m.DiskBytes = 0 },
		"disk huge":   func(m *snapformat.Manifest) { m.DiskBytes = snapformat.MaxDiskBytes + 1 },
		"chunk size":  func(m *snapformat.Manifest) { m.ChunkSize = 1 << 20 },
		"created":     func(m *snapformat.Manifest) { m.CreatedAt = time.Time{} },
		"past end":    func(m *snapformat.Manifest) { m.Chunks = []snapformat.Entry{{Index: 2560, Hash: h}} },
		"order": func(m *snapformat.Manifest) {
			m.Chunks = []snapformat.Entry{{Index: 2, Hash: h}, {Index: 1, Hash: h}}
		},
		"repeat": func(m *snapformat.Manifest) {
			m.Chunks = []snapformat.Entry{{Index: 1, Hash: h}, {Index: 1, Hash: h}}
		},
		"neither":     func(m *snapformat.Manifest) { m.Chunks = []snapformat.Entry{{Index: 1}} },
		"hash + zero": func(m *snapformat.Manifest) { m.Chunks = []snapformat.Entry{{Index: 1, Hash: h, Zero: true}} },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			m := manifest(snapformat.Entry{Index: 0, Hash: h})
			mutate(m)
			if err := m.Validate(); err == nil {
				t.Fatal("Validate accepted it")
			}
			if _, err := snapformat.EncodeManifest(m); err == nil {
				t.Fatal("EncodeManifest accepted it")
			}
		})
	}
}

func TestDecodeManifestRejects(t *testing.T) {
	enc, _ := zstd.NewWriter(nil)
	cases := map[string][]byte{
		"not zstd":      []byte("{}"),
		"bad json":      enc.EncodeAll([]byte("{"), nil),
		"invalid":       enc.EncodeAll([]byte(`{"version":1}`), nil),
		"bad hash":      enc.EncodeAll([]byte(`{"version":1,"chainId":"`+chain+`","image":"i","diskBytes":4194304,"chunkSize":4194304,"createdAt":"2026-11-21T00:00:00Z","chunks":[{"i":0,"h":"zz"}]}`), nil),
		"too large":     enc.EncodeAll(bytes.Repeat([]byte(" "), snapformat.MaxManifestBytes+1), nil),
		"duplicate key": enc.EncodeAll([]byte(`{"version":1,"version":1}`), nil),
	}
	for name, b := range cases {
		if _, err := snapformat.DecodeManifest(b); err == nil {
			t.Errorf("%s: DecodeManifest accepted it", name)
		}
	}
}

func TestLastChunkLen(t *testing.T) {
	m := manifest()
	m.DiskBytes = 3*snapformat.ChunkSize + 100
	if m.NumChunks() != 4 || m.ChunkLen(3) != 100 || m.ChunkLen(0) != snapformat.ChunkSize {
		t.Fatalf("NumChunks %d, ChunkLen(3) %d", m.NumChunks(), m.ChunkLen(3))
	}
}

func TestChunkRoundTrip(t *testing.T) {
	data := bytes.Repeat([]byte("bamboo"), 100000)
	h, obj, err := snapformat.EncodeChunk(data)
	if err != nil {
		t.Fatal(err)
	}
	if len(obj) >= len(data) {
		t.Errorf("compressed %d bytes into %d", len(data), len(obj))
	}
	got, err := snapformat.DecodeChunk(h, obj, nil)
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("DecodeChunk: %v", err)
	}
	other := snapformat.HashChunk([]byte("other"))
	if _, err := snapformat.DecodeChunk(other, obj, nil); !errors.Is(err, snapformat.ErrCorruptChunk) {
		t.Fatalf("wrong hash: %v, want ErrCorruptChunk", err)
	}
	if _, err := snapformat.DecodeChunk(h, obj[:len(obj)/2], nil); !errors.Is(err, snapformat.ErrCorruptChunk) {
		t.Fatalf("truncated: %v, want ErrCorruptChunk", err)
	}
	enc, _ := zstd.NewWriter(nil)
	big := make([]byte, snapformat.ChunkSize+1)
	big[0] = 1
	if _, err := snapformat.DecodeChunk(snapformat.HashChunk(big), enc.EncodeAll(big, nil), nil); err == nil {
		t.Fatal("DecodeChunk accepted a chunk larger than ChunkSize")
	}
	if _, _, err := snapformat.EncodeChunk(nil); err == nil {
		t.Fatal("EncodeChunk accepted an empty chunk")
	}
}

func TestIsZero(t *testing.T) {
	z := make([]byte, 1000)
	if !snapformat.IsZero(z) {
		t.Fatal("zeroes not zero")
	}
	for _, i := range []int{0, 7, 8, 999} {
		z[i] = 1
		if snapformat.IsZero(z) {
			t.Fatalf("byte %d set, still zero", i)
		}
		z[i] = 0
	}
}

func TestKeys(t *testing.T) {
	h := snapformat.HashChunk([]byte("k"))
	got, err := snapformat.ParseChunkKey(snapformat.ChunkKey(h))
	if err != nil || got != h {
		t.Fatalf("ParseChunkKey: %v", err)
	}
	k := snapformat.ManifestKey(chain, 12)
	if k != "manifests/"+chain+"/12.json.zst" {
		t.Fatalf("ManifestKey = %s", k)
	}
	c, g, err := snapformat.ParseManifestKey(k)
	if err != nil || c != chain || g != 12 {
		t.Fatalf("ParseManifestKey = %s %d %v", c, g, err)
	}
	for _, bad := range []string{
		"chunks/" + strings.ToUpper(h.String()), "chunks/abc", "manifests/" + chain + "/012.json.zst",
		"manifests/" + chain + "/x.json.zst", "manifests/" + chain + "/1.json", "manifests/nope/1.json.zst",
		"manifests/" + chain + "/4294967296.json.zst", "other/" + chain + "/1.json.zst",
	} {
		_, errC := snapformat.ParseChunkKey(bad)
		_, _, errM := snapformat.ParseManifestKey(bad)
		if errC == nil || errM == nil {
			t.Errorf("%q parsed: chunk err %v, manifest err %v", bad, errC, errM)
		}
	}
	defer func() {
		if recover() == nil {
			t.Error("ManifestKey with a bad chain id did not panic")
		}
	}()
	snapformat.ManifestKey("bad", 0)
}
