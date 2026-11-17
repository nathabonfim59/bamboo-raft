// image.go verifies guest image directories before any VM can boot from
// them (docs/guest-image.md, schema version 1). The node repeats the same
// checks images/verify.sh performs at build time: a directory is an image
// only when its entry set is exact, its manifest parses, its id matches the
// directory name, every sha256 and size matches, and root.qcow2 is a
// standalone qcow2 of the manifest's virtual size.
//
// Hashing and qemu-img calls honour the caller's context, and every read goes
// through an os.Root with O_NOFOLLOW: a symlinked manifest or artifact can
// never redirect verification outside the image directory.
package storage

import (
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"
)

// artifactNames are the only files an image directory may declare. They are
// fixed filenames, never host paths chosen by a request.
var artifactNames = []string{"initrd", "kernel", "packages.json", "root.qcow2"}

// imageFiles is the exact entry set of an image directory.
var imageFiles = []string{"initrd", "kernel", "manifest.json", "packages.json", "root.qcow2"}

// ValidImageID reports whether s is a safe image version id: one component
// of [a-z0-9][a-z0-9._-]{0,63} (the same rule images/build.sh enforces).
func ValidImageID(s string) bool {
	if s == "" || len(s) > 64 {
		return false
	}
	for i := range len(s) {
		c := s[i]
		switch {
		case c >= 'a' && c <= 'z', c >= '0' && c <= '9':
			continue
		case i > 0 && (c == '.' || c == '_' || c == '-'):
			continue
		}
		return false
	}
	return true
}

// imageManifest is schema version 1 of docs/guest-image.md. Only the fields
// the node acts on are decoded; the rest is informational.
type imageManifest struct {
	SchemaVersion int    `json:"schemaVersion"`
	ID            string `json:"id"`
	Root          struct {
		Filesystem       string `json:"filesystem"`
		Format           string `json:"format"`
		DiskGiB          uint64 `json:"diskGiB"`
		VirtualSizeBytes uint64 `json:"virtualSizeBytes"`
	} `json:"root"`
	Artifacts map[string]imageArtifact `json:"artifacts"`
}

type imageArtifact struct {
	SHA256 string `json:"sha256"`
	Size   int64  `json:"size"`
}

// Image is a verified image the node may boot VMs from.
type Image struct {
	// ID is the image version id, equal to the directory name.
	ID string
	// Dir is the absolute image directory (kernel, initrd, root.qcow2).
	Dir string
	// RootVirtualSize is root.qcow2's base virtual size in bytes; overlays
	// start at this size and grow to the requested disk.
	RootVirtualSize uint64
	// RootVirtualSizeGiB is RootVirtualSize rounded up to whole GiB.
	RootVirtualSizeGiB uint64
	// RootSHA256 is the verified hash of root.qcow2 from the manifest. It is
	// what Info reports for the image.
	RootSHA256 string
}

// ImagePath returns the absolute directory of one image id, without checking
// that it exists.
func (s *Store) ImagePath(id string) string {
	return filepath.Join(s.imageDir, id)
}

// VerifyImages scans the image root and returns every directory that passes
// the full verification, sorted by id. A directory that fails is skipped
// with a log line and never used: a broken image must fail the boot that
// names it, not the node. Directories without a manifest are not images and
// are ignored.
func (s *Store) VerifyImages(ctx context.Context) ([]Image, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	root, err := os.OpenRoot(s.imageDir)
	if err != nil {
		return nil, fmt.Errorf("storage: open image root: %w", err)
	}
	defer root.Close()
	dir, err := root.Open(".")
	if err != nil {
		return nil, fmt.Errorf("storage: list images: %w", err)
	}
	entries, err := dir.ReadDir(-1)
	closeErr := dir.Close()
	if err != nil {
		return nil, fmt.Errorf("storage: list images: %w", err)
	}
	if closeErr != nil {
		return nil, fmt.Errorf("storage: list images: %w", closeErr)
	}
	var images []Image
	for _, e := range entries {
		if e.Type()&fs.ModeSymlink != 0 || !e.IsDir() || !ValidImageID(e.Name()) {
			continue
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		img, err := s.verifyImage(ctx, root, e.Name())
		if err != nil {
			if !errors.Is(err, fs.ErrNotExist) {
				s.log.Warn("image failed verification; skipping", "image", e.Name(), "error", err)
			}
			continue
		}
		images = append(images, img)
	}
	slices.SortFunc(images, func(a, b Image) int { return cmp.Compare(a.ID, b.ID) })
	return images, nil
}

// verifyImage performs the full check of one image directory.
func (s *Store) verifyImage(ctx context.Context, root *os.Root, id string) (Image, error) {
	info, err := root.Lstat(id)
	if err != nil {
		return Image{}, err
	}
	if info.Mode()&fs.ModeSymlink != 0 || !info.IsDir() {
		return Image{}, fmt.Errorf("storage: image %s is not a plain directory", id)
	}
	if err := ctx.Err(); err != nil {
		return Image{}, err
	}
	raw, err := readArtifact(ctx, root, filepath.Join(id, "manifest.json"), "manifest")
	if err != nil {
		return Image{}, err
	}
	var m imageManifest
	if err := json.Unmarshal(raw, &m); err != nil {
		return Image{}, fmt.Errorf("storage: image %s: manifest: %w", id, err)
	}
	if m.SchemaVersion != 1 {
		return Image{}, fmt.Errorf("storage: image %s: schemaVersion %d, want 1", id, m.SchemaVersion)
	}
	if m.ID != id {
		return Image{}, fmt.Errorf("storage: image dir %s holds manifest id %q", id, m.ID)
	}
	if err := checkImageEntries(root, id); err != nil {
		return Image{}, err
	}
	if len(m.Artifacts) != len(artifactNames) {
		return Image{}, fmt.Errorf("storage: image %s declares %d artifacts, want exactly %v", id, len(m.Artifacts), artifactNames)
	}
	for _, name := range artifactNames {
		a, ok := m.Artifacts[name]
		if !ok {
			return Image{}, fmt.Errorf("storage: image %s misses artifact %q", id, name)
		}
		if !validSHA256(a.SHA256) {
			return Image{}, fmt.Errorf("storage: image %s: artifact %s has no sha256", id, name)
		}
		if a.Size < 0 {
			return Image{}, fmt.Errorf("storage: image %s: artifact %s has negative size", id, name)
		}
		if err := verifyArtifact(ctx, root, id, name, a); err != nil {
			return Image{}, err
		}
	}
	if m.Root.Format != "qcow2" {
		return Image{}, fmt.Errorf("storage: image %s: manifest root format %q, want qcow2", id, m.Root.Format)
	}
	if m.Root.Filesystem != "ext4" {
		return Image{}, fmt.Errorf("storage: image %s: manifest root filesystem %q, want ext4", id, m.Root.Filesystem)
	}
	diskBytes, ok := checkedDiskBytes(m.Root.DiskGiB)
	if !ok || diskBytes == 0 {
		return Image{}, fmt.Errorf("storage: image %s: manifest diskGiB %d is invalid", id, m.Root.DiskGiB)
	}
	if diskBytes != m.Root.VirtualSizeBytes {
		return Image{}, fmt.Errorf("storage: image %s: diskGiB %d and virtualSizeBytes %d disagree", id, m.Root.DiskGiB, m.Root.VirtualSizeBytes)
	}

	baseFile, err := pinNoFollow(root, path.Join(id, "root.qcow2"), os.O_RDONLY)
	if err != nil {
		return Image{}, fmt.Errorf("storage: image %s: pin root.qcow2: %w", id, err)
	}
	defer baseFile.Close()
	// A base is standalone: the expected backing is empty and it is read
	// shared-ro from the images root.
	qimg := ImageRef{File: baseFile, Root: ImageRootImages, Path: path.Join(id, "root.qcow2"), Format: "qcow2"}
	qinfo, err := s.imager.Info(ctx, qimg)
	if err != nil {
		return Image{}, fmt.Errorf("storage: image %s: %w", id, err)
	}
	if qinfo.Format != "qcow2" {
		return Image{}, fmt.Errorf("storage: image %s: root.qcow2 is %s, want qcow2", id, qinfo.Format)
	}
	if qinfo.BackingFile != "" {
		return Image{}, fmt.Errorf("storage: image %s: root.qcow2 has backing file %q", id, qinfo.BackingFile)
	}
	if qinfo.VirtualSize != m.Root.VirtualSizeBytes {
		return Image{}, fmt.Errorf("storage: image %s: root.qcow2 virtual size %d, manifest says %d", id, qinfo.VirtualSize, m.Root.VirtualSizeBytes)
	}
	if err := s.imager.Check(ctx, qimg); err != nil {
		return Image{}, fmt.Errorf("storage: image %s: %w", id, err)
	}
	return Image{
		ID:                 id,
		Dir:                s.ImagePath(id),
		RootVirtualSize:    qinfo.VirtualSize,
		RootVirtualSizeGiB: ceilGiB(qinfo.VirtualSize),
		RootSHA256:         strings.ToLower(m.Artifacts["root.qcow2"].SHA256),
	}, nil
}

// checkImageEntries rejects extra files and directories: the manifest
// declares a fixed artifact set and the directory must match it exactly.
func checkImageEntries(root *os.Root, id string) error {
	dir, err := root.Open(id)
	if err != nil {
		return fmt.Errorf("storage: image %s: %w", id, err)
	}
	entries, err := dir.ReadDir(-1)
	closeErr := dir.Close()
	if err != nil {
		return fmt.Errorf("storage: image %s: list: %w", id, err)
	}
	if closeErr != nil {
		return fmt.Errorf("storage: image %s: list: %w", id, closeErr)
	}
	for _, e := range entries {
		if e.Type()&fs.ModeSymlink != 0 {
			return fmt.Errorf("storage: image %s: entry %q is a symlink", id, e.Name())
		}
		if e.IsDir() || !slices.Contains(imageFiles, e.Name()) {
			return fmt.Errorf("storage: image %s: unexpected entry %q", id, e.Name())
		}
	}
	for _, name := range imageFiles {
		found := slices.ContainsFunc(entries, func(e fs.DirEntry) bool { return e.Name() == name })
		if !found {
			return fmt.Errorf("storage: image %s misses %q", id, name)
		}
	}
	return nil
}

func verifyArtifact(ctx context.Context, root *os.Root, dir, name string, a imageArtifact) error {
	p := filepath.Join(dir, name)
	li, err := root.Lstat(p)
	if err != nil {
		return fmt.Errorf("storage: image %s: artifact %s: %w", dir, name, err)
	}
	if li.Mode()&fs.ModeSymlink != 0 || !li.Mode().IsRegular() {
		return fmt.Errorf("storage: image %s: artifact %s is not a regular file", dir, name)
	}
	if li.Size() != a.Size {
		return fmt.Errorf("storage: image %s: artifact %s size %d, manifest says %d", dir, name, li.Size(), a.Size)
	}
	sum, err := hashFile(ctx, root, p)
	if err != nil {
		return fmt.Errorf("storage: image %s: artifact %s: %w", dir, name, err)
	}
	if sum != strings.ToLower(a.SHA256) {
		return fmt.Errorf("storage: image %s: artifact %s sha256 mismatch", dir, name)
	}
	return nil
}

// readArtifact reads one image file through the root, refusing symlinks.
func readArtifact(ctx context.Context, root *os.Root, name, kind string) ([]byte, error) {
	f, err := openNoFollow(root, name)
	if err != nil {
		return nil, fmt.Errorf("storage: %s %s: %w", kind, name, err)
	}
	defer f.Close()
	raw, err := io.ReadAll(ctxReader{ctx: ctx, r: f})
	if err != nil {
		return nil, fmt.Errorf("storage: read %s %s: %w", kind, name, err)
	}
	return raw, nil
}

func openNoFollow(root *os.Root, name string) (*os.File, error) {
	return openChecked(root, name, os.O_RDONLY, false)
}

func hashFile(ctx context.Context, root *os.Root, name string) (string, error) {
	f, err := openNoFollow(root, name)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	buf := make([]byte, 1<<20)
	for {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		n, err := f.Read(buf)
		if n > 0 {
			h.Write(buf[:n])
		}
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return "", err
		}
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// ctxReader aborts a buffered read when the context ends between reads.
type ctxReader struct {
	ctx context.Context
	r   io.Reader
}

func (c ctxReader) Read(p []byte) (int, error) {
	if err := c.ctx.Err(); err != nil {
		return 0, err
	}
	return c.r.Read(p)
}

func validSHA256(s string) bool {
	if len(s) != sha256.Size*2 {
		return false
	}
	for i := range len(s) {
		c := s[i]
		if c >= '0' && c <= '9' || c >= 'a' && c <= 'f' || c >= 'A' && c <= 'F' {
			continue
		}
		return false
	}
	return true
}

// checkedDiskBytes converts whole GiB to bytes without overflow.
func checkedDiskBytes(gib uint64) (uint64, bool) {
	if gib > math.MaxUint64>>30 {
		return 0, false
	}
	return gib << 30, true
}

// ceilGiB rounds up without overflowing near MaxUint64.
func ceilGiB(bytes uint64) uint64 {
	const gib = uint64(1) << 30
	out := bytes >> 30
	if bytes&(gib-1) != 0 {
		out++
	}
	return out
}
