// Package images_test proves that a test-only unpinned image manifest is
// accepted by the real node image loader. The node's storage.imageManifest
// decodes only schemaVersion, id, root and artifacts; the informational
// distribution.snapshot=unpinned and distribution.mirror fields must neither
// be required nor rejected. The image is a genuine qcow2 with real artifact
// hashes, and the test asserts the image actually appears in the scan.
package images_test

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json/v2"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/nathabonfim59/bamboo-raft/internal/node/storage"
)

const (
	unpinnedImageID     = "ubuntu-24.04-v1-unpinned-test"
	unpinnedSnapshot    = "unpinned"
	unpinnedMirror      = "http://archive.ubuntu.com/ubuntu"
	unpinnedVirtualSize = 1 << 30
)

type testManifest struct {
	SchemaVersion int    `json:"schemaVersion"`
	ID            string `json:"id"`
	Distribution  struct {
		Snapshot string `json:"snapshot"`
		Mirror   string `json:"mirror"`
	} `json:"distribution"`
	Root struct {
		Filesystem       string `json:"filesystem"`
		Format           string `json:"format"`
		DiskGiB          uint64 `json:"diskGiB"`
		VirtualSizeBytes uint64 `json:"virtualSizeBytes"`
	} `json:"root"`
	Artifacts map[string]testArtifact `json:"artifacts"`
}

type testArtifact struct {
	SHA256 string `json:"sha256"`
	Size   int64  `json:"size"`
}

// TestUnpinnedManifestNodeLoaderCompatibility builds a schema-version-1 image
// directory with the two reserved test markers and hands it to
// storage.VerifyImages, the production path the node uses to install images.
func TestUnpinnedManifestNodeLoaderCompatibility(t *testing.T) {
	qemuImg, err := exec.LookPath("qemu-img")
	if err != nil {
		if os.Getenv("BBRAFT_TEST_REQUIRE_ALL") != "" {
			t.Fatalf("qemu-img is required by this node-loader gate when BBRAFT_TEST_REQUIRE_ALL is set: %v", err)
		}
		t.Skip("qemu-img is not installed; set BBRAFT_TEST_REQUIRE_ALL=1 to require it")
	}

	root := t.TempDir()
	imageDir := filepath.Join(root, "images")
	imagePath := filepath.Join(imageDir, unpinnedImageID)
	if err := os.MkdirAll(imagePath, 0o755); err != nil {
		t.Fatal(err)
	}

	for name, data := range map[string][]byte{
		"kernel":        []byte("kernel\n"),
		"initrd":        []byte("initrd\n"),
		"packages.json": []byte("{}\n"),
	} {
		if err := os.WriteFile(filepath.Join(imagePath, name), data, 0o644); err != nil {
			t.Fatal(err)
		}
	}

	rootPath := filepath.Join(imagePath, "root.qcow2")
	create := exec.CommandContext(t.Context(), qemuImg, "create", "-q", "-f", "qcow2", rootPath, "1G")
	if out, err := create.CombinedOutput(); err != nil {
		t.Fatalf("qemu-img create: %v: %s", err, out)
	}

	m := testManifest{
		SchemaVersion: 1,
		ID:            unpinnedImageID,
		Artifacts:     make(map[string]testArtifact),
	}
	m.Distribution.Snapshot = unpinnedSnapshot
	m.Distribution.Mirror = unpinnedMirror
	m.Root.Filesystem = "ext4"
	m.Root.Format = "qcow2"
	m.Root.DiskGiB = 1
	m.Root.VirtualSizeBytes = unpinnedVirtualSize

	for _, name := range []string{"kernel", "initrd", "root.qcow2", "packages.json"} {
		path := filepath.Join(imagePath, name)
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256(data)
		m.Artifacts[name] = testArtifact{SHA256: hex.EncodeToString(sum[:]), Size: int64(len(data))}
	}

	raw, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(imagePath, "manifest.json"), raw, 0o644); err != nil {
		t.Fatal(err)
	}

	store, err := storage.Open(storage.Config{
		DataDir:  filepath.Join(root, "data"),
		ImageDir: imageDir,
	})
	if err != nil {
		t.Fatalf("storage.Open: %v", err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Errorf("storage.Close: %v", err)
		}
	})

	images, err := store.VerifyImages(t.Context())
	if err != nil {
		t.Fatalf("VerifyImages: %v", err)
	}
	if len(images) != 1 {
		t.Fatalf("VerifyImages returned %d images, want the test image to be installed", len(images))
	}
	if images[0].ID != unpinnedImageID {
		t.Fatalf("image id = %q, want %q", images[0].ID, unpinnedImageID)
	}
	if images[0].RootVirtualSize != unpinnedVirtualSize {
		t.Fatalf("root virtual size = %d, want %d", images[0].RootVirtualSize, unpinnedVirtualSize)
	}
	if images[0].Dir != imagePath {
		t.Fatalf("image dir = %q, want %q", images[0].Dir, imagePath)
	}
}
