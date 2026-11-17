package node

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestLeadJailSubtreeEnumerationIsComplete(t *testing.T) {
	root := t.TempDir()
	jailKVMFakeCgroup(t, root, false, "")
	const children = 4100
	for i := range children {
		jailKVMFakeCgroup(t, filepath.Join(root, fmt.Sprintf("child-%04d", i)), false, "")
	}
	f, err := os.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	entries, err := f.ReadDir(4096)
	if closeErr := f.Close(); err != nil || closeErr != nil {
		t.Fatalf("enumerate: %v, close: %v", err, closeErr)
	}
	page := map[string]bool{}
	for _, e := range entries {
		page[e.Name()] = true
	}
	hidden := ""
	for i := range children {
		name := fmt.Sprintf("child-%04d", i)
		if !page[name] {
			hidden = name
			break
		}
	}
	if hidden == "" {
		t.Fatal("fixture has no child beyond the first page")
	}
	// Updating files inside this existing child leaves root directory order
	// untouched, so the first page provably omits the populated descendant.
	jailKVMFakeCgroup(t, filepath.Join(root, hidden), true, "123\n")
	if err := jailKVMSubtreeEmpty(t.Context(), root, children+10); err == nil {
		t.Fatal("an incomplete directory page hid a populated descendant")
	}
}
