package qemu

import (
	"bufio"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/nathabonfim59/bamboo-raft/internal/node/qmp"
)

// bitmapFakeQMP is an in-process QMP peer that models the dirty bitmaps of
// the rootdisk node the way QEMU 8.2 answers them: add refuses an existing
// name, remove and enable refuse a missing one, an added bitmap is
// consistent and recording.
type bitmapFakeQMP struct {
	path string

	mu       sync.Mutex
	bitmaps  []BlockDirtyBitmap
	noRoot   bool
	failCmd  string // this command answers GenericError
	commands []string
	// sticky keeps the bitmap's state unchanged by enable (a QEMU that
	// replies OK without doing it), to prove the post-condition check.
	sticky bool
}

func bitmapStartFake(t *testing.T, bitmaps ...BlockDirtyBitmap) *bitmapFakeQMP {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "bbraft-bitmap-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	f := &bitmapFakeQMP{path: filepath.Join(dir, "qmp.sock"), bitmaps: bitmaps}
	ln, err := net.Listen("unix", f.path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go f.serve(conn)
		}
	}()
	return f
}

func (f *bitmapFakeQMP) dial(t *testing.T) *qmp.Client {
	t.Helper()
	c, err := qmp.Dial(t.Context(), f.path, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

func (f *bitmapFakeQMP) log() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.commands)
}

func (f *bitmapFakeQMP) serve(conn net.Conn) {
	defer conn.Close()
	fmt.Fprintln(conn, `{"QMP":{"version":{"qemu":{"major":8,"minor":2,"micro":2},"package":""},"capabilities":[]}}`)
	sc := bufio.NewScanner(conn)
	for sc.Scan() {
		var req struct {
			Execute   string         `json:"execute"`
			Arguments jsontext.Value `json:"arguments"`
			ID        uint64         `json:"id"`
		}
		if err := json.Unmarshal(sc.Bytes(), &req); err != nil {
			return
		}
		result, qerr := f.handle(req.Execute, req.Arguments)
		var msg map[string]any
		if qerr != "" {
			msg = map[string]any{"error": map[string]string{"class": "GenericError", "desc": qerr}, "id": req.ID}
		} else {
			msg = map[string]any{"return": result, "id": req.ID}
		}
		raw, _ := json.Marshal(msg)
		if _, err := conn.Write(append(raw, '\n')); err != nil {
			return
		}
	}
}

func (f *bitmapFakeQMP) handle(cmd string, args jsontext.Value) (any, string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if cmd != "qmp_capabilities" {
		f.commands = append(f.commands, cmd)
	}
	if cmd == f.failCmd {
		return nil, "injected failure of " + cmd
	}
	var a struct {
		Node        string `json:"node"`
		Name        string `json:"name"`
		Persistent  bool   `json:"persistent"`
		Granularity uint64 `json:"granularity"`
		Flat        bool   `json:"flat"`
	}
	if len(args) > 0 {
		if err := json.Unmarshal(args, &a); err != nil {
			return nil, "bad arguments: " + err.Error()
		}
	}
	idx := slices.IndexFunc(f.bitmaps, func(b BlockDirtyBitmap) bool { return b.Name == a.Name })
	switch cmd {
	case "qmp_capabilities":
		return map[string]any{}, ""
	case "query-named-block-nodes":
		if !a.Flat {
			return nil, "test wants flat=true"
		}
		nodes := []map[string]any{{"node-name": "base", "dirty-bitmaps": []any{}}}
		if !f.noRoot {
			nodes = append(nodes, map[string]any{"node-name": RootDiskNode, "dirty-bitmaps": f.bitmaps})
		}
		return nodes, ""
	case "block-dirty-bitmap-add":
		if a.Node != RootDiskNode {
			return nil, "wrong node " + a.Node
		}
		if idx >= 0 {
			return nil, "Bitmap already exists: " + a.Name
		}
		f.bitmaps = append(f.bitmaps, BlockDirtyBitmap{Name: a.Name, Granularity: a.Granularity, Recording: true, Persistent: a.Persistent})
		return map[string]any{}, ""
	case "block-dirty-bitmap-remove":
		if idx < 0 {
			return nil, "Dirty bitmap '" + a.Name + "' not found"
		}
		f.bitmaps = slices.Delete(f.bitmaps, idx, idx+1)
		return map[string]any{}, ""
	case "block-dirty-bitmap-enable":
		if idx < 0 {
			return nil, "Dirty bitmap '" + a.Name + "' not found"
		}
		if !f.sticky {
			f.bitmaps[idx].Recording = true
		}
		return map[string]any{}, ""
	}
	return nil, "unexpected command " + cmd
}

func healthyBitmap() BlockDirtyBitmap {
	return BlockDirtyBitmap{Name: DirtyBitmap, Count: 79691776, Granularity: DirtyBitmapGranularity, Recording: true, Persistent: true}
}

func TestEnsureDirtyBitmap(t *testing.T) {
	healthy := healthyBitmap()
	inconsistent := BlockDirtyBitmap{Name: DirtyBitmap, Granularity: DirtyBitmapGranularity, Persistent: true, Inconsistent: true}
	notPersistent := healthy
	notPersistent.Persistent = false
	wrongGranularity := healthy
	wrongGranularity.Granularity = 64 << 10
	disabled := healthy
	disabled.Recording = false
	other := BlockDirtyBitmap{Name: "someone-else", Granularity: 65536, Recording: true}

	query := "query-named-block-nodes"
	tests := []struct {
		name     string
		bitmaps  []BlockDirtyBitmap
		action   BitmapAction
		fullScan bool
		commands []string
	}{
		{"healthy is kept", []BlockDirtyBitmap{healthy}, BitmapKept, false,
			[]string{query}},
		{"missing is added", nil, BitmapAdded, true,
			[]string{query, "block-dirty-bitmap-add", query}},
		{"missing next to an unrelated bitmap", []BlockDirtyBitmap{other}, BitmapAdded, true,
			[]string{query, "block-dirty-bitmap-add", query}},
		{"inconsistent is replaced", []BlockDirtyBitmap{inconsistent}, BitmapReplaced, true,
			[]string{query, "block-dirty-bitmap-remove", "block-dirty-bitmap-add", query}},
		{"not persistent is replaced", []BlockDirtyBitmap{notPersistent}, BitmapReplaced, true,
			[]string{query, "block-dirty-bitmap-remove", "block-dirty-bitmap-add", query}},
		{"wrong granularity is replaced", []BlockDirtyBitmap{wrongGranularity}, BitmapReplaced, true,
			[]string{query, "block-dirty-bitmap-remove", "block-dirty-bitmap-add", query}},
		// Never stored disabled on a correct path: writes may have gone
		// untracked, so enabling it still means a full scan.
		{"disabled is enabled and scans fully", []BlockDirtyBitmap{disabled}, BitmapEnabled, true,
			[]string{query, "block-dirty-bitmap-enable", query}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := bitmapStartFake(t, tt.bitmaps...)
			var hooked []BitmapResult
			var commandsAtHook int
			res, err := EnsureDirtyBitmap(t.Context(), f.dial(t), "bx_test", func(r BitmapResult) error {
				hooked = append(hooked, r)
				commandsAtHook = len(f.log())
				return nil
			})
			if err != nil {
				t.Fatalf("EnsureDirtyBitmap: %v", err)
			}
			// The hook runs exactly for full-scan repairs, before any change.
			if tt.fullScan {
				if len(hooked) != 1 || hooked[0] != res || commandsAtHook != 1 {
					t.Fatalf("beforeRepair calls %v after %d commands, want one before any change", hooked, commandsAtHook)
				}
			} else if len(hooked) != 0 {
				t.Fatalf("beforeRepair ran for %+v", res)
			}
			if res.Action != tt.action || res.FullScan != tt.fullScan {
				t.Fatalf("result = %+v, want action %s fullScan %t", res, tt.action, tt.fullScan)
			}
			if (res.Reason == "") != (tt.action == BitmapKept) {
				t.Fatalf("reason %q for action %s", res.Reason, res.Action)
			}
			if got := f.log(); !slices.Equal(got, tt.commands) {
				t.Fatalf("commands = %q, want %q", got, tt.commands)
			}
			bm, found, _ := FindBitmap([]NamedBlockNode{{NodeName: RootDiskNode, DirtyBitmaps: f.bitmaps}}, RootDiskNode, DirtyBitmap)
			if !found || !bm.Persistent || !bm.Recording || bm.Inconsistent || bm.Granularity != DirtyBitmapGranularity {
				t.Fatalf("bitmap after ensure = %+v (found %t)", bm, found)
			}
			if tt.action == BitmapKept && bm.Count != healthy.Count {
				t.Fatalf("a kept bitmap lost its count: %+v", bm)
			}
		})
	}
}

func TestEnsureDirtyBitmapFailures(t *testing.T) {
	disabled := healthyBitmap()
	disabled.Recording = false
	busy := healthyBitmap()
	busy.Busy = true
	inconsistent := BlockDirtyBitmap{Name: DirtyBitmap, Granularity: DirtyBitmapGranularity, Persistent: true, Inconsistent: true}
	tests := []struct {
		name    string
		bitmaps []BlockDirtyBitmap
		setup   func(f *bitmapFakeQMP)
		want    string
	}{
		{"no rootdisk node", nil, func(f *bitmapFakeQMP) { f.noRoot = true }, `block node "rootdisk" not found`},
		{"query fails", nil, func(f *bitmapFakeQMP) { f.failCmd = "query-named-block-nodes" }, "query-named-block-nodes"},
		{"busy bitmap", []BlockDirtyBitmap{busy}, nil, "busy"},
		{"add fails", nil, func(f *bitmapFakeQMP) { f.failCmd = "block-dirty-bitmap-add" }, "add bbraft-dirty"},
		{"remove fails", []BlockDirtyBitmap{inconsistent}, func(f *bitmapFakeQMP) { f.failCmd = "block-dirty-bitmap-remove" }, "remove bbraft-dirty"},
		{"enable fails", []BlockDirtyBitmap{disabled}, func(f *bitmapFakeQMP) { f.failCmd = "block-dirty-bitmap-enable" }, "enable bbraft-dirty"},
		{"enable has no effect", []BlockDirtyBitmap{disabled}, func(f *bitmapFakeQMP) { f.sticky = true }, "want persistent, consistent, recording"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := bitmapStartFake(t, tt.bitmaps...)
			if tt.setup != nil {
				tt.setup(f)
			}
			_, err := EnsureDirtyBitmap(t.Context(), f.dial(t), "bx_test", nil)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("EnsureDirtyBitmap = %v, want an error containing %q", err, tt.want)
			}
			if !strings.Contains(err.Error(), "bx_test") {
				t.Fatalf("error %q does not name the VM", err)
			}
		})
	}
	if _, err := EnsureDirtyBitmap(t.Context(), nil, "bx_test", nil); err == nil {
		t.Fatal("EnsureDirtyBitmap without a client succeeded")
	}
	// A failing beforeRepair aborts before any change.
	f := bitmapStartFake(t)
	_, err := EnsureDirtyBitmap(t.Context(), f.dial(t), "bx_test", func(BitmapResult) error { return errors.New("disk full") })
	if err == nil || !strings.Contains(err.Error(), "disk full") {
		t.Fatalf("EnsureDirtyBitmap with a failing hook = %v", err)
	}
	if got := f.log(); !slices.Equal(got, []string{"query-named-block-nodes"}) {
		t.Fatalf("commands after a failing hook = %q, want only the query", got)
	}
}
