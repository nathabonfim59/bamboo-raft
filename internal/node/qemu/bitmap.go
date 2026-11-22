package qemu

import (
	"context"
	"fmt"

	"github.com/nathabonfim59/bamboo-raft/internal/node/qmp"
	"github.com/nathabonfim59/bamboo-raft/internal/snapformat"
)

// Phase 2 block graph names. They are fixed: the node never renames a
// bitmap or a node, so a restarted node finds them by name.
const (
	// RootDiskNode is the QMP node name of the VM's writable qcow2 overlay.
	RootDiskNode = rootDiskNode
	// DirtyBitmap is the persistent dirty bitmap on RootDiskNode that
	// records every guest write since the last snapshot cut.
	DirtyBitmap = "bbraft-dirty"
	// CutBitmap is the temporary, disabled, non-persistent bitmap holding
	// the dirty set of one snapshot cut.
	CutBitmap = "bbraft-cut"
	// DirtyBitmapGranularity is DirtyBitmap's granularity in bytes: one
	// snapshot chunk, so a dirty bit names exactly one chunk.
	DirtyBitmapGranularity = snapformat.ChunkSize
)

// BlockDirtyBitmap is one entry of a block node's dirty-bitmaps in
// query-named-block-nodes (QAPI BlockDirtyInfo).
type BlockDirtyBitmap struct {
	Name         string `json:"name"`
	Count        uint64 `json:"count"`
	Granularity  uint64 `json:"granularity"`
	Recording    bool   `json:"recording"`
	Busy         bool   `json:"busy"`
	Persistent   bool   `json:"persistent"`
	Inconsistent bool   `json:"inconsistent"`
}

// NamedBlockNode is the part of one query-named-block-nodes entry (QAPI
// BlockDeviceInfo) Phase 2 reads.
type NamedBlockNode struct {
	NodeName     string             `json:"node-name"`
	DirtyBitmaps []BlockDirtyBitmap `json:"dirty-bitmaps"`
}

// QueryNamedBlockNodes runs query-named-block-nodes with flat=true, which
// lists every named node once without its backing chain nested inside.
func QueryNamedBlockNodes(ctx context.Context, client *qmp.Client) ([]NamedBlockNode, error) {
	if client == nil {
		return nil, fmt.Errorf("query-named-block-nodes: no QMP client")
	}
	var nodes []NamedBlockNode
	if err := client.Execute(ctx, "query-named-block-nodes", map[string]bool{"flat": true}, &nodes); err != nil {
		return nil, fmt.Errorf("query-named-block-nodes: %w", err)
	}
	return nodes, nil
}

// FindBitmap returns the bitmap name on node, if node and bitmap exist.
// nodeFound reports whether the node itself is in nodes.
func FindBitmap(nodes []NamedBlockNode, node, name string) (bm BlockDirtyBitmap, found, nodeFound bool) {
	for _, n := range nodes {
		if n.NodeName != node {
			continue
		}
		for _, b := range n.DirtyBitmaps {
			if b.Name == name {
				return b, true, true
			}
		}
		return BlockDirtyBitmap{}, false, true
	}
	return BlockDirtyBitmap{}, false, false
}

// BitmapAction is what EnsureDirtyBitmap did to DirtyBitmap.
type BitmapAction string

const (
	// BitmapKept: the bitmap was healthy and is unchanged.
	BitmapKept BitmapAction = "kept"
	// BitmapAdded: there was no bitmap; a new empty one was added.
	BitmapAdded BitmapAction = "added"
	// BitmapReplaced: the bitmap was unusable (inconsistent, not
	// persistent or of the wrong granularity) and was removed and re-added.
	BitmapReplaced BitmapAction = "replaced"
	// BitmapEnabled: the bitmap was stored disabled and was enabled.
	BitmapEnabled BitmapAction = "enabled"
)

// BitmapResult reports EnsureDirtyBitmap's outcome. FullScan is true
// whenever the bitmap cannot be trusted to hold every write since the last
// snapshot: the next snapshot must then scan the whole disk.
type BitmapResult struct {
	Action   BitmapAction
	FullScan bool
	// Reason says why the bitmap was changed; empty for BitmapKept.
	Reason string
}

// bitmapPlan is the decision for one observed bitmap state.
type bitmapPlan struct {
	remove, add, enable bool
	result              BitmapResult
}

// planDirtyBitmap decides what to do with DirtyBitmap given the observed
// block nodes. A missing, inconsistent, non-persistent, wrongly sized or
// disabled bitmap always means a full scan next: a healthy bitmap is never
// stored disabled (the maintenance launch re-enables it before quitting,
// and a killed QEMU leaves it inconsistent), so a disabled one may have
// missed writes in an earlier run.
func planDirtyBitmap(nodes []NamedBlockNode) (bitmapPlan, error) {
	bm, found, nodeFound := FindBitmap(nodes, RootDiskNode, DirtyBitmap)
	if !nodeFound {
		return bitmapPlan{}, fmt.Errorf("qemu: block node %q not found", RootDiskNode)
	}
	replace := func(why string) (bitmapPlan, error) {
		return bitmapPlan{remove: true, add: true, result: BitmapResult{Action: BitmapReplaced, FullScan: true, Reason: why}}, nil
	}
	switch {
	case !found:
		return bitmapPlan{add: true, result: BitmapResult{Action: BitmapAdded, FullScan: true, Reason: "bitmap missing"}}, nil
	case bm.Busy:
		// Another operation (an export or a job) uses it; nothing here may
		// change it underneath.
		return bitmapPlan{}, fmt.Errorf("qemu: bitmap %s on %s is busy", DirtyBitmap, RootDiskNode)
	case bm.Inconsistent:
		return replace("bitmap inconsistent (QEMU did not exit cleanly)")
	case !bm.Persistent:
		return replace("bitmap not persistent")
	case bm.Granularity != DirtyBitmapGranularity:
		return replace(fmt.Sprintf("bitmap granularity %d, want %d", bm.Granularity, DirtyBitmapGranularity))
	case !bm.Recording:
		return bitmapPlan{enable: true, result: BitmapResult{Action: BitmapEnabled, FullScan: true, Reason: "bitmap stored disabled"}}, nil
	}
	return bitmapPlan{result: BitmapResult{Action: BitmapKept}}, nil
}

type bitmapRef struct {
	Node string `json:"node"`
	Name string `json:"name"`
}

type bitmapAddArgs struct {
	Node        string `json:"node"`
	Name        string `json:"name"`
	Persistent  bool   `json:"persistent"`
	Granularity uint64 `json:"granularity"`
}

// EnsureDirtyBitmap makes DirtyBitmap on RootDiskNode persistent, consistent,
// of DirtyBitmapGranularity and recording, repairing it when it is not, and
// then proves that state with a second query. Launches run it while QEMU is
// still paused before the first cont, so no guest write predates the
// bitmap; adoption runs it on a running QEMU too. id names the VM in errors.
//
// beforeRepair, when not nil, runs after the decision and before the first
// change, for every result with FullScan: the caller records "full scan
// next" durably there, so a crash during or after the repair can never leave
// a healthy-looking bitmap without the flag. Its error aborts the repair.
func EnsureDirtyBitmap(ctx context.Context, client *qmp.Client, id string, beforeRepair func(BitmapResult) error) (BitmapResult, error) {
	nodes, err := QueryNamedBlockNodes(ctx, client)
	if err != nil {
		return BitmapResult{}, fmt.Errorf("qemu: %s: %w", id, err)
	}
	plan, err := planDirtyBitmap(nodes)
	if err != nil {
		return BitmapResult{}, fmt.Errorf("qemu: %s: %w", id, err)
	}
	if plan.result.FullScan && beforeRepair != nil {
		if err := beforeRepair(plan.result); err != nil {
			return BitmapResult{}, fmt.Errorf("qemu: %s: before repairing %s: %w", id, DirtyBitmap, err)
		}
	}
	ref := bitmapRef{Node: RootDiskNode, Name: DirtyBitmap}
	if plan.remove {
		if err := client.Execute(ctx, "block-dirty-bitmap-remove", ref, nil); err != nil {
			return BitmapResult{}, fmt.Errorf("qemu: %s: remove %s: %w", id, DirtyBitmap, err)
		}
	}
	if plan.add {
		args := bitmapAddArgs{Node: RootDiskNode, Name: DirtyBitmap, Persistent: true, Granularity: DirtyBitmapGranularity}
		if err := client.Execute(ctx, "block-dirty-bitmap-add", args, nil); err != nil {
			return BitmapResult{}, fmt.Errorf("qemu: %s: add %s: %w", id, DirtyBitmap, err)
		}
	}
	if plan.enable {
		if err := client.Execute(ctx, "block-dirty-bitmap-enable", ref, nil); err != nil {
			return BitmapResult{}, fmt.Errorf("qemu: %s: enable %s: %w", id, DirtyBitmap, err)
		}
	}
	if !plan.remove && !plan.add && !plan.enable {
		return plan.result, nil
	}
	nodes, err = QueryNamedBlockNodes(ctx, client)
	if err != nil {
		return BitmapResult{}, fmt.Errorf("qemu: %s: verify %s: %w", id, DirtyBitmap, err)
	}
	bm, found, _ := FindBitmap(nodes, RootDiskNode, DirtyBitmap)
	switch {
	case !found:
		return BitmapResult{}, fmt.Errorf("qemu: %s: %s missing after %s", id, DirtyBitmap, plan.result.Action)
	case bm.Inconsistent || !bm.Persistent || !bm.Recording || bm.Granularity != DirtyBitmapGranularity:
		return BitmapResult{}, fmt.Errorf("qemu: %s: %s after %s is %+v, want persistent, consistent, recording, granularity %d",
			id, DirtyBitmap, plan.result.Action, bm, DirtyBitmapGranularity)
	}
	return plan.result, nil
}
