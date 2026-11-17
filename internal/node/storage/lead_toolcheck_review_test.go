package storage

import (
	"context"
	"errors"
	"maps"
	"testing"
)

func TestLeadToolSelfCheckPreCanceledNoMutation(t *testing.T) {
	e := newTCEnv(t)
	before := tcTree(t, e.dataDir)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err := e.store.ToolSelfCheck(ctx, e.img)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error=%v want Canceled", err)
	}
	if calls := e.imager.callsCopy(); len(calls) != 0 {
		t.Errorf("pre-canceled self-check called imager: %+v", calls)
	}
	if after := tcTree(t, e.dataDir); !maps.Equal(before, after) {
		t.Errorf("pre-canceled self-check changed data root: before=%v after=%v", before, after)
	}
}

func TestLeadToolSelfCheckCancelBeforeStepNoTool(t *testing.T) {
	e := newTCEnv(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	witnessed := false
	e.store.tc.fault = func(point string) error {
		if point == "step:"+toolCheckStepCreate {
			witnessed = true
			cancel()
		}
		return nil
	}
	_, err := e.store.ToolSelfCheck(ctx, e.img)
	if !witnessed || !errors.Is(err, context.Canceled) {
		t.Fatalf("witnessed=%v error=%v want Canceled", witnessed, err)
	}
	if calls := e.imager.callsCopy(); len(calls) != 0 {
		t.Errorf("canceled before first tool, but imager called: %+v", calls)
	}
}
