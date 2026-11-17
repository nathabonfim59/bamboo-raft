package node

import (
	"context"
	"errors"
	"github.com/nathabonfim59/bamboo-raft/internal/node/jailer"
	"testing"
	"time"
)

func TestLeadProbeCancellationDuringFinalRelease(t *testing.T) {
	f := newJailProbeFixture(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	original := f.jail.reclaim
	f.jail.reclaim = func(c context.Context, r jailer.Record) error { err := original(c, r); cancel(); return err }
	ev, err := f.probe(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("probe returned evidence %+v and error %v after cancellation during release; want Canceled", ev, err)
	}
}
func TestLeadProbeUnprovenDeathDoesNotLosePrivateHandle(t *testing.T) {
	f := newJailProbeFixture(t)
	f.opts.Grace = 20 * time.Millisecond
	f.jail.probeErr = errors.New("injected post-spawn proof error")
	f.jail.killNoop = true
	if _, err := f.probe(t.Context()); err == nil {
		t.Fatal("probe succeeded")
	}
	run := f.jail.lastRun()
	if _, detaches, _ := run.counts(); detaches != 1 {
		t.Fatalf("probe error API lost its only private handle owner: Detach count %d, want 1 with durable record retained", detaches)
	}
	if _, ok := f.jail.Record(jailer.ProbeID); !ok {
		t.Fatal("record released")
	}
}
