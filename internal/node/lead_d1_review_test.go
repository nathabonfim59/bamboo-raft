package node

import (
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
	"testing"
)

func TestLeadStopVMKeepsRetainedToolReservation(t *testing.T) {
	fj := newFakeJail(t)
	j := newJailedHarness(t, jailedOptions{jail: fj, fallback: pingOnlyGuest{}, stubGuest: true, configure: func(cfg *Config) { cfg.Imager = retainingImager{fj: fj, t: t} }})
	id := "bx_88888888"
	if _, err := j.mg.CreateVM(t.Context(), &nodev1.CreateVMRequest{SandboxId: id, Resources: &nodev1.Resources{Vcpus: 1, MemoryMib: 1024, DiskGib: defaultTestBaseGiB + 4}}); err == nil {
		t.Fatal("CreateVM succeeded despite retained grow")
	}
	before := j.reserved()
	if out, err := j.mg.StopVM(t.Context(), &nodev1.StopVMRequest{SandboxId: id, GraceSeconds: 1}); err == nil {
		t.Errorf("StopVM published %v while a tool still held the disk", out.GetState())
	}
	if after := j.reserved(); after != before {
		t.Errorf("StopVM freed retained-tool capacity: before %+v after %+v", before, after)
	}
}
