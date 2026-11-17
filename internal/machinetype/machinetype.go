// Package machinetype holds the sandbox sizes.
package machinetype

import (
	"slices"

	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

// Type is one machine size.
type Type struct {
	Name              string
	VCPUs             uint32
	MemoryMiB         uint64
	DiskGiB           uint64
	BillingMultiplier float64
}

// Default is the size used when a request names none.
const Default = "default"

// All lists the sizes, smallest first.
var All = []Type{
	{Name: "small", VCPUs: 2, MemoryMiB: 4 << 10, DiskGiB: 51, BillingMultiplier: 0.5},
	{Name: "default", VCPUs: 4, MemoryMiB: 8 << 10, DiskGiB: 51, BillingMultiplier: 1},
	{Name: "large", VCPUs: 8, MemoryMiB: 16 << 10, DiskGiB: 111, BillingMultiplier: 2},
	{Name: "xlarge", VCPUs: 16, MemoryMiB: 32 << 10, DiskGiB: 251, BillingMultiplier: 50.0 / 9},
}

// Lookup finds a size by name; "" means Default.
func Lookup(name string) (Type, bool) {
	if name == "" {
		name = Default
	}
	i := slices.IndexFunc(All, func(t Type) bool { return t.Name == name })
	if i < 0 {
		return Type{}, false
	}
	return All[i], true
}

// Resources converts the size for nodev1.CreateVMRequest.
func (t Type) Resources() *nodev1.Resources {
	return &nodev1.Resources{Vcpus: t.VCPUs, MemoryMib: t.MemoryMiB, DiskGib: t.DiskGiB}
}
