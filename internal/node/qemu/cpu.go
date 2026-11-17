package qemu

import (
	"cmp"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"
)

// NestedMode is the -node.cpu-nested policy: whether the fixed CPU model
// exposes the host's nested-virtualization feature to the guest.
type NestedMode string

const (
	// NestedAuto adds +vmx only on a GenuineIntel host where every CPU
	// reports vmx. Everything else (AMD, unknown, missing vmx) adds nothing.
	NestedAuto NestedMode = "auto"
	// NestedOn requires +vmx or fails with a clear error.
	NestedOn NestedMode = "on"
	// NestedOff never adds a nested-virtualization feature.
	NestedOff NestedMode = "off"
)

// ParseNestedMode validates a mode string. Empty means NestedAuto.
func ParseNestedMode(s string) (NestedMode, error) {
	switch NestedMode(s) {
	case "", NestedAuto:
		return NestedAuto, nil
	case NestedOn:
		return NestedOn, nil
	case NestedOff:
		return NestedOff, nil
	default:
		return "", fmt.Errorf("qemu: invalid cpu-nested mode %q (want auto, on or off)", s)
	}
}

// String returns the mode name.
func (m NestedMode) String() string { return string(m) }

// HostCPU is the host facts CPU resolution needs. It is deliberately small:
// the policy only distinguishes Intel/AMD/unknown and whether every offered
// CPU advertises vmx/svm.
type HostCPU struct {
	// Vendor is "GenuineIntel", "AuthenticAMD", or "" when the vendor is
	// unknown, empty or mixed across CPUs.
	Vendor string
	// Vmx and Svm are true only when every processor block reports the flag.
	Vmx bool
	Svm bool
	// CPUs is the number of processor blocks that were inspected.
	CPUs int
}

// DetectHostCPU reads host CPU facts from path ("" means /proc/cpuinfo).
// It is conservative for a node: a feature counts only when every offered CPU
// reports it, and a mixed vendor is reported as unknown. The result is used by
// the CPU policy, never passed to QEMU as "-cpu host"/"-cpu max".
func DetectHostCPU(path string) (HostCPU, error) {
	if path == "" {
		path = "/proc/cpuinfo"
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return HostCPU{}, fmt.Errorf("qemu: read host CPU info: %w", err)
	}
	return parseHostCPU(data)
}

// parseHostCPU is DetectHostCPU's pure parser, split out for tests.
func parseHostCPU(data []byte) (HostCPU, error) {
	var (
		host        HostCPU
		vendor      string
		vendorKnown bool
		mixedVendor bool
		vmxEvery    = true
		svmEvery    = true
	)
	for block := range strings.SplitSeq(string(data), "\n\n") {
		var (
			isCPU   bool
			bVendor string
			flags   []string
		)
		for line := range strings.SplitSeq(block, "\n") {
			key, value, ok := strings.Cut(line, ":")
			if !ok {
				continue
			}
			key = strings.TrimSpace(key)
			value = strings.TrimSpace(value)
			switch key {
			case "processor":
				isCPU = true
			case "vendor_id":
				bVendor = value
			case "flags", "Features":
				flags = strings.Fields(value)
			}
		}
		if !isCPU {
			continue
		}
		host.CPUs++
		switch {
		case host.CPUs == 1:
			vendor = bVendor
			vendorKnown = bVendor != ""
		case bVendor != vendor:
			mixedVendor = true
		}
		if !slices.Contains(flags, "vmx") {
			vmxEvery = false
		}
		if !slices.Contains(flags, "svm") {
			svmEvery = false
		}
	}
	if host.CPUs == 0 {
		return HostCPU{}, errors.New("qemu: host CPU info has no processor blocks")
	}
	if !vendorKnown || mixedVendor {
		vendor = ""
	}
	host.Vendor = vendor
	host.Vmx = vmxEvery
	host.Svm = svmEvery
	return host, nil
}

// CPUOption is the resolved -cpu value: a fixed model, an optional nested
// feature, the operator's explicit feature toggles and always enforce.
type CPUOption struct {
	Model  string
	Nested string // "vmx", or "" when nested virtualization is not exposed
	// Features are validated "+name"/"-name" toggles (ParseCPUFeatures),
	// rendered after the nested feature and before enforce.
	Features []string
}

// String renders the -cpu argument. enforce is always present so a host that
// cannot provide the fixed model, the requested nesting or a "+" feature
// fails to boot instead of silently filtering features.
func (o CPUOption) String() string {
	parts := []string{o.Model}
	if o.Nested != "" {
		parts = append(parts, "+"+o.Nested)
	}
	parts = append(parts, o.Features...)
	return strings.Join(append(parts, "enforce"), ",")
}

// maxCPUFeatures bounds -node.cpu-features; a real host needs a handful.
const maxCPUFeatures = 32

// ParseCPUFeatures parses the -node.cpu-features list: comma-separated QEMU
// CPU feature toggles, each "+name" or "-name" with a lowercase QEMU feature
// name ("-vmx-apicv-vid"). Empty means none. It exists for hosts that cannot
// provide every feature of the fixed model, for example a CPU without APICv
// under nested VMX; enforce still proves the result on the host. Names that
// would bypass the policy are refused: vmx and svm belong to -node.cpu-nested,
// and "enforce"/"check" or key=value properties are not toggles.
func ParseCPUFeatures(s string) ([]string, error) {
	if strings.TrimSpace(s) == "" {
		return nil, nil
	}
	var out []string
	seen := map[string]bool{}
	for item := range strings.SplitSeq(s, ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			return nil, fmt.Errorf("qemu: cpu-features %q has an empty entry", s)
		}
		name := item[1:]
		if (item[0] != '+' && item[0] != '-') || !validFeatureName(name) {
			return nil, fmt.Errorf("qemu: cpu feature %q is not +name or -name with a lowercase QEMU feature name", item)
		}
		if name == "vmx" || name == "svm" {
			return nil, fmt.Errorf("qemu: cpu feature %q is set by -node.cpu-nested, not -node.cpu-features", item)
		}
		if seen[name] {
			return nil, fmt.Errorf("qemu: cpu feature %q is listed twice", name)
		}
		seen[name] = true
		out = append(out, item)
	}
	if len(out) > maxCPUFeatures {
		return nil, fmt.Errorf("qemu: cpu-features lists %d features; at most %d", len(out), maxCPUFeatures)
	}
	return out, nil
}

// validFeatureName accepts QEMU feature names: lowercase letters, digits,
// '-', '_' and '.', starting with a letter or digit, at most 64 bytes.
func validFeatureName(s string) bool {
	if s == "" || len(s) > 64 {
		return false
	}
	for i, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
		case i > 0 && (r == '-' || r == '_' || r == '.'):
		default:
			return false
		}
	}
	return true
}

// ResolveCPU is the only production CPU policy. It combines the configured
// fixed model, the -node.cpu-nested mode and the injected host facts. It never
// returns "host" or "max" and never implies TCG.
//
// The default model is DefaultCPUModel (Haswell-noTSX). An explicit model is
// honoured unchanged, apart from validation. Empty mode means NestedAuto.
func ResolveCPU(model string, mode NestedMode, host HostCPU) (CPUOption, error) {
	return resolveCPU(model, mode, host, false)
}

// ResolveTestCPU is the explicit test-only variant used by the KVM test
// harness and image probe. It accepts exactly the raw "host" model (never
// "max") and otherwise shares ResolveCPU's nested policy, so on Intel auto the
// result can be "host,+vmx,enforce" and on AMD auto "host,enforce". enforce is
// still always appended. Production callers use ResolveCPU.
func ResolveTestCPU(model string, mode NestedMode, host HostCPU) (CPUOption, error) {
	if model != "host" {
		return CPUOption{}, fmt.Errorf("qemu: test CPU model %q is not allowed; only \"host\"", model)
	}
	return resolveCPU(model, mode, host, true)
}

func resolveCPU(model string, mode NestedMode, host HostCPU, testHost bool) (CPUOption, error) {
	if !testHost {
		model = cmp.Or(model, DefaultCPUModel)
		if !validCPUModel(model) || isHostDependentCPU(model) {
			return CPUOption{}, fmt.Errorf("qemu: invalid fixed CPU model %q", model)
		}
	}
	nested, err := ParseNestedMode(string(mode))
	if err != nil {
		return CPUOption{}, err
	}
	option := CPUOption{Model: model}
	switch nested {
	case NestedOff:
		return option, nil
	case NestedAuto:
		// Phase 0.5 exposes only Intel VMX. AMD hosts get no +svm: the
		// fixed model keeps an Intel CPUID vendor, which the in-guest
		// kvm_amd refuses (Linux __kvm_is_svm_supported).
		if host.Vendor == "GenuineIntel" && host.Vmx {
			option.Nested = "vmx"
		}
		return option, nil
	case NestedOn:
		if host.Vendor == "GenuineIntel" && host.Vmx {
			option.Nested = "vmx"
			return option, nil
		}
		switch host.Vendor {
		case "AuthenticAMD":
			if testHost {
				return CPUOption{}, errors.New("qemu: cpu-nested=on is not supported on an AuthenticAMD host with the test host CPU model (no +svm is appended in phase 0.5)")
			}
			return CPUOption{}, errors.New("qemu: cpu-nested=on is not supported on an AuthenticAMD host with the fixed Intel model (no +svm in phase 0.5)")
		case "GenuineIntel":
			return CPUOption{}, errors.New("qemu: cpu-nested=on requires vmx on every CPU, but not every CPU advertises vmx on this GenuineIntel host")
		default:
			return CPUOption{}, fmt.Errorf("qemu: cpu-nested=on requires a GenuineIntel host with vmx; host vendor %q is unknown or mixed", host.Vendor)
		}
	}
	return CPUOption{}, fmt.Errorf("qemu: invalid cpu-nested mode %q", mode)
}
