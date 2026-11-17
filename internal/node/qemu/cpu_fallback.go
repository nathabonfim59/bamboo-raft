package qemu

import (
	"slices"
	"strings"
)

// missingFeatureMarker starts QEMU's enforce diagnostic for one feature the
// host cannot provide, e.g.
//
//	qemu-system-x86_64: warning: host doesn't support requested feature: MSR(48BH).vmx-apicv-register [bit 8]
//	qemu-system-x86_64: warning: host doesn't support requested feature: CPUID.07H:EBX.hle [bit 4]
const missingFeatureMarker = "host doesn't support requested feature: "

// MissingCPUFeatures extracts the feature names from QEMU's enforce
// diagnostics in text (a failed probe's error or stderr), in order, without
// duplicates. It returns nil when text names none.
func MissingCPUFeatures(text string) []string {
	var names []string
	for line := range strings.Lines(text) {
		_, rest, ok := strings.Cut(line, missingFeatureMarker)
		if !ok {
			continue
		}
		rest, _, _ = strings.Cut(rest, " [")
		rest = strings.TrimSpace(rest)
		// The register prefix (MSR(48BH), CPUID.07H:EBX) ends at the last '.'.
		if _, name, ok := strings.CutLast(rest, "."); ok {
			rest = name
		}
		if validFeatureName(rest) && !slices.Contains(names, rest) {
			names = append(names, rest)
		}
	}
	return names
}

// NestedFallback decides whether a failed probe of option may be retried with
// fewer nested-virtualization features. It applies only when option exposes
// nested VMX, every missing feature is a VMX capability ("vmx-..."), and none
// of them was requested explicitly in option.Features. It returns the "-name"
// toggles to append and the dropped names; ok is false when the failure must
// stand (a missing base-model feature keeps the fixed model honest).
//
// Nested KVM works without such optional VMX capabilities (APICv, posted
// interrupts on desktop CPUs), only slower for the nested guest.
func NestedFallback(option CPUOption, probeErr error) (toggles, dropped []string, ok bool) {
	if option.Nested != "vmx" || probeErr == nil {
		return nil, nil, false
	}
	missing := MissingCPUFeatures(probeErr.Error())
	if len(missing) == 0 {
		return nil, nil, false
	}
	for _, name := range missing {
		if !strings.HasPrefix(name, "vmx-") {
			return nil, nil, false
		}
		if slices.ContainsFunc(option.Features, func(f string) bool { return f[1:] == name }) {
			return nil, nil, false
		}
		toggles = append(toggles, "-"+name)
	}
	return toggles, missing, true
}
