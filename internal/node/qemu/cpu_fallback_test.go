package qemu

import (
	"errors"
	"slices"
	"testing"
)

// qemuAPICvStderr is QEMU 10.0's enforce output for Haswell-noTSX,+vmx on an
// desktop Intel CPU that has VMX but no APICv or posted interrupts.
const qemuAPICvStderr = `qemu-system-x86_64: warning: host doesn't support requested feature: MSR(48BH).vmx-apicv-register [bit 8]
qemu-system-x86_64: warning: host doesn't support requested feature: MSR(48BH).vmx-apicv-vid [bit 9]
qemu-system-x86_64: warning: host doesn't support requested feature: MSR(48DH).vmx-posted-intr [bit 7]
qemu-system-x86_64: Host doesn't support requested features`

func TestMissingCPUFeatures(t *testing.T) {
	for _, tc := range []struct {
		name string
		text string
		want []string
	}{
		{"apicv", qemuAPICvStderr, []string{"vmx-apicv-register", "vmx-apicv-vid", "vmx-posted-intr"}},
		{"cpuid", "qemu: cpu probe: qemu-system-x86_64: warning: host doesn't support requested feature: CPUID.01H:ECX.pcid [bit 17]", []string{"pcid"}},
		{"duplicates collapse", qemuAPICvStderr + "\n" + qemuAPICvStderr, []string{"vmx-apicv-register", "vmx-apicv-vid", "vmx-posted-intr"}},
		{"none", "qemu-system-x86_64: -accel kvm: Could not access KVM kernel module", nil},
		{"garbage name ignored", "warning: host doesn't support requested feature: MSR(1).Bad Name [bit 1]", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := MissingCPUFeatures(tc.text); !slices.Equal(got, tc.want) {
				t.Fatalf("MissingCPUFeatures = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestNestedFallback(t *testing.T) {
	nested := CPUOption{Model: DefaultCPUModel, Nested: "vmx"}
	apicv := errors.New("qemu: cpu probe: qemu exited during startup: exit status 1: " + qemuAPICvStderr)
	for _, tc := range []struct {
		name        string
		option      CPUOption
		err         error
		wantOK      bool
		wantToggles []string
	}{
		{"apicv missing under nested vmx", nested, apicv, true,
			[]string{"-vmx-apicv-register", "-vmx-apicv-vid", "-vmx-posted-intr"}},
		{"no error", nested, nil, false, nil},
		{"nested off never falls back", CPUOption{Model: DefaultCPUModel}, apicv, false, nil},
		{"base model feature missing", nested,
			errors.New("warning: host doesn't support requested feature: CPUID.01H:ECX.pcid [bit 17]\n" + qemuAPICvStderr), false, nil},
		{"unrelated failure", nested, errors.New("qemu: cpu probe: start qemu-system-x86_64: no such file"), false, nil},
		{"explicitly requested feature stands", CPUOption{Model: DefaultCPUModel, Nested: "vmx", Features: []string{"+vmx-apicv-vid"}}, apicv, false, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			toggles, dropped, ok := NestedFallback(tc.option, tc.err)
			if ok != tc.wantOK || !slices.Equal(toggles, tc.wantToggles) {
				t.Fatalf("NestedFallback = %q, %v; want %q, %v", toggles, ok, tc.wantToggles, tc.wantOK)
			}
			if ok && len(dropped) != len(toggles) {
				t.Fatalf("dropped %q does not match toggles %q", dropped, toggles)
			}
		})
	}
}

func TestParseCPUFeatures(t *testing.T) {
	for _, tc := range []struct {
		in      string
		want    []string
		wantErr bool
	}{
		{"", nil, false},
		{"  ", nil, false},
		{"-vmx-apicv-vid", []string{"-vmx-apicv-vid"}, false},
		{" -vmx-apicv-vid, +pcid ", []string{"-vmx-apicv-vid", "+pcid"}, false},
		{"vmx-apicv-vid", nil, true},       // no sign
		{"+vmx", nil, true},                // owned by -node.cpu-nested
		{"-svm", nil, true},                // owned by -node.cpu-nested
		{"enforce", nil, true},             // not a toggle
		{"+level=13", nil, true},           // property, not a toggle
		{"+Pcid", nil, true},               // QEMU names are lowercase
		{"-a,,-b", nil, true},              // empty entry
		{"-pcid,+pcid", nil, true},         // same feature twice
		{"+-pcid", nil, true},              // name must start alphanumeric
		{"-vmx-apicv-vid,host", nil, true}, // a model smuggled in
	} {
		got, err := ParseCPUFeatures(tc.in)
		if (err != nil) != tc.wantErr || !slices.Equal(got, tc.want) {
			t.Errorf("ParseCPUFeatures(%q) = %q, %v; want %q, error %v", tc.in, got, err, tc.want, tc.wantErr)
		}
	}
	many := ""
	for i := range maxCPUFeatures + 1 {
		if i > 0 {
			many += ","
		}
		many += "-f" + string(rune('a'+i%26)) + string(rune('a'+i/26))
	}
	if _, err := ParseCPUFeatures(many); err == nil {
		t.Errorf("ParseCPUFeatures accepted %d features", maxCPUFeatures+1)
	}
}

func TestCPUOptionStringFeatures(t *testing.T) {
	for _, tc := range []struct {
		option CPUOption
		want   string
	}{
		{CPUOption{Model: "Haswell-noTSX", Features: []string{"-vmx-apicv-vid"}}, "Haswell-noTSX,-vmx-apicv-vid,enforce"},
		{CPUOption{Model: "Haswell-noTSX", Nested: "vmx", Features: []string{"-vmx-apicv-register", "-vmx-apicv-vid"}},
			"Haswell-noTSX,+vmx,-vmx-apicv-register,-vmx-apicv-vid,enforce"},
	} {
		if got := tc.option.String(); got != tc.want {
			t.Errorf("String() = %q, want %q", got, tc.want)
		}
	}
}

func TestConfigCPUCarriesFeatures(t *testing.T) {
	host := HostCPU{Vendor: "GenuineIntel", Vmx: true, CPUs: 8}
	c := Config{CPUFeatures: []string{"-vmx-apicv-vid"}, HostCPU: &host}
	got, err := c.CPU()
	if err != nil {
		t.Fatal(err)
	}
	if want := "Haswell-noTSX,+vmx,-vmx-apicv-vid,enforce"; got.String() != want {
		t.Fatalf("CPU() = %q, want %q", got.String(), want)
	}
	c.CPUFeatures[0] = "-changed"
	if got.Features[0] != "-vmx-apicv-vid" {
		t.Fatal("CPU() shares the Config's feature slice")
	}
}

func TestArgsAndValidateCPUFeatures(t *testing.T) {
	c := testConfig(t)
	c.CPUFeatures = []string{"-vmx-posted-intr"}
	args, err := c.Args()
	if err != nil {
		t.Fatal(err)
	}
	i := slices.Index(args, "-cpu")
	if i < 0 || i+1 >= len(args) || !slices.Contains([]string{
		"Haswell-noTSX,+vmx,-vmx-posted-intr,enforce",
		"Haswell-noTSX,-vmx-posted-intr,enforce",
	}, args[i+1]) {
		t.Fatalf("-cpu argument missing the feature toggle: %q", args)
	}
	c.CPUFeatures = []string{"+vmx"}
	if err := c.Validate(); err == nil {
		t.Fatal("Validate accepted +vmx in CPUFeatures")
	}
}
