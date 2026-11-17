package qemu

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func cpuinfoFixture(vendor string, vmx, svm bool, cpus int) string {
	var b strings.Builder
	for i := range cpus {
		b.WriteString("processor\t: ")
		b.WriteString(strconv.Itoa(i))
		b.WriteString("\nvendor_id\t: ")
		b.WriteString(vendor)
		b.WriteString("\nflags\t\t: fpu sse sse2")
		if vmx {
			b.WriteString(" vmx")
		}
		if svm {
			b.WriteString(" svm")
		}
		b.WriteString("\n\n")
	}
	return b.String()
}

func writeCPUInfo(t *testing.T, data string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "cpuinfo")
	if err := os.WriteFile(path, []byte(data), 0o600); err != nil {
		t.Fatalf("write cpuinfo: %v", err)
	}
	return path
}

func TestParseNestedMode(t *testing.T) {
	tests := []struct {
		in      string
		want    NestedMode
		wantErr bool
	}{
		{in: "", want: NestedAuto},
		{in: "auto", want: NestedAuto},
		{in: "on", want: NestedOn},
		{in: "off", want: NestedOff},
		{in: "sometimes", wantErr: true},
		{in: "AUTO", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			got, err := ParseNestedMode(tt.in)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("ParseNestedMode(%q) succeeded", tt.in)
				}
				return
			}
			if err != nil || got != tt.want {
				t.Fatalf("ParseNestedMode(%q) = %q, %v; want %q", tt.in, got, err, tt.want)
			}
		})
	}
}

func TestDetectHostCPU(t *testing.T) {
	tests := []struct {
		name    string
		data    string
		want    HostCPU
		wantErr bool
	}{
		{
			name: "intel with vmx on every cpu",
			data: cpuinfoFixture("GenuineIntel", true, false, 4),
			want: HostCPU{Vendor: "GenuineIntel", Vmx: true, CPUs: 4},
		},
		{
			name: "amd with svm on every cpu",
			data: cpuinfoFixture("AuthenticAMD", false, true, 2),
			want: HostCPU{Vendor: "AuthenticAMD", Svm: true, CPUs: 2},
		},
		{
			name: "intel one cpu lacks vmx",
			data: cpuinfoFixture("GenuineIntel", true, false, 1) +
				cpuinfoFixture("GenuineIntel", false, false, 1),
			want: HostCPU{Vendor: "GenuineIntel", Vmx: false, CPUs: 2},
		},
		{
			name: "mixed vendor and flags",
			data: cpuinfoFixture("GenuineIntel", true, false, 1) +
				cpuinfoFixture("AuthenticAMD", false, true, 1),
			want: HostCPU{Vmx: false, Svm: false, CPUs: 2},
		},
		{
			name:    "no processor blocks",
			data:    "vendor_id\t: GenuineIntel\n",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := writeCPUInfo(t, tt.data)
			got, err := DetectHostCPU(path)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("DetectHostCPU succeeded: %+v", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("DetectHostCPU: %v", err)
			}
			if got != tt.want {
				t.Fatalf("DetectHostCPU = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestDetectHostCPUMissingFile(t *testing.T) {
	if _, err := DetectHostCPU(filepath.Join(t.TempDir(), "missing")); err == nil {
		t.Fatal("DetectHostCPU succeeded on a missing file")
	}
}

func TestResolveCPU(t *testing.T) {
	intel := HostCPU{Vendor: "GenuineIntel", Vmx: true, CPUs: 8}
	intelNoVmx := HostCPU{Vendor: "GenuineIntel", CPUs: 8}
	amd := HostCPU{Vendor: "AuthenticAMD", Svm: true, CPUs: 8}
	unknown := HostCPU{CPUs: 8}

	tests := []struct {
		name    string
		model   string
		mode    NestedMode
		host    HostCPU
		want    string
		wantErr string
	}{
		{name: "default auto intel vmx", mode: NestedAuto, host: intel, want: "Haswell-noTSX,+vmx,enforce"},
		{name: "default auto intel no vmx", mode: NestedAuto, host: intelNoVmx, want: "Haswell-noTSX,enforce"},
		{name: "default auto amd", mode: NestedAuto, host: amd, want: "Haswell-noTSX,enforce"},
		{name: "default auto unknown", mode: NestedAuto, host: unknown, want: "Haswell-noTSX,enforce"},
		{name: "empty mode means auto", host: intel, want: "Haswell-noTSX,+vmx,enforce"},
		{name: "on intel vmx", mode: NestedOn, host: intel, want: "Haswell-noTSX,+vmx,enforce"},
		{name: "on intel no vmx", mode: NestedOn, host: intelNoVmx, wantErr: "requires vmx"},
		{name: "on amd", mode: NestedOn, host: amd, wantErr: "AuthenticAMD"},
		{name: "on unknown", mode: NestedOn, host: unknown, wantErr: "unknown or mixed"},
		{name: "off intel", mode: NestedOff, host: intel, want: "Haswell-noTSX,enforce"},
		{name: "off amd", mode: NestedOff, host: amd, want: "Haswell-noTSX,enforce"},
		{name: "explicit model honored", model: "EPYC-Rome", mode: NestedAuto, host: amd, want: "EPYC-Rome,enforce"},
		{name: "explicit model on amd refused", model: "EPYC-Rome", mode: NestedOn, host: amd, wantErr: "AuthenticAMD"},
		{name: "host model rejected", model: "host", mode: NestedOff, wantErr: "invalid fixed CPU model"},
		{name: "max model rejected", model: "max", mode: NestedOff, wantErr: "invalid fixed CPU model"},
		{name: "invalid mode", mode: "sometimes", host: intel, wantErr: "invalid cpu-nested mode"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ResolveCPU(tt.model, tt.mode, tt.host)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("ResolveCPU = %q, %v; want error %q", got.String(), err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("ResolveCPU: %v", err)
			}
			if got.String() != tt.want {
				t.Fatalf("ResolveCPU = %q, want %q", got.String(), tt.want)
			}
		})
	}
}

func TestCPUOptionString(t *testing.T) {
	tests := []struct {
		option CPUOption
		want   string
	}{
		{option: CPUOption{Model: "Haswell-noTSX"}, want: "Haswell-noTSX,enforce"},
		{option: CPUOption{Model: "Haswell-noTSX", Nested: "vmx"}, want: "Haswell-noTSX,+vmx,enforce"},
	}
	for _, tt := range tests {
		if got := tt.option.String(); got != tt.want {
			t.Errorf("CPUOption.String() = %q, want %q", got, tt.want)
		}
	}
}

func TestResolveTestCPU(t *testing.T) {
	intel := HostCPU{Vendor: "GenuineIntel", Vmx: true, CPUs: 8}
	intelNoVmx := HostCPU{Vendor: "GenuineIntel", CPUs: 8}
	amd := HostCPU{Vendor: "AuthenticAMD", Svm: true, CPUs: 8}

	tests := []struct {
		name    string
		model   string
		mode    NestedMode
		host    HostCPU
		want    string
		wantErr string
	}{
		{name: "host intel auto", model: "host", mode: NestedAuto, host: intel, want: "host,+vmx,enforce"},
		{name: "host amd auto", model: "host", mode: NestedAuto, host: amd, want: "host,enforce"},
		{name: "host intel off", model: "host", mode: NestedOff, host: intel, want: "host,enforce"},
		{name: "host amd off", model: "host", mode: NestedOff, host: amd, want: "host,enforce"},
		{name: "host intel on", model: "host", mode: NestedOn, host: intel, want: "host,+vmx,enforce"},
		{name: "host intel no vmx on", model: "host", mode: NestedOn, host: intelNoVmx, wantErr: "requires vmx"},
		{name: "host amd on uses test wording", model: "host", mode: NestedOn, host: amd, wantErr: "test host CPU model"},
		{name: "max still rejected", model: "max", mode: NestedOff, wantErr: `only "host"`},
		{name: "fixed model rejected by test resolver", model: "Haswell-noTSX", mode: NestedOff, wantErr: `only "host"`},
		{name: "empty model rejected by test resolver", model: "", mode: NestedOff, wantErr: `only "host"`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ResolveTestCPU(tt.model, tt.mode, tt.host)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("ResolveTestCPU = %q, %v; want error %q", got.String(), err, tt.wantErr)
				}
				if strings.Contains(err.Error(), "fixed Intel") {
					t.Fatalf("test resolver used the production fixed-Intel reason: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("ResolveTestCPU: %v", err)
			}
			if got.String() != tt.want {
				t.Fatalf("ResolveTestCPU = %q, want %q", got.String(), tt.want)
			}
		})
	}
}

func TestConfigCPUAllowHostCPU(t *testing.T) {
	cfg := testConfig(t)
	cfg.CPUModel = "host"
	cfg.AllowHostCPU = true
	cfg.Nested = NestedAuto
	option, err := cfg.CPU()
	if err != nil {
		t.Fatalf("CPU: %v", err)
	}
	if got, want := option.Model, "host"; got != want {
		t.Fatalf("CPU model = %q, want %q", got, want)
	}
	if got, want := option.String(), "host,+vmx,enforce"; got != want {
		t.Fatalf("CPU = %q, want %q", got, want)
	}

	cfg.AllowHostCPU = false
	if _, err := cfg.CPU(); err == nil || !strings.Contains(err.Error(), "invalid fixed CPU model") {
		t.Fatalf("CPU without opt-in = %v, want strict rejection", err)
	}

	cfg.AllowHostCPU = true
	cfg.CPUModel = "max"
	if _, err := cfg.CPU(); err == nil || !strings.Contains(err.Error(), "invalid fixed CPU model") {
		t.Fatalf("CPU max with opt-in = %v, want rejection", err)
	}
}

func TestArgsAllowHostCPU(t *testing.T) {
	tests := []struct {
		name    string
		mode    NestedMode
		host    *HostCPU
		want    string
		wantErr string
	}{
		{name: "host intel auto", mode: NestedAuto, host: &HostCPU{Vendor: "GenuineIntel", Vmx: true, CPUs: 8}, want: "host,+vmx,enforce"},
		{name: "host amd auto", mode: NestedAuto, host: &HostCPU{Vendor: "AuthenticAMD", Svm: true, CPUs: 8}, want: "host,enforce"},
		{name: "host off", mode: NestedOff, host: nil, want: "host,enforce"},
		{name: "host amd on", mode: NestedOn, host: &HostCPU{Vendor: "AuthenticAMD", Svm: true, CPUs: 8}, wantErr: "test host CPU model"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := testConfig(t)
			cfg.CPUModel = "host"
			cfg.AllowHostCPU = true
			cfg.Nested = tt.mode
			cfg.HostCPU = tt.host
			got, err := argvCPU(t, cfg)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("Args cpu = %q, %v; want error %q", got, err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("Args: %v", err)
			}
			if got != tt.want {
				t.Fatalf("Args cpu = %q, want %q", got, tt.want)
			}
		})
	}

	cfg := testConfig(t)
	cfg.CPUModel = "max"
	cfg.AllowHostCPU = true
	if _, err := cfg.Args(); err == nil || !strings.Contains(err.Error(), "host-dependent") {
		t.Fatalf("Args max = %v, want rejection", err)
	}
}

func TestConfigCPURequiresInjectedHost(t *testing.T) {
	cfg := testConfig(t)
	cfg.HostCPU = nil
	for _, mode := range []NestedMode{"", NestedAuto, NestedOn} {
		cfg.Nested = mode
		if _, err := cfg.CPU(); err == nil || !strings.Contains(err.Error(), "not injected") {
			t.Fatalf("CPU with nil HostCPU and mode %q = %v, want not-injected error", mode, err)
		}
	}

	cfg.Nested = NestedOff
	option, err := cfg.CPU()
	if err != nil {
		t.Fatalf("CPU off with nil HostCPU: %v", err)
	}
	if got, want := option.String(), "Haswell-noTSX,enforce"; got != want {
		t.Fatalf("CPU off = %q, want %q", got, want)
	}

	cfg.Nested = NestedAuto
	cfg.HostCPU = &HostCPU{}
	option, err = cfg.CPU()
	if err != nil {
		t.Fatalf("CPU auto with empty facts: %v", err)
	}
	if got, want := option.String(), "Haswell-noTSX,enforce"; got != want {
		t.Fatalf("CPU auto empty facts = %q, want %q", got, want)
	}
}

func argvCPU(t *testing.T, cfg Config) (string, error) {
	t.Helper()
	argv, err := cfg.Args()
	if err != nil {
		return "", err
	}
	for i := 0; i+1 < len(argv); i++ {
		if argv[i] == "-cpu" {
			return argv[i+1], nil
		}
	}
	t.Fatal("argv has no -cpu")
	return "", nil
}

func TestArgsNestedModes(t *testing.T) {
	intel := &HostCPU{Vendor: "GenuineIntel", Vmx: true, CPUs: 8}
	intelNoVmx := &HostCPU{Vendor: "GenuineIntel", CPUs: 8}
	amd := &HostCPU{Vendor: "AuthenticAMD", Svm: true, CPUs: 8}

	tests := []struct {
		name    string
		mode    NestedMode
		host    *HostCPU
		want    string
		wantErr string
	}{
		{name: "auto intel", mode: NestedAuto, host: intel, want: "Haswell-noTSX,+vmx,enforce"},
		{name: "on intel", mode: NestedOn, host: intel, want: "Haswell-noTSX,+vmx,enforce"},
		{name: "off intel", mode: NestedOff, host: intel, want: "Haswell-noTSX,enforce"},
		{name: "auto amd", mode: NestedAuto, host: amd, want: "Haswell-noTSX,enforce"},
		{name: "off amd", mode: NestedOff, host: amd, want: "Haswell-noTSX,enforce"},
		{name: "auto intel no vmx", mode: NestedAuto, host: intelNoVmx, want: "Haswell-noTSX,enforce"},
		{name: "on intel no vmx", mode: NestedOn, host: intelNoVmx, wantErr: "requires vmx"},
		{name: "on amd", mode: NestedOn, host: amd, wantErr: "AuthenticAMD"},
		{name: "auto no host", mode: NestedAuto, host: nil, wantErr: "not injected"},
		{name: "on no host", mode: NestedOn, host: nil, wantErr: "not injected"},
		{name: "off no host", mode: NestedOff, host: nil, want: "Haswell-noTSX,enforce"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := testConfig(t)
			cfg.Nested = tt.mode
			cfg.HostCPU = tt.host
			got, err := argvCPU(t, cfg)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("Args cpu = %q, %v; want error %q", got, err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("Args: %v", err)
			}
			if got != tt.want {
				t.Fatalf("Args cpu = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestArgsExplicitModelHonored(t *testing.T) {
	cfg := testConfig(t)
	cfg.CPUModel = "EPYC-Rome"
	cfg.Nested = NestedAuto
	cfg.HostCPU = &HostCPU{Vendor: "AuthenticAMD", Svm: true, CPUs: 8}
	got, err := argvCPU(t, cfg)
	if err != nil {
		t.Fatalf("Args: %v", err)
	}
	if got != "EPYC-Rome,enforce" {
		t.Fatalf("Args cpu = %q, want explicit model unchanged with enforce", got)
	}
}
