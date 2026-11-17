package main

import (
	"os"
	"strings"
	"testing"

	"github.com/nathabonfim59/bamboo-raft/internal/node/qemu"
)

func TestNestedReason(t *testing.T) {
	tests := []struct {
		name  string
		mode  qemu.NestedMode
		host  qemu.HostCPU
		model string
		have  string
		want  string
		fails bool // want the reason not to contain want
	}{
		{
			name: "amd auto explains no svm",
			mode: qemu.NestedAuto,
			host: qemu.HostCPU{Vendor: "AuthenticAMD", Svm: true},
			have: "",
			want: "AuthenticAMD",
		},
		{
			name: "intel without vmx explains it",
			mode: qemu.NestedAuto,
			host: qemu.HostCPU{Vendor: "GenuineIntel", Vmx: false},
			have: "",
			want: "GenuineIntel",
		},
		{
			name: "unknown vendor explains it",
			mode: qemu.NestedAuto,
			host: qemu.HostCPU{},
			have: "",
			want: "unknown or mixed",
		},
		{
			name: "off names the mode",
			mode: qemu.NestedOff,
			host: qemu.HostCPU{Vendor: "GenuineIntel", Vmx: true},
			have: "",
			want: "cpu-nested=off",
		},
		{
			name:  "explicit test host on amd names the test override",
			mode:  qemu.NestedAuto,
			host:  qemu.HostCPU{Vendor: "AuthenticAMD", Svm: true},
			model: "host",
			have:  "",
			want:  "explicit test -cpu host",
		},
		{
			name:  "explicit test host on amd never claims the fixed intel vendor",
			mode:  qemu.NestedAuto,
			host:  qemu.HostCPU{Vendor: "AuthenticAMD", Svm: true},
			model: "host",
			have:  "",
			fails: true,
			want:  "fixed Intel",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := nestedReason(tt.mode, tt.host, tt.have, tt.model)
			if tt.fails {
				if strings.Contains(got, tt.want) {
					t.Fatalf("nestedReason(%s, %+v, %q, %q) = %q, must not mention %q", tt.mode, tt.host, tt.have, tt.model, got, tt.want)
				}
				return
			}
			if !strings.Contains(got, tt.want) {
				t.Fatalf("nestedReason(%s, %+v, %q, %q) = %q, want it to mention %q", tt.mode, tt.host, tt.have, tt.model, got, tt.want)
			}
		})
	}
}

// The resolver path used by both modes is the shared qemu policy; these
// checks pin the smoke-side integration (flag strings to option rendering).
func TestResolveCPUShared(t *testing.T) {
	intelCpuinfo := "processor\t: 0\nvendor_id\t: GenuineIntel\nflags\t\t: fpu vmx sse4_2\n\nprocessor\t: 1\nvendor_id\t: GenuineIntel\nflags\t\t: fpu vmx sse4_2\n"
	amdCpuinfo := "processor\t: 0\nvendor_id\t: AuthenticAMD\nflags\t\t: fpu svm sse4_2\n"

	t.Run("intel vmx auto", func(t *testing.T) {
		option, host, mode, reason, err := resolveCPUShared("", "auto", writeTemp(t, intelCpuinfo), false)
		if err != nil {
			t.Fatal(err)
		}
		if option.String() != "Haswell-noTSX,+vmx,enforce" || option.Nested != "vmx" {
			t.Fatalf("option = %q nested=%q", option.String(), option.Nested)
		}
		if host.Vendor != "GenuineIntel" || !host.Vmx || host.CPUs != 2 {
			t.Fatalf("host = %+v", host)
		}
		if mode != qemu.NestedAuto || reason == "" {
			t.Fatalf("mode=%s reason=%q", mode, reason)
		}
	})

	t.Run("amd svm auto adds nothing", func(t *testing.T) {
		option, _, _, reason, err := resolveCPUShared("", "auto", writeTemp(t, amdCpuinfo), false)
		if err != nil {
			t.Fatal(err)
		}
		if option.String() != "Haswell-noTSX,enforce" || option.Nested != "" {
			t.Fatalf("AMD auto must not add svm in phase 0.5, got %q", option.String())
		}
		if !strings.Contains(reason, "AuthenticAMD") {
			t.Fatalf("AMD auto reason must explain the vendor, got %q", reason)
		}
	})

	t.Run("amd svm on fails clearly", func(t *testing.T) {
		_, _, _, _, err := resolveCPUShared("", "on", writeTemp(t, amdCpuinfo), false)
		if err == nil || !strings.Contains(err.Error(), "AuthenticAMD") {
			t.Fatalf("AMD on error = %v, want a clear AuthenticAMD refusal", err)
		}
	})

	t.Run("off skips detection entirely", func(t *testing.T) {
		// A missing cpuinfo path must not matter: off never detects.
		option, host, mode, _, err := resolveCPUShared("", "off", t.TempDir()+"/no-such-cpuinfo", false)
		if err != nil {
			t.Fatal(err)
		}
		if option.String() != "Haswell-noTSX,enforce" || mode != qemu.NestedOff {
			t.Fatalf("off option = %q mode=%s", option.String(), mode)
		}
		if host.CPUs != 0 || host.Vendor != "" {
			t.Fatalf("off must not report host facts it never read: %+v", host)
		}
	})

	t.Run("auto detection failure clears facts and resolves none", func(t *testing.T) {
		option, host, _, reason, err := resolveCPUShared("", "auto", t.TempDir()+"/no-such-cpuinfo", false)
		if err != nil {
			t.Fatal(err)
		}
		if option.String() != "Haswell-noTSX,enforce" || option.Nested != "" {
			t.Fatalf("auto with no facts must resolve without nested, got %q", option.String())
		}
		if host.CPUs != 0 || host.Vendor != "" {
			t.Fatalf("partial host facts must be cleared: %+v", host)
		}
		if !strings.Contains(reason, "host CPU detection failed") {
			t.Fatalf("reason must name the detection failure, got %q", reason)
		}
	})

	t.Run("on detection failure fails clearly", func(t *testing.T) {
		_, _, _, _, err := resolveCPUShared("", "on", t.TempDir()+"/no-such-cpuinfo", false)
		if err == nil || !strings.Contains(err.Error(), "cpu-nested=on") {
			t.Fatalf("on with missing cpuinfo error = %v, want cpu-nested=on prefix", err)
		}
	})

	t.Run("invalid mode rejected", func(t *testing.T) {
		if _, _, _, _, err := resolveCPUShared("", "bogus", "", false); err == nil {
			t.Fatal("invalid mode accepted")
		}
	})

	t.Run("host rejected without test opt-in", func(t *testing.T) {
		if _, _, _, _, err := resolveCPUShared("host", "auto", writeTemp(t, intelCpuinfo), false); err == nil {
			t.Fatal("-cpu-model host accepted without -cpu-model-test-host")
		}
	})

	t.Run("test opt-in allows explicit host", func(t *testing.T) {
		option, host, _, reason, err := resolveCPUShared("host", "auto", writeTemp(t, intelCpuinfo), true)
		if err != nil {
			t.Fatal(err)
		}
		if option.String() != "host,+vmx,enforce" {
			t.Fatalf("explicit host option = %q, want host,+vmx,enforce (nested append unchanged)", option.String())
		}
		if host.Vendor != "GenuineIntel" {
			t.Fatalf("host facts = %+v", host)
		}
		if strings.Contains(reason, "fixed Intel") {
			t.Fatalf("explicit host reason must not claim the fixed Intel vendor: %q", reason)
		}
	})

	t.Run("test opt-in host on amd is host,enforce", func(t *testing.T) {
		option, _, _, reason, err := resolveCPUShared("host", "auto", writeTemp(t, amdCpuinfo), true)
		if err != nil {
			t.Fatal(err)
		}
		if option.String() != "host,enforce" {
			t.Fatalf("AMD explicit host option = %q, want host,enforce", option.String())
		}
		if !strings.Contains(reason, "explicit test -cpu host") || strings.Contains(reason, "fixed Intel") {
			t.Fatalf("AMD explicit host reason = %q, want the test override and no fixed Intel claim", reason)
		}
	})

	t.Run("test opt-in still rejects max", func(t *testing.T) {
		if _, _, _, _, err := resolveCPUShared("max", "auto", writeTemp(t, intelCpuinfo), true); err == nil {
			t.Fatal("-cpu-model max accepted with test opt-in")
		}
	})
}

func writeTemp(t *testing.T, content string) string {
	t.Helper()
	path := t.TempDir() + "/cpuinfo"
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}
