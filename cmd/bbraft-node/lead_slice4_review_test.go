package main

import (
	"io"
	"testing"
)

func TestLeadUserModeRejectsExplicitJailDefaults(t *testing.T) {
	for name, flags := range map[string][]string{"binary default": {"-node.network-mode=user", "-node.jail-qemu-binary=" + defaultJailQEMUBinary}, "explicit zero": {"-node.network-mode=user", "-node.jail-io-read-bps=0"}} {
		t.Run(name, func(t *testing.T) {
			o, err := parseFlags(flags, io.Discard, testEnv(nil), func() (string, error) { return "h", nil })
			if err != nil {
				t.Fatal(err)
			}
			if _, err := o.jail(); err == nil {
				t.Fatal("user mode accepted an operator-supplied jail setting")
			}
		})
	}
}
