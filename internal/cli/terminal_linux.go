//go:build linux

package cli

import (
	"os"

	"golang.org/x/sys/unix"
)

// isTerminalFile reports whether f is a terminal. A character device alone is
// not enough: /dev/null is one, and treating it as interactive would enable
// prompts in scripts. TCGETS only succeeds on a real terminal.
func isTerminalFile(f *os.File) bool {
	_, err := unix.IoctlGetTermios(int(f.Fd()), unix.TCGETS)
	return err == nil
}
