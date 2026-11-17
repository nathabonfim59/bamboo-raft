//go:build !linux

package cli

import "os"

// isTerminalFile falls back to the character-device check on platforms without
// the Linux TCGETS ioctl. Linux, the supported server and CLI platform, uses
// the precise check in terminal_linux.go.
func isTerminalFile(f *os.File) bool {
	info, err := f.Stat()
	return err == nil && info.Mode()&os.ModeCharDevice != 0
}
