package cli

import (
	"io"
	"os"
	"strings"
)

// sshKnownHostsPattern is the os.CreateTemp pattern of a per-invocation pin.
const sshKnownHostsPattern = "bbraft-known-hosts-*"

// writeTempKnownHosts creates a private known_hosts file that contains exactly
// one authenticated alias/key line. The caller must run cleanup after the ssh
// or scp child has joined. There is no shared file, so concurrent invocations
// cannot lose each other's pin and the user's own known_hosts is never edited.
func writeTempKnownHosts(key sshHostKey, alias string) (string, func(), error) {
	file, err := os.CreateTemp("", sshKnownHostsPattern)
	if err != nil {
		return "", nil, sshHostKeyError("create a temporary known-hosts file failed")
	}
	name := file.Name()
	cleanup := func() { _ = os.Remove(name) }
	if strings.ContainsAny(name, "\r\n\x00") {
		_ = file.Close()
		cleanup()
		return "", nil, sshHostKeyError("temporary known-hosts path cannot be used in an ssh option")
	}
	if err := file.Chmod(0o600); err != nil {
		_ = file.Close()
		cleanup()
		return "", nil, sshHostKeyError("set temporary known-hosts permissions failed")
	}
	if _, err := io.WriteString(file, key.knownHostsLine(alias)); err != nil {
		_ = file.Close()
		cleanup()
		return "", nil, sshHostKeyError("write the temporary known-hosts file failed")
	}
	if err := file.Close(); err != nil {
		cleanup()
		return "", nil, sshHostKeyError("close the temporary known-hosts file failed")
	}
	return name, cleanup, nil
}

// sshConfigValue renders value as one ssh_config option value. OpenSSH parses
// the value of -o with its config tokenizer, so a path containing spaces,
// quotes or backslashes must stay inside one argv element and be escaped.
func sshConfigValue(value string) string {
	var builder strings.Builder
	builder.Grow(len(value) + 2)
	builder.WriteByte('"')
	for i := 0; i < len(value); i++ {
		switch value[i] {
		case '\\', '"':
			builder.WriteByte('\\')
		}
		builder.WriteByte(value[i])
	}
	builder.WriteByte('"')
	return builder.String()
}
