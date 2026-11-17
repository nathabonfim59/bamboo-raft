#!/usr/bin/env bash
# Regression for dpkg_version.sh: build-time package metadata, no runtime exec.
#
# R7 failed after mkosi unmounted the image because build.sh ran
# `chroot <root> /usr/bin/podman --version` and the chroot had no /proc. The
# version must come from the target dpkg status database instead.
set -euo pipefail

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)
query="$script_dir/dpkg_version.sh"
tmp=$(mktemp -d "${TMPDIR:-/tmp}/bbraft-dpkg-version.XXXXXX")
trap 'rm -rf "$tmp"' EXIT

failures=0
pass() { printf 'PASS %s\n' "$*"; }
fail() {
	printf 'FAIL %s\n' "$*" >&2
	failures=$((failures + 1))
}

status=$tmp/status
cat >"$status" <<'EOF'
Package: containernetworking-plugins
Status: install ok installed
Version: 1.4.0-1ubuntu0.1
Architecture: amd64

Package: podman
Status: install ok installed
Version: 4.9.3+ds1-1ubuntu0.1
Architecture: amd64

Package: old-podman-helper
Status: deinstall ok config-files
Version: 1.0
Architecture: amd64
EOF

if got=$("$query" "$status" podman) && [ "$got" = "4.9.3+ds1-1ubuntu0.1" ]; then
	pass "reads real dpkg-status version"
else
	fail "podman version query returned '$got'"
fi

if "$query" "$status" absent-package >"$tmp/absent.log" 2>&1; then
	fail "absent package accepted"
else
	pass "absent package rejected"
fi
if grep -q 'not installed' "$tmp/absent.log"; then
	pass "absent failure is explicit"
else
	fail "absent failure is not explicit"
fi

if "$query" "$status" old-podman-helper >"$tmp/config-files.log" 2>&1; then
	fail "config-files-only package accepted"
else
	pass "config-files-only package rejected"
fi

cat >>"$status" <<'EOF'

Package: podman
Status: install ok installed
Version: 4.9.3+ds1-1ubuntu0.2
Architecture: amd64
EOF
if "$query" "$status" podman >"$tmp/ambiguous.log" 2>&1; then
	fail "ambiguous installed stanzas accepted"
else
	pass "ambiguous installed stanzas rejected"
fi
if grep -q 'multiple installed stanzas' "$tmp/ambiguous.log"; then
	pass "ambiguous failure is explicit"
else
	fail "ambiguous failure is not explicit"
fi

# Failing-before shape: the fixture has no runtime podman binary at all.
if [ -x "$tmp/usr/bin/podman" ]; then
	fail "fixture unexpectedly has a runtime podman binary"
else
	pass "metadata query needs no runtime podman binary"
fi
if "$tmp/usr/bin/podman" --version >/dev/null 2>&1; then
	fail "old runtime query unexpectedly worked"
else
	pass "old runtime query cannot run without the image binary"
fi

if [ "$failures" -ne 0 ]; then
	printf 'dpkg_version_test: %d failure(s)\n' "$failures" >&2
	exit 1
fi
printf 'dpkg_version_test: all checks passed\n'
