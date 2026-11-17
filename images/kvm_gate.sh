#!/usr/bin/env bash
# Presence gate for the KVM review runs.
#
#   kvm_gate.sh <package> <top-level-test-name>...
#
# `go test -list` matches only top-level test functions, never subtests, and
# exits 0 with just an "ok <pkg>" summary even when no test matched — and a
# compile failure exits nonzero. So this script requires: the list command
# itself succeeds (compile errors fail the gate loudly), and every named
# top-level test appears as an exact standalone line in its output. A
# renamed, not-yet-published or missing test therefore fails the review
# before the expensive image build instead of silently running nothing.
set -euo pipefail

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)
repo_root=$(cd -- "$script_dir/.." && pwd -P)
GO=${GO:-go}

if [ $# -lt 2 ]; then
	printf 'usage: kvm_gate.sh <package> <top-level-test-name>...\n' >&2
	exit 2
fi
pkg=$1
shift

list_err=$(mktemp "${TMPDIR:-/tmp}/bbraft-kvm-gate.XXXXXX")
trap 'rm -f "$list_err"' EXIT
if ! list=$("$GO" -C "$repo_root" test -list 'Test.' "$pkg" 2>"$list_err"); then
	printf 'kvm_gate: go test -list failed for %s:\n' "$pkg" >&2
	tail -n 30 "$list_err" >&2
	exit 1
fi

missing=0
for name in "$@"; do
	if printf '%s\n' "$list" | grep -qx "$name"; then
		continue
	fi
	printf 'kvm_gate: required test %s is missing from %s\n' "$name" "$pkg" >&2
	missing=1
done
if [ "$missing" -ne 0 ]; then
	printf 'kvm_gate: present top-level tests:\n%s\n' "$list" >&2
	exit 1
fi
printf 'kvm_gate: all %d required test(s) present in %s\n' "$#" "$pkg" >&2
