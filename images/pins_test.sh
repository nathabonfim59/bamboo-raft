#!/usr/bin/env bash
# Fast pin-invariant checks shared by build.sh and docs:
# - Go toolchain pin equals the go.mod go directive;
# - mkosi.conf Snapshot= equals pins.sh UBUNTU_SNAPSHOT;
# - download pins are real 64-character lowercase sha256 values.
set -euo pipefail

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)
repo_root=$(cd -- "$script_dir/.." && pwd -P)
# shellcheck source-path=SCRIPTDIR
# shellcheck source=pins.sh
# shellcheck disable=SC1091
. "$script_dir/pins.sh"

die() {
	printf 'pins_test: %s\n' "$*" >&2
	exit 1
}

gomod_go=$(awk '$1 == "go" { print $2; exit }' "$repo_root/go.mod")
[ -n "$gomod_go" ] || die "go.mod has no go directive"
[ "$gomod_go" = "$GO_VERSION" ] ||
	die "GO_VERSION=$GO_VERSION does not match go.mod go $gomod_go"

grep -q "^Snapshot=$UBUNTU_SNAPSHOT$" "$script_dir/mkosi.conf" ||
	die "mkosi.conf Snapshot= does not match UBUNTU_SNAPSHOT=$UBUNTU_SNAPSHOT"

for name in MKOSI_SHA256 GO_SHA256 NODE_SHA256; do
	value=${!name}
	[ "${#value}" -eq 64 ] || die "$name is not 64 hex characters"
	case "$value" in
	*[!0-9a-f]*) die "$name is not lowercase hex" ;;
	esac
done

printf 'pins_test: all checks passed\n'
