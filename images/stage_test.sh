#!/usr/bin/env bash
# Materialize a TEST-ONLY unpinned image into the disposable test output.
#
#   images/stage_test.sh <stage-dir> <dest-dir>
#
# This is the only path that moves a test image into place. It never reaches
# the release output: the destination parent must be exactly IMAGE_TEST_OUT,
# must not equal or contain IMAGE_OUT, the image id must carry the reserved
# -unpinned-test suffix, and the manifest must declare
# distribution.snapshot=unpinned plus a non-empty http(s) mirror. Normal
# publishing stays in images/publish.sh, which refuses these markers.
#
# The stage must already pass images/verify.sh. Failure leaves the stage and
# any existing destination untouched.
set -euo pipefail

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)
# shellcheck source-path=SCRIPTDIR
# shellcheck source=pins.sh
# shellcheck disable=SC1091
. "$script_dir/pins.sh"

usage() {
	cat <<'EOF'
usage: images/stage_test.sh <stage-dir> <dest-dir>
EOF
}

die() {
	printf 'stage_test: %s\n' "$*" >&2
	exit 1
}

[ $# -eq 2 ] || {
	usage >&2
	exit 2
}
stage=$1
dest=$2

[ -d "$stage" ] || die "stage '$stage' is not a directory"
[ -f "$stage/manifest.json" ] || die "stage '$stage' has no manifest.json"
command -v jq >/dev/null 2>&1 || die "jq is required"

image_id=$(basename "$stage")
# BEGIN TEST ID GRAMMAR GUARD
# Same safe one-component rule and 64-byte bound as build.sh/publish.sh.
case "$image_id" in
"" | "." | "..")
	die "invalid image id '$image_id'"
	;;
*[!a-z0-9._-]*)
	die "invalid image id '$image_id'; use only lowercase letters, digits, '.', '_' and '-'"
	;;
[!a-z0-9]*)
	die "invalid image id '$image_id'; must start with a lowercase letter or digit"
	;;
esac
[ "${#image_id}" -le 64 ] ||
	die "image id '$image_id' is longer than 64 characters"
# END TEST ID GRAMMAR GUARD
case "$image_id" in
*"$TEST_UNPINNED_SUFFIX") ;;
*) die "stage id '$image_id' does not end with the test-only suffix '$TEST_UNPINNED_SUFFIX'" ;;
esac
manifest_id=$(jq -r '.id // empty' "$stage/manifest.json")
[ "$manifest_id" = "$image_id" ] ||
	die "stage basename '$image_id' does not match manifest id '$manifest_id'"
[ "$(basename "$dest")" = "$image_id" ] ||
	die "destination basename must be '$image_id'"

manifest_snapshot=$(jq -r '.distribution.snapshot // empty' "$stage/manifest.json")
[ "$manifest_snapshot" = "$UBUNTU_UNPINNED_SNAPSHOT" ] ||
	die "stage '$image_id' must declare distribution.snapshot=$UBUNTU_UNPINNED_SNAPSHOT"
manifest_mirror=$(jq -r '.distribution.mirror // empty' "$stage/manifest.json")
[ -n "$manifest_mirror" ] ||
	die "stage '$image_id' has no distribution.mirror"
case "$manifest_mirror" in
http://?* | https://?*) ;;
*) die "stage '$image_id' has invalid distribution.mirror '$manifest_mirror'" ;;
esac

dest_parent=$(canonical_path "$(dirname "$dest")")
test_out=$(canonical_path "$IMAGE_TEST_OUT")
[ "$dest_parent" = "$test_out" ] ||
	die "destination parent '$dest_parent' must be IMAGE_TEST_OUT '$test_out'"
# Keep the original one-way release check before the symmetric guard: removing
# the marked guard must reproduce the old ancestor-hole behaviour.
case "$dest_parent/" in
"$(canonical_path "$IMAGE_OUT")"/*)
	die "refusing to materialize a test image in the release output '$IMAGE_OUT'"
	;;
esac

# BEGIN SYMMETRIC MODE PATH GUARD
# The test stager follows the same canonical policy as build.sh: equal or
# overlapping roots are refused in both directions, including the pinned
# cache and symlink aliases. A test output that is an ancestor of the release
# output can no longer receive a destination that is itself the release root.
pinned_out=$(canonical_path "$IMAGE_OUT")
pinned_cache=$(canonical_path "$IMAGE_CACHE")
test_cache=$(canonical_path "$IMAGE_TEST_CACHE")
if paths_overlap "$test_out" "$pinned_out"; then
	die "test output '$test_out' must not equal or overlap the release output '$pinned_out'"
fi
if paths_overlap "$test_out" "$pinned_cache"; then
	die "test output '$test_out' must not equal or overlap the pinned cache '$pinned_cache'"
fi
if paths_overlap "$test_out" "$test_cache"; then
	die "test output '$test_out' must not equal or overlap the test cache '$test_cache'"
fi
# END SYMMETRIC MODE PATH GUARD

[ -d "$dest_parent" ] || die "destination parent '$dest_parent' does not exist"
[ "$(stat -c %d "$(dirname "$stage")")" = "$(stat -c %d "$dest_parent")" ] ||
	die "stage and destination must be on the same filesystem"

# No-clobber, serialized with an atomic mkdir lock like publish.sh.
[ -e "$dest" ] && die "refusing to replace existing test image '$dest'"
lock="$dest.lock"
if ! mkdir "$lock" 2>/dev/null; then
	die "another test materialization for '$image_id' is in progress (lock $lock)"
fi
cleanup() {
	rmdir "$lock" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

[ -e "$dest" ] && die "refusing to replace existing test image '$dest'"

mv -T -- "$stage" "$dest" || die "rename '$stage' to '$dest' failed"
printf 'stage_test: %s\n' "$dest"
