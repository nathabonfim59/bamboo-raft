#!/usr/bin/env bash
# Atomically publish a built image directory without ever replacing an
# existing version. Published version directories are immutable.
#
#   images/publish.sh <stage-dir> <dest-dir>
#
# The stage directory must already pass images/verify.sh and must be on the
# same filesystem as dest (it is, both under images/out). Failure leaves the
# stage and any existing destination untouched.
set -euo pipefail

usage() {
	cat <<'EOF'
usage: images/publish.sh <stage-dir> <dest-dir>
EOF
}

die() {
	printf 'publish: %s\n' "$*" >&2
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
case "$image_id" in
"" | "." | "..")
	die "invalid image id '$image_id'"
	;;
*[!a-z0-9._-]*)
	die "invalid image id '$image_id'; use only lowercase letters, digits, '.', '_' and '-'"
	;;
esac
case "$image_id" in
[!a-z0-9]*)
	die "invalid image id '$image_id'; must start with a lowercase letter or digit"
	;;
esac

manifest_id=$(jq -r '.id // empty' "$stage/manifest.json")
[ "$manifest_id" = "$image_id" ] ||
	die "stage basename '$image_id' does not match manifest id '$manifest_id'"
[ "$(basename "$dest")" = "$image_id" ] ||
	die "destination basename must be '$image_id'"

# BEGIN RELEASE TEST-MARKER GUARD
# Published images are pinned images. Test-only unpinned images carry two
# independent, reserved markers: the id suffix and distribution.snapshot.
# Either one refuses publication; there is no environment or flag bypass.
# Manifests without a distribution object remain publishable, so this stays
# backward compatible with ordinary/legacy images.
case "$image_id" in
*-unpinned-test)
	die "refusing to publish test-only image id '$image_id'"
	;;
esac
manifest_snapshot=$(jq -r '.distribution.snapshot // empty' "$stage/manifest.json")
if [ "$manifest_snapshot" = "unpinned" ]; then
	die "refusing to publish unpinned image '$image_id' (distribution.snapshot=unpinned)"
fi
# END RELEASE TEST-MARKER GUARD

out_root=$(dirname "$dest")
[ -d "$out_root" ] || die "destination parent '$out_root' does not exist"

# No-clobber, and serialize publishers with an atomic mkdir lock.
[ -e "$dest" ] && die "refusing to replace existing published image '$dest'"
lock="$dest.lock"
if ! mkdir "$lock" 2>/dev/null; then
	die "another publish for '$image_id' is in progress (lock $lock)"
fi
cleanup() {
	rmdir "$lock" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

[ -e "$dest" ] && die "refusing to replace existing published image '$dest'"

# Same-filesystem rename: either the whole version directory appears or
# nothing changes.
mv -T -- "$stage" "$dest" || die "rename '$stage' to '$dest' failed"
printf 'publish: %s\n' "$dest"
