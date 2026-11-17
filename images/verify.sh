#!/usr/bin/env bash
# Verify a built image directory against its manifest.
#
#   images/verify.sh images/out/ubuntu-24.04-v1
#
# Checks: manifest schema/id, exactly the four fixed artifact filenames, every
# sha256 and size, that packages.json is JSON, and that root.qcow2 is a
# standalone qcow2 whose virtual size matches the manifest. Exit nonzero on
# any mismatch. The node performs the same checks in internal/node (A2).
set -euo pipefail

usage() {
	cat <<'EOF'
usage: images/verify.sh <image-dir>
EOF
}

case "${1:-}" in
--help | -h)
	usage
	exit 0
	;;
"") usage >&2; exit 2 ;;
esac

dir=$1
[ -d "$dir" ] || {
	printf 'verify: %s is not a directory\n' "$dir" >&2
	exit 1
}
manifest="$dir/manifest.json"
[ -s "$manifest" ] || {
	printf 'verify: %s/manifest.json is missing\n' "$dir" >&2
	exit 1
}
for tool in jq sha256sum stat qemu-img; do
	command -v "$tool" >/dev/null 2>&1 || {
		printf 'verify: missing tool %s\n' "$tool" >&2
		exit 1
	}
done

die() {
	printf 'verify: %s\n' "$*" >&2
	exit 1
}

jq empty "$manifest" || die "manifest.json is not valid JSON"
schema=$(jq -r '.schemaVersion' "$manifest")
[ "$schema" = "1" ] || die "unsupported schemaVersion '$schema'"
id=$(jq -r '.id' "$manifest")
[ -n "$id" ] && [ "$id" != "null" ] || die "manifest has no id"
[ "$(basename "$dir")" = "$id" ] || die "directory name does not match manifest id '$id'"

# BEGIN UNPINNED TEST IMAGE CONSISTENCY
# A test-marked image is identified by either reserved marker (the id suffix
# or distribution.snapshot=unpinned). Both must be present together and the
# manifest must record a usable archive mirror; a lone marker is a corrupt
# or hand-edited test image. Ordinary/legacy manifests are unaffected.
manifest_snapshot=$(jq -r '.distribution.snapshot // empty' "$manifest")
test_id=0
case "$id" in
*-unpinned-test) test_id=1 ;;
esac
test_snapshot=0
if [ "$manifest_snapshot" = "unpinned" ]; then
	test_snapshot=1
fi
if [ "$test_id" = 1 ] || [ "$test_snapshot" = 1 ]; then
	[ "$test_id" = 1 ] ||
		die "manifest snapshot is unpinned but id '$id' lacks the test-only suffix"
	[ "$test_snapshot" = 1 ] ||
		die "test-only id '$id' must declare distribution.snapshot=unpinned"
	manifest_mirror=$(jq -r '.distribution.mirror // empty' "$manifest")
	[ -n "$manifest_mirror" ] ||
		die "unpinned test image '$id' has no distribution.mirror"
	case "$manifest_mirror" in
	http://?* | https://?*) ;;
	*) die "unpinned test image '$id' has invalid distribution.mirror '$manifest_mirror'" ;;
	esac
fi
# END UNPINNED TEST IMAGE CONSISTENCY

# Fixed filenames only: no host paths and no surprises in the version directory.
expected=$(printf '%s\n' initrd kernel packages.json root.qcow2)
actual=$(jq -r '.artifacts | keys[]' "$manifest" | sort)
[ "$actual" = "$expected" ] || die "artifact set is
$actual
want
$expected"

for name in kernel initrd root.qcow2 packages.json; do
	file="$dir/$name"
	[ -f "$file" ] || die "missing artifact $name"
	want_size=$(jq -r --arg name "$name" '.artifacts[$name].size' "$manifest")
	got_size=$(stat -c %s "$file")
	[ "$want_size" = "$got_size" ] || die "$name size is $got_size, manifest says $want_size"
	want_sha=$(jq -r --arg name "$name" '.artifacts[$name].sha256' "$manifest")
	got_sha=$(sha256sum "$file" | cut -d' ' -f1)
	[ "$want_sha" = "$got_sha" ] || die "$name sha256 is $got_sha, manifest says $want_sha"
done

[ -s "$dir/kernel" ] || die "kernel is empty"
[ -s "$dir/initrd" ] || die "initrd is empty"
jq empty "$dir/packages.json" || die "packages.json is not valid JSON"

want_virtual=$(jq -r '.root.virtualSizeBytes' "$manifest")
info=$(qemu-img info --output=json "$dir/root.qcow2") || die "qemu-img info failed"
format=$(printf '%s\n' "$info" | jq -r '.format // empty')
[ "$format" = "qcow2" ] || die "root.qcow2 format is '${format:-<unset>}', want qcow2"
got_virtual=$(printf '%s\n' "$info" | jq -r '."virtual-size"')
[ "$want_virtual" = "$got_virtual" ] || die "root.qcow2 virtual size is $got_virtual, manifest says $want_virtual"
backing=$(printf '%s\n' "$info" | jq -r '."backing-filename" // empty')
[ -z "$backing" ] || die "root.qcow2 has backing file '$backing'"
qemu-img check "$dir/root.qcow2" >/dev/null || die "qemu-img check failed"

printf 'verify: %s ok (%s)\n' "$id" "$dir"
