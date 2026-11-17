#!/usr/bin/env bash
# Regression tests for images/publish.sh: immutable versions, no-clobber,
# atomic same-filesystem rename, safe image ids and lock serialization.
# Fast and unprivileged; run by `make -C images check`.
set -euo pipefail

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)
publish="$script_dir/publish.sh"
tmp=$(mktemp -d "${TMPDIR:-/tmp}/bbraft-publish.XXXXXX")
trap 'rm -rf "$tmp"' EXIT

failures=0
pass() { printf 'PASS %s\n' "$*"; }
fail() {
	printf 'FAIL %s\n' "$*" >&2
	failures=$((failures + 1))
}

make_stage() { # <dir> <manifest-id>
	mkdir -p "$(dirname "$1")"
	rm -rf "$1"
	mkdir -p "$1"
	printf 'artifact\n' >"$1/payload"
	printf '{"schemaVersion":1,"id":"%s"}\n' "$2" >"$1/manifest.json"
}

make_marked_stage() { # <dir> <manifest-id> <snapshot> <mirror>
	mkdir -p "$(dirname "$1")"
	rm -rf "$1"
	mkdir -p "$1"
	printf 'artifact\n' >"$1/payload"
	jq -n --arg id "$2" --arg snapshot "$3" --arg mirror "$4" \
		'{schemaVersion:1,id:$id,distribution:{snapshot:$snapshot,mirror:$mirror}}' \
		>"$1/manifest.json"
}

out="$tmp/out"
mkdir -p "$out"

# 1. An existing published version is never replaced; its sentinel survives.
make_stage "$tmp/stage/test-v1" test-v1
mkdir -p "$out/test-v1"
printf 'sentinel\n' >"$out/test-v1/sentinel"
if "$publish" "$tmp/stage/test-v1" "$out/test-v1" >/dev/null 2>&1; then
	fail "publish replaced an existing version"
else
	pass "publish refuses an existing version"
fi
if [ -f "$out/test-v1/sentinel" ] && [ ! -f "$out/test-v1/payload" ]; then
	pass "prior sentinel intact and no stage payload leaked"
else
	fail "existing version was modified"
fi
if [ -d "$tmp/stage/test-v1" ]; then
	pass "failed publish leaves the stage intact"
else
	fail "failed publish removed the stage"
fi

# 2. A fresh version publishes atomically and consumes the stage.
rm -rf "$out/test-v1"
if "$publish" "$tmp/stage/test-v1" "$out/test-v1" >/dev/null 2>&1; then
	pass "publish creates a new version"
else
	fail "publish failed for a fresh version"
fi
if [ -f "$out/test-v1/payload" ] && [ ! -e "$tmp/stage/test-v1" ]; then
	pass "fresh version complete and stage renamed"
else
	fail "fresh publish is incomplete"
fi

# 3. Unsafe ids are rejected before any move.
make_stage "$tmp/stage/Bad" Bad
if "$publish" "$tmp/stage/Bad" "$out/Bad" >/dev/null 2>&1; then
	fail "publish accepted an unsafe image id"
else
	pass "publish rejects an unsafe image id"
fi
make_stage "$tmp/stage/test-v2" ../evil
if "$publish" "$tmp/stage/test-v2" "$out/test-v2" >/dev/null 2>&1; then
	fail "publish accepted a mismatched manifest id"
else
	pass "publish rejects a mismatched manifest id"
fi

# 4. The publish lock serializes concurrent publishers.
make_stage "$tmp/stage/locked-v1" locked-v1
mkdir "$out/locked-v1.lock"
if "$publish" "$tmp/stage/locked-v1" "$out/locked-v1" >/dev/null 2>&1; then
	fail "publish ignored an existing lock"
else
	pass "publish respects an existing lock"
fi
rmdir "$out/locked-v1.lock"
if [ ! -e "$out/locked-v1" ]; then
	pass "locked publish left no destination"
else
	fail "locked publish created a destination"
fi

# 5. The reserved test-only id marker is refused even when the manifest has no
# distribution metadata (the id alone is the test/image marker).
make_stage "$tmp/stage/unpinned-id-v1-unpinned-test" unpinned-id-v1-unpinned-test
if "$publish" "$tmp/stage/unpinned-id-v1-unpinned-test" "$out/unpinned-id-v1-unpinned-test" >/dev/null 2>&1; then
	fail "publish accepted the test-only id marker"
else
	pass "publish rejects the test-only id marker"
fi
if [ -d "$tmp/stage/unpinned-id-v1-unpinned-test" ] && [ ! -e "$out/unpinned-id-v1-unpinned-test" ]; then
	pass "rejected test-only id leaves stage intact and no destination"
else
	fail "rejected test-only id modified the stage or destination"
fi

# 6. distribution.snapshot=unpinned is an independent refusal marker.
make_marked_stage "$tmp/stage/unpinned-snapshot-v1" unpinned-snapshot-v1 unpinned http://archive.ubuntu.com/ubuntu
if "$publish" "$tmp/stage/unpinned-snapshot-v1" "$out/unpinned-snapshot-v1" >/dev/null 2>&1; then
	fail "publish accepted distribution.snapshot=unpinned"
else
	pass "publish rejects distribution.snapshot=unpinned"
fi

# 7. Ordinary/legacy manifests (no distribution object) stay publishable.
make_stage "$tmp/stage/legacy-v1" legacy-v1
if "$publish" "$tmp/stage/legacy-v1" "$out/legacy-v1" >/dev/null 2>&1; then
	pass "publish accepts a legacy manifest without distribution metadata"
else
	fail "publish rejected a legacy manifest"
fi

# 8. Pre-fix reproduction: the same publisher without the marked guard accepts
# the unpinned id, proving this regression would catch a removed guard.
mkdir -p "$tmp/old"
sed '/^# BEGIN RELEASE TEST-MARKER GUARD$/,/^# END RELEASE TEST-MARKER GUARD$/d' \
	"$publish" >"$tmp/old/publish.sh"
chmod 0755 "$tmp/old/publish.sh"
mkdir -p "$tmp/old-out"
make_stage "$tmp/stage/prefix-v1-unpinned-test" prefix-v1-unpinned-test
if "$tmp/old/publish.sh" "$tmp/stage/prefix-v1-unpinned-test" "$tmp/old-out/prefix-v1-unpinned-test" >/dev/null 2>&1; then
	pass "pre-fix publisher accepts the unpinned id (failing-before demonstrated)"
else
	fail "pre-fix publisher did not accept the unpinned id"
fi

if [ "$failures" -ne 0 ]; then
	printf 'publish_test: %d failure(s)\n' "$failures" >&2
	exit 1
fi
printf 'publish_test: all checks passed\n'
