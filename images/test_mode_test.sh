#!/usr/bin/env bash
# Fast regressions for the test-only unpinned archive mode.
#
# No root, downloads, mkosi build or QEMU: a fake sudo/curl capture build.sh's
# mode guards and sudo re-exec environment, fixtures exercise verify.sh,
# stage_test.sh and the Makefile's image selection, and a fake mkosi source
# tree proves mkosi_config_check.py inspects the resolved Config (the real
# pinned parser runs in a VM through build.sh/make mkosi-config-check).
#
# Pre-fix demonstrations remove the marked guards from copies of the scripts
# and show the old behaviour (unpinned images accepted), so the regressions
# fail if a guard disappears.
set -euo pipefail

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)
tmp=$(mktemp -d "${TMPDIR:-/tmp}/bbraft-test-mode.XXXXXX")
trap 'rm -rf "$tmp"' EXIT

failures=0
pass() { printf 'PASS %s\n' "$*"; }
fail() {
	printf 'FAIL %s\n' "$*" >&2
	failures=$((failures + 1))
}

# shellcheck source-path=SCRIPTDIR
# shellcheck source=pins.sh
# shellcheck disable=SC1091
. "$script_dir/pins.sh"

get_test_version() { # <base>; prints the suffixed test id
	(
		export BBRAFT_IMAGE_VERSION=$1
		# shellcheck source=/dev/null
		. "$script_dir/pins.sh"
		printf '%s' "$IMAGE_TEST_VERSION"
	)
}

# 1. Suffixing is idempotent and the reserved marker is not doubled.
base=$(tr -d '[:space:]' <"$script_dir/version")
if [ "$(get_test_version "$base")" = "$base$TEST_UNPINNED_SUFFIX" ]; then
	pass "test id adds the marker to the normal id"
else
	fail "test id does not add the marker to the normal id"
fi
if [ "$(get_test_version "$base$TEST_UNPINNED_SUFFIX")" = "$base$TEST_UNPINNED_SUFFIX" ]; then
	pass "test id suffixing is idempotent (sudo re-exec safe)"
else
	fail "test id suffixing doubles the marker"
fi

# Canonical overlap handles the root and trailing slashes explicitly. The old
# slash-appending predicate missed both cases.
old_paths_overlap() {
	[ "$1" = "$2" ] && return 0
	case "$1/" in "$2/"*) return 0 ;; esac
	case "$2/" in "$1/"*) return 0 ;; esac
	return 1
}
if paths_overlap / /tmp && paths_overlap /tmp /tmp/child && paths_overlap /tmp/ /tmp/child; then
	pass "paths_overlap treats / and trailing slashes as ancestors"
else
	fail "paths_overlap misses a root or trailing-slash ancestor"
fi
if old_paths_overlap / /tmp; then
	fail "pre-fix overlap unexpectedly matched the root ancestor"
else
	pass "pre-fix overlap misses the root ancestor (failing-before demonstrated)"
fi
if old_paths_overlap /tmp/ /tmp/child; then
	fail "pre-fix overlap unexpectedly matched a trailing-slash ancestor"
else
	pass "pre-fix overlap misses a trailing-slash ancestor (failing-before demonstrated)"
fi

# Fake host tools for the build guards: sudo records the re-exec argv and
# exits 0 (exec replaces build.sh), curl records a violation.
mkdir -p "$tmp/fakebin" "$tmp/pymods"
cat >"$tmp/fakebin/sudo" <<'EOF'
#!/bin/sh
if [ "$1" = "-n" ] && [ "$2" = "true" ]; then
	exit 0
fi
: >"$BBRAFT_TEST_SUDO_LOG"
for arg in "$@"; do
	printf '%s\n' "$arg" >>"$BBRAFT_TEST_SUDO_LOG"
done
exit 0
EOF
cat >"$tmp/fakebin/curl" <<'EOF'
#!/bin/sh
: >"$BBRAFT_TEST_CURL_LOG"
exit 1
EOF
printf 'VALUE = 1\n' >"$tmp/pymods/pefile.py"
chmod 0755 "$tmp/fakebin/sudo" "$tmp/fakebin/curl"
export BBRAFT_TEST_SUDO_LOG="$tmp/sudo.log"
export BBRAFT_TEST_CURL_LOG="$tmp/curl.log"
export PATH="$tmp/fakebin:$PATH"
export PYTHONPATH="$tmp/pymods"
export MKOSI_INTERPRETER=/usr/bin/python3

mkdir -p "$tmp/pinned-out" "$tmp/pinned-cache" "$tmp/test-out" "$tmp/test-cache"

# run_build <env...>; writes $tmp/build.log and returns build.sh's status.
run_build() {
	env BBRAFT_IMAGE_OUT="$tmp/pinned-out" BBRAFT_IMAGE_CACHE="$tmp/pinned-cache" \
		BBRAFT_IMAGE_TEST_OUT="$tmp/test-out" BBRAFT_IMAGE_TEST_CACHE="$tmp/test-cache" \
		"$@" "$script_dir/build.sh" >"$tmp/build.log" 2>&1
}

reset_sentinels() {
	rm -f "$BBRAFT_TEST_SUDO_LOG" "$BBRAFT_TEST_CURL_LOG"
}

expect_early_failure() { # <description> <grep> <env...>
	local description=$1 want=$2
	shift 2
	reset_sentinels
	if run_build "$@"; then
		fail "$description unexpectedly succeeded"
	else
		pass "$description is rejected"
	fi
	if grep -q -- "$want" "$tmp/build.log"; then
		pass "$description names the reason"
	else
		fail "$description does not name '$want'"
	fi
	if [ ! -e "$BBRAFT_TEST_SUDO_LOG" ] && [ ! -e "$BBRAFT_TEST_CURL_LOG" ]; then
		pass "$description stops before sudo/download"
	else
		fail "$description reached sudo/download"
	fi
}

expect_early_failure "invalid test opt-in" "must be unset or 1" \
	BBRAFT_IMAGE_TEST_UNPINNED=yes
expect_early_failure "pinned id with the test marker" "contains the test-only marker" \
	BBRAFT_IMAGE_VERSION="$base$TEST_UNPINNED_SUFFIX"
expect_early_failure "test id with a mid-string marker" "does not end with" \
	BBRAFT_IMAGE_TEST_UNPINNED=1 BBRAFT_IMAGE_VERSION="x-unpinned-test-y"
expect_early_failure "test id with a double/mid marker" "more than one test-only marker" \
	BBRAFT_IMAGE_TEST_UNPINNED=1 BBRAFT_IMAGE_VERSION="x-unpinned-test-y-unpinned-test"
long_base=$(printf 'a%.0s' {1..60})
expect_early_failure "overlength test id" "longer than 64 characters" \
	BBRAFT_IMAGE_TEST_UNPINNED=1 BBRAFT_IMAGE_VERSION="$long_base"
expect_early_failure "test cache equal to the pinned cache" "must not equal or overlap" \
	BBRAFT_IMAGE_TEST_UNPINNED=1 BBRAFT_IMAGE_TEST_CACHE="$tmp/pinned-cache"
expect_early_failure "test cache overlapping the pinned cache" "must not equal or overlap" \
	BBRAFT_IMAGE_TEST_UNPINNED=1 BBRAFT_IMAGE_TEST_CACHE="$tmp/pinned-cache/nested"
expect_early_failure "test output equal to the pinned output" "must not equal or overlap" \
	BBRAFT_IMAGE_TEST_UNPINNED=1 BBRAFT_IMAGE_TEST_OUT="$tmp/pinned-out"
expect_early_failure "test output overlapping the pinned output" "must not equal or overlap" \
	BBRAFT_IMAGE_TEST_UNPINNED=1 BBRAFT_IMAGE_TEST_OUT="$tmp/pinned-out/nested"

ln -s "$tmp/pinned-cache" "$tmp/alias-cache"
expect_early_failure "test cache that is a symlink alias of the pinned cache" "must not equal or overlap" \
	BBRAFT_IMAGE_TEST_UNPINNED=1 BBRAFT_IMAGE_TEST_CACHE="$tmp/alias-cache"
ln -s "$tmp/pinned-out" "$tmp/alias-out"
expect_early_failure "test output that is a symlink alias of the pinned output" "must not equal or overlap" \
	BBRAFT_IMAGE_TEST_UNPINNED=1 BBRAFT_IMAGE_TEST_OUT="$tmp/alias-out"
expect_early_failure "test output and cache that overlap each other" "must not equal or overlap" \
	BBRAFT_IMAGE_TEST_UNPINNED=1 BBRAFT_IMAGE_TEST_OUT="$tmp/same-mode" \
	BBRAFT_IMAGE_TEST_CACHE="$tmp/same-mode/cache"

# The reverse direction is guarded too: a later pinned invocation must not
# reuse the test output/cache explicitly, through either cross pairing, or
# through a symlink alias.
isolation_msg="pinned and test-only image roots must not equal or overlap"
expect_early_failure "pinned output equal to the test output" "$isolation_msg" \
	BBRAFT_IMAGE_TEST_OUT="$tmp/pinned-out"
expect_early_failure "pinned cache equal to the test cache" "$isolation_msg" \
	BBRAFT_IMAGE_TEST_CACHE="$tmp/pinned-cache"
expect_early_failure "pinned output equal to the test cache" "$isolation_msg" \
	BBRAFT_IMAGE_TEST_CACHE="$tmp/pinned-out"
expect_early_failure "pinned cache equal to the test output" "$isolation_msg" \
	BBRAFT_IMAGE_TEST_OUT="$tmp/pinned-cache"
ln -s "$tmp/pinned-out" "$tmp/alias-pinned-out"
expect_early_failure "pinned output alias selected as the test output" "$isolation_msg" \
	BBRAFT_IMAGE_TEST_OUT="$tmp/alias-pinned-out"
expect_early_failure "test output and cache overlap in pinned mode" "test-only mode output" \
	BBRAFT_IMAGE_TEST_OUT="$tmp/pinned-own" \
	BBRAFT_IMAGE_TEST_CACHE="$tmp/pinned-own/cache"

# 2. A valid test build reaches sudo and preserves the explicit opt-in and
# disposable roots. A missing preservation would drop the mode after re-exec.
reset_sentinels
if run_build BBRAFT_IMAGE_TEST_UNPINNED=1; then
	pass "valid test build reaches the sudo re-exec"
else
	fail "valid test build failed before the sudo re-exec"
fi
if [ -e "$BBRAFT_TEST_SUDO_LOG" ]; then
	for want in BBRAFT_IMAGE_TEST_UNPINNED BBRAFT_IMAGE_TEST_OUT BBRAFT_IMAGE_TEST_CACHE; do
		if grep -q -- "$want" "$BBRAFT_TEST_SUDO_LOG"; then
			pass "sudo re-exec preserves $want"
		else
			fail "sudo re-exec drops $want"
		fi
	done
else
	fail "valid test build did not reach sudo"
fi
if [ ! -e "$BBRAFT_TEST_CURL_LOG" ]; then
	pass "valid test build stops at sudo before any download"
else
	fail "valid test build downloaded before the sudo re-exec"
fi

# Default normal pinned paths (distinct pinned/test roots) remain valid.
reset_sentinels
if run_build; then
	pass "default pinned paths still reach the sudo re-exec"
else
	fail "default pinned paths failed before the sudo re-exec"
fi
if [ -e "$BBRAFT_TEST_SUDO_LOG" ] && [ ! -e "$BBRAFT_TEST_CURL_LOG" ]; then
	pass "default pinned paths are accepted before sudo/download"
else
	fail "default pinned paths did not reach sudo cleanly"
fi

# Pre-fix: the suffix branch returned before the mid-marker check, so a double
# marker reached the sudo re-exec (and a build). Remove the marked guard from
# a copy and prove the old behaviour, then the current rejection above.
mkdir -p "$tmp/old-build/images"
sed '/^[[:space:]]*# BEGIN DOUBLE TEST MARKER GUARD$/,/^[[:space:]]*# END DOUBLE TEST MARKER GUARD$/d' \
	"$script_dir/build.sh" >"$tmp/old-build/images/build.sh"
cp "$script_dir/pins.sh" "$script_dir/version" "$tmp/old-build/images/"
cp "$script_dir/../go.mod" "$tmp/old-build/go.mod"
chmod 0755 "$tmp/old-build/images/build.sh"
reset_sentinels
if env BBRAFT_IMAGE_OUT="$tmp/pinned-out" BBRAFT_IMAGE_CACHE="$tmp/pinned-cache" \
	BBRAFT_IMAGE_TEST_OUT="$tmp/test-out" BBRAFT_IMAGE_TEST_CACHE="$tmp/test-cache" \
	BBRAFT_IMAGE_TEST_UNPINNED=1 BBRAFT_IMAGE_VERSION="x-unpinned-test-y-unpinned-test" \
	"$tmp/old-build/images/build.sh" >"$tmp/old-build.log" 2>&1; then
	pass "pre-fix build accepts a double marker (failing-before demonstrated)"
else
	fail "pre-fix build rejected the double marker"
fi
if [ -e "$BBRAFT_TEST_SUDO_LOG" ]; then
	pass "pre-fix double marker reached the sudo re-exec"
else
	fail "pre-fix double marker did not reach sudo"
fi

# Pre-fix: the old validate_mode_paths checked cross-mode overlap only when
# the test flag was set, so a pinned invocation could reuse the test output or
# cache. Remove the marked cross guard from a copy and prove both directions
# reached sudo before the fix.
mkdir -p "$tmp/old-cross/images"
sed '/^[[:space:]]*# BEGIN CROSS MODE ISOLATION GUARD$/,/^[[:space:]]*# END CROSS MODE ISOLATION GUARD$/d' \
	"$script_dir/build.sh" >"$tmp/old-cross/images/build.sh"
cp "$script_dir/pins.sh" "$script_dir/version" "$tmp/old-cross/images/"
cp "$script_dir/../go.mod" "$tmp/old-cross/go.mod"
chmod 0755 "$tmp/old-cross/images/build.sh"
old_cross_cases=(
	"pinned output equal to test output|BBRAFT_IMAGE_TEST_OUT=$tmp/pinned-out"
	"pinned cache equal to test cache|BBRAFT_IMAGE_TEST_CACHE=$tmp/pinned-cache"
	"pinned output equal to test cache|BBRAFT_IMAGE_TEST_CACHE=$tmp/pinned-out"
	"pinned cache equal to test output|BBRAFT_IMAGE_TEST_OUT=$tmp/pinned-cache"
	"pinned output alias as test output|BBRAFT_IMAGE_TEST_OUT=$tmp/alias-pinned-out"
)
for old_case in "${old_cross_cases[@]}"; do
	old_description=${old_case%%|*}
	old_assignment=${old_case#*|}
	reset_sentinels
	if env BBRAFT_IMAGE_OUT="$tmp/pinned-out" BBRAFT_IMAGE_CACHE="$tmp/pinned-cache" \
		BBRAFT_IMAGE_TEST_OUT="$tmp/test-out" BBRAFT_IMAGE_TEST_CACHE="$tmp/test-cache" \
		"$old_assignment" "$tmp/old-cross/images/build.sh" >/dev/null 2>&1 &&
		[ -e "$BBRAFT_TEST_SUDO_LOG" ]; then
		pass "pre-fix pinned build accepts $old_description (failing-before demonstrated)"
	else
		fail "pre-fix pinned build rejected $old_description"
	fi
done

# 3. verify.sh: a test-marked image must carry both markers and a usable
# mirror; ordinary/legacy images are unchanged.
mkdir -p "$tmp/fakeqemu"
cat >"$tmp/fakeqemu/qemu-img" <<'EOF'
#!/usr/bin/env bash
case "${1:-}" in
info) printf '{"virtual-size": 1073741824, "format": "qcow2"}\n' ;;
check) printf 'No errors were found on the image.\n' ;;
*) exit 1 ;;
esac
EOF
chmod 0755 "$tmp/fakeqemu/qemu-img"

make_image_fixture() { # <dir> <id> <snapshot> <mirror> <with-distribution 0|1>
	local dir=$1 id=$2 snapshot=$3 mirror=$4 dist=$5
	mkdir -p "$dir"
	printf 'kernel\n' >"$dir/kernel"
	printf 'initrd\n' >"$dir/initrd"
	printf 'qcow2\n' >"$dir/root.qcow2"
	printf '{}\n' >"$dir/packages.json"
	jq -n \
		--arg id "$id" \
		--arg snapshot "$snapshot" \
		--arg mirror "$mirror" \
		--arg sha_kernel "$(sha256sum "$dir/kernel" | cut -d' ' -f1)" \
		--argjson size_kernel "$(stat -c %s "$dir/kernel")" \
		--arg sha_initrd "$(sha256sum "$dir/initrd" | cut -d' ' -f1)" \
		--argjson size_initrd "$(stat -c %s "$dir/initrd")" \
		--arg sha_root "$(sha256sum "$dir/root.qcow2" | cut -d' ' -f1)" \
		--argjson size_root "$(stat -c %s "$dir/root.qcow2")" \
		--arg sha_packages "$(sha256sum "$dir/packages.json" | cut -d' ' -f1)" \
		--argjson size_packages "$(stat -c %s "$dir/packages.json")" \
		--argjson dist "$dist" \
		'{schemaVersion:1,id:$id,root:{filesystem:"ext4",format:"qcow2",diskGiB:1,virtualSizeBytes:1073741824},artifacts:{
			kernel:{sha256:$sha_kernel,size:$size_kernel},
			initrd:{sha256:$sha_initrd,size:$size_initrd},
			"root.qcow2":{sha256:$sha_root,size:$size_root},
			"packages.json":{sha256:$sha_packages,size:$size_packages}}}
		+ (if $dist == 1 then {distribution:{snapshot:$snapshot,mirror:$mirror}} else {} end)' \
		>"$dir/manifest.json"
}

verify_fixture() { # <dir>; uses the fake qemu-img
	PATH="$tmp/fakeqemu:$PATH" "$script_dir/verify.sh" "$1" >"$tmp/verify.log" 2>&1
}

make_image_fixture "$tmp/img-valid/test-v1-unpinned-test" test-v1-unpinned-test unpinned http://archive.ubuntu.com/ubuntu 1
if verify_fixture "$tmp/img-valid/test-v1-unpinned-test"; then
	pass "verify accepts a consistent unpinned test image"
else
	fail "verify rejected a consistent unpinned test image"
fi

make_image_fixture "$tmp/img-no-mirror/test-v1-unpinned-test" test-v1-unpinned-test unpinned "" 1
if verify_fixture "$tmp/img-no-mirror/test-v1-unpinned-test"; then
	fail "verify accepted an unpinned test image without a mirror"
else
	pass "verify rejects an unpinned test image without a mirror"
fi
if grep -q 'no distribution.mirror' "$tmp/verify.log"; then
	pass "missing-mirror failure names distribution.mirror"
else
	fail "missing-mirror failure does not name distribution.mirror"
fi

make_image_fixture "$tmp/img-bad-mirror/test-v1-unpinned-test" test-v1-unpinned-test unpinned file:///srv/mirror 1
if verify_fixture "$tmp/img-bad-mirror/test-v1-unpinned-test"; then
	fail "verify accepted a non-http(s) test mirror"
else
	pass "verify rejects a non-http(s) test mirror"
fi

make_image_fixture "$tmp/img-id-only/test-v1-unpinned-test" test-v1-unpinned-test 20261108T182718Z http://archive.ubuntu.com/ubuntu 1
if verify_fixture "$tmp/img-id-only/test-v1-unpinned-test"; then
	fail "verify accepted a test id with a pinned snapshot"
else
	pass "verify rejects a test id with a pinned snapshot"
fi
make_image_fixture "$tmp/img-snap-only/test-v1" test-v1 unpinned http://archive.ubuntu.com/ubuntu 1
if verify_fixture "$tmp/img-snap-only/test-v1"; then
	fail "verify accepted an unpinned snapshot without the test id suffix"
else
	pass "verify rejects an unpinned snapshot without the test id suffix"
fi

make_image_fixture "$tmp/img-legacy/test-v1" test-v1 "" "" 0
if verify_fixture "$tmp/img-legacy/test-v1"; then
	pass "verify still accepts a legacy manifest without distribution metadata"
else
	fail "verify rejected a legacy manifest"
fi
make_image_fixture "$tmp/img-pinned/test-v1" test-v1 20261108T182718Z "" 1
if verify_fixture "$tmp/img-pinned/test-v1"; then
	pass "verify still accepts an ordinary pinned manifest"
else
	fail "verify rejected an ordinary pinned manifest"
fi

# Pre-fix: verify.sh without the marked consistency block accepts the
# unpinned image that has no mirror.
mkdir -p "$tmp/old-verify"
sed '/^# BEGIN UNPINNED TEST IMAGE CONSISTENCY$/,/^# END UNPINNED TEST IMAGE CONSISTENCY$/d' \
	"$script_dir/verify.sh" >"$tmp/old-verify/verify.sh"
chmod 0755 "$tmp/old-verify/verify.sh"
if PATH="$tmp/fakeqemu:$PATH" "$tmp/old-verify/verify.sh" "$tmp/img-no-mirror/test-v1-unpinned-test" >/dev/null 2>&1; then
	pass "pre-fix verifier accepts an unpinned image without a mirror (failing-before demonstrated)"
else
	fail "pre-fix verifier did not accept the unpinned image"
fi

# 4. stage_test.sh: only a suffixed, unpinned, mirrored image may move, and
# only under IMAGE_TEST_OUT.
mkdir -p "$tmp/stage-root" "$tmp/other-out"
make_stage_fixture() { # <dir> <id> <snapshot> <mirror>
	mkdir -p "$1"
	printf 'payload\n' >"$1/payload"
	jq -n --arg id "$2" --arg snapshot "$3" --arg mirror "$4" \
		'{schemaVersion:1,id:$id,distribution:{snapshot:$snapshot,mirror:$mirror}}' \
		>"$1/manifest.json"
}
run_stage() { # <test-out> <stage> <dest> [pinned-cache] [test-cache]
	env BBRAFT_IMAGE_OUT="$tmp/pinned-out" BBRAFT_IMAGE_CACHE="${4:-$tmp/pinned-cache}" \
		BBRAFT_IMAGE_TEST_OUT="$1" BBRAFT_IMAGE_TEST_CACHE="${5:-$tmp/test-cache}" \
		"$script_dir/stage_test.sh" "$2" "$3" >"$tmp/stage.log" 2>&1
}
make_stage_fixture "$tmp/stage-root/test-v1-unpinned-test" test-v1-unpinned-test unpinned http://archive.ubuntu.com/ubuntu
if run_stage "$tmp/test-out" "$tmp/stage-root/test-v1-unpinned-test" "$tmp/test-out/test-v1-unpinned-test"; then
	pass "stage_test materializes a consistent test image"
else
	fail "stage_test rejected a consistent test image"
fi
if [ -f "$tmp/test-out/test-v1-unpinned-test/payload" ] && [ ! -e "$tmp/stage-root/test-v1-unpinned-test" ]; then
	pass "stage_test consumed the stage atomically"
else
	fail "stage_test did not move the stage"
fi

# stage_test applies the same safe id grammar and 64-byte bound as
# build.sh/publish.sh.
mkdir -p "$tmp/stage-root/grammar"
make_stage_fixture "$tmp/stage-root/grammar/X-v1-unpinned-test" X-v1-unpinned-test unpinned http://archive.ubuntu.com/ubuntu
if run_stage "$tmp/test-out" "$tmp/stage-root/grammar/X-v1-unpinned-test" "$tmp/test-out/X-v1-unpinned-test"; then
	fail "stage_test accepted an id with an uppercase character"
else
	pass "stage_test rejects an unsafe id grammar"
fi
long_id=$(printf 'a%.0s' {1..60})"$TEST_UNPINNED_SUFFIX"
make_stage_fixture "$tmp/stage-root/grammar/$long_id" "$long_id" unpinned http://archive.ubuntu.com/ubuntu
if run_stage "$tmp/test-out" "$tmp/stage-root/grammar/$long_id" "$tmp/test-out/$long_id"; then
	fail "stage_test accepted an overlength id"
else
	pass "stage_test rejects an id longer than 64 characters"
fi

make_stage_fixture "$tmp/stage-root/plain-v1" plain-v1 unpinned http://archive.ubuntu.com/ubuntu
if run_stage "$tmp/test-out" "$tmp/stage-root/plain-v1" "$tmp/test-out/plain-v1"; then
	fail "stage_test accepted an id without the test suffix"
else
	pass "stage_test requires the test id suffix"
fi
make_stage_fixture "$tmp/stage-root/pinned-v1-unpinned-test" pinned-v1-unpinned-test 20261108T182718Z http://archive.ubuntu.com/ubuntu
if run_stage "$tmp/test-out" "$tmp/stage-root/pinned-v1-unpinned-test" "$tmp/test-out/pinned-v1-unpinned-test"; then
	fail "stage_test accepted a pinned snapshot under a test id"
else
	pass "stage_test requires distribution.snapshot=unpinned"
fi
make_stage_fixture "$tmp/stage-root/no-mirror-v1-unpinned-test" no-mirror-v1-unpinned-test unpinned ""
if run_stage "$tmp/test-out" "$tmp/stage-root/no-mirror-v1-unpinned-test" "$tmp/test-out/no-mirror-v1-unpinned-test"; then
	fail "stage_test accepted a test image without a mirror"
else
	pass "stage_test requires a mirror"
fi
make_stage_fixture "$tmp/stage-root/other-v1-unpinned-test" other-v1-unpinned-test unpinned http://archive.ubuntu.com/ubuntu
if run_stage "$tmp/test-out" "$tmp/stage-root/other-v1-unpinned-test" "$tmp/other-out/other-v1-unpinned-test"; then
	fail "stage_test accepted a destination outside IMAGE_TEST_OUT"
else
	pass "stage_test restricts destinations to IMAGE_TEST_OUT"
fi
make_stage_fixture "$tmp/stage-root/release-v1-unpinned-test" release-v1-unpinned-test unpinned http://archive.ubuntu.com/ubuntu
if run_stage "$tmp/pinned-out" "$tmp/stage-root/release-v1-unpinned-test" "$tmp/pinned-out/release-v1-unpinned-test"; then
	fail "stage_test wrote into the release output"
else
	pass "stage_test refuses the release output even when named as IMAGE_TEST_OUT"
fi

# Symmetric canonical isolation: an ancestor of the release output, the
# release output itself, the pinned cache and symlink aliases are all refused.
mkdir -p "$tmp/ancestor/pinned-out" "$tmp/ancestor/pinned-cache" "$tmp/ancestor/stage"
make_stage_fixture "$tmp/ancestor/stage/ancestor-v1-unpinned-test" ancestor-v1-unpinned-test unpinned http://archive.ubuntu.com/ubuntu
if env BBRAFT_IMAGE_OUT="$tmp/ancestor/pinned-out" BBRAFT_IMAGE_CACHE="$tmp/ancestor/pinned-cache" \
	BBRAFT_IMAGE_TEST_OUT="$tmp/ancestor" BBRAFT_IMAGE_TEST_CACHE="$tmp/ancestor/test-cache" \
	"$script_dir/stage_test.sh" "$tmp/ancestor/stage/ancestor-v1-unpinned-test" \
	"$tmp/ancestor/ancestor-v1-unpinned-test" >"$tmp/stage.log" 2>&1; then
	fail "stage_test accepted an ancestor of the release output"
else
	pass "stage_test rejects an ancestor of the release output"
fi

mkdir -p "$tmp/equal" "$tmp/equal/stage"
make_stage_fixture "$tmp/equal/stage/equal-v1-unpinned-test" equal-v1-unpinned-test unpinned http://archive.ubuntu.com/ubuntu
if env BBRAFT_IMAGE_OUT="$tmp/equal" BBRAFT_IMAGE_CACHE="$tmp/pinned-cache" \
	BBRAFT_IMAGE_TEST_OUT="$tmp/equal" BBRAFT_IMAGE_TEST_CACHE="$tmp/test-cache" \
	"$script_dir/stage_test.sh" "$tmp/equal/stage/equal-v1-unpinned-test" \
	"$tmp/equal/equal-v1-unpinned-test" >"$tmp/stage.log" 2>&1; then
	fail "stage_test accepted a test output equal to the release output"
else
	pass "stage_test rejects a test output equal to the release output"
fi

mkdir -p "$tmp/cross-cache/stage"
make_stage_fixture "$tmp/cross-cache/stage/cross-v1-unpinned-test" cross-v1-unpinned-test unpinned http://archive.ubuntu.com/ubuntu
if env BBRAFT_IMAGE_OUT="$tmp/pinned-out" BBRAFT_IMAGE_CACHE="$tmp/cross-cache" \
	BBRAFT_IMAGE_TEST_OUT="$tmp/cross-cache" BBRAFT_IMAGE_TEST_CACHE="$tmp/test-cache" \
	"$script_dir/stage_test.sh" "$tmp/cross-cache/stage/cross-v1-unpinned-test" \
	"$tmp/cross-cache/cross-v1-unpinned-test" >"$tmp/stage.log" 2>&1; then
	fail "stage_test accepted the pinned cache as its output"
else
	pass "stage_test rejects a test output that is the pinned cache"
fi

ln -s "$tmp/pinned-cache" "$tmp/alias-pinned-cache"
mkdir -p "$tmp/alias-stage"
make_stage_fixture "$tmp/alias-stage/alias-v1-unpinned-test" alias-v1-unpinned-test unpinned http://archive.ubuntu.com/ubuntu
if env BBRAFT_IMAGE_OUT="$tmp/pinned-out" BBRAFT_IMAGE_CACHE="$tmp/pinned-cache" \
	BBRAFT_IMAGE_TEST_OUT="$tmp/alias-pinned-cache" BBRAFT_IMAGE_TEST_CACHE="$tmp/test-cache" \
	"$script_dir/stage_test.sh" "$tmp/alias-stage/alias-v1-unpinned-test" \
	"$tmp/alias-pinned-cache/alias-v1-unpinned-test" >"$tmp/stage.log" 2>&1; then
	fail "stage_test accepted a symlink alias of the pinned cache"
else
	pass "stage_test rejects a symlink alias of the pinned cache"
fi

mkdir -p "$tmp/same-mode/stage"
make_stage_fixture "$tmp/same-mode/stage/same-v1-unpinned-test" same-v1-unpinned-test unpinned http://archive.ubuntu.com/ubuntu
if env BBRAFT_IMAGE_OUT="$tmp/pinned-out" BBRAFT_IMAGE_CACHE="$tmp/pinned-cache" \
	BBRAFT_IMAGE_TEST_OUT="$tmp/same-mode" BBRAFT_IMAGE_TEST_CACHE="$tmp/same-mode/cache" \
	"$script_dir/stage_test.sh" "$tmp/same-mode/stage/same-v1-unpinned-test" \
	"$tmp/same-mode/same-v1-unpinned-test" >"$tmp/stage.log" 2>&1; then
	fail "stage_test accepted overlapping test output and cache roots"
else
	pass "stage_test rejects overlapping test output and cache roots"
fi

# Pre-fix: removing the marked symmetric guard reproduces the one-way hole;
# the old stager moves an image when IMAGE_TEST_OUT is an ancestor of the
# release output, and also when it is the pinned cache.
mkdir -p "$tmp/old-stage"
sed '/^# BEGIN SYMMETRIC MODE PATH GUARD$/,/^# END SYMMETRIC MODE PATH GUARD$/d' \
	"$script_dir/stage_test.sh" >"$tmp/old-stage/stage_test.sh"
cp "$script_dir/pins.sh" "$script_dir/version" "$tmp/old-stage/"
chmod 0755 "$tmp/old-stage/stage_test.sh"
mkdir -p "$tmp/old-ancestor/pinned-out" "$tmp/old-ancestor/pinned-cache" "$tmp/old-ancestor/stage"
make_stage_fixture "$tmp/old-ancestor/stage/old-v1-unpinned-test" old-v1-unpinned-test unpinned http://archive.ubuntu.com/ubuntu
if env BBRAFT_IMAGE_OUT="$tmp/old-ancestor/pinned-out" BBRAFT_IMAGE_CACHE="$tmp/old-ancestor/pinned-cache" \
	BBRAFT_IMAGE_TEST_OUT="$tmp/old-ancestor" BBRAFT_IMAGE_TEST_CACHE="$tmp/old-ancestor/test-cache" \
	"$tmp/old-stage/stage_test.sh" "$tmp/old-ancestor/stage/old-v1-unpinned-test" \
	"$tmp/old-ancestor/old-v1-unpinned-test" >/dev/null 2>&1; then
	pass "pre-fix stager accepts an ancestor of the release output (failing-before demonstrated)"
else
	fail "pre-fix stager rejected the ancestor"
fi
mkdir -p "$tmp/old-cross/stage"
make_stage_fixture "$tmp/old-cross/stage/old-cross-v1-unpinned-test" old-cross-v1-unpinned-test unpinned http://archive.ubuntu.com/ubuntu
if env BBRAFT_IMAGE_OUT="$tmp/pinned-out" BBRAFT_IMAGE_CACHE="$tmp/old-cross" \
	BBRAFT_IMAGE_TEST_OUT="$tmp/old-cross" BBRAFT_IMAGE_TEST_CACHE="$tmp/test-cache" \
	"$tmp/old-stage/stage_test.sh" "$tmp/old-cross/stage/old-cross-v1-unpinned-test" \
	"$tmp/old-cross/old-cross-v1-unpinned-test" >/dev/null 2>&1; then
	pass "pre-fix stager accepts the pinned cache as output (failing-before demonstrated)"
else
	fail "pre-fix stager rejected the pinned cache"
fi

# Pre-fix: the grammar guard is new; the old stager accepted an unsafe id that
# otherwise carried every required marker.
sed '/^# BEGIN TEST ID GRAMMAR GUARD$/,/^# END TEST ID GRAMMAR GUARD$/d' \
	"$script_dir/stage_test.sh" >"$tmp/old-stage/stage_test.sh.nogrammar"
mv "$tmp/old-stage/stage_test.sh.nogrammar" "$tmp/old-stage/stage_test.sh"
chmod 0755 "$tmp/old-stage/stage_test.sh"
mkdir -p "$tmp/old-grammar/stage"
make_stage_fixture "$tmp/old-grammar/stage/Bad-v1-unpinned-test" Bad-v1-unpinned-test unpinned http://archive.ubuntu.com/ubuntu
if env BBRAFT_IMAGE_OUT="$tmp/pinned-out" BBRAFT_IMAGE_CACHE="$tmp/pinned-cache" \
	BBRAFT_IMAGE_TEST_OUT="$tmp/old-grammar" BBRAFT_IMAGE_TEST_CACHE="$tmp/test-cache" \
	"$tmp/old-stage/stage_test.sh" "$tmp/old-grammar/stage/Bad-v1-unpinned-test" \
	"$tmp/old-grammar/Bad-v1-unpinned-test" >/dev/null 2>&1; then
	pass "pre-fix stager accepts an unsafe id grammar (failing-before demonstrated)"
else
	fail "pre-fix stager rejected the unsafe id"
fi
make_stage_fixture "$tmp/stage-root/locked-v1-unpinned-test" locked-v1-unpinned-test unpinned http://archive.ubuntu.com/ubuntu
mkdir -p "$tmp/test-out/locked-v1-unpinned-test"
printf 'sentinel\n' >"$tmp/test-out/locked-v1-unpinned-test/sentinel"
if run_stage "$tmp/test-out" "$tmp/stage-root/locked-v1-unpinned-test" "$tmp/test-out/locked-v1-unpinned-test"; then
	fail "stage_test replaced an existing test image"
else
	pass "stage_test refuses an existing destination"
fi
if [ -f "$tmp/test-out/locked-v1-unpinned-test/sentinel" ] &&
	[ -f "$tmp/stage-root/locked-v1-unpinned-test/payload" ]; then
	pass "failed stage_test leaves destination and stage intact"
else
	fail "failed stage_test modified destination or stage"
fi

# 5. Makefile selection: the test path is a shell variable sourced in the
# recipe, so quotes and shell-looking text are argv data, never code. A fake
# smoke binary records its exact argv and sentinels prove nothing executed.
cat >"$tmp/fake-smoke" <<'EOF'
#!/bin/sh
: >"$FAKE_SMOKE_ARGV"
for arg in "$@"; do
	printf '%s\n' "$arg" >>"$FAKE_SMOKE_ARGV"
done
exit 0
EOF
chmod 0755 "$tmp/fake-smoke"
export FAKE_SMOKE_ARGV="$tmp/smoke.argv"
pwn_backtick="$tmp/pwn-backtick"
pwn_sub="$tmp/pwn-sub"
pwn_semi="$tmp/pwn-semi"
nasty_out="$tmp/nasty out 'single' \"double\" \$HOME \`touch $pwn_backtick\` \$(touch $pwn_sub) ; touch $pwn_semi"
expected_test_dir="$nasty_out/ubuntu-24.04-v1-unpinned-test"
if env BBRAFT_IMAGE_TEST_OUT="$nasty_out" BBRAFT_IMAGE_VERSION=ubuntu-24.04-v1 \
	make -C "$script_dir" smoke-unpinned-test SMOKE="$tmp/fake-smoke" >"$tmp/smoke-make.log" 2>&1; then
	pass "smoke-unpinned-test runs with a shell-metacharacter path"
else
	fail "smoke-unpinned-test failed with a shell-metacharacter path"
fi
if [ -f "$FAKE_SMOKE_ARGV" ]; then
	mapfile -t smoke_args <"$FAKE_SMOKE_ARGV"
	if [ "${#smoke_args[@]}" -eq 2 ] &&
		[ "${smoke_args[0]}" = "--image-dir" ] &&
		[ "${smoke_args[1]}" = "$expected_test_dir" ]; then
		pass "smoke-unpinned-test passes the test path as exactly one argv value"
	else
		fail "smoke-unpinned-test argv is not the exact --image-dir data"
	fi
else
	fail "fake smoke was not invoked"
fi
if [ ! -e "$pwn_backtick" ] && [ ! -e "$pwn_sub" ] && [ ! -e "$pwn_semi" ]; then
	pass "smoke-unpinned-test executed no injected sentinel"
else
	fail "smoke-unpinned-test executed a sentinel from the path"
fi

# Pre-fix: Make-time interpolation substituted the path into quoted recipe
# text, where the shell performed command substitution inside the quotes.
cat >"$tmp/old.mk" <<'EOF'
SHELL := /bin/bash
IMAGE_TEST_DIR := $(shell . ./pins.sh; printf '%s' "$$IMAGE_TEST_DIR")
all:
	"$(SMOKE)" --image-dir "$(IMAGE_TEST_DIR)"
EOF
old_pwn="$tmp/old-pwn"
old_out="$tmp/old-nasty \$(touch $old_pwn)"
if env BBRAFT_IMAGE_TEST_OUT="$old_out" BBRAFT_IMAGE_VERSION=ubuntu-24.04-v1 \
	make -C "$script_dir" -f "$tmp/old.mk" all SMOKE="$tmp/fake-smoke" >/dev/null 2>&1; then
	:
fi
if [ -e "$old_pwn" ]; then
	pass "pre-fix Make interpolation executes a substituted sentinel (failing-before demonstrated)"
else
	fail "pre-fix interpolation did not reproduce the injection"
fi

: >"$FAKE_SMOKE_ARGV"
if env -u BBRAFT_IMAGE_TEST_OUT -u BBRAFT_IMAGE_TEST_CACHE -u BBRAFT_IMAGE_VERSION \
	make -C "$script_dir" smoke SMOKE="$tmp/fake-smoke" >/dev/null 2>&1 &&
	[ ! -s "$FAKE_SMOKE_ARGV" ]; then
	pass "default smoke keeps the pinned selection (no explicit test path)"
else
	fail "default smoke changed its selection"
fi
image_recipe=$(env -u BBRAFT_IMAGE_TEST_OUT -u BBRAFT_IMAGE_TEST_CACHE -u BBRAFT_IMAGE_VERSION \
	make -C "$script_dir" -n image-unpinned-test 2>/dev/null)
case "$image_recipe" in
*BBRAFT_IMAGE_TEST_UNPINNED=1*)
	pass "image-unpinned-test sets the explicit opt-in"
	;;
*)
	fail "image-unpinned-test does not set the explicit opt-in"
	;;
esac

# 6. mkosi_config_check.py inspects the resolved Config, not argv text. A fake
# pinned source tree returns Config objects; the real pinned parser is
# exercised by build.sh and make mkosi-config-check in a VM.
mkdir -p "$tmp/fake-mkosi/mkosi" "$tmp/fake-mkosi/bin"
cat >"$tmp/fake-mkosi/mkosi/__init__.py" <<'EOF'
def check_workspace_directory(image):
	pass
EOF
cat >"$tmp/fake-mkosi/mkosi/resources.py" <<'EOF'
from pathlib import Path
HERE = Path(__file__).parent
EOF
cat >"$tmp/fake-mkosi/mkosi/config.py" <<'EOF'
import os


class Image:
	def __init__(self, argv):
		self.image = "main"
		self.snapshot = None
		self.mirror = None
		self.build_sources = []
		i = 0
		while i < len(argv):
			if argv[i] == "--snapshot":
				value = argv[i + 1]
				if value and os.environ.get("FAKE_MKOSI_PINNED_BROKEN") == "1":
					self.snapshot = "broken"
				else:
					self.snapshot = value or None
				i += 2
			elif argv[i] == "--mirror":
				self.mirror = argv[i + 1] or None
				i += 2
			else:
				i += 1

	def workspace_dir_or_default(self):
		return "/tmp/mkosi-workspace"


def parse_config(argv, *, resources):
	return None, None, (Image(argv),)
EOF
printf '#!/bin/sh\nexit 0\n' >"$tmp/fake-mkosi/bin/mkosi"
chmod 0755 "$tmp/fake-mkosi/bin/mkosi"
mkosi_check="$script_dir/mkosi_config_check.py"
if python3 "$mkosi_check" --mkosi "$tmp/fake-mkosi/bin/mkosi" \
	--expect-snapshot none --expect-mirror http://archive.ubuntu.com/ubuntu -- \
	--snapshot "" --mirror http://archive.ubuntu.com/ubuntu build >"$tmp/mkosi.log" 2>&1; then
	pass "mkosi check accepts a reset snapshot and mirror"
else
	fail "mkosi check rejected a reset snapshot and mirror"
fi
if grep -q 'snapshot=none mirror=http://archive.ubuntu.com/ubuntu' "$tmp/mkosi.log"; then
	pass "mkosi check reports the resolved snapshot and mirror"
else
	fail "mkosi check does not report resolved values"
fi
if python3 "$mkosi_check" --mkosi "$tmp/fake-mkosi/bin/mkosi" \
	--expect-snapshot 20261108T182718Z -- \
	--snapshot "" build >/dev/null 2>&1; then
	fail "mkosi check ignored a wrong resolved snapshot"
else
	pass "mkosi check rejects a wrong resolved snapshot"
fi
if python3 "$mkosi_check" --mkosi "$tmp/fake-mkosi/bin/mkosi" -- \
	--snapshot "" build >/dev/null 2>&1; then
	pass "mkosi check keeps expectations optional for existing callers"
else
	fail "mkosi check failed without expectations"
fi

# The Make recipe must stop when the first (pinned) parser check fails, even
# when the second (test) check would pass. A fake pinned Config that reports a
# wrong snapshot reproduces the first-fails/second-would-pass ordering.
if env FAKE_MKOSI_PINNED_BROKEN=1 \
	make -C "$script_dir" mkosi-config-check MKOSI_BIN="$tmp/fake-mkosi/bin/mkosi" \
	>"$tmp/mkosi-make.log" 2>&1; then
	fail "mkosi-config-check masked the first parser failure"
else
	pass "mkosi-config-check propagates the first parser failure"
fi
if grep -q 'resolved snapshot broken' "$tmp/mkosi-make.log"; then
	pass "mkosi-config-check reports the pinned parser failure"
else
	fail "mkosi-config-check does not report the pinned parser failure"
fi
if make -C "$script_dir" mkosi-config-check MKOSI_BIN="$tmp/fake-mkosi/bin/mkosi" \
	>"$tmp/mkosi-make-ok.log" 2>&1; then
	pass "mkosi-config-check passes when both modes resolve correctly"
else
	fail "mkosi-config-check failed for correct resolved configs"
fi

# Pre-fix: the old recipe joined the two checks with ';', so the second
# command's success made make return zero after the first failed.
cat >"$tmp/old-mask.mk" <<'EOF'
all:
	@if true; then /bin/false; echo second-ok; else echo skipped; fi
EOF
if make -f "$tmp/old-mask.mk" all >/dev/null 2>&1; then
	pass "pre-fix ';' recipe masks a first failure (failing-before demonstrated)"
else
	fail "pre-fix recipe did not reproduce the masking"
fi

if [ "$failures" -ne 0 ]; then
	printf 'test_mode_test: %d failure(s)\n' "$failures" >&2
	exit 1
fi
printf 'test_mode_test: all checks passed\n'
