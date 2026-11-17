#!/usr/bin/env bash
# Regression for the early build.sh host-Python preflight.
#
# R4 burned ~8 minutes after image work because host /usr/bin/python3 could
# not import pefile. build.sh must reject that environment before sudo,
# downloads, mkosi or output/cache work. The test uses sentinel fake sudo and
# curl programs to prove they are never invoked, and removes the marked
# preflight block to demonstrate the failing-before behavior with the same
# bounded sentinels.
set -euo pipefail

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)
tmp=$(mktemp -d "${TMPDIR:-/tmp}/bbraft-build-preflight.XXXXXX")
trap 'rm -rf "$tmp"' EXIT
export BBRAFT_PREFLIGHT_SENTINELS="$tmp/sentinels"
mkdir -p "$BBRAFT_PREFLIGHT_SENTINELS"

failures=0
pass() { printf 'PASS %s\n' "$*"; }
fail() {
	printf 'FAIL %s\n' "$*" >&2
	failures=$((failures + 1))
}

mkdir -p "$tmp/fakebin"
# Self-contained pinned Go so the fixture does not depend on the host PATH.
# build_preflight_test only needs the go preflight to pass; sudo is then faked.
go_version=$(sed -n 's/^GO_VERSION="\([^"]*\)"$/\1/p' "$script_dir/pins.sh")
fakegoroot="$tmp/fakegoroot"
mkdir -p "$fakegoroot/bin"
cat >"$tmp/fakebin/go" <<EOF
#!/bin/sh
case "\$1" in
env)
	case "\$2" in
	GOROOT) echo "$fakegoroot" ;;
	GOVERSION) echo "go$go_version" ;;
	*) exit 1 ;;
	esac
	;;
*) exec "$fakegoroot/bin/go" "\$@" ;;
esac
EOF
cat >"$fakegoroot/bin/go" <<EOF
#!/bin/sh
case "\$1" in
env)
	case "\$2" in
	GOROOT) echo "$fakegoroot" ;;
	GOVERSION)
		if [ "\${GOTOOLCHAIN:-auto}" = local ]; then echo "go$go_version"; else echo "go$go_version"; fi
		;;
	*) exit 1 ;;
	esac
	;;
*) exit 0 ;;
esac
EOF
chmod 0755 "$tmp/fakebin/go" "$fakegoroot/bin/go"
cat >"$tmp/fakebin/sudo" <<'EOF'
#!/bin/sh
: >"$BBRAFT_PREFLIGHT_SENTINELS/sudo.called"
exit 1
EOF
cat >"$tmp/fakebin/curl" <<'EOF'
#!/bin/sh
: >"$BBRAFT_PREFLIGHT_SENTINELS/curl.called"
exit 1
EOF
chmod 0755 "$tmp/fakebin/sudo" "$tmp/fakebin/curl"
export PATH="$tmp/fakebin:$PATH"

reset_sentinels() {
	rm -f "$BBRAFT_PREFLIGHT_SENTINELS/sudo.called" "$BBRAFT_PREFLIGHT_SENTINELS/curl.called"
}

run_build() { # <script> <out> <cache> <interpreter>
	MKOSI_INTERPRETER=$4 BBRAFT_IMAGE_OUT="$2" BBRAFT_IMAGE_CACHE="$3" \
		"$1" >"$tmp/build.log" 2>&1
}

# 1. Unsupported override outside /usr is rejected early (mkosi's
# python_binary would fall back to python3, so this is not a supported build).
reset_sentinels
printf '#!/bin/sh\nexit 0\n' >"$tmp/outside-python"
chmod 0755 "$tmp/outside-python"
if run_build "$script_dir/build.sh" "$tmp/out-a" "$tmp/cache-a" "$tmp/outside-python"; then
	fail "build.sh accepted an interpreter outside /usr"
else
	pass "build.sh rejects an interpreter outside /usr"
fi
if grep -q 'outside /usr' "$tmp/build.log"; then
	pass "outside-/usr failure explains the mkosi fallback"
else
	fail "outside-/usr failure is not explained"
fi
if [ ! -e "$BBRAFT_PREFLIGHT_SENTINELS/sudo.called" ] && [ ! -e "$BBRAFT_PREFLIGHT_SENTINELS/curl.called" ]; then
	pass "unsupported interpreter stopped before sudo/download"
else
	fail "unsupported interpreter reached sudo/download"
fi

# 2. An in-/usr interpreter that cannot import pefile is rejected early.
reset_sentinels
if run_build "$script_dir/build.sh" "$tmp/out-b" "$tmp/cache-b" /usr/bin/false; then
	fail "build.sh accepted a Python without pefile"
else
	pass "build.sh rejects a Python without pefile"
fi
if grep -q 'python3-pefile' "$tmp/build.log"; then
	pass "missing-pefile failure names python3-pefile"
else
	fail "missing-pefile failure does not name python3-pefile"
fi
if [ ! -e "$BBRAFT_PREFLIGHT_SENTINELS/sudo.called" ] && [ ! -e "$BBRAFT_PREFLIGHT_SENTINELS/curl.called" ]; then
	pass "missing pefile stopped before sudo/download"
else
	fail "missing pefile reached sudo/download"
fi
if [ ! -e "$tmp/out-b" ] && [ ! -e "$tmp/cache-b" ]; then
	pass "preflight ran before creating output/cache"
else
	fail "preflight created output/cache before failing"
fi

if grep -q 'preserve-env=.*MKOSI_INTERPRETER' "$script_dir/build.sh"; then
	pass "validated MKOSI_INTERPRETER is preserved across sudo"
else
	fail "MKOSI_INTERPRETER is not preserved across sudo"
fi

# 3. Failing-before: remove the marked preflight block. The same fake sudo is
# now reached (sentinel), proving the regression detects a missing preflight.
mkdir -p "$tmp/old"
sed '/^# BEGIN PREFLIGHT$/,/^# END PREFLIGHT$/d' "$script_dir/build.sh" >"$tmp/old/build.sh"
cp "$script_dir/pins.sh" "$script_dir/version" "$tmp/old/"
cp "$script_dir/../go.mod" "$tmp/go.mod"
chmod 0755 "$tmp/old/build.sh"
reset_sentinels
if run_build "$tmp/old/build.sh" "$tmp/out-c" "$tmp/cache-c" /usr/bin/false; then
	fail "preflight-less copy unexpectedly succeeded"
else
	pass "preflight-less copy fails"
fi
if [ -e "$BBRAFT_PREFLIGHT_SENTINELS/sudo.called" ]; then
	pass "preflight-less copy reaches fake sudo (failing-before demonstrated)"
else
	fail "preflight-less copy did not reach fake sudo"
fi
if [ ! -e "$BBRAFT_PREFLIGHT_SENTINELS/curl.called" ]; then
	pass "fake sudo is bounded before download"
else
	fail "preflight-less copy reached fake curl"
fi

if [ "$failures" -ne 0 ]; then
	printf 'build_preflight_test: %d failure(s)\n' "$failures" >&2
	exit 1
fi
printf 'build_preflight_test: all checks passed\n'
