#!/usr/bin/env bash
# Regression for build.sh Go resolution across sudo.
#
# R5 failed because GO=go was preserved across sudo and sudo's secure_path
# selected an old system Go that cannot parse go.mod's `tool` block. build.sh
# must resolve the real toolchain before dropping privileges, require
# GOTOOLCHAIN=local to report the go.mod version, and preserve that absolute
# path. The fake sudo can also simulate a successful probe and root re-exec
# with an older Go on PATH.
set -euo pipefail

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)
tmp=$(mktemp -d "${TMPDIR:-/tmp}/bbraft-go-preflight.XXXXXX")
trap 'rm -rf "$tmp"' EXIT
export BBRAFT_GO_SENTINELS="$tmp/sentinels"
mkdir -p "$BBRAFT_GO_SENTINELS"

failures=0
pass() { printf 'PASS %s\n' "$*"; }
fail() {
	printf 'FAIL %s\n' "$*" >&2
	failures=$((failures + 1))
}

make_toolchain() { # <dir> <local-version> <auto-version>
	local dir=$1 local_version=$2 auto_version=$3
	mkdir -p "$dir/bin" "$dir/goroot/bin"
	cat >"$dir/bin/go" <<EOF
#!/bin/sh
case "\$1" in
env)
	case "\$2" in
	GOROOT) echo "$dir/goroot" ;;
	GOVERSION) echo "$auto_version" ;;
	*) exit 1 ;;
	esac
	;;
*) exec "$dir/goroot/bin/go" "\$@" ;;
esac
EOF
	cat >"$dir/goroot/bin/go" <<EOF
#!/bin/sh
case "\$1" in
env)
	case "\$2" in
	GOROOT) echo "$dir/goroot" ;;
	GOVERSION)
		if [ "\${GOTOOLCHAIN:-auto}" = local ]; then
			echo "$local_version"
		else
			echo "$auto_version"
		fi
		;;
	*) exit 1 ;;
	esac
	;;
*) exit 0 ;;
esac
EOF
	cat >"$dir/bin/sudo" <<'EOF'
#!/bin/sh
if [ "${BBRAFT_SUDO_REEXEC:-0}" = "1" ]; then
	if [ "${1:-}" = "-n" ] && [ "${2:-}" = "true" ]; then
		exit 0
	fi
	if [ "${1:-}" = "-n" ]; then
		shift
		case "${1:-}" in --preserve-env=*) shift ;; esac
		[ "${1:-}" = "--" ] && shift
		printf '%s\n' "${GO:-<unset>}" >"$BBRAFT_GO_SENTINELS/go.reexec"
		: >"$BBRAFT_GO_SENTINELS/sudo.reexec"
		exec env PATH="${BBRAFT_ROOT_FAKEBIN}:$PATH" "$@"
	fi
fi
printf '%s\n' "${GO:-<unset>}" >"$BBRAFT_GO_SENTINELS/go.resolved"
: >"$BBRAFT_GO_SENTINELS/sudo.called"
exit 1
EOF
	cat >"$dir/bin/curl" <<'EOF'
#!/bin/sh
: >"$BBRAFT_GO_SENTINELS/curl.called"
exit 1
EOF
	chmod 0755 "$dir/bin/go" "$dir/goroot/bin/go" "$dir/bin/sudo" "$dir/bin/curl"
}

reset_sentinels() {
	rm -f "$BBRAFT_GO_SENTINELS"/*
}
sentinel_present() { [ -e "$BBRAFT_GO_SENTINELS/$1" ]; }

run_build() { # <script> <path> <out> <cache> [go-value]
	PATH="$2:$PATH" GO="${5:-go}" MKOSI_INTERPRETER=/usr/bin/true \
		BBRAFT_IMAGE_OUT="$3" BBRAFT_IMAGE_CACHE="$4" \
		"$1" >"$tmp/build.log" 2>&1
}

make_toolchain "$tmp/good" go1.27.3 go1.27.3
make_toolchain "$tmp/bad" go1.22.0 go1.22.0
# auto claims a modern toolchain while the executable is old: this is the R5
# class of false success and must be rejected by GOTOOLCHAIN=local.
make_toolchain "$tmp/liar" go1.22.0 go1.27.3

# 1. A bare GO=go resolves through the shim to the real absolute Go and that
# path is what reaches sudo.
reset_sentinels
if run_build "$script_dir/build.sh" "$tmp/good/bin" "$tmp/out-a" "$tmp/cache-a"; then
	fail "build.sh unexpectedly succeeded past fake sudo"
else
	pass "build.sh reaches fake sudo after Go preflight"
fi
if [ -f "$BBRAFT_GO_SENTINELS/go.resolved" ]; then
	got=$(cat "$BBRAFT_GO_SENTINELS/go.resolved")
	if [ "$got" = "$tmp/good/goroot/bin/go" ]; then
		pass "sudo receives the resolved absolute Go ($got)"
	else
		fail "sudo received GO='$got', want $tmp/good/goroot/bin/go"
	fi
else
	fail "fake sudo did not record GO"
fi
if ! sentinel_present curl.called; then
	pass "fake sudo is bounded before download"
else
	fail "build reached fake curl"
fi

# 2. A toolchain whose local GOVERSION does not match go.mod is rejected.
reset_sentinels
if run_build "$script_dir/build.sh" "$tmp/bad/bin" "$tmp/out-b" "$tmp/cache-b"; then
	fail "build.sh accepted Go 1.22 for a go 1.27.3 module"
else
	pass "build.sh rejects the wrong local Go version"
fi
if ! sentinel_present sudo.called; then
	pass "wrong Go rejected before sudo"
else
	fail "wrong Go reached sudo"
fi
if grep -q 'want go1.27.3' "$tmp/build.log"; then
	pass "wrong-Go failure names the required version"
else
	fail "wrong-Go failure does not name the required version"
fi

# 3. GOTOOLCHAIN=auto claiming 1.27.3 while local is 1.22.0 must be rejected.
reset_sentinels
if run_build "$script_dir/build.sh" "$tmp/liar/bin" "$tmp/out-d" "$tmp/cache-d"; then
	fail "build.sh accepted an auto-selected newer toolchain"
else
	pass "build.sh rejects auto-selected newer toolchain"
fi
if ! sentinel_present sudo.called; then
	pass "auto-selected toolchain rejected before sudo"
else
	fail "auto-selected toolchain reached sudo"
fi
if grep -q 'GOTOOLCHAIN=local' "$tmp/build.log"; then
	pass "auto-selected failure names GOTOOLCHAIN=local"
else
	fail "auto-selected failure does not name GOTOOLCHAIN=local"
fi

# 4. Simulated root re-exec: fake sudo probe succeeds, re-exec changes PATH to
# an older Go, and the preserved absolute GO must still be used.
mkdir -p "$tmp/rootbin"
cat >"$tmp/rootbin/id" <<'EOF'
#!/bin/sh
if [ "${1:-}" = "-u" ]; then
	echo 0
else
	exec /usr/bin/id "$@"
fi
EOF
cat >"$tmp/rootbin/go" <<'EOF'
#!/bin/sh
: >"$BBRAFT_GO_SENTINELS/root-go.called"
exit 1
EOF
chmod 0755 "$tmp/rootbin/id" "$tmp/rootbin/go"
for tool in qemu-img apt-get dpkg; do
	cat >"$tmp/rootbin/$tool" <<'EOF'
#!/bin/sh
exit 0
EOF
	chmod 0755 "$tmp/rootbin/$tool"
done

export BBRAFT_ROOT_FAKEBIN="$tmp/rootbin"
export BBRAFT_SUDO_REEXEC=1

for variant in bare explicit; do
	reset_sentinels
	go_value=go
	[ "$variant" = explicit ] && go_value="$tmp/good/bin/go"
	run_build "$script_dir/build.sh" "$tmp/good/bin" \
		"$tmp/out-reexec-$variant" "$tmp/cache-reexec-$variant" "$go_value" || true
	cp "$tmp/build.log" "$tmp/reexec-$variant.log"
	if sentinel_present sudo.reexec; then
		pass "fake sudo probe+re-exec happened ($variant GO)"
	else
		fail "fake sudo re-exec did not run ($variant GO): $(tail -n 3 "$tmp/reexec-$variant.log" | tr '\n' ' ')"
	fi
	got=$(cat "$BBRAFT_GO_SENTINELS/go.reexec" 2>/dev/null || true)
	if [ "$got" = "$tmp/good/goroot/bin/go" ]; then
		pass "root re-exec receives absolute pinned Go ($variant)"
	else
		fail "root re-exec GO='$got', want $tmp/good/goroot/bin/go ($variant)"
	fi
	if ! sentinel_present root-go.called; then
		pass "post-root preflight ignored older PATH go ($variant)"
	else
		fail "post-root preflight used older PATH go ($variant)"
	fi
	if sentinel_present curl.called; then
		pass "post-root work bounded at fake download ($variant)"
	else
		fail "post-root flow did not reach the bounded fake download ($variant): $(tail -n 4 "$tmp/reexec-$variant.log" | tr '\n' ' ')"
	fi
done
unset BBRAFT_SUDO_REEXEC

# 5. Failing-before: removing the marked GO preflight block lets the bare
# GO=go survive into sudo, reproducing the R5 wrong-compiler path.
mkdir -p "$tmp/old"
sed '/^# BEGIN GO PREFLIGHT$/,/^# END GO PREFLIGHT$/d' "$script_dir/build.sh" >"$tmp/old/build.sh"
cp "$script_dir/pins.sh" "$script_dir/version" "$tmp/old/"
cp "$script_dir/../go.mod" "$tmp/go.mod"
chmod 0755 "$tmp/old/build.sh"
reset_sentinels
if run_build "$tmp/old/build.sh" "$tmp/good/bin" "$tmp/out-c" "$tmp/cache-c"; then
	fail "preflight-less copy unexpectedly succeeded"
else
	pass "preflight-less copy fails at fake sudo"
fi
if [ "$(cat "$BBRAFT_GO_SENTINELS/go.resolved" 2>/dev/null || true)" = "go" ]; then
	pass "preflight-less copy preserves bare GO=go into sudo (failing-before demonstrated)"
else
	fail "preflight-less copy did not preserve bare GO=go"
fi
if ! sentinel_present curl.called; then
	pass "preflight-less copy is bounded before download"
else
	fail "preflight-less copy reached fake curl"
fi

if [ "$failures" -ne 0 ]; then
	printf 'go_preflight_test: %d failure(s)\n' "$failures" >&2
	exit 1
fi
printf 'go_preflight_test: all checks passed\n'
