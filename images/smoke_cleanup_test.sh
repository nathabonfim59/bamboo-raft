#!/usr/bin/env bash
# Regression for the smoke harness cleanup path.
#
# Pre-fix failure: smoke.sh installed `trap cleanup EXIT INT TERM` before
# stop_qemu was defined. Any early failure after the work directory existed
# (client build, CPU resolver, qemu-img) ran the trap while stop_qemu was
# still undefined: cleanup aborted, the temporary directory and any QEMU
# process survived, and the original error was replaced by a
# "stop_qemu: command not found" diagnostic.
#
# The fix defines the helper before the trap. This test extracts the real
# stop_qemu from smoke.sh and runs it against a live child, proves the
# ordering in the file, and reproduces the pre-fix shape with a fixture that
# loses its cleanup.
set -euo pipefail

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)
smoke="$script_dir/smoke.sh"
tmp=$(mktemp -d "${TMPDIR:-/tmp}/bbraft-smoke-cleanup.XXXXXX")
trap 'rm -rf "$tmp"' EXIT

failures=0
pass() { printf 'PASS %s\n' "$*"; }
fail() {
	printf 'FAIL %s\n' "$*" >&2
	failures=$((failures + 1))
}

# 1. The helper must be defined before the trap that calls it.
stop_line=$(grep -n '^stop_qemu() {' "$smoke" | head -n 1 | cut -d: -f1)
trap_line=$(grep -n '^trap cleanup EXIT INT TERM$' "$smoke" | head -n 1 | cut -d: -f1)
if [ -n "$stop_line" ] && [ -n "$trap_line" ] && [ "$stop_line" -lt "$trap_line" ]; then
	pass "stop_qemu is defined before the cleanup trap (line $stop_line < $trap_line)"
else
	fail "stop_qemu (line ${stop_line:-missing}) is not defined before the trap (line ${trap_line:-missing})"
fi
wait_line=$(grep -n '^wait_qemu_exit() {' "$smoke" | head -n 1 | cut -d: -f1)
boot_line=$(grep -n '^boot_and_check() {' "$smoke" | head -n 1 | cut -d: -f1)
if [ -n "$wait_line" ] && [ -n "$boot_line" ] && [ "$wait_line" -lt "$boot_line" ]; then
	pass "wait_qemu_exit is defined before its caller"
else
	fail "wait_qemu_exit is not defined before boot_and_check"
fi

# 2. The real helper terminates and reaps a live child.
stop_func=$(awk '/^stop_qemu\(\) \{/ { printing = 1 } printing { print } printing && /^}/ { exit }' "$smoke")
if [ -z "$stop_func" ]; then
	fail "could not extract stop_qemu from smoke.sh"
elif bash -c 'eval "$1"; sleep 300 & p=$!; qemu_pid=$p; stop_qemu; if kill -0 "$p" 2>/dev/null; then echo "child survived" >&2; exit 1; fi' _ "$stop_func"; then
	pass "real stop_qemu terminates and reaps its own child"
else
	fail "real stop_qemu left its child running"
fi

# 3. Pre-fix reproduction: trap before the helper. The fixture starts its own
# child, records the PID and exits early; the helper is undefined, so nothing
# stops the child. The parent only checks liveness and kills the stray (it is
# not the child's parent and must not claim to reap it).
cat >"$tmp/pre-fix.sh" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
sleep 300 &
qemu_pid=$!
printf '%s\n' "$qemu_pid" >"$PID_FILE"
cleanup() {
	stop_qemu
	exit 1
}
trap cleanup EXIT INT TERM
# Simulate an early failure (client build/resolver/qemu-img) before the
# helper definitions in the pre-fix file.
false
EOF
chmod 0755 "$tmp/pre-fix.sh"
if PID_FILE="$tmp/pre-fix.pid" "$tmp/pre-fix.sh" >"$tmp/pre-fix.out" 2>&1; then
	fail "pre-fix fixture unexpectedly succeeded"
else
	pass "pre-fix fixture fails as expected"
fi
if grep -q 'stop_qemu: command not found' "$tmp/pre-fix.out"; then
	pass "pre-fix fixture reports the undefined cleanup helper"
else
	fail "pre-fix fixture did not report the undefined helper: $(cat "$tmp/pre-fix.out")"
fi
pre_pid=$(cat "$tmp/pre-fix.pid" 2>/dev/null || true)
if [ -n "$pre_pid" ] && kill -0 "$pre_pid" 2>/dev/null; then
	pass "pre-fix fixture leaves its own child running (failure reproduced)"
else
	fail "pre-fix fixture did not leave its child running"
fi
[ -z "$pre_pid" ] || kill -KILL "$pre_pid" 2>/dev/null || true

# 4. Post-fix shape with the helper first: the fixture launches its child,
# stop_qemu kills and waits on that same child, and no live process remains.
cat >"$tmp/post-fix.sh" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
stop_qemu() {
	if [ -n "${qemu_pid:-}" ] && kill -0 "$qemu_pid" 2>/dev/null; then
		kill -TERM "$qemu_pid" 2>/dev/null || true
		for _ in 1 2 3 4 5 6 7 8 9 10; do
			kill -0 "$qemu_pid" 2>/dev/null || break
			sleep 1
		done
		kill -KILL "$qemu_pid" 2>/dev/null || true
		wait "$qemu_pid" 2>/dev/null || true
	fi
	qemu_pid=""
}
sleep 300 &
qemu_pid=$!
printf '%s\n' "$qemu_pid" >"$PID_FILE"
cleanup() {
	stop_qemu
	exit 1
}
trap cleanup EXIT INT TERM
false
EOF
chmod 0755 "$tmp/post-fix.sh"
if PID_FILE="$tmp/post-fix.pid" "$tmp/post-fix.sh" >"$tmp/post-fix.out" 2>&1; then
	fail "post-fix fixture unexpectedly succeeded"
else
	pass "post-fix fixture still reports the original failure (status 1)"
fi
post_pid=$(cat "$tmp/post-fix.pid" 2>/dev/null || true)
if [ -n "$post_pid" ] && kill -0 "$post_pid" 2>/dev/null; then
	fail "post-fix fixture left its own child running"
else
	pass "post-fix fixture leaves no live child (its stop_qemu reaped it)"
fi

if [ "$failures" -ne 0 ]; then
	printf 'smoke_cleanup_test: %d failure(s)\n' "$failures" >&2
	exit 1
fi
printf 'smoke_cleanup_test: all checks passed\n'
