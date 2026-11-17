#!/usr/bin/env bash
# Regression for kvm_gate.sh using a fake `go` binary. The gate must fail on:
# an "ok" summary with no matching tests, a partial match, and a compile
# failure; and pass only when every required top-level name is listed
# exactly. The fake go never compiles anything, so the test is bounded.
set -euo pipefail

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)
gate="$script_dir/kvm_gate.sh"
tmp=$(mktemp -d "${TMPDIR:-/tmp}/bbraft-kvm-gate-test.XXXXXX")
trap 'rm -rf "$tmp"' EXIT

failures=0
pass() {
	printf 'PASS %s\n' "$*"
}
fail() {
	printf 'FAIL %s\n' "$*" >&2
	failures=$((failures + 1))
}

# fake go: `test -list` output comes from $FAKE_GO_LIST, exit status from
# $FAKE_GO_RC (default 0); stderr from $FAKE_GO_ERR (for compile failures).
fake_go="$tmp/go"
cat >"$fake_go" <<'EOF'
#!/usr/bin/env bash
# Records how it was called, then answers -list from fixtures.
printf '%s\n' "$*" >"$FAKE_GO_CALL"
[ -n "${FAKE_GO_ERR:-}" ] && printf '%s\n' "$FAKE_GO_ERR" >&2
if [ -n "${FAKE_GO_LIST_FILE:-}" ]; then
	cat "$FAKE_GO_LIST_FILE"
fi
exit "${FAKE_GO_RC:-0}"
EOF
chmod +x "$fake_go"

# An empty listing (nothing matches) must fail the gate.
if FAKE_GO_CALL="$tmp/call" FAKE_GO_LIST_FILE=/dev/null GO="$fake_go" \
	"$gate" ./internal/node TestAFullSet >/dev/null 2>&1; then
	fail "gate passed with an empty test listing"
else
	pass "gate fails when no test is listed at all"
fi

run_gate() { # <names...>
	FAKE_GO_CALL="$tmp/call" FAKE_GO_LIST_FILE="$tmp/list" GO="$fake_go" \
		"$gate" ./internal/node "$@" >"$tmp/out" 2>"$tmp/err"
}

# All required names present: pass.
cat >"$tmp/list" <<'EOF'
TestKVMConformanceDirect
TestKVMConformanceLink
TestUnrelated
ok  	github.com/nathabonfim59/bamboo-raft/internal/node	0.01s
EOF
if run_gate TestKVMConformanceDirect TestKVMConformanceLink; then
	pass "full presence passes"
else
	fail "full presence rejected: $(cat "$tmp/err")"
fi
grep -q 'test -list Test. ./internal/node' "$tmp/call" ||
	fail "gate did not run go test -list on the package"

# The go ok-summary alone (no matching tests) must not pass.
cat >"$tmp/list" <<'EOF'
ok  	github.com/nathabonfim59/bamboo-raft/internal/node	0.01s
EOF
if run_gate TestKVMConformanceDirect; then
	fail "ok-summary without tests accepted"
elif grep -q 'required test TestKVMConformanceDirect is missing' "$tmp/err"; then
	pass "ok-summary without tests rejected"
else
	fail "empty-list rejection does not name the test"
fi

# A partial match must fail and name the missing one.
cat >"$tmp/list" <<'EOF'
TestKVMConformanceDirect
ok  	github.com/nathabonfim59/bamboo-raft/internal/node	0.01s
EOF
if run_gate TestKVMConformanceDirect TestKVMNodeManagerRestartDuringFreeze; then
	fail "partial presence accepted"
elif grep -q 'TestKVMNodeManagerRestartDuringFreeze is missing' "$tmp/err"; then
	pass "partial presence rejected with the missing name"
else
	fail "partial rejection does not name the missing test"
fi

# Similar-name prefix must not satisfy an exact name (grep -x, not substring).
cat >"$tmp/list" <<'EOF'
TestKVMConformanceDirectExtra
ok  	github.com/nathabonfim59/bamboo-raft/internal/node	0.01s
EOF
if run_gate TestKVMConformanceDirect; then
	fail "a longer similar name satisfied the exact requirement"
else
	pass "exact-line matching enforced"
fi

# A compile failure (nonzero go exit) must fail with go's stderr surfaced.
cat >"$tmp/list" <<'EOF'
EOF
if FAKE_GO_CALL="$tmp/call" FAKE_GO_LIST_FILE="$tmp/list" FAKE_GO_RC=1 \
	FAKE_GO_ERR="internal/node/snapshot_kvm_test.go:12:2: undefined: helper" \
	GO="$fake_go" "$gate" ./internal/node TestKVMConformanceDirect \
	>"$tmp/out" 2>"$tmp/err"; then
	fail "compile failure accepted"
elif grep -q 'go test -list failed' "$tmp/err" &&
	grep -q 'snapshot_kvm_test.go' "$tmp/err"; then
	pass "compile failure rejected with go stderr"
else
	fail "compile failure diagnostics lost: $(cat "$tmp/err")"
fi

# Usage without a test name is an error.
if GO="$fake_go" "$gate" ./internal/node >/dev/null 2>&1; then
	fail "gate accepted a call without test names"
else
	pass "gate requires at least one test name"
fi

if [ "$failures" -ne 0 ]; then
	printf 'kvm_gate_test: %d failure(s)\n' "$failures" >&2
	exit 1
fi
printf 'kvm_gate_test: all checks passed\n'
