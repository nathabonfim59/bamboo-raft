#!/usr/bin/env bash
# Fast regressions for per-boot root growth (images/mkosi.extra/usr/local/sbin/
# bbraft-grow-root).
#
# The unit must grow a partitioned root before the filesystem (growpart, then
# resize2fs), remain a no-op when the partition cannot change (growpart exits
# 1 = NOCHANGE), and fail the unit when growpart or resize2fs really fails so
# bbraft-guest.service (Requires=bbraft-grow-root.service) never claims
# readiness first. A whole-disk ext4 root (this image's format) must skip
# growpart entirely.
#
# No block devices, root or QEMU are needed: findmnt/lsblk/growpart/resize2fs/
# blockdev are stubbed and the script's documented test knob admits the fake
# source path.
set -euo pipefail

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)
grow_root="$script_dir/mkosi.extra/usr/local/sbin/bbraft-grow-root"
unit="$script_dir/mkosi.extra/usr/lib/systemd/system/bbraft-grow-root.service"
guest_unit="$script_dir/mkosi.extra/usr/lib/systemd/system/bbraft-guest.service"
tmp=$(mktemp -d "${TMPDIR:-/tmp}/bbraft-grow-root.XXXXXX")
trap 'rm -rf "$tmp"' EXIT

failures=0
pass() { printf 'PASS %s\n' "$*"; }
fail() {
	printf 'FAIL %s\n' "$*" >&2
	failures=$((failures + 1))
}

mkdir -p "$tmp/bin"
cat >"$tmp/bin/findmnt" <<'EOF'
#!/usr/bin/env bash
if printf '%s\n' "$*" | grep -q -- '-o SIZE'; then
	printf '12G\n'
else
	printf '%s\n' "${FAKE_ROOT_SOURCE:?}"
fi
EOF
cat >"$tmp/bin/lsblk" <<'EOF'
#!/usr/bin/env bash
case "$*" in
*PKNAME*) [ -n "${FAKE_PKNAME:-}" ] && printf '%s\n' "$FAKE_PKNAME" ;;
*PARTN*) [ -n "${FAKE_PARTN:-}" ] && printf '%s\n' "$FAKE_PARTN" ;;
esac
EOF
cat >"$tmp/bin/growpart" <<'EOF'
#!/usr/bin/env bash
printf 'growpart %s\n' "$*" >>"$CALL_LOG"
printf 'CHANGED: partition=%s\n' "${2:-0}"
exit "${FAKE_GROWPART_STATUS:-0}"
EOF
cat >"$tmp/bin/resize2fs" <<'EOF'
#!/usr/bin/env bash
printf 'resize2fs %s\n' "$*" >>"$CALL_LOG"
exit "${FAKE_RESIZE_STATUS:-0}"
EOF
cat >"$tmp/bin/blockdev" <<'EOF'
#!/usr/bin/env bash
printf '12884901888\n'
EOF
chmod 0755 "$tmp/bin"/*

run_grow() { # <log> [env assignments...]; returns the script status
	local log=$1
	shift
	: >"$log"
	env CALL_LOG="$log" PATH="$tmp/bin:$PATH" \
		BBRAFT_GROW_ROOT_TEST_ALLOW_NON_BLOCK=1 "$@" \
		"$grow_root" >"$tmp/stdout" 2>"$tmp/stderr"
}

# 1. Whole-disk ext4 (the image format): no growpart, resize2fs on the disk.
if run_grow "$tmp/whole.log" FAKE_ROOT_SOURCE=/dev/vda; then
	pass "whole-disk root succeeds"
else
	fail "whole-disk root failed: $(cat "$tmp/stderr")"
fi
if ! grep -q '^growpart ' "$tmp/whole.log" && grep -qx 'resize2fs /dev/vda' "$tmp/whole.log"; then
	pass "whole-disk root skips growpart and resizes the filesystem"
else
	fail "whole-disk calls are wrong: $(cat "$tmp/whole.log")"
fi

# 2. Partitioned root: growpart first, then resize2fs.
if run_grow "$tmp/part.log" FAKE_ROOT_SOURCE=/dev/vda1 FAKE_PKNAME=vda FAKE_PARTN=1; then
	pass "partitioned root succeeds"
else
	fail "partitioned root failed: $(cat "$tmp/stderr")"
fi
expected=$(printf '%s\n' 'growpart /dev/vda 1' 'resize2fs /dev/vda1')
if [ "$(cat "$tmp/part.log")" = "$expected" ]; then
	pass "partitioned root runs growpart before resize2fs"
else
	fail "partitioned call order is wrong: $(cat "$tmp/part.log")"
fi

# 3. growpart NOCHANGE (exit 1): still resize2fs, overall success.
if run_grow "$tmp/nochange.log" FAKE_ROOT_SOURCE=/dev/vda1 FAKE_PKNAME=vda FAKE_PARTN=1 FAKE_GROWPART_STATUS=1; then
	pass "growpart NOCHANGE is not a failure"
else
	fail "growpart NOCHANGE failed the unit: $(cat "$tmp/stderr")"
fi
if grep -qx 'resize2fs /dev/vda1' "$tmp/nochange.log"; then
	pass "growpart NOCHANGE still runs resize2fs"
else
	fail "growpart NOCHANGE skipped resize2fs"
fi

# 4. growpart hard failure: fail before resize2fs.
if run_grow "$tmp/growfail.log" FAKE_ROOT_SOURCE=/dev/vda1 FAKE_PKNAME=vda FAKE_PARTN=1 FAKE_GROWPART_STATUS=2; then
	fail "growpart failure was ignored"
else
	pass "growpart failure fails the unit"
fi
if ! grep -q '^resize2fs ' "$tmp/growfail.log"; then
	pass "growpart failure does not run resize2fs"
else
	fail "growpart failure still ran resize2fs: $(cat "$tmp/growfail.log")"
fi

# 5. resize2fs failure propagates.
if run_grow "$tmp/resizefail.log" FAKE_ROOT_SOURCE=/dev/vda FAKE_RESIZE_STATUS=1; then
	fail "resize2fs failure was ignored"
else
	pass "resize2fs failure fails the unit"
fi

# 6. A non-block root is skipped (no test knob).
: >"$tmp/skip.log"
if env CALL_LOG="$tmp/skip.log" PATH="$tmp/bin:$PATH" FAKE_ROOT_SOURCE=overlay \
	"$grow_root" >"$tmp/stdout" 2>"$tmp/stderr"; then
	pass "non-block root succeeds as a skip"
else
	fail "non-block root failed: $(cat "$tmp/stderr")"
fi
if [ ! -s "$tmp/skip.log" ]; then
	pass "non-block root performs no resize"
else
	fail "non-block root resized: $(cat "$tmp/skip.log")"
fi

# 7. Ordering: the guest must not be ready before growth succeeded.
if grep -qx 'Requires=bbraft-grow-root.service' "$guest_unit" &&
	grep -qx 'After=.*bbraft-grow-root.service.*' "$guest_unit"; then
	pass "bbraft-guest.service requires and orders after growth"
else
	fail "bbraft-guest.service does not require/order after growth"
fi
if grep -qx 'ExecStart=/usr/local/sbin/bbraft-grow-root' "$unit"; then
	pass "growth unit runs the checked script"
else
	fail "growth unit has an unexpected ExecStart"
fi

# 8. The script is POSIX sh (the unit has no shell interpreter).
if bash -n "$grow_root" && ! grep -q '^#!.*bash' "$grow_root"; then
	pass "grow-root syntax and /bin/sh interpreter"
else
	fail "grow-root is not a valid /bin/sh script"
fi

if [ "$failures" -ne 0 ]; then
	printf 'grow_root_test: %d failure(s)\n' "$failures" >&2
	exit 1
fi
printf 'grow_root_test: all checks passed\n'
