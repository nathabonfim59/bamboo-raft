#!/usr/bin/env bash
# Fast regression for guest command/package availability.
#
# The r3 test-VM build reached mkosi.postinst.chroot and failed at `apt-get clean`
# with exit 127: mkosi uses apt on the host, but the guest image did not list
# the apt package. This test asserts the packages needed by postinst and the
# later toolchain checks are parsed from mkosi.conf, and that the in-guest
# toolchain check verifies apt-get. It also proves the assertion fails when apt
# is removed (the exact pre-fix config).
set -euo pipefail

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)
tmp=$(mktemp -d "${TMPDIR:-/tmp}/bbraft-toolchain-req.XXXXXX")
trap 'rm -rf "$tmp"' EXIT

failures=0
pass() { printf 'PASS %s\n' "$*"; }
fail() {
	printf 'FAIL %s\n' "$*" >&2
	failures=$((failures + 1))
}

parse_packages() { # <mkosi.conf>; prints Packages= entries in [Content]
	awk '
		/^\[/ { section = $0; next }
		section == "[Content]" && /^[[:space:]]+[a-z0-9][a-z0-9+.-]*[[:space:]]*$/ {
			line = $0
			gsub(/^[[:space:]]+/, "", line)
			gsub(/[[:space:]]+$/, "", line)
			print line
		}
	' "$1"
}

# Every package below is required by an actual command in
# mkosi.postinst.chroot, bbraft-grow-root, or bbraft-toolchain-check:
#   apt->apt-get; coreutils->id/install/ln/chmod/chown/rm/basename/stat/
#   timeout/paste/mktemp; libc-bin->getent; mawk->awk; passwd->groupadd/
#   useradd/usermod; sudo->visudo; systemd->systemctl; initramfs-tools->
#   update-initramfs/lsinitramfs; e2fsprogs->resize2fs; util-linux->findmnt/
#   readlink/blockdev; kmod->module tools; iproute2->ip; podman->podman;
#   busybox-static->offline Podman import; tar/gzip->guest checks;
#   tzdata->/usr/share/zoneinfo/UTC for postinst.
required="apt bash coreutils grep libc-bin mawk passwd sudo systemd initramfs-tools e2fsprogs util-linux kmod iproute2 podman busybox-static tar gzip tzdata"

check_config() { # <mkosi.conf>; fails if a required package is missing
	local conf=$1 missing=0 name
	local packages
	packages=$(parse_packages "$conf")
	for name in $required; do
		if ! printf '%s\n' "$packages" | grep -qx "$name"; then
			printf 'missing package %s in %s\n' "$name" "$conf" >&2
			missing=1
		fi
	done
	return "$missing"
}

if check_config "$script_dir/mkosi.conf"; then
	pass "mkosi.conf lists all postinst/toolchain packages"
else
	fail "mkosi.conf is missing a required guest package"
fi

toolchain="$script_dir/mkosi.extra/usr/local/bin/bbraft-toolchain-check"
if grep -q 'for command in apt-get' "$toolchain"; then
	pass "guest toolchain check requires apt-get"
else
	fail "guest toolchain check does not check apt-get"
fi

# Pre-fix reproduction: the same config without the apt package must fail.
sed '/^[[:space:]]*apt[[:space:]]*$/d' "$script_dir/mkosi.conf" >"$tmp/mkosi.conf"
if check_config "$tmp/mkosi.conf" >"$tmp/missing.log" 2>&1; then
	fail "config assertion accepted the pre-fix config without apt"
else
	pass "config assertion rejects the pre-fix config without apt"
fi
if grep -q 'missing package apt' "$tmp/missing.log"; then
	pass "pre-fix failure names the missing apt package"
else
	fail "pre-fix failure does not name the missing apt package"
fi

if [ "$failures" -ne 0 ]; then
	printf 'toolchain_requirements_test: %d failure(s)\n' "$failures" >&2
	exit 1
fi
printf 'toolchain_requirements_test: all checks passed\n'
