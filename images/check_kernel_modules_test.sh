#!/usr/bin/env bash
# Regression for check_kernel_modules.sh: real module vs builtin vs missing.
#
# The old build guard required virtio_blk.ko in the initrd, but Ubuntu
# 6.8.0-149 has CONFIG_VIRTIO_BLK=y and lists kernel/drivers/block/virtio_blk.ko
# in modules.builtin. The new guard must accept that, accept modules for vsock,
# and still reject a driver that is absent everywhere.
set -euo pipefail

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)
checker="$script_dir/check_kernel_modules.sh"
tmp=$(mktemp -d "${TMPDIR:-/tmp}/bbraft-kernel-modules.XXXXXX")
trap 'rm -rf "$tmp"' EXIT

failures=0
pass() { printf 'PASS %s\n' "$*"; }
fail() {
	printf 'FAIL %s\n' "$*" >&2
	failures=$((failures + 1))
}

kver=6.8.0-test
new_root() { # <dir>
	local root=$1
	mkdir -p "$root/boot" "$root/usr/lib/modules/$kver/kernel/net/vmw_vsock"
	: >"$root/boot/config-$kver"
	: >"$root/usr/lib/modules/$kver/modules.builtin"
	printf 'kernel/dummy.ko\n' >"$root/initrd.list"
}
module() { # <root> <subdir> <name>
	mkdir -p "$1/usr/lib/modules/$kver/kernel/drivers/$2"
	: >"$1/usr/lib/modules/$kver/kernel/drivers/$2/$3.ko.zst"
}
vsock_modules() { # <root>
	: >"$1/usr/lib/modules/$kver/kernel/net/vmw_vsock/vsock.ko.zst"
	: >"$1/usr/lib/modules/$kver/kernel/net/vmw_vsock/vmw_vsock_virtio_transport.ko.zst"
}
run_check() { # <root>
	"$checker" "$1" "$kver" "$1/initrd.list" >"$tmp/check.log" 2>&1
}

# Old pre-fix logic, for the failing-before assertion.
old_boot_check() { # <root>
	local module_name
	for module_name in virtio_blk virtio_pci; do
		grep -q "$module_name\\.ko" "$1/initrd.list" || return 1
	done
}

# 1. Real Ubuntu 6.8.0-149 shape: virtio builtin via config and
# modules.builtin, vsock/transport as modules.
root="$tmp/ubuntu"
new_root "$root"
printf 'CONFIG_VIRTIO_BLK=y\nCONFIG_VIRTIO_PCI=y\nCONFIG_VIRTIO_NET=y\n' >"$root/boot/config-$kver"
printf '%s\n' \
	kernel/drivers/block/virtio_blk.ko \
	kernel/drivers/virtio/virtio_pci.ko \
	kernel/drivers/net/virtio_net.ko >>"$root/usr/lib/modules/$kver/modules.builtin"
vsock_modules "$root"
if run_check "$root"; then
	pass "Ubuntu-like builtin virtio + modular vsock accepted"
else
	fail "Ubuntu-like builtin virtio was rejected: $(tail -n 2 "$tmp/check.log" | tr '\n' ' ')"
fi
if old_boot_check "$root"; then
	fail "old initrd-only guard unexpectedly accepted builtin virtio_blk"
else
	pass "old initrd-only guard rejects builtin virtio_blk (failing-before demonstrated)"
fi

# 2. Module-only boot drivers in the initrd and modules tree.
root="$tmp/modules"
new_root "$root"
printf '%s\n' \
	kernel/drivers/block/virtio_blk.ko \
	kernel/drivers/virtio/virtio_pci.ko >"$root/initrd.list"
module "$root" block virtio_blk
module "$root" virtio virtio_pci
module "$root" net virtio_net
vsock_modules "$root"
if run_check "$root"; then
	pass "initrd modules + modules tree accepted"
else
	fail "module form was rejected: $(tail -n 2 "$tmp/check.log" | tr '\n' ' ')"
fi

# 3. Builtin proven only by modules.builtin (config not =y).
root="$tmp/builtin-list"
new_root "$root"
printf '%s\n' \
	kernel/drivers/block/virtio_blk.ko \
	kernel/drivers/virtio/virtio_pci.ko \
	kernel/drivers/net/virtio_net.ko >>"$root/usr/lib/modules/$kver/modules.builtin"
vsock_modules "$root"
if run_check "$root"; then
	pass "modules.builtin evidence alone accepted"
else
	fail "modules.builtin evidence was rejected"
fi

# 4. virtio_blk absent everywhere must fail.
root="$tmp/missing-boot"
new_root "$root"
printf '%s\n' \
	kernel/drivers/virtio/virtio_pci.ko \
	kernel/drivers/net/virtio_net.ko >>"$root/usr/lib/modules/$kver/modules.builtin"
vsock_modules "$root"
if run_check "$root"; then
	fail "missing virtio_blk was accepted"
else
	pass "missing virtio_blk rejected"
fi
if grep -q 'lacks virtio_blk' "$tmp/check.log"; then
	pass "missing virtio_blk error is explicit"
else
	fail "missing virtio_blk error is not explicit"
fi

# 5. vsock transport module absent and not builtin must fail.
root="$tmp/missing-vsock"
new_root "$root"
printf 'CONFIG_VIRTIO_BLK=y\nCONFIG_VIRTIO_PCI=y\nCONFIG_VIRTIO_NET=y\n' >"$root/boot/config-$kver"
printf '%s\n' \
	kernel/drivers/block/virtio_blk.ko \
	kernel/drivers/virtio/virtio_pci.ko \
	kernel/drivers/net/virtio_net.ko >>"$root/usr/lib/modules/$kver/modules.builtin"
: >"$root/usr/lib/modules/$kver/kernel/net/vmw_vsock/vsock.ko.zst"
if run_check "$root"; then
	fail "missing vsock transport was accepted"
else
	pass "missing vsock transport rejected"
fi
if grep -q 'vmw_vsock_virtio_transport' "$tmp/check.log"; then
	pass "missing vsock transport error names the module"
else
	fail "missing vsock transport error does not name the module"
fi

# 6. Compressed module suffixes are accepted for later drivers.
root="$tmp/compressed"
new_root "$root"
printf 'CONFIG_VIRTIO_BLK=y\nCONFIG_VIRTIO_PCI=y\nCONFIG_VIRTIO_NET=y\n' >"$root/boot/config-$kver"
: >"$root/usr/lib/modules/$kver/kernel/net/vmw_vsock/vsock.ko.xz"
: >"$root/usr/lib/modules/$kver/kernel/net/vmw_vsock/vmw_vsock_virtio_transport.ko.gz"
if run_check "$root"; then
	pass "compressed .ko.xz/.ko.gz modules accepted"
else
	fail "compressed modules rejected: $(tail -n 2 "$tmp/check.log" | tr '\n' ' ')"
fi

# 7. A directory named vsock.ko is not a module.
root="$tmp/module-dir"
new_root "$root"
printf 'CONFIG_VIRTIO_BLK=y\nCONFIG_VIRTIO_PCI=y\nCONFIG_VIRTIO_NET=y\n' >"$root/boot/config-$kver"
mkdir -p "$root/usr/lib/modules/$kver/kernel/net/vmw_vsock/vsock.ko"
: >"$root/usr/lib/modules/$kver/kernel/net/vmw_vsock/vmw_vsock_virtio_transport.ko.zst"
if run_check "$root"; then
	fail "directory named vsock.ko was accepted as a module"
else
	pass "directory named vsock.ko rejected"
fi
if grep -q 'kernel module vsock is missing' "$tmp/check.log"; then
	pass "directory-vsock failure is explicit"
else
	fail "directory-vsock failure is not explicit"
fi

# 8. A garbage suffix such as vsock.ko.bak is not a module.
root="$tmp/module-garbage"
new_root "$root"
printf 'CONFIG_VIRTIO_BLK=y\nCONFIG_VIRTIO_PCI=y\nCONFIG_VIRTIO_NET=y\n' >"$root/boot/config-$kver"
: >"$root/usr/lib/modules/$kver/kernel/net/vmw_vsock/vsock.ko.bak"
: >"$root/usr/lib/modules/$kver/kernel/net/vmw_vsock/vmw_vsock_virtio_transport.ko.zst"
if run_check "$root"; then
	fail "vsock.ko.bak was accepted as a module"
else
	pass "garbage module suffix vsock.ko.bak rejected"
fi

if [ "$failures" -ne 0 ]; then
	printf 'check_kernel_modules_test: %d failure(s)\n' "$failures" >&2
	exit 1
fi
printf 'check_kernel_modules_test: all checks passed\n'
