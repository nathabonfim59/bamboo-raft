#!/usr/bin/env bash
# Verify the boot-critical virtio/vsock drivers for a built kernel.
#
#   check_kernel_modules.sh <image-root> <kver> <initrd-list>
#
# A driver is accepted when:
#   - for boot modules: its .ko[.xz|.zst|.gz] is listed in the initrd, or
#   - for later modules: its .ko* exists under /usr/lib/modules/<kver>/, or
#   - it is proven builtin by CONFIG_<symbol>=y in /boot/config-<kver>, or the
#     exact path appears in /usr/lib/modules/<kver>/modules.builtin.
# A driver present nowhere is a hard failure. This matches Ubuntu 24.04's
# 6.8.0-149-generic, where virtio_blk/pci/net are builtin while vsock and its
# virtio transport are modules.
set -euo pipefail

die() {
	printf 'check_kernel_modules: %s\n' "$*" >&2
	exit 1
}

[ $# -eq 3 ] || die "usage: check_kernel_modules.sh <image-root> <kver> <initrd-list>"
root=$1
kver=$2
initrd_list=$3

config="$root/boot/config-$kver"
builtin="$root/usr/lib/modules/$kver/modules.builtin"
modules_dir="$root/usr/lib/modules/$kver"
[ -d "$modules_dir" ] || die "kernel modules for $kver are missing ($modules_dir)"
[ -s "$initrd_list" ] || die "initrd listing for $kver is missing ($initrd_list)"

in_initrd() { # <module>
	grep -Eq "(^|/)$1\\.ko(\\.(xz|zst|gz))?$" "$initrd_list"
}

in_modules() { # <module>
	# Exact module filenames as regular files only; a directory named
	# module.ko or module.ko.bak/signature must not count as the driver.
	[ -n "$(find "$modules_dir" -type f \( \
		-name "$1.ko" -o -name "$1.ko.xz" -o -name "$1.ko.zst" -o -name "$1.ko.gz" \
		\) -print -quit)" ]
}

in_config() { # <symbol>
	[ -f "$config" ] && grep -qx "CONFIG_$1=y" "$config"
}

in_builtin_list() { # <modules.builtin path>
	[ -f "$builtin" ] && grep -qx "$1" "$builtin"
}

is_builtin() { # <symbol> <modules.builtin path>
	in_config "$1" || in_builtin_list "$2"
}

check_boot() { # <module> <symbol> <builtin-path>
	if in_initrd "$1"; then
		printf 'check_kernel_modules: %s module in initrd\n' "$1" >&2
		return 0
	fi
	if is_builtin "$2" "$3"; then
		printf 'check_kernel_modules: %s builtin (CONFIG_%s=y or modules.builtin)\n' "$1" "$2" >&2
		return 0
	fi
	die "initrd for $kver lacks $1 and it is not builtin (CONFIG_$2 != y and $3 not in modules.builtin)"
}

check_later() { # <module> <symbol> <builtin-path>
	if in_modules "$1"; then
		printf 'check_kernel_modules: %s module present under %s\n' "$1" "$modules_dir" >&2
		return 0
	fi
	if is_builtin "$2" "$3"; then
		printf 'check_kernel_modules: %s builtin (CONFIG_%s=y or modules.builtin)\n' "$1" "$2" >&2
		return 0
	fi
	die "kernel module $1 is missing from $modules_dir and it is not builtin (CONFIG_$2 != y and $3 not in modules.builtin)"
}

check_boot virtio_blk VIRTIO_BLK kernel/drivers/block/virtio_blk.ko
check_boot virtio_pci VIRTIO_PCI kernel/drivers/virtio/virtio_pci.ko
check_later virtio_net VIRTIO_NET kernel/drivers/net/virtio_net.ko
check_later vsock VSOCKETS kernel/net/vmw_vsock/vsock.ko
check_later vmw_vsock_virtio_transport VIRTIO_VSOCKETS kernel/net/vmw_vsock/vmw_vsock_virtio_transport.ko
