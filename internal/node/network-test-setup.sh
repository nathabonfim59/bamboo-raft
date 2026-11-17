#!/bin/sh
# Privileged prerequisites for the A8 real-TAP and real-SSH acceptance tests
# (internal/node/network_kvm_test.go).
#
# Run this ONLY inside a throwaway test VM, never on a workstation or a
# node. Compile the test binary as the normal VM user first; sudo only this
# setup and the already-built binary (never `sudo go test`):
#
#   go test -c -o /tmp/bbraft-node-network.test ./internal/node
#   sudo env BBRAFT_TEST_VM_SETUP=1 BBRAFT_TEST_KVM_NET_SETUP=1 \
#     sh internal/node/network-test-setup.sh
#
# Optional M3 test-only fallback: add BBRAFT_TEST_KVM_NET_UNPINNED=1 to the
# acceptance command (throwaway VM lifetime only; omit for the final pinned-v1
# gate). This script never builds or selects an image itself.
#
# It reuses internal/node/net/test-setup.sh for packages, modules and the
# existing device ACLs, then checks that the unprivileged review user can open
# /dev/kvm, /dev/vhost-vsock, /dev/net/tun and /dev/vhost-net. It does not add
# device ACLs itself and does not configure networking, addresses, routes,
# bridges or firewalls on the outer VM. The Go tests create and delete their
# own throwaway network namespaces.

set -eu

if [ "${BBRAFT_TEST_KVM_NET_SETUP:-}" != "1" ]; then
	echo "network-test-setup: refusing to run; set BBRAFT_TEST_KVM_NET_SETUP=1 (a throwaway test VM only)" >&2
	exit 2
fi

if [ "${BBRAFT_TEST_VM_SETUP:-}" != "1" ]; then
	echo "network-test-setup: refusing to run; set BBRAFT_TEST_VM_SETUP=1 (a throwaway test VM only)" >&2
	exit 2
fi

if [ "$(id -u)" -ne 0 ]; then
	echo "network-test-setup: run as root (sudo) inside the throwaway VM" >&2
	exit 2
fi

fail() {
	echo "network-test-setup: $*" >&2
	exit 1
}

script_dir=$(cd -- "$(dirname -- "$0")" && pwd -P)
repo_root=$(cd -- "$script_dir/../.." && pwd -P)

# util-linux is used by the Go test itself, so check it before the package
# step: this script only installs openssh-client, and only when it is missing.
for binary in unshare setpriv ip nft; do
	command -v "$binary" >/dev/null 2>&1 || fail "$binary is missing; install util-linux/iproute2/nftables in the base image"
done

if ! unshare --help 2>&1 | grep -q -- '--kill-child'; then
	fail "unshare has no --kill-child; util-linux 2.39.3 is required"
fi
if ! setpriv --help 2>&1 | grep -q -- '--pdeathsig'; then
	fail "setpriv has no --pdeathsig; util-linux 2.39.3 is required"
fi

test_uid=${BBRAFT_TEST_KVM_NET_UID:-${SUDO_UID:-}}
test_gid=${BBRAFT_TEST_KVM_NET_GID:-${SUDO_GID:-}}
[ -n "$test_uid" ] || fail "cannot determine the review uid: run with sudo or set BBRAFT_TEST_KVM_NET_UID"
[ -n "$test_gid" ] || fail "cannot determine the review gid: run with sudo or set BBRAFT_TEST_KVM_NET_GID"
case "$test_uid" in
*[!0-9]*) fail "BBRAFT_TEST_KVM_NET_UID must be numeric" ;;
esac
case "$test_gid" in
*[!0-9]*) fail "BBRAFT_TEST_KVM_NET_GID must be numeric" ;;
esac
[ "$test_uid" -ne 0 ] || fail "the node and QEMU must run nonroot; choose the review user, not root"
[ "$test_gid" -ne 0 ] || fail "the node and QEMU must not run with gid 0; choose the review user's nonroot gid"

test_user=${SUDO_USER:-}
if [ -z "$test_user" ]; then
	test_user=$(id -nu "$test_uid" 2>/dev/null || true)
fi
[ -n "$test_user" ] || fail "cannot resolve uid $test_uid to a user name for initgroups"

# Reuse the reviewed A7 setup: packages nftables/iproute2/kmod/acl, the tun,
# vhost_net and vhost_vsock modules, and the ACLs it already owns.
BBRAFT_TEST_VM_SETUP=1 BBRAFT_TEST_USER="$test_user" sh "$repo_root/internal/node/net/test-setup.sh"

# OpenSSH clients only: the acceptance test runs real ssh, scp and sftp in the
# client namespace. Do not install or change anything else here.
if ! command -v ssh >/dev/null 2>&1 || ! command -v scp >/dev/null 2>&1 || ! command -v sftp >/dev/null 2>&1; then
	command -v apt-get >/dev/null 2>&1 || fail "openssh-client is missing and apt-get is unavailable"
	apt-get update -qq || fail "apt-get update failed"
	DEBIAN_FRONTEND=noninteractive apt-get install -y -qq openssh-client >/dev/null || fail "apt-get install openssh-client failed"
fi
for binary in ssh scp sftp; do
	command -v "$binary" >/dev/null 2>&1 || fail "$binary is missing after install"
done

# Check, but do not add, the devices the nonroot node/QEMU needs. The
# throwaway base owns these grants.
for device in /dev/kvm /dev/vhost-vsock /dev/net/tun /dev/vhost-net; do
	[ -c "$device" ] || fail "$device is missing (module or device node unavailable)"
	if ! setpriv --reuid="$test_uid" --regid="$test_gid" --init-groups -- \
		sh -c 'test -r "$1" && test -w "$1"' sh "$device"; then
		fail "$device is not readable and writable by uid $test_uid; the throwaway base must grant it (this script does not add ACLs)"
	fi
done

# A10 jailed harness (internal/node/jail_kvm_test.go) only: the production
# unit needs DelegateSubgroup= (systemd >= 254) on a unified cgroup v2 host.
# Checked, never changed.
if [ "${BBRAFT_TEST_KVM_JAIL_SETUP:-}" = "1" ]; then
	command -v systemctl >/dev/null 2>&1 || fail "systemctl is missing; the jailed harness runs real systemd units"
	systemd_version=$(systemctl --version | sed -n '1s/^systemd \([0-9][0-9]*\).*/\1/p')
	[ -n "$systemd_version" ] || fail "cannot read the systemd version from systemctl --version"
	[ "$systemd_version" -ge 254 ] || fail "systemd $systemd_version is too old: DelegateSubgroup= needs systemd 254 or newer"
	[ "$(stat -fc %T /sys/fs/cgroup)" = "cgroup2fs" ] || fail "/sys/fs/cgroup is not a unified cgroup v2 mount"
	echo "network-test-setup: systemd $systemd_version with unified cgroup v2 (jailed harness)"
fi

echo "network-test-setup: ready"
echo "network-test-setup: containment first (no image needed):"
echo "  sudo env BBRAFT_TEST_KVM_NET=1 /tmp/bbraft-node-network.test -test.run '^TestKVMNetworkContainment$' -test.v -test.timeout 10m"
echo "network-test-setup: full real-TAP/SSH acceptance:"
echo "  sudo env BBRAFT_TEST_KVM_NET=1 BBRAFT_TEST_KVM=1 BBRAFT_TEST_IMAGE_DIR=/path/to/images/out BBRAFT_TEST_REQUIRE_ALL=1 /tmp/bbraft-node-network.test -test.run '^TestKVMNetwork' -test.v -test.count=1 -test.timeout 60m"
echo "network-test-setup: optional M3 test-only fallback: add BBRAFT_TEST_KVM_NET_UNPINNED=1 to the acceptance env (throwaway VM lifetime only; omit for the pinned-v1 final gate)"
