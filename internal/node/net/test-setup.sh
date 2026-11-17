#!/bin/sh
# Privileged prerequisites for the A7 network tests (internal/node/net).
#
# Run this ONLY inside a throwaway test VM with sudo, never on a workstation
# or a node:
#
#   sudo BBRAFT_TEST_VM_SETUP=1 sh internal/node/net/test-setup.sh
#
# The script is idempotent and safe against the warmed bamboo-raft base: it
# installs what is missing, loads the required modules and grants device ACLs.
# It does NOT configure networking, addresses, routes, bridges or firewalls on
# the outer VM; the Go tests create their own throwaway network namespaces with
# their own nftables tables and delete them again.
#
# Refuses to run unless BBRAFT_TEST_VM_SETUP=1 is set explicitly.

set -eu

if [ "${BBRAFT_TEST_VM_SETUP:-}" != "1" ]; then
	echo "test-setup: refusing to run; set BBRAFT_TEST_VM_SETUP=1 (a throwaway test VM only)" >&2
	exit 2
fi

if [ "$(id -u)" -ne 0 ]; then
	echo "test-setup: run as root (sudo) inside the throwaway VM" >&2
	exit 2
fi

fail() {
	echo "test-setup: $*" >&2
	exit 1
}

# Packages. apt-get is idempotent, so an already warmed base is a no-op.
if command -v apt-get >/dev/null 2>&1; then
	apt-get update -qq || fail "apt-get update failed"
	DEBIAN_FRONTEND=noninteractive apt-get install -y -qq \
		nftables iproute2 kmod acl >/dev/null || fail "apt-get install failed"
fi

# Binaries the tests invoke.
for binary in ip nft modprobe; do
	command -v "$binary" >/dev/null 2>&1 || fail "$binary is missing after install"
done

# Modules: tun for TAPs, vhost_net for the pre-opened vhost descriptor, and
# vhost_vsock for the neighbouring node suites. modprobe of a loaded module is a
# no-op, so this is safe on the warmed base.
for module in tun vhost_net vhost_vsock; do
	modprobe "$module" 2>/dev/null || fail "modprobe $module failed"
done

# Devices. /dev/net/tun and /dev/vhost-net are required by this package;
# /dev/kvm and /dev/vhost-vsock are reported for the neighbouring suites.
for device in /dev/net/tun /dev/vhost-net; do
	[ -c "$device" ] || fail "$device is missing (module or device node unavailable)"
	[ -r "$device" ] && [ -w "$device" ] || fail "$device is not readable and writable"
done
for device in /dev/kvm /dev/vhost-vsock; do
	if [ -c "$device" ]; then
		echo "test-setup: optional device present: $device"
	else
		echo "test-setup: warning: optional device missing: $device" >&2
	fi
done

# Device ACLs for the invoking user, so an operator can also run the tests as
# themselves when their sudo policy allows the required capabilities. Root
# already has access.
test_user="${BBRAFT_TEST_USER:-${SUDO_USER:-}}"
if [ -n "$test_user" ] && [ "$test_user" != "root" ]; then
	for device in /dev/net/tun /dev/vhost-net /dev/kvm; do
		[ -c "$device" ] || continue
		setfacl -m "u:${test_user}:rw" "$device" || fail "setfacl on $device failed"
	done
	echo "test-setup: granted $test_user rw access to /dev/net/tun /dev/vhost-net /dev/kvm"
fi

# The tests themselves need IPv4 forwarding *inside* their namespaces (they set
# it per namespace). They never change the outer VM's forwarding or firewall, so
# nothing is written here on purpose.

echo "test-setup: ready"
echo "test-setup: run the packet tests with"
echo "  sudo BBRAFT_TEST_NET=1 go test ./internal/node/net/ -run 'TestNetnsIsolation|TestRealTapDevice' -v -count=1"
echo "test-setup: add BBRAFT_TEST_REQUIRE_ALL=1 to turn any missing prerequisite into a failure"
