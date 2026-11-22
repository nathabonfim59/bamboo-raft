#!/usr/bin/env bash
# Builds the pinned upstream QEMU 10.0 release into a throwaway prefix on a
# test VM, so the KVM suites can run against the QEMU major version production
# ships (Debian 13 has 10.0.x) while the test VM's own package is 8.2.2
# (Ubuntu 24.04 has no QEMU 10.0 in its archive, backports or cloud archive).
#
# Throwaway test VM only, never a workstation or a node. Run as the normal VM
# user with passwordless sudo (only the build dependencies use it):
#
#   bash internal/node/qemu10.sh [PREFIX]
#
# PREFIX defaults to ${TMPDIR:-/tmp}/bbraft-qemu-10.0.13 and must not exist
# yet; /usr, /usr/local and /opt are refused (it is a throwaway, never a system
# install). The script ends by printing the one line that points the tests at
# it:
#
#   export BBRAFT_TEST_QEMU_PREFIX=<PREFIX>
#
# With that variable set, the unjailed KVM tests (requireKVM, kvm_test.go) put
# <PREFIX>/bin first on PATH, so they start this "qemu-system-x86_64" and
# "qemu-img" instead of the distro's (qemu.DefaultBinary is resolved by name).
# make check never needs it. Then run for example:
#
#   BBRAFT_TEST_KVM=1 BBRAFT_TEST_IMAGE_DIR=images/out \
#     go test ./internal/node -run 'TestKVMPhase2Schema|TestKVMBitmap|TestKVMSnapshot|TestKVMRestore|TestKVMConformanceDirect' -v -timeout 60m
#
# (TestKVMPhase2Schema logs the version the capability gate saw.) The jailed
# harness (jail-acceptance.sh) renders -node.jail-qemu-binary from the role
# (default /usr/bin/qemu-system-x86_64) and was not run against this build.
#
# Optional: QEMU10_TARBALL=/path/to/qemu-10.0.13.tar.xz uses a tarball fetched
# elsewhere; it is checked against the same pinned SHA-256 either way.
#
# Pins: the release tarball is signed upstream (key CEAC C9E1 5534 EBAB B82D
# 3FA0 3353 C9CE F108 B584, Michael Roth); the signature was verified once
# when the checksum below was taken from it.
set -euo pipefail

version=10.0.13
sha256=4a59b74dac7ead8b162a7c4595dcbbeebe989b90c89cfe6bb4dec5447342f6fa
url=https://download.qemu.org/qemu-$version.tar.xz

if [ "$(id -u)" = 0 ]; then
	echo "qemu10: run as the normal user; only apt-get uses sudo" >&2
	exit 2
fi
prefix=${1:-${TMPDIR:-/tmp}/bbraft-qemu-$version}
case $prefix in /*) ;; *)
	echo "qemu10: PREFIX must be an absolute path, got '$prefix'" >&2
	exit 2
	;;
esac
case $prefix in /usr | /usr/* | /opt | /opt/* | /bin | /bin/* | /lib | /lib/* | /etc | /etc/* | /var/lib | /var/lib/*)
	echo "qemu10: PREFIX '$prefix' is a system path; use a scratch directory" >&2
	exit 2
	;;
esac
if [ -e "$prefix" ]; then
	echo "qemu10: $prefix already exists; pick a fresh prefix (it is a throwaway)" >&2
	exit 2
fi
for tool in sudo curl sha256sum tar make; do
	command -v "$tool" >/dev/null || {
		echo "qemu10: $tool not found" >&2
		exit 1
	}
done

work=$(mktemp -d "${TMPDIR:-/tmp}/bbraft-qemu10-build.XXXXXX")
trap 'rm -rf "$work"' EXIT

echo "== build dependencies (Ubuntu 24.04 package names)"
sudo DEBIAN_FRONTEND=noninteractive apt-get update -qq
sudo DEBIAN_FRONTEND=noninteractive apt-get install -y -qq \
	build-essential ninja-build pkg-config python3 python3-venv python3-pip flex bison \
	libglib2.0-dev libpixman-1-dev libslirp-dev libseccomp-dev

tarball=$work/qemu-$version.tar.xz
if [ -n "${QEMU10_TARBALL:-}" ]; then
	cp "$QEMU10_TARBALL" "$tarball"
else
	echo "== fetching $url"
	curl -fsS --retry 3 -o "$tarball" "$url"
fi
echo "$sha256  $tarball" | sha256sum -c - || {
	echo "qemu10: the tarball does not match the pinned SHA-256 $sha256" >&2
	exit 1
}

echo "== building QEMU $version into $prefix"
tar -C "$work" -xf "$tarball"
mkdir "$work/build"
(
	cd "$work/build"
	# x86-64 system emulator only, KVM, user networking (the unjailed tests use
	# it) and seccomp (-sandbox on is in every launch line): a missing library
	# is a configure error, never a silently different build.
	"$work/qemu-$version/configure" --prefix="$prefix" --target-list=x86_64-softmmu \
		--enable-kvm --enable-slirp --enable-seccomp --disable-docs --disable-werror \
		--disable-user --disable-guest-agent
	timeout --kill-after=60s 45m make -j"$(nproc)"
	make install
)

echo "== checking the build"
qemu=$prefix/bin/qemu-system-x86_64
got=$("$qemu" --version | head -n1)
case $got in "QEMU emulator version $version"*) ;; *)
	echo "qemu10: $qemu reports '$got', want $version" >&2
	exit 1
	;;
esac
devices=$("$qemu" -device help 2>&1) || true
case $devices in *'name "vhost-vsock-pci"'*) ;; *)
	echo "qemu10: the build has no vhost-vsock-pci device" >&2
	exit 1
	;;
esac
printf '%s\n' '{"execute":"qmp_capabilities"}' '{"execute":"quit"}' |
	timeout 30s "$qemu" -sandbox on -machine none -nodefaults -display none -qmp stdio >/dev/null 2>&1 || {
	echo "qemu10: a QEMU started with -sandbox on does not run (seccomp)" >&2
	exit 1
}
"$prefix/bin/qemu-img" --version | head -n1

echo "== done: $got"
echo "export BBRAFT_TEST_QEMU_PREFIX=$prefix"
