#!/usr/bin/env bash
# Jailed KVM acceptance (lead-owned gate). Runs the A10 harness
# (internal/node/jail_kvm_test.go, ^TestKVMJail) against the production
# bbraft-node main and the jailed test helper.
#
# Throwaway test VM only, never a workstation or a node. Run it from the
# repository root as the normal VM user with passwordless sudo; only the
# already-built test binary runs as root:
#
#   bash internal/node/jail-acceptance.sh [-run REGEXP]
#
# Phase 2 conformance starts a real Garage (the pinned garage binary on PATH,
# BBRAFT_TEST_S3_GARAGE=1 is set below).
#
# Needs /dev/kvm, /dev/vhost-vsock, /dev/vhost-net, /dev/net/tun, QEMU,
# systemd >= 254 on cgroup v2, mise with ansible (infra/ansible), and a built
# image under images/out/<version> (built here when missing:
# make -C images image). Environment overrides:
#   JAIL_ACCEPT_IMAGE   image version dir (default images/out/$(cat images/version))
#   JAIL_ACCEPT_RUN     test regexp (default ^TestKVMJail; -run wins)
#   JAIL_ACCEPT_MINUTES go test timeout in minutes (default 90; the outer
#                       kill bound is 5 minutes later, so the test's own
#                       timeout and cleanup always run first)
set -euo pipefail

run=${JAIL_ACCEPT_RUN:-^TestKVMJail}
if [ "${1:-}" = "-run" ] && [ -n "${2:-}" ]; then
	run=$2
fi
minutes=${JAIL_ACCEPT_MINUTES:-90}
timeout=${minutes}m
outer=$((minutes + 5))m

if [ "$(id -u)" = 0 ]; then
	echo "jail-acceptance: run as the normal user; only the test binary runs under sudo" >&2
	exit 2
fi
# The script lives in internal/node; test VMs hold a git archive (no .git).
root=$(cd "$(dirname "$0")/../.." && pwd)
cd "$root"
[ -f go.mod ] && [ -d infra/ansible ] || {
	echo "jail-acceptance: $root is not the repository root" >&2
	exit 1
}
for tool in go sudo mise lsblk findmnt sha256sum systemctl; do
	command -v "$tool" >/dev/null || {
		echo "jail-acceptance: $tool not found" >&2
		exit 1
	}
done

image=${JAIL_ACCEPT_IMAGE:-$root/images/out/$(cat images/version)}
if [ ! -d "$image" ]; then
	echo "== building the image (missing $image)"
	make -C images image
fi
[ -d "$image" ] || {
	echo "jail-acceptance: image $image not found after the build" >&2
	exit 1
}

dir=$(mktemp -d "${TMPDIR:-/tmp}/bbraft-jail-accept.XXXXXX")
units=$dir/units
trap 'rm -rf "$dir"' EXIT

# The whole disk under the data root (a partition walked up to its disk):
# the jail's IO limits are per device.
io_device() {
	local src dev parent
	src=$(findmnt -no SOURCE --target / | sed 's/\[.*//')
	dev=$src
	for _ in 1 2 3 4; do
		parent=$(lsblk -no PKNAME "$dev" 2>/dev/null | head -n1 | tr -d ' ')
		[ -n "$parent" ] || break
		dev=/dev/$parent
	done
	lsblk -dno MAJ:MIN "$dev" | tr -d ' '
}
io=$(io_device)
case $io in [1-9]*:[0-9]*) ;; *)
	echo "jail-acceptance: could not derive the root disk's MAJ:MIN (got '$io')" >&2
	exit 1
	;;
esac

echo "== building bbraft-node and node.test into $dir"
go build -o "$dir/bbraft-node" ./cmd/bbraft-node
go test -c -o "$dir/node.test" ./internal/node
sum=$(sha256sum "$dir/bbraft-node" | cut -d' ' -f1)

echo "== rendering the harness units (image $image, io $io)"
(
	cd infra/ansible
	mise exec -- ansible-playbook \
		-i roles/bbraft_node/tests/inventory.yml roles/bbraft_node/tests/kvm-harness-render.yml \
		-e kt_out="$units" -e kt_binary="$dir/bbraft-node" -e kt_binary_sha256="$sum" \
		-e kt_image="$image" -e kt_jail_uid_range=900000-900999 \
		-e kt_io_device="$io" -e kt_io_write_bps=104857600
)

echo "== running $run as root"
sudo env \
	BBRAFT_TEST_KVM_JAIL=1 BBRAFT_TEST_KVM=1 BBRAFT_TEST_REQUIRE_ALL=1 BBRAFT_TEST_S3_GARAGE=1 \
	BBRAFT_TEST_IMAGE_DIR="$image" \
	BBRAFT_TEST_KVM_JAIL_UNITS="$units" BBRAFT_TEST_KVM_JAIL_BIN="$dir" \
	${BBRAFT_TEST_CPU_MODEL:+BBRAFT_TEST_CPU_MODEL="$BBRAFT_TEST_CPU_MODEL"} \
	PATH="$PATH" \
	timeout --kill-after=120s "$outer" \
	"$dir/node.test" -test.run "$run" -test.v -test.count=1 -test.timeout "$timeout"
