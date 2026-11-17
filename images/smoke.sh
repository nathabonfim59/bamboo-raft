#!/usr/bin/env bash
# Boot a built image with unprivileged QEMU (KVM + vhost-vsock) and run the
# guest smoke client over AF_VSOCK. The smoke process never runs as root and
# kills only the QEMU process it started.
#
#   images/smoke.sh [--image-dir DIR] [--cid N] [--disk-gib N] [--timeout S]
#                   [--single-boot] [--keep] [--qemu PATH]
#
# By default it boots twice from the same overlay and requires a different
# per-boot ed25519 sshd host key on the second boot. --single-boot skips that
# comparison (one boot, no rotation evidence).
#
# On failure the serial logs and QEMU stderr are printed (bounded) and their
# path is kept. On success the temporary overlay/logs are removed unless
# --keep is given.
set -euo pipefail

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)
repo_root=$(cd -- "$script_dir/.." && pwd -P)
# shellcheck source-path=SCRIPTDIR
# shellcheck source=pins.sh
# shellcheck disable=SC1091
. "$script_dir/pins.sh"
GO=${GO:-go}
# test-only test override, read once at the entry boundary. Empty keeps the
# production default; host additionally needs the deliberate internal opt-in.
test_cpu_model=${BBRAFT_TEST_CPU_MODEL:-}
cpu_model=$test_cpu_model

image_dir="$IMAGE_OUT/$IMAGE_VERSION"
cid=42
disk_gib=12
timeout=240
keep=0
single_boot=0
qemu=${QEMU_SYSTEM_X86_64:-qemu-system-x86_64}
# Fixed CPU plus nested feature policy, resolved by the smoke client through
# the node's shared qemu helpers (never an independent shell policy, never
# -cpu host/max, always enforce). Phase 0.5 exposes no +svm.
cpu_nested=${BBRAFT_SMOKE_CPU_NESTED:-auto}
vsock_port=10789

die() {
	printf 'images/smoke: %s\n' "$*" >&2
	exit 1
}

usage() {
	cat <<'EOF'
usage: images/smoke.sh [options]
  --image-dir DIR   built image directory (default images/out/<version>)
  --cid N           guest vsock CID for QEMU (default 42)
  --disk-gib N      virtual disk size for the temporary overlay (default 12)
  --cpu-nested M    nested mode auto|on|off (default auto; env BBRAFT_SMOKE_CPU_NESTED)
  --timeout S       seconds to wait for the guest/client (default 240)
  --single-boot     boot once and skip the per-boot host key rotation check
  --keep            keep the overlay, serial log and QEMU stderr
  --qemu PATH       qemu-system-x86_64 binary (default from PATH)
EOF
}

while [ $# -gt 0 ]; do
	case "$1" in
	--image-dir)
		[ $# -ge 2 ] || { usage >&2; exit 2; }
		image_dir=$2
		shift 2
		;;
	--cid)
		[ $# -ge 2 ] || { usage >&2; exit 2; }
		cid=$2
		shift 2
		;;
	--disk-gib)
		[ $# -ge 2 ] || { usage >&2; exit 2; }
		disk_gib=$2
		shift 2
		;;
	--cpu-nested)
		[ $# -ge 2 ] || { usage >&2; exit 2; }
		cpu_nested=$2
		shift 2
		;;
	--timeout)
		[ $# -ge 2 ] || { usage >&2; exit 2; }
		timeout=$2
		shift 2
		;;
	--keep)
		keep=1
		shift
		;;
	--single-boot)
		single_boot=1
		shift
		;;
	--qemu)
		[ $# -ge 2 ] || { usage >&2; exit 2; }
		qemu=$2
		shift 2
		;;
	--help | -h)
		usage
		exit 0
		;;
	*)
		usage >&2
		exit 2
		;;
	esac
done

# Validate shell numeric arguments canonically before any arithmetic, host
# probe or QEMU start. Leading zeros and huge digit strings are rejected.
valid_uint() { # <value> <max-digits>
	case "$1" in
	"" | 0* | *[!0-9]*) return 1 ;;
	esac
	[ "${#1}" -le "$2" ]
}
valid_uint "$disk_gib" 4 && [ "$disk_gib" -ge 1 ] && [ "$disk_gib" -le 1024 ] ||
	die "--disk-gib must be an integer between 1 and 1024, got '$disk_gib'"
valid_uint "$cid" 10 && [ "$cid" -ge 3 ] && [ "$cid" -le 4294967294 ] ||
	die "--cid must be an integer between 3 and 4294967294 (0xffffffff is reserved), got '$cid'"
valid_uint "$timeout" 5 && [ "$timeout" -ge 1 ] && [ "$timeout" -le 86400 ] ||
	die "--timeout must be an integer between 1 and 86400 seconds, got '$timeout'"
case "$cpu_nested" in
auto | on | off) ;;
*) die "invalid --cpu-nested '$cpu_nested' (want auto, on or off)" ;;
esac

[ "$(id -u)" -ne 0 ] || die "run the smoke test unprivileged, not as root"
for tool in qemu-img curl sha256sum jq; do
	command -v "$tool" >/dev/null 2>&1 || die "missing host tool '$tool'"
done
[ -x "$qemu" ] || command -v "$qemu" >/dev/null 2>&1 || die "QEMU not found: $qemu"
[ -r /dev/kvm ] && [ -w /dev/kvm ] || die "/dev/kvm is not accessible to uid $(id -u); use a KVM host"
if [ ! -r /dev/vhost-vsock ] || [ ! -w /dev/vhost-vsock ]; then
	die "/dev/vhost-vsock is missing or not writable by uid $(id -u); vhost-vsock support is required"
fi
[ -f "$script_dir/smoke/main.go" ] ||
	die "images/smoke/main.go is missing from the smoke client source"
"$script_dir/verify.sh" "$image_dir"
base_gib=$(jq -r '.root.diskGiB' "$image_dir/manifest.json")
valid_uint "$base_gib" 4 && [ "$base_gib" -ge 1 ] && [ "$base_gib" -le 1024 ] ||
	die "manifest root.diskGiB is invalid: '$base_gib'"
if [ "$disk_gib" -lt "$base_gib" ]; then
	die "--disk-gib $disk_gib is smaller than the image base ${base_gib} GiB"
fi

work=$(mktemp -d "${TMPDIR:-/tmp}/bbraft-smoke.XXXXXX")
overlay="$work/root.qcow2"
qemu_pid=""
stop_qemu() {
	if [ -n "${qemu_pid:-}" ] && kill -0 "$qemu_pid" 2>/dev/null; then
		kill -TERM "$qemu_pid" 2>/dev/null || true
		for _ in 1 2 3 4 5 6 7 8 9 10; do
			kill -0 "$qemu_pid" 2>/dev/null || break
			sleep 1
		done
		if kill -0 "$qemu_pid" 2>/dev/null; then
			kill -KILL "$qemu_pid" 2>/dev/null || true
		fi
		wait "$qemu_pid" 2>/dev/null || true
	fi
	qemu_pid=""
}
cleanup() {
	status=$?
	trap - EXIT INT TERM
	stop_qemu
	if [ "$status" -ne 0 ] || [ "$keep" -eq 1 ]; then
		printf 'images/smoke: logs kept in %s\n' "$work" >&2
		for serial in "$work"/serial-*.log; do
			[ -f "$serial" ] || continue
			printf 'images/smoke: last serial lines (%s):\n' "$serial" >&2
			tail -n 200 "$serial" >&2 || true
		done
		for qemu_err in "$work"/qemu-*.err; do
			[ -s "$qemu_err" ] || continue
			printf 'images/smoke: QEMU stderr (%s):\n' "$qemu_err" >&2
			tail -n 100 "$qemu_err" >&2 || true
		done
	else
		rm -rf "$work"
	fi
	exit "$status"
}
trap cleanup EXIT INT TERM

# Build the client first so a compile error never leaves a QEMU running.
(
	cd "$repo_root"
	"$GO" build -trimpath -buildvcs=false -o "$work/smoke" ./images/smoke
)

# Resolve the CPU policy through the same shared Go helpers the node uses
# (qemu.DetectHostCPU + qemu.ResolveCPU); this is the smoke client's
# -resolve-cpu mode. The reason for a disabled nested feature goes to stderr.
resolve_args=(-resolve-cpu -cpu-nested "$cpu_nested")
if [ -n "$cpu_model" ]; then
	resolve_args+=(-cpu-model "$cpu_model")
fi
if [ "$cpu_model" = host ]; then
	resolve_args+=(-cpu-model-test-host)
fi
if ! resolved=$("$work/smoke" "${resolve_args[@]}"); then
	die "cannot resolve the CPU policy for nested=$cpu_nested"
fi
printf '%s\n' "$resolved" >&2
QEMU_CPU=$(printf '%s\n' "$resolved" | grep -o 'cpu=[^ ]*' | head -n 1 | cut -d= -f2-)
NESTED_FEATURE=$(printf '%s\n' "$resolved" | grep -o 'nested=[^ ]*' | head -n 1 | cut -d= -f2-)
[ -n "$QEMU_CPU" ] || die "cannot parse the resolved CPU from: $resolved"
case "$QEMU_CPU" in
*max* | *tcg*) die "resolver emitted a forbidden CPU '$QEMU_CPU'" ;;
*host*)
	# The Go resolver only emits host when the VM test opt-in was set; this
	# guard just catches a wrapper/forwarding mistake without re-deciding policy.
	[ "$cpu_model" = host ] || die "resolver emitted a forbidden CPU '$QEMU_CPU'"
	;;
esac
case "$QEMU_CPU" in
*,enforce) ;;
*) die "resolved CPU '$QEMU_CPU' does not end in enforce" ;;
esac
case "${NESTED_FEATURE:-}" in
"" | vmx | svm | none) ;;
*) die "resolved an invalid nested feature '${NESTED_FEATURE:-}'" ;;
esac
printf 'images/smoke: cpu=%s nested=%s\n' "$QEMU_CPU" "${NESTED_FEATURE:-none}" >&2

# Larger virtual disk than the image so the guest's grow unit is exercised.
qemu-img create -q -f qcow2 -b "$image_dir/root.qcow2" -F qcow2 \
	-o size="${disk_gib}G" "$overlay" ||
	die "cannot create the temporary qcow2 overlay"

# wait_qemu_exit <label> <seconds>; returns 0 only when QEMU exited by itself
# with status 0 (guest poweroff), after reaping it. The timeout path leaves the
# process for the caller's forced stop and returns 1.
wait_qemu_exit() {
	local label=$1 seconds=$2 status i
	for ((i = 0; i < seconds; i++)); do
		kill -0 "$qemu_pid" 2>/dev/null || break
		sleep 1
	done
	if kill -0 "$qemu_pid" 2>/dev/null; then
		return 1
	fi
	if wait "$qemu_pid"; then
		status=0
	else
		status=$?
	fi
	qemu_pid=""
	if [ "$status" -ne 0 ]; then
		die "QEMU boot $label exited with status $status during guest poweroff"
	fi
	return 0
}

# boot_and_check <label> <expect-root-gib> <poweroff 0|1>; one QEMU boot of
# the same overlay, full client run, output tee'd to $work/boot-<label>.log
# for cross-boot comparisons. A clean guest poweroff is required when
# <poweroff> is 1; SIGTERM/SIGKILL remain only the failure-path cleanup.
boot_and_check() {
	local label=$1 expect_gib=$2 power=$3
	local serial="$work/serial-$1.log" err="$work/qemu-$1.err" log="$work/boot-$1.log"
	local extra=()
	printf 'images/smoke: boot %s: %s (cid %s, disk %s GiB, serial %s)\n' \
		"$label" "$image_dir" "$cid" "$disk_gib" "$serial" >&2
	[ "$power" -eq 1 ] && extra+=(-poweroff)
	[ "$expect_gib" -gt 0 ] && extra+=(-expect-root-gib "$expect_gib")
	boot_start_ms=$(date +%s%3N)
	"$qemu" \
		-name "bbraft-smoke-$cid-$label" \
		-machine q35,accel=kvm \
		-cpu "$QEMU_CPU" \
		-nodefaults \
		-no-user-config \
		-no-reboot \
		-m 2048 \
		-smp 2 \
		-kernel "$image_dir/kernel" \
		-initrd "$image_dir/initrd" \
		-append 'console=ttyS0,115200 root=/dev/vda rootfstype=ext4 rw rootwait=1 systemd.unit=multi-user.target' \
		-drive "file=$overlay,if=virtio,format=qcow2,cache=writeback,discard=unmap" \
		-netdev user,id=net0 \
		-device virtio-net-pci,netdev=net0 \
		-device "vhost-vsock-pci,guest-cid=$cid" \
		-object rng-random,filename=/dev/urandom,id=rng0 \
		-device virtio-rng-pci,rng=rng0 \
		-serial "file:$serial" \
		-display none \
		-monitor none \
		-sandbox on \
		>>"$err" 2>&1 &
	qemu_pid=$!
	sleep 1
	if ! kill -0 "$qemu_pid" 2>/dev/null; then
		wait "$qemu_pid" 2>/dev/null || true
		qemu_pid=""
		die "QEMU exited during boot $label; see $err and $serial"
	fi
	"$work/smoke" -cid "$cid" -port "$vsock_port" -timeout "${timeout}s" \
		-boot-start-unix-ms "$boot_start_ms" -expect-nested "${NESTED_FEATURE:-none}" \
		-boot-label "$label" "${extra[@]}" \
		2>&1 | tee "$log"
	if [ "$power" -eq 1 ]; then
		# A clean poweroff flushes the disk, so the next boot can rely on the
		# authorized_keys and the filesystem the previous boot left behind.
		if ! wait_qemu_exit "$label" 45; then
			printf 'images/smoke: boot %s did not power off cleanly; forcing cleanup\n' "$label" >&2
			stop_qemu
			die "guest did not power off cleanly within 45s on boot $label"
		fi
	else
		stop_qemu
	fi
}

# Boot twice by default: the same overlay (the sandbox disk) is resumed while
# /run is fresh, so the second boot must advertise a different ed25519 host
# key. --single-boot is for quick manual checks; the acceptance run uses both.
boot_and_check 1 "$disk_gib" 1
host_key_1=$(sed -n 's/^PASS sshd host key ED25519 //p' "$work/boot-1.log" | tail -n 1)
case "$host_key_1" in
SHA256:?*) ;;
*) die "boot 1 did not report an ed25519 sshd host key fingerprint" ;;
esac

if [ "$single_boot" -eq 0 ]; then
	# Grow the stopped sandbox disk, then boot it again: the guest must grow
	# the filesystem to the new device size before it reports readiness, and
	# authorized_keys written on boot 1 must still be there.
	grown_gib=$((disk_gib + 2))
	qemu-img resize "$overlay" "${grown_gib}G" ||
		die "cannot grow the stopped overlay to ${grown_gib} GiB"
	printf 'images/smoke: grew the stopped overlay %s -> %s GiB\n' "$disk_gib" "$grown_gib" >&2
	boot_and_check 2 "$grown_gib" 1
	host_key_2=$(sed -n 's/^PASS sshd host key ED25519 //p' "$work/boot-2.log" | tail -n 1)
	case "$host_key_2" in
	SHA256:?*) ;;
	*) die "boot 2 did not report an ed25519 sshd host key fingerprint" ;;
	esac
	if [ "$host_key_1" = "$host_key_2" ]; then
		die "sshd host key did not rotate across boots ($host_key_1)"
	fi
	printf 'images/smoke: host key rotated across boots: %s -> %s\n' \
		"$host_key_1" "$host_key_2" >&2
else
	printf 'images/smoke: --single-boot: host key rotation and resume growth not checked\n' >&2
fi
printf 'images/smoke: PASS\n' >&2
