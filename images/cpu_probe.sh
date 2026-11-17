#!/usr/bin/env bash
# Thin wrapper for the CPU-only QEMU probe (the smoke client's -probe-cpu
# mode). The probe resolves the shared CPU policy through the node's qemu
# helpers, launches an unprivileged paused QEMU with the enforced fixed model,
# proves via QMP (greeting, peer PID, prelaunch/paused status, realized CPUs)
# that this host can provide that CPU, then stops the child with a bounded
# quit-then-kill and proves its whole process group is gone. This file only
# parses arguments and builds the client: it execs the Go binary, so the
# wrapper PID *is* the client — a SIGINT/SIGTERM reaches the client's
# signal.NotifyContext directly, its independent cleanup still reaps the QEMU
# child, and its exit status propagates unchanged. No shell signal forwarding
# is needed (or safe: a shell trap cannot reliably carry the exit status).
#
#   images/cpu_probe.sh [--qemu PATH] [--cpu-nested auto|on|off]
#                       [--cpuinfo PATH] [--probe-dir DIR]
#                       [--kvm-device PATH] [--timeout S] [--grace S]
#
# Run unprivileged on a KVM host. No image, boot or network is involved, so
# this is the cheap gate `make review` runs before the mkosi build. The
# client binary is kept in the gitignored images/.cache/cpu-probe/ build
# cache; nothing else is written outside --probe-dir.
set -euo pipefail

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)
repo_root=$(cd -- "$script_dir/.." && pwd -P)
GO=${GO:-go}

qemu_bin=${QEMU_SYSTEM_X86_64:-qemu-system-x86_64}
cpu_nested=${BBRAFT_SMOKE_CPU_NESTED:-auto}
# Read the test-only test model override once at the entry boundary. An
# explicit host is the only model that needs the deliberate internal opt-in;
# unset (or any other value) keeps the production resolver path.
test_cpu_model=${BBRAFT_TEST_CPU_MODEL:-}
cpu_model=$test_cpu_model
cpu_model_test_host=0
cpuinfo=""
probe_dir=""
kvm_device=/dev/kvm
timeout=20
grace=5

die() {
	printf 'images/cpu-probe: %s\n' "$*" >&2
	exit 1
}

usage() {
	cat <<'EOF'
usage: images/cpu_probe.sh [options]
  --qemu PATH       qemu-system-x86_64 binary (default from PATH or
                    QEMU_SYSTEM_X86_64)
  --cpu-nested M    nested mode auto|on|off (default auto; env
                    BBRAFT_SMOKE_CPU_NESTED)
  --cpu-model M     fixed CPU model (default env BBRAFT_TEST_CPU_MODEL, empty
                    means the production default; host needs the test opt-in)
  --cpuinfo PATH    host cpuinfo file to resolve against (default
                    /proc/cpuinfo; for tests)
  --probe-dir DIR   probe work directory for the QMP socket (default a fresh
                    temp dir)
  --kvm-device PATH device the probe must open read-write before launching
                    (default /dev/kvm; a writable ordinary file for tests)
  --timeout S       seconds to wait for QMP readiness (default 20)
  --grace S         seconds between QMP quit and SIGKILL (default 5)
EOF
}

while [ $# -gt 0 ]; do
	case "$1" in
	--qemu)
		[ $# -ge 2 ] || { usage >&2; exit 2; }
		qemu_bin=$2
		shift 2
		;;
	--cpu-nested)
		[ $# -ge 2 ] || { usage >&2; exit 2; }
		cpu_nested=$2
		shift 2
		;;
	--cpu-model)
		[ $# -ge 2 ] || { usage >&2; exit 2; }
		cpu_model=$2
		shift 2
		;;
	--cpuinfo)
		[ $# -ge 2 ] || { usage >&2; exit 2; }
		cpuinfo=$2
		shift 2
		;;
	--probe-dir)
		[ $# -ge 2 ] || { usage >&2; exit 2; }
		probe_dir=$2
		shift 2
		;;
	--kvm-device)
		[ $# -ge 2 ] || { usage >&2; exit 2; }
		kvm_device=$2
		shift 2
		;;
	--timeout)
		[ $# -ge 2 ] || { usage >&2; exit 2; }
		timeout=$2
		shift 2
		;;
	--grace)
		[ $# -ge 2 ] || { usage >&2; exit 2; }
		grace=$2
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

# The opt-in is derived only after --cpu-model parsing: an inherited
# BBRAFT_TEST_CPU_MODEL=host must not force the opt-in when an explicit fixed
# model overrides it. Both the env-selected model and the final model must be
# exactly host.
if [ "$test_cpu_model" = host ] && [ "$cpu_model" = host ]; then
	cpu_model_test_host=1
fi

# Canonical numeric validation before anything runs.
valid_uint() { # <value> <max-digits>
	case "$1" in
	"" | 0* | *[!0-9]*) return 1 ;;
	esac
	[ "${#1}" -le "$2" ]
}
valid_uint "$timeout" 4 && [ "$timeout" -ge 1 ] && [ "$timeout" -le 3600 ] ||
	die "--timeout must be an integer between 1 and 3600 seconds, got '$timeout'"
valid_uint "$grace" 3 && [ "$grace" -ge 1 ] && [ "$grace" -le 600 ] ||
	die "--grace must be an integer between 1 and 600 seconds, got '$grace'"
case "$cpu_nested" in
	auto | on | off) ;;
	*) die "invalid --cpu-nested '$cpu_nested' (want auto, on or off)" ;;
esac

[ "$(id -u)" -ne 0 ] || die "run the CPU probe unprivileged, not as root"
[ -f "$script_dir/smoke/main.go" ] ||
	die "images/smoke/main.go is missing from the smoke client source"
[ -d "$repo_root/internal/proto/guestv1" ] ||
	die "internal/proto/guestv1 is missing; the smoke client needs the guest stubs"
[ -x "$qemu_bin" ] || command -v "$qemu_bin" >/dev/null 2>&1 ||
	die "QEMU not found: $qemu_bin"

# Build the client into the retained (gitignored) cache first so a compile
# error never starts anything.
cache="$script_dir/.cache/cpu-probe"
mkdir -p "$cache"
(
	cd "$repo_root"
	"$GO" build -trimpath -buildvcs=false -o "$cache/smoke" ./images/smoke
)

args=(-probe-cpu -qemu "$qemu_bin" -cpu-nested "$cpu_nested" -kvm-device "$kvm_device" -probe-timeout "${timeout}s" -probe-grace "${grace}s")
if [ -n "$cpu_model" ]; then
	args+=(-cpu-model "$cpu_model")
fi
if [ "$cpu_model_test_host" -eq 1 ]; then
	args+=(-cpu-model-test-host)
fi
[ -n "$cpuinfo" ] && args+=(-cpuinfo "$cpuinfo")
[ -n "$probe_dir" ] && args+=(-probe-dir "$probe_dir")
exec "$cache/smoke" "${args[@]}"
