#!/usr/bin/env bash
# End-to-end regression for the shared CPU resolver and the CPU-only QEMU
# probe, using fake QEMU binaries only. No real QEMU is launched, nothing
# needs KVM, and the /dev/kvm preflight is pointed at an ordinary file.
#
# Coverage required by the review:
#   - the resolver matrix: Intel+vmx auto -> +vmx; AMD (svm) auto -> no
#     nested feature with an explained reason (phase 0.5 exposes no +svm);
#     AMD on -> clear error; off -> none; invalid mode/model -> error
#   - probe success proves real QMP readiness: greeting handshake, peer PID
#     equality, query-status prelaunch/paused with running=false (QEMU 8.2.2
#     RunState semantics), nonempty query-cpus-fast
#   - probe failures: wrong states, empty/omitted fields, early fatal child,
#     alive-but-no-QMP timeout, wrong socket peer, cleanup escalation to
#     SIGKILL with SIGTERM ignored, and no launch when resolution or the KVM
#     device check fails
set -euo pipefail

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)
repo_root=$(cd -- "$script_dir/.." && pwd -P)
probe="$script_dir/cpu_probe.sh"
fake_qmp="$script_dir/fake_qmp.py"
GO=${GO:-go}

if [ ! -d "$repo_root/internal/proto/guestv1" ]; then
	printf 'cpu_probe_test: skip: internal/proto/guestv1 is not in this worktree yet\n' >&2
	exit 0
fi

tmp=$(mktemp -d "${TMPDIR:-/tmp}/bbraft-cpu-probe-test.XXXXXX")
: >"$tmp/groups"
# Independent safety net: whatever the probe did or failed to do, terminate
# exactly the process groups the fakes recorded (leader pid = group id), so a
# buggy baseline run cannot leak fake processes into the machine. The log is
# append-only so a later case can never erase an earlier group record.
cleanup() {
	local p
	while read -r p; do
		[ -n "$p" ] || continue
		kill -KILL -- "-$p" 2>/dev/null || true
	done <"$tmp/groups"
	rm -rf "$tmp"
}
trap cleanup EXIT

failures=0
pass() {
	printf 'PASS %s\n' "$*"
}
fail() {
	printf 'FAIL %s\n' "$*" >&2
	failures=$((failures + 1))
}
alive() { kill -0 "$1" 2>/dev/null; }

# Build the client once; every case below drives the same binary the wrapper
# builds (and the wrapper rebuild is cached).
client="$tmp/smoke"
(
	cd "$repo_root"
	"$GO" build -trimpath -buildvcs=false -o "$client" ./images/smoke
)

# --- fixtures ---------------------------------------------------------------

printf 'processor\t: 0\nvendor_id\t: GenuineIntel\nflags\t\t: fpu vmx sse4_2\n' >"$tmp/intel"
printf 'processor\t: 0\nvendor_id\t: AuthenticAMD\nflags\t\t: fpu svm sse4_2\n' >"$tmp/amd"
printf 'processor\t: 0\nvendor_id\t: GenuineIntel\nflags\t\t: fpu sse4_2\n' >"$tmp/plain"

# A fake QEMU that exits 1 immediately with a QEMU-style stderr line.
fake_fatal="$tmp/fake-fatal"
	cat >"$fake_fatal" <<'EOF'
#!/usr/bin/env bash
printf '%s\n' "$@" >"$FAKE_ARGV"
printf '%s\n' "$$" >"$FAKE_PID"
echo "qemu: this KVM host cannot provide the enforced CPU feature" >&2
exit 1
EOF
chmod +x "$fake_fatal"

# A fake QEMU that stays alive but never creates the QMP socket.
fake_noqmp="$tmp/fake-noqmp"
	cat >"$fake_noqmp" <<'EOF'
#!/usr/bin/env bash
printf '%s\n' "$@" >"$FAKE_ARGV"
printf '%s\n' "$$" >"$FAKE_PID"
trap 'exit 143' TERM INT
while :; do sleep 1; done
EOF
chmod +x "$fake_noqmp"

# A fake QEMU whose direct child stays alive while a grandchild serves QMP:
# the socket peer pid differs from the probe's child pid.
fake_wrongpeer="$tmp/fake-wrongpeer"
	cat >"$fake_wrongpeer" <<'EOF'
#!/usr/bin/env bash
printf '%s\n' "$@" >"$FAKE_ARGV"
printf '%s\n' "$$" >"$FAKE_PID"
python3 "$FAKE_QMP" &
py=$!
printf '%s\n' "$py" >"$FAKE_PID2"
trap 'kill "$py" 2>/dev/null' TERM INT
wait "$py" || true
EOF
chmod +x "$fake_wrongpeer"

chmod +x "$fake_qmp"

probe_dir="$tmp/probe"
socket="$probe_dir/qmp.sock"

run_probe() { # <extra args...>; captures stdout/stderr and exit code
	# Isolate the test VM base's inherited BBRAFT_TEST_CPU_MODEL: default cases
	# must see the production default, and forwarding cases opt in explicitly
	# through PROBE_TEST_CPU_MODEL.
	local cpu_model_env=${PROBE_TEST_CPU_MODEL-}
	rm -rf "$probe_dir"
	rm -f "$tmp/pid" "$tmp/pid2" "$tmp/desc"
	BBRAFT_TEST_CPU_MODEL="$cpu_model_env" \
		FAKE_SOCKET="$socket" FAKE_QMP="$fake_qmp" FAKE_ARGV="$tmp/argv" \
		FAKE_PID="$tmp/pid" FAKE_PID2="$tmp/pid2" FAKE_TERM_FILE="$tmp/term" \
		FAKE_DESC="$tmp/desc" \
		"$probe" --kvm-device /dev/null --probe-dir "$probe_dir" "$@" \
		>"$tmp/out" 2>"$tmp/err"
	status=$?
	# Record every group a fake reported owning, for the EXIT safety net.
	local f p
	for f in "$tmp/pid" "$tmp/pid2"; do
		[ -s "$f" ] || continue
		p=$(cat "$f")
		case "$p" in '' | *[!0-9]*) ;; *) printf '%s\n' "$p" >>"$tmp/groups" ;; esac
	done
	return "$status"
}

# --- resolver matrix --------------------------------------------------------

resolver_case() { # <label> <want-exit> <args...> <grep-args...>
	local label=$1 want_exit=$2
	shift 2
	local -a client_args=() greps=()
	local arg
	local collecting=client
	for arg in "$@"; do
		if [ "$arg" = "--GREP--" ]; then
			collecting="grep"
			continue
		fi
		if [ "$collecting" = client ]; then
			client_args+=("$arg")
		else
			greps+=("$arg")
		fi
	done
	local out
	if out=$("$client" -resolve-cpu "${client_args[@]}" 2>"$tmp/rerr"); then
		rc=0
	else
		rc=$?
	fi
	if [ "$rc" -eq 0 ] && [ "$want_exit" = fail ]; then
		fail "$label: resolver accepted what it must reject"
		return
	fi
	if [ "$rc" -ne 0 ] && [ "$want_exit" = pass ]; then
		fail "$label: resolver failed: $(cat "$tmp/rerr")"
		return
	fi
	local g
	for g in "${greps[@]}"; do
		if grep -q -- "$g" <<<"$out$(cat "$tmp/rerr")"; then
			:
		else
			fail "$label: output does not mention '$g': $out / $(cat "$tmp/rerr")"
			return
		fi
	done
	pass "$label"
}

resolver_case "intel vmx auto resolves +vmx" pass \
	-cpuinfo "$tmp/intel" --GREP-- 'cpu=Haswell-noTSX,+vmx,enforce' 'nested=vmx'
resolver_case "amd svm auto adds nothing" pass \
	-cpuinfo "$tmp/amd" --GREP-- 'cpu=Haswell-noTSX,enforce' 'nested=none'
resolver_case "amd auto reason names the vendor" pass \
	-cpuinfo "$tmp/amd" --GREP-- 'AuthenticAMD'
resolver_case "plain intel auto adds nothing" pass \
	-cpuinfo "$tmp/plain" --GREP-- 'cpu=Haswell-noTSX,enforce' 'nested=none'
resolver_case "intel off never nests" pass \
	-cpuinfo "$tmp/intel" -cpu-nested off --GREP-- 'cpu=Haswell-noTSX,enforce' 'nested=none'
resolver_case "amd on fails clearly" fail \
	-cpuinfo "$tmp/amd" -cpu-nested on --GREP-- 'AuthenticAMD'
resolver_case "off needs no host facts at all" pass \
	-cpuinfo "$tmp/no-such-file" -cpu-nested off --GREP-- 'cpu=Haswell-noTSX,enforce' 'nested=none'
resolver_case "auto detection failure resolves none with a reason" pass \
	-cpuinfo "$tmp/no-such-file" --GREP-- 'cpu=Haswell-noTSX,enforce' 'nested=none' 'detection failed'
resolver_case "on detection failure fails clearly" fail \
	-cpuinfo "$tmp/no-such-file" -cpu-nested on --GREP-- 'cpu-nested=on'
resolver_case "invalid mode rejected" fail \
	-cpuinfo "$tmp/intel" -cpu-nested bogus --GREP-- 'invalid cpu-nested mode'
resolver_case "host rejected without test opt-in" fail \
	-cpuinfo "$tmp/intel" -cpu-model host --GREP-- 'invalid fixed CPU model'
resolver_case "test opt-in requires host model" fail \
	-cpuinfo "$tmp/intel" -cpu-model-test-host --GREP-- 'requires -cpu-model host'
resolver_case "test opt-in allows explicit host" pass \
	-cpuinfo "$tmp/intel" -cpu-model host -cpu-model-test-host --GREP-- 'cpu=host,+vmx,enforce'
resolver_case "test opt-in host on amd is host,enforce" pass \
	-cpuinfo "$tmp/amd" -cpu-model host -cpu-model-test-host --GREP-- 'cpu=host,enforce'
resolver_case "max rejected without test opt-in" fail \
	-cpuinfo "$tmp/intel" -cpu-model max --GREP-- 'invalid fixed CPU model'

# --- probe: QMP-proven success ----------------------------------------------

if run_probe --qemu "$fake_qmp" --cpuinfo "$tmp/intel"; then
	pass "probe passes with prelaunch status on an Intel fixture"
else
	fail "probe failed with a well-behaved fake: $(cat "$tmp/err")"
fi
if grep -q '^PASS probe-cpu cpu=Haswell-noTSX,+vmx,enforce status=prelaunch cpus=1 qemu=8.2.2 pid=' "$tmp/out"; then
	pass "probe PASS line carries cpu/status/cpus/version evidence"
else
	fail "probe PASS line missing evidence: $(cat "$tmp/out")"
fi

# The probe argv is the shared-resolved CPU, q35+kvm, paused, QMP-only, and
# never carries a kernel/initrd/disk or TCG fallback.
argv_ok=1
grep -qx 'q35,accel=kvm' "$tmp/argv" || argv_ok=0
grep -qx 'Haswell-noTSX,+vmx,enforce' "$tmp/argv" || argv_ok=0
grep -qx -- '-S' "$tmp/argv" || argv_ok=0
grep -qx "unix:$socket,server=on,wait=off" "$tmp/argv" || argv_ok=0
if grep -qx -- '-kernel' "$tmp/argv" || grep -qx -- '-initrd' "$tmp/argv" ||
	grep -qx -- '-drive' "$tmp/argv" || grep -q 'tcg' "$tmp/argv"; then
	argv_ok=0
fi
if [ "$argv_ok" -eq 1 ]; then
	pass "probe argv is CPU-only and shared-resolved"
else
	fail "probe argv is wrong: $(cat "$tmp/argv")"
fi

# test-only explicit host override: the wrapper reads BBRAFT_TEST_CPU_MODEL at
# the entry boundary and forwards -cpu-model host plus the deliberate internal
# opt-in. The production default refusal is covered by the resolver cases.
rm -f "$tmp/argv"
if PROBE_TEST_CPU_MODEL=host run_probe --qemu "$fake_qmp" --cpuinfo "$tmp/intel"; then
	pass "env host override probe passes"
else
	fail "env host override probe failed: $(cat "$tmp/err")"
fi
if grep -qx 'host,+vmx,enforce' "$tmp/argv"; then
	pass "env host override reaches the QEMU argv"
else
	fail "env host override missing from QEMU argv: $(cat "$tmp/argv" 2>/dev/null)"
fi
if grep -q 'TEST OVERRIDE' "$tmp/err"; then
	pass "env host override is logged as a test override"
else
	fail "env host override notice missing: $(cat "$tmp/err")"
fi
if grep -q '^PASS probe-cpu cpu=host,+vmx,enforce ' "$tmp/out"; then
	pass "env host override PASS line carries the explicit host model"
else
	fail "env host override PASS line wrong: $(cat "$tmp/out")"
fi

# An explicit fixed --cpu-model beats an inherited env host: the wrapper must
# not forward the test opt-in, and the production resolver path is used.
rm -f "$tmp/argv"
if PROBE_TEST_CPU_MODEL=host run_probe --qemu "$fake_qmp" --cpuinfo "$tmp/intel" \
	--cpu-model Haswell-noTSX; then
	pass "explicit fixed model beats the inherited env host"
else
	fail "explicit fixed model with env host failed: $(cat "$tmp/err")"
fi
if grep -qx 'Haswell-noTSX,+vmx,enforce' "$tmp/argv"; then
	pass "explicit fixed model reaches the QEMU argv without the host opt-in"
else
	fail "explicit fixed model argv wrong: $(cat "$tmp/argv" 2>/dev/null)"
fi
if grep -q 'TEST OVERRIDE' "$tmp/err"; then
	fail "explicit fixed model still logged the host test override"
else
	pass "explicit fixed model does not log the host test override"
fi

rm -f "$tmp/argv"
if PROBE_TEST_CPU_MODEL=max run_probe --qemu "$fake_qmp" --cpuinfo "$tmp/intel" 2>/dev/null; then
	fail "env max override accepted"
else
	if grep -q 'invalid fixed CPU model' "$tmp/err"; then
		pass "env max override still rejected"
	else
		fail "env max rejection missing: $(cat "$tmp/err")"
	fi
fi
if [ ! -e "$tmp/argv" ]; then
	pass "no qemu launched for an env max override"
else
	fail "qemu launched despite an env max override"
fi

rm -f "$tmp/argv"
if run_probe --qemu "$fake_qmp" --cpuinfo "$tmp/intel" \
	--cpu-model host 2>/dev/null; then
	fail "host accepted via --cpu-model without the env opt-in"
else
	if grep -q 'invalid fixed CPU model' "$tmp/err"; then
		pass "host via --cpu-model without the env opt-in is refused"
	else
		fail "no-env host refusal missing: $(cat "$tmp/err")"
	fi
fi
if [ ! -e "$tmp/argv" ]; then
	pass "no qemu launched for a no-env host model"
else
	fail "qemu launched despite a no-env host model"
fi

# paused is the other accepted state (QEMU 8.2.2 RunState).
if FAKE_STATUS=paused run_probe --qemu "$fake_qmp" --cpuinfo "$tmp/intel" &&
	grep -q 'status=paused' "$tmp/out"; then
	pass "probe accepts paused with running=false"
else
	fail "probe rejected a correctly paused fake: $(cat "$tmp/out" 2>/dev/null) $(cat "$tmp/err" 2>/dev/null)"
fi

# The fake exits on quit; the probe must have reaped it.
fake_pid=$(cat "$tmp/pid")
if ! alive "$fake_pid"; then
	pass "probe child reaped after quit"
else
	fail "probe child pid $fake_pid is still alive after the probe"
fi

# A leader that exits on quit while a background descendant survives in the
# owned group (holding the inherited stderr) is NOT a successful cleanup: the
# probe must terminate the whole group, not just reap the leader.
rm -f "$tmp/argv" "$tmp/pid" "$tmp/desc"
if FAKE_LEAVE_DESCENDANT=1 run_probe --qemu "$fake_qmp" --cpuinfo "$tmp/intel"; then
	pass "probe completes when the leader exits on quit leaving a descendant"
else
	fail "probe failed on the leader-exits-descendant case: $(cat "$tmp/err")"
fi
if [ -s "$tmp/desc" ]; then
	desc_pid=$(cat "$tmp/desc")
	if alive "$desc_pid"; then
		fail "descendant pid $desc_pid survived a supposedly successful probe"
	else
		pass "left-behind descendant terminated by group cleanup"
	fi
	leader_pid=$(cat "$tmp/pid")
	if kill -0 -- "-$leader_pid" 2>/dev/null; then
		fail "process group $leader_pid still has live members after the probe"
	else
		pass "process group proven empty after the probe"
	fi
else
	fail "descendant fixture did not record its pid"
fi

# --- probe: rejected states and malformed readiness -------------------------

if FAKE_STATUS=running FAKE_RUNNING=true run_probe --qemu "$fake_qmp" --cpuinfo "$tmp/intel" 2>/dev/null; then
	fail "probe accepted a running machine"
else
	if grep -q 'status="running"' "$tmp/err"; then
		pass "probe rejects a running machine"
	else
		fail "running rejection missing status evidence: $(cat "$tmp/err")"
	fi
fi

if FAKE_CPUS=none run_probe --qemu "$fake_qmp" --cpuinfo "$tmp/intel" 2>/dev/null; then
	fail "probe accepted an empty query-cpus-fast"
else
	if grep -q 'query-cpus-fast returned 0 CPUs' "$tmp/err"; then
		pass "probe rejects empty query-cpus-fast"
	else
		fail "empty-cpus rejection does not name the query: $(cat "$tmp/err")"
	fi
fi

if FAKE_CPUS=bare run_probe --qemu "$fake_qmp" --cpuinfo "$tmp/intel" 2>/dev/null; then
	fail "probe accepted a cpu entry with no fields"
else
	if grep -q 'no cpu-index' "$tmp/err"; then
		pass "probe rejects a bare cpu entry"
	else
		fail "bare-entry rejection does not name cpu-index: $(cat "$tmp/err")"
	fi
fi

if FAKE_CPUS=two run_probe --qemu "$fake_qmp" --cpuinfo "$tmp/intel" 2>/dev/null; then
	fail "probe accepted two cpus for -smp 1"
else
	if grep -q '2 CPUs, want exactly 1' "$tmp/err"; then
		pass "probe requires the expected single cpu"
	else
		fail "two-cpus rejection does not name the count: $(cat "$tmp/err")"
	fi
fi

if FAKE_CPUS=nothread run_probe --qemu "$fake_qmp" --cpuinfo "$tmp/intel" 2>/dev/null; then
	fail "probe accepted a cpu entry without a thread id"
else
	if grep -q 'no host thread-id' "$tmp/err"; then
		pass "probe rejects a cpu entry without a thread id"
	else
		fail "nothread rejection does not name the field: $(cat "$tmp/err")"
	fi
fi

if FAKE_OMIT=1 run_probe --qemu "$fake_qmp" --cpuinfo "$tmp/intel" 2>/dev/null; then
	fail "probe accepted a missing status field"
else
	if grep -q 'status=""' "$tmp/err"; then
		pass "probe rejects a missing status field"
	else
		fail "missing-status rejection does not name the field: $(cat "$tmp/err")"
	fi
fi

if FAKE_OMIT_RUNNING=1 run_probe --qemu "$fake_qmp" --cpuinfo "$tmp/intel" 2>/dev/null; then
	fail "probe accepted a missing running field"
else
	if grep -q 'running=<missing>' "$tmp/err"; then
		pass "probe rejects a missing running field"
	else
		fail "missing-running rejection does not name the field: $(cat "$tmp/err")"
	fi
fi

# --- probe: fatal child, no-QMP timeout, wrong peer --------------------------

rm -f "$tmp/argv"
if run_probe --qemu "$fake_fatal" --cpuinfo "$tmp/intel" 2>/dev/null; then
	fail "probe accepted a qemu that exited fatally"
else
	if grep -q 'cannot provide the enforced CPU feature' "$tmp/err"; then
		pass "fatal qemu stderr is surfaced"
	else
		fail "fatal qemu stderr not surfaced: $(cat "$tmp/err")"
	fi
fi

rm -f "$tmp/argv" "$tmp/pid"
if run_probe --qemu "$fake_noqmp" --cpuinfo "$tmp/intel" --timeout 2 2>/dev/null; then
	fail "probe accepted a child that never served QMP"
else
	if grep -q 'QMP not ready' "$tmp/err"; then
		pass "no-QMP timeout fails with evidence"
	else
		fail "no-QMP failure does not name QMP: $(cat "$tmp/err")"
	fi
fi
noqmp_pid=$(cat "$tmp/pid")
if alive "$noqmp_pid"; then
	fail "no-QMP child survived the failed probe"
else
	pass "no-QMP child killed and reaped"
fi

rm -f "$tmp/argv" "$tmp/pid" "$tmp/pid2"
if run_probe --qemu "$fake_wrongpeer" --cpuinfo "$tmp/intel" 2>/dev/null; then
	fail "probe accepted a socket owned by another process"
else
	if grep -q 'owned by pid' "$tmp/err"; then
		pass "wrong socket peer rejected"
	else
		fail "wrong-peer rejection does not name the owner: $(cat "$tmp/err")"
	fi
fi
wrongpeer_child=$(cat "$tmp/pid")
wrongpeer_py=$(cat "$tmp/pid2")
if alive "$wrongpeer_child" || alive "$wrongpeer_py"; then
	fail "wrong-peer processes survived the failed probe (child=$wrongpeer_child py=$wrongpeer_py)"
else
	pass "wrong-peer processes killed and reaped"
fi

# --- probe: cleanup escalation when quit and SIGTERM are ignored -------------

rm -f "$tmp/term" "$tmp/pid"
if FAKE_IGNORE_QUIT=1 run_probe --qemu "$fake_qmp" --cpuinfo "$tmp/intel" --grace 2; then
	pass "probe still proves readiness when the child ignores quit"
else
	fail "probe failed on a quit-ignoring child: $(cat "$tmp/err")"
fi
if grep -q 'TERM' "$tmp/term" 2>/dev/null; then
	pass "escalation delivered SIGTERM before SIGKILL"
else
	fail "SIGTERM was never delivered: $(cat "$tmp/term" 2>/dev/null)"
fi
ignore_pid=$(cat "$tmp/pid")
if alive "$ignore_pid"; then
	fail "quit/TERM-ignoring child survived the probe (pid $ignore_pid)"
else
	pass "quit/TERM-ignoring child killed and reaped"
fi

# --- probe: manual SIGTERM to the wrapper -------------------------------------

# Interrupt regressions run through a python driver: it launches the wrapper
# in its own session (start_new_session) so only the exact wrapper PID is
# signalled, waits for a recorded readiness condition, sends SIGTERM, and
# reports the outcome. Two windows are covered: readiness in flight (the fake
# stalls on query-status) and the cleanup window (readiness proven, the fake
# has received quit and ignores TERM). Both must exit nonzero without PASS
# and leave no live process in the fake's recorded group.
cat >"$tmp/sigdriver.py" <<'PYEOF'
#!/usr/bin/env python3
"""Interrupt driver for cpu_probe_test.sh.

usage: sigdriver.py <midflight|cleanup> <probe> <fake_qmp> <cpuinfo>
                    <probe_dir> <pid_file> <argv_file> <stage_file>
                    <out_file> <err_file> <budget_seconds>
"""
import os
import signal
import subprocess
import sys
import time

(mode, probe, fake_qmp, cpuinfo, probe_dir, pid_file, argv_file,
 stage_file, out_file, err_file, budget) = sys.argv[1:12]
budget = float(budget)
socket = os.path.join(probe_dir, "qmp.sock")

env = dict(os.environ)
env["BBRAFT_TEST_CPU_MODEL"] = ""
env.update({
    "FAKE_SOCKET": socket,
    "FAKE_QMP": fake_qmp,
    "FAKE_ARGV": argv_file,
    "FAKE_PID": pid_file,
    "FAKE_STAGE_FILE": stage_file,
})
if mode == "midflight":
    env["FAKE_SLOW_STATUS"] = "60"
else:
    env["FAKE_IGNORE_QUIT"] = "1"

os.makedirs(probe_dir, exist_ok=True)
p = subprocess.Popen(
    [probe, "--kvm-device", "/dev/null", "--probe-dir", probe_dir,
     "--qemu", fake_qmp, "--cpuinfo", cpuinfo, "--timeout", "90",
     "--grace", "2"],
    env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    start_new_session=True, text=True,
)


def ready():
    try:
        if mode == "midflight":
            return (os.path.getsize(argv_file) > 0 and
                    os.path.getsize(pid_file) > 0)
        with open(stage_file) as f:
            return "quit-received" in f.read()
    except FileNotFoundError:
        return False


deadline = time.time() + 30
while not ready():
    if time.time() > deadline or p.poll() is not None:
        print(f"DRIVER-FAIL readiness mode={mode} wrapper_poll={p.poll()}")
        sys.exit(3)
    time.sleep(0.1)

os.kill(p.pid, signal.SIGTERM)
try:
    out, err = p.communicate(timeout=budget)
    print(f"DRIVER-OK mode={mode} rc={p.returncode}")
except subprocess.TimeoutExpired:
    p.kill()
    out, err = p.communicate()
    print(f"DRIVER-FAIL timeout mode={mode} rc={p.returncode}")
    sys.exit(4)
with open(out_file, "w") as f:
    f.write(out or "")
with open(err_file, "w") as f:
    f.write(err or "")
sys.exit(0)
PYEOF

sigcase() { # <mode> <label-prefix>
	local mode=$1 prefix=$2 budget grace=2
	budget=$((grace * 6 + 20 + 40))
	rm -rf "$probe_dir"
	rm -f "$tmp/argv" "$tmp/pid" "$tmp/desc" "$tmp/stage"
	driver_out=$(python3 "$tmp/sigdriver.py" "$mode" "$probe" "$fake_qmp" \
		"$tmp/intel" "$probe_dir" "$tmp/pid" "$tmp/argv" "$tmp/stage" \
		"$tmp/sig-out" "$tmp/sig-err" "$budget" 2>&1) || true
	[ -s "$tmp/pid" ] && printf '%s\n' "$(cat "$tmp/pid")" >>"$tmp/groups"
	case "$driver_out" in
	DRIVER-FAIL*)
		fail "$prefix: driver reported $driver_out"
		return
		;;
	esac
	rc=$(printf '%s\n' "$driver_out" | sed -n 's/.*rc=\(-\{0,1\}[0-9]\{1,\}\).*/\1/p')
	if [ -n "$rc" ] && [ "$rc" -ne 0 ]; then
		pass "$prefix: interrupted wrapper exits nonzero (rc=$rc)"
	else
		fail "$prefix: interrupted wrapper exited rc=${rc:-unknown}: $driver_out"
		return
	fi
	if grep -q 'PASS probe-cpu' "$tmp/sig-out" 2>/dev/null; then
		fail "$prefix: interrupted probe printed PASS"
	else
		pass "$prefix: interrupted probe prints no PASS"
	fi
	local group_pid
	group_pid=$(cat "$tmp/pid" 2>/dev/null || true)
	if [ -n "$group_pid" ] && kill -0 -- "-$group_pid" 2>/dev/null; then
		fail "$prefix: fake group $group_pid survived the interrupt"
	else
		pass "$prefix: interrupted probe's owned process group is gone"
	fi
}

sigcase midflight "mid-flight interrupt"
sigcase cleanup "cleanup-window interrupt"

# --- probe: nothing launches when preflight fails ----------------------------

rm -f "$tmp/argv"
if BBRAFT_TEST_CPU_MODEL='' "$probe" --kvm-device "$tmp/no-kvm-device" --qemu "$fake_qmp" --probe-dir "$probe_dir" \
	--cpuinfo "$tmp/intel" >"$tmp/out" 2>"$tmp/err"; then
	fail "probe passed with an inaccessible KVM device"
else
	if grep -q 'open host device' "$tmp/err"; then
		pass "inaccessible KVM device fails clearly"
	else
		fail "KVM device failure not named: $(cat "$tmp/err")"
	fi
fi
if [ ! -e "$tmp/argv" ]; then
	pass "no qemu launched after a KVM preflight failure"
else
	fail "qemu launched despite KVM preflight failure"
fi

rm -f "$tmp/argv"
if BBRAFT_TEST_CPU_MODEL='' "$probe" --kvm-device /dev/null --qemu "$fake_qmp" --probe-dir "$probe_dir" \
	--cpuinfo "$tmp/amd" --cpu-nested on >"$tmp/out" 2>"$tmp/err"; then
	fail "probe passed with an unresolvable CPU policy"
else
	if grep -q 'AuthenticAMD' "$tmp/err"; then
		pass "resolver failure blocks the launch"
	else
		fail "resolver failure not surfaced: $(cat "$tmp/err")"
	fi
fi
if [ ! -e "$tmp/argv" ]; then
	pass "no qemu launched after a resolver failure"
else
	fail "qemu launched despite resolver failure"
fi

# --- wrapper validation ------------------------------------------------------

if BBRAFT_TEST_CPU_MODEL='' "$probe" --timeout bogus >"$tmp/out" 2>"$tmp/err"; then
	fail "wrapper accepted a non-numeric timeout"
else
	pass "wrapper rejects a non-numeric timeout"
fi
if BBRAFT_TEST_CPU_MODEL='' "$probe" --help >"$tmp/out" 2>"$tmp/err" && grep -q 'cpu-nested' "$tmp/out"; then
	pass "wrapper help documents the knobs"
else
	fail "wrapper help is missing"
fi

if [ "$failures" -ne 0 ]; then
	printf 'cpu_probe_test: %d failure(s)\n' "$failures" >&2
	exit 1
fi
printf 'cpu_probe_test: all checks passed\n'
