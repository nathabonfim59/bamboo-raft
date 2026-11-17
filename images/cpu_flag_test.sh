#!/usr/bin/env bash
# Wiring regression for the shared CPU policy used by smoke and the CPU probe.
#
# The policy itself lives in internal/node/qemu (ResolveCPU/DetectHostCPU,
# owned by the qmp runner) and is exercised end to end, including the
# early CPU-only QEMU probe with a fake qemu, by cpu_probe_test.sh. This
# file checks the wiring that must hold in every worktree, with or without
# the Go stubs:
#   - no independent shell policy remains (cpu_policy.sh is gone)
#   - smoke.sh and cpu_probe.sh resolve through the shared Go helper and
#     never emit -cpu host/max, TCG or +svm (phase 0.5 has no +svm)
#   - the resolved nested feature is plumbed to the guest toolchain check
#   - make review runs the cheap CPU probe before the expensive image build
set -euo pipefail

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)

failures=0
pass() {
	printf 'PASS %s\n' "$*"
}
fail() {
	printf 'FAIL %s\n' "$*" >&2
	failures=$((failures + 1))
}

if [ -e "$script_dir/cpu_policy.sh" ]; then
	fail "independent shell policy cpu_policy.sh still exists"
else
	pass "no independent shell CPU policy (cpu_policy.sh removed)"
fi

for file in Makefile smoke.sh cpu_probe.sh smoke/main.go; do
	if [ -f "$script_dir/$file" ]; then
		pass "$file exists"
	else
		fail "$file is missing"
	fi
done

# No worktree may reference the removed policy or the pre-decision model.
for file in Makefile smoke.sh cpu_probe.sh build.sh smoke/main.go \
	mkosi.extra/usr/local/bin/bbraft-toolchain-check ../docs/guest-image.md; do
	path="$script_dir/$file"
	[ -f "$path" ] || continue
	if grep -q 'cpu_policy' "$path"; then
		fail "$file still references cpu_policy"
	fi
	if grep -q 'x86-64-v3' "$path"; then
		fail "$file still contains x86-64-v3"
	fi
done
if ! grep -qr 'cpu_policy\|x86-64-v3' "$script_dir" 2>/dev/null; then
	pass "no cpu_policy or x86-64-v3 references under images/"
fi

# The smoke client (main.go plus probe.go) must call the shared resolver and
# default the model the same way the node does.
smoke_pkg=("$script_dir/smoke/"*.go)
if grep -lq 'resolve-cpu' "${smoke_pkg[@]}" && grep -lq 'qemu\.ResolveCPU' "${smoke_pkg[@]}" &&
	grep -lq 'qemu\.DetectHostCPU' "${smoke_pkg[@]}"; then
	pass "smoke client resolves through qemu.ResolveCPU/DetectHostCPU"
else
	fail "smoke client does not use the shared Go CPU policy"
fi
if grep -lq 'DefaultCPUModel' "${smoke_pkg[@]}"; then
	pass "smoke client defaults the model to qemu.DefaultCPUModel"
else
	fail "smoke client does not reference qemu.DefaultCPUModel"
fi

# The test-only explicit host override is read at the wrapper entry boundary
# and forwarded to the Go client's deliberate internal opt-in; the production
# resolver path stays the default.
for file in smoke.sh cpu_probe.sh; do
	if grep -q 'BBRAFT_TEST_CPU_MODEL' "$script_dir/$file"; then
		pass "$file reads BBRAFT_TEST_CPU_MODEL at the entry boundary"
	else
		fail "$file does not read BBRAFT_TEST_CPU_MODEL"
	fi
	if grep -q -- '-cpu-model-test-host' "$script_dir/$file"; then
		pass "$file forwards the internal test-host opt-in"
	else
		fail "$file does not forward the internal test-host opt-in"
	fi
done
if grep -q -- '-cpu-model-test-host' "$script_dir/smoke/main.go" &&
	grep -q 'qemu\.ResolveTestCPU' "$script_dir/smoke/probe.go"; then
	pass "smoke client wires the internal test-host opt-in to qemu.ResolveTestCPU"
else
	fail "smoke client does not wire the internal test-host opt-in"
fi
if grep -q 'qemu\.ResolveCPU' "$script_dir/smoke/probe.go"; then
	pass "smoke client keeps the production qemu.ResolveCPU path"
else
	fail "smoke client lost the production qemu.ResolveCPU path"
fi

# code_of prints a file without full-line comments, so forbidden-pattern
# greps only judge code, not prose.
code_of() { grep -v '^[[:space:]]*#' "$1" || true; }

# Both launchers stay thin and never fall back to a forbidden CPU policy.
for file in smoke.sh cpu_probe.sh; do
	path="$script_dir/$file"
	[ -f "$path" ] || continue
	case "$file" in
	smoke.sh)
		# smoke.sh must carry the real machine itself, not rely on a match in
		# the shared probe core, or a TCG regression here would be masked.
		if grep -q 'accel=kvm' "$path"; then
			pass "smoke.sh keeps q35,accel=kvm"
		else
			fail "smoke.sh lost q35,accel=kvm"
		fi
		;;
	cpu_probe.sh)
		# cpu_probe.sh is a thin wrapper over the shared -probe-cpu core.
		if grep -q -- '-probe-cpu' "$path" &&
			grep -q 'accel=kvm' "$script_dir/../internal/node/qemu/cpu_probe.go"; then
			pass "cpu_probe.sh delegates to the shared -probe-cpu q35,accel=kvm core"
		else
			fail "cpu_probe.sh lost its shared -probe-cpu q35,accel=kvm delegation"
		fi
		;;
	esac
	if grep -q 'enforce' "$path"; then
		pass "$file keeps the enforce guard on the resolved CPU"
	else
		fail "$file lost the enforce guard"
	fi
	if code_of "$path" | grep -Eq -- '\+svm|-cpu (host|max)|accel=tcg|-accel tcg'; then
		fail "$file emits forbidden +svm/host/max/TCG"
	else
		pass "$file emits no +svm, host, max or TCG"
	fi
	case "$file" in
	smoke.sh)
		mode_flag=('-resolve-cpu')
		;;
	cpu_probe.sh)
		mode_flag=('-probe-cpu')
		;;
	esac
	if grep -q -- "${mode_flag[0]}" "$path"; then
		pass "$file drives the shared client's ${mode_flag[0]} mode"
	else
		fail "$file does not drive the shared client's ${mode_flag[0]} mode"
	fi
done

# The selected nested feature must reach the guest toolchain check, which
# accepts the selected vmx/none instead of hard-requiring vmx.
toolchain="$script_dir/mkosi.extra/usr/local/bin/bbraft-toolchain-check"
if grep -q 'BBRAFT_EXPECT_NESTED' "$toolchain" && grep -q 'expected svm' "$toolchain"; then
	pass "guest check supports the selected vmx/svm and does not hard-require vmx"
else
	fail "guest check does not use BBRAFT_EXPECT_NESTED/svm"
fi
if grep -q 'expect-nested' "$script_dir/smoke.sh"; then
	pass "smoke passes the selected nested feature to the guest"
else
	fail "smoke does not plumb the selected nested feature"
fi

# Every review runs the cheap CPU probe before the expensive build: the
# shared review-prereqs target owns the probe, and each review target depends
# on it before build.sh.
makefile="$script_dir/Makefile"
if grep -q '^review-prereqs:' "$makefile" && grep -q $'^\t\\./cpu_probe\\.sh' "$makefile"; then
	pass "review-prereqs runs the CPU probe"
else
	fail "review-prereqs does not run the CPU probe"
fi
for target in review review-node review-all; do
	if grep -q "^${target}: review-prereqs" "$makefile"; then
		pass "$target depends on review-prereqs"
	else
		fail "$target does not depend on review-prereqs"
	fi
	# Within the target's recipe (up to the next target line), build.sh must
	# not appear before the prerequisite-providing first recipe line.
	body=$(awk -v t="^${target}:" '$0 ~ t {in_target=1; next} /^[a-zA-Z_-]+:/ {in_target=0} in_target {print}' "$makefile" || true)
	if printf '%s\n' "$body" | grep -q '\./build\.sh'; then
		pass "$target builds the image after the CPU probe"
	else
		fail "$target does not build the image"
	fi
done
if grep -qx 'review-node: review-prereqs' "$makefile"; then
	pass "review-node stays the A2 gate on review-prereqs, without role tests"
else
	fail "review-node lost its review-prereqs dependency"
fi

# QMP unix sockets must fit in sockaddr_un.sun_path (107 bytes). The restart
# test adds t.TempDir's test-name suffix (and a /001 subdirectory when it
# calls t.TempDir again) plus the VM work directory /vms/<sandbox>/qmp.sock;
# the old $HOME/bbraft-kvm-review default reached 110 bytes on this HOME.
kvm_tmp=$(sed -n 's/^KVM_TMP ?= //p' "$makefile")
kvm_tmp=${kvm_tmp//"\$(HOME)"/$HOME}
worst_socket="$kvm_tmp/TestKVMNodeManagerRestartDuringFreeze0123456789/001/vms/bx_22222222/qmp.sock"
if [ -n "$kvm_tmp" ] && [ "${#worst_socket}" -le 107 ]; then
	pass "KVM_TMP keeps the worst-case QMP socket path within 107 bytes"
else
	fail "KVM_TMP '$kvm_tmp' gives a ${#worst_socket}-byte QMP socket path (limit 107)"
fi
if grep -q "TMPDIR=\"\$(KVM_TMP)\"" "$makefile" && grep -q "GOTMPDIR=\"\$(KVM_TMP)\"" "$makefile"; then
	pass "KVM runs override both TMPDIR and GOTMPDIR"
else
	fail "KVM runs do not override both TMPDIR and GOTMPDIR"
fi

if [ "$failures" -ne 0 ]; then
	printf 'cpu_flag_test: %d failure(s)\n' "$failures" >&2
	exit 1
fi
printf 'cpu_flag_test: all checks passed\n'
