#!/bin/sh
# A10 role checks (root jail supervisor, jail flags, fresh v1 data root).
# Focused subset of run.sh, also called by it. Local connection only,
# temporary trees only. Run from infra/ansible:
#
#   mise exec -- sh roles/bbraft_node/tests/run-a10.sh
set -eu
export PYTHONDONTWRITEBYTECODE=1
here=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
ansible_dir=$(CDPATH='' cd -- "$here/../../.." && pwd)
cd "$ansible_dir"
export ANSIBLE_CONFIG="$ansible_dir/ansible.cfg"
inventory="$here/inventory.yml"

play() {
    ansible-playbook -i "$inventory" "$@"
}

expect_fail() {
    desc=$1
    expected=$2
    playbook=$3
    shift 3
    log=$(mktemp)
    if play "$playbook" "$@" >"$log" 2>&1; then
        echo "FAIL: accepted $desc" >&2
        rm -f "$log"
        return 1
    fi
    if ! grep -F -q -- "$expected" "$log"; then
        echo "FAIL: $desc failed at an unexpected stage (missing: $expected)" >&2
        grep -E 'msg|ERROR' "$log" | head -5 >&2
        rm -f "$log"
        return 1
    fi
    rm -f "$log"
    echo "ok: rejected $desc ($expected)"
}

failures=0
check() { "$@" || failures=$((failures + 1)); }

echo "== A10 syntax =="
play "$here/legacy-data.yml" --syntax-check >/dev/null
play "$here/legacy-transition.yml" --syntax-check >/dev/null
play "$here/data-mount.yml" --syntax-check >/dev/null

echo "== A10 read-only host probes in check mode =="
play "$here/host-check-mode.yml" --check

echo "== A10 valid jail inputs =="
if play "$here/validate.yml" >/dev/null 2>&1; then echo "ok: valid jail placeholders accepted"; else echo "FAIL: valid jail placeholders refused" >&2; failures=$((failures + 1)); fi

echo "== A10 rejected jail inputs =="
jail="jailed QEMU inputs"
check expect_fail "empty jail uid range" "$jail" "$here/validate.yml" -e 'bbraft_node_jail_uid_range='
check expect_fail "single-number uid range" "$jail" "$here/validate.yml" -e 'bbraft_node_jail_uid_range=900000'
check expect_fail "uid range from 0" "$jail" "$here/validate.yml" -e 'bbraft_node_jail_uid_range=0-99'
check expect_fail "inverted uid range" "$jail" "$here/validate.yml" -e 'bbraft_node_jail_uid_range=900999-900000'
check expect_fail "single-uid range" "$jail" "$here/validate.yml" -e 'bbraft_node_jail_uid_range=900000-900000'
check expect_fail "uid range past 2^31-1" "$jail" "$here/validate.yml" -e 'bbraft_node_jail_uid_range=2147483000-2147483648'
check expect_fail "padded uid range" "$jail" "$here/validate.yml" -e 'bbraft_node_jail_uid_range=0900000-0900999'
check expect_fail "empty io device" "$jail" "$here/validate.yml" -e 'bbraft_node_jail_io_device='
check expect_fail "partition-style io device" "$jail" "$here/validate.yml" -e 'bbraft_node_jail_io_device=8'
check expect_fail "zero-major io device" "$jail" "$here/validate.yml" -e 'bbraft_node_jail_io_device=0:5'
check expect_fail "text io device" "$jail" "$here/validate.yml" -e 'bbraft_node_jail_io_device=sda'
check expect_fail "no positive io rate" "$jail" "$here/validate.yml" -e 'bbraft_node_jail_io_write_bps=0'
check expect_fail "negative io rate" "$jail" "$here/validate.yml" -e 'bbraft_node_jail_io_read_iops=-1'
check expect_fail "text io rate" "$jail" "$here/validate.yml" -e 'bbraft_node_jail_io_read_bps=fast'
check expect_fail "relative qemu binary" "$jail" "$here/validate.yml" -e 'bbraft_node_jail_qemu_binary=qemu-system-x86_64'
check expect_fail "relative tool binary" "$jail" "$here/validate.yml" -e 'bbraft_node_jail_tool_binary=qemu-img'
check expect_fail "unclean tool binary" "$jail" "$here/validate.yml" -e 'bbraft_node_jail_tool_binary=/usr/bin/../bin/qemu-img'
check expect_fail "relative firmware" "$jail" "$here/validate.yml" -e '{"bbraft_node_jail_firmware": ["bios-256k.bin"]}'
check expect_fail "duplicate firmware" "$jail" "$here/validate.yml" -e '{"bbraft_node_jail_firmware": ["/usr/share/qemu/kvmvapic.bin", "/usr/share/qemu/kvmvapic.bin"]}'
check expect_fail "firmware with a comma" "$jail" "$here/validate.yml" -e '{"bbraft_node_jail_firmware": ["/usr/share/qemu/a,b.bin"]}'
check expect_fail "firmware as a string" "$jail" "$here/validate.yml" -e 'bbraft_node_jail_firmware=/usr/share/qemu/kvmvapic.bin'
check expect_fail "relative library dir" "$jail" "$here/validate.yml" -e '{"bbraft_node_jail_library_dirs": ["lib"]}'

echo "== A10 hardening bridge pairing =="
pair="hardening_vm_bridge_interfaces"
check expect_fail "missing pairing" "$pair" "$here/validate.yml" -e '{"hardening_vm_bridge_interfaces": null}'
check expect_fail "pairing without the bridge" "$pair" "$here/validate.yml" -e '{"hardening_vm_bridge_interfaces": ["other0"]}'
check expect_fail "pairing with the ingress iface" "$pair" "$here/validate.yml" -e '{"hardening_vm_bridge_interfaces": ["bbraft0", "example0"]}'

echo "== A10 root ownership policy =="
check expect_fail "non-root token owner" "must be root-owned" "$here/validate.yml" -e 'bbraft_node_token_owner=nobody'
check expect_fail "non-root data owner" "must be root-owned" "$here/validate.yml" -e 'bbraft_node_data_owner=nobody'

echo "== A10 fix D: unit file named after the service =="
check expect_fail "unit path not named after the service" "unit_path" "$here/validate.yml" -e 'bbraft_node_unit_path=/etc/systemd/system/other.service'
check expect_fail "unit path without .service" "unit_path" "$here/validate.yml" -e 'bbraft_node_unit_path=/etc/systemd/system/bbraft-node'
if play "$here/validate.yml" -e 'bbraft_node_service=bbraft-node-alt' >/dev/null 2>&1; then echo "ok: custom service name with the derived unit path accepted"; else echo "FAIL: custom service name refused" >&2; failures=$((failures + 1)); fi
if play "$here/validate.yml" -e 'bbraft_node_service=bbraft-node-alt' -e 'bbraft_node_unit_path=/run/systemd/system/bbraft-node-alt.service' >/dev/null 2>&1; then echo "ok: explicit matching unit path accepted"; else echo "FAIL: explicit matching unit path refused" >&2; failures=$((failures + 1)); fi

echo "== A10 fix B: systemd floor =="
check expect_fail "lowered systemd floor" "systemd 254" "$here/validate.yml" -e 'bbraft_node_systemd_min_version=253'
check expect_fail "text systemd floor" "systemd 254" "$here/validate.yml" -e 'bbraft_node_systemd_min_version=new'
if play "$here/validate.yml" -e 'bbraft_node_systemd_min_version=256' >/dev/null 2>&1; then echo "ok: raised systemd floor accepted"; else echo "FAIL: raised systemd floor refused" >&2; failures=$((failures + 1)); fi

echo "== A10 fix B: device set =="
if play "$here/devices.yml" >/dev/null 2>&1; then echo "ok: four-device policy"; else echo "FAIL: four-device policy" >&2; failures=$((failures + 1)); fi

echo "== A10 fix C: data-root mount target =="
if play "$here/data-mount.yml" >/dev/null 2>&1; then echo "ok: data-root mount target"; else echo "FAIL: data-root mount target" >&2; failures=$((failures + 1)); fi

echo "== A10 fix A: exclusive data-root acquisition =="
if python3 "$here/test_data_root.py" >/dev/null 2>&1; then echo "ok: data-root module unit tests"; else echo "FAIL: data-root module unit tests" >&2; failures=$((failures + 1)); fi
if play "$here/legacy-transition.yml" >/dev/null 2>&1; then echo "ok: stale-state transitions"; else echo "FAIL: stale-state transitions" >&2; failures=$((failures + 1)); fi

echo "== A10 legacy and foreign data roots =="
if play "$here/legacy-data.yml" >/dev/null 2>&1; then echo "ok: legacy/foreign data-dir recognition"; else echo "FAIL: legacy/foreign data-dir recognition" >&2; failures=$((failures + 1)); fi

if [ "$failures" -ne 0 ]; then
    echo "A10 role checks: $failures failure(s)" >&2
    exit 1
fi
echo "A10 role checks passed"
