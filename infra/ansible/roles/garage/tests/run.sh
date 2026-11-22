#!/bin/sh
# Role-local checks for garage. Local connection only: no package, user,
# Garage process, directory or systemd unit is changed on the host; the fake
# garage CLI and a temporary tree are used. Run from infra/ansible:
#
#   mise exec -- sh roles/garage/tests/run.sh
set -eu

here=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
ansible_dir=$(CDPATH='' cd -- "$here/../../.." && pwd)
cd "$ansible_dir"

if ! command -v ansible-playbook >/dev/null 2>&1; then
    echo "ansible-playbook not found; run through: cd infra/ansible && mise exec -- sh roles/garage/tests/run.sh" >&2
    exit 2
fi

export ANSIBLE_CONFIG="$ansible_dir/ansible.cfg"
inventory="$here/inventory.yml"

play() {
    ansible-playbook -i "$inventory" "$@"
}

echo "== syntax check =="
play "$here/render.yml" --syntax-check

echo "== credential render =="
play "$here/render.yml"

echo "== verbose output secret check =="
log=$(mktemp)
trap 'rm -f "$log"' EXIT INT TERM
if ! play "$here/render.yml" -vvv >"$log" 2>&1; then
    echo "FAIL: verbose render run failed" >&2
    sed -n '1,160p' "$log" >&2
    exit 1
fi
if grep -F -q -- 'SENTINEL-garage-secret-7a1c' "$log"; then
    echo "FAIL: verbose output leaked the secret sentinel" >&2
    exit 1
fi

echo "garage role tests passed"
