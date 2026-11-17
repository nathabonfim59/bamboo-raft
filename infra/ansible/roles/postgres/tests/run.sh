#!/bin/sh
# Role-local checks for the postgres role. Local connection only: no host,
# package, cluster or service is touched. Run from infra/ansible:
#
#   mise exec -- sh roles/postgres/tests/run.sh
#
# Requires community.postgresql only for the syntax check of the full role;
# the render/validation plays themselves use ansible.builtin.
set -eu

# Keep the role tree free of __pycache__ from the filter unit test and Ansible.
export PYTHONDONTWRITEBYTECODE=1

here=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
ansible_dir=$(CDPATH= cd -- "$here/../../.." && pwd)
cd "$ansible_dir"

if ! command -v ansible-playbook >/dev/null 2>&1; then
    echo "ansible-playbook not found; run through: cd infra/ansible && mise exec -- sh roles/postgres/tests/run.sh" >&2
    exit 2
fi

export ANSIBLE_CONFIG="$ansible_dir/ansible.cfg"
inventory="$here/inventory.yml"

play() {
    ansible-playbook -i "$inventory" "$@"
}

echo "== filter unit tests =="
python3 "$here/test_filters.py"

echo "== syntax check =="
play "$here/syntax.yml" --syntax-check
play "$here/validate.yml" --syntax-check
play "$here/render.yml" --syntax-check

# The assertions must run before apt so an invalid install cannot touch the
# host. --list-tasks expands import_tasks without executing anything.
echo "== validation runs before package installation =="
task_list=$(play "$here/syntax.yml" --list-tasks)
validation_line=$(printf '%s\n' "$task_list" | grep -n 'PostgreSQL password is supplied' | head -n1 | cut -d: -f1)
install_line=$(printf '%s\n' "$task_list" | grep -n 'Install PostgreSQL' | head -n1 | cut -d: -f1)
restart_line=$(printf '%s\n' "$task_list" | grep -n 'Apply pending PostgreSQL restarts' | head -n1 | cut -d: -f1)
probe_line=$(printf '%s\n' "$task_list" | grep -n 'Read the server version from the configured socket' | head -n1 | cut -d: -f1)
database_line=$(printf '%s\n' "$task_list" | grep -n 'Create or update the bbraft PostgreSQL role' | head -n1 | cut -d: -f1)
if [ -z "$validation_line" ] || [ -z "$install_line" ] || [ "$validation_line" -ge "$install_line" ]; then
    echo "FAIL: main.yml must import validate.yml before the apt task (validation=$validation_line install=$install_line)" >&2
    exit 1
fi
if [ -z "$restart_line" ] || [ -z "$probe_line" ] || [ -z "$database_line" ] \
    || [ "$restart_line" -ge "$probe_line" ] || [ "$restart_line" -ge "$database_line" ]; then
    echo "FAIL: the pending restart must precede the port probe and user/database tasks (restart=$restart_line probe=$probe_line database=$database_line)" >&2
    exit 1
fi
echo "ok: validation precedes the package install; restart precedes probe and database"

echo "== valid input =="
play "$here/validate.yml" >/dev/null
play "$here/validate.yml" -e 'postgres_port=55432' >/dev/null
echo "ok: valid placeholders and an ASCII-decimal string port accepted"

echo "== render =="
play "$here/render.yml"

echo "== rejected input =="
expect_fail() {
    desc=$1
    shift
    log=$(mktemp)
    if play "$here/validate.yml" "$@" >"$log" 2>&1; then
        echo "FAIL: accepted $desc" >&2
        sed -n '1,120p' "$log" >&2
        rm -f "$log"
        exit 1
    fi
    rm -f "$log"
    echo "ok: rejected $desc"
}

expect_fail "empty password" -e @"$here/invalid/password-empty.yml"
expect_fail "port 0" -e @"$here/invalid/port-zero.yml"
expect_fail "float port" -e @"$here/invalid/port-float.yml"
expect_fail "non-decimal port" -e @"$here/invalid/port-string.yml"
expect_fail "PostgreSQL 16" -e @"$here/invalid/version-old.yml"
expect_fail "SQL injection in the user name" -e @"$here/invalid/user-injection.yml"
expect_fail "quote in listen_addresses" -e @"$here/invalid/listen-quote.yml"

echo "== verbose output credential check =="
for playbook in validate render; do
    log=$(mktemp)
    if ! play "$here/$playbook.yml" -vvv >"$log" 2>&1; then
        echo "FAIL: verbose $playbook run failed" >&2
        rm -f "$log"
        exit 1
    fi
    if grep -F -q -- 'placeholder-password' "$log"; then
        echo "FAIL: verbose output leaked the database password placeholder" >&2
        rm -f "$log"
        exit 1
    fi
    rm -f "$log"
done
echo "ok: verbose validation and render print no password placeholder"

echo "postgres role tests passed"
