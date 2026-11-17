#!/bin/sh
# Pinned-controller wrapper for the bbraft_node role tests.
#
# ansible-core 2.22.0 declares Requires-Python >=3.13, but a minimal Ubuntu
# test VM base has Python 3.12 and no Ansible. This wrapper provisions only
# user-space, pinned tools: uv 0.13.7, a uv-managed CPython 3.13.12, and
# ansible-core 2.22.0. Nothing is installed with apt or system-wide and the
# shared infra/ansible/mise.toml is not modified.
#
# Usage:
#   tests/controller.sh <path-to-bbraft-node>
#   tests/controller.sh --dry-run <path-to-bbraft-node>
#   tests/controller.sh --help
#
# The actual test-VM run is the lead's: this script is safe to inspect and
# dry-run locally. All downloads and the full role suite happen only when it is
# executed with a real node binary on the test controller.
set -eu

UV_VERSION=0.13.7
PYTHON_VERSION=3.13.12
ANSIBLE_VERSION=2.22.0

here=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)

abspath() {
    if command -v readlink >/dev/null 2>&1; then
        readlink -m -- "$1"
    else
        case "$1" in
            /*) printf '%s\n' "$1" ;;
            *) printf '%s/%s\n' "$(pwd -P)" "$1" ;;
        esac
    fi
}

root=${BBRAFT_ROLE_CONTROLLER_HOME:-${XDG_CACHE_HOME:-$HOME/.cache}/bbraft-node-role/controller}
root=$(abspath "$root")
uv="$root/bin/uv"
python_dir="$root/python"
venv="$root/venv"
cache_dir="$root/cache"

usage() {
    cat <<'EOF'
usage: tests/controller.sh [--dry-run] <path-to-bbraft-node>

Provisions user-space uv 0.13.7 + uv-managed CPython 3.13.12 +
ansible-core 2.22.0 under $BBRAFT_ROLE_CONTROLLER_HOME (default
~/.cache/bbraft-node-role/controller), verifies the versions, then runs
tests/run.sh with BBRAFT_NODE_BIN set. No apt, sudo or system-wide install.
EOF
}

dry_run=0
node_bin=
for arg in "$@"; do
    case "$arg" in
        --help | -h)
            usage
            exit 0
            ;;
        --dry-run)
            dry_run=1
            ;;
        -*)
            usage >&2
            exit 2
            ;;
        *)
            if [ -n "$node_bin" ]; then
                usage >&2
                exit 2
            fi
            node_bin=$arg
            ;;
    esac
done

if [ -z "$node_bin" ]; then
    usage >&2
    exit 2
fi
node_bin=$(abspath "$node_bin")
if [ "$dry_run" -eq 0 ] && [ ! -x "$node_bin" ]; then
    echo "controller: $node_bin is not executable" >&2
    exit 2
fi

# All uv state stays under $root. UV_PYTHON_INSTALL_DIR keeps the interpreter
# out of any shared location; UV_CACHE_DIR keeps downloads here too.
export UV_PYTHON_INSTALL_DIR="$python_dir"
export UV_CACHE_DIR="$cache_dir"
export UV_NO_PROGRESS=1

print_plan() {
    cat <<EOF
controller plan (no commands run):
  root:              $root
  uv:                $uv
  uv version:        $UV_VERSION (official installer, UV_INSTALL_DIR=$root/bin, UV_NO_MODIFY_PATH=1)
  python:            $PYTHON_VERSION (uv python install --install-dir=$python_dir --managed-python)
  venv:              $venv (uv venv --python <managed 3.13.12> --clear)
  ansible-core:      $ANSIBLE_VERSION (uv pip install --python $venv/bin/python)
  node binary:       $node_bin
  final command:     PATH=$venv/bin:\$PATH BBRAFT_NODE_BIN=$node_bin sh $here/run.sh
EOF
}

if [ "$dry_run" -eq 1 ]; then
    print_plan
    exit 0
fi

if ! command -v curl >/dev/null 2>&1; then
    echo "controller: curl is required to fetch the pinned uv installer" >&2
    exit 2
fi
if ! command -v tar >/dev/null 2>&1; then
    echo "controller: tar is required by the uv installer" >&2
    exit 2
fi

mkdir -p "$root/bin" "$python_dir" "$cache_dir"

if [ ! -x "$uv" ]; then
    echo "controller: installing pinned uv $UV_VERSION into $root/bin"
    curl --proto '=https' --tlsv1.2 -LsSf \
        "https://astral.sh/uv/${UV_VERSION}/install.sh" |
        env UV_INSTALL_DIR="$root/bin" UV_NO_MODIFY_PATH=1 sh
fi
if [ ! -x "$uv" ]; then
    echo "controller: uv installer did not create $uv" >&2
    exit 1
fi

uv_got=$("$uv" --version)
case "$uv_got" in
    "uv $UV_VERSION"|"uv $UV_VERSION "*) ;;
    *)
        echo "controller: $uv reports '$uv_got', want exactly uv $UV_VERSION; remove $root and retry" >&2
        exit 1
        ;;
esac

echo "controller: installing uv-managed CPython $PYTHON_VERSION"
"$uv" python install "$PYTHON_VERSION" --install-dir "$python_dir" --managed-python
python_bin=$("$uv" python find --managed-python "$PYTHON_VERSION")
if [ ! -x "$python_bin" ]; then
    echo "controller: could not resolve uv-managed Python $PYTHON_VERSION" >&2
    exit 1
fi
python_got=$("$python_bin" -c 'import sys; print(".".join(map(str, sys.version_info[:3])))')
if [ "$python_got" != "$PYTHON_VERSION" ]; then
    echo "controller: $python_bin reports Python $python_got, want $PYTHON_VERSION" >&2
    exit 1
fi

echo "controller: creating $venv"
"$uv" venv --python "$python_bin" --clear "$venv"

echo "controller: installing ansible-core $ANSIBLE_VERSION"
"$uv" pip install --python "$venv/bin/python" "ansible-core==$ANSIBLE_VERSION"

ansi_got=$("$venv/bin/ansible-playbook" --version | sed -n '1p')
ansi_ver=$(printf '%s\n' "$ansi_got" | sed -n 's/.*\[core \([0-9][0-9.]*\)\].*/\1/p')
if [ "$ansi_ver" != "$ANSIBLE_VERSION" ]; then
    echo "controller: ansible-playbook reports '$ansi_got', want exactly core $ANSIBLE_VERSION" >&2
    exit 1
fi
echo "controller: $("$venv/bin/python" --version 2>&1); $ansi_got"

export PATH="$venv/bin:$PATH"
export BBRAFT_NODE_BIN="$node_bin"
echo "controller: running the role suite with the pinned controller stack"
exec sh "$here/run.sh"
