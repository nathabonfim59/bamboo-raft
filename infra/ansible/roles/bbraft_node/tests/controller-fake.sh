#!/bin/sh
# Bounded, offline end-to-end regression for tests/controller.sh.
#
# It copies the wrapper into an isolated tree, replaces run.sh with a sentinel,
# and puts fake bounded curl/uv/python/ansible-playbook first on PATH. The real
# wrapper provisioning flow runs without network or system installs and must:
#   - create uv through the fake installer,
#   - install/find fake Python and create the fake venv,
#   - verify exact versions,
#   - canonicalize a relative binary and relative BBRAFT_ROLE_CONTROLLER_HOME,
#   - prepend the venv and hand off to the sentinel run.sh.
# Wrong uv, Python or ansible-core versions must stop before handoff.
set -eu

here=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)
tmp=$(mktemp -d)
cleanup() {
    chmod -R u+w "$tmp" 2>/dev/null || true
    rm -rf "$tmp"
}
trap cleanup EXIT INT TERM

fakebin="$tmp/fakebin"
mkdir -p "$fakebin" "$tmp/isolated" "$tmp/work"
cp "$here/controller.sh" "$tmp/isolated/controller.sh"

cat >"$fakebin/curl" <<'EOF'
#!/bin/sh
# Bounded fake: never network; print the configured installer script once.
cat "$FAKE_UV_INSTALLER"
EOF

cat >"$tmp/fake-installer" <<'EOF'
#!/bin/sh
set -eu
: "${UV_INSTALL_DIR:?}"
mkdir -p "$UV_INSTALL_DIR"
cp "$FAKE_UV_BIN" "$UV_INSTALL_DIR/uv"
chmod +x "$UV_INSTALL_DIR/uv"
EOF

cat >"$fakebin/uv" <<'EOF'
#!/bin/sh
set -eu
case "${1:-}" in
    --version)
        printf 'uv %s (fake)\n' "$FAKE_UV_VERSION"
        ;;
    python)
        case "${2:-}" in
            install)
                shift 2
                install_dir=
                while [ "$#" -gt 0 ]; do
                    case "$1" in
                        --install-dir)
                            install_dir=$2
                            shift 2
                            ;;
                        --managed-python)
                            shift
                            ;;
                        *)
                            shift
                            ;;
                    esac
                done
                [ -n "$install_dir" ]
                mkdir -p "$install_dir/bin"
                cp "$FAKE_PYTHON_BIN" "$install_dir/bin/python3.13"
                chmod +x "$install_dir/bin/python3.13"
                ;;
            find)
                printf '%s\n' "$UV_PYTHON_INSTALL_DIR/bin/python3.13"
                ;;
            *)
                exit 1
                ;;
        esac
        ;;
    venv)
        shift
        venv=
        while [ "$#" -gt 0 ]; do
            case "$1" in
                --python)
                    shift 2
                    ;;
                --clear)
                    shift
                    ;;
                -*)
                    shift
                    ;;
                *)
                    venv=$1
                    shift
                    ;;
            esac
        done
        [ -n "$venv" ]
        mkdir -p "$venv/bin"
        cp "$FAKE_PYTHON_BIN" "$venv/bin/python"
        chmod +x "$venv/bin/python"
        ln -sf python "$venv/bin/python3"
        cp "$FAKE_ANSIBLE_BIN" "$venv/bin/ansible-playbook"
        chmod +x "$venv/bin/ansible-playbook"
        ;;
    pip)
        # The wrapper pins ansible-core; nothing needs to be installed here.
        ;;
    *)
        exit 1
        ;;
esac
EOF

cat >"$fakebin/python3" <<'EOF'
#!/bin/sh
set -eu
case "${1:-}" in
    -c)
        printf '%s\n' "$FAKE_PYTHON_VERSION"
        ;;
    --version)
        printf 'Python %s\n' "$FAKE_PYTHON_VERSION"
        ;;
    *)
        exit 1
        ;;
esac
EOF

cat >"$fakebin/ansible-playbook" <<'EOF'
#!/bin/sh
set -eu
case "${1:-}" in
    --version)
        printf 'ansible-playbook [core %s] (fake)\n' "$FAKE_ANSIBLE_VERSION"
        ;;
    *)
        exit 1
        ;;
esac
EOF

chmod +x "$fakebin/curl" "$fakebin/uv" "$fakebin/python3" "$fakebin/ansible-playbook" "$tmp/fake-installer"

cat >"$tmp/isolated/run.sh" <<'EOF'
#!/bin/sh
set -eu
printf '%s\n' "$BBRAFT_NODE_BIN" >"$FAKE_SENTINEL.node"
printf '%s\n' "$PATH" >"$FAKE_SENTINEL.path"
test -x "$BBRAFT_NODE_BIN"
case "$BBRAFT_NODE_BIN" in
    /*) ;;
    *) echo "sentinel: BBRAFT_NODE_BIN is not absolute" >&2; exit 91 ;;
esac
test "$(command -v ansible-playbook)" = "$FAKE_EXPECT_VENV/bin/ansible-playbook" || {
    echo "sentinel: ansible-playbook did not resolve to the provisioned venv" >&2
    exit 92
}
case "$PATH" in
    "$FAKE_EXPECT_VENV/bin:"*) ;;
    *) echo "sentinel: venv is not first on PATH" >&2; exit 93 ;;
esac
EOF
chmod +x "$tmp/isolated/run.sh"

printf '#!/bin/sh\nexit 0\n' >"$tmp/work/relative-bin"
chmod +x "$tmp/work/relative-bin"

run_case() {
    desc=$1
    root_name=$2
    uv_version=$3
    python_version=$4
    ansible_version=$5
    expect=$6
    sentinel="$tmp/sentinel-$root_name"
    log="$tmp/log-$root_name"
    (
        cd "$tmp/work"
        env \
            HOME="$tmp/home" \
            PATH="$fakebin:/usr/bin:/bin" \
            FAKE_UV_INSTALLER="$tmp/fake-installer" \
            FAKE_UV_BIN="$fakebin/uv" \
            FAKE_PYTHON_BIN="$fakebin/python3" \
            FAKE_ANSIBLE_BIN="$fakebin/ansible-playbook" \
            FAKE_UV_VERSION="$uv_version" \
            FAKE_PYTHON_VERSION="$python_version" \
            FAKE_ANSIBLE_VERSION="$ansible_version" \
            FAKE_SENTINEL="$sentinel" \
            FAKE_EXPECT_VENV="$tmp/work/$root_name/venv" \
            BBRAFT_ROLE_CONTROLLER_HOME="$root_name" \
            sh "$tmp/isolated/controller.sh" relative-bin
    ) >"$log" 2>&1 || rc=$?
    rc=${rc:-0}
    if [ "$expect" = pass ]; then
        if [ "$rc" -ne 0 ]; then
            echo "FAIL: $desc unexpectedly failed" >&2
            sed -n '1,120p' "$log" >&2
            exit 1
        fi
        if [ ! -f "$sentinel.node" ] || [ ! -f "$sentinel.path" ]; then
            echo "FAIL: $desc did not reach the sentinel run.sh" >&2
            cat "$log" >&2
            exit 1
        fi
        node=$(cat "$sentinel.node")
        if [ "$node" != "$tmp/work/relative-bin" ]; then
            echo "FAIL: $desc handed off a non-canonical node path: $node" >&2
            exit 1
        fi
        echo "ok: $desc reached the sentinel with an absolute binary and venv-first PATH"
    else
        if [ "$rc" -eq 0 ]; then
            echo "FAIL: $desc unexpectedly passed" >&2
            cat "$log" >&2
            exit 1
        fi
        if [ -f "$sentinel.node" ]; then
            echo "FAIL: $desc ran the sentinel despite a version mismatch" >&2
            exit 1
        fi
        echo "ok: $desc was rejected before handoff"
    fi
}

run_case "relative binary/root provisioning" relative-root 0.13.7 3.13.12 2.22.0 pass
run_case "wrong uv version" bad-uv 0.13.70 3.13.12 2.22.0 fail
run_case "wrong Python version" bad-python 0.13.7 3.13.120 2.22.0 fail
run_case "wrong ansible-core version" bad-ansible 0.13.7 3.13.12 2.22.00 fail

echo "controller fake provisioning regression passed"
