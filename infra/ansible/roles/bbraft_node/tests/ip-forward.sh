#!/bin/sh
# Role-local checks for tasks/ip-forward.yml: the live-nft guard, the
# persisted /etc/sysctl.d file and the guarded runtime apply. Fake `nft` and
# `sysctl` programs go first in PATH and the conf is a temporary file, so
# nothing real is touched and no root is needed. Run from infra/ansible:
#
#   mise exec -- sh roles/bbraft_node/tests/ip-forward.sh
set -eu

export PYTHONDONTWRITEBYTECODE=1

here=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
ansible_dir=$(CDPATH='' cd -- "$here/../../.." && pwd)
cd "$ansible_dir"

if ! command -v ansible-playbook >/dev/null 2>&1; then
    echo "ansible-playbook not found; run through: cd infra/ansible && mise exec -- sh roles/bbraft_node/tests/ip-forward.sh" >&2
    exit 2
fi

export ANSIBLE_CONFIG="$ansible_dir/ansible.cfg"
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT INT TERM
owner=$(id -un)
group=$(id -gn)

python3 "$here/ip-forward-fixtures.py" "$tmp/fixtures"

mkdir "$tmp/bin"
cat >"$tmp/bin/nft" <<'EOF'
#!/bin/sh
echo "nft $*" >>"$FAKE_LOG"
[ "$*" = "-j list ruleset" ] || exit 99
n=$(cat "$NFT_COUNT" 2>/dev/null || echo 0)
n=$((n + 1))
echo "$n" >"$NFT_COUNT"
if [ "$n" -eq 1 ]; then
    cat "$NFT_FIXTURE_1"
else
    cat "${NFT_FIXTURE_2:-$NFT_FIXTURE_1}"
fi
exit "${NFT_RC:-0}"
EOF
cat >"$tmp/bin/sysctl" <<'EOF'
#!/bin/sh
echo "sysctl $*" >>"$FAKE_LOG"
case "$1" in
-n)
    [ "$#" -eq 2 ] && [ "$2" = net.ipv4.ip_forward ] || exit 99
    cat "$SYSCTL_STATE"
    ;;
--load=*)
    [ "$#" -eq 1 ] || exit 99
    grep -qx 'net.ipv4.ip_forward = 1' "${1#--load=}" || exit 98
    [ -n "${SYSCTL_NOAPPLY:-}" ] || echo 1 >"$SYSCTL_STATE"
    ;;
*)
    exit 99
    ;;
esac
EOF
chmod 0755 "$tmp/bin/nft" "$tmp/bin/sysctl"

prior_conf='# prior content
net.ipv4.ip_forward = 0
'
printf '%s' "$prior_conf" >"$tmp/prior-bytes"

fail() {
    echo "FAIL: $*" >&2
    exit 1
}

# run_case NAME FIXTURE1 FIXTURE2 RUNTIME CONF_MODE... : sets up a case dir in
# $tmp/NAME (conf prepared by the caller beforehand as $tmp/NAME/ip.conf when
# wanted), runs the play and leaves rc in $rc, output in $tmp/NAME/out.
rc=0
run_case() {
    name=$1
    fix1=$2
    fix2=$3
    state=$4
    shift 4
    d=$tmp/$name
    mkdir -p "$d"
    echo "$state" >"$d/state"
    : >"$d/log"
    rm -f "$d/count"
    set +e
    PATH="$tmp/bin:$PATH" FAKE_LOG="$d/log" NFT_COUNT="$d/count" SYSCTL_STATE="$d/state" \
        NFT_FIXTURE_1="$tmp/fixtures/$fix1.json" NFT_FIXTURE_2="$tmp/fixtures/$fix2.json" \
        ansible-playbook -i "$here/inventory.yml" "$here/ip-forward.yml" \
        -e "ipf_conf=$d/ip.conf" -e "ipf_owner=$owner" -e "ipf_group=$group" "$@" >"$d/out" 2>&1
    rc=$?
    set -e
}

expect_ok() {
    [ "$rc" -eq 0 ] || { sed -n '1,120p' "$tmp/$1/out" >&2; fail "$1: play failed (rc $rc)"; }
}
expect_refused() {
    [ "$rc" -ne 0 ] || fail "$1: play unexpectedly succeeded"
    if [ -n "${2:-}" ] && ! grep -F -q -- "$2" "$tmp/$1/out"; then
        sed -n '1,80p' "$tmp/$1/out" >&2
        fail "$1: refused for another reason (missing: $2)"
    fi
}
log_is() {
    name=$1
    shift
    printf '%s\n' "$@" >"$tmp/$name/want"
    cmp -s "$tmp/$name/log" "$tmp/$name/want" || {
        echo "--- want" >&2
        cat "$tmp/$name/want" >&2
        echo "--- got" >&2
        cat "$tmp/$name/log" >&2
        fail "$name: unexpected command sequence"
    }
}
conf_is_managed() {
    conf=$tmp/$1/ip.conf
    [ -f "$conf" ] && [ ! -L "$conf" ] || fail "$1: conf missing"
    [ "$(stat -c %a "$conf")" = 644 ] || fail "$1: conf mode is $(stat -c %a "$conf"), want 644"
    [ "$(grep -v '^#' "$conf")" = 'net.ipv4.ip_forward = 1' ] || fail "$1: conf content is not exactly one ip_forward=1 setting"
}
conf_untouched() {
    # $1 case, $2 "absent" or a file holding the exact prior conf bytes
    conf=$tmp/$1/ip.conf
    if [ "$2" = absent ]; then
        [ ! -e "$conf" ] || fail "$1: a refused run created $conf"
    else
        cmp -s "$conf" "$2" || fail "$1: a refused run changed the prior conf"
    fi
    ! grep -q '^sysctl --load' "$tmp/$1/log" || fail "$1: a refused run called sysctl --load"
}
state_is() { [ "$(cat "$tmp/$1/state")" = "$2" ] || fail "$1: runtime state is $(cat "$tmp/$1/state"), want $2"; }

echo "== fresh host: guard, persist, apply, verify =="
run_case fresh good good 0
expect_ok fresh
conf_is_managed fresh
state_is fresh 1
conf=$tmp/fresh/ip.conf
log_is fresh "nft -j list ruleset" "sysctl -n net.ipv4.ip_forward" "nft -j list ruleset" \
    "sysctl --load=$conf" "sysctl -n net.ipv4.ip_forward"
echo "ok: fresh host guarded, persisted, applied and verified"

echo "== idempotent second run =="
cp "$tmp/fresh/ip.conf" "$tmp/idem-conf"
mkdir -p "$tmp/idem"
cp "$tmp/idem-conf" "$tmp/idem/ip.conf"
run_case idem good good 1
expect_ok idem
log_is idem "nft -j list ruleset" "sysctl -n net.ipv4.ip_forward"
grep -q 'changed=0' "$tmp/idem/out" || fail "idem: second run reported changes"
cmp -s "$tmp/idem-conf" "$tmp/idem/ip.conf" || fail "idem: conf changed"
echo "ok: unchanged conf and runtime 1 neither rewrite nor reapply"

echo "== runtime drift with an unchanged conf is repaired =="
mkdir -p "$tmp/drift"
cp "$tmp/idem-conf" "$tmp/drift/ip.conf"
run_case drift good good 0
expect_ok drift
conf=$tmp/drift/ip.conf
log_is drift "nft -j list ruleset" "sysctl -n net.ipv4.ip_forward" "nft -j list ruleset" \
    "sysctl --load=$conf" "sysctl -n net.ipv4.ip_forward"
state_is drift 1
cmp -s "$tmp/idem-conf" "$tmp/drift/ip.conf" || fail "drift: conf changed"
echo "ok: runtime 0 with unchanged conf is reapplied and verified"

echo "== changed conf is applied even at runtime 1 =="
mkdir -p "$tmp/changed"
printf '%s' "$prior_conf" >"$tmp/changed/ip.conf"
run_case changed good good 1
expect_ok changed
conf_is_managed changed
conf=$tmp/changed/ip.conf
log_is changed "nft -j list ruleset" "sysctl -n net.ipv4.ip_forward" "nft -j list ruleset" \
    "sysctl --load=$conf" "sysctl -n net.ipv4.ip_forward"
echo "ok: a rewritten conf is loaded"

echo "== mode drift is repaired =="
mkdir -p "$tmp/mode"
cp "$tmp/idem-conf" "$tmp/mode/ip.conf"
chmod 0600 "$tmp/mode/ip.conf"
run_case mode good good 1
expect_ok mode
conf_is_managed mode
echo "ok: conf mode drift rewritten to 0644"

echo "== check mode changes nothing =="
run_case check good good 0 --check
expect_ok check
[ ! -e "$tmp/check/ip.conf" ] || fail "check: check mode wrote the conf"
! grep -q '^sysctl --load' "$tmp/check/log" || fail "check: check mode applied sysctl"
state_is check 0
echo "ok: check mode reads, writes and applies nothing"

echo "== refused live rulesets preserve the prior conf and never call sysctl =="
for fix in policy-accept priority extra-rule generic-accept ipv6 no-rules no-table garbage duplicate-key; do
    for prior in absent present; do
        case_name=refuse-$fix-$prior
        mkdir -p "$tmp/$case_name"
        want=absent
        if [ "$prior" = present ]; then
            printf '%s' "$prior_conf" >"$tmp/$case_name/ip.conf"
            want=$tmp/prior-bytes
        fi
        run_case "$case_name" "$fix" "$fix" 0
        expect_refused "$case_name"
        conf_untouched "$case_name" "$want"
        # The guard refuses before the runtime is even read.
        log_is "$case_name" "nft -j list ruleset"
        state_is "$case_name" 0
    done
    echo "ok: refused $fix"
done

echo "== refused bridge configuration =="
refuse_bridges() {
    name=$1
    bridges=$2
    expected=$3
    mkdir -p "$tmp/$name"
    printf '%s' "$prior_conf" >"$tmp/$name/ip.conf"
    run_case "$name" good good 0 -e "{\"ipf_bridges\": $bridges}"
    expect_refused "$name" "$expected"
    conf_untouched "$name" "$tmp/prior-bytes"
    log_is "$name" "nft -j list ruleset"
    echo "ok: refused $name"
}
refuse_bridges bridges-empty '[]' "nonempty list"
refuse_bridges bridges-without-node-bridge '["other0"]' "does not contain the node bridge"
refuse_bridges bridges-with-ssh-iface '["bbraft0","example0"]' "ssh ingress interface"
refuse_bridges bridges-duplicate '["bbraft0","bbraft0"]' "duplicate"
# A configured bridge the live ruleset does not forward is refused, never added.
refuse_bridges bridges-not-live '["bbraft0","br1"]' "br1"

echo "== nft failure =="
export NFT_RC=1
run_case nft-fails good good 0
unset NFT_RC
expect_refused nft-fails
[ ! -e "$tmp/nft-fails/ip.conf" ] || fail "nft-fails: conf written after a failed nft"
log_is nft-fails "nft -j list ruleset"
echo "ok: a failing nft stops before any change"

echo "== live ruleset changes between the guard and the apply =="
mkdir -p "$tmp/second"
cp "$tmp/idem-conf" "$tmp/second/ip.conf"
run_case second good policy-accept 0
expect_refused second "policy"
conf_untouched second "$tmp/idem-conf"
state_is second 0
log_is second "nft -j list ruleset" "sysctl -n net.ipv4.ip_forward" "nft -j list ruleset"
echo "ok: the second live read gates the apply"

echo "== apply that does not take effect =="
mkdir -p "$tmp/noapply"
export SYSCTL_NOAPPLY=1
run_case noapply good good 0
unset SYSCTL_NOAPPLY
expect_refused noapply
echo "ok: runtime verified after apply"

echo "== real nft in a private namespace, real hardening template =="
# The REAL nft loads the REAL rendered hardening template inside `unshare -rn`
# (user + network namespace, no root, no host firewall). Only sysctl is a fake.
if ! command -v nft >/dev/null 2>&1 || ! unshare -rn nft -j list ruleset >/dev/null 2>&1; then
    if [ "${BBRAFT_TEST_REQUIRE_ALL:-}" = 1 ]; then
        fail "real nft in an unprivileged namespace is unavailable (BBRAFT_TEST_REQUIRE_ALL=1)"
    fi
    echo "skip: real nft in an unprivileged user+network namespace is unavailable"
else
    mkdir "$tmp/sysbin"
    cp "$tmp/bin/sysctl" "$tmp/sysbin/sysctl"
    render() {
        ansible-playbook -i "$here/inventory.yml" "$here/ip-forward-render.yml" \
            -e "out=$tmp/$1.nft" -e "{\"bridges\": $2}" >/dev/null
    }
    render real-one '["bbraft0"]'
    render real-two '["bbraft0","br1"]'
    render real-none '[]'
    : >"$tmp/real-empty.nft"
    cat >"$tmp/real-accept.nft" <<'EOF'
table inet bbraft_filter {
	chain forward {
		type filter hook forward priority filter; policy accept;
	}
}
EOF
    real_case() {
        # NAME NFTFILE BRIDGES-JSON ok|refused [needle]
        name=$1
        mkdir -p "$tmp/$name"
        set +e
        unshare -rn sh "$here/ip-forward-netns.sh" "$tmp/$2.nft" "$tmp/sysbin" "$tmp/$name" "$3" "$ansible_dir" >"$tmp/$name/out" 2>&1
        rc=$?
        set -e
        if [ "$4" = ok ]; then
            expect_ok "$name"
            conf_is_managed "$name"
            state_is "$name" 1
            grep -q "^sysctl --load=$tmp/$name/ip.conf\$" "$tmp/$name/log" || fail "$name: sysctl --load did not run"
        else
            expect_refused "$name" "${5:-}"
            conf_untouched "$name" absent
            [ ! -s "$tmp/$name/log" ] || fail "$name: a refused real ruleset reached sysctl"
        fi
        echo "ok: real $name ($4)"
    }
    real_case real-good real-one '["bbraft0"]' ok
    real_case real-two-bridges real-two '["bbraft0","br1"]' ok
    real_case real-extra-live real-two '["bbraft0"]' refused "br1"
    real_case real-missing-live real-one '["bbraft0","br1"]' refused "exactly 2"
    real_case real-no-forward-rules real-none '["bbraft0"]' refused "exactly 2"
    real_case real-no-table real-empty '["bbraft0"]' refused "no inet bbraft_filter forward chain"
    real_case real-policy-accept real-accept '["bbraft0"]' refused "policy"
fi

echo "bbraft_node ip-forward tests passed"
