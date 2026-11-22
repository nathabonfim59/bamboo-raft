#!/bin/sh
# Role-local checks for the bbraft_server role. Local connection only: no
# user, directory, binary, systemd unit or service is created. Run from
# infra/ansible:
#
#   mise exec -- sh roles/bbraft_server/tests/run.sh
#
# Only ansible.builtin is used, so no collection is needed.
set -eu

# Keep the role tree free of __pycache__ from the filter unit test and Ansible.
export PYTHONDONTWRITEBYTECODE=1

here=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
ansible_dir=$(CDPATH= cd -- "$here/../../.." && pwd)
cd "$ansible_dir"

if ! command -v ansible-playbook >/dev/null 2>&1; then
    echo "ansible-playbook not found; run through: cd infra/ansible && mise exec -- sh roles/bbraft_server/tests/run.sh" >&2
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

echo "== valid input =="
play "$here/validate.yml" >/dev/null
echo "ok: valid placeholders accepted"

play "$here/validate.yml" -e @"$here/node-token-shared-digest.yml" >/dev/null
echo "ok: different node ids may share a digest"

play "$here/validate.yml" -e 'bbraft_server_node_listen=[::ffff:127.0.0.1]:7701' >/dev/null
echo "ok: IPv4-mapped loopback node listener is private"

play "$here/validate.yml" \
    -e 'bbraft_server_stream_max_seconds=7200' >/dev/null
echo "ok: an ASCII-decimal string interval is accepted"

# Zero is meaningful for the settings where it disables the feature.
play "$here/validate.yml" \
    -e 'bbraft_server_node_lost_seconds=0' \
    -e 'bbraft_server_data_retention_seconds=0' \
    -e 'bbraft_server_disk_low_percent=0' >/dev/null
echo "ok: zero disables node loss, data retention and disk pressure"

# An empty endpoint is AWS S3, whose region then defaults to us-east-1.
play "$here/validate.yml" \
    -e 'bbraft_server_s3_enabled=true' \
    -e 'bbraft_server_s3_endpoint=' \
    -e 'bbraft_server_s3_region=' >/dev/null
echo "ok: object-store settings are accepted when enabled"

# A public node listener and an off-loopback gateway are valid with TLS.
play "$here/validate.yml" \
    -e 'bbraft_server_node_listen=[::ffff:203.0.113.10]:7701' \
    -e 'bbraft_server_node_tls_cert_file=/tmp/placeholder-node.crt' \
    -e 'bbraft_server_node_tls_key_file=/tmp/placeholder-node.key' \
    -e 'bbraft_server_gateway_listen=203.0.113.10:7702' \
    -e 'bbraft_server_gateway_tls_cert_file=/tmp/placeholder-gateway.crt' \
    -e 'bbraft_server_gateway_tls_key_file=/tmp/placeholder-gateway.key' \
    >/dev/null
echo "ok: TLS enables public node and off-loopback gateway listeners"

# An explicit trusted public URL lets the api role bind a wildcard listener.
# Encoded base paths stay valid, as net/url accepts them.
play "$here/validate.yml" \
    -e 'bbraft_server_listen=0.0.0.0:7700' \
    -e 'bbraft_server_public_url=https://bbraft.example.test/a%20b/base' \
    >/dev/null
echo "ok: wildcard API listener with an explicit public URL"

# publicURL has no port range rule for the advertised URL, matching net/url.
play "$here/validate.yml" \
    -e 'bbraft_server_public_url=https://bbraft.example.test:99999' \
    >/dev/null
echo "ok: numeric out-of-range advertised public URL port is accepted"

# Only the api role builds signed URLs; nodehub alone needs no public URL.
play "$here/validate.yml" \
    -e 'bbraft_server_roles=nodehub' \
    -e 'bbraft_server_listen=0.0.0.0:7700' \
    >/dev/null
echo "ok: nodehub-only wildcard listener needs no public URL"

# A wildcard gateway bind keeps the derived loopback dial target.
play "$here/validate.yml" \
    -e 'bbraft_server_gateway_listen=0.0.0.0:7702' \
    -e 'bbraft_server_gateway_tls_cert_file=/tmp/placeholder-gateway.crt' \
    -e 'bbraft_server_gateway_tls_key_file=/tmp/placeholder-gateway.key' \
    >/dev/null
echo "ok: wildcard gateway bind still dials loopback by default"

# A separate TLS dial target with its own CA bundle and certificate name.
play "$here/validate.yml" \
    -e 'bbraft_server_gateway_dial=gateway.internal:9702' \
    -e 'bbraft_server_gateway_tls_cert_file=/tmp/placeholder-gateway.crt' \
    -e 'bbraft_server_gateway_tls_key_file=/tmp/placeholder-gateway.key' \
    -e 'bbraft_server_gateway_ca_file=/tmp/placeholder-gateway-ca.pem' \
    -e 'bbraft_server_gateway_server_name=gateway.internal' \
    >/dev/null
echo "ok: TLS gateway dial with a CA bundle and server name"

play "$here/validate.yml" \
    -e 'bbraft_server_gateway_dial=[::ffff:127.0.0.1]:9702' >/dev/null
echo "ok: IPv4-mapped loopback dial target is loopback"

play "$here/validate.yml" \
    -e 'bbraft_server_gateway_dial=localhost:9702' >/dev/null
echo "ok: localhost dial target is loopback"

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

expect_fail "empty database URL" -e 'bbraft_server_database_url='
expect_fail "empty gateway token" -e 'bbraft_server_gateway_token='
expect_fail "empty master key source" -e 'bbraft_server_master_key_source='
expect_fail "empty binary source" -e 'bbraft_server_binary_src='
expect_fail "one-sided TLS key" -e 'bbraft_server_tls_key_file=/tmp/placeholder.key'
expect_fail "public plaintext node listener" -e 'bbraft_server_node_listen=203.0.113.10:7701'
expect_fail "off-loopback plaintext gateway" -e 'bbraft_server_gateway_listen=0.0.0.0:7702'
expect_fail "unbracketed IPv6 node listener" -e 'bbraft_server_node_listen=::1:7701'
expect_fail "float node port" -e 'bbraft_server_node_listen=[::1]:79.5'
expect_fail "Unicode-digit node port" -e 'bbraft_server_node_listen=[::1]:٧٧٠١'
expect_fail "IPv4-mapped public plaintext node listener" -e 'bbraft_server_node_listen=[::ffff:203.0.113.10]:7701'
expect_fail "invalid server.listen even with TLS" \
    -e 'bbraft_server_listen=::1:7700' \
    -e 'bbraft_server_tls_cert_file=/tmp/placeholder-server.crt' \
    -e 'bbraft_server_tls_key_file=/tmp/placeholder-server.key'
expect_fail "invalid node.listen even with TLS" \
    -e 'bbraft_server_node_listen=not-an-address' \
    -e 'bbraft_server_node_tls_cert_file=/tmp/placeholder-node.crt' \
    -e 'bbraft_server_node_tls_key_file=/tmp/placeholder-node.key'
expect_fail "invalid gateway.listen even with TLS" \
    -e 'bbraft_server_gateway_listen=[::1]7702' \
    -e 'bbraft_server_gateway_tls_cert_file=/tmp/placeholder-gateway.crt' \
    -e 'bbraft_server_gateway_tls_key_file=/tmp/placeholder-gateway.key'
expect_fail "duplicate role" -e 'bbraft_server_roles=api,api'
expect_fail "unknown role" -e 'bbraft_server_roles=bogus'
expect_fail "empty key prefix" -e 'bbraft_server_key_prefix='
expect_fail "empty API key" -e @"$here/invalid/api-key-empty.yml"
expect_fail "duplicate effective API key id" -e @"$here/invalid/api-key-duplicate-effective-id.yml"
expect_fail "float interval" -e @"$here/invalid/intervals-float.yml"
expect_fail "non-decimal interval" -e @"$here/invalid/intervals-string.yml"
expect_fail "no API keys for the api role" -e @"$here/invalid/api-keys-empty.yml"
expect_fail "raw node token" -e @"$here/invalid/node-token-raw.yml"
expect_fail "duplicate node id" -e @"$here/invalid/node-token-duplicate-id.yml"
expect_fail "no node tokens for the nodehub role" -e @"$here/invalid/node-tokens-empty.yml"
expect_fail "uppercase node digest" -e @"$here/invalid/node-token-uppercase.yml"
expect_fail "wildcard API listener without a public URL" \
    -e 'bbraft_server_listen=0.0.0.0:7700'
expect_fail "empty-host public URL" \
    -e 'bbraft_server_public_url=https://:443'
expect_fail "public URL with credentials" \
    -e 'bbraft_server_public_url=https://user:password@example.test'
expect_fail "public URL with a query" \
    -e 'bbraft_server_public_url=https://example.test?token=placeholder-url-secret'
expect_fail "public URL with a fragment" \
    -e 'bbraft_server_public_url=https://example.test#fragment'
expect_fail "relative public URL" \
    -e 'bbraft_server_public_url=/api'
expect_fail "non-http public URL" \
    -e 'bbraft_server_public_url=ftp://example.test'
expect_fail "public URL without a host" \
    -e 'bbraft_server_public_url=http:/path'
expect_fail "public URL with a signed port" \
    -e 'bbraft_server_public_url=https://example.test:+7702'
expect_fail "public URL with a malformed percent escape" \
    -e 'bbraft_server_public_url=https://example.test/%zz'
expect_fail "public URL with a malformed IPv6 authority" \
    -e 'bbraft_server_public_url=https://[::1'
expect_fail "public URL with a leading space" \
    -e @"$here/invalid/public-url-leading-space.yml"
expect_fail "signed gateway dial port" \
    -e 'bbraft_server_gateway_dial=127.0.0.1:+7702'
expect_fail "Unicode-digit gateway dial port" \
    -e 'bbraft_server_gateway_dial=127.0.0.1:٧٧٠٢'
expect_fail "empty gateway dial host" \
    -e 'bbraft_server_gateway_dial=:7702'
expect_fail "remote plaintext gateway dial" \
    -e 'bbraft_server_gateway_dial=10.0.0.5:7702'
expect_fail "CA bundle without the gateway TLS pair" \
    -e 'bbraft_server_gateway_ca_file=/tmp/placeholder-gateway-ca.pem'
expect_fail "gateway server name without the TLS pair" \
    -e 'bbraft_server_gateway_server_name=gateway.internal'
expect_fail "whitespace gateway server name" \
    -e @"$here/invalid/gateway-server-name-whitespace.yml"
expect_fail "NUL gateway server name" \
    -e @"$here/invalid/gateway-server-name-nul.yml"
expect_fail "line-separator gateway server name" \
    -e @"$here/invalid/gateway-server-name-line-separator.yml"
expect_fail "paragraph-separator gateway server name" \
    -e @"$here/invalid/gateway-server-name-paragraph-separator.yml"
expect_fail "zero snapshot interval" \
    -e 'bbraft_server_snapshot_interval_seconds=0'
expect_fail "zero snapshot timeout" \
    -e 'bbraft_server_snapshot_timeout_seconds=0'
expect_fail "node lost below the two-poll minimum" \
    -e 'bbraft_server_node_lost_seconds=59'
expect_fail "negative node lost" \
    -e 'bbraft_server_node_lost_seconds=-1'
expect_fail "negative data retention" \
    -e 'bbraft_server_data_retention_seconds=-1'
expect_fail "disk pressure above ninety percent" \
    -e 'bbraft_server_disk_low_percent=91'
expect_fail "negative disk pressure" \
    -e 'bbraft_server_disk_low_percent=-1'
expect_fail "object store without a bucket" \
    -e 'bbraft_server_s3_enabled=true' \
    -e 'bbraft_server_s3_bucket='
expect_fail "custom object-store endpoint without a region" \
    -e 'bbraft_server_s3_enabled=true' \
    -e 'bbraft_server_s3_region='
expect_fail "object store without an access key id" \
    -e 'bbraft_server_s3_enabled=true' \
    -e 'bbraft_server_s3_access_key_id='
expect_fail "object store without a secret" \
    -e 'bbraft_server_s3_enabled=true' \
    -e 'bbraft_server_s3_secret_access_key='
expect_fail "object-store endpoint that is not http(s)" \
    -e 'bbraft_server_s3_enabled=true' \
    -e 'bbraft_server_s3_endpoint=ftp://s3.example.test'
expect_fail "object-store bucket with whitespace" \
    -e @"$here/invalid/s3-bucket-whitespace.yml"
expect_fail "object-store env file outside its directory" \
    -e 'bbraft_server_s3_enabled=true' \
    -e 'bbraft_server_s3_env_file=/tmp/s3.env'
expect_fail "multi-line object-store secret" \
    -e @"$here/invalid/s3-secret-newline.yml"

echo "== verbose output credential check =="
check_no_leak() {
    log=$1
    for value in \
        'placeholder-password' \
        'placeholder-gateway-token-not-real' \
        'bmb_placeholder_key_not_real' \
        'placeholder-s3-secret-not-real'
    do
        if grep -F -q -- "$value" "$log"; then
            echo "FAIL: verbose output leaked a credential placeholder value" >&2
            rm -f "$log"
            exit 1
        fi
    done
    rm -f "$log"
}

for playbook in validate render; do
    log=$(mktemp)
    if ! play "$here/$playbook.yml" -vvv >"$log" 2>&1; then
        echo "FAIL: verbose $playbook run failed" >&2
        rm -f "$log"
        exit 1
    fi
    check_no_leak "$log"
done
echo "ok: verbose validation and render print no credential placeholders"

echo "bbraft_server role tests passed"
