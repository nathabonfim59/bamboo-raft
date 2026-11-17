#!/bin/sh
# Role-local checks for bbraft_node. Local connection only: no system package,
# user, directory, systemd unit or service is changed on the host; temporary
# trees are used. Run from infra/ansible:
#
#   mise exec -- sh roles/bbraft_node/tests/run.sh
#
# Set BBRAFT_NODE_BIN to a real built bbraft-node to additionally pass the
# rendered ExecStart flags and values through the actual Go flag parser.
set -eu

# Keep the role tree free of __pycache__ from the filter unit test and Ansible.
export PYTHONDONTWRITEBYTECODE=1

here=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
ansible_dir=$(CDPATH='' cd -- "$here/../../.." && pwd)
cd "$ansible_dir"

if ! command -v ansible-playbook >/dev/null 2>&1; then
    echo "ansible-playbook not found; run through: cd infra/ansible && mise exec -- sh roles/bbraft_node/tests/run.sh" >&2
    exit 2
fi

export ANSIBLE_CONFIG="$ansible_dir/ansible.cfg"
inventory="$here/inventory.yml"
fixtures="$here/fixtures"
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT INT TERM

play() {
    ansible-playbook -i "$inventory" "$@"
}

images_json() {
    printf '{"bbraft_node_images":["%s"]}' "$1"
}

echo "== filter unit tests =="
python3 "$here/test_filters.py"
python3 "$here/test_forward_guard.py"

echo "== controller fake provisioning regression =="
sh "$here/controller-fake.sh"

echo "== syntax check =="
play "$here/syntax.yml" --syntax-check
play "$here/validate.yml" --syntax-check
play "$here/render.yml" --syntax-check
play "$here/immutable.yml" --syntax-check
play "$here/staged-corruption.yml" --syntax-check
play "$here/devices.yml" --syntax-check
play "$here/image-publish.yml" --syntax-check
play "$here/ip-forward.yml" --syntax-check

echo "== valid input =="
play "$here/validate.yml" >/dev/null
echo "ok: valid placeholders accepted"

echo "== render =="
if [ -n "${BBRAFT_NODE_BIN:-}" ]; then
    play "$here/render.yml" \
        -e "bbraft_node_render_output=$tmp/unit-full.service" \
        -e "bbraft_node_render_output_default=$tmp/unit.service"
    python3 "$here/check_unit_flags.py" "$BBRAFT_NODE_BIN" "$tmp/unit.service"
    python3 "$here/check_unit_flags.py" "$BBRAFT_NODE_BIN" "$tmp/unit-full.service"
else
    play "$here/render.yml"
fi

echo "== immutable images and failed sources =="
play "$here/immutable.yml"

echo "== target staged-image corruption =="
play "$here/staged-corruption.yml"

echo "== device policy task =="
play "$here/devices.yml"

echo "== no-clobber image publication =="
play "$here/image-publish.yml"

echo "== guarded IPv4 forwarding (fake nft/sysctl, real nft in a private namespace) =="
sh "$here/ip-forward.sh"

echo "== rejected input =="
expect_fail() {
    desc=$1
    expected=$2
    playbook=$3
    shift 3
    log=$(mktemp)
    if play "$playbook" "$@" >"$log" 2>&1; then
        echo "FAIL: accepted $desc" >&2
        sed -n '1,160p' "$log" >&2
        rm -f "$log"
        exit 1
    fi
    if ! grep -F -q -- "$expected" "$log"; then
        echo "FAIL: $desc failed at an unexpected stage (missing: $expected)" >&2
        sed -n '1,160p' "$log" >&2
        rm -f "$log"
        exit 1
    fi
    rm -f "$log"
    echo "ok: rejected $desc ($expected)"
}

python3 -c 'print("bbraft_node_token: \"" + "a" * 8192 + "\"")' >"$tmp/token-8192.yml"
python3 -c 'print("bbraft_node_token: \"" + "\u00e9" * 4096 + "\"")' >"$tmp/token-8192-utf8.yml"
python3 -c 'print("bbraft_node_token: \"" + "a" * 8193 + "\"")' >"$tmp/long-token.yml"
python3 -c 'print("bbraft_node_token: \"" + "\u00e9" * 4097 + "\"")' >"$tmp/long-token-utf8.yml"

play "$here/validate.yml" -e @"$tmp/token-8192.yml" >/dev/null
play "$here/validate.yml" -e @"$tmp/token-8192-utf8.yml" >/dev/null
echo "ok: 8192-byte ASCII and multibyte tokens accepted"

play "$here/validate.yml" -e 'bbraft_node_cpu_overcommit=2.5' -e 'bbraft_node_memory_overcommit=4' -e 'bbraft_node_disk_overcommit=1.25' >/dev/null
echo "ok: non-default overcommit ratios accepted"

play "$here/validate.yml" -e '{"bbraft_node_network_management": ["100.64.0.0/10"]}' >/dev/null
play "$here/validate.yml" -e '{"bbraft_node_network_resolvers": ["1.1.1.1"]}' >/dev/null
play "$here/validate.yml" -e 'bbraft_node_ssh_ingress_addr=100.100.1.1' >/dev/null
echo "ok: management networks, a single resolver and a CGNAT ingress address accepted"

expect_fail "no binary source" "Required caller inputs are present" "$here/validate.yml" -e 'bbraft_node_binary_src='
expect_fail "both binary sources" "set bbraft_node_binary_src or bbraft_node_binary_url, not both" "$here/validate.yml" -e 'bbraft_node_binary_url=https://example.invalid/bbraft-node'
expect_fail "short binary checksum" "bbraft_node_binary_sha256 must be 64 lowercase hex characters" "$here/validate.yml" -e 'bbraft_node_binary_sha256=deadbeef'
expect_fail "mismatched binary checksum" "does not match bbraft_node_binary_sha256" "$here/validate.yml" \
    -e 'bbraft_node_binary_sha256=0000000000000000000000000000000000000000000000000000000000000000'
expect_fail "no token source" "Required caller inputs are present" "$here/validate.yml" -e 'bbraft_node_token=' -e 'bbraft_node_token_src='
expect_fail "both token sources" "set bbraft_node_token or bbraft_node_token_src, not both" "$here/validate.yml" -e "bbraft_node_token_src=$fixtures/node.token"
expect_fail "token with a newline" "The token value is a single line within the node's byte bound" "$here/validate.yml" -e @"$here/invalid/token-newline.yml"
expect_fail "token over 8192 bytes" "The token value is a single line within the node's byte bound" "$here/validate.yml" -e @"$tmp/long-token.yml"
expect_fail "multibyte token over 8192 bytes" "The token value is a single line within the node's byte bound" "$here/validate.yml" -e @"$tmp/long-token-utf8.yml"
expect_fail "missing token file" "The token source is a small regular file" "$here/validate.yml" \
    -e 'bbraft_node_token=' -e 'bbraft_node_token_src=/nonexistent/node.token'
expect_fail "whitespace-only token file" "The token source contains one nonempty token line" "$here/validate.yml" \
    -e 'bbraft_node_token=' -e "bbraft_node_token_src=$fixtures/token-whitespace"
expect_fail "public server address" "host must be loopback or private" "$here/validate.yml" -e 'bbraft_node_server_addr=203.0.113.10:7701'
expect_fail "wildcard server address" "the wildcard address is not usable" "$here/validate.yml" -e 'bbraft_node_server_addr=0.0.0.0:7701'
expect_fail "portless server address" "is not host:port" "$here/validate.yml" -e 'bbraft_node_server_addr=127.0.0.1'
expect_fail "link-local server address" "host must be loopback or private" "$here/validate.yml" -e 'bbraft_node_server_addr=169.254.1.1:7701'
expect_fail "IPv6 link-local server address" "host must be loopback or private" "$here/validate.yml" -e 'bbraft_node_server_addr=[fe80::1]:7701'
expect_fail "hostname server address" "host must be localhost or an IP literal" "$here/validate.yml" -e 'bbraft_node_server_addr=example.com:7701'
expect_fail "unbracketed IPv6 server address" "IPv6 addresses must be bracketed" "$here/validate.yml" -e 'bbraft_node_server_addr=::1:7701'
expect_fail "root data dir" "clean canonical absolute POSIX text" "$here/validate.yml" \
    -e 'bbraft_node_data_dir=/' -e 'bbraft_node_image_dir=/images'
expect_fail "root image dir" "clean canonical absolute POSIX text" "$here/validate.yml" -e 'bbraft_node_image_dir=/'
expect_fail "root config dir" "clean canonical absolute POSIX text" "$here/validate.yml" \
    -e 'bbraft_node_config_dir=/' -e 'bbraft_node_token_file=/node.token'
expect_fail "root token file" "clean canonical absolute POSIX text" "$here/validate.yml" -e 'bbraft_node_token_file=/'
expect_fail "root binary path" "clean canonical absolute POSIX text" "$here/validate.yml" -e 'bbraft_node_binary_path=/'
expect_fail "relative data dir" "is not absolute" "$here/validate.yml" -e 'bbraft_node_data_dir=var/lib/bbraft'
expect_fail "traversing data dir" "contains a '..' component" "$here/validate.yml" -e 'bbraft_node_data_dir=/var/lib/../etc'
expect_fail "dot-aliased data dir" "is not a clean canonical path" "$here/validate.yml" -e 'bbraft_node_data_dir=/var/lib/./bbraft'
expect_fail "double-slash data dir" "contains repeated slashes" "$here/validate.yml" -e 'bbraft_node_data_dir=//var/lib/bbraft'
expect_fail "trailing-slash data dir" "is not a clean canonical path" "$here/validate.yml" -e 'bbraft_node_data_dir=/var/lib/bbraft/'
# Default-independent: both sides of each conflict are set explicitly.
expect_fail "conflicting data and image dirs" "clean canonical absolute POSIX text" "$here/validate.yml" \
    -e 'bbraft_node_data_dir=/var/lib/bbraft-fixture' -e 'bbraft_node_image_dir=/var/lib/bbraft-fixture'
expect_fail "conflicting aliased destinations" "is not a clean canonical path" "$here/validate.yml" \
    -e 'bbraft_node_data_dir=/var/lib/bbraft-fixture' -e 'bbraft_node_image_dir=/var/lib/./bbraft-fixture'
expect_fail "conflicting token and binary paths" "clean canonical absolute POSIX text" "$here/validate.yml" \
    -e 'bbraft_node_config_dir=/usr/local/bin' -e 'bbraft_node_binary_path=/usr/local/bin/bbraft-node' \
    -e 'bbraft_node_token_file=/usr/local/bin/bbraft-node'
expect_fail "token outside the config directory" "clean canonical absolute POSIX text" "$here/validate.yml" -e 'bbraft_node_token_file=/tmp/node.token'
expect_fail "node id with a space" "node.id/default-image" "$here/validate.yml" -e '{"bbraft_node_id":"bad node"}'
expect_fail "default image traversal" "node.id/default-image" "$here/validate.yml" -e 'bbraft_node_default_image=../image'
expect_fail "host CPU model" "node.id/default-image" "$here/validate.yml" -e 'bbraft_node_cpu_model=host'
expect_fail "max CPU model" "node.id/default-image" "$here/validate.yml" -e 'bbraft_node_cpu_model=max'
expect_fail "cpu-features owning vmx" "cpu-features must be at most 32" "$here/validate.yml" -e '{"bbraft_node_cpu_features": ["+vmx"]}'
expect_fail "unsigned cpu feature" "cpu-features must be at most 32" "$here/validate.yml" -e '{"bbraft_node_cpu_features": ["pcid"]}'
expect_fail "cpu feature twice" "cpu-features must be at most 32" "$here/validate.yml" -e '{"bbraft_node_cpu_features": ["-pcid", "+pcid"]}'
expect_fail "cpu-features as a string" "cpu-features must be at most 32" "$here/validate.yml" -e 'bbraft_node_cpu_features=-pcid'
expect_fail "invalid cpu-nested" "node.cpu-nested must be auto, on or off" "$here/validate.yml" -e 'bbraft_node_cpu_nested=maybe'
expect_fail "zero boot timeout" "must be a positive Go duration" "$here/validate.yml" -e 'bbraft_node_boot_timeout=0s'
expect_fail "sub-nanosecond boot timeout" "must be a positive Go duration" "$here/validate.yml" -e 'bbraft_node_boot_timeout=0.5ns'
expect_fail "oversized boot timeout" "overflows int64 nanoseconds" "$here/validate.yml" -e 'bbraft_node_boot_timeout=2562048h'
expect_fail "unparseable boot timeout" "must be a positive Go duration" "$here/validate.yml" -e 'bbraft_node_boot_timeout=soon'
expect_fail "negative capacity" "expected a non-negative decimal integer" "$here/validate.yml" -e 'bbraft_node_capacity_vcpus=-1'
expect_fail "fractional capacity" "expected a non-negative decimal integer" "$here/validate.yml" -e 'bbraft_node_capacity_vcpus=1.5'
expect_fail "overflowing vCPU capacity" "overflows the resource type" "$here/validate.yml" -e 'bbraft_node_capacity_vcpus=4294967296'
expect_fail "zero cpu overcommit" "expected a finite positive ratio" "$here/validate.yml" -e 'bbraft_node_cpu_overcommit=0'
expect_fail "negative memory overcommit" "expected a finite positive ratio" "$here/validate.yml" -e 'bbraft_node_memory_overcommit=-1.5'
expect_fail "text disk overcommit" "expected a finite positive ratio" "$here/validate.yml" -e 'bbraft_node_disk_overcommit=abc'
expect_fail "overflowing cpu overcommit" "expected a finite positive ratio" "$here/validate.yml" -e 'bbraft_node_cpu_overcommit=1e309'
expect_fail "underflowing memory overcommit" "expected a finite positive ratio" "$here/validate.yml" -e 'bbraft_node_memory_overcommit=1e-400'
expect_fail "whitespace-padded disk overcommit" "expected a finite positive ratio" "$here/validate.yml" -e '{"bbraft_node_disk_overcommit":" 1.5 "}'
expect_fail "boolean cpu overcommit" "expected a finite positive ratio" "$here/validate.yml" -e @"$here/invalid/overcommit-cpu-bool.yml"
expect_fail "NaN memory overcommit" "expected a finite positive ratio" "$here/validate.yml" -e @"$here/invalid/overcommit-memory-nan.yml"
expect_fail "infinite disk overcommit" "expected a finite positive ratio" "$here/validate.yml" -e @"$here/invalid/overcommit-disk-inf.yml"
expect_fail "world-readable token mode" "token must be owned by the service at 0600/0400" "$here/validate.yml" -e 'bbraft_node_secret_mode=0644'
expect_fail "owner-executable token mode" "token must be owned by the service at 0600/0400" "$here/validate.yml" -e 'bbraft_node_secret_mode=0500'
expect_fail "class-shadowed image file mode" "image files 0444" "$here/validate.yml" -e 'bbraft_node_image_file_mode=0004'
expect_fail "class-shadowed image directory mode" "image directories 0555" "$here/validate.yml" -e 'bbraft_node_image_mode=0005'
expect_fail "world-writable data mode" "data 0700" "$here/validate.yml" -e 'bbraft_node_data_mode=0755'
expect_fail "group-readable config mode" "config 0750" "$here/validate.yml" -e 'bbraft_node_config_mode=0700'
expect_fail "writable image mode" "image directories 0555" "$here/validate.yml" -e 'bbraft_node_image_mode=0755'
expect_fail "writable image file mode" "image files 0444" "$here/validate.yml" -e 'bbraft_node_image_file_mode=0666'
expect_fail "non-executable binary mode" "binary 0755" "$here/validate.yml" -e 'bbraft_node_binary_mode=0644'
expect_fail "read-only binary without world exec" "binary 0755" "$here/validate.yml" -e 'bbraft_node_binary_mode=0751'
expect_fail "dev user network mode in the production role" "always deploys production TAP networking" "$here/validate.yml" \
    -e 'bbraft_node_network_mode=user'
expect_fail "no network prefix" "always deploys production TAP networking" "$here/validate.yml" -e 'bbraft_node_network_prefix='
expect_fail "no network gateway" "always deploys production TAP networking" "$here/validate.yml" -e 'bbraft_node_network_gateway='
expect_fail "no ssh ingress address" "always deploys production TAP networking" "$here/validate.yml" \
    -e 'bbraft_node_ssh_ingress_addr='
expect_fail "no ssh ingress interface" "always deploys production TAP networking" "$here/validate.yml" \
    -e 'bbraft_node_ssh_ingress_iface='
expect_fail "no ssh port range" "always deploys production TAP networking" "$here/validate.yml" -e 'bbraft_node_ssh_port_range='
expect_fail "public guest prefix" "is not a private range" "$here/validate.yml" -e 'bbraft_node_network_prefix=8.8.8.0/24'
expect_fail "narrow guest prefix" "must be between /8 and /30" "$here/validate.yml" -e 'bbraft_node_network_prefix=10.42.0.0/31'
expect_fail "gateway outside the guest prefix" "is outside" "$here/validate.yml" -e 'bbraft_node_network_gateway=10.43.0.1'
expect_fail "gateway on the network address" "is the network address of" "$here/validate.yml" \
    -e 'bbraft_node_network_gateway=10.42.0.0'
expect_fail "gateway on the broadcast address" "is the broadcast address of" "$here/validate.yml" \
    -e 'bbraft_node_network_gateway=10.42.255.255'
expect_fail "three resolvers" "VM networking must be a complete" "$here/validate.yml" \
    -e '{"bbraft_node_network_resolvers":["1.1.1.1","9.9.9.9","8.8.8.8"]}'
expect_fail "no resolvers" "VM networking must be a complete" "$here/validate.yml" \
    -e '{"bbraft_node_network_resolvers":[]}'
expect_fail "resolvers as a string" "VM networking must be a complete" "$here/validate.yml" \
    -e 'bbraft_node_network_resolvers=1.1.1.1,9.9.9.9'
expect_fail "management networks as a string" "VM networking must be a complete" "$here/validate.yml" \
    -e 'bbraft_node_network_management=100.64.0.0/10'
expect_fail "private resolver" "is not a public unicast address" "$here/validate.yml" \
    -e '{"bbraft_node_network_resolvers":["10.42.0.53"]}'
expect_fail "management network overlapping the guest prefix" "VM networking must be a complete" "$here/validate.yml" \
    -e '{"bbraft_node_network_management":["10.42.0.0/24"]}'
expect_fail "malformed management network" "is not an IPv4 prefix" "$here/validate.yml" \
    -e '{"bbraft_node_network_management":["tailnet"]}'
expect_fail "ssh ingress inside the guest prefix" "VM networking must be a complete" "$here/validate.yml" \
    -e 'bbraft_node_ssh_ingress_addr=10.42.0.9'
expect_fail "loopback ssh ingress address" "is not a unicast address" "$here/validate.yml" \
    -e 'bbraft_node_ssh_ingress_addr=127.0.0.1'
expect_fail "unspecified ssh ingress address" "is not a unicast address" "$here/validate.yml" \
    -e 'bbraft_node_ssh_ingress_addr=0.0.0.0'
expect_fail "multicast ssh ingress address" "is not a unicast address" "$here/validate.yml" \
    -e 'bbraft_node_ssh_ingress_addr=224.0.0.1'
expect_fail "link-local ssh ingress address" "is not a unicast address" "$here/validate.yml" \
    -e 'bbraft_node_ssh_ingress_addr=169.254.1.1'
expect_fail "broadcast ssh ingress address" "is not a unicast address" "$here/validate.yml" \
    -e 'bbraft_node_ssh_ingress_addr=255.255.255.255'
expect_fail "dotted-netmask guest prefix" "is not an IPv4 prefix" "$here/validate.yml" \
    -e 'bbraft_node_network_prefix=10.42.0.0/255.255.0.0'
expect_fail "dotted-netmask management prefix" "is not an IPv4 prefix" "$here/validate.yml" \
    -e '{"bbraft_node_network_management":["100.64.0.0/255.192.0.0"]}'
expect_fail "hostmask management prefix" "is not an IPv4 prefix" "$here/validate.yml" \
    -e '{"bbraft_node_network_management":["100.64.0.0/0.63.255.255"]}'
expect_fail "ssh ingress interface equals the bridge" "VM networking must be a complete" "$here/validate.yml" \
    -e 'bbraft_node_ssh_ingress_iface=bbraft0'
expect_fail "invalid ssh ingress interface" "contains a forbidden character" "$here/validate.yml" \
    -e 'bbraft_node_ssh_ingress_iface=bad/iface'
expect_fail "reserved bridge name" '"lo" is reserved' "$here/validate.yml" -e 'bbraft_node_network_bridge=lo'
expect_fail "overlong bridge name" "longer than 15 bytes" "$here/validate.yml" \
    -e 'bbraft_node_network_bridge=bbraft-bridge-0123'
expect_fail "privileged ssh ingress port" "must start at or above 1024" "$here/validate.yml" \
    -e 'bbraft_node_ssh_port_range=22-22999'
expect_fail "descending ssh port range" "is above maximum" "$here/validate.yml" \
    -e 'bbraft_node_ssh_port_range=22999-22000'
expect_fail "malformed ssh port range" "is not an inclusive min-max port range" "$here/validate.yml" \
    -e 'bbraft_node_ssh_port_range=22000'
expect_fail "no image" "nonempty list of unique absolute controller" "$here/validate.yml" -e '{"bbraft_node_images":[]}'
expect_fail "missing image manifest" "manifest.json" "$here/validate.yml" -e "$(images_json "/nonexistent/image-fixture")"
expect_fail "relative image path" "is not absolute" "$here/validate.yml" -e "$(images_json "relative/image-fixture")"
expect_fail "trailing-slash image path" "is not a clean canonical path" "$here/validate.yml" -e "$(images_json "$fixtures/image-fixture/")"
expect_fail "image id mismatch" "directory basename" "$here/validate.yml" -e "$(images_json "$fixtures/image-bad-id")"
expect_fail "unsupported image schema" "directory basename" "$here/validate.yml" -e "$(images_json "$fixtures/image-bad-schema")"
expect_fail "extra image artifact" "directory basename" "$here/validate.yml" -e "$(images_json "$fixtures/image-extra")"
expect_fail "non-qcow2 image root" "root must be qcow2/ext4" "$here/validate.yml" -e "$(images_json "$fixtures/image-bad-root")"
expect_fail "inconsistent image virtual size" "root must be qcow2/ext4" "$here/validate.yml" -e "$(images_json "$fixtures/image-bad-virtual-size")"
expect_fail "numeric-string schemaVersion" "genuine positive integer" "$here/validate.yml" -e "$(images_json "$fixtures/image-string-schema")"
expect_fail "numeric-string diskGiB" "genuine positive integer" "$here/validate.yml" -e "$(images_json "$fixtures/image-string-root")"
expect_fail "numeric-string artifact size" "genuine positive integer" "$here/validate.yml" -e "$(images_json "$fixtures/image-string-artifact")"
expect_fail "boolean virtualSizeBytes" "genuine positive integer" "$here/validate.yml" -e "$(images_json "$fixtures/image-bool-root")"
expect_fail "image artifacts that do not match the manifest" "does not match its manifest; nothing was published" "$here/render.yml" \
    -e "$(images_json "$fixtures/image-bad-hash")"

mkdir -p "$tmp/dup"
cp -a "$fixtures/image-fixture" "$tmp/dup/image-fixture"
expect_fail "duplicate image id" "contains two directories with the same manifest id" "$here/validate.yml" \
    -e "{\"bbraft_node_images\":[\"$fixtures/image-fixture\",\"$tmp/dup/image-fixture\"]}"

# Disposable release-guard images: images/publish.sh refuses an id ending in
# -unpinned-test or distribution.snapshot unpinned; the role publishes
# directly, so it must refuse each marker independently before any target
# mutation. A pinned distribution block stays valid and legacy manifests need
# no distribution at all.
make_image() {
    src=$1
    dst=$2
    snapshot=$3
    cp -a "$src" "$dst"
    python3 -c 'import json, pathlib, sys
path = pathlib.Path(sys.argv[1])
manifest = json.loads(path.read_text())
manifest["id"] = path.parent.name
if sys.argv[2] != "-":
    manifest["distribution"] = {"snapshot": sys.argv[2], "mirror": "https://example.invalid/image-mirror"}
path.write_text(json.dumps(manifest))' "$dst/manifest.json" "$snapshot"
}

make_image "$fixtures/image-fixture" "$tmp/image-unpinned-test" -
make_image "$fixtures/image-fixture" "$tmp/image-unpinned-snapshot" unpinned
make_image "$fixtures/image-fixture" "$tmp/image-pinned-distribution" pinned

play "$here/validate.yml" -e "$(images_json "$tmp/image-pinned-distribution")" >/dev/null
echo "ok: pinned distribution metadata accepted"
expect_fail "disposable unpinned-test image id" "is a disposable unpinned test image" "$here/validate.yml" \
    -e "$(images_json "$tmp/image-unpinned-test")"
expect_fail "unpinned image snapshot marker" "is a disposable unpinned test image" "$here/validate.yml" \
    -e "$(images_json "$tmp/image-unpinned-snapshot")"

echo "== A10 jail supervisor, jail flags and fresh v1 data root =="
sh "$here/run-a10.sh"

echo "== verbose output secret check =="
log=$(mktemp)
if play "$here/validate.yml" -vvv -e @"$here/invalid/token-sentinel.yml" >"$log" 2>&1; then
    echo "FAIL: accepted a multiline sentinel token" >&2
    rm -f "$log"
    exit 1
fi
if grep -F -q -- 'SENTINEL-secret-9f3e' "$log"; then
    echo "FAIL: verbose invalid-secret output leaked the sentinel" >&2
    rm -f "$log"
    exit 1
fi
rm -f "$log"
log=$(mktemp)
if ! play "$here/render.yml" -vvv >"$log" 2>&1; then
    echo "FAIL: verbose render run failed" >&2
    sed -n '1,160p' "$log" >&2
    rm -f "$log"
    exit 1
fi
if grep -F -q -- 'placeholder-node-token-not-real' "$log"; then
    echo "FAIL: verbose output leaked the token placeholder" >&2
    rm -f "$log"
    exit 1
fi
rm -f "$log"
echo "ok: verbose render prints no token"

echo "bbraft_node role tests passed"
