# bamboo-raft Phase 1 disposable-VM node driver. Source; do not execute.
#
# Public API (all paths must be absolute, clean and non-symlinked inputs):
#   e2e_node_start NODE_BINARY IMAGE_VERSION_DIR NODE_ID SERVER_ADDR TOKEN_FILE WORK_DIR
#       Render through the real bbraft_node role, stage root-owned inputs and
#       start one attempt-unique hardened systemd unit. WORK_DIR must not exist;
#       its parent must exist. The helper creates WORK_DIR/{render,evidence} and
#       never deletes WORK_DIR. Root state is outside HOME and survives failure.
#   e2e_node_wait_ready [TIMEOUT_SECONDS]       # default 300
#   e2e_node_wait_registered CALLBACK [ARG...]  # default total deadline 120
#       CALLBACK returns zero only for fresh connected+Info evidence in
#       public.nodes. It receives E2E_NODE_ID, E2E_NODE_IMAGE_ID,
#       E2E_NODE_START_EPOCH, E2E_NODE_READY_EPOCH,
#       E2E_NODE_EXPECTED_VCPUS, E2E_NODE_EXPECTED_MEMORY_MIB,
#       E2E_NODE_EXPECTED_DISK_GIB from this invocation's ready log, and
#       E2E_NODE_ADMITTED_QEMU_VERSION from its jailed CPU probe.
#       Check exact id/image/QEMU version, fresh last_seen_at, vCPU/memory
#       agreement and a positive bounded disk offer; do not compare later free
#       disk with disk_gib exactly. Each callback must bound its own duration
#       and output (for psql, connect_timeout and statement_timeout); the
#       helper bounds the whole retry loop but cannot interrupt shell code.
#   e2e_client COMMAND [ARG...]
#       Run in the helper-owned external client netns as the original UID/GID
#       and groups, with original HOME. Only HOME, PATH, USER, LOGNAME,
#       XDG_CONFIG_HOME, XDG_CACHE_HOME, SSH_AUTH_SOCK, TERM, LANG,
#       BBRAFT_API_URL, BBRAFT_API_KEY and BBRAFT_ORG cross sudo, by name; the helper never puts secrets in argv.
#   e2e_node_stop
#       Refuse before systemd while VMs, allocation records, jail/tool records
#       or self-check fixtures remain, leaving the node available for API
#       deletion. After a proven stop, validate every object before deleting
#       only attempt-owned state. Any failure preserves remaining state for
#       another e2e_node_stop after the caller resolves the named resource;
#       a retry keeps an already-proven stop (the unit may be gone) and
#       revalidates every remaining object before deleting it.
#   e2e_node_leak_proof
#       Read-only final proof of unit/cgroup/netns/link/nft/UID/path absence.
#
# Exports at source time (planned topology, fixed names with collision refusal):
# E2E_NODE_SSH_INGRESS_ADDR=192.0.2.1, E2E_NODE_SSH_INGRESS_IFACE=bbe2e-in0,
# E2E_NODE_HOST_VETH_ADDR=192.0.2.1, E2E_NODE_CLIENT_ADDR=192.0.2.2 and
# E2E_NODE_CLIENT_NETNS=bbe2e-client. The real node stays in the host netns and
# dials the server at SERVER_ADDR (normally 127.0.0.1:7701). The client reaches
# SSH DNAT through the host veth, never loopback/OUTPUT. After sourcing, set
# BBRAFT_API_URL and server.public-url to
# http://$E2E_NODE_HOST_VETH_ADDR:7700 and bind the API to that address or
# 0.0.0.0; signed downloads are fetched through e2e_client too. All client
# commands must use that URL, never a loopback address in their own netns.
# e2e_node_start TCP-probes both listeners before starting and uses exactly
# these planned names; unit and root paths still get a fresh attempt id.
# There is no NetworkNamespacePath substitution or production listener.
#
# REQUIRED GATE: BBRAFT_TEST_VM_SETUP=1 explicitly authorizes sudo -n, systemd,
# network and root-state mutation in one throwaway VM. Never run on a host.
# The caller owns disposable-VM setup/restore of net.ipv4.ip_forward=1; this
# helper always fails closed if forwarding is off and never changes sysctls.
# Commands are timeout-bounded. Defaults: E2E_NODE_READY_TIMEOUT=300,
# E2E_NODE_REGISTERED_TIMEOUT=120, E2E_NODE_STOP_TIMEOUT=120 (seconds).
# Ansible: set E2E_NODE_ANSIBLE_PLAYBOOK to an existing ansible-playbook
# executable to use that controller directly. Otherwise the helper uses the
# pinned user-space controller of the role's tests/controller.sh at
# $BBRAFT_ROLE_CONTROLLER_HOME (default ${XDG_CACHE_HOME:-$HOME/.cache}/bbraft-node-role/
# controller) when it already reports ansible-core 2.22.0, and bootstraps it
# with a work-dir copy of that installer (its final run.sh exec replaced by
# exit 0, run unprivileged with a 600s bound) when it does not. No mise is
# needed. Both paths verify ansible-core 2.22.0 and render validate.yml plus
# the role template (never the full role suite).
#
# Sourcing otherwise defines functions/constants only: no tool checks, sudo,
# umask, traps, options, cd, exits, stdout or kernel/filesystem/network actions.
# Functions return nonzero on error with caller errexit either enabled or
# disabled. One attempt per shell.

_E2E_NODE_ANSIBLE_VERSION=2.22.0
_E2E_NODE_UID_RANGE=900000-900999
_E2E_NODE_HOST_ADDR=192.0.2.1
_E2E_NODE_CLIENT_ADDR=192.0.2.2
_E2E_NODE_GUEST_PREFIX=10.42.0.0/16
_E2E_NODE_GUEST_GATEWAY=10.42.0.1
_E2E_NODE_CAP_MASK=00000000082c11eb
_E2E_NODE_ENV_LIST=HOME,PATH,USER,LOGNAME,XDG_CONFIG_HOME,XDG_CACHE_HOME,SSH_AUTH_SOCK,TERM,LANG,BBRAFT_API_URL,BBRAFT_API_KEY,BBRAFT_ORG
export E2E_NODE_HOST_VETH_ADDR=$_E2E_NODE_HOST_ADDR
export E2E_NODE_CLIENT_ADDR=$_E2E_NODE_CLIENT_ADDR
export E2E_NODE_SSH_INGRESS_ADDR=$_E2E_NODE_HOST_ADDR
export E2E_NODE_SSH_INGRESS_IFACE=bbe2e-in0
export E2E_NODE_CLIENT_NETNS=bbe2e-client

_e2e_node_error() {
	printf 'E2E NODE FAIL: %s\n' "$*" >&2
	return 1
}

_e2e_node_root() {
	local seconds=$1
	shift
	if [[ ! $seconds =~ ^[1-9][0-9]*$ ]] || [ "$#" -eq 0 ]; then
		_e2e_node_error 'internal root command bound' || return
	fi
	sudo -n -- timeout --signal=TERM --kill-after=2s "${seconds}s" "$@"
}

_e2e_node_timeout() {
	local value=${1:-} default=$2 name=$3
	if [ -z "$value" ]; then value=$default; fi
	if [[ ! $value =~ ^[1-9][0-9]*$ ]] || [ "$value" -gt 3600 ]; then
		_e2e_node_error "$name must be 1..3600 seconds" || return
	fi
	printf '%s\n' "$value"
}

_e2e_node_now() {
	REPLY=$EPOCHSECONDS
}

_e2e_node_sleep() {
	sleep 1
}

_e2e_node_tcp_check() {
	local address=$1
	python3 -I - "$address" <<'PY'
import ipaddress, socket, sys
addr = sys.argv[1]
if addr.startswith("["):
    host, sep, port = addr[1:].partition("]:")
else:
    host, sep, port = addr.rpartition(":")
if not sep or not port.isdecimal() or not 1 <= int(port) <= 65535:
    raise SystemExit("invalid host:port")
if host != "localhost":
    ip = ipaddress.ip_address(host)
    if not (ip.is_loopback or ip.is_private):
        raise SystemExit("node/API test address must be loopback or private")
with socket.create_connection((host, int(port)), timeout=5) as s:
    s.settimeout(5)
PY
}

_e2e_node_inputs() {
	[ "$#" -eq 6 ] || { _e2e_node_error 'e2e_node_start: six arguments required'; return 1; }
	[ "${BBRAFT_TEST_VM_SETUP:-}" = 1 ] || { _e2e_node_error 'set BBRAFT_TEST_VM_SETUP=1 only inside the disposable VM'; return 1; }
	[ -z "${_e2e_node_attempt:-}" ] || { _e2e_node_error 'one node attempt is already active in this shell'; return 1; }
	local binary=$1 image=$2 node_id=$3 server=$4 token=$5 work=$6 p
	for p in "$binary" "$image" "$token" "$work"; do
		[[ $p == /* && $p != *$'\n'* && $p != *$'\r'* ]] || { _e2e_node_error "not an absolute safe path: $p"; return 1; }
		[ "$(python3 -I -c 'import os,sys; print(os.path.normpath(sys.argv[1]))' "$p")" = "$p" ] || { _e2e_node_error "path is not clean: $p"; return 1; }
	done
	[[ $node_id =~ ^[a-z0-9][a-z0-9_-]{0,62}$ ]] || { _e2e_node_error 'invalid node id'; return 1; }
	python3 -I - "$server" <<'PY' || { _e2e_node_error 'invalid or non-private server link address'; return 1; }
import ipaddress, sys
addr = sys.argv[1]
if addr.startswith("["):
    host, sep, port = addr[1:].partition("]:")
else:
    host, sep, port = addr.rpartition(":")
assert sep and port.isdecimal() and str(int(port)) == port and 1 <= int(port) <= 65535
if host != "localhost":
    ip = ipaddress.ip_address(host)
    assert ip.is_loopback or ip.is_private
PY
	[ -f "$binary" ] && [ ! -L "$binary" ] && [ -x "$binary" ] || { _e2e_node_error 'node binary is not a regular executable'; return 1; }
	[ -d "$image" ] && [ ! -L "$image" ] || { _e2e_node_error 'image version is not a regular directory'; return 1; }
	[ -f "$token" ] && [ ! -L "$token" ] && [ -r "$token" ] || { _e2e_node_error 'token is not a regular readable file'; return 1; }
	local mode size
	mode=$(stat -c %a -- "$token") || return 1
	size=$(stat -c %s -- "$token") || return 1
	[ $((8#$mode & 077)) -eq 0 ] || { _e2e_node_error 'token mode must not grant group/other access'; return 1; }
	[ "$size" -gt 0 ] && [ "$size" -le 8192 ] || { _e2e_node_error 'token size must be 1..8192 bytes'; return 1; }
	[ ! -e "$work" ] && [ ! -L "$work" ] || { _e2e_node_error 'fresh work dir already exists'; return 1; }
	local parent=${work%/*}
	[ -n "$parent" ] || parent=/
	[ -d "$parent" ] && [ -w "$parent" ] || { _e2e_node_error 'work parent is missing or unwritable'; return 1; }
	python3 -I - "$image" <<'PY' || { _e2e_node_error 'image manifest id does not match its directory'; return 1; }
import json, os, sys
root = sys.argv[1]
with open(os.path.join(root, "manifest.json"), "rb") as f:
    manifest = json.load(f)
assert manifest.get("id") == os.path.basename(root)
PY
}

_e2e_node_io_device() {
	local dev parent name
	dev=$(findmnt -rno MAJ:MIN -T /var/lib) || return 1
	# findmnt may pad the column with blanks. Exactly one line with exactly one
	# MAJ:MIN token is accepted: trim only leading/trailing spaces and tabs.
	[[ $dev != *$'\n'* ]] || { _e2e_node_error 'findmnt reported more than one data device'; return 1; }
	dev=${dev#"${dev%%[! $'\t']*}"}
	dev=${dev%"${dev##*[! $'\t']}"}
	[[ $dev =~ ^[1-9][0-9]*:[0-9]+$ ]] || { _e2e_node_error "cannot identify data block device: $dev"; return 1; }
	if [ -e "/sys/dev/block/$dev/partition" ]; then
		parent=$(readlink -f "/sys/dev/block/$dev/..") || return 1
		name=${parent##*/}
		dev=$(cat "/sys/class/block/$name/dev") || return 1
	fi
	[[ $dev =~ ^[1-9][0-9]*:[0-9]+$ ]] || { _e2e_node_error 'data root is not on one whole block disk'; return 1; }
	printf '%s\n' "$dev"
}

_e2e_node_show() {
	local service=${1:-$_e2e_node_service}
	shift || true
	_e2e_node_root 30 systemctl show "$service.service" "$@"
}

_e2e_node_preflight() {
	local show
	show=$(_e2e_node_show "$_e2e_node_service" -p LoadState) || { _e2e_node_error 'cannot query systemd'; return 1; }
	[ "$show" = 'LoadState=not-found' ] || { _e2e_node_error "unit already exists: $show"; return 1; }
	_e2e_node_root 30 python3 -I - "$_e2e_node_service" "$_e2e_node_unit_path" "$_e2e_node_data_dir" "$_e2e_node_config_dir" "$_e2e_node_stage_dir" "$_e2e_node_client_netns" <<'PY' || { _e2e_node_error 'root path/device/cgroup preflight refused'; return 1; }
import os, stat, sys
service, unit, data, config, stage, netns = sys.argv[1:]
paths = [unit, unit + ".d", data, config, stage, "/run/netns/" + netns,
         "/etc/systemd/system/" + service + ".service",
         "/etc/systemd/system/" + service + ".service.d",
         "/etc/systemd/system/multi-user.target.wants/" + service + ".service",
         "/run/systemd/system/multi-user.target.wants/" + service + ".service",
         "/sys/fs/cgroup/system.slice/" + service + ".service"]
for path in paths:
    try:
        os.lstat(path)
    except FileNotFoundError:
        continue
    raise SystemExit("already exists: " + path)
for path in ["/dev/kvm", "/dev/vhost-vsock", "/dev/vhost-net", "/dev/net/tun",
             "/usr/bin/qemu-system-x86_64", "/usr/bin/qemu-img",
             "/usr/share/seabios/bios-256k.bin", "/usr/share/qemu/linuxboot_dma.bin",
             "/usr/share/qemu/kvmvapic.bin"]:
    st = os.stat(path, follow_symlinks=False)
    if path.startswith("/dev/"):
        assert stat.S_ISCHR(st.st_mode), path
    else:
        assert stat.S_ISREG(st.st_mode) and st.st_uid == 0 and not st.st_mode & 0o022, path
with open("/proc/sys/net/ipv4/ip_forward", "rb") as f:
    assert f.read(16).strip() == b"1", "net.ipv4.ip_forward must be 1"
PY
	local links addresses rules procs
	links=$(_e2e_node_root 30 ip -j link show) || return 1
	addresses=$(_e2e_node_root 30 ip -j addr show) || return 1
	printf '%s\n' "$links" >"$_e2e_node_evidence_dir/preflight-links.json"
	printf '%s\n' "$addresses" >"$_e2e_node_evidence_dir/preflight-addresses.json"
	python3 -I - "$_e2e_node_bridge" "$_e2e_node_host_if" "$_e2e_node_client_if" "$_e2e_node_evidence_dir/preflight-links.json" "$_e2e_node_evidence_dir/preflight-addresses.json" <<'PY' || { _e2e_node_error 'network name/address collision'; return 1; }
import json, sys
names = set(sys.argv[1:4])
with open(sys.argv[4], "rb") as f:
    links = json.load(f)
with open(sys.argv[5], "rb") as f:
    addrs = json.load(f)
assert not names.intersection(x.get("ifname") for x in links)
for link in addrs:
    for addr in link.get("addr_info", []):
        assert addr.get("local") not in {"192.0.2.1", "192.0.2.2"}
PY
	rules=$(_e2e_node_root 30 nft -j list ruleset) || { _e2e_node_error 'cannot read nft ruleset'; return 1; }
	printf '%s\n' "$rules" >"$_e2e_node_evidence_dir/preflight-nft.json"
	python3 -I - "$_e2e_node_evidence_dir/preflight-nft.json" <<'PY' || { _e2e_node_error 'bbraft nft tables already exist'; return 1; }
import json, sys
with open(sys.argv[1], "rb") as f:
    doc = json.load(f)
for item in doc.get("nftables", []):
    table = item.get("table")
    assert not table or (table.get("family"), table.get("name")) not in {("inet", "bbraft_net"), ("bridge", "bbraft_bridge")}
PY
	procs=$(_e2e_node_root 30 ps -eo uid=,pid=,comm=) || return 1
	printf '%s\n' "$procs" >"$_e2e_node_evidence_dir/preflight-processes.txt"
	python3 -I - "$_e2e_node_evidence_dir/preflight-processes.txt" <<'PY' || { _e2e_node_error 'jail UID range is already in use'; return 1; }
import sys
with open(sys.argv[1], "rb") as f:
    for raw in f:
        line = raw.decode()
        fields = line.split()
        if fields and fields[0].isdecimal() and 900000 <= int(fields[0]) <= 900999:
            raise SystemExit("jail uid process: " + line.strip())
PY
}

_e2e_node_render() {
	local repo=$1 binary=$2 image=$3 token=$4 io_device=$5
	local ansible=$repo/infra/ansible
	local play=$_e2e_node_work/render/play.yml vars=$_e2e_node_work/render/vars.json
	cat >"$play" <<'YAML'
- name: Render one disposable Phase 1 node through the production role
  hosts: localhost
  connection: local
  gather_facts: false
  vars:
    bbraft_node_service: "{{ e2e.service }}"
    bbraft_node_id: "{{ e2e.node_id }}"
    bbraft_node_server_addr: "{{ e2e.server }}"
    bbraft_node_data_dir: "{{ e2e.data }}"
    bbraft_node_image_dir: "{{ e2e.images }}"
    bbraft_node_config_dir: "{{ e2e.config }}"
    bbraft_node_token_file: "{{ e2e.config }}/node.token"
    bbraft_node_binary_path: "{{ e2e.binary_path }}"
    bbraft_node_unit_path: "{{ e2e.unit }}"
    bbraft_node_binary_src: "{{ e2e.binary_src }}"
    bbraft_node_binary_sha256: "{{ e2e.binary_sha256 }}"
    bbraft_node_token_src: "{{ e2e.token_src }}"
    bbraft_node_images: ["{{ e2e.image_src }}"]
    bbraft_node_default_image: "{{ e2e.image_id }}"
    bbraft_node_network_bridge: "{{ e2e.bridge }}"
    hardening_vm_bridge_interfaces: ["{{ e2e.bridge }}"]
    bbraft_node_network_prefix: "10.42.0.0/16"
    bbraft_node_network_gateway: "10.42.0.1"
    bbraft_node_ssh_ingress_addr: "192.0.2.1"
    bbraft_node_ssh_ingress_iface: "{{ e2e.host_if }}"
    bbraft_node_ssh_port_range: "30000-30010"
    bbraft_node_jail_uid_range: "900000-900999"
    bbraft_node_jail_io_device: "{{ e2e.io_device }}"
    bbraft_node_jail_io_write_bps: 104857600
    bbraft_node_disk_overcommit: 1.0
    bbraft_node_manage_service: false
  tasks:
    - name: Validate with the actual role
      ansible.builtin.include_role:
        name: bbraft_node
        tasks_from: validate
        public: true
    - name: Render the actual hardened unit
      ansible.builtin.template:
        src: "{{ e2e.template }}"
        dest: "{{ e2e.rendered_unit }}"
        mode: "0644"
YAML
	python3 -I - "$vars" "$binary" "$image" "$token" "$io_device" \
		"$ansible/roles/bbraft_node/templates/bbraft-node.service.j2" \
		"$_e2e_node_service" "$E2E_NODE_ID" "$_e2e_node_server_addr" "$_e2e_node_data_dir" \
		"$_e2e_node_image_root" "$_e2e_node_config_dir" "$_e2e_node_binary_path" \
		"$_e2e_node_unit_path" "$_e2e_node_rendered_unit" "$E2E_NODE_IMAGE_ID" \
		"$_e2e_node_bridge" "$_e2e_node_host_if" <<'PY' || return 1
import hashlib, json, sys
(out, binary, image, token, io_device, template, service, node_id, server, data_dir,
 image_root, config_dir, binary_path, unit, rendered_unit, image_id, bridge, host_if) = sys.argv[1:]
# Values arrive as argv from this shell's private attempt state; they were
# never exported, so the child environment must not be consulted.
for value in (service, node_id, server, data_dir, image_root, config_dir,
              binary_path, unit, rendered_unit, image_id, bridge, host_if):
    assert "\n" not in value and "\r" not in value, value
with open(binary, "rb") as f:
    sha = hashlib.file_digest(f, "sha256").hexdigest()
vars_doc = {"e2e": {
    "service": service, "node_id": node_id, "server": server, "data": data_dir,
    "images": image_root, "config": config_dir, "binary_path": binary_path, "unit": unit,
    "binary_src": binary, "binary_sha256": sha, "token_src": token,
    "image_src": image, "image_id": image_id, "bridge": bridge, "host_if": host_if,
    "io_device": io_device, "template": template, "rendered_unit": rendered_unit}}
with open(out, "x") as f:
    json.dump(vars_doc, f, indent=2, sort_keys=True)
    f.write("\n")
PY
	local version
	local -a ansible_cmd
	if [ -n "${E2E_NODE_ANSIBLE_PLAYBOOK:-}" ]; then
		[[ $E2E_NODE_ANSIBLE_PLAYBOOK == /* ]] && [ -x "$E2E_NODE_ANSIBLE_PLAYBOOK" ] && [ ! -L "$E2E_NODE_ANSIBLE_PLAYBOOK" ] || { _e2e_node_error 'E2E_NODE_ANSIBLE_PLAYBOOK must be an absolute regular executable'; return 1; }
		ansible_cmd=("$E2E_NODE_ANSIBLE_PLAYBOOK")
	else
		local controller_root=${BBRAFT_ROLE_CONTROLLER_HOME:-${XDG_CACHE_HOME:-${HOME:-}/.cache}/bbraft-node-role/controller}
		local pinned=$controller_root/venv/bin/ansible-playbook
		[[ $controller_root == /* ]] || { _e2e_node_error 'cannot locate an absolute pinned-controller cache (set BBRAFT_ROLE_CONTROLLER_HOME)'; return 1; }
		version=
		if [ -x "$pinned" ] && [ ! -L "$pinned" ]; then version=$("$pinned" --version 2>&1) || version=; fi
		if [[ $version != *"[core $_E2E_NODE_ANSIBLE_VERSION]"* ]]; then
			# Same pinned installer as the role tests, minus the full suite: the
			# copy keeps everything up to the final `exec sh "$here/run.sh"`.
			local bootstrap=$_e2e_node_work/render/bootstrap.sh
			# shellcheck disable=SC2016 # the Python text must not expand shell variables
			python3 -I -c 'import os, sys
src, dst = sys.argv[1:]
with open(src, "rb") as f:
    text = f.read()
anchor = b"exec sh \"$here/run.sh\"\n"
assert text.count(anchor) == 1
fd = os.open(dst, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o700)
with os.fdopen(fd, "wb") as f:
    os.fchmod(f.fileno(), 0o700)
    f.write(text.replace(anchor, b"exit 0\n"))' "$ansible/roles/bbraft_node/tests/controller.sh" "$bootstrap" || { _e2e_node_error 'cannot prepare the pinned Ansible bootstrap'; return 1; }
			BBRAFT_ROLE_CONTROLLER_HOME=$controller_root timeout --signal=TERM --kill-after=5s 600s sh "$bootstrap" "$binary" >"$_e2e_node_evidence_dir/ansible-bootstrap.log" 2>&1 || { _e2e_node_error "pinned Ansible bootstrap failed; see $_e2e_node_evidence_dir/ansible-bootstrap.log"; return 1; }
		fi
		ansible_cmd=("$pinned")
	fi
	version=$("${ansible_cmd[@]}" --version 2>&1) || { _e2e_node_error 'cannot execute pinned ansible-playbook'; return 1; }
	[[ $version == *"[core $_E2E_NODE_ANSIBLE_VERSION]"* ]] || { _e2e_node_error "ansible-core $_E2E_NODE_ANSIBLE_VERSION required"; return 1; }
	ANSIBLE_CONFIG=$ansible/ansible.cfg ANSIBLE_ROLES_PATH=$ansible/roles \
		"${ansible_cmd[@]}" -i "$ansible/roles/bbraft_node/tests/inventory.yml" "$play" -e "@$vars" >"$_e2e_node_evidence_dir/ansible-render.log" 2>&1 || { _e2e_node_error "role render failed; see $_e2e_node_evidence_dir/ansible-render.log"; return 1; }
	[ -f "$_e2e_node_rendered_unit" ] || { _e2e_node_error 'role rendered no unit'; return 1; }
	if grep -q '^NetworkNamespacePath=' "$_e2e_node_rendered_unit"; then _e2e_node_error 'test-only NetworkNamespacePath is forbidden'; return 1; fi
}

_e2e_node_stage() {
	_e2e_node_root 120 python3 -I - \
		"$_e2e_node_stage_dir" "$_e2e_node_config_dir" "$_e2e_node_data_dir" \
		"$_e2e_node_image_root" "$_e2e_node_binary_path" "$_e2e_node_unit_path" \
		"$_e2e_node_rendered_unit" "$_e2e_node_input_binary" "$_e2e_node_input_token" "$_e2e_node_input_image" <<'PY'
import hashlib, json, os, stat, sys
stage, config, data, images, binary, unit, unit_src, binary_src, token_src, image_src = sys.argv[1:]
state = stage + "/state"
log_path = state + "/ownership.ndjson"
expected_sha = hashlib.sha256()
with open(binary_src, "rb") as f:
    for block in iter(lambda: f.read(1024 * 1024), b""):
        expected_sha.update(block)
expected_sha = expected_sha.hexdigest()

def identity(path):
    st = os.lstat(path)
    return {"path": path, "dir": stat.S_ISDIR(st.st_mode), "dev": st.st_dev,
            "ino": st.st_ino, "mode": stat.S_IMODE(st.st_mode), "uid": st.st_uid, "gid": st.st_gid}

def record(path):
    item = identity(path)
    fd = os.open(log_path, os.O_WRONLY | os.O_APPEND | os.O_NOFOLLOW)
    with os.fdopen(fd, "ab", buffering=0) as f:
        f.write(json.dumps(item, sort_keys=True).encode() + b"\n")
        os.fsync(f.fileno())

def mkdir(path, mode):
    os.mkdir(path, mode)
    os.chmod(path, mode, follow_symlinks=False)
    record(path)

def copy(src, dst, mode, limit=1024 * 1024 * 1024 * 1024):
    infd = os.open(src, os.O_RDONLY | os.O_NOFOLLOW)
    inst = os.fstat(infd)
    if not stat.S_ISREG(inst.st_mode) or inst.st_size > limit:
        os.close(infd)
        raise RuntimeError("invalid source: " + src)
    outfd = os.open(dst, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, mode)
    h = hashlib.sha256()
    try:
        with os.fdopen(infd, "rb", closefd=True) as inp, os.fdopen(outfd, "wb", closefd=True) as out:
            remaining = inst.st_size
            while remaining:
                block = inp.read(min(1024 * 1024, remaining))
                if not block:
                    raise RuntimeError("source shrank: " + src)
                remaining -= len(block)
                h.update(block)
                out.write(block)
            if inp.read(1):
                raise RuntimeError("source grew: " + src)
            out.flush()
            os.fsync(out.fileno())
        os.chmod(dst, mode, follow_symlinks=False)
        record(dst)
        return h.hexdigest()
    except Exception:
        try:
            os.close(infd)
        except OSError:
            pass
        try:
            os.close(outfd)
        except OSError:
            pass
        raise

os.mkdir(stage, 0o755)
os.mkdir(state, 0o700)
fd = os.open(log_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
os.close(fd)
record(stage)
record(state)
record(log_path)
mkdir(stage + "/bin", 0o755)
if copy(binary_src, binary, 0o555) != expected_sha:
    raise RuntimeError("staged node checksum mismatch")
mkdir(config, 0o750)
copy(token_src, config + "/node.token", 0o600, 8192)
mkdir(data, 0o700)
marker = data + "/.bbraft-node-data-v1"
fd = os.open(marker, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
with os.fdopen(fd, "wb") as f:
    f.write(b"bbraft-node data root v1\n")
    os.fsync(f.fileno())
record(marker)
mkdir(images, 0o555)
version = images + "/" + os.path.basename(image_src)
mkdir(version, 0o555)
with open(image_src + "/manifest.json", "rb") as f:
    manifest = json.load(f)
assert manifest["id"] == os.path.basename(image_src)
assert set(os.listdir(image_src)) == {"kernel", "initrd", "root.qcow2", "packages.json", "manifest.json"}
for name in ["kernel", "initrd", "root.qcow2", "packages.json", "manifest.json"]:
    digest = copy(image_src + "/" + name, version + "/" + name, 0o444)
    artifact = manifest.get("artifacts", {}).get(name)
    if artifact and (artifact.get("sha256") != digest or artifact.get("size") != os.lstat(version + "/" + name).st_size):
        raise RuntimeError("staged image artifact mismatch: " + name)
copy(unit_src, unit, 0o644, 1024 * 1024)
PY
}

_e2e_node_pin_link() {
	local namespace=$1 name=$2 variable=$3 out link_index
	if [ -n "$namespace" ]; then
		out=$(_e2e_node_root 30 ip netns exec "$namespace" ip -o link show dev "$name") || return 1
	else
		out=$(_e2e_node_root 30 ip -o link show dev "$name") || return 1
	fi
	link_index=${out%%:*}
	[[ $link_index =~ ^[1-9][0-9]*$ ]] || { _e2e_node_error "cannot pin link $name"; return 1; }
	printf -v "$variable" '%s' "$link_index"
}

_e2e_node_network_create() {
	_e2e_node_root 30 ip link add name "$_e2e_node_bridge" type bridge || { _e2e_node_error 'create owned bridge failed'; return 1; }
	_e2e_node_pin_link '' "$_e2e_node_bridge" _e2e_node_bridge_index || return 1
	_e2e_node_root 30 ip link set dev "$_e2e_node_bridge" alias "bbraft-e2e-$_e2e_node_attempt" || return 1
	_e2e_node_root 30 ip netns add "$_e2e_node_client_netns" || { _e2e_node_error 'create client netns failed'; return 1; }
	_e2e_node_netns_ino=$(_e2e_node_root 30 stat -c %i "/run/netns/$_e2e_node_client_netns") || return 1
	[[ $_e2e_node_netns_ino =~ ^[1-9][0-9]*$ ]] || { _e2e_node_error 'client netns identity unavailable; preserving'; return 1; }
	_e2e_node_root 30 ip link add "$_e2e_node_host_if" type veth peer name "$_e2e_node_client_if" || { _e2e_node_error 'create client veth failed'; return 1; }
	_e2e_node_pin_link '' "$_e2e_node_host_if" _e2e_node_host_if_index || return 1
	_e2e_node_root 30 ip link set "$_e2e_node_client_if" netns "$_e2e_node_client_netns" || return 1
	_e2e_node_pin_link "$_e2e_node_client_netns" "$_e2e_node_client_if" _e2e_node_client_if_index || return 1
	_e2e_node_root 30 ip addr add "$_E2E_NODE_HOST_ADDR/30" dev "$_e2e_node_host_if" || return 1
	_e2e_node_root 30 ip link set dev "$_e2e_node_host_if" up || return 1
	_e2e_node_root 30 ip netns exec "$_e2e_node_client_netns" ip addr add "$_E2E_NODE_CLIENT_ADDR/30" dev "$_e2e_node_client_if" || return 1
	_e2e_node_root 30 ip netns exec "$_e2e_node_client_netns" ip link set dev "$_e2e_node_client_if" up || return 1
	_e2e_node_root 30 ip netns exec "$_e2e_node_client_netns" ip link set dev lo up || return 1
	e2e_client python3 -I - "$_E2E_NODE_HOST_ADDR:7700" <<'PY' || { _e2e_node_error 'server API is not reachable from the owned client netns'; return 1; }
import socket, sys
host, port = sys.argv[1].rsplit(":", 1)
with socket.create_connection((host, int(port)), timeout=5):
    pass
PY
}

e2e_client() {
	if [ -z "${_e2e_node_attempt:-}" ] || [ -z "${_e2e_node_client_netns:-}" ]; then
		_e2e_node_error 'e2e_client requires an active node attempt' || return
	fi
	if [ "$#" -eq 0 ]; then _e2e_node_error 'e2e_client: command required' || return; fi
	sudo -n --preserve-env="$_E2E_NODE_ENV_LIST" -- \
		ip netns exec "$_e2e_node_client_netns" \
		setpriv --reuid="$_e2e_node_caller_uid" --regid="$_e2e_node_caller_gid" --init-groups -- "$@"
}

e2e_node_start() {
	_e2e_node_inputs "$@" || return 1
	local binary=$1 image=$2 node_id=$3 server=$4 token=$5 work=$6
	local repo script_dir attempt io_device
	script_dir=$(dirname -- "${BASH_SOURCE[0]}") || return 1
	repo=$(readlink -f "$script_dir/../..") || return 1
	[ -f "$repo/infra/ansible/mise.toml" ] || { _e2e_node_error 'cannot locate the repository Ansible role'; return 1; }
	attempt=$(python3 -I -c 'import uuid; print(uuid.uuid4().hex[:12])') || return 1
	[[ $attempt =~ ^[0-9a-f]{12}$ ]] || return 1
	io_device=$(_e2e_node_io_device) || return 1
	mkdir -m 0755 -- "$work" || { _e2e_node_error 'cannot create fresh work dir'; return 1; }
	mkdir -m 0755 -- "$work/render" "$work/evidence" || { _e2e_node_error 'cannot create work subdirectories'; return 1; }

	_e2e_node_attempt=$attempt
	_e2e_node_work=$work
	_e2e_node_evidence_dir=$work/evidence
	_e2e_node_service=bbraft-e2e-$attempt
	_e2e_node_unit_path=/run/systemd/system/$_e2e_node_service.service
	_e2e_node_cgroup_dir=/sys/fs/cgroup/system.slice/$_e2e_node_service.service
	_e2e_node_data_dir=/var/lib/$_e2e_node_service
	_e2e_node_image_root=$_e2e_node_data_dir/images
	_e2e_node_config_dir=/etc/$_e2e_node_service
	_e2e_node_stage_dir=/var/lib/$_e2e_node_service-stage
	_e2e_node_binary_path=$_e2e_node_stage_dir/bin/bbraft-node
	_e2e_node_rendered_unit=$work/render/$_e2e_node_service.service
	_e2e_node_input_binary=$binary
	_e2e_node_input_token=$token
	_e2e_node_input_image=$image
	_e2e_node_server_addr=$server
	_e2e_node_bridge=br$attempt
	_e2e_node_host_if=$E2E_NODE_SSH_INGRESS_IFACE
	_e2e_node_client_if=bbe2e-cl0
	_e2e_node_client_netns=$E2E_NODE_CLIENT_NETNS
	_e2e_node_caller_uid=$(id -u)
	_e2e_node_caller_gid=$(id -g)
	_e2e_node_caller_home=$HOME
	_e2e_node_started=0 _e2e_node_ready=0 _e2e_node_stopped=0 _e2e_node_stop_proven=0
	_e2e_node_preflight_failed=0 _e2e_node_unit_owned=0 _e2e_node_cgroup_owned=0 _e2e_node_cgroup_ino=
	export E2E_NODE_ID=$node_id E2E_NODE_IMAGE_ID=${image##*/}
	export E2E_NODE_SSH_INGRESS_ADDR=$_E2E_NODE_HOST_ADDR E2E_NODE_SSH_INGRESS_IFACE=$_e2e_node_host_if
	export E2E_NODE_HOST_VETH_ADDR=$_E2E_NODE_HOST_ADDR E2E_NODE_CLIENT_ADDR=$_E2E_NODE_CLIENT_ADDR E2E_NODE_CLIENT_NETNS=$_e2e_node_client_netns

	_e2e_node_preflight || { _e2e_node_preflight_failed=1; _e2e_node_diagnostics; return 1; }
	_e2e_node_tcp_check "$server" || { _e2e_node_error 'server node link is not reachable'; return 1; }
	_e2e_node_render "$repo" "$binary" "$image" "$token" "$io_device" || return 1
	_e2e_node_stage || { _e2e_node_error 'root staging failed; state preserved for e2e_node_stop'; return 1; }
	_e2e_node_unit_owned=1
	_e2e_node_network_create || { _e2e_node_error 'network setup failed; state preserved for e2e_node_stop'; return 1; }
	_e2e_node_root 60 systemctl daemon-reload || { _e2e_node_error 'systemd daemon-reload failed'; return 1; }
	local show
	show=$(_e2e_node_show "$_e2e_node_service" -p LoadState -p FragmentPath -p DropInPaths) || return 1
	printf '%s\n' "$show" >"$_e2e_node_evidence_dir/installed-show.txt"
	python3 -I - "$_e2e_node_unit_path" "$_e2e_node_evidence_dir/installed-show.txt" <<'PY' || { _e2e_node_error 'installed unit did not load exactly as rendered'; return 1; }
import sys
with open(sys.argv[2], "rb") as f:
    props = dict(line.split("=", 1) for line in f.read().decode().splitlines() if "=" in line)
assert props.get("LoadState") == "loaded"
assert props.get("FragmentPath") == sys.argv[1]
assert not props.get("DropInPaths")
PY
	_e2e_node_now
	export E2E_NODE_START_EPOCH=$REPLY
	_e2e_node_root "${E2E_NODE_STOP_TIMEOUT:-120}" systemctl start "$_e2e_node_service.service" || { _e2e_node_diagnostics; _e2e_node_error 'systemd start failed'; return 1; }
	_e2e_node_started=1
	show=$(_e2e_node_show "$_e2e_node_service" -p InvocationID -p ActiveState -p MainPID -p NRestarts) || return 1
	local line key value
	_e2e_node_invocation=
	while IFS= read -r line; do
		key=${line%%=*}; value=${line#*=}
		case $key in InvocationID) _e2e_node_invocation=$value ;; esac
	done <<<"$show"
	[[ $_e2e_node_invocation =~ ^[0-9a-f]{32}$ ]] || { _e2e_node_diagnostics; _e2e_node_error 'no InvocationID after start'; return 1; }
	_e2e_node_cgroup_ino=$(_e2e_node_root 10 stat -c %i "$_e2e_node_cgroup_dir") || { _e2e_node_error 'cannot pin the new service cgroup; preserving'; return 1; }
	[[ $_e2e_node_cgroup_ino =~ ^[1-9][0-9]*$ ]] || { _e2e_node_error 'invalid service cgroup inode; preserving'; return 1; }
	_e2e_node_cgroup_owned=1
	printf '%s\n' "$show" >"$_e2e_node_evidence_dir/start-show.txt"
	printf 'node attempt %s started; evidence %s\n' "$attempt" "$_e2e_node_evidence_dir"
}

_e2e_node_main_sample() {
	local show line key value inv="" pid="" exe=0:0 ticks=0 link=unavailable
	show=$(_e2e_node_show "$_e2e_node_service" -p InvocationID -p MainPID) || return 1
	while IFS= read -r line; do
		key=${line%%=*}; value=${line#*=}
		case $key in InvocationID) inv=$value ;; MainPID) pid=$value ;; esac
	done <<<"$show"
	[[ $pid =~ ^[1-9][0-9]*$ ]] || { _e2e_node_error 'MainPID is zero or invalid'; return 1; }
	exe=$(_e2e_node_root 10 stat -Lc %d:%i "/proc/$pid/exe" 2>/dev/null) || exe=0:0
	link=$(_e2e_node_root 10 readlink "/proc/$pid/exe" 2>/dev/null) || link=unavailable
	ticks=$(_e2e_node_root 10 python3 -I - "$pid" 2>/dev/null <<'PY'
import sys
try:
    text = open("/proc/" + sys.argv[1] + "/stat").read()
    print(text[text.rfind(") ") + 2:].split()[19])
except Exception:
    raise SystemExit(1)
PY
) || ticks=0
	printf '%s|%s|%s|%s|%s\n' "$inv" "$pid" "$exe" "$ticks" "$link"
}

_e2e_node_check_main_status() {
	local pid=$1 status
	status=$(_e2e_node_root 10 cat "/proc/$pid/status") || return 1
	printf '%s\n' "$status" >"$_e2e_node_evidence_dir/main-status.txt"
	python3 -I - "$_E2E_NODE_CAP_MASK" "$_e2e_node_evidence_dir/main-status.txt" <<'PY'
import sys
want = sys.argv[1]
with open(sys.argv[2], "rb") as f:
    lines = f.read().decode().splitlines()
st = {}
for line in lines:
    if ":" in line:
        k, v = line.split(":", 1)
        st[k] = v.strip()
assert st.get("Uid", "").split() == ["0", "0", "0", "0"]
assert st.get("Gid", "").split() == ["0", "0", "0", "0"]
assert st.get("NoNewPrivs") == "1"
for key in ["CapPrm", "CapEff", "CapBnd"]:
    assert st.get(key) == want, (key, st.get(key), want)
assert st.get("CapInh") == "0000000000000000"
assert st.get("CapAmb") == "0000000000000000"
PY
}

_e2e_node_main_proof() {
	local deadline=$1 staged sample
	staged=$(_e2e_node_root 10 stat -c %d:%i "$_e2e_node_binary_path") || return 1
	while true; do
		_e2e_node_now
		[ "$REPLY" -lt "$deadline" ] || break
		sample=$(_e2e_node_main_sample) || return 1
		local inv pid exe ticks link
		IFS='|' read -r inv pid exe ticks link <<<"$sample"
		[ "$inv" = "$_e2e_node_invocation" ] || { _e2e_node_error "main InvocationID changed to $inv"; return 1; }
		if [ "$exe" != "$staged" ] || [ "$ticks" = 0 ]; then
			_e2e_node_sleep || return 1
			continue
		fi
		_e2e_node_check_main_status "$pid" || { _e2e_node_error 'staged main process has a non-exact credential boundary'; return 1; }
		local again
		again=$(_e2e_node_main_sample) || return 1
		[ "$again" = "$sample" ] || { _e2e_node_error 'main process identity changed around credential proof'; return 1; }
		local cg
		cg=$(_e2e_node_root 10 cat "/proc/$pid/cgroup") || return 1
		[ "$cg" = "0::/system.slice/$_e2e_node_service.service/supervisor" ] || { _e2e_node_error "main cgroup is $cg"; return 1; }
		[ "$(_e2e_node_main_sample)" = "$sample" ] || { _e2e_node_error 'main process changed around cgroup proof'; return 1; }
		_e2e_node_now
		[ "$REPLY" -le "$deadline" ] || { _e2e_node_error 'main process proof finished after the readiness deadline'; return 1; }
		printf 'main=%s exe=%s start_ticks=%s\n' "$pid" "$link" "$ticks" >"$_e2e_node_evidence_dir/main-proof.txt"
		return 0
	done
	_e2e_node_diagnostics
	_e2e_node_error 'MainPID never reached the staged executable before deadline'
}

_e2e_node_check_boundary() {
	local show
	show=$(_e2e_node_show "$_e2e_node_service" \
		-p LoadState -p FragmentPath -p DropInPaths -p User -p Group -p DynamicUser \
		-p CapabilityBoundingSet -p AmbientCapabilities -p NoNewPrivileges -p SystemCallArchitectures \
		-p Delegate -p DelegateSubgroup -p DevicePolicy -p DeviceAllow -p ProtectSystem -p ProtectHome \
		-p PrivateTmp -p PrivateDevices -p ProtectControlGroups -p ProtectKernelTunables \
		-p ProtectKernelModules -p ProtectKernelLogs -p ProtectClock -p RestrictSUIDSGID \
		-p LockPersonality -p RestrictAddressFamilies -p RestrictNamespaces -p SystemCallFilter \
		-p ReadOnlyPaths -p ReadWritePaths -p KillMode -p UMask -p LimitCORE -p LimitNOFILE \
		-p TasksMax -p TimeoutStopUSec -p NetworkNamespacePath) || return 1
	printf '%s\n' "$show" >"$_e2e_node_evidence_dir/live-boundary.txt"
	python3 -I - "$_e2e_node_unit_path" "$_e2e_node_data_dir" "/sys/fs/cgroup/system.slice/$_e2e_node_service.service" "$_e2e_node_evidence_dir/live-boundary.txt" <<'PY'
import sys
unit, data, cgroup, evidence = sys.argv[1:]
with open(evidence, "rb") as f:
    lines_in = f.read().decode().splitlines()
props = {}
for line in lines_in:
    if "=" in line:
        k, v = line.split("=", 1)
        props[k] = props.get(k, "") + ("\n" if k in props else "") + v

def words(key):
    values = props.get(key, "").lower().split()
    assert len(values) == len(set(values)), key
    return set(values)

def lines(key):
    values = props.get(key, "").splitlines() if props.get(key) else []
    assert len(values) == len(set(values)), key
    return set(values)

exact = {"LoadState":"loaded", "FragmentPath":unit, "DropInPaths":"", "User":"", "Group":"",
 "DynamicUser":"no", "AmbientCapabilities":"", "NoNewPrivileges":"yes", "SystemCallArchitectures":"native",
 "Delegate":"yes", "DelegateSubgroup":"supervisor", "DevicePolicy":"closed", "ProtectSystem":"strict",
 "ProtectHome":"yes", "PrivateTmp":"yes", "PrivateDevices":"no", "ProtectControlGroups":"no",
 "ProtectKernelTunables":"yes", "ProtectKernelModules":"yes", "ProtectKernelLogs":"yes", "ProtectClock":"yes",
 "RestrictSUIDSGID":"yes", "LockPersonality":"yes", "RestrictNamespaces":"no", "KillMode":"process",
 "UMask":"0077", "LimitCORE":"0", "LimitNOFILE":"65536", "TasksMax":"infinity", "TimeoutStopUSec":"30s",
 "NetworkNamespacePath":""}
for key, want in exact.items():
    assert props.get(key, "") == want, (key, props.get(key), want)
assert words("CapabilityBoundingSet") == {x.lower() for x in ["CAP_SYS_ADMIN","CAP_SYS_CHROOT","CAP_MKNOD","CAP_SETUID","CAP_SETGID","CAP_SETPCAP","CAP_CHOWN","CAP_FOWNER","CAP_DAC_OVERRIDE","CAP_KILL","CAP_SYS_PTRACE","CAP_NET_ADMIN"]}
assert lines("DeviceAllow") == {"/dev/kvm rwm","/dev/vhost-vsock rw","/dev/vhost-net rw","/dev/net/tun rw","/dev/null rwm","/dev/urandom rwm","/dev/vsock r","char-rtc r"}
assert words("RestrictAddressFamilies") == {"af_unix","af_inet","af_inet6","af_netlink","af_vsock"}
assert words("ReadOnlyPaths") == {"/sys/fs/cgroup"}
assert words("ReadWritePaths") == {data, cgroup}
assert props.get("SystemCallFilter", "") in {"", "~"}
PY
}

_e2e_node_check_startup() {
	python3 -I - "$1" <<'PY'
import re, sys
with open(sys.argv[1], "rb") as f:
    lines = f.read().decode().splitlines()
phases = ["jail admitted", "cpu realized", "vm networking ready", "node: qemu-img self-check passed", "bbraft-node ready"]
seen = []
ready = None
qemu = None
for line in lines:
    m = re.search(r'(?:^| )msg=(?:"((?:[^"\\]|\\.)*)"|(\S+))', line)
    if not m:
        continue
    msg = m.group(1) or m.group(2)
    if msg.startswith("vm networking: QEMU user-mode"):
        raise SystemExit(1)
    if msg in phases:
        if msg == "cpu realized":
            if "jailed=true" not in line.split():
                raise SystemExit(1)
            q = re.search(r'(?:^| )qemu=(?:"([^"]+)"|(\S+))', line)
            if q:
                qemu = q.group(1) or q.group(2)
        seen.append(phases.index(msg))
    if msg == "bbraft-node ready":
        ready = line
if ready is None:
    raise SystemExit(2)
if seen != list(range(5)) or not qemu:
    raise SystemExit(1)
def number(name):
    m = re.search(r"(?:^| )" + name + r"=([0-9]+)(?: |$)", ready)
    if not m or int(m.group(1)) <= 0:
        raise SystemExit(1)
    return m.group(1)
print(number("vcpus"), number("memory_mib"), number("disk_gib"), qemu)
PY
}

_e2e_node_await_journal() {
	local deadline=$1 show journal rc caps line key value
	while true; do
		_e2e_node_now
		[ "$REPLY" -lt "$deadline" ] || break
		show=$(_e2e_node_show "$_e2e_node_service" -p InvocationID -p ActiveState -p NRestarts) || return 1
		local inv="" state="" restarts=""
		while IFS= read -r line; do
			key=${line%%=*}; value=${line#*=}
			case $key in InvocationID) inv=$value ;; ActiveState) state=$value ;; NRestarts) restarts=$value ;; esac
		done <<<"$show"
		[ "$inv" = "$_e2e_node_invocation" ] || { _e2e_node_error "unit restarted to InvocationID $inv"; return 1; }
		[ "$restarts" = 0 ] || { _e2e_node_error "unit NRestarts=$restarts"; return 1; }
		case $state in failed|inactive|deactivating) _e2e_node_diagnostics; _e2e_node_error "unit became $state"; return 1 ;; esac
		journal=$(_e2e_node_root 30 journalctl --no-pager -o cat -n 1000 "_SYSTEMD_INVOCATION_ID=$_e2e_node_invocation") || { _e2e_node_sleep; continue; }
		printf '%s\n' "$journal" >"$_e2e_node_evidence_dir/journal-$_e2e_node_invocation.log"
		if caps=$(_e2e_node_check_startup "$_e2e_node_evidence_dir/journal-$_e2e_node_invocation.log"); then
			show=$(_e2e_node_show "$_e2e_node_service" -p InvocationID -p NRestarts) || return 1
			printf '%s\n' "$show" | python3 -I -c 'import sys; p=dict(x.split("=",1) for x in sys.stdin.read().splitlines() if "=" in x); assert p.get("InvocationID")==sys.argv[1] and p.get("NRestarts")=="0"' "$_e2e_node_invocation" || { _e2e_node_error 'invocation changed after ready journal'; return 1; }
			read -r E2E_NODE_EXPECTED_VCPUS E2E_NODE_EXPECTED_MEMORY_MIB E2E_NODE_EXPECTED_DISK_GIB E2E_NODE_ADMITTED_QEMU_VERSION <<<"$caps"
			_e2e_node_now
			[ "$REPLY" -le "$deadline" ] || { _e2e_node_error 'ready journal completed after the readiness deadline'; return 1; }
			export E2E_NODE_EXPECTED_VCPUS E2E_NODE_EXPECTED_MEMORY_MIB E2E_NODE_EXPECTED_DISK_GIB E2E_NODE_ADMITTED_QEMU_VERSION
			export E2E_NODE_READY_EPOCH=$REPLY
			_e2e_node_ready=1
			return 0
		else
			rc=$?
			[ "$rc" -eq 2 ] || { _e2e_node_error 'startup log phases are not the exact jailed production sequence'; return 1; }
		fi
		_e2e_node_sleep || return 1
	done
	_e2e_node_diagnostics
	_e2e_node_error 'node not ready within the shared readiness deadline'
}

_e2e_node_require_drained() {
	[ -n "${_e2e_node_data_dir:-}" ] || return 1
	_e2e_node_root 30 python3 -I - "$_e2e_node_data_dir" <<'PY'
import os, sys
root = sys.argv[1]
for name in ["vms", "net", "jailer/records", "jailer/admission", "toolcheck", "orphans"]:
    path = os.path.join(root, name)
    try:
        st = os.lstat(path)
    except FileNotFoundError:
        continue
    if not os.path.isdir(path) or os.path.islink(path):
        raise SystemExit("not a real directory: " + path)
    entries = os.listdir(path)
    if len(entries) > 1024:
        raise SystemExit("too many entries: " + path)
    if entries:
        raise SystemExit("undrained " + path + ": " + ", ".join(sorted(entries)[:16]))
PY
}

_e2e_node_nft_snapshot() {
	local label=$1
	local raw=$_e2e_node_evidence_dir/nft-$label-ruleset.json
	_e2e_node_root 30 nft -j -a list ruleset >"$raw" || return 1
	python3 -I - "$raw" "$_e2e_node_evidence_dir" "$label" <<'PY'
import json, os, sys
raw, evidence, label = sys.argv[1:]
with open(raw, "rb") as f:
    doc = json.load(f)
items = doc.get("nftables", [])
assert len(items) <= 10000

def clean(value):
    if isinstance(value, dict):
        return {k: clean(v) for k, v in value.items() if k != "handle"}
    if isinstance(value, list):
        return [clean(v) for v in value]
    return value

for family, table, empty_set in [("inet", "bbraft_net", "vm_addrs"), ("bridge", "bbraft_bridge", "vm_ports")]:
    selected = []
    handle = 0
    for item in items:
        if "metainfo" in item:
            continue
        kind, body = next(iter(item.items()))
        if kind == "table":
            if body.get("family") == family and body.get("name") == table:
                assert isinstance(body.get("handle"), int) and body["handle"] > 0
                handle = body["handle"]
                selected.append(item)
            continue
        if body.get("family") != family or body.get("table") != table:
            continue
        # Anything living in an owned table must be a kind this snapshot
        # captures whole; a foreign map/flowtable/stateful object or element
        # cannot hide outside the evidence that authorizes table deletion.
        assert kind in {"chain", "set", "rule"}, "unknown object in owned table: " + kind
        selected.append(item)
    if selected:
        assert handle, "table without current handle"
        normalized = {"nftables": clean(selected)}
        out = os.path.join(evidence, "nft-" + label + "-" + family + ".json")
        with open(out, "w") as f:
            json.dump(normalized, f, sort_keys=True, separators=(",", ":"))
            f.write("\n")
        sets = [x["set"] for x in selected if "set" in x]
        target = [s for s in sets if s.get("name") == empty_set]
        assert len(target) == 1, "allocation set is missing or duplicated"
        # nft >= 1.0 writes set members as "elem" (verified against a real
        # 1.0.9 ruleset); older spellings used "elements". Either name must
        # be absent or empty before the set counts as drained.
        assert not target[0].get("elem") and not target[0].get("elements"), "allocation set is not empty"
    else:
        assert not handle
    print(family + "=" + str(handle))
PY
}

_e2e_node_capture_nft_empty() {
	_e2e_node_require_drained || { _e2e_node_error 'not empty before nft baseline'; return 1; }
	local result
	result=$(_e2e_node_nft_snapshot empty) || return 1
	printf '%s\n' "$result" | python3 -I -c 'import sys; p=dict(x.strip().split("=",1) for x in sys.stdin if "=" in x); assert int(p.get("inet","0"))>0 and int(p.get("bridge","0"))>0' || { _e2e_node_error 'ready node lacks its two owned nft tables'; return 1; }
}

e2e_node_wait_ready() {
	[ "${_e2e_node_started:-0}" = 1 ] || { _e2e_node_error 'node has not been started'; return 1; }
	local timeout deadline
	timeout=$(_e2e_node_timeout "${1:-${E2E_NODE_READY_TIMEOUT:-}}" 300 E2E_NODE_READY_TIMEOUT) || return 1
	_e2e_node_now
	deadline=$((REPLY + timeout))
	_e2e_node_main_proof "$deadline" || return 1
	_e2e_node_check_boundary || { _e2e_node_error 'live systemd boundary differs from the exact role boundary'; return 1; }
	_e2e_node_await_journal "$deadline" || return 1
	_e2e_node_capture_nft_empty || { _e2e_node_error 'cannot capture proven empty node nft tables'; return 1; }
	_e2e_node_assert_invocation || return 1
	_e2e_node_now
	[ "$REPLY" -le "$deadline" ] || { _e2e_node_error 'readiness evidence completed after the shared deadline'; return 1; }
	printf 'node ready: id=%s image=%s vcpus=%s memory_mib=%s disk_gib=%s qemu=%s\n' "$E2E_NODE_ID" "$E2E_NODE_IMAGE_ID" "$E2E_NODE_EXPECTED_VCPUS" "$E2E_NODE_EXPECTED_MEMORY_MIB" "$E2E_NODE_EXPECTED_DISK_GIB" "$E2E_NODE_ADMITTED_QEMU_VERSION"
}

_e2e_node_assert_invocation() {
	local show
	show=$(_e2e_node_show "$_e2e_node_service" -p InvocationID -p ActiveState -p NRestarts) || return 1
	printf '%s\n' "$show" | python3 -I -c 'import sys; p=dict(x.split("=",1) for x in sys.stdin.read().splitlines() if "=" in x); assert p.get("InvocationID")==sys.argv[1] and p.get("ActiveState")=="active" and p.get("NRestarts")=="0"' "$_e2e_node_invocation" || { _e2e_node_error 'node invocation/state changed'; return 1; }
}

e2e_node_wait_registered() {
	[ "${_e2e_node_ready:-0}" = 1 ] || { _e2e_node_error 'wait for local ready before registered'; return 1; }
	[ "$#" -gt 0 ] || { _e2e_node_error 'registration callback required'; return 1; }
	local timeout deadline rc
	timeout=$(_e2e_node_timeout "${E2E_NODE_REGISTERED_TIMEOUT:-}" 120 E2E_NODE_REGISTERED_TIMEOUT) || return 1
	_e2e_node_now
	deadline=$((REPLY + timeout))
	while true; do
		_e2e_node_now
		[ "$REPLY" -lt "$deadline" ] || break
		_e2e_node_assert_invocation || return 1
		if "$@"; then
			_e2e_node_assert_invocation || return 1
			_e2e_node_now
			[ "$REPLY" -le "$deadline" ] || { _e2e_node_error 'registration callback succeeded after the total deadline'; return 1; }
			printf 'node registered: %s\n' "$E2E_NODE_ID"
			return 0
		else
			rc=$?
		fi
		_e2e_node_sleep || return 1
	done
	_e2e_node_error "server-side registration was not proven within ${timeout}s (last callback status ${rc:-unknown})"
}

_e2e_node_cgroup_empty() {
	_e2e_node_root 30 python3 -I - "$_e2e_node_cgroup_dir" "${_e2e_node_cgroup_owned:-0}" "${_e2e_node_cgroup_ino:-}" <<'PY'
import os, stat, sys
root, owned, expected = sys.argv[1:]
try:
    root_st = os.lstat(root)
except FileNotFoundError:
    raise SystemExit(0)
assert owned == "1" and expected.isdecimal() and root_st.st_ino == int(expected), "cgroup was never pinned by this attempt or changed"
assert stat.S_ISDIR(root_st.st_mode) and not stat.S_ISLNK(root_st.st_mode)
queue = [root]
seen = 0
while queue:
    path = queue.pop()
    seen += 1
    assert seen <= 256, "cgroup bound exceeded"
    st = os.lstat(path)
    assert stat.S_ISDIR(st.st_mode) and not stat.S_ISLNK(st.st_mode), path
    with open(path + "/cgroup.events", "rb") as f:
        events = f.read(65537)
    assert len(events) <= 65536, path
    lines = [x for x in events.decode().splitlines() if x.startswith("populated")]
    assert lines == ["populated 0"], (path, lines)
    with open(path + "/cgroup.procs", "rb") as f:
        procs = f.read(65537)
    assert len(procs) <= 65536 and not procs.split(), path
    for entry in os.scandir(path):
        if entry.is_dir(follow_symlinks=False):
            queue.append(entry.path)
        elif entry.is_symlink():
            raise AssertionError("cgroup symlink: " + entry.path)
PY
}

_e2e_node_uid_free() {
	local procs
	procs=$(_e2e_node_root 30 ps -eo uid=,pid=,comm=) || return 1
	printf '%s\n' "$procs" >"$_e2e_node_evidence_dir/processes.txt"
	python3 -I - "$_e2e_node_evidence_dir/processes.txt" <<'PY'
import sys
bad = []
with open(sys.argv[1], "rb") as f:
    for raw in f:
        line = raw.decode()
        fields = line.split()
        if fields and fields[0].isdecimal() and 900000 <= int(fields[0]) <= 900999:
            bad.append(line.strip())
assert not bad, "jail UID processes: " + "; ".join(bad[:16])
PY
}

_e2e_node_network_validate_empty() {
	local links result family handle
	links=$(_e2e_node_root 30 ip -d -j link show) || return 1
	printf '%s\n' "$links" >"$_e2e_node_evidence_dir/network-current-links.json"
	result=$(python3 -I - "$_e2e_node_evidence_dir/network-current-links.json" "$_e2e_node_attempt" "${_e2e_node_bridge_index:-}" "${_e2e_node_host_if_index:-}" "$_e2e_node_bridge" "$_e2e_node_host_if" <<'PY'
import json, sys
path, attempt, want_bridge, want_host, bridge_name, host_name = sys.argv[1:]
with open(path, "rb") as f:
    links = json.load(f)
assert not [x for x in links if x.get("linkinfo", {}).get("info_kind") == "tun"], "TAP links remain"
def one(name):
    found = [x for x in links if x.get("ifname") == name]
    assert len(found) <= 1
    return found[0] if found else None
bridge = one(bridge_name)
host = one(host_name)
if bridge:
    assert want_bridge and bridge.get("ifindex") == int(want_bridge), "bridge ifindex changed"
    assert bridge.get("linkinfo", {}).get("info_kind") == "bridge"
    assert bridge.get("ifalias") == "bbraft-e2e-" + attempt, "bridge alias changed"
if host:
    assert want_host and host.get("ifindex") == int(want_host), "host veth ifindex changed"
print(bridge.get("ifindex") if bridge else 0, host.get("ifindex") if host else 0)
PY
) || { _e2e_node_error 'host link inventory is not empty/owned'; return 1; }
	read -r _e2e_node_bridge_present _e2e_node_host_present <<<"$result"
	result=$(_e2e_node_nft_snapshot current) || return 1
	_e2e_node_nft_inet_handle=0 _e2e_node_nft_bridge_handle=0
	local line
	while IFS= read -r line; do
		case $line in inet=*) _e2e_node_nft_inet_handle=${line#inet=} ;; bridge=*) _e2e_node_nft_bridge_handle=${line#bridge=} ;; *) return 1 ;; esac
	done <<<"$result"
	for family in inet bridge; do
		if [ "$family" = inet ]; then handle=$_e2e_node_nft_inet_handle; else handle=$_e2e_node_nft_bridge_handle; fi
		if [ "$handle" != 0 ]; then
			[ -f "$_e2e_node_evidence_dir/nft-empty-$family.json" ] || { _e2e_node_error 'nft table exists without a captured empty baseline'; return 1; }
			python3 -I - "$_e2e_node_evidence_dir/nft-empty-$family.json" "$_e2e_node_evidence_dir/nft-current-$family.json" <<'PY' || { _e2e_node_error 'nft table differs from semantic empty baseline'; return 1; }
import json, sys
with open(sys.argv[1], "rb") as f:
    expected = json.load(f)
with open(sys.argv[2], "rb") as f:
    current = json.load(f)
assert current == expected
PY
		fi
	done
	_e2e_node_netns_present=0
	if [ -e "/run/netns/$_e2e_node_client_netns" ] || [ -L "/run/netns/$_e2e_node_client_netns" ]; then
		local ino pids client_links
		ino=$(_e2e_node_root 10 stat -c %i "/run/netns/$_e2e_node_client_netns") || return 1
		[ "$ino" = "${_e2e_node_netns_ino:-}" ] || { _e2e_node_error 'client netns inode changed'; return 1; }
		_e2e_node_netns_present=1
		pids=$(_e2e_node_root 10 ip netns pids "$_e2e_node_client_netns") || return 1
		[ -z "$pids" ] || { _e2e_node_error 'client netns still has processes'; return 1; }
		client_links=$(_e2e_node_root 10 ip netns exec "$_e2e_node_client_netns" ip -j link show) || return 1
		printf '%s\n' "$client_links" >"$_e2e_node_evidence_dir/client-current-links.json"
		python3 -I - "$_e2e_node_evidence_dir/client-current-links.json" "${_e2e_node_client_if_index:-}" "$_e2e_node_client_if" <<'PY' || { _e2e_node_error 'client veth identity changed'; return 1; }
import json, sys
with open(sys.argv[1], "rb") as f:
    links = json.load(f)
found = [x for x in links if x.get("ifname") == sys.argv[3]]
assert len(found) <= 1
if found:
    assert sys.argv[2] and found[0].get("ifindex") == int(sys.argv[2])
PY
	fi
}

_e2e_node_fs_validate() {
	_e2e_node_root 60 python3 -I - "$_e2e_node_stage_dir" "$_e2e_node_config_dir" "$_e2e_node_data_dir" "$_e2e_node_unit_path" >"$_e2e_node_evidence_dir/filesystem-inventory.txt" <<'PY'
import json, os, stat, sys
stage, config, data, unit = sys.argv[1:]
log = stage + "/state/ownership.ndjson"
if not os.path.lexists(stage):
    assert not any(os.path.lexists(p) for p in [config, data, unit]), "owned root exists without state"
    raise SystemExit(0)
fd = os.open(log, os.O_RDONLY | os.O_NOFOLLOW)
with os.fdopen(fd, "rb") as f:
    raw = f.read(4 * 1024 * 1024 + 1)
assert len(raw) <= 4 * 1024 * 1024, "ownership log too large"
records = [json.loads(line) for line in raw.splitlines()]
by_path = {r["path"]: r for r in records}
assert records and len(by_path) == len(records) and len(records) <= 10000
# Every owned root that still exists must carry a complete record; an
# unrecorded root is preserved untouched instead of silently leaked.
assert stage in by_path, "authority root is unrecorded"
for root_path in (config, data, unit):
    assert not os.path.lexists(root_path) or root_path in by_path, "unrecorded owned root: " + root_path
objects = []

def inventory(path, recorded, depth=0):
    assert depth <= 64, "tree too deep"
    st = os.lstat(path)
    if recorded:
        assert (st.st_dev, st.st_ino, stat.S_IMODE(st.st_mode), st.st_uid, st.st_gid) == (recorded["dev"], recorded["ino"], recorded["mode"], recorded["uid"], recorded["gid"]), "changed owned object: " + path
    assert not stat.S_ISLNK(st.st_mode), "symlink preserved: " + path
    assert stat.S_ISDIR(st.st_mode) or stat.S_ISREG(st.st_mode), "special file preserved: " + path
    objects.append(path)
    assert len(objects) <= 10000, "tree entry bound exceeded"
    print(st.st_dev, st.st_ino, oct(stat.S_IMODE(st.st_mode)), st.st_uid, st.st_gid, path)
    if stat.S_ISDIR(st.st_mode):
        for entry in os.scandir(path):
            cst = entry.stat(follow_symlinks=False)
            assert cst.st_dev == st.st_dev, "mount/filesystem boundary: " + entry.path
            inventory(entry.path, by_path.get(entry.path), depth + 1)

for path, record in by_path.items():
    assert path == unit or path == stage or path.startswith(stage + "/") or path == config or path.startswith(config + "/") or path == data or path.startswith(data + "/")
done = set()
for path in sorted(by_path, key=lambda p: p.count("/")):
    if any(path == p or path.startswith(p + "/") for p in done):
        continue
    try:
        os.lstat(path)
    except FileNotFoundError:
        continue
    inventory(path, by_path[path])
    done.add(path)
PY
}

_e2e_node_remove_unit() {
	_e2e_node_root 30 python3 -I - "$_e2e_node_stage_dir" "$_e2e_node_unit_path" <<'PY'
import json, os, sys
stage, unit = sys.argv[1:]
log = stage + "/state/ownership.ndjson"
try:
    with open(log, "rb") as f:
        records = [json.loads(x) for x in f if x.strip()]
except FileNotFoundError:
    assert not os.path.lexists(unit), "unit exists without attempt ownership state"
    raise SystemExit(0)
record = next((r for r in records if r["path"] == unit), None)
try:
    st = os.lstat(unit)
except FileNotFoundError:
    raise SystemExit(0)
assert record and not record["dir"] and (st.st_dev, st.st_ino) == (record["dev"], record["ino"])
os.unlink(unit)
PY
}

_e2e_node_remove_files() {
	_e2e_node_root 120 python3 -I - "$_e2e_node_stage_dir" "$_e2e_node_config_dir" "$_e2e_node_data_dir" "$_e2e_node_unit_path" <<'PY'
import json, os, stat, sys
stage, config, data, unit = sys.argv[1:]
log = stage + "/state/ownership.ndjson"
if not os.path.lexists(stage):
    assert not any(os.path.lexists(p) for p in [config, data, unit]), "owned root exists without state"
    raise SystemExit(0)
fd = os.open(log, os.O_RDONLY | os.O_NOFOLLOW)
with os.fdopen(fd, "rb") as f:
    raw = f.read(4 * 1024 * 1024 + 1)
assert len(raw) <= 4 * 1024 * 1024
records = [json.loads(line) for line in raw.splitlines()]
by_path = {r["path"]: r for r in records}
assert records and len(by_path) == len(records) and len(records) <= 10000
# Deletion authority is complete only when every owned root that still
# exists is recorded; anything else is preserved for manual resolution.
assert stage in by_path, "authority root is unrecorded"
for root_path in (config, data, unit):
    assert not os.path.lexists(root_path) or root_path in by_path, "unrecorded owned root: " + root_path
objects = []

def inventory(path, recorded, depth=0):
    assert depth <= 64, "tree too deep"
    st = os.lstat(path)
    if recorded:
        assert (st.st_dev, st.st_ino, stat.S_IMODE(st.st_mode), st.st_uid, st.st_gid) == (recorded["dev"], recorded["ino"], recorded["mode"], recorded["uid"], recorded["gid"]), path
    assert not stat.S_ISLNK(st.st_mode), "symlink preserved: " + path
    assert stat.S_ISDIR(st.st_mode) or stat.S_ISREG(st.st_mode), "special file preserved: " + path
    objects.append((path, st.st_dev, st.st_ino, stat.S_ISDIR(st.st_mode)))
    assert len(objects) <= 10000, "tree entry bound exceeded"
    if stat.S_ISDIR(st.st_mode):
        for entry in os.scandir(path):
            child = entry.path
            cst = entry.stat(follow_symlinks=False)
            assert cst.st_dev == st.st_dev, "mount/filesystem boundary: " + child
            inventory(child, by_path.get(child), depth + 1)

for path, record in by_path.items():
    assert path == unit or path == stage or path.startswith(stage + "/") or path == config or path.startswith(config + "/") or path == data or path.startswith(data + "/")
    try:
        os.lstat(path)
    except FileNotFoundError:
        continue
# Inventory each still-present recorded root once, including node-created contents.
done = set()
for path in sorted(by_path, key=lambda p: p.count("/")):
    if any(path == p or path.startswith(p + "/") for p in done):
        continue
    try:
        os.lstat(path)
    except FileNotFoundError:
        continue
    inventory(path, by_path[path])
    done.add(path)
# Remove every non-stage object first, then the rest of the stage subtree.
# The exact authority chain (ownership log, its state dir, the stage root)
# goes last, so every other removal, including the staged binary, has
# already succeeded before the retry authority disappears.
authority = {stage + "/state/ownership.ndjson", stage + "/state", stage}
objects.sort(key=lambda x: (x[0] in authority, x[0] == stage or x[0].startswith(stage + "/"), -x[0].count("/"), -len(x[0]), x[0]))
for path, dev, ino, is_dir in objects:
    try:
        st = os.lstat(path)
    except FileNotFoundError:
        continue
    assert (st.st_dev, st.st_ino) == (dev, ino), "identity changed before removal: " + path
    if is_dir:
        os.rmdir(path)
    else:
        os.unlink(path)
PY
}

_e2e_node_remove_cgroup() {
	_e2e_node_cgroup_empty || return 1
	_e2e_node_root 30 python3 -I - "$_e2e_node_cgroup_dir" "${_e2e_node_cgroup_owned:-0}" "${_e2e_node_cgroup_ino:-}" <<'PY'
import os, sys
root, owned, expected = sys.argv[1:]
try:
    st = os.lstat(root)
except FileNotFoundError:
    raise SystemExit(0)
assert owned == "1" and expected.isdecimal() and st.st_ino == int(expected), "refusing an unpinned/replaced cgroup"
paths = []
for base, dirs, files in os.walk(root, topdown=False, followlinks=False):
    assert len(paths) <= 256
    paths.append(base)
assert os.lstat(root).st_ino == int(expected)
for path in paths:
    os.rmdir(path)
PY
}

_e2e_node_delete_network() {
	local family handle
	for family in bridge inet; do
		if [ "$family" = inet ]; then handle=${_e2e_node_nft_inet_handle:-0}; else handle=${_e2e_node_nft_bridge_handle:-0}; fi
		if [ "$handle" != 0 ] && [ -n "$handle" ]; then
			_e2e_node_root 30 nft delete table "$family" handle "$handle" || { _e2e_node_error "delete owned $family nft handle failed"; return 1; }
		fi
	done
	if [ "${_e2e_node_host_present:-0}" != 0 ]; then
		local index
		_e2e_node_pin_link '' "$_e2e_node_host_if" index || return 1
		[ "$index" = "$_e2e_node_host_if_index" ] || { _e2e_node_error 'host veth replaced before delete'; return 1; }
		_e2e_node_root 30 ip link delete dev "$_e2e_node_host_if" || return 1
	fi
	if [ "${_e2e_node_netns_present:-0}" != 0 ]; then
		local ino pids
		ino=$(_e2e_node_root 10 stat -c %i "/run/netns/$_e2e_node_client_netns") || return 1
		[ "$ino" = "$_e2e_node_netns_ino" ] || { _e2e_node_error 'netns replaced before delete'; return 1; }
		pids=$(_e2e_node_root 10 ip netns pids "$_e2e_node_client_netns") || return 1
		[ -z "$pids" ] || { _e2e_node_error 'netns gained processes'; return 1; }
		_e2e_node_root 30 ip netns del "$_e2e_node_client_netns" || return 1
	fi
	if [ "${_e2e_node_bridge_present:-0}" != 0 ]; then
		local index
		_e2e_node_pin_link '' "$_e2e_node_bridge" index || return 1
		[ "$index" = "$_e2e_node_bridge_index" ] || { _e2e_node_error 'bridge replaced before delete'; return 1; }
		_e2e_node_root 30 ip link delete dev "$_e2e_node_bridge" || return 1
	fi
}

_e2e_node_pre_stop_proof() {
	local show
	show=$(_e2e_node_show "$_e2e_node_service" -p InvocationID -p NRestarts) || return 1
	printf '%s\n' "$show" >"$_e2e_node_evidence_dir/pre-stop-show.txt"
	python3 -I -c 'import sys; p=dict(x.split("=",1) for x in sys.stdin.read().splitlines() if "=" in x); assert p.get("InvocationID")==sys.argv[1] and p.get("NRestarts")=="0"' "$_e2e_node_invocation" <<<"$show" || { _e2e_node_error 'unit invocation or restart count changed before stop; nothing stopped'; return 1; }
	_e2e_node_root 30 python3 -I - "$_e2e_node_stage_dir" "$_e2e_node_unit_path" "$_e2e_node_cgroup_dir" "${_e2e_node_cgroup_owned:-0}" "${_e2e_node_cgroup_ino:-}" <<'PY' || { _e2e_node_error 'owned unit or cgroup identity changed before stop; nothing stopped'; return 1; }
import json, os, sys
stage, unit, cgroup, owned, cgroup_ino = sys.argv[1:]
with open(stage + "/state/ownership.ndjson", "rb") as f:
    records = [json.loads(x) for x in f if x.strip()]
record = next((r for r in records if r["path"] == unit), None)
st = os.lstat(unit)
assert record and not record["dir"] and (st.st_dev, st.st_ino) == (record["dev"], record["ino"]), "owned unit inode changed"
try:
    cg = os.lstat(cgroup)
except FileNotFoundError:
    raise SystemExit(0)
assert owned == "1" and cgroup_ino.isdecimal() and cg.st_ino == int(cgroup_ino), "service cgroup is not the pinned one"
PY
}

e2e_node_stop() {
	[ -n "${_e2e_node_attempt:-}" ] || { _e2e_node_error 'no node attempt to stop'; return 1; }
	[ "${_e2e_node_preflight_failed:-0}" = 0 ] || { _e2e_node_error 'preflight collision: stop stays read-only for every pre-existing object'; return 1; }
	_e2e_node_require_drained || { _e2e_node_error 'node left running: delete VMs/held records through the API first'; return 1; }
	# A retry keeps an already-proven stop: the unit may have been removed
	# since, and every remaining object is still revalidated below.
	if [ "${_e2e_node_started:-0}" = 1 ] && [ "${_e2e_node_stop_proven:-0}" != 1 ]; then
		# Prove the attempt's own unit before stopping it: after a successful
		# stop systemd clears InvocationID, so identity cannot be proven later.
		_e2e_node_pre_stop_proof || return 1
		_e2e_node_root "${E2E_NODE_STOP_TIMEOUT:-120}" systemctl stop "$_e2e_node_service.service" || { _e2e_node_diagnostics; _e2e_node_error 'systemd stop failed; all state preserved'; return 1; }
		local show
		show=$(_e2e_node_show "$_e2e_node_service" -p ActiveState -p MainPID -p InvocationID -p NRestarts) || return 1
		printf '%s\n' "$show" >"$_e2e_node_evidence_dir/stop-show.txt"
		python3 -I - "$_e2e_node_invocation" "$_e2e_node_evidence_dir/stop-show.txt" <<'PY' || { _e2e_node_error 'unit stop is not proven'; return 1; }
import sys
with open(sys.argv[2], "rb") as f:
    props = dict(x.split("=", 1) for x in f.read().decode().splitlines() if "=" in x)
assert props.get("ActiveState") in {"inactive", "failed"}
assert props.get("MainPID") == "0"
# systemd clears InvocationID once the unit has stopped; any other value than
# the proven original means a different invocation.
assert props.get("InvocationID") in {"", sys.argv[1]}
assert props.get("NRestarts") == "0"
PY
		_e2e_node_stop_proven=1
	fi
	_e2e_node_require_drained || { _e2e_node_error 'records appeared during stop; preserving'; return 1; }
	_e2e_node_cgroup_empty || { _e2e_node_error 'unit cgroup subtree is not empty; preserving'; return 1; }
	_e2e_node_uid_free || { _e2e_node_error 'jail UID processes remain; preserving'; return 1; }
	_e2e_node_network_validate_empty || { _e2e_node_error 'network is not proven empty/owned; preserving'; return 1; }
	_e2e_node_fs_validate || { _e2e_node_error 'owned filesystem inventory is not intact; preserving'; return 1; }
	# Nothing below runs until every check above has succeeded completely.
	_e2e_node_delete_network || { _e2e_node_error 'network deletion failed; retry e2e_node_stop'; return 1; }
	_e2e_node_remove_cgroup || { _e2e_node_error 'cgroup deletion failed; retry e2e_node_stop'; return 1; }
	_e2e_node_remove_unit || { _e2e_node_error 'owned unit removal failed'; return 1; }
	if [ "${_e2e_node_unit_owned:-0}" = 1 ]; then
		_e2e_node_root 60 systemctl daemon-reload || { _e2e_node_error 'daemon-reload after unit removal failed'; return 1; }
		local load
		if ! _e2e_node_root 30 systemctl reset-failed "$_e2e_node_service.service"; then
			# reset-failed can legitimately fail for an already-unloaded
			# unit; accepting that requires proven absence, never an
			# ignored error.
			load=$(_e2e_node_show "$_e2e_node_service" -p LoadState) || { _e2e_node_error 'cannot prove unit absence after reset-failed'; return 1; }
			[ "$load" = 'LoadState=not-found' ] || { _e2e_node_error 'reset-failed after unit removal failed'; return 1; }
		fi
		_e2e_node_unit_owned=0
	fi
	_e2e_node_remove_files || { _e2e_node_error 'owned file removal failed; remaining state preserved for retry'; return 1; }
	_e2e_node_stopped=1
	e2e_node_leak_proof || return 1
	printf 'node stopped and owned resources removed: %s\n' "$_e2e_node_attempt"
}

e2e_node_leak_proof() {
	[ -n "${_e2e_node_attempt:-}" ] || { _e2e_node_error 'no node attempt to inspect'; return 1; }
	local show
	show=$(_e2e_node_show "$_e2e_node_service" -p LoadState) || return 1
	[ "$show" = 'LoadState=not-found' ] || { _e2e_node_error "unit remains: $show"; return 1; }
	_e2e_node_uid_free || { _e2e_node_error 'jail UID process leak'; return 1; }
	_e2e_node_root 30 python3 -I - "$_e2e_node_service" "$_e2e_node_unit_path" "$_e2e_node_data_dir" "$_e2e_node_config_dir" "$_e2e_node_stage_dir" "$_e2e_node_client_netns" "$_e2e_node_bridge" "$_e2e_node_host_if" <<'PY'
import json, os, subprocess, sys
service, unit, data, config, stage, netns, bridge, host = sys.argv[1:]
paths = [unit, unit + ".d", data, config, stage, "/run/netns/" + netns,
         "/etc/systemd/system/" + service + ".service",
         "/etc/systemd/system/" + service + ".service.d",
         "/etc/systemd/system/multi-user.target.wants/" + service + ".service",
         "/run/systemd/system/multi-user.target.wants/" + service + ".service",
         "/sys/fs/cgroup/system.slice/" + service + ".service"]
for path in paths:
    assert not os.path.lexists(path), "leak: " + path
links = json.loads(subprocess.check_output(["ip", "-j", "link", "show"]))
assert not {bridge, host}.intersection(x.get("ifname") for x in links), "owned link leak"
taps = json.loads(subprocess.check_output(["ip", "-j", "link", "show", "type", "tun"]))
assert not taps, "TAP leak"
rules = json.loads(subprocess.check_output(["nft", "-j", "list", "ruleset"]))
for item in rules.get("nftables", []):
    table = item.get("table")
    assert not table or (table.get("family"), table.get("name")) not in {("inet", "bbraft_net"), ("bridge", "bbraft_bridge")}, "nft leak"
PY
}

_e2e_node_diagnostics() {
	[ -n "${_e2e_node_evidence_dir:-}" ] || return 0
	if [ -n "${_e2e_node_service:-}" ]; then
		_e2e_node_root 30 systemctl status "$_e2e_node_service.service" --no-pager -n 40 >"$_e2e_node_evidence_dir/status.txt" 2>&1 || true
		_e2e_node_show "$_e2e_node_service" >"$_e2e_node_evidence_dir/show.txt" 2>&1 || true
		if [ -n "${_e2e_node_invocation:-}" ]; then
			_e2e_node_root 30 journalctl --no-pager -o cat -n 200 "_SYSTEMD_INVOCATION_ID=$_e2e_node_invocation" >"$_e2e_node_evidence_dir/journal-tail.log" 2>&1 || true
		fi
	fi
	_e2e_node_root 30 ip -details link show >"$_e2e_node_evidence_dir/links.txt" 2>&1 || true
	_e2e_node_root 30 nft -a list ruleset >"$_e2e_node_evidence_dir/nft.txt" 2>&1 || true
	printf 'node diagnostics: %s (secrets are never included)\n' "$_e2e_node_evidence_dir" >&2
}
