#!/usr/bin/env bash
# bamboo-raft two-node end-to-end for the object-store phase. Run on one large
# throwaway KVM VM (/dev/kvm, /dev/vhost-vsock, sudo -n, Postgres, QEMU, the
# pinned garage binary in PATH, openssh-client):
#
#   BBRAFT_TEST_DATABASE_URL=postgres://... bash deploy/dev/e2e-p2.sh
#
# It starts Garage (single node, bucket and key created here), a fresh
# Postgres database, bbraft-server -roles all with the object store and short
# lifecycle intervals, and TWO real nodes, then proves with the real CLI and
# API that sandboxes survive node loss and move between nodes:
#
#   node a   the production path of e2e-p1.sh: root supervisor under the unit
#            the bbraft_node role renders, jailed QEMU, TAP networking, SSH
#            ingress. Offers ONE running small sandbox (2 vCPU, 4 GiB) and two
#            small disks (110 GiB), so capacity decides every placement below.
#   node b   a plain process of the calling user in -node.network-mode=user
#            (QEMU unjailed, no TAP). Offers ten small sandboxes' worth
#            (20 vCPU, 40960 MiB, 510 GiB): "count 10" puts nine of them on b.
#            It is the node this script kills with SIGKILL and restarts.
#
# Node b is sized from the host at run time (nproc, MemTotal, free disk, minus
# node a's offer and a memory reserve for the host). The node refuses an
# explicit capacity above the detected host times its overcommit factor, so b
# gets its own -node.cpu-overcommit, -node.memory-overcommit and
# -node.disk-overcommit, computed so that the offer fits with a 25% margin and
# printed by the preflight (disk factors carry a 100% margin, since free space
# shrinks as guests write). If even the ceilings (cpu 8, memory 8, disk 20)
# cannot fit ten small sandboxes across both nodes, the preflight fails with
# the numbers. Overcommit is fine here: the guests idle, a small idle guest
# touches a few hundred MiB of the 4 GiB it reserves, and the disks are thin.
# Node a keeps its offer (110 GiB: one running small sandbox and two small
# disks) and gets only a disk overcommit, through the role variable
# bbraft_node_disk_overcommit, computed from the free space under /var/lib;
# its cpu and memory keep the production defaults. Offers are promises: what
# must be real is free space, so the preflight refuses a host with less than
# 30 GiB actually free under either data directory (one shared filesystem is
# counted once) and prints the numbers.
#
# Why node b is user-mode: two real tap nodes on one host collide on the
# nft tables (bbraft_net, bbraft_bridge), the bridge, the guest subnet and the
# SSH ingress, and per-node network naming is a later item. Node b therefore
# has no guest networking: it checks data only. Guest networking is checked on
# node a (ssh through the SSH ingress and an address inside the node a guest
# prefix, 10.88.0.0/16, exported by the node driver) every time a sandbox
# lands there. The preflight refuses a host that holds an address inside that
# prefix (a guest would be handed the host's own address). Both nodes still share one host, so their vsock
# CIDs must not overlap: node b is started with -node.vsock-cid-range (see
# E2E_NODEB_EXTRA_FLAGS) and the preflight fails clearly when the node binary
# does not know that flag.
#
# Stages (one PASS/FAIL line each; every /api/v1 response goes through the
# recording proxy and is validated by internal/compat):
#   preflight build image helpers+baseline services node-b write-snapshot
#   node-a node-loss node-returns stop-resume-lost-disk-node
#   stop-resume-a-to-b fork saved-count teardown-proof
# node-loss: a sandbox created while only b exists (so its disk was written
# on b) has a file written and a snapshot generation taken after it; b is
# killed with SIGKILL; within the node-lost grace the sandbox is ready on a
# with the file, and the guest networking on a works. node-returns: b comes
# back and its stale copy of the VM is deleted. stop-resume-lost-disk-node:
# a sandbox stopped on b whose node is then lost resumes on a. stop-resume-a-to-b:
# a sandbox stopped on a, with a made unable to run it (a filler sandbox
# takes a's one running slot), resumes on b with its data. fork: fork of a
# ready sandbox, source unchanged. saved-count: save as e2e-base and
# create 10 sandboxes from it across both nodes, each with the file.
#
# Needs: the integrated branch (a node with object-store support and the
# bbraft_node role's S3 variables, node flag -node.vsock-cid-range). Optional
# environment:
#   E2E_NODEB_EXTRA_FLAGS   extra flags for node b (default
#                           "-node.vsock-cid-range 1000000-1999999")
#   E2E2_DIAG_ON_FAIL=1     when a stage fails, before the teardown, collect and
#                           print (redacted, bounded) the API view, node a's nft
#                           ruleset, DNAT, journal and serial log, probes run
#                           inside the guest through exec, and one ssh -vvv
#                           attempt, for the sandboxes the stage touched; saved
#                           under $E2E_OUT/diag-<run>/. No secret is collected:
#                           everything passes e2e_redact, cloud-init is only
#                           listed. Off by default: nothing changes without it.
#   E2E_REUSE_IMAGE=1       never build: reuse images/out/<pinned version> or
#                           fail (an image already present is reused anyway
#                           when it passes the pinned-image check below)
# The image stage builds the pinned image when it is absent. When
# images/out/<version> exists it is reused only if images/verify.sh passes
# (manifest, sha256 and size of every artifact) and its manifest records the
# pinned id, Ubuntu snapshot (no mirror override), Go and Node versions; any
# other image there is refused with the reason (a published version is never
# overwritten, so it must be removed by hand).
# Expected wall time on a fresh VM: about 35-45 minutes with the image build,
# about 25 minutes with a cached image: node-lost waits dominate (two node
# losses, each at least the 60 s grace plus up to two 30 s inventory polls,
# then a restore) and ten sandboxes boot at the end.
#
# Self-test (no VM, no server, no node, no Garage, no real network):
#
#   E2E_SELFTEST=1 bash deploy/dev/e2e-p2.sh
#
# This script sources deploy/dev/e2e-p1.sh as a library (E2E_HELPERS_ONLY=1:
# recording proxy, validator, redaction, database and teardown helpers) and
# deploy/dev/e2e-p1-node.sh (node a). The cleanup trap prints E2E PASS only
# after the teardown proofs and the ip_forward restoration succeeded.
set -euo pipefail
set -E

E2E_OUT=${E2E_OUT:-$HOME/.cache/bamboo-raft/p2}
export E2E_OUT
e2e2_here=$(dirname -- "${BASH_SOURCE[0]}")
# shellcheck source=deploy/dev/e2e-p1.sh
E2E_HELPERS_ONLY=1 source "$e2e2_here/e2e-p1.sh"

# ---------------------------------------------------------------------------
# Constants and state (this script's own; the p1 library owns work, helpers,
# server_pid, proxy_pid, api_key, owner_uuid, e2e_ids, ...).
# ---------------------------------------------------------------------------
E2E_PROC_RE='qemu-system|bbraft-server|bbraft-node|e2e-p1-proxy|garage'
E2E2_SNAPSHOT_INTERVAL=10      # -lifecycle.snapshot-interval-seconds
E2E2_NODE_LOST=60              # -lifecycle.node-lost-seconds (the minimum)
E2E2_MOVE_BUDGET=300           # node lost -> sandbox ready elsewhere, seconds
E2E2_A_VCPUS=2                 # node a offers one running small sandbox
E2E2_A_MEMORY_MIB=4096
E2E2_A_DISK_GIB=110            # and two small disks (51 GiB each)
E2E2_NODEB_CID_RANGE=1000000-1999999
# Node b is sized from the host it runs on (e2e2_capacity_plan): ten small
# sandboxes' worth (one spare over what "count 10" asks of b), small being
# 2 vCPU / 4096 MiB / 51 GiB.
E2E2_SMALL_VCPUS=2 E2E2_SMALL_MEMORY_MIB=4096 E2E2_SMALL_DISK_GIB=51
E2E2_B_SLOTS=10
E2E2_MARGIN_PCT=125             # cpu and memory factors carry a 25% margin
E2E2_DISK_MARGIN_PCT=200        # disk factors carry 100%: free space shrinks as guests write
E2E2_MIN_FREE_DISK_GIB=30       # real free space required under each data dir
E2E2_MAX_CPU_OVERCOMMIT=8       # ceilings of the factors this script will use
E2E2_MAX_MEMORY_OVERCOMMIT=8
E2E2_MAX_DISK_OVERCOMMIT=20
E2E2_HOST_RESERVE_MEMORY_MIB=1024
E2E2_B_VCPUS="" E2E2_B_MEMORY_MIB="" E2E2_B_DISK_GIB=""
E2E2_B_CPU_OC="" E2E2_B_MEMORY_OC="" E2E2_B_DISK_OC=""
E2E2_A_DISK_OC=""
E2E2_FILE=/home/user/e2e.txt
E2E2_GARAGE_REGION=garage
E2E2_GARAGE_BUCKET=bbrafte2e
E2E2_GARAGE_KEY=bbrafte2e
E2E2_STAGES=(
  stage_preflight stage_build stage_image stage_helpers stage_services
  stage_node_b stage_write_snapshot stage_node_a stage_node_loss
  stage_node_returns stage_lost_disk_resume stage_a_to_b_resume
  stage_fork stage_saved_count stage_validate_live stage_teardown_proof
)
nodeb_pid=""          # setsid leader of node b, empty while it is down
nodeb_started=0       # node b was started at least once and is not yet stopped for good
nodeb_epoch=0
garage_pid=""
garage_cfg=""
s3_endpoint=""
s3_key_id=""
s3_root=""            # root-owned directory holding node a's S3 files
api_local=""          # loopback URL of the validating proxy
api_veth=""           # the same proxy as the client netns reaches it
a_vars=""
s1="" s2="" filler="" forkid=""
e2e2_count_ids=()
img_dir=""

# Per-instance names: node a goes through the e2e-p1-node.sh driver (its own
# attempt-unique unit, netns, veth and paths); node b is a plain process under
# this run's work directory. Nothing below depends on a fixed global name
# other than the server's node link port.
e2e2_node_id() { e2e2_instance_ok "$1" && printf 'e2e-node-%s\n' "$1"; }
e2e2_node_token_file() { e2e2_instance_ok "$1" && printf '%s/secrets/node-%s.token\n' "$work" "$1"; }
e2e2_node_dir() { e2e2_instance_ok "$1" && printf '%s/n-%s\n' "$work" "$1"; }
e2e2_instance_ok() { case ${1:-} in a | b) return 0 ;; *) echo "node instance must be a or b" >&2; return 1 ;; esac; }

# A p1 helper wrapper: failure dumps also show node b and Garage.
eval "$(declare -f e2e_dump | sed '1s/^e2e_dump/e2e1_dump/')"
e2e_dump() {
  e2e1_dump
  e2e2_diag_on_fail
  local f
  for f in "$work"/n-b/node.log "$work"/garage/garage.log; do
    [ -f "$f" ] || continue
    echo "--- tail ${f#"$work"/}"; tail -30 "$f" | e2e_redact | cut -c1-400
  done
}

# e2e_cli: the external client reaches the API through the veth address.
e2e_cli() {
  e2e_client env "XDG_CONFIG_HOME=$XDG_CONFIG_HOME" "BBRAFT_API_URL=$api_veth" "$B" "$@"
}

# ---------------------------------------------------------------------------
# Small helpers.
# ---------------------------------------------------------------------------
e2e2_sql() { # SQL [psql -v name=value...] -> trimmed single value
  local out
  e2e_pg_env || return 1
  out=$(pg_query "$@") || return 1
  trim "$out"
}

# e2e2_poll SECONDS CMD... returns 0 as soon as CMD succeeds, 1 on timeout. It
# never exits (teardown uses it).
e2e2_poll() {
  local secs=$1 i; shift
  for ((i = 0; i < secs; i++)); do
    if "$@" >/dev/null 2>&1; then return 0; fi
    sleep 1
  done
  return 1
}

e2e2_node_of() { e2e2_sql "SELECT coalesce(node_id,'') FROM public.sandboxes WHERE id=:'id'" -v id="$1"; }
e2e2_disk_node_of() { e2e2_sql "SELECT coalesce(disk_node_id,'') FROM public.sandboxes WHERE id=:'id'" -v id="$1"; }

# e2e2_ready_on ID NODE: ready, idle or running on NODE (empty NODE: any).
e2e2_ready_on() {
  local n
  n=$(e2e2_sql "SELECT count(*) FROM public.sandboxes WHERE id=:'id' AND deleted_at IS NULL
    AND state IN ('ready','idle','running') AND (:'node'='' OR node_id=:'node')" \
    -v id="$1" -v node="${2:-}") || return 1
  [ "$n" = 1 ]
}

# e2e2_wait_ready ID NODE SECONDS: fails the stage on timeout, prints how long
# it took.
e2e2_wait_ready() {
  local id=$1 node=$2 secs=$3
  e2e2_poll "$secs" e2e2_ready_on "$id" "$node" \
    || e2e_fail "timeout after ${secs}s: $id not ready${node:+ on $node} (now: state $(e2e2_sql "SELECT state FROM public.sandboxes WHERE id=:'id'" -v id="$id" 2>/dev/null) node '$(e2e2_node_of "$id" 2>/dev/null)')"
}

e2e2_create() { # NAME -> prints the sandbox id (registered for teardown)
  local id
  e2e_run_rc "$work/logs/new-$1.out" "$B" new --type small --name "$1" --ttl 7200 --json
  [ "$E2E_RC" = 0 ] || e2e_fail "new $1: $(tail -3 "$work/logs/new-$1.out" | e2e_redact)"
  id=$(jq -r 'select(.event == "ready") | .id' "$work/logs/new-$1.out" | tail -1)
  [ -n "$id" ] && [[ $id == bx_* ]] || e2e_fail "new $1: no bx_ id"
  e2e_ids+=("$id")
  printf '%s\n' "$id"
}

e2e2_write() { # ID PATH CONTENT: content is a plain marker word
  e2e_ok "$B" exec "$1" -- bash -lc "printf '%s\n' '$3' > '$2' && sync"
}

e2e2_expect_file() { # ID PATH CONTENT
  e2e_ok "$B" exec "$1" -- cat "$2"
  e2e_have "$3" "$work/logs/last.out"
}

# e2e2_wait_gen_after ID EPOCH SECONDS: the latest snapshot generation was
# captured strictly after EPOCH (a write that finished at EPOCH is in it).
e2e2_wait_gen_after() {
  local id=$1 after=$2 secs=$3 i code
  for ((i = 0; i < secs; i++)); do
    code=$(api_curl GET "/sandboxes/$id/snapshots/latest")
    if [ "$code" = 200 ] && jq -e --argjson t "$after" \
      '.snapshot != null and ((.snapshot.createdAt | sub("(\\.[0-9]+)?(Z|[+]00:00)$"; "Z") | fromdateiso8601) > $t)' \
      "$work/logs/last-api.out" >/dev/null; then
      return 0
    fi
    sleep 1
  done
  e2e_fail "no snapshot generation of $id newer than the write within ${secs}s (interval ${E2E2_SNAPSHOT_INTERVAL}s)"
}

# e2e2_tap_check ID: guest networking on node a: ssh through the SSH ingress
# from the client netns, and the guest holds an address inside the node a guest
# prefix (E2E_NODE_GUEST_PREFIX, from the node driver).
# e2e2_prefix_regex PREFIX prints the grep pattern of an address inside PREFIX:
# a leading space and the network octets, for prefixes on an octet boundary.
e2e2_prefix_regex() {
  local net=${1%/*} len=${1#*/} n i out=" " oct
  local -a o
  case $len in 8 | 16 | 24) n=$((len / 8)) ;; *) echo "guest prefix $1 is not on an octet boundary" >&2; return 1 ;; esac
  IFS=. read -r -a o <<<"$net"
  for ((i = 0; i < n; i++)); do oct=${o[$i]}; out="$out$oct\\."; done
  printf '%s\n' "$out"
}
e2e2_tap_check() {
  local re
  e2e_expect_rc 0 "$work/logs/ssh-$1.out" \
    e2e_cli ssh "$1" 'echo ssh-ok; ip -4 -o addr show'
  e2e_have ssh-ok "$work/logs/ssh-$1.out"
  re=$(e2e2_prefix_regex "${E2E_NODE_GUEST_PREFIX:?}" 2>&1) || e2e_fail "guest prefix: $re"
  e2e_have "$re" "$work/logs/ssh-$1.out"
}

e2e2_delete() { # ID: delete and wait for the durable operation
  e2e_ok "$B" delete "$1" --yes --wait
}

e2e2_wait_stopped() { # ID
  e2e2_poll 240 e2e2_state_is "$1" archived || e2e_fail "timeout: $1 not archived"
}
e2e2_state_is() {
  local n
  n=$(e2e2_sql "SELECT count(*) FROM public.sandboxes WHERE id=:'id' AND state=:'st'" -v id="$1" -v st="$2") || return 1
  [ "$n" = 1 ]
}

# e2e2_eviction_open_count NODE ID: open (not yet deleted) VM copies of ID on NODE.
e2e2_eviction_state() { # NODE ID -> "rows open"
  local rows open
  rows=$(e2e2_sql "SELECT count(*) FROM public.vm_evictions WHERE node_id=:'n' AND sandbox_id=:'id'" -v n="$1" -v id="$2") || return 1
  open=$(e2e2_sql "SELECT count(*) FROM public.vm_evictions WHERE node_id=:'n' AND sandbox_id=:'id' AND done_at IS NULL" -v n="$1" -v id="$2") || return 1
  printf '%s %s\n' "$rows" "$open"
}
e2e2_eviction_done() { # NODE ID: at least one row and none open
  local st
  st=$(e2e2_eviction_state "$1" "$2") || return 1
  [ "$st" != "0 0" ] && [ "${st#* }" = 0 ]
}

e2e2_disk_released() { [ -z "$(e2e2_disk_node_of "$1")" ]; }
e2e2_node_stale() { # NODE: marked stale (epoch last_seen_at) by the inventory
  local n
  n=$(e2e2_sql "SELECT count(*) FROM public.nodes WHERE id=:'n' AND last_seen_at < now() - interval '1 hour'" -v n="$1") || return 1
  [ "$n" = 1 ]
}
e2e2_nodeb_no_qemu() { [ "$(e2e2_nodeb_qemu_count)" = 0 ]; }

# e2e2_nodeb_qemu_count: QEMU processes whose command line holds node b's data
# directory (everything node b ever ran).
e2e2_nodeb_qemu_count() {
  pgrep -f "qemu-system.*$(e2e2_node_dir b)/" 2>/dev/null | wc -l | tr -d ' '
}

# ---------------------------------------------------------------------------
# Node b: a plain process in user mode.
# ---------------------------------------------------------------------------
# e2e2_nodeb_flags ARRAY_NAME fills ARRAY_NAME with node b's extra flags: the
# default CID range unless E2E_NODEB_EXTRA_FLAGS is set (empty disables it).
e2e2_nodeb_flags() {
  local -n out=$1
  read -r -a out <<<"${E2E_NODEB_EXTRA_FLAGS-"-node.vsock-cid-range $E2E2_NODEB_CID_RANGE"}"
}

# ---------------------------------------------------------------------------
# Node capacities, sized from the host.
#
# Node b offers ten small sandboxes' worth, which on one modest host is more
# than the host has left once node a's share is taken. The node refuses an
# explicit capacity above (detected host) x (overcommit factor), so b gets its
# own -node.cpu-overcommit / -node.memory-overcommit / -node.disk-overcommit,
# computed here from nproc, MemTotal and the free disk, never fixed numbers.
# Node a keeps its small offer (one running small sandbox, two small disks:
# 110 GiB) and gets a disk overcommit through the bbraft_node role variable
# bbraft_node_disk_overcommit when its filesystem has less free than that.
# Overcommit is safe for this test: the guests idle, a small idle guest touches
# a few hundred MiB of the 4 GiB it reserves, its CPUs sleep, and the disks are
# thin qcow2 files. What must be real is free space, so the plan refuses a
# host with less than E2E2_MIN_FREE_DISK_GIB actually free under either data
# directory (one shared filesystem is the same free space, counted once).
# ---------------------------------------------------------------------------
# e2e2_overcommit_centi NEED FAIR [MARGIN_PCT] prints the factor, in
# hundredths, that lets FAIR carry NEED with the margin: at least 1.00,
# rounded up.
e2e2_overcommit_centi() {
  local need=$1 fair=$2 margin=${3:-$E2E2_MARGIN_PCT} c
  c=$(((need * margin + fair - 1) / fair))
  echo $((c < 100 ? 100 : c))
}

e2e2_centi_fmt() { printf '%d.%02d' $(($1 / 100)) $(($1 % 100)); }

# e2e2_host_facts prints "VCPUS MEMORY_MIB B_FREE_GIB A_FREE_GIB SAME_DEVICE":
# the host's CPUs and MemTotal, the free space under node b's data directory
# (its parent while it does not exist) and under node a's (/var/lib), and
# whether those two are the same filesystem.
e2e2_host_facts() {
  local vcpus mem bdir adir bfree afree same=0
  vcpus=$(nproc)
  mem=$(awk '/^MemTotal:/ { print int($2 / 1024) }' /proc/meminfo)
  bdir=${1:-$HOME/.bbk}
  adir=/var/lib
  bfree=$(df -B1 --output=avail "$bdir" | awk 'NR == 2 { printf "%d", $1 / 1073741824 }')
  afree=$(df -B1 --output=avail "$adir" | awk 'NR == 2 { printf "%d", $1 / 1073741824 }')
  [ "$(stat -c %d "$bdir")" = "$(stat -c %d "$adir")" ] && same=1
  echo "$vcpus $mem $bfree $afree $same"
}

# e2e2_capacity_plan HOST_VCPUS HOST_MEMORY_MIB B_FREE_GIB A_FREE_GIB SAME_DEVICE
# fills E2E2_B_* (node b's capacity and its three overcommit factors) and
# E2E2_A_DISK_OC (node a's disk factor) and prints the arithmetic. It returns
# 1 with the reason on stderr when the free-space floor is not met or even the
# ceilings of the factors cannot make the scenario fit (never aborts).
# vCPUs and memory are shared out: b gets what is left of the host after node
# a's offer. Disk offers are promises, not usage: each node's factor is taken
# against the free space of its own data directory, which on one shared
# filesystem is the same free space.
e2e2_capacity_plan() {
  local hv=$1 hm=$2 bfree=$3 afree=$4 same=$5
  local fv fm cv cm cd ov om odb oda why=""
  cv=$((E2E2_B_SLOTS * E2E2_SMALL_VCPUS))
  cm=$((E2E2_B_SLOTS * E2E2_SMALL_MEMORY_MIB))
  cd=$((E2E2_B_SLOTS * E2E2_SMALL_DISK_GIB))
  # What is left for node b once node a's offer is taken from the host.
  fv=$((hv - E2E2_A_VCPUS))
  fm=$((hm - E2E2_A_MEMORY_MIB - E2E2_HOST_RESERVE_MEMORY_MIB))
  if [ "$afree" -lt "$E2E2_MIN_FREE_DISK_GIB" ]; then
    why="only $afree GiB are free under /var/lib (node a); at least $E2E2_MIN_FREE_DISK_GIB GiB must be really free"
  elif [ "$bfree" -lt "$E2E2_MIN_FREE_DISK_GIB" ]; then
    why="only $bfree GiB are free under node b's data directory; at least $E2E2_MIN_FREE_DISK_GIB GiB must be really free"
  elif [ "$fv" -le 0 ]; then
    why="the host has $hv vCPUs and node a offers $E2E2_A_VCPUS: none left for node b"
  elif [ "$fm" -le 0 ]; then
    why="the host has $hm MiB of memory; node a offers $E2E2_A_MEMORY_MIB and $E2E2_HOST_RESERVE_MEMORY_MIB stay with the host: none left for node b"
  fi
  if [ -z "$why" ]; then
    ov=$(e2e2_overcommit_centi "$cv" "$fv")
    om=$(e2e2_overcommit_centi "$cm" "$fm")
    odb=$(e2e2_overcommit_centi "$cd" "$bfree" "$E2E2_DISK_MARGIN_PCT")
    oda=$(e2e2_overcommit_centi "$E2E2_A_DISK_GIB" "$afree" "$E2E2_DISK_MARGIN_PCT")
    [ "$ov" -le $((E2E2_MAX_CPU_OVERCOMMIT * 100)) ] || why="vCPUs: node b needs $cv (ten small sandboxes), $fv are left: factor $(e2e2_centi_fmt "$ov") exceeds the allowed $E2E2_MAX_CPU_OVERCOMMIT"
    [ -n "$why" ] || [ "$om" -le $((E2E2_MAX_MEMORY_OVERCOMMIT * 100)) ] || why="memory: node b needs $cm MiB, $fm MiB are left: factor $(e2e2_centi_fmt "$om") exceeds the allowed $E2E2_MAX_MEMORY_OVERCOMMIT"
    [ -n "$why" ] || [ "$odb" -le $((E2E2_MAX_DISK_OVERCOMMIT * 100)) ] || why="disk: node b offers $cd GiB, $bfree GiB are free: factor $(e2e2_centi_fmt "$odb") exceeds the allowed $E2E2_MAX_DISK_OVERCOMMIT"
    [ -n "$why" ] || [ "$oda" -le $((E2E2_MAX_DISK_OVERCOMMIT * 100)) ] || why="disk: node a offers $E2E2_A_DISK_GIB GiB, $afree GiB are free: factor $(e2e2_centi_fmt "$oda") exceeds the allowed $E2E2_MAX_DISK_OVERCOMMIT"
  fi
  if [ -n "$why" ]; then
    echo "host too small for the two-node scenario: $why (\"count 10\" needs $E2E2_B_SLOTS small sandboxes across both nodes; use a larger VM)" >&2
    return 1
  fi
  E2E2_B_VCPUS=$cv E2E2_B_MEMORY_MIB=$cm E2E2_B_DISK_GIB=$cd
  E2E2_B_CPU_OC=$(e2e2_centi_fmt "$ov")
  E2E2_B_MEMORY_OC=$(e2e2_centi_fmt "$om")
  E2E2_B_DISK_OC=$(e2e2_centi_fmt "$odb")
  E2E2_A_DISK_OC=$(e2e2_centi_fmt "$oda")
  echo "host: $hv vCPUs, $hm MiB; free disk: $bfree GiB under node b's data, $afree GiB under /var/lib (same filesystem: $same; floor $E2E2_MIN_FREE_DISK_GIB GiB)"
  echo "left for node b after node a: $fv vCPUs, $fm MiB"
  echo "node b offers $cv vCPUs, $cm MiB, $cd GiB; overcommit cpu $E2E2_B_CPU_OC memory $E2E2_B_MEMORY_OC disk $E2E2_B_DISK_OC (margin ${E2E2_MARGIN_PCT}%, disk ${E2E2_DISK_MARGIN_PCT}%)"
  echo "node a offers $E2E2_A_VCPUS vCPUs, $E2E2_A_MEMORY_MIB MiB, $E2E2_A_DISK_GIB GiB; disk overcommit $E2E2_A_DISK_OC (bbraft_node_disk_overcommit)"
}

# e2e2_nodeb_capacity_flags ARRAY_NAME fills ARRAY_NAME with node b's capacity
# and overcommit flags (after e2e2_capacity_plan).
e2e2_nodeb_capacity_flags() {
  local -n out=$1
  [ -n "$E2E2_B_VCPUS" ] || { echo "e2e2_nodeb_capacity_flags: no capacity plan" >&2; return 1; }
  out=(-node.capacity-vcpus "$E2E2_B_VCPUS" -node.capacity-memory-mib "$E2E2_B_MEMORY_MIB"
    -node.capacity-disk-gib "$E2E2_B_DISK_GIB"
    -node.cpu-overcommit "$E2E2_B_CPU_OC" -node.memory-overcommit "$E2E2_B_MEMORY_OC"
    -node.disk-overcommit "$E2E2_B_DISK_OC")
}

e2e2_nodeb_start() {
  local d
  local -a extra cap
  d=$(e2e2_node_dir b)
  e2e2_nodeb_flags extra
  e2e2_nodeb_capacity_flags cap
  mkdir -p "$d/data"
  nodeb_epoch=$(date +%s)
  BBRAFT_S3_ENDPOINT=$s3_endpoint BBRAFT_S3_REGION=$E2E2_GARAGE_REGION \
    BBRAFT_S3_BUCKET=$E2E2_GARAGE_BUCKET BBRAFT_S3_PATH_STYLE=true \
    BBRAFT_S3_ACCESS_KEY_ID=$s3_key_id \
    BBRAFT_S3_SECRET_ACCESS_KEY_FILE=$work/secrets/s3-secret \
    setsid "$repo/bin/bbraft-node" \
    -server.addr 127.0.0.1:7701 -node.id "$(e2e2_node_id b)" \
    -node.token-file "$(e2e2_node_token_file b)" \
    -node.data-dir "$d/data" -node.image-dir "$repo/images/out" \
    -node.default-image "$IMAGE_VERSION" \
    -node.network-mode user \
    "${cap[@]}" \
    ${extra[@]+"${extra[@]}"} \
    >>"$d/node.log" 2>&1 &
  nodeb_pid=$!
  nodeb_started=1
}

e2e2_nodeb_alive() { [ -n "$nodeb_pid" ] && kill -0 "$nodeb_pid" 2>/dev/null; }

# e2e2_nodeb_registered: node b connected and freshly inventoried since its
# last start.
e2e2_nodeb_registered() {
  local n
  e2e2_nodeb_alive || return 1
  n=$(e2e2_sql "SELECT count(*) FROM public.nodes WHERE id=:'id' AND default_image=:'img'
    AND total_disk_gib>0 AND last_seen_at>=to_timestamp(:'epoch'::bigint)
    AND last_seen_at>=now()-interval '120 seconds'" \
    -v id="$(e2e2_node_id b)" -v img="$IMAGE_VERSION" -v epoch="$nodeb_epoch") || return 1
  [ "$n" = 1 ]
}

# e2e2_nodeb_kill9: SIGKILL of the node process only (its QEMU children are
# not signalled and may outlive it), reaped, marked down.
e2e2_nodeb_kill9() {
  [ -n "$nodeb_pid" ] || return 0
  kill -KILL "$nodeb_pid" 2>/dev/null || true
  wait "$nodeb_pid" 2>/dev/null || true
  nodeb_pid=""
}

# e2e2_nodeb_stop terminates node b cleanly (TERM to its group, KILL after the
# grace), then removes any QEMU it left behind, and proves nothing of it runs.
e2e2_nodeb_stop() {
  local rc=0 d
  d=$(e2e2_node_dir b)
  if [ -n "$nodeb_pid" ]; then
    if e2e_reap_daemon "$nodeb_pid" 25; then :; else
      echo "TEARDOWN LEAK: node b process group survived"; rc=1
    fi
    nodeb_pid=""
  fi
  if [ "$(e2e2_nodeb_qemu_count)" != 0 ]; then
    pkill -KILL -f "qemu-system.*$d/" 2>/dev/null || true
    sleep 1
    if [ "$(e2e2_nodeb_qemu_count)" != 0 ]; then
      echo "TEARDOWN LEAK: QEMU of node b still running"; rc=1
    else
      echo "TEARDOWN: removed QEMU processes node b left behind"
    fi
  fi
  [ "$rc" = 0 ] && nodeb_started=0
  return "$rc"
}

# ---------------------------------------------------------------------------
# Garage and node a's object-store files.
# ---------------------------------------------------------------------------
e2e2_free_ports() { # N -> N distinct free loopback ports, one per line
  python3 -I - "$1" <<'PY'
import socket, sys
socks, ports = [], []
for _ in range(int(sys.argv[1])):
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    socks.append(s)
    ports.append(s.getsockname()[1])
for p in ports:
    print(p)
for s in socks:
    s.close()
PY
}

e2e2_garage() { timeout 30 garage -c "$garage_cfg" "$@"; }

# e2e2_garage_start starts a single-node Garage from the binary in PATH the
# way internal/objstore/s3/s3test does, creates the bucket and key, and leaves
# the endpoint, the key id and the secret file (0600) for the server and the
# nodes. The secret never reaches argv or output.
e2e2_garage_start() {
  local g=$work/garage ports node_id key_out secret i f
  mkdir -p "$g"/{meta,data,secrets}
  chmod 700 "$g" "$g/secrets"
  for f in rpc_secret admin_token metrics_token; do
    head -c 32 /dev/urandom | od -An -tx1 | tr -d ' \n' >"$g/secrets/$f"
  done
  mapfile -t ports < <(e2e2_free_ports 3)
  garage_cfg=$g/garage.toml
  cat >"$garage_cfg" <<EOF
metadata_dir = "$g/meta"
data_dir = "$g/data"
db_engine = "lmdb"
replication_factor = 1
metadata_fsync = false
data_fsync = false
compression_level = "none"
rpc_bind_addr = "127.0.0.1:${ports[0]}"
rpc_public_addr = "127.0.0.1:${ports[0]}"
rpc_secret_file = "$g/secrets/rpc_secret"

[s3_api]
s3_region = "$E2E2_GARAGE_REGION"
api_bind_addr = "127.0.0.1:${ports[1]}"

[admin]
api_bind_addr = "127.0.0.1:${ports[2]}"
admin_token_file = "$g/secrets/admin_token"
metrics_token_file = "$g/secrets/metrics_token"
EOF
  s3_endpoint=http://127.0.0.1:${ports[1]}
  setsid garage -c "$garage_cfg" server >"$g/garage.log" 2>&1 &
  garage_pid=$!
  e2e2_poll 60 e2e2_garage status || e2e_fail "garage did not answer within 60s (see garage/garage.log)"
  node_id=$(e2e2_garage node id -q) || e2e_fail "garage node id"
  node_id=${node_id%%@*}
  [ -n "$node_id" ] || e2e_fail "garage reported no node id"
  e2e2_garage layout assign -z dc1 -c 1G "$node_id" >/dev/null || e2e_fail "garage layout assign"
  e2e2_garage layout apply --version 1 >/dev/null || e2e_fail "garage layout apply"
  key_out=$(e2e2_garage key create "$E2E2_GARAGE_KEY" 2>&1) || e2e_fail "garage key create"
  s3_key_id=$(printf '%s\n' "$key_out" | sed -n 's/^Key ID: *//p' | head -1)
  secret=$(printf '%s\n' "$key_out" | sed -n 's/^Secret key: *//p' | head -1)
  unset key_out
  [ -n "$s3_key_id" ] && [ -n "$secret" ] || e2e_fail "garage key create printed no key id and secret"
  printf %s "$secret" >"$work/secrets/s3-secret"
  chmod 600 "$work/secrets/s3-secret"
  printf '%s\n%s\n' "$s3_key_id" "$secret" >>"$work/secrets/redact.list"
  unset secret
  for ((i = 1; i <= 20; i++)); do
    e2e2_garage bucket create "$E2E2_GARAGE_BUCKET" >/dev/null 2>&1 && break
    [ "$i" -lt 20 ] || e2e_fail "garage bucket create kept failing"
    sleep 0.5
  done
  e2e2_garage bucket allow --read --write --owner "$E2E2_GARAGE_BUCKET" --key "$E2E2_GARAGE_KEY" >/dev/null \
    || e2e_fail "garage bucket allow"
}

e2e2_garage_stop() {
  [ -n "$garage_pid" ] || return 0
  if ! e2e_reap_daemon "$garage_pid" 15; then
    echo "TEARDOWN LEAK: garage process group survived"
    return 1
  fi
  garage_pid=""
}

# e2e2_s3_stage writes node a's object-store settings as root-owned 0600 files
# (the role's unit reads the env file and loads the secret as a credential)
# and the extra role variables for the node driver.
e2e2_s3_stage() {
  s3_root=/etc/bbraft-e2e-s3-$(head -c 6 /dev/urandom | od -An -tx1 | tr -d ' \n')
  e2e_priv install -d -m 0700 -o root -g root "$s3_root" || return 1
  {
    printf 'BBRAFT_S3_ENDPOINT=%s\n' "$s3_endpoint"
    printf 'BBRAFT_S3_REGION=%s\n' "$E2E2_GARAGE_REGION"
    printf 'BBRAFT_S3_BUCKET=%s\n' "$E2E2_GARAGE_BUCKET"
    printf 'BBRAFT_S3_PATH_STYLE=true\n'
    printf 'BBRAFT_S3_ACCESS_KEY_ID=%s\n' "$s3_key_id"
  } >"$work/secrets/s3.env"
  e2e_priv install -m 0600 -o root -g root "$work/secrets/s3.env" "$s3_root/s3.env" || return 1
  e2e_priv install -m 0600 -o root -g root "$work/secrets/s3-secret" "$s3_root/s3-secret-access-key" || return 1
  [ -n "$E2E2_A_DISK_OC" ] || { echo "e2e2_s3_stage: no capacity plan for node a" >&2; return 1; }
  a_vars=$work/secrets/node-a-vars.json
  jq -n --arg env "$s3_root/s3.env" --arg secret "$s3_root/s3-secret-access-key" \
    --argjson vcpus "$E2E2_A_VCPUS" --argjson mem "$E2E2_A_MEMORY_MIB" --argjson disk "$E2E2_A_DISK_GIB" \
    --arg diskoc "$E2E2_A_DISK_OC" \
    '{bbraft_node_s3_env_file: $env, bbraft_node_s3_secret_file: $secret,
      bbraft_node_capacity_vcpus: $vcpus, bbraft_node_capacity_memory_mib: $mem,
      bbraft_node_capacity_disk_gib: $disk, bbraft_node_disk_overcommit: $diskoc,
      bbraft_node_chunk_cache_bytes: 1073741824}' >"$a_vars"
  chmod 600 "$a_vars"
}

e2e2_s3_unstage() {
  [ -n "$s3_root" ] || return 0
  case $s3_root in /etc/bbraft-e2e-s3-*) ;; *) echo "TEARDOWN LEAK: refusing to remove $s3_root"; return 1 ;; esac
  e2e_priv rm -rf -- "$s3_root" || return 1
  [ ! -e "$s3_root" ] || { echo "TEARDOWN LEAK: $s3_root still exists"; return 1; }
  s3_root=""
}

# e2e2_reservation_proof: both nodes' counters are zero and no row of THIS
# owner keeps a reservation.
e2e2_reservation_proof() {
  local n
  n=$(e2e2_sql "SELECT count(*) FROM public.nodes WHERE id IN ('e2e-node-a','e2e-node-b')
    AND reserved_vcpus=0 AND reserved_memory_mib=0 AND reserved_disk_gib=0") || return 1
  [ "$n" = 2 ] || { echo "node reservations not zero ($n of 2 nodes clean)"; return 1; }
  [ -n "$owner_uuid" ] || return 0
  n=$(e2e2_sql "SELECT count(*) FROM public.sandboxes WHERE owner_id=:'owner'::uuid AND (reserved_vcpus>0 OR reserved_memory_mib>0 OR reserved_disk_gib>0)" \
    -v owner="$owner_uuid") || return 1
  [ "$n" = 0 ] || { echo "$n owner sandbox rows still hold reservations"; return 1; }
}

# ---------------------------------------------------------------------------
# Strict teardown. Replaces the one-node teardown of the p1 library: both
# nodes, Garage and node a's S3 files.
# ---------------------------------------------------------------------------
e2e_teardown() { # $1 = original rc
  local rc=$1 id listed gone_leaks=0 pg
  set +e; trap - ERR
  # 1. Every own sandbox through the API (node b must be up to delete its VMs).
  if [ "$e2e_body_complete" != 1 ] && [ "$api_started" = 1 ] && [ -x "${B:-}" ]; then
    if [ "$nodeb_started" = 1 ] && ! e2e2_nodeb_alive; then
      e2e2_nodeb_start
      e2e2_poll 90 e2e2_nodeb_registered || echo "TEARDOWN: node b did not come back for the sweep"
    fi
    if BBRAFT_API_KEY=$api_key e2e_run_rc "$work/logs/sweep.out" "$B" list --all --json && [ "$E2E_RC" = 0 ]; then
      for id in $(jq -r '.sandboxes[]?.id' "$work/logs/sweep.out" 2>/dev/null); do
        case " ${e2e_ids[*]:-} " in *" $id "*) ;; *) e2e_ids+=("$id");; esac
      done
    else
      echo "TEARDOWN LEAK: sweep list failed (API down?)" | e2e_redact
      gone_leaks=1
    fi
    if [ -n "$owner_uuid" ] && [ -x "$work/pgenv" ] && [ -f "$work/secrets/database-url" ] && e2e_pg_env; then
      for id in $(pg_query "SELECT id FROM public.sandboxes WHERE owner_id=:'owner'::uuid AND (reserved_vcpus>0 OR reserved_memory_mib>0 OR reserved_disk_gib>0) UNION SELECT target_id FROM public.deletion_operations WHERE owner_id=:'owner'::uuid AND kind='sandbox' AND status<>'completed'" \
        -v owner="$owner_uuid" 2>/dev/null); do
        case " ${e2e_ids[*]:-} " in *" $id "*) ;; *) e2e_ids+=("$id");; esac
      done
    fi
    for id in "${e2e_ids[@]:-}"; do
      [ -n "$id" ] || continue
      BBRAFT_API_KEY=$api_key "$B" delete "$id" --yes --wait >"$work/logs/teardown-delete.out" 2>&1
      if [ $? != 0 ]; then
        if BBRAFT_API_KEY=$api_key e2e_run_rc "$work/logs/teardown-info.out" "$B" info "$id" --json && [ "$E2E_RC" = 1 ] &&
          grep -q 'not_found' "$work/logs/teardown-info.out"; then
          :
        else
          echo "TEARDOWN LEAK: sandbox $id not deleted" | e2e_redact
          gone_leaks=1
        fi
      fi
    done
    if [ -n "$owner_uuid" ] && [ -x "$work/pgenv" ] && [ -f "$work/secrets/database-url" ]; then
      listed=$(e2e_teardown_wait_ops 2>&1) || { echo "TEARDOWN LEAK: deletion operations: $listed" | e2e_redact; gone_leaks=1; }
    fi
    if [ -x "$work/pgenv" ] && [ -f "$work/secrets/database-url" ]; then
      listed=$(e2e2_reservation_proof 2>&1) || { echo "TEARDOWN LEAK: reservation proof: $listed" | e2e_redact; gone_leaks=1; }
    fi
  fi
  # 2. Node b, then node a (stop and leak proof on every path, partial starts
  # too; the helpers run as standalone errexit-active children).
  if [ "$nodeb_started" = 1 ]; then
    e2e2_nodeb_stop || gone_leaks=1
  fi
  if [ "$node_started" = 1 ] && declare -F e2e_node_stop >/dev/null 2>&1; then
    e2e_run_helper 300 e2e_node_stop
    [ "$E2E_HELPER_RC" = 0 ] || { echo "TEARDOWN LEAK: e2e_node_stop failed ($E2E_HELPER_RC)"; gone_leaks=1; }
    e2e_run_helper 180 e2e_node_leak_proof
    [ "$E2E_HELPER_RC" = 0 ] || { echo "TEARDOWN LEAK: e2e_node_leak_proof failed ($E2E_HELPER_RC)"; gone_leaks=1; }
  fi
  # 3. Children: proxy drain, capture validation, server, Garage, database.
  if [ -n "$proxy_pid" ]; then
    e2e_stop_proxy || gone_leaks=1
  fi
  if [ "$e2e_body_complete" != 1 ] && [ -f "$work/capture/index.ndjson" ] && [ -d "${helpers:-/nonexistent}/validator" ]; then
    e2e_validate final "$work/logs/validate-teardown.log" || { echo "TEARDOWN: capture validation failed"; gone_leaks=1; }
  fi
  if [ -n "$server_pid" ]; then
    e2e_stop_server || gone_leaks=1
  fi
  if [ -n "$garage_pid" ]; then
    e2e2_garage_stop || gone_leaks=1
  fi
  if [ "$e2e_db_created" = 1 ] && [ -x "$work/pgenv" ] && [ -f "$work/secrets/database-url-base" ]; then
    e2e_drop_database || { echo "TEARDOWN LEAK: could not drop the owned e2e database"; gone_leaks=1; }
  fi
  for pg in "${e2e_bg[@]:-}"; do
    [ -n "$pg" ] || continue
    kill -KILL "$pg" 2>/dev/null
    wait "$pg" 2>/dev/null
  done
  e2e_bg=()
  # 4. Leak scan against the baseline, node a's S3 files.
  if [ "$e2e_body_complete" != 1 ] && [ -f "$work/baseline.links" ]; then
    e2e_leak_scan || gone_leaks=1
  fi
  if [ -n "$s3_root" ]; then
    e2e2_s3_unstage || gone_leaks=1
  fi
  [ -n "${helpers:-}" ] && rm -rf "$helpers"
  rm -rf "${work:-}/capture" "${work:-}/secrets" "${work:-}/cfg" "${work:-}/cfg-ci" "${work:-}/garage/data" "${work:-}/garage/meta" "${work:-}/garage/secrets" 2>/dev/null
  if [ "$rc" != 0 ]; then
    echo "(work dir kept for post-mortem: ${work:-}; sensitive capture/secrets removed)"
  else
    find "${work:-}" -mindepth 1 -maxdepth 1 ! -name logs ! -name n-b ! -name garage ${node_parent:+! -name "$(basename "$node_parent")"} \
      -exec rm -rf {} + 2>/dev/null
    echo "(evidence kept: ${work:-}/logs, ${node_parent:-no node parent})"
  fi
  # 5. Restore and verify the prior ip_forward.
  if [ "${ip_forward_set:-0}" = 1 ]; then
    if e2e_ipf_write "$ip_forward_prev"; then
      local now
      now=$(e2e_ipf_read 2>/dev/null || echo unknown)
      if [ "$now" = "$ip_forward_prev" ]; then
        echo "ip_forward restored to $ip_forward_prev"
      else
        echo "LEAK: net.ipv4.ip_forward is $now, want $ip_forward_prev"
        gone_leaks=1
      fi
    else
      echo "LEAK: could not restore net.ipv4.ip_forward to $ip_forward_prev"
      gone_leaks=1
    fi
  fi
  [ "$gone_leaks" = 0 ]
}

# ---------------------------------------------------------------------------
# Failure diagnostics (opt-in): E2E2_DIAG_ON_FAIL=1.
#
# When a stage fails, e2e_dump (which runs before the teardown) calls
# e2e2_diag_collect: for the sandboxes the stage touched it collects, into
# $E2E_OUT/diag-<run>/ AND on stdout, what is needed to tell where guest
# networking or the SSH path broke: the API view, the node's nft ruleset and
# DNAT, its journal, the VM serial log, and probes run INSIDE the guest through
# exec (never ssh), then one ssh -vvv attempt. Nothing here may carry a secret:
# every collected text goes through e2e_redact before it is written or printed;
# cloud-init is only ever listed (ls -l), never read; no env file, token or
# sandbox env value is touched. Every step is bounded and failure-tolerant;
# without the variable nothing runs.
# ---------------------------------------------------------------------------
E2E2_DIAG_BUDGET=900          # seconds for the whole collection
E2E2_DIAG_MAX_SANDBOXES=4
e2e2_diag_dir=""
e2e2_diag_n=0
e2e2_diag_deadline=0
e2e2_diag_api_raw=""
e2e2_stage_ids_from=0

# The probe run inside the guest through exec. R is a root prefix for the
# self-test only (empty in a real guest). No command reads cloud-init user-data,
# vendor-data or network-config: /var/lib/cloud is only listed.
E2E2_GUEST_PROBE=$(
  cat <<'EOS'
R=${R:-}
sec() { printf '\n## %s\n' "$*"; }
sec "id"; id
sec "cat /proc/cmdline"; cat "$R/proc/cmdline" 2>&1
sec "ip -4 -o addr show"; ip -4 -o addr show 2>&1
sec "ip route"; ip route 2>&1
sec "netplan listing"; ls -l "$R"/etc/netplan 2>&1
sec "netplan yaml"; for f in "$R"/etc/netplan/*.yaml; do [ -f "$f" ] || continue; echo "# $f"; cat "$f"; done 2>&1
sec "networkctl status"; networkctl status --no-pager 2>&1 | head -60
sec "systemctl status bbraft-network-config systemd-networkd"; systemctl status bbraft-network-config systemd-networkd --no-pager 2>&1 | head -80
sec "cloud-init status --long"; cloud-init status --long 2>&1
sec "cloud instances (listing only)"; ls -l "$R"/var/lib/cloud/instances/ "$R"/var/lib/cloud/instances/*/ 2>&1
sec "machine-id"; cat "$R"/etc/machine-id 2>&1
sec "systemctl --failed"; systemctl --failed --no-pager 2>&1
sec "ssh directories (listing)"; ls -la "$HOME/.ssh" "$R"/home/user/.ssh 2>&1
sec "authorized_keys (public keys)"
for f in "$HOME/.ssh/authorized_keys" "$R/home/user/.ssh/authorized_keys"; do echo "# $f"; cat "$f" 2>&1; done
sec "sshd"; systemctl is-active ssh sshd 2>&1; ss -ltn 2>&1 | head -20
EOS
)

# e2e2_diag_run LABEL LINES CMD...: run CMD (it bounds itself with timeout),
# redact, save the whole text (bounded) to the diag directory and print its
# last LINES lines. Never fails.
e2e2_diag_run() {
  local label=$1 lines=$2 out f
  shift 2
  if [ "$(date +%s)" -gt "$e2e2_diag_deadline" ]; then
    echo "--- diag $label: skipped (collection budget ${E2E2_DIAG_BUDGET}s used)"
    return 0
  fi
  e2e2_diag_n=$((e2e2_diag_n + 1))
  f=$e2e2_diag_dir/$(printf '%02d-%s' "$e2e2_diag_n" "$(printf '%s' "$label" | tr -c 'A-Za-z0-9._-' '_' | cut -c1-80)").txt
  out=$({ "$@" 2>&1 || echo "(exit $?)"; } | e2e_redact | head -n 4000 | cut -c1-600 || true)
  printf '%s\n' "$out" >"$f" 2>/dev/null || true
  printf -- '--- diag %s\n' "$label"
  printf '%s\n' "$out" | tail -n "$lines"
  return 0
}

# Probe bodies (each bounds its external commands itself).
e2e2_dg_api() { printf '%s' "$e2e2_diag_api_raw" | jq 'del(.env, .sandbox.env)' 2>&1 || printf '%s\n' "$e2e2_diag_api_raw"; }
e2e2_dg_guest() { timeout -k 5 90 "$B" exec "$1" -- bash -c "$E2E2_GUEST_PROBE"; }
e2e2_dg_server_ssh() { grep -i ssh "$work/logs/server.log" 2>/dev/null | grep -F -- "$1" | tail -n 40; }
e2e2_dg_ssh_log() { tail -n 40 "$work/logs/ssh-$1.out" 2>/dev/null; }
e2e2_dg_nft_filter() { # PATTERN: lines of the ruleset matching PATTERN
  e2e_priv timeout 20 nft list ruleset 2>&1 | grep -n -i -E -- "$1"
}
e2e2_dg_journal_filter() { # ID
  e2e_priv timeout 30 journalctl -u "$_e2e_node_service" --no-pager -n 3000 2>&1 | grep -i -E -- "ssh|ingress|forward|$1"
}
e2e2_dg_serial() { e2e_priv timeout 20 tail -n 200 "$_e2e_node_data_dir/vms/$1/serial.log"; }
e2e2_dg_vmdir() { e2e_priv timeout 20 ls -la "$_e2e_node_data_dir/vms/$1"; }
# One ssh -vvv attempt exactly as the CLI makes it: a PATH shim adds -vvv to the
# ssh the CLI starts, so the endpoint, key and host-key pin are the CLI's own.
e2e2_dg_ssh_vvv() {
  local shim=$e2e2_diag_dir/shim real
  real=$(command -v ssh) || { echo "no ssh in PATH"; return 0; }
  mkdir -p "$shim" && printf '#!/bin/sh\nexec %s -vvv "$@"\n' "$real" >"$shim/ssh" && chmod 700 "$shim/ssh" || return 0
  e2e_client timeout -k 5 30 env "XDG_CONFIG_HOME=$XDG_CONFIG_HOME" "BBRAFT_API_URL=$api_veth" "PATH=$shim:$PATH" \
    "$B" ssh "$1" 'echo ssh-ok'
}

# e2e2_diag_ids prints the sandboxes the failing stage touched: the named ones
# of the scenario and the ones registered since the stage began, still alive.
e2e2_diag_ids() {
  local id seen=" " n=0 cand
  cand=(${s1:-} ${s2:-} ${filler:-} ${forkid:-})
  cand+=("${e2e_ids[@]:$e2e2_stage_ids_from}")
  for id in "${cand[@]}"; do
    [[ $id == bx_* ]] || continue
    case $seen in *" $id "*) continue ;; esac
    seen="$seen$id "
    n=$((n + 1))
    [ "$n" -le "$E2E2_DIAG_MAX_SANDBOXES" ] || break
    printf '%s\n' "$id"
  done
}

e2e2_diag_sandbox() { # ID: everything about one sandbox
  local id=$1 node port ep
  node=$(e2e2_node_of "$id" 2>/dev/null) || node=""
  echo
  echo "=== diag sandbox $id (stage ${stage_name:-?}, node '${node:-?}')"
  e2e2_diag_run "$id: database row" 10 e2e2_sql "SELECT id||' state='||state||' node='||coalesce(node_id,'')||' disk_node='||coalesce(disk_node_id,'') FROM public.sandboxes WHERE id=:'id'" -v id="$id"
  e2e2_diag_run "$id: bbraft info --json" 60 timeout 30 "$B" info "$id" --json
  e2e2_diag_api_raw=$(curl -sS --max-time 20 -H "Authorization: Bearer $api_key" "$api_local/api/v1/sandboxes/$id" 2>&1 || true)
  e2e2_diag_run "$id: API GET /api/v1/sandboxes/$id" 60 e2e2_dg_api
  ep=$(printf '%s' "$e2e2_diag_api_raw" | jq -r '[.. | .sshEndpoint? | select(. != null)] | first // ""' 2>/dev/null || true)
  echo "sshEndpoint of $id: '${ep:-null}'"
  port=${ep##*:}
  e2e2_diag_run "$id: failed ssh attempt output (logs/ssh-$id.out)" 40 e2e2_dg_ssh_log "$id"
  e2e2_diag_run "$id: server log lines about ssh for this id" 40 e2e2_dg_server_ssh "$id"
  if [ -n "${_e2e_node_service:-}" ] && [ -n "${_e2e_node_data_dir:-}" ] && [ "$node" = "$(e2e2_node_id a)" ]; then
    case $port in '' | *[!0-9]*) ;; *) e2e2_diag_run "$id: node a nft lines for port $port (DNAT/forward)" 40 e2e2_dg_nft_filter "dport $port|:$port|dnat" ;; esac
    e2e2_diag_run "$id: node a journal lines about ssh/ingress/forward/this id" 80 e2e2_dg_journal_filter "$id"
    e2e2_diag_run "$id: node a VM directory" 30 e2e2_dg_vmdir "$id"
    e2e2_diag_run "$id: node a serial console (tail 200)" 200 e2e2_dg_serial "$id"
  elif [ "$node" = "$(e2e2_node_id b)" ]; then
    e2e2_diag_run "$id: node b serial console (tail 200)" 200 tail -n 200 "$(e2e2_node_dir b)/data/vms/$id/serial.log"
  fi
  e2e2_diag_run "$id: inside the guest (exec, not ssh)" 150 e2e2_dg_guest "$id"
  if [ -n "${_e2e_node_attempt:-}" ] && declare -F e2e_client >/dev/null && [ "$node" = "$(e2e2_node_id a)" ]; then
    e2e2_diag_run "$id: one ssh -vvv attempt (CLI endpoint and key)" 120 e2e2_dg_ssh_vvv "$id"
  fi
}

# e2e2_diag_node_a: host-wide facts of node a, once.
e2e2_diag_node_a() {
  [ -n "${_e2e_node_service:-}" ] || { echo "--- diag node a: not started, nothing to collect"; return 0; }
  e2e2_diag_run "node a: nft list ruleset" 200 e2e_priv timeout 20 nft list ruleset
  e2e2_diag_run "node a: ip -4 -o addr show" 40 timeout 20 ip -4 -o addr show
  e2e2_diag_run "node a: ip route" 40 timeout 20 ip route
  e2e2_diag_run "node a: ip -o link show" 40 timeout 20 ip -o link show
  e2e2_diag_run "node a: unit journal (tail 200)" 200 e2e_priv timeout 30 journalctl -u "$_e2e_node_service" --no-pager -n 200
}

# e2e2_diag_control: a freshly created sandbox on node a, probed the same way,
# when node a has a free slot (it offers one running small sandbox; with that
# slot taken a control cannot exist and the reason is printed).
e2e2_diag_control() {
  local busy id out
  [ -n "${_e2e_node_service:-}" ] || return 0
  busy=$(e2e2_sql "SELECT count(*) FROM public.sandboxes WHERE node_id=:'n' AND deleted_at IS NULL AND state IN ('ready','idle','running','starting','creating')" -v n="$(e2e2_node_id a)" 2>/dev/null) || busy=""
  if [ "$busy" != 0 ]; then
    echo "--- diag control sandbox: skipped, node a has ${busy:-?} active sandbox(es) (its one running slot is taken)"
    return 0
  fi
  out=$e2e2_diag_dir/control-new.out
  if ! timeout -k 5 240 "$B" new --type small --name diag-control --ttl 900 --json >"$out" 2>&1; then
    echo "--- diag control sandbox: creation failed:"; tail -n 5 "$out" | e2e_redact
    return 0
  fi
  id=$(jq -r 'select(.event == "ready") | .id' "$out" 2>/dev/null | tail -1)
  [[ $id == bx_* ]] || { echo "--- diag control sandbox: no id"; return 0; }
  e2e_ids+=("$id") # the teardown deletes it
  echo "--- diag control sandbox $id (fresh, on node '$(e2e2_node_of "$id" 2>/dev/null)')"
  e2e2_diag_sandbox "$id"
}

e2e2_diag_collect() {
  local id
  set +e
  [ -d "${work:-}" ] || { echo "diag: no work directory yet, nothing to collect"; return 0; }
  e2e2_diag_dir=$E2E_OUT/diag-$(basename "$work")
  mkdir -p "$e2e2_diag_dir" && chmod 700 "$e2e2_diag_dir" || { echo "diag: cannot create $e2e2_diag_dir"; return 0; }
  e2e2_diag_deadline=$(($(date +%s) + E2E2_DIAG_BUDGET))
  printf '\n=== diagnostics of the failed stage %s (E2E2_DIAG_ON_FAIL=1; redacted; saved in %s)\n' "${stage_name:-?}" "$e2e2_diag_dir"
  if [ "${api_started:-0}" = 1 ] && [ -x "${B:-}" ]; then
    for id in $(e2e2_diag_ids); do e2e2_diag_sandbox "$id"; done
  else
    echo "diag: the API was not up; no sandbox to inspect"
  fi
  e2e2_diag_node_a
  [ "${api_started:-0}" = 1 ] && [ -x "${B:-}" ] && e2e2_diag_control
  printf '=== end of diagnostics (%s files in %s)\n' "$e2e2_diag_n" "$e2e2_diag_dir"
  return 0
}

e2e2_diag_on_fail() {
  [ "${E2E2_DIAG_ON_FAIL:-}" = 1 ] || return 0
  e2e2_diag_collect || true
}

# ---------------------------------------------------------------------------
# Stages. Each is called at top level by e2e2_run_stages (never under
# if/&&/||), so errexit stays active inside.
# ---------------------------------------------------------------------------
stage_preflight() {
  e2e_stage "preflight"
  repo=$PWD
  [ -f "$repo/go.mod" ] || { echo "run e2e-p2.sh from the repository root" >&2; exit 2; }
  B=$repo/bin/bbraft
  mkdir -p "$HOME/.bbk"
  work=$(mktemp -d "$HOME/.bbk/e2e-p2.XXXXXXXX") # short: QMP socket paths < 107 bytes
  trap e2e_cleanup EXIT
  mkdir -p "$repo/.cache"
  helpers=$(mktemp -d "$repo/.cache/e2e-p2.XXXXXXXX")
  mkdir -p "$work"/{secrets,logs,capture,cfg}
  chmod 700 "$work" "$work/secrets" "$work/capture"
  E2E_ERRLOG=$work/logs/err.log
  export E2E_ERRLOG
  : >"$E2E_ERRLOG"
  export XDG_CONFIG_HOME=$work/cfg
  node_helper=$repo/deploy/dev/e2e-p1-node.sh
  [ -f "$node_helper" ] || e2e_fail "node helper $node_helper missing"
  # shellcheck source=deploy/dev/e2e-p1-node.sh
  source "$node_helper"
  local fn tool ev
  for fn in e2e_node_start e2e_node_wait_registered e2e_node_wait_ready e2e_client e2e_node_stop e2e_node_leak_proof; do
    declare -F "$fn" >/dev/null || e2e_fail "node helper lacks $fn"
  done
  for tool in ssh scp script timeout jq curl psql garage python3 go make pgrep pkill setsid; do
    command -v "$tool" >/dev/null 2>&1 || e2e_fail "required program '$tool' is not installed (ssh/scp: openssh-client; garage: the pinned binary)"
  done
  for ev in E2E_NODE_HOST_VETH_ADDR E2E_NODE_SSH_INGRESS_ADDR E2E_NODE_SSH_INGRESS_IFACE \
    E2E_NODE_CLIENT_ADDR E2E_NODE_CLIENT_NETNS E2E_NODE_GUEST_PREFIX E2E_NODE_GUEST_GATEWAY; do
    [ -n "${!ev:-}" ] || e2e_fail "node helper did not export planned-topology $ev at source time"
  done
  e2e2_prefix_regex "$E2E_NODE_GUEST_PREFIX" >/dev/null 2>&1 || e2e_fail "guest prefix $E2E_NODE_GUEST_PREFIX is not on an octet boundary"
  # A host address inside the guest prefix would be handed to a guest.
  local chk
  chk=$(e2e_node_guest_prefix_preflight 2>&1) || e2e_fail "$chk"
  node_helper_sourced=1
  # Node b runs QEMU as this user: it needs the KVM and vsock devices.
  [ -r /dev/kvm ] && [ -w /dev/kvm ] || e2e_fail "/dev/kvm is not readable and writable by $(id -un) (node b runs unjailed QEMU)"
  [ -r /dev/vhost-vsock ] && [ -w /dev/vhost-vsock ] || e2e_fail "/dev/vhost-vsock is not readable and writable by $(id -un) (node b needs it)"
  [ -n "${BBRAFT_TEST_DATABASE_URL:-}" ] || e2e_fail "BBRAFT_TEST_DATABASE_URL not set"
  # Node b's capacity and overcommit factors from this host (printed here).
  local facts plan
  facts=$(e2e2_host_facts "$HOME/.bbk") || e2e_fail "cannot read the host's CPUs, memory and free disk"
  # shellcheck disable=SC2086 # facts is five plain integers
  plan=$(e2e2_capacity_plan $facts 2>&1) || e2e_fail "$plan"
  # shellcheck disable=SC2086
  e2e2_capacity_plan $facts >/dev/null
  printf '%s\n' "$plan"
  api_local=http://127.0.0.1:$E2E_PROXY_PORT
  api_veth=http://${E2E_NODE_HOST_VETH_ADDR:?}:$E2E_PROXY_PORT
  export BBRAFT_API_URL=$api_local
  api_url=$api_veth
  e2e_pass
}

stage_build() {
  e2e_stage "build"
  make build >"$work/logs/build.log" 2>&1 || e2e_fail "make build (see logs/build.log)"
  local b help
  for b in bbraft bbraft-server bbraft-node; do
    [ -x "$repo/bin/$b" ] || e2e_fail "bin/$b missing"
  done
  # Fail here, with a clear message, instead of at a QEMU CID clash later.
  help=$("$repo/bin/bbraft-node" -h 2>&1 || true)
  grep -q -- '-s3.bucket' <<<"$help" || e2e_fail "bbraft-node has no object-store flags (-s3.*): this script needs the integrated branch"
  local -a extra
  local flag
  e2e2_nodeb_flags extra
  extra+=(-node.cpu-overcommit -node.memory-overcommit -node.disk-overcommit)
  for flag in "${extra[@]:-}"; do
    case $flag in -node.*) ;; *) continue ;; esac
    grep -q -- "$flag" <<<"$help" \
      || e2e_fail "bbraft-node does not know node b's flag $flag: this script needs the integrated branch (two nodes on one host need disjoint vsock CID ranges, so -node.vsock-cid-range; node b is sized with the -node.*-overcommit flags; E2E_NODEB_EXTRA_FLAGS overrides the CID flag)"
  done
  e2e_pass
}

# e2e2_pinned_image_ok DIR succeeds only for the pinned image of this
# checkout: images/verify.sh passes (manifest schema and id equal to the
# directory name, every artifact's sha256 and size, qcow2 shape), the manifest
# id is IMAGE_VERSION without the test-only marker, the distribution snapshot
# is the pinned UBUNTU_SNAPSHOT with no mirror override, and the recorded Go
# and Node versions are the pinned ones. The last output line is the reason.
# Needs images/pins.sh sourced (IMAGE_VERSION, UBUNTU_SNAPSHOT, GO_VERSION,
# NODE_VERSION) and $repo.
e2e2_pinned_image_ok() {
  local dir=$1 m=$1/manifest.json got
  bash "$repo/images/verify.sh" "$dir" || { echo "verify.sh refused the image"; return 1; }
  got=$(jq -r '.id' "$m") || { echo "manifest unreadable"; return 1; }
  [ "$got" = "$IMAGE_VERSION" ] || { echo "manifest id '$got' is not the pinned $IMAGE_VERSION"; return 1; }
  case $got in *unpinned*) echo "test-only image id '$got'"; return 1 ;; esac
  got=$(jq -r '.distribution.snapshot // ""' "$m")
  [ "$got" = "$UBUNTU_SNAPSHOT" ] || { echo "image snapshot '$got' is not the pinned $UBUNTU_SNAPSHOT"; return 1; }
  [ -z "$(jq -r '.distribution.mirror // ""' "$m")" ] || { echo "image records a mirror override"; return 1; }
  got=$(jq -r '.toolchain.go // ""' "$m")
  [ "$got" = "$GO_VERSION" ] || { echo "image Go '$got' is not the pinned $GO_VERSION"; return 1; }
  got=$(jq -r '.toolchain.node // ""' "$m")
  [ "$got" = "$NODE_VERSION" ] || { echo "image Node '$got' is not the pinned $NODE_VERSION"; return 1; }
  echo "pinned image $IMAGE_VERSION verified"
}

stage_image() {
  e2e_stage "image"
  local t0
  t0=$(date +%s)
  # shellcheck source=/dev/null
  source "$repo/images/pins.sh"
  [ -n "${IMAGE_VERSION:-}" ] || e2e_fail "images/pins.sh did not set IMAGE_VERSION"
  img_dir=$repo/images/out/$IMAGE_VERSION
  if [ "${E2E_REUSE_IMAGE:-}" = 1 ]; then
    # Explicit override: never build, reuse or fail.
    [ -d "$img_dir" ] || e2e_fail "E2E_REUSE_IMAGE=1 but images/out/$IMAGE_VERSION does not exist"
    e2e2_pinned_image_ok "$img_dir" >"$work/logs/image-verify.log" 2>&1 \
      || e2e_fail "E2E_REUSE_IMAGE=1: $(tail -1 "$work/logs/image-verify.log") (see logs/image-verify.log)"
    echo "reusing verified pinned image $IMAGE_VERSION (E2E_REUSE_IMAGE=1, not rebuilt)"
  elif [ -d "$img_dir" ]; then
    # Present already: images/publish.sh never replaces a published version,
    # so a rebuild would fail. Reuse it only when it is provably the pinned
    # image; anything else is refused with the reason, never silently used.
    e2e2_pinned_image_ok "$img_dir" >"$work/logs/image-verify.log" 2>&1 \
      || e2e_fail "images/out/$IMAGE_VERSION exists but is not the verified pinned image: $(tail -1 "$work/logs/image-verify.log") (see logs/image-verify.log); remove that directory to rebuild"
    echo "reusing the pinned image $IMAGE_VERSION already present (verified, not rebuilt)"
  else
    make -C images image >"$work/logs/image-build.log" 2>&1 || e2e_fail "image build (see logs/image-build.log)"
    [ -d "$img_dir" ] || e2e_fail "images/out/$IMAGE_VERSION missing"
  fi
  echo "image $IMAGE_VERSION ready in $(( $(date +%s) - t0 ))s"
  e2e_pass
}

stage_helpers() {
  e2e_stage "helpers+baseline"
  e2e_write_go_helpers "$helpers"
  [ -z "$(gofmt -l "$helpers")" ] || e2e_fail "generated helpers not gofmt-clean"
  go build -o "$work/e2e-p1-proxy" "./${helpers#"$repo"/}/proxy" || e2e_fail "proxy build"
  go build -o "$work/pgenv" "./${helpers#"$repo"/}/pgenv" || e2e_fail "pgenv build"
  go test -run TestNothing "./${helpers#"$repo"/}/validator" -count=1 >/dev/null || e2e_fail "validator compile"
  pgrep -af "$E2E_PROC_RE" >"$work/baseline.procs.raw" || true
  sort "$work/baseline.procs.raw" >"$work/baseline.procs"
  ip -o link >"$work/baseline.links.raw" || e2e_fail "baseline ip link collection"
  sort "$work/baseline.links.raw" >"$work/baseline.links"
  e2e_priv nft list tables >"$work/baseline.nft.raw" || e2e_fail "baseline nft collection"
  sort "$work/baseline.nft.raw" >"$work/baseline.nft"
  e2e_unit_names >"$work/baseline.units" || e2e_fail "baseline unit collection"
  e2e_pass
}

stage_services() {
  e2e_stage "services"
  umask 077
  head -c 32 /dev/urandom >"$work/secrets/master.key"
  head -c 32 /dev/urandom | od -An -tx1 | tr -d ' \n' >"$work/secrets/gateway.token"
  local inst token sha nodes_json=""
  for inst in a b; do
    token=$(head -c 32 /dev/urandom | od -An -tx1 | tr -d ' \n')
    printf %s "$token" >"$(e2e2_node_token_file "$inst")"
    sha=$(printf %s "$token" | sha256sum | cut -d' ' -f1)
    nodes_json="${nodes_json:+$nodes_json,}{\"id\":\"$(e2e2_node_id "$inst")\",\"tokenSha256\":\"$sha\"}"
    printf '%s\n' "$token" >>"$work/secrets/redact.list"
  done
  unset token
  printf '{"nodes":[%s]}' "$nodes_json" >"$work/secrets/nodes.json"
  api_key="bmb_$(head -c 24 /dev/urandom | od -An -tx1 | tr -d ' \n')"
  local owner
  owner="e2e-$(date +%s)-$RANDOM"
  printf '{"keys":[{"id":"main","owner":"%s","key":"%s"}]}' "$owner" "$api_key" >"$work/secrets/keys.json"
  printf %s "$BBRAFT_TEST_DATABASE_URL" >"$work/secrets/database-url-base"
  local master_hex
  master_hex=$(od -An -tx1 <"$work/secrets/master.key" | tr -d ' \n')
  { printf '%s\n' "$api_key" "$(cat "$work/secrets/gateway.token")" \
      "$BBRAFT_TEST_DATABASE_URL" "$master_hex"; } >>"$work/secrets/redact.list"
  chmod 600 "$work/secrets/redact.list"
  export E2E_REDACT_FILE=$work/secrets/redact.list
  unset master_hex
  e2e_new_database || e2e_fail "owned database creation"
  e2e2_garage_start

  setsid "$repo/bin/bbraft-server" -roles all \
    -server.listen 127.0.0.1:7710 -node.listen 127.0.0.1:7701 \
    -server.public-url "$api_veth" \
    -database.url-file "$work/secrets/database-url" \
    -secrets.master-key-file "$work/secrets/master.key" \
    -node.gateway-token-file "$work/secrets/gateway.token" \
    -auth.keys-file "$work/secrets/keys.json" \
    -node.tokens-file "$work/secrets/nodes.json" \
    -s3.endpoint "$s3_endpoint" -s3.region "$E2E2_GARAGE_REGION" -s3.bucket "$E2E2_GARAGE_BUCKET" \
    -s3.path-style -s3.access-key-id "$s3_key_id" \
    -s3.secret-access-key-file "$work/secrets/s3-secret" \
    -lifecycle.snapshot-interval-seconds "$E2E2_SNAPSHOT_INTERVAL" \
    -lifecycle.node-lost-seconds "$E2E2_NODE_LOST" \
    -lifecycle.reaper-seconds 5 -lifecycle.retry-seconds 2 -lifecycle.job-poll-seconds 5 \
    >"$work/logs/server.log" 2>&1 &
  server_pid=$!
  setsid "$work/e2e-p1-proxy" -listen "0.0.0.0:$E2E_PROXY_PORT" -upstream 127.0.0.1:7710 \
    -capture "$work/capture" >"$work/logs/proxy.log" 2>&1 &
  proxy_pid=$!
  e2e_wait_for 60 "server answers /me (direct)" \
    curl -fsS -o /dev/null "http://127.0.0.1:7710/api/v1/me" -H "Authorization: Bearer $api_key"
  e2e_wait_for 60 "proxy answers /me on loopback" \
    curl -fsS -o /dev/null "$api_local/api/v1/me" -H "Authorization: Bearer $api_key"
  e2e_wait_for 60 "node-link listener answers on 127.0.0.1:7701" \
    bash -c 'exec 3<>/dev/tcp/127.0.0.1/7701'
  printf %s "$api_key" | e2e_ok "$B" login --key-stdin --json
  local code
  code=$(api_curl GET /me)
  [ "$code" = 200 ] || e2e_fail "GET /me: HTTP $code"
  owner_uuid=$(jq -r '.user.id // empty' "$work/logs/last-api.out")
  [ -n "$owner_uuid" ] || e2e_fail "no .user.id from /me"
  api_started=1
  e2e_pass
}

# Node b first, alone: every sandbox created before node a exists has its
# disk written on b.
stage_node_b() {
  e2e_stage "node-b"
  e2e2_nodeb_start
  e2e2_poll 180 e2e2_nodeb_registered \
    || e2e_fail "node b did not register within 180s (tail: $(tail -3 "$(e2e2_node_dir b)/node.log" | e2e_redact | cut -c1-300))"
  e2e_pass
}

stage_write_snapshot() {
  e2e_stage "write-snapshot"
  s1=$(e2e2_create e2e-s1)
  e2e2_wait_ready "$s1" "$(e2e2_node_id b)" 240
  [ "$(e2e2_node_of "$s1")" = "$(e2e2_node_id b)" ] || e2e_fail "s1 is not on node b"
  e2e2_write "$s1" "$E2E2_FILE" file-one
  local wrote
  wrote=$(date +%s)
  e2e2_wait_gen_after "$s1" "$wrote" 180
  echo "snapshot generation newer than the write exists for $s1 (interval ${E2E2_SNAPSHOT_INTERVAL}s)"
  e2e_pass
}

stage_node_a() {
  e2e_stage "node-a"
  e2e2_s3_stage || e2e_fail "staging node a's object-store files"
  ip_forward_prev=$(e2e_ipf_read) || e2e_fail "read net.ipv4.ip_forward"
  ip_forward_set=1
  e2e_ipf_write 1 || e2e_fail "enable net.ipv4.ip_forward"
  echo "ip_forward: prior=$ip_forward_prev enabled=1 (restored and verified in teardown)"
  node_parent=$(mktemp -d "$work/node-parent.XXXXXXXX")
  export BBRAFT_TEST_VM_SETUP=1
  export E2E_NODE_EXTRA_VARS_FILE=$a_vars
  node_started=1
  e2e_node_start "$repo/bin/bbraft-node" "$img_dir" "$(e2e2_node_id a)" \
    "127.0.0.1:7701" "$(e2e2_node_token_file a)" "$node_parent/node"
  e2e_node_wait_ready 300
  local ev
  for ev in E2E_NODE_ID E2E_NODE_IMAGE_ID E2E_NODE_EXPECTED_VCPUS \
    E2E_NODE_EXPECTED_MEMORY_MIB E2E_NODE_EXPECTED_DISK_GIB \
    E2E_NODE_ADMITTED_QEMU_VERSION E2E_NODE_START_EPOCH E2E_NODE_READY_EPOCH; do
    [ -n "${!ev:-}" ] || e2e_fail "node helper did not export runtime $ev after wait_ready"
  done
  e2e_node_wait_registered e2e_node_ready_cb
  e2e_pass
}

stage_node_loss() {
  e2e_stage "node-loss"
  local b a t0 elapsed
  a=$(e2e2_node_id a); b=$(e2e2_node_id b)
  e2e_ok "$B" exec "$s1" -- bash -lc "printf '%s\n' file-two > /home/user/e2e2.txt && sync"
  # Victim: node b (a plain process: a SIGKILL needs no unit surgery, and
  # keeping a alive keeps the jailed TAP node under the final leak proof).
  e2e2_nodeb_kill9
  t0=$(date +%s)
  echo "node b killed with SIGKILL; QEMU processes of b still running: $(e2e2_nodeb_qemu_count) (informational)"
  e2e2_wait_ready "$s1" "$a" "$E2E2_MOVE_BUDGET"
  elapsed=$(( $(date +%s) - t0 ))
  echo "s1 ready on node a ${elapsed}s after the kill (grace ${E2E2_NODE_LOST}s, budget ${E2E2_MOVE_BUDGET}s)"
  [ "$(e2e2_disk_node_of "$s1")" = "$a" ] || e2e_fail "s1's disk is not on node a after the move"
  e2e2_expect_file "$s1" "$E2E2_FILE" file-one
  if e2e_run_rc "$work/logs/f2.out" "$B" exec "$s1" -- cat /home/user/e2e2.txt && [ "$E2E_RC" = 0 ] && grep -q file-two "$work/logs/f2.out"; then
    echo "informational: the write right before the kill SURVIVED (a generation was cut in between)"
  else
    echo "informational: the write right before the kill was lost (at most one snapshot interval, ${E2E2_SNAPSHOT_INTERVAL}s, may be lost)"
  fi
  e2e2_tap_check "$s1"
  local st
  st=$(e2e2_eviction_state "$b" "$s1") || e2e_fail "eviction state query"
  [ "${st%% *}" -ge 1 ] || e2e_fail "no vm_evictions row for the stale copy of $s1 on node b"
  e2e_ok "$B" list --all --json
  e2e_jq --arg id "$s1" '(.sandboxes | map(select(.id == $id)) | length) == 1' "$work/logs/last.out"
  e2e_pass
}

stage_node_returns() {
  e2e_stage "node-returns"
  local a b
  a=$(e2e2_node_id a); b=$(e2e2_node_id b)
  e2e2_nodeb_start
  e2e2_poll 180 e2e2_nodeb_registered || e2e_fail "node b did not register again within 180s"
  e2e2_poll 240 e2e2_eviction_done "$b" "$s1" \
    || e2e_fail "the stale copy of $s1 on node b was not deleted (eviction state: $(e2e2_eviction_state "$b" "$s1"))"
  e2e2_poll 60 e2e2_nodeb_no_qemu \
    || e2e_fail "QEMU of the stale copy still runs on node b"
  [ "$(e2e2_node_of "$s1")" = "$a" ] || e2e_fail "s1 moved away from node a"
  e2e_ok "$B" list --all --json
  e2e_jq --arg id "$s1" '(.sandboxes | length) == 1 and .sandboxes[0].id == $id' "$work/logs/last.out"
  e2e2_expect_file "$s1" "$E2E2_FILE" file-one
  e2e_pass
}

# A sandbox stopped on b whose node is then lost resumes on a (a disk last
# written on b lands on a; guest networking checked there).
stage_lost_disk_resume() {
  e2e_stage "stop-resume-lost-disk-node"
  local a b wrote t0
  a=$(e2e2_node_id a); b=$(e2e2_node_id b)
  # a's one running slot is taken by s1, so s2 is created on b.
  s2=$(e2e2_create e2e-s2)
  e2e2_wait_ready "$s2" "$b" 240
  e2e2_write "$s2" "$E2E2_FILE" file-five
  wrote=$(date +%s)
  e2e2_wait_gen_after "$s2" "$wrote" 180
  e2e_ok "$B" stop "$s2" --json
  e2e2_wait_stopped "$s2"
  [ "$(e2e2_disk_node_of "$s2")" = "$b" ] || e2e_fail "stopped s2's disk is not on node b"
  e2e2_nodeb_kill9
  t0=$(date +%s)
  e2e2_poll "$E2E2_MOVE_BUDGET" e2e2_disk_released "$s2" \
    || e2e_fail "s2's disk was not released from the lost node within ${E2E2_MOVE_BUDGET}s"
  echo "s2 disk released from lost node b $(( $(date +%s) - t0 ))s after the kill"
  # Room for s2 on a: s1 goes (its data is not needed any more).
  e2e2_delete "$s1"
  e2e_ok "$B" resume "$s2" --json
  e2e2_wait_ready "$s2" "$a" 300
  e2e2_expect_file "$s2" "$E2E2_FILE" file-five
  e2e2_tap_check "$s2"
  # b returns; its copy of s2 goes.
  e2e2_nodeb_start
  e2e2_poll 180 e2e2_nodeb_registered || e2e_fail "node b did not register again within 180s"
  e2e2_poll 240 e2e2_eviction_done "$b" "$s2" \
    || e2e_fail "the stale copy of $s2 on node b was not deleted (eviction state: $(e2e2_eviction_state "$b" "$s2"))"
  e2e_pass
}

# The opposite direction: a sandbox that last ran on a is stopped, a is made
# unable to run it (a filler takes a's one running slot while b is out of the
# way), and the resume lands on b with the data.
stage_a_to_b_resume() {
  e2e_stage "stop-resume-a-to-b"
  local a b
  a=$(e2e2_node_id a); b=$(e2e2_node_id b)
  e2e2_write "$s2" /home/user/e2e-a.txt written-on-a
  e2e_ok "$B" stop "$s2" --json
  e2e2_wait_stopped "$s2"
  [ "$(e2e2_disk_node_of "$s2")" = "$a" ] || e2e_fail "stopped s2's disk is not on node a"
  # Node b out of the way (clean stop: it holds no VM), so the filler can only
  # be placed on a.
  e2e2_nodeb_stop || e2e_fail "node b did not stop cleanly"
  e2e2_poll 120 e2e2_node_stale "$b" \
    || e2e_fail "node b was not marked stale after its stop"
  filler=$(e2e2_create e2e-filler)
  e2e2_wait_ready "$filler" "$a" 240
  e2e2_nodeb_start
  e2e2_poll 180 e2e2_nodeb_registered || e2e_fail "node b did not register again within 180s"
  e2e_ok "$B" resume "$s2" --json
  e2e2_wait_ready "$s2" "$b" 300
  e2e2_expect_file "$s2" "$E2E2_FILE" file-five
  e2e2_expect_file "$s2" /home/user/e2e-a.txt written-on-a
  echo "s2 resumed on node b from a disk last run on node a: data intact (b has no guest networking: data check only)"
  e2e_pass
}

stage_fork() {
  e2e_stage "fork"
  local code n_before
  n_before=$(e2e2_node_of "$s2")
  code=$(api_curl POST "/sandboxes/$s2/fork" '{}')
  [ "$code" = 202 ] || e2e_fail "fork: HTTP $code"
  forkid=$(jq -r '.id // empty' "$work/logs/last-api.out")
  [ -n "$forkid" ] && [[ $forkid == bx_* ]] || e2e_fail "fork: no bx_ id"
  e2e_ids+=("$forkid")
  [ "$forkid" != "$s2" ] || e2e_fail "fork returned the source id"
  e2e2_wait_ready "$forkid" "" 300
  echo "fork $forkid ready on $(e2e2_node_of "$forkid") (source on $n_before)"
  e2e2_expect_file "$forkid" "$E2E2_FILE" file-five
  e2e2_write "$forkid" /home/user/fork-only.txt fork-only
  # Source unchanged: same node, still ready, same data, nothing from the fork.
  [ "$(e2e2_node_of "$s2")" = "$n_before" ] || e2e_fail "the source moved during the fork"
  e2e2_ready_on "$s2" "$n_before" || e2e_fail "the source is not ready after the fork"
  e2e2_expect_file "$s2" "$E2E2_FILE" file-five
  e2e_run_rc "$work/logs/fork-only.out" "$B" exec "$s2" -- test -e /home/user/fork-only.txt
  [ "$E2E_RC" != 0 ] || e2e_fail "the fork's file shows up in the source"
  e2e_pass
}

stage_saved_count() {
  e2e_stage "saved-count"
  local code a b i id n_a n_b
  a=$(e2e2_node_id a); b=$(e2e2_node_id b)
  e2e_ok "$B" snapshot "$s2" e2e-base
  code=$(api_curl GET /saved-snapshots)
  [ "$code" = 200 ] || e2e_fail "list saved snapshots: HTTP $code"
  e2e_jq '[.. | objects | .name? // empty] | index("e2e-base") != null' "$work/logs/last-api.out"
  # Free every slot: the new sandboxes only need the saved snapshot.
  e2e2_delete "$forkid"
  e2e2_delete "$filler"
  e2e2_delete "$s2"
  code=$(api_curl POST /sandboxes '{"from":"e2e-base","count":10,"type":"small","ttlSeconds":7200}')
  [ "$code" = 202 ] || e2e_fail "create from e2e-base count 10: HTTP $code"
  mapfile -t e2e2_count_ids < <(jq -r '.sandboxes[]?.id' "$work/logs/last-api.out")
  [ "${#e2e2_count_ids[@]}" = 10 ] || e2e_fail "count 10 returned ${#e2e2_count_ids[@]} sandboxes"
  for id in "${e2e2_count_ids[@]}"; do e2e_ids+=("$id"); done
  for id in "${e2e2_count_ids[@]}"; do
    e2e2_wait_ready "$id" "" 600
  done
  for id in "${e2e2_count_ids[@]}"; do
    e2e2_expect_file "$id" "$E2E2_FILE" file-five
  done
  n_a=0; n_b=0
  for id in "${e2e2_count_ids[@]}"; do
    case $(e2e2_node_of "$id") in
      "$a") n_a=$((n_a + 1)) ;;
      "$b") n_b=$((n_b + 1)) ;;
      *) e2e_fail "$id is on neither node" ;;
    esac
  done
  echo "10 sandboxes from e2e-base: $n_a on node a, $n_b on node b"
  [ "$n_a" -ge 1 ] && [ "$n_b" -ge 1 ] || e2e_fail "the 10 sandboxes are not spread over both nodes ($n_a on a, $n_b on b)"
  e2e_pass
}

stage_validate_live() {
  e2e_stage "validate-api-v1-live"
  if ! e2e_validate live "$work/logs/validate-live.log"; then e2e_fail "compat validation (live preflight; see validate-live.log)"; fi
  e2e_pass
}

stage_teardown_proof() {
  e2e_stage "delete-all+leaks"
  local id leftover
  e2e_ok "$B" list --all --json
  for leftover in $(jq -r '.sandboxes[]?.id' "$work/logs/last.out"); do
    case " ${e2e_ids[*]:-} " in *" $leftover "*) ;; *) e2e_ids+=("$leftover");; esac
  done
  for id in "${e2e_ids[@]:-}"; do
    [ -n "$id" ] || continue
    e2e_ok "$B" delete "$id" --yes --wait
  done
  e2e_ok "$B" list --all --json
  e2e_jq '(.sandboxes | length) == 0' "$work/logs/last.out"
  if ! e2e2_reservation_proof; then e2e_fail "reservation proof"; fi
  e2e2_nodeb_stop || e2e_fail "node b leaked state"
  e2e_node_stop
  e2e_node_leak_proof
  node_started=0
  if ! e2e_stop_proxy; then e2e_fail "proxy drain"; fi
  if ! e2e_validate final "$work/logs/validate-final.log"; then e2e_fail "compat validation (final; see validate-final.log)"; fi
  if ! e2e_stop_server; then e2e_fail "server did not stop"; fi
  e2e2_garage_stop || e2e_fail "garage did not stop"
  e2e2_s3_unstage || e2e_fail "node a's object-store files were not removed"
  if ! e2e_leak_scan; then e2e_fail "leaks found (see above)"; fi
  e2e_body_complete=1
  e2e_pass
}

e2e2_run_stages() {
  local s
  for s in "${E2E2_STAGES[@]}"; do
    e2e2_stage_ids_from=${#e2e_ids[@]}
    "$s"
  done
}

# ---------------------------------------------------------------------------
# Self-test. Children source this script as a library with fakes; nothing
# here starts a server, a node, Garage or touches the network.
# ---------------------------------------------------------------------------
e2e2_selftest() {
  local out self=${BASH_SOURCE[0]} child rc
  mkdir -p "$E2E_OUT"
  out=$E2E_OUT/e2e-p2-selftest
  rm -rf "$out"; mkdir -p "$out"
  echo "selftest artifacts: $out"
  fail() { echo "SELFTEST FAIL: $*"; exit 1; }

  # 1. Syntax of this script and the node driver.
  bash -n "$self" || fail "syntax of e2e-p2.sh"
  bash -n "$e2e2_here/e2e-p1-node.sh" || fail "syntax of e2e-p1-node.sh"
  echo "PASS syntax"

  # 2. Two node instances get distinct names, and bad instances are refused.
  child=$out/names.sh
  cat >"$child" <<'CHILD'
set -euo pipefail; set -E
E2E_P2_LIB=1 source "$1"
work=/w
declare -A seen=()
for fn in e2e2_node_id e2e2_node_token_file e2e2_node_dir; do
  for inst in a b; do
    v=$("$fn" "$inst")
    [ -z "${seen[$v]:-}" ] || { echo "duplicate name $v"; exit 3; }
    seen[$v]=1
  done
done
[[ $(e2e2_node_id a) =~ ^[a-z0-9][a-z0-9_-]{0,62}$ ]] || exit 4
if e2e2_node_id c 2>/dev/null; then exit 5; fi
[ "$(e2e2_node_dir b)" != "$(e2e2_node_dir a)" ] || exit 6
CHILD
  e2e_run_rc "$out/names.out" bash "$child" "$self"
  [ "$E2E_RC" = 0 ] || { cat "$out/names.out"; fail "instance names (rc $E2E_RC)"; }
  echo "PASS distinct-instance-names"

  # 3. Stage order and the runner: every listed stage is a function, the list
  #    is the documented order, and the runner calls them in that order.
  child=$out/order.sh
  cat >"$child" <<'CHILD'
set -euo pipefail; set -E
E2E_P2_LIB=1 source "$1"
want="stage_preflight stage_build stage_image stage_helpers stage_services stage_node_b stage_write_snapshot stage_node_a stage_node_loss stage_node_returns stage_lost_disk_resume stage_a_to_b_resume stage_fork stage_saved_count stage_validate_live stage_teardown_proof"
[ "${E2E2_STAGES[*]}" = "$want" ] || { echo "order: ${E2E2_STAGES[*]}"; exit 3; }
for s in "${E2E2_STAGES[@]}"; do declare -F "$s" >/dev/null || { echo "no function $s"; exit 4; }; done
calls=""
for s in "${E2E2_STAGES[@]}"; do eval "$s() { calls=\"\$calls $s\"; }"; done
e2e2_run_stages
[ "${calls# }" = "$want" ] || { echo "ran: $calls"; exit 5; }
CHILD
  e2e_run_rc "$out/order.out" bash "$child" "$self"
  [ "$E2E_RC" = 0 ] || { cat "$out/order.out"; fail "stage order (rc $E2E_RC)"; }
  echo "PASS stage-order"

  # 4. Teardown on a failing stage, redaction, and the PASS line only after
  #    the teardown: fakes for every process, node, Garage and root action.
  child=$out/flow.sh
  cat >"$child" <<'CHILD'
set -euo pipefail; set -E
E2E_P2_LIB=1 source "$1"
d=$2; MODE=$3; CALLS=$d/calls; export CALLS; : >"$CALLS"
work=$d/work; mkdir -p "$work/logs" "$work/secrets" "$work/n-b"
helpers=$d/helpers
printf '%s\n' 'FAKE-SECRET-VALUE-123' >"$work/secrets/redact.list"
export E2E_REDACT_FILE=$work/secrets/redact.list
E2E_ERRLOG=$work/logs/err.log; : >"$E2E_ERRLOG"
nodeb_started=1; node_started=1; garage_pid=4242424; s3_root=/etc/bbraft-e2e-s3-fake
nodeb_pid=""
e2e_node_stop() { echo node-stop >>"$CALLS"; }
e2e_node_leak_proof() { echo leak-proof >>"$CALLS"; }
e2e2_nodeb_stop() { echo nodeb-stop >>"$CALLS"; nodeb_started=0; }
e2e2_garage_stop() { echo garage-stop >>"$CALLS"; garage_pid=""; }
e2e2_s3_unstage() { echo s3-unstage >>"$CALLS"; s3_root=""; }
e2e_leak_scan() { echo leak-scan >>"$CALLS"; }
e2e_dump() { :; }
trap e2e_cleanup EXIT
stage_a() { e2e_stage first; echo "stage a ran" >>"$CALLS"; e2e_pass; }
stage_b() {
  e2e_stage second
  if [ "$MODE" = fail ]; then e2e_fail "boom with FAKE-SECRET-VALUE-123 inside"; fi
  e2e_pass
}
stage_c() { e2e_stage third; echo "stage c ran" >>"$CALLS"; e2e_pass; }
E2E2_STAGES=(stage_a stage_b stage_c)
e2e2_run_stages
e2e_pass_pending=1
CHILD
  mkdir -p "$out/flow-fail" "$out/flow-ok"
  e2e_run_rc "$out/flow-fail.out" bash "$child" "$self" "$out/flow-fail" fail
  [ "$E2E_RC" = 1 ] || { cat "$out/flow-fail.out"; fail "failing stage exit $E2E_RC, want 1"; }
  grep -q 'FAIL second' "$out/flow-fail.out" || fail "no FAIL line for the failing stage"
  grep -q 'E2E PASS' "$out/flow-fail.out" && fail "E2E PASS printed after a failure"
  grep -q 'FAKE-SECRET-VALUE-123' "$out/flow-fail.out" && fail "fake secret leaked into the output"
  grep -q 'stage c ran' "$out/flow-fail/calls" && fail "a stage ran after the failing one"
  for call in nodeb-stop node-stop leak-proof garage-stop s3-unstage; do
    grep -q "^$call\$" "$out/flow-fail/calls" || fail "teardown after a failing stage did not call $call"
  done
  [ "$(grep -n '^nodeb-stop$' "$out/flow-fail/calls" | cut -d: -f1)" -lt "$(grep -n '^node-stop$' "$out/flow-fail/calls" | cut -d: -f1)" ] || fail "node b must stop before node a"
  [ "$(grep -n '^node-stop$' "$out/flow-fail/calls" | cut -d: -f1)" -lt "$(grep -n '^garage-stop$' "$out/flow-fail/calls" | cut -d: -f1)" ] || fail "nodes must stop before Garage"
  echo "PASS teardown-on-failing-stage + redaction"
  e2e_run_rc "$out/flow-ok.out" bash "$child" "$self" "$out/flow-ok" ok
  [ "$E2E_RC" = 0 ] || { cat "$out/flow-ok.out"; fail "passing flow exit $E2E_RC, want 0"; }
  grep -q 'E2E PASS' "$out/flow-ok.out" || fail "no E2E PASS after a passing flow"
  grep -q '^PASS third' "$out/flow-ok.out" || fail "no per-stage PASS lines"
  for call in nodeb-stop node-stop garage-stop s3-unstage; do
    grep -q "^$call\$" "$out/flow-ok/calls" || fail "teardown of a passing flow did not call $call"
  done
  echo "PASS pass-only-after-teardown"

  # 5. Garage key parsing and the generation-newer-than-write filter, with
  #    fixed samples.
  local key_out id secret
  key_out=$'==== ACCESS KEY INFORMATION ====\nKey ID:              GKexample0123456789abcdef\nKey name:            bbrafte2e\nSecret key:          s3cr3t-sample-value\nCan create buckets:  false'
  id=$(printf '%s\n' "$key_out" | sed -n 's/^Key ID: *//p' | head -1)
  secret=$(printf '%s\n' "$key_out" | sed -n 's/^Secret key: *//p' | head -1)
  [ "$id" = GKexample0123456789abcdef ] && [ "$secret" = s3cr3t-sample-value ] || fail "garage key parsing"
  printf '{"ok":true,"snapshot":{"createdAt":"2026-11-21T10:00:07.123456Z"}}' >"$out/latest.json"
  local t_old t_new
  t_old=$(date -u -d 2026-11-21T10:00:06Z +%s); t_new=$(date -u -d 2026-11-21T10:00:07Z +%s)
  jq -e --argjson t "$t_old" '.snapshot != null and ((.snapshot.createdAt | sub("(\\.[0-9]+)?(Z|[+]00:00)$"; "Z") | fromdateiso8601) > $t)' "$out/latest.json" >/dev/null || fail "newer generation rejected"
  jq -e --argjson t "$t_new" '.snapshot != null and ((.snapshot.createdAt | sub("(\\.[0-9]+)?(Z|[+]00:00)$"; "Z") | fromdateiso8601) > $t)' "$out/latest.json" >/dev/null && fail "same-second generation accepted as newer"
  echo "PASS garage-key-and-generation-filter"

  # 6. Node b's capacity from the host: a large VM (8 vCPU / 16 GiB) gets
  #    factors under which the node would accept the offer and ten small
  #    sandboxes fit across a and b; a 2 vCPU host, or one short of memory
  #    or disk, fails cleanly (a message, return 1, no shell error).
  child=$out/capacity.sh
  mkdir -p "$out/capacity"
  cat >"$child" <<'CHILD'
set -euo pipefail; set -E
E2E_P2_LIB=1 source "$1"
# fits NAME DETECTED CAP FACTOR: the node's rule, scaled = floor(detected x factor) >= cap.
fits() {
  local c=${4/./} scaled
  c=$((10#$c))
  scaled=$((($2 * c) / 100))
  [ "$scaled" -ge "$3" ] || { echo "$1: scaled $scaled < offer $3 (detected $2 factor $4)"; exit 3; }
}
# Large VM: 8 vCPUs, 16008 MiB (MemTotal of a 16 GiB guest), 400 GiB free, one filesystem.
plan=$(e2e2_capacity_plan 8 16008 400 400 1) || { echo "large host refused: $plan"; exit 4; }
echo "$plan"
e2e2_capacity_plan 8 16008 400 400 1 >/dev/null
fits cpu 8 "$E2E2_B_VCPUS" "$E2E2_B_CPU_OC"
fits memory 16008 "$E2E2_B_MEMORY_MIB" "$E2E2_B_MEMORY_OC"
fits disk 400 "$E2E2_B_DISK_GIB" "$E2E2_B_DISK_OC"
fits disk-a 400 "$E2E2_A_DISK_GIB" "$E2E2_A_DISK_OC"
# Ten small sandboxes across both nodes: a keeps its one slot, b holds the other nine.
[ "$E2E2_B_VCPUS" -ge $((9 * E2E2_SMALL_VCPUS)) ] || exit 5
[ "$E2E2_B_MEMORY_MIB" -ge $((9 * E2E2_SMALL_MEMORY_MIB)) ] || exit 6
[ "$E2E2_B_DISK_GIB" -ge $((9 * E2E2_SMALL_DISK_GIB)) ] || exit 7
[ $((E2E2_A_VCPUS / E2E2_SMALL_VCPUS)) -ge 1 ] && [ $((E2E2_A_MEMORY_MIB / E2E2_SMALL_MEMORY_MIB)) -ge 1 ] || exit 8
# The factors are within the ceilings and never below the default.
[ "${E2E2_B_CPU_OC/./}" -ge 100 ] || exit 9
e2e2_nodeb_capacity_flags f
printf '%s\n' "${f[*]}" | grep -q -- '-node.cpu-overcommit .* -node.memory-overcommit .* -node.disk-overcommit' || exit 10
# The large VM type: 8 vCPUs, 16008 MiB, about 97 GiB free on ONE shared filesystem.
# Node a (110 GiB) and node b (510 GiB) both need a disk factor under the ceiling.
plan=$(e2e2_capacity_plan 8 16008 97 97 1) || { echo "97 GiB shared refused: $plan"; exit 14; }
echo "$plan"
e2e2_capacity_plan 8 16008 97 97 1 >/dev/null
fits disk-b 97 "$E2E2_B_DISK_GIB" "$E2E2_B_DISK_OC"
fits disk-a 97 "$E2E2_A_DISK_GIB" "$E2E2_A_DISK_OC"
fits cpu 8 "$E2E2_B_VCPUS" "$E2E2_B_CPU_OC"
fits memory 16008 "$E2E2_B_MEMORY_MIB" "$E2E2_B_MEMORY_OC"
[ "${E2E2_A_DISK_OC/./}" -le $((E2E2_MAX_DISK_OVERCOMMIT * 100)) ] && [ "${E2E2_B_DISK_OC/./}" -le $((E2E2_MAX_DISK_OVERCOMMIT * 100)) ] || exit 15
# Node a's factor travels through the extra-vars file under the role variable.
declare -f e2e2_s3_stage | grep -q 'bbraft_node_disk_overcommit: \$diskoc' || exit 16
declare -f e2e2_s3_stage | grep -q -- '--arg diskoc "\$E2E2_A_DISK_OC"' || exit 17
# A roomy host needs no overcommit at all.
e2e2_capacity_plan 64 262144 5000 5000 0 >/dev/null
[ "$E2E2_B_CPU_OC" = 1.00 ] && [ "$E2E2_B_MEMORY_OC" = 1.00 ] && [ "$E2E2_B_DISK_OC" = 1.00 ] && [ "$E2E2_A_DISK_OC" = 1.00 ] || { echo "roomy host overcommits"; exit 11; }
# Hosts that cannot fit: each must refuse with the host-too-small message.
refuse() {
  local msg rc=0
  msg=$(e2e2_capacity_plan "$@" 2>&1) || rc=$?
  [ "$rc" = 1 ] || { echo "plan for $* returned $rc: $msg"; exit 12; }
  case $msg in "host too small for the two-node scenario"*) ;; *) echo "unclear refusal for $*: $msg"; exit 13 ;; esac
  echo "refused ($*): ${msg%% (\"count*}"
}
refuse 2 16008 400 400 1   # 2 vCPUs: nothing left after node a
refuse 4 16008 400 400 1   # 4 vCPUs: factor above the ceiling
refuse 8 4000 400 400 1    # memory short
refuse 8 16008 25 25 1     # 25 GiB shared: under the real-free floor
refuse 8 16008 400 25 0    # node a's filesystem under the floor
refuse 8 16008 25 400 0    # node b's data directory under the floor
refuse 8 16008 40 40 1     # above the floor, but node b's disk factor passes the ceiling
CHILD
  e2e_run_rc "$out/capacity.out" bash "$child" "$self" "$out/capacity"
  [ "$E2E_RC" = 0 ] || { cat "$out/capacity.out"; fail "capacity plan (rc $E2E_RC)"; }
  sed 's/^/  /' "$out/capacity.out"
  echo "PASS capacity-plan"

  # 7. The pinned-image check: a matching manifest passes, every drift is
  #    refused with its reason (verify.sh itself is stubbed: it needs qemu-img
  #    and real artifacts, and is the node's own check).
  child=$out/image.sh
  cat >"$child" <<'CHILD'
set -euo pipefail; set -E
E2E_P2_LIB=1 source "$1"
repo=$2/repo; mkdir -p "$repo/images" "$2/img"
printf '#!/usr/bin/env bash\nexit 0\n' >"$repo/images/verify.sh"
IMAGE_VERSION=ubuntu-24.04-v1 UBUNTU_SNAPSHOT=20261108T182718Z GO_VERSION=1.27.3 NODE_VERSION=24.22.2
mk2() { # mk2 ID SNAPSHOT MIRROR GO NODE
  jq -n --arg id "$1" --arg s "$2" --arg m "$3" --arg g "$4" --arg n "$5" \
    '{id:$id, distribution:({snapshot:$s}+(if $m=="" then {} else {mirror:$m} end)), toolchain:{go:$g,node:$n}}' >"$IMG/manifest.json"
}
IMG=$2/img
mk2 ubuntu-24.04-v1 20261108T182718Z "" 1.27.3 24.22.2
e2e2_pinned_image_ok "$IMG" >/dev/null || { echo "good image refused"; exit 3; }
bad() { # bad WHY ID SNAPSHOT MIRROR GO NODE
  local why=$1 out
  shift; mk2 "$@"
  if out=$(e2e2_pinned_image_ok "$IMG" 2>&1); then echo "$why accepted"; exit 4; fi
  echo "refused ($why): ${out##*$'\n'}"
}
bad "other id" ubuntu-24.04-v2 20261108T182718Z "" 1.27.3 24.22.2
bad "test id" ubuntu-24.04-v1-unpinned-test unpinned http://mirror.invalid/ubuntu 1.27.3 24.22.2
bad "other snapshot" ubuntu-24.04-v1 20250101T000000Z "" 1.27.3 24.22.2
bad "mirror" ubuntu-24.04-v1 20261108T182718Z http://mirror.invalid/ubuntu 1.27.3 24.22.2
bad "other go" ubuntu-24.04-v1 20261108T182718Z "" 1.26.0 24.22.2
bad "other node" ubuntu-24.04-v1 20261108T182718Z "" 1.27.3 22.0.0
printf '#!/usr/bin/env bash\necho verify: sha256 mismatch >&2\nexit 1\n' >"$repo/images/verify.sh"
mk2 ubuntu-24.04-v1 20261108T182718Z "" 1.27.3 24.22.2
if e2e2_pinned_image_ok "$IMG" >/dev/null 2>&1; then echo "failing verify.sh accepted"; exit 5; fi
CHILD
  mkdir -p "$out/image"
  e2e_run_rc "$out/image.out" bash "$child" "$self" "$out/image"
  [ "$E2E_RC" = 0 ] || { cat "$out/image.out"; fail "pinned image check (rc $E2E_RC)"; }
  sed 's/^/  /' "$out/image.out"
  echo "PASS pinned-image-check"

  # 8. The opt-in failure diagnostics. (a) a failing stage with
  #    E2E2_DIAG_ON_FAIL=1 calls the collector before the teardown, and
  #    without the variable it does not; (b) with the real collector and fakes
  #    for the node, the CLI and sudo, a fake secret in the journal, the serial
  #    log, the netplan file and cloud-init user-data stubs never reaches the
  #    output or the saved files (user-data is only listed), while the public
  #    facts do (endpoint, DNAT line, authorized key, listing).
  child=$out/diagflow.sh
  cat >"$child" <<'CHILD'
set -euo pipefail; set -E
E2E_P2_LIB=1 source "$1"
d=$2; CALLS=$d/calls; export CALLS; : >"$CALLS"
work=$d/work; mkdir -p "$work/logs" "$work/secrets" "$work/n-b"
helpers=$d/helpers
printf '%s\n' 'FAKE-SECRET-VALUE-123' >"$work/secrets/redact.list"
export E2E_REDACT_FILE=$work/secrets/redact.list
E2E_ERRLOG=$work/logs/err.log; : >"$E2E_ERRLOG"
nodeb_started=1; node_started=1; garage_pid=""; s3_root=""; nodeb_pid=""
e2e_node_stop() { echo node-stop >>"$CALLS"; }
e2e_node_leak_proof() { :; }
e2e2_nodeb_stop() { echo nodeb-stop >>"$CALLS"; nodeb_started=0; }
e2e2_garage_stop() { :; }
e2e2_s3_unstage() { :; }
e2e_leak_scan() { :; }
e2e1_dump() { echo p1-dump >>"$CALLS"; }
e2e2_diag_collect() { echo diag-collect >>"$CALLS"; }
trap e2e_cleanup EXIT
stage_x() { e2e_stage boom; e2e_fail "stage failed"; }
E2E2_STAGES=(stage_x)
e2e2_run_stages
CHILD
  mkdir -p "$out/diag-on" "$out/diag-off"
  E2E2_DIAG_ON_FAIL=1 e2e_run_rc "$out/diag-on.out" bash "$child" "$self" "$out/diag-on"
  [ "$E2E_RC" = 1 ] || { cat "$out/diag-on.out"; fail "diag flow (flag set) exit $E2E_RC, want 1"; }
  grep -q '^diag-collect$' "$out/diag-on/calls" || fail "the collector was not called with E2E2_DIAG_ON_FAIL=1"
  [ "$(grep -n '^diag-collect$' "$out/diag-on/calls" | cut -d: -f1)" -lt "$(grep -n '^nodeb-stop$' "$out/diag-on/calls" | cut -d: -f1)" ] || fail "the collector must run before the teardown"
  E2E2_DIAG_ON_FAIL= e2e_run_rc "$out/diag-off.out" bash "$child" "$self" "$out/diag-off"
  [ "$E2E_RC" = 1 ] || { cat "$out/diag-off.out"; fail "diag flow (flag unset) exit $E2E_RC, want 1"; }
  grep -q '^diag-collect$' "$out/diag-off/calls" && fail "the collector ran without E2E2_DIAG_ON_FAIL=1"
  grep -q '^nodeb-stop$' "$out/diag-off/calls" || fail "teardown did not run without the flag"
  echo "PASS diag-hook-before-teardown-only-when-set"

  child=$out/diagleak.sh
  cat >"$child" <<'CHILD'
set -euo pipefail; set -E
E2E_P2_LIB=1 source "$1"
d=$2; SECRET=FAKE-SECRET-VALUE-123; UDSECRET=USERDATA-ONLY-SECRET-XYZ
work=$d/work; stub=$d/guest; mkdir -p "$work/logs" "$work/secrets" "$d/bin" "$d/data/vms/bx_t1"
printf '%s\n' "$SECRET" >"$work/secrets/redact.list"; export E2E_REDACT_FILE=$work/secrets/redact.list
E2E_OUT=$d/out; stage_name=node-loss; api_started=1; api_key=bmb_testkey; api_local=http://127.0.0.1:1
s1=bx_t1; e2e_ids=(); node_started=1
_e2e_node_service=fakeunit; _e2e_node_data_dir=$d/data
# A fake guest filesystem: public keys and a netplan file with the secret (the
# redactor must hide it), cloud-init data files that must only ever be listed.
mkdir -p "$stub/proc" "$stub/etc/netplan" "$stub/etc/systemd" "$stub/var/lib/cloud/instances/i-1" "$stub/home/user/.ssh"
echo "root=/dev/vda ip=10.88.0.5::10.88.0.1:255.255.255.0::eth0 console=ttyS0" >"$stub/proc/cmdline"
printf 'network: {token: %s}\n' "$SECRET" >"$stub/etc/netplan/50.yaml"
for f in user-data.txt vendor-data.txt network-config.json; do printf 'password: %s %s\n' "$UDSECRET" "$SECRET" >"$stub/var/lib/cloud/instances/i-1/$f"; done
echo "ssh-ed25519 AAAAC3PUBLICKEYSAMPLE user@guest" >"$stub/home/user/.ssh/authorized_keys"
echo deadbeefmachineid >"$stub/etc/machine-id"
printf 'serial: boot ok %s\n' "$SECRET" >"$d/data/vms/bx_t1/serial.log"
for t in id cat ls head; do ln -s "$(command -v "$t")" "$d/bin/$t"; done
for t in ip systemctl networkctl cloud-init ss; do printf '#!/bin/sh\necho stub-%s "$@"\n' "$t" >"$d/bin/$t"; chmod +x "$d/bin/$t"; done
printf '%s\n' "node line: ssh ingress for bx_t1 token $SECRET" "unrelated" >"$d/journal.txt"
printf '%s\n' 'table ip bbraft_net {' '  tcp dport 2201 dnat to 10.88.0.5:22' '}' >"$d/nft.txt"
cat >"$d/fakebbraft" <<EOF
#!/usr/bin/env bash
case "\$1" in
  exec) shift 3; PATH=$d/bin R=$stub HOME=$stub/home/user $BASH -c "\$3" ;;
  info) echo '{"id":"bx_t1","state":"ready"}' ;;
  *) echo "unexpected \$1" >&2; exit 1 ;;
esac
EOF
chmod +x "$d/fakebbraft"; B=$d/fakebbraft
e2e_priv() { case "$*" in *journalctl*) cat "$d/journal.txt" ;; *"nft list ruleset"*) cat "$d/nft.txt" ;; *) "$@" ;; esac; }
e2e2_sql() { case "$1" in *count*) echo 1 ;; *) echo "bx_t1 state=ready node=e2e-node-a disk_node=e2e-node-a" ;; esac; }
# The host's own network facts must never enter a self-test run: ip is stubbed.
timeout() { local a; for a in "$@"; do [ "$a" = ip ] && { echo "stub ip $*"; return 0; }; done; command timeout "$@"; }
e2e2_node_of() { echo e2e-node-a; }
curl() { printf '{"sandbox":{"id":"bx_t1","sshEndpoint":"192.0.2.1:2201","state":"ready"}}'; }
e2e2_diag_collect >"$d/diag.out" 2>&1
CHILD
  mkdir -p "$out/diag-leak"
  e2e_run_rc "$out/diag-leak.out" bash "$child" "$self" "$out/diag-leak"
  [ "$E2E_RC" = 0 ] || { cat "$out/diag-leak.out" "$out/diag-leak/diag.out" 2>/dev/null; fail "diag collector run (rc $E2E_RC)"; }
  local dout=$out/diag-leak/diag.out
  grep -r -l -e 'FAKE-SECRET-VALUE-123' -e 'USERDATA-ONLY-SECRET-XYZ' -e 'bmb_testkey' "$dout" "$out/diag-leak/out" >/dev/null 2>&1 \
    && { grep -r -n -e 'FAKE-SECRET-VALUE-123' -e 'USERDATA-ONLY-SECRET-XYZ' "$dout" "$out/diag-leak/out" | cut -c1-120; fail "a secret reached the diag output or files"; }
  grep -q 'REDACTED' "$dout" || fail "the redactor did not act on the journal/serial/netplan lines"
  grep -q 'sshEndpoint of bx_t1: .192.0.2.1:2201.' "$dout" || fail "no sshEndpoint line"
  grep -q 'dport 2201 dnat' "$dout" || fail "no DNAT line for the endpoint port"
  grep -q 'ssh-ed25519 AAAAC3PUBLICKEYSAMPLE' "$dout" || fail "no authorized_keys listing"
  grep -q 'user-data.txt' "$dout" || fail "cloud-init user-data is not even listed"
  grep -q 'ip=10.88.0.5' "$dout" || fail "no /proc/cmdline"
  grep -q 'serial: boot ok' "$dout" || fail "no serial console tail"
  grep -q 'control sandbox: skipped' "$dout" || fail "no control sandbox line"
  [ -n "$(ls "$out/diag-leak/out"/diag-*/ 2>/dev/null)" ] || fail "nothing saved in the diag directory"
  # Static guard: the guest probe never reads cloud-init data files.
  E2E_P2_LIB=1 bash -c 'source "$1"; printf "%s" "$E2E2_GUEST_PROBE"' _ "$self" | grep -E '(cat|head|tail|less|more|grep|strings|base64|cp)[^#]*(user-data|vendor-data|network-config|var/lib/cloud)' >/dev/null && fail "the guest probe reads cloud-init data"
  echo "PASS diag-collects-without-secrets"

  # 9. The guest prefix: one source of truth in the node driver (exported), the
  #    tap check's pattern follows it, the DNS resolvers lie outside it, and a
  #    host address inside it is refused with the address named.
  child=$out/prefix.sh
  cat >"$child" <<'CHILD'
set -euo pipefail; set -E
E2E_P2_LIB=1 source "$1"
drv=$2
source "$drv"
[ "$E2E_NODE_GUEST_PREFIX" = 10.88.0.0/16 ] && [ "$E2E_NODE_GUEST_GATEWAY" = 10.88.0.1 ] || { echo "prefix/gateway: $E2E_NODE_GUEST_PREFIX $E2E_NODE_GUEST_GATEWAY"; exit 3; }
# The gateway lies inside the prefix; the SSH ingress and client addresses and the resolvers do not.
chk() { printf '%s\n' "$1" | e2e_node_guest_prefix_check "$E2E_NODE_GUEST_PREFIX" 2>/dev/null; }
chk "$E2E_NODE_GUEST_GATEWAY" && { echo "the gateway is not inside the prefix?"; exit 4; }
chk "$E2E_NODE_SSH_INGRESS_ADDR/30" || { echo "ingress address inside the prefix"; exit 5; }
chk "$E2E_NODE_CLIENT_ADDR/30" || { echo "client address inside the prefix"; exit 6; }
resolvers=$(awk '/^bbraft_node_network_resolvers:/ { on = 1; next } on && /^[[:space:]]+- / { print $2; next } on { exit }' "$4")
[ -n "$resolvers" ] || { echo "no resolver defaults found"; exit 7; }
for r in $resolvers; do chk "$r" || { echo "resolver $r lies inside the guest prefix"; exit 8; }; done
echo "prefix $E2E_NODE_GUEST_PREFIX gateway $E2E_NODE_GUEST_GATEWAY; resolvers outside: $(echo $resolvers)"
# No second literal of the prefix in the driver's code: the render uses the constants.
n=$(grep -v '^[[:space:]]*#' "$drv" | grep -c '10\.88\.' || true)
[ "$n" = 2 ] || { echo "driver code holds $n literals of the guest prefix, want the 2 constants"; exit 9; }
grep -v '^[[:space:]]*#' "$drv" | grep -q '10\.42\.' && { echo "old prefix left in the driver"; exit 10; }
# The tap check's pattern follows the prefix.
re=$(e2e2_prefix_regex "$E2E_NODE_GUEST_PREFIX")
[ "$re" = ' 10\.88\.' ] || { echo "pattern '$re'"; exit 11; }
grep -q -- "$re" <<<"2: eth0    inet 10.88.0.5/16 brd 10.88.255.255 scope global eth0" || { echo "pattern misses a guest address"; exit 12; }
grep -q -- "$re" <<<"2: eth0    inet 10.42.0.2/16 scope global eth0" && { echo "pattern matches the old prefix"; exit 13; }
[ "$(e2e2_prefix_regex 192.168.4.0/24)" = ' 192\.168\.4\.' ] || exit 14
if e2e2_prefix_regex 10.88.0.0/12 2>/dev/null; then echo "unaligned prefix accepted"; exit 15; fi
# A host address inside the prefix is refused, naming it; addresses outside are fine.
msg=$(printf '%s\n' "127.0.0.1/8" "192.0.2.1/30" "10.42.0.2/16" | e2e_node_guest_prefix_check 2>&1) || { echo "refused outside addresses: $msg"; exit 16; }
rc=0; msg=$(printf '%s\n' "127.0.0.1/8" "10.88.3.4/16" "192.0.2.1/30" | e2e_node_guest_prefix_check 2>&1) || rc=$?
[ "$rc" = 1 ] || { echo "host address inside the prefix accepted (rc $rc)"; exit 17; }
case $msg in *10.88.3.4*) ;; *) echo "the refusal does not name the address: $msg"; exit 18 ;; esac
echo "refused: $msg"
# The same through the preflight wrapper, with a fake ip.
ip() { printf '%s\n' '1: lo    inet 127.0.0.1/8 scope host lo' '3: eth0    inet 10.88.0.2/16 brd 10.88.255.255 scope global eth0'; }
rc=0; msg=$(e2e_node_guest_prefix_preflight 2>&1) || rc=$?
[ "$rc" = 1 ] && case $msg in *10.88.0.2*) true ;; *) false ;; esac || { echo "preflight wrapper: rc $rc: $msg"; exit 19; }
ip() { printf '%s\n' '1: lo    inet 127.0.0.1/8 scope host lo' '3: eth0    inet 10.42.0.2/16 brd 10.42.255.255 scope global eth0'; }
e2e_node_guest_prefix_preflight || { echo "preflight wrapper refused a host outside the prefix"; exit 20; }
CHILD
  e2e_run_rc "$out/prefix.out" bash "$child" "$self" "$e2e2_here/e2e-p1-node.sh" x "$e2e2_here/../../infra/ansible/roles/bbraft_node/defaults/main.yml"
  [ "$E2E_RC" = 0 ] || { cat "$out/prefix.out"; fail "guest prefix (rc $E2E_RC)"; }
  sed 's/^/  /' "$out/prefix.out"
  echo "PASS guest-prefix"
  echo "SELFTEST OK"
}

if [ "${E2E_P2_LIB:-}" = 1 ]; then return 0 2>/dev/null || exit 0; fi
if [ "${E2E_SELFTEST:-}" = 1 ]; then e2e2_selftest; exit 0; fi

# ===========================================================================
# Main run.
# ===========================================================================
e2e2_run_stages

# All stages passed. The summary and E2E PASS itself are printed by the EXIT
# trap, only after the teardown and the ip_forward restoration verify clean.
printf '\nall stages passed; tearing down for the final leak proof\n'
e2e_pass_pending=1
