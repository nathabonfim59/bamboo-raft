#!/bin/sh
# Runs inside a private user+network namespace (ip-forward.sh starts it with
# `unshare -rn`): loads a rendered nftables file with the REAL nft, then runs
# tasks/ip-forward.yml against that real live ruleset. Only sysctl is a fake
# (a fake-only bin dir), so no real kernel setting is written.
#
#   ip-forward-netns.sh <rendered.nft> <sysctl-bin-dir> <work-dir> <bridges-json> <ansible-dir>
set -eu

nft_file=$1
sysbin=$2
work=$3
bridges=$4
ansible_dir=$5
here=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)

cd "$ansible_dir"
nft -f "$nft_file"
mkdir -p "$work"
echo 0 >"$work/state"
: >"$work/log"
PATH="$sysbin:$PATH" FAKE_LOG="$work/log" SYSCTL_STATE="$work/state" \
    ansible-playbook -i "$here/inventory.yml" "$here/ip-forward.yml" \
    -e "ipf_conf=$work/ip.conf" -e "ipf_owner=$(id -un)" -e "ipf_group=$(id -gn)" \
    -e "{\"ipf_bridges\": $bridges}"
