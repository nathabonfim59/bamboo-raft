#!/usr/bin/env bash
# bamboo-raft Phase 0.5 end-to-end on one KVM host (a test VM from bbraft-p05).
# Runs from the repository root:
#   bash e2e.sh   (on a large throwaway KVM VM with Postgres and QEMU)
# Builds everything, builds the guest image, starts bbraft-server (all roles,
# real Postgres) and the real bbraft-node, then drives the bbraft CLI exactly
# the way an automated client drives it. Prints one PASS/FAIL line per stage.
set -euo pipefail
repo=$PWD
work=$HOME/.bbk/e2e   # short: QMP socket paths must fit 107 bytes
rm -rf "$work"; mkdir -p "$work"/{secrets,node,cfg,logs}
export XDG_CONFIG_HOME=$work/cfg
pids=()
cleanup() {
	for p in "${pids[@]}"; do kill "$p" 2>/dev/null || true; done
	sleep 2
	for p in "${pids[@]}"; do kill -9 "$p" 2>/dev/null || true; done
}
trap cleanup EXIT
stage() { printf '\n=== %s (%s)\n' "$1" "$(date -u +%H:%M:%S)"; }
fail() {
	echo "E2E FAIL: $*"
	echo "--- processes"; pgrep -af 'bbraft-(server|node)|qemu-system' | cut -c1-200 || true
	echo "--- api"; curl -sS -m 5 http://127.0.0.1:7700/api/v1/me -H "Authorization: Bearer ${api_key:-}" | cut -c1-300 || true
	echo; ls -la "$work/logs"
	for f in "$work"/logs/*.log "$work"/logs/*.out; do [ -f "$f" ] || continue; echo "--- tail $f"; tail -40 "$f" | grep -v '^‣' | cut -c1-400; done
	exit 1
}

stage build
# A clean source copy to push, taken before the image build fills
# images/.cache and images/out (test checkouts have no .git, so push would
# send everything). This is what a developer's git checkout pushes anyway.
src=$work/bamboo-raft
mkdir -p "$src"
tar -C "$repo" --exclude=./.cache --exclude=./images/.cache --exclude=./images/out --exclude=./bin -cf - . | tar -C "$src" -xf -
du -sh "$src"
make build >/dev/null || fail "make build"
ls bin/

stage image
t0=$(date +%s)
make -C images image > "$work/logs/image-build.log" 2>&1 || fail "image build"
echo "image built in $(( $(date +%s) - t0 ))s"; ls images/out

stage secrets
umask 077
head -c 32 /dev/urandom > "$work/secrets/master.key"
head -c 32 /dev/urandom | od -An -tx1 | tr -d ' \n' > "$work/secrets/gateway.token"
node_token=$(head -c 32 /dev/urandom | od -An -tx1 | tr -d ' \n')
printf %s "$node_token" > "$work/secrets/node.token"
node_sha=$(printf %s "$node_token" | sha256sum | cut -d' ' -f1)
printf '{"nodes":[{"id":"e2e-node","tokenSha256":"%s"}]}' "$node_sha" > "$work/secrets/nodes.json"
api_key="bmb_$(head -c 24 /dev/urandom | od -An -tx1 | tr -d ' \n')"
printf '{"keys":[{"id":"main","owner":"e2e","key":"%s"}]}' "$api_key" > "$work/secrets/keys.json"
printf %s "$BBRAFT_TEST_DATABASE_URL" > "$work/secrets/database-url"
umask 022

stage server
bin/bbraft-server -roles all \
	-database.url-file "$work/secrets/database-url" \
	-secrets.master-key-file "$work/secrets/master.key" \
	-node.gateway-token-file "$work/secrets/gateway.token" \
	-auth.keys-file "$work/secrets/keys.json" \
	-node.tokens-file "$work/secrets/nodes.json" \
	-server.public-url http://127.0.0.1:7700 \
	> "$work/logs/server.log" 2>&1 &
pids+=($!)
for _ in $(seq 60); do curl -fsS -o /dev/null http://127.0.0.1:7700/api/v1/me -H "Authorization: Bearer $api_key" && break; sleep 1; done
curl -fsS http://127.0.0.1:7700/api/v1/me -H "Authorization: Bearer $api_key" >/dev/null || fail "server not answering"
echo server up

stage node
# A large test VM (8 vCPU, 16 GiB) offers 7 vCPU / ~14.6 GiB after the
# reserve: room for "default" sandboxes (4 vCPU, 8 GiB), not "large".
# E2E_TYPE=large needs node overcommit flags (E2E_NODE_FLAGS).
cpu_flags=()
# -h exits non-zero, so capture its text first (pipefail would hide a match).
node_help=$(bin/bbraft-node -h 2>&1 || true)
if [ -n "${BBRAFT_TEST_CPU_MODEL:-}" ] && grep -q cpu-allow-host <<<"$node_help"; then
	cpu_flags=(-node.cpu-model "$BBRAFT_TEST_CPU_MODEL" -node.cpu-allow-host)
fi
bin/bbraft-node -server.addr 127.0.0.1:7701 -node.id e2e-node \
	-node.token-file "$work/secrets/node.token" \
	-node.data-dir "$work/node" -node.image-dir "$repo/images/out" \
	${E2E_NODE_FLAGS:--node.disk-overcommit 4} \
	"${cpu_flags[@]}" \
	> "$work/logs/node.log" 2>&1 &
pids+=($!)
sleep 5
kill -0 "${pids[-1]}" 2>/dev/null || fail "node exited at start"
grep -m1 'cpu resolved' "$work/logs/node.log" || true

stage login
export BBRAFT_API_URL=http://127.0.0.1:7700
printf %s "$api_key" | bin/bbraft login --key-stdin >/dev/null || fail login

B=bin/bbraft
stage "new --json (ready event)"
t0=$(date +%s)
out=$($B new --type small --name e2e-base --ttl 7200 --json 2>&1) || fail "new: $out"
id=$(jq -r 'select(.event == "ready") | .id' <<<"$out" | tail -1)
[ -n "$id" ] && [ "$id" != null ] || fail "no ready id in: $out"
echo "sandbox $id ready in $(( $(date +%s) - t0 ))s"

stage "push --to"
$B push "$id" "$src" --to /home/user/repo >/dev/null || fail push

stage "exec --timeout 0 --stream --json (agent harness form)"
# bbraft exec exits with the remote code (3 here), so the pipeline "fails"
# by design: suspend -e/pipefail around it, exactly like an agent harness does.
set +e +o pipefail
$B exec "$id" --timeout 0 --stream --json -- bash -lc 'cd ~/repo && ls go.mod && go version && echo hello-from-guest; exit 3' |
	jq -rj --unbuffered 'if .type == "stdout" then .data elif .type == "stderr" then (.data|stderr|empty) elif .type == "exit" then "\nEXIT:\(.exitCode // 1)\n" else empty end' \
	> "$work/logs/exec1.out"
set -e -o pipefail
cat "$work/logs/exec1.out"
code=$(sed -n 's/^EXIT://p' "$work/logs/exec1.out")
grep -q hello-from-guest "$work/logs/exec1.out" || fail "exec output"
[ "$code" = 3 ] || fail "exec exit code $code, want 3"
echo "exec ok, exit $code"

stage "snapshot (base)"
$B exec "$id" --timeout 600 -- bash -lc 'echo warm > ~/warm-cache-marker' >/dev/null || fail "exec marker"
$B snapshot "$id" e2e-base >/dev/null || fail snapshot
$B delete "$id" --yes --wait >/dev/null || fail "delete base"

echo "node capacity before restore:"; grep -o '"diskFreeMib":[0-9]*\|disk_free[^ ]*' "$work/logs/node.log" | tail -1 || true
stage "new --from --type ${E2E_TYPE:-default}"
out=$($B new --from e2e-base --type "${E2E_TYPE:-default}" --ttl 7200 --json 2>&1) || fail "new --from: $out"
id2=$(jq -r 'select(.event == "ready") | .id' <<<"$out" | tail -1)
[ -n "$id2" ] && [ "$id2" != null ] || fail "no ready id from snapshot: $out"
$B exec "$id2" -- bash -lc 'cat ~/warm-cache-marker; nproc; df -BG --output=size / | tail -1' || fail "exec on restored"
$B info "$id2" --json | jq -r '.state // .sandbox.state'
$B delete "$id2" --yes --wait >/dev/null || fail "delete restored"

stage "bbraft run --push . -- make check (the dogfood exit criterion)"
t0=$(date +%s)
set +e
$B run --type "${E2E_TYPE:-default}" --push "$src" --json -- make check > "$work/logs/run.out" 2>&1
rc=$?
set -e
tail -15 "$work/logs/run.out"
echo "run exit $rc in $(( $(date +%s) - t0 ))s"
[ "$rc" = 0 ] || fail "bbraft run make check exit $rc"

stage leaks
$B list --json | jq -c '[.sandboxes[]? | {id, state}]'
echo "E2E PASS"
