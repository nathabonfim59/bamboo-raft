#!/usr/bin/env bash
# bamboo-raft Phase 1 real-node end-to-end (successor of deploy/dev/e2e-p05.sh,
# which stays untouched). Run on one large throwaway KVM VM with Postgres
# and QEMU:
#
#   bash deploy/dev/e2e-p1.sh
#
# Builds everything and the pinned v1 image, starts bbraft-server -roles all on
# real Postgres and the REAL bbraft-node the way the bbraft_node role deploys it
# (root supervisor under the role-rendered systemd unit, jailed QEMU, TAP/NAT,
# fresh data dir, SSH ingress on a local test address) through Lead A's sourced
# helper deploy/dev/e2e-p1-node.sh, then drives the bbraft CLI through the
# quickstart and the "In scripts and CI" examples plus the Phase 1 stage list.
# Every /api/v1 response the script sees is spooled by a recording proxy and
# validated against the v1 API spec by internal/compat. One PASS/FAIL line
# per stage; logs are dumped redacted on failure; teardown proves no leaks; the
# final E2E PASS is printed by the EXIT trap only after teardown and the
# ip_forward restoration are verified.
#
# Self-test (no VM, no server, no node, no sysctl, no real network):
#
#   E2E_SELFTEST=1 bash deploy/dev/e2e-p1.sh
#
# Node helper seam (deploy/dev/e2e-p1-node.sh, fail-closed when absent):
#   e2e_node_start BINARY IMAGE_VERSION_DIR NODE_ID SERVER_LINK_ADDR TOKEN_FILE FRESH_WORK_DIR
#   e2e_node_wait_registered CALLBACK [ARG...]   e2e_node_wait_ready [SECONDS]
#   e2e_client CMD [ARG...]                      e2e_node_stop
#   e2e_node_leak_proof
# At SOURCE time the helper header exports only the PLANNED FIXED TOPOLOGY
# (E2E_NODE_HOST_VETH_ADDR et al.) with no side effects; the runtime exports
# E2E_NODE_ID E2E_NODE_IMAGE_ID E2E_NODE_EXPECTED_VCPUS
# E2E_NODE_EXPECTED_MEMORY_MIB E2E_NODE_EXPECTED_DISK_GIB
# E2E_NODE_ADMITTED_QEMU_VERSION E2E_NODE_START_EPOCH E2E_NODE_READY_EPOCH
# appear after e2e_node_start/wait_ready and are gated there. A missing
# required export fails - never a positive-only fallback.
set -euo pipefail
set -E

# ---------------------------------------------------------------------------
# Environment and constants (no side effects before the helper definitions).
# ---------------------------------------------------------------------------
E2E_OUT=${E2E_OUT:-$HOME/.cache/bamboo-raft/p1-b}          # persistent local artifacts
E2E_JSON_BOUND=${E2E_JSON_BOUND:-$((128 * 1024 * 1024))}   # JSON spool+validate cap, bytes
E2E_FRAME_BOUND=${E2E_FRAME_BOUND:-$((16 * 1024 * 1024))}  # one NDJSON frame cap
E2E_TOTAL_SPOOL=${E2E_TOTAL_SPOOL:-$((512 * 1024 * 1024))} # whole capture cap
export E2E_JSON_BOUND E2E_FRAME_BOUND E2E_TOTAL_SPOOL
# Forced, not defaulted: a missing pinned spec must FAIL validation, never
# skip as a passing conformance run.
export BBRAFT_TEST_REQUIRE_ALL=1

stage_name=""
stage_t0=0
work=""
helpers=""
e2e_ids=()        # every accepted sandbox id, deleted in teardown
server_pid=""     # setsid leader (= process group id) of MY bbraft-server
proxy_pid=""      # setsid leader (= process group id) of MY recording proxy
e2e_bg=()         # my background children (attach CLI etc.), reaped unconditionally
E2E_DAEMON_RC=""  # exit status of the last reaped daemon
E2E_HELPER_RC=0   # exit status of the last e2e_run_helper
E2E_BARRIER_SEQ=0 # largest ASSIGNED capture sequence at the last barrier
E2E_PROXY_PORT=${E2E_PROXY_PORT:-7700}
E2E_PROC_RE='qemu-system|bbraft-server|bbraft-node|e2e-p1-proxy'
node_helper_sourced=0
node_started=0
node_parent=""        # the ACTUAL mktemp node-parent.XXXXXXXX the helper works in
api_started=0         # node registered and API reachable: sandboxes may exist (cleanup sweeps only then)
e2e_body_complete=0   # stage 14 finished every proof, API sweep not needed
e2e_db_name=""        # this run's OWNED disposable database (never the base one)
e2e_db_created=0      # armed before CREATE DATABASE, cleared after DROP
ip_forward_prev=""
ip_forward_set=0
api_url=""
api_key=""
owner_uuid=""
E2E_RC=0
e2e_results=()
e2e_pass_pending=0

# ---------------------------------------------------------------------------
# Small helpers. Nothing here runs a stage; expected-nonzero commands always go
# through if-condition capture so neither errexit nor the ERR trap fires on a
# planned remote exit code. Stage bodies are only ever called at top level,
# never inside if/&&/|| that could mask a failure, and node-helper functions
# are never called as the lhs of || (that would disable errexit inside them).
# ---------------------------------------------------------------------------

# e2e_redact is a literal streaming redactor: every literal listed in
# E2E_REDACT_FILE is replaced byte-for-byte (awk index/substr, no regex
# escaping hazards, punctuation- and newline-safe), plus fixed regex patterns
# for signed receiver tokens, Authorization headers, bmb_ keys and DSNs. No
# secret ever appears in argv or error text.
e2e_redact() {
  awk -v list_file="${E2E_REDACT_FILE:-}" '
    BEGIN {
      c = 0
      if (list_file != "") {
        while ((getline line < list_file) > 0)
          if (line != "") lit[++c] = line
        close(list_file)
      }
    }
    {
      out = $0
      for (i = 1; i <= c; i++) {
        while ((idx = index(out, lit[i])) > 0)
          out = substr(out, 1, idx - 1) "REDACTED" substr(out, idx + length(lit[i]))
      }
      gsub(/\/api\/bbraft\/downloads\/[A-Za-z0-9._~=-]+/, "/api/bbraft/downloads/REDACTED", out)
      gsub(/\/api\/bbraft\/uploads\/[A-Za-z0-9._~=-]+/, "/api/bbraft/uploads/REDACTED", out)
      gsub(/Authorization: Bearer [A-Za-z0-9._~=-]+/, "Authorization: Bearer REDACTED", out)
      gsub(/bmb_[A-Za-z0-9]+/, "bmb_REDACTED", out)
      gsub(/postgres(ql)?:\/\/[^\/@:]+:[^@ ]*@/, "postgresql://REDACTED:REDACTED@", out)
      print out
    }'
}

# on_err records the failing command (BASH_COMMAND is unexpanded source text;
# still redacted) for the failure dump. It never exits and never leaks.
on_err() {
  local rc=$1 cmd=$2
  echo "ERR rc=$rc cmd=$(printf '%s' "$cmd" | e2e_redact)" \
    >>"${E2E_ERRLOG:-/dev/null}" 2>/dev/null || true
}
trap 'on_err "$?" "$BASH_COMMAND"' ERR

# e2e_run_rc OUTFILE CMD... runs CMD with stdout+stderr into OUTFILE and sets
# E2E_RC to its exact exit status. The command runs as an if-condition, so a
# planned nonzero status trips neither errexit nor the ERR trap.
e2e_run_rc() {
  local out=$1; shift
  if "$@" >"$out" 2>&1; then E2E_RC=0; else E2E_RC=$?; fi
  return 0
}

# e2e_expect_rc WANT OUTFILE CMD... fails the stage unless CMD exits WANT.
e2e_expect_rc() {
  local want=$1 out=$2; shift 2
  e2e_run_rc "$out" "$@"
  if [ "$E2E_RC" != "$want" ]; then
    e2e_fail "exit $E2E_RC, want $want: $(printf '%s ' "$@" | cut -c1-160 | e2e_redact)"
  fi
}

# e2e_ok CMD... fails the stage unless CMD exits 0 (output to the stage log).
e2e_ok() {
  e2e_expect_rc 0 "$work/logs/last.out" "$@"
}

# e2e_have MARKER FILE fails unless FILE contains MARKER.
e2e_have() { grep -q -- "$1" "$2" || e2e_fail "missing '$1' in $(basename "$2")"; }

# Stage bookkeeping: one PASS line per stage, collected for the summary.
e2e_stage() {
  stage_name=$1
  stage_t0=$(date +%s)
  printf '\n=== %s (%s)\n' "$stage_name" "$(date -u +%H:%M:%S)"
  # Deliberate-break path: E2E_BREAK=<stage name> fails this stage cleanly.
  if [ "${E2E_BREAK:-}" = "$stage_name" ]; then
    e2e_fail "deliberate break at stage $stage_name (E2E_BREAK)"
  fi
}
e2e_pass() {
  local secs=$(( $(date +%s) - stage_t0 ))
  printf 'PASS %s %ss\n' "$stage_name" "$secs"
  e2e_results+=("PASS $stage_name ${secs}s")
}

# e2e_fail dumps redacted diagnostics and exits 1; the EXIT trap then runs the
# best-effort teardown and preserves this exit status. The message itself is
# sanitized: a caller may embed command output.
e2e_fail() {
  local msg
  msg=$(printf '%s' "$*" | e2e_redact)
  printf 'FAIL %s: %s\n' "${stage_name:-startup}" "${msg:-unspecified}" >&2
  e2e_results+=("FAIL ${stage_name:-startup}: $msg")
  exit 1
}

# e2e_dump prints redacted failure diagnostics: process list, my log tails,
# the node helper's log tails, and a counts-only capture summary.
e2e_dump() {
  echo "--- processes (scope: bbraft/qemu)"
  pgrep -af 'bbraft-(server|node)|e2e-p1-proxy|qemu-system' 2>/dev/null | cut -c1-200 | e2e_redact || true
  local f
  for f in "$work"/logs/server.log "$work"/logs/proxy.log; do
    [ -f "$f" ] || continue
    echo "--- tail $(basename "$f")"; tail -40 "$f" | e2e_redact | cut -c1-400
  done
  if [ -n "$node_parent" ]; then
    for f in "$node_parent"/*/*.log "$node_parent"/*/*/*.log; do
      [ -f "$f" ] || continue
      echo "--- tail ${f#"$work"/}"; tail -20 "$f" | e2e_redact | cut -c1-300
    done
  fi
  if [ -f "$work/capture/index.ndjson" ]; then
    echo "--- capture summary (counts only)"
    jq -s 'group_by(.class) | map({(.[0].class): length}) | add' "$work/capture/index.ndjson" 2>/dev/null || true
  fi
  [ -s "${E2E_ERRLOG:-/dev/null}" ] && { echo "--- err log"; cat "$E2E_ERRLOG"; } || true
}

trim() { printf '%s' "$1" | tr -d '[:space:]'; }

# e2e_pg_env runs the generated pgenv helper: pgx's libpq-compatible parser
# decodes the DSN from the umask-077 file into a 0600 env file (percent-
# decoding preserved, explicit sslmode/options preserved, no silent disable),
# which is then sourced for psql. The DSN and password never reach argv or
# logs. Unsupported forms fail loudly.
e2e_pg_env() { # [DSN_FILE: default = the run's OWNED database]
  "$work/pgenv" -dsn-file "${1:-$work/secrets/database-url}" -out "$work/secrets" || return 1
  set -a
  # shellcheck source=/dev/null
  source "$work/secrets/pgenv.sh" || { set +a; return 1; }
  set +a
}

# pg_query SQL [-v name=value...] runs one bounded query. The SQL travels on
# STDIN so psql performs :'var' interpolation (the manual forbids psql
# features with -c); -v variables and bounds stay on the command line.
pg_query() {
  local sql=$1; shift
  printf '%s' "$sql" | psql -X -A -t -v ON_ERROR_STOP=1 "$@"
}

# e2e_priv CMD... runs CMD directly as root, else through sudo -n: nft and the
# one sysctl this run touches need privileges. Collection that cannot run is a
# FAILURE, never a skipped proof.
e2e_priv() {
  if [ "$(id -u)" = 0 ]; then "$@"; else sudo -n "$@"; fi
}

# e2e_ipf_read / e2e_ipf_write wrap the one sysctl this run touches. Selftest
# never reaches these: they only run on the main path of the disposable VM.
e2e_ipf_read() { cat /proc/sys/net/ipv4/ip_forward; }
e2e_ipf_write() { e2e_priv sysctl -q -w "net.ipv4.ip_forward=$1"; } # $1 = value

# e2e_node_ready_cb is the readiness callback handed to
# e2e_node_wait_registered. It requires every A export it uses; no
# positive-only fallback anywhere. See internal/db/node.go for the columns and
# internal/server/adapters.go for the epoch semantics on disconnect. vcpu and
# memory are exact, the admitted QEMU version is exact, disk is positive and
# at most the exported expectation (Info disk is dynamic), reservations are
# zero, and last_seen_at must postdate START_EPOCH and stay fresh.
e2e_node_ready_cb() {
  local nid img vcpus mem disk qemu epoch n
  nid=${E2E_NODE_ID:?readiness: E2E_NODE_ID not exported}
  img=${E2E_NODE_IMAGE_ID:?readiness: E2E_NODE_IMAGE_ID not exported}
  vcpus=${E2E_NODE_EXPECTED_VCPUS:?readiness: E2E_NODE_EXPECTED_VCPUS not exported}
  mem=${E2E_NODE_EXPECTED_MEMORY_MIB:?readiness: E2E_NODE_EXPECTED_MEMORY_MIB not exported}
  disk=${E2E_NODE_EXPECTED_DISK_GIB:?readiness: E2E_NODE_EXPECTED_DISK_GIB not exported}
  qemu=${E2E_NODE_ADMITTED_QEMU_VERSION:?readiness: E2E_NODE_ADMITTED_QEMU_VERSION not exported}
  epoch=${E2E_NODE_START_EPOCH:?readiness: E2E_NODE_START_EPOCH not exported}
  e2e_pg_env || return 1
  n=$(pg_query "SELECT count(*) FROM public.nodes WHERE id=:'nid' AND default_image=:'img'
      AND cpu_model<>'' AND qemu_version=:'qemu'
      AND total_vcpus=:'vcpus'::int AND total_memory_mib=:'mem'::bigint
      AND total_disk_gib>0 AND total_disk_gib<=:'disk'::bigint
      AND reserved_vcpus=0 AND reserved_memory_mib=0 AND reserved_disk_gib=0
      AND last_seen_at>=to_timestamp(:'epoch'::bigint)
      AND last_seen_at>=now()-interval '120 seconds'" \
      -v nid="$nid" -v img="$img" -v vcpus="$vcpus" -v mem="$mem" \
      -v disk="$disk" -v qemu="$qemu" -v epoch="$epoch") || return 1
  [ "$(trim "$n")" = 1 ]
}

# ---------------------------------------------------------------------------
# Embedded Go helpers. The proxy is a recording reverse proxy (stdlib + json/v2
# only) with real httptest coverage; the validator is a generated go test
# importing internal/compat. Bounds: a JSON body above E2E_JSON_BOUND or an
# NDJSON frame above E2E_FRAME_BOUND is a harness-bound FAILURE record - never
# truncation, never a skip - charged per write, and the whole spool above
# E2E_TOTAL_SPOOL fails the run. Index/spool IO errors are fatal. Neither
# helper logs bodies, URLs, tokens or native errors.
# ---------------------------------------------------------------------------
e2e_write_go_helpers() { # $1 = helpers dir
  local d=$1
  mkdir -p "$d/proxy" "$d/validator"
  cat >"$d/proxy/main.go" <<'EOF'
// Command e2e-p1-proxy is the throwaway recording reverse proxy of
// deploy/dev/e2e-p1.sh. It fronts bbraft-server, spools every /api/v1
// response body (streaming, bounded, charged incrementally BEFORE any byte is
// buffered) to per-record files, captures the response headers, and appends
// one NDJSON index line per response. Deliberate one-shot aborts are armed by
// files in the capture directory: "arm-cut" holds "sandboxId:minStdoutFrames"
// and binds to the exact GET attach stream of the most recently detached
// process of that sandbox (method, path segments and pid all match);
// "arm-cancel" holds the exact stream path of the response a SIGINT-interrupted
// client abandons, and only an actual client-side abort is classified as a
// cancel. The armed cut fires at a flushed complete frame boundary - the
// committed frame is written AND flushed successfully first, the abort
// happens before any later frame is forwarded - and the ErrAbortHandler
// sentinel is re-panicked after the record is persisted so the connection
// truly aborts. Any other aborted, truncated or oversized stream is a failure
// record. Index, header, spool and audit IO errors are fatal for the run. A
// loopback-only control route (/__e2e/barrier) reports one consistent
// snapshot of the assignment/accepted/completed counters. It logs fixed
// classes only.
package main

import (
	"bytes"
	"context"
	"encoding/json/v2"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// barrierPath is the loopback-only control route; it is never forwarded and
// never captured as a v1 record.
const barrierPath = "/__e2e/barrier"

var (
	errHarnessBound = errors.New("harness bound exceeded")
	errAuditWrite   = errors.New("audit write failed")
	errShutdown     = errors.New("shutdown not graceful")
	errJoin         = errors.New("handlers did not finish")
)

func envInt(name string, def int64) int64 {
	if v := os.Getenv(name); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n > 0 {
			return n
		}
	}
	return def
}

type indexLine struct {
	Seq           int64  `json:"seq"`
	Method        string `json:"method"`
	Path          string `json:"path"`
	Status        int    `json:"status"`
	ContentType   string `json:"contentType"`
	Class         string `json:"class"`                // json|ndjson|cut|cancel|fail
	FailReason    string `json:"failReason,omitempty"` // harness-bound|unexpected-abort|unexpected-eof|upstream-abort|upstream-error|total-spool|spool-open|spool-write|bad-frame|short-write|flush
	File          string `json:"file"`
	BodyBytes     int64  `json:"bodyBytes"`
	Frames        int64  `json:"frames"`
	StdoutFrames  int64  `json:"stdoutFrames"`
	LastNextOff   int64  `json:"lastNextOffset"`
	LastStderrOff int64  `json:"lastStderrOffset"`
	TailBytes     int64  `json:"tailBytes"`
	Terminal      bool   `json:"terminalFrame,omitempty"`
	ClientAborted bool   `json:"clientAborted,omitempty"`
}

type auditLine struct {
	Accepted  int64 `json:"accepted"`
	Completed int64 `json:"completed"`
	Failed    int64 `json:"failed"`
	Fatal     bool  `json:"fatal"`
}

type proxy struct {
	upstream   *url.URL
	rp         *httputil.ReverseProxy
	capDir     string
	jsonBound  int64
	frameBound int64
	totalCap   int64
	logger     *slog.Logger

	// seq/accepted/completed/failed are only ever mutated under mu so the
	// barrier snapshot is consistent; they stay atomic for cheap reads.
	seq       atomic.Int64
	accepted  atomic.Int64
	completed atomic.Int64
	failed    atomic.Int64
	inflight  sync.WaitGroup

	mu          sync.Mutex
	spool       int64
	nonAPI      int64
	fatal       bool
	cutClaimed  bool
	cutSandbox  string
	cutMin      int64
	detachedPID map[string]string // sandbox id -> pid of its latest detached start
}

// fixedLogWriter swallows native ReverseProxy error text and emits one fixed
// class instead, so no upstream error or URL is ever logged.
type fixedLogWriter struct{ l *slog.Logger }

func (f fixedLogWriter) Write(p []byte) (int, error) {
	f.l.Warn("upstream-log", "class", "fixed")
	return len(p), nil
}

func newProxy(upstream *url.URL, capDir string, logger *slog.Logger) *proxy {
	p := &proxy{
		upstream:    upstream,
		capDir:      capDir,
		jsonBound:   envInt("E2E_JSON_BOUND", 128<<20),
		frameBound:  envInt("E2E_FRAME_BOUND", 16<<20),
		totalCap:    envInt("E2E_TOTAL_SPOOL", 512<<20),
		logger:      logger,
		detachedPID: map[string]string{},
	}
	p.rp = &httputil.ReverseProxy{
		Rewrite: func(r *httputil.ProxyRequest) {
			r.SetURL(p.upstream)
			r.Out.Host = r.In.Host
		},
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			p.logger.Warn("upstream-error", "class", "upstream-unreachable")
			// An unanswerable upstream is a failure record, never a silent 200.
			if rec, ok := w.(*recorder); ok {
				rec.fail("upstream-error")
			}
			w.WriteHeader(http.StatusBadGateway)
		},
		ErrorLog: log.New(fixedLogWriter{logger}, "", 0),
	}
	return p
}

func main() {
	listen := flag.String("listen", "127.0.0.1:7702", "listen address")
	upstream := flag.String("upstream", "127.0.0.1:7700", "upstream address")
	capDir := flag.String("capture", "", "capture directory")
	flag.Parse()
	if *capDir == "" {
		fmt.Fprintln(os.Stderr, "e2e-p1-proxy: -capture is required")
		os.Exit(2)
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	p := newProxy(&url.URL{Scheme: "http", Host: *upstream}, *capDir, logger)
	srv := &http.Server{Handler: p}
	ln, err := netListen(*listen)
	if err != nil {
		logger.Error("listen", "class", "listen")
		os.Exit(1)
	}
	logger.Info("proxy listening", "bounds",
		fmt.Sprintf("json=%d frame=%d total=%d", p.jsonBound, p.frameBound, p.totalCap))
	go func() {
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("serve", "class", "serve")
			p.setFatal()
		}
	}()
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	<-ctx.Done()
	if err := p.drain(srv, 10*time.Second, 5*time.Second); err != nil {
		logger.Error("drain", "class", "drain")
		os.Exit(1)
	}
	if p.isFatal() {
		os.Exit(1)
	}
}

func netListen(addr string) (net.Listener, error) {
	return net.Listen("tcp", addr)
}

// drain stops accepting, gives active handlers shutdownTimeout to finish,
// force-closes every connection on a timeout, JOINS all handlers (bounded by
// joinTimeout) and only then writes the final audit. Any shutdown failure,
// unjoined handler or audit write failure is fatal and returned; the returned
// errors are fixed classes and never carry native text.
func (p *proxy) drain(srv *http.Server, shutdownTimeout, joinTimeout time.Duration) error {
	var failure error
	ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		_ = srv.Close() // force-close active connections
		p.setFatal()
		failure = errShutdown
	}
	done := make(chan struct{})
	go func() { p.inflight.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(joinTimeout):
		p.setFatal()
		failure = errors.Join(failure, errJoin)
	}
	if err := p.writeAudit(); err != nil {
		p.setFatal()
		failure = errors.Join(failure, errAuditWrite)
	}
	return failure
}

func (p *proxy) setFatal() {
	p.mu.Lock()
	p.fatal = true
	p.mu.Unlock()
	// The in-memory flag already decides exit status and the audit; the
	// marker is a best-effort second witness for a crashed audit.
	_ = writeFile0600(filepath.Join(p.capDir, "proxy-fatal"), []byte("fatal\n"))
}

func (p *proxy) isFatal() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.fatal
}

// writeFile0600 creates/truncates path and checks write AND close errors.
func writeFile0600(path string, data []byte) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

// writeAudit atomically publishes the final counters (temp file + rename).
func (p *proxy) writeAudit() error {
	p.mu.Lock()
	a := auditLine{
		Accepted: p.accepted.Load(), Completed: p.completed.Load(),
		Failed: p.failed.Load(), Fatal: p.fatal,
	}
	p.mu.Unlock()
	b, err := json.Marshal(a)
	if err != nil {
		return errAuditWrite
	}
	tmp := filepath.Join(p.capDir, "audit.json.tmp")
	if err := writeFile0600(tmp, append(b, '\n')); err != nil {
		return errAuditWrite
	}
	if err := os.Rename(tmp, filepath.Join(p.capDir, "audit.json")); err != nil {
		return errAuditWrite
	}
	return nil
}

// begin assigns the next sequence and counts the request as accepted in one
// critical section, so the barrier never sees one without the other.
func (p *proxy) begin() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.accepted.Add(1)
	return p.seq.Add(1)
}

// finish counts one finalized record (failed iff class is a failure).
func (p *proxy) finish(failed bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if failed {
		p.failed.Add(1)
	}
	p.completed.Add(1)
}

// serveBarrier answers the loopback-only snapshot route. The ACTUAL peer
// address decides: forwarded headers and the Host header are never trusted,
// and a non-loopback peer gets a plain 404 (the route does not exist for it).
func (p *proxy) serveBarrier(w http.ResponseWriter, r *http.Request) {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	ip := net.ParseIP(host)
	if err != nil || ip == nil || !ip.IsLoopback() || r.Method != http.MethodGet {
		http.NotFound(w, r)
		return
	}
	p.mu.Lock()
	snap := struct {
		Seq       int64 `json:"seq"`
		Accepted  int64 `json:"accepted"`
		Completed int64 `json:"completed"`
		Failed    int64 `json:"failed"`
		Fatal     bool  `json:"fatal"`
	}{p.seq.Load(), p.accepted.Load(), p.completed.Load(), p.failed.Load(), p.fatal}
	p.mu.Unlock()
	b, err := json.Marshal(snap)
	if err != nil {
		http.Error(w, "barrier", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(append(b, '\n'))
}

func (p *proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == barrierPath {
		p.serveBarrier(w, r)
		return
	}
	if !strings.HasPrefix(r.URL.Path, "/api/v1") {
		p.mu.Lock()
		p.nonAPI++
		p.mu.Unlock()
		p.rp.ServeHTTP(w, r)
		return
	}
	p.inflight.Add(1)
	defer p.inflight.Done() // registered first: runs after finalize
	rec := newRecorder(w, p, r)
	defer rec.finalize()
	p.rp.ServeHTTP(rec, r)
}

// recorder tees the response body to a bounded spool file while streaming it
// to the client, with Flusher passthrough. Armed streams forward only
// complete flushed frames so a deliberate cut lands exactly between frames.
type recorder struct {
	p   *proxy
	w   http.ResponseWriter
	r   *http.Request
	seq int64

	status    int
	ct        string
	headers   http.Header
	file      *os.File
	spoolName string
	ndjson    bool
	sniff     []byte // bounded JSON sniff buffer for detached processId

	line       []byte // partial frame (unarmed path)
	pending    []byte // unforwarded bytes of ONE frame in progress (armed path)
	frames     int64
	stdout     int64
	lastOff    int64
	lastErrOff int64
	terminal   bool
	complete   int64 // bytes through the last complete frame
	bodyBytes  int64
	tail       int64
	failReason string
	spoolStop  bool

	cutTarget    bool
	cancelTarget bool
	cutDone      bool
	clientAbort  bool
	flushFailed  bool
}

func newRecorder(w http.ResponseWriter, p *proxy, r *http.Request) *recorder {
	rec := &recorder{p: p, w: w, r: r, seq: p.begin()}
	rec.claimArms()
	return rec
}

// fail records the FIRST fixed failure class of this record.
func (rec *recorder) fail(reason string) {
	if rec.failReason == "" {
		rec.failReason = reason
	}
}

// streamPath returns ("sandbox","pid",ok) for an exact
// /api/v1/sandboxes/{id}/commands/{pid}/stream GET.
func streamPath(r *http.Request) (string, string, bool) {
	if r.Method != http.MethodGet {
		return "", "", false
	}
	s := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(s) != 7 || s[0] != "api" || s[1] != "v1" || s[2] != "sandboxes" ||
		s[3] == "" || s[4] != "commands" || s[5] == "" || s[6] != "stream" {
		return "", "", false
	}
	return s[3], s[5], true
}

// claimArms consumes the one-shot arming files for this request. The cut
// claims the attach stream of the exact sandbox whose latest detached start
// returned the pid in the path; the cancel claims the exact path.
func (rec *recorder) claimArms() {
	p := rec.p
	sandbox, pid, isStream := streamPath(rec.r)
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.cutClaimed {
		if b, err := os.ReadFile(filepath.Join(p.capDir, "arm-cut")); err == nil {
			minFrames := int64(2)
			spec := strings.TrimSpace(string(b))
			if i := strings.IndexByte(spec, ':'); i >= 0 {
				if n, err := strconv.ParseInt(strings.TrimSpace(spec[i+1:]), 10, 64); err == nil && n > 0 {
					minFrames = n
				}
				spec = strings.TrimSpace(spec[:i])
			}
			if isStream && spec != "" && sandbox == spec && p.detachedPID[sandbox] == pid {
				_ = os.Remove(filepath.Join(p.capDir, "arm-cut"))
				p.cutClaimed, p.cutSandbox, p.cutMin = true, spec, minFrames
				rec.cutTarget = true
			}
		}
	}
	if b, err := os.ReadFile(filepath.Join(p.capDir, "arm-cancel")); err == nil {
		if target := strings.TrimSpace(string(b)); target != "" && target == rec.r.URL.Path {
			_ = os.Remove(filepath.Join(p.capDir, "arm-cancel"))
			rec.cancelTarget = true
		}
	}
}

// trackDetached sniffs {"processId":N} out of a completed JSON command start
// so the cut can bind to the exact pid.
func (rec *recorder) trackDetached() {
	if rec.r.Method != http.MethodPost || rec.ndjson || rec.status != 200 {
		return
	}
	if !strings.HasSuffix(rec.r.URL.Path, "/commands") {
		return
	}
	var v struct {
		ProcessID int64 `json:"processId"`
	}
	if err := json.Unmarshal(rec.sniff, &v); err != nil || v.ProcessID == 0 {
		return
	}
	segs := strings.Split(strings.Trim(rec.r.URL.Path, "/"), "/")
	if len(segs) != 5 || segs[4] != "commands" || segs[3] == "" {
		return
	}
	rec.p.mu.Lock()
	rec.p.detachedPID[segs[3]] = strconv.FormatInt(v.ProcessID, 10)
	rec.p.mu.Unlock()
}

func (rec *recorder) Header() http.Header { return rec.w.Header() }

func (rec *recorder) WriteHeader(status int) {
	rec.status = status
	rec.ct = rec.w.Header().Get("Content-Type")
	rec.headers = rec.w.Header().Clone()
	rec.openSink()
	rec.captureHeaders()
	rec.w.WriteHeader(status)
}

// captureHeaders persists the response headers; every failure is fatal.
func (rec *recorder) captureHeaders() {
	b, err := json.Marshal(rec.headers)
	if err != nil {
		rec.p.setFatal()
		return
	}
	name := fmt.Sprintf("rec-%06d.headers.json", rec.seq)
	if err := writeFile0600(filepath.Join(rec.p.capDir, name), b); err != nil {
		rec.p.setFatal()
	}
}

func (rec *recorder) openSink() {
	if rec.file != nil || rec.spoolName != "" {
		return
	}
	rec.ndjson = strings.Contains(rec.ct, "application/x-ndjson")
	rec.spoolName = fmt.Sprintf("rec-%06d.body", rec.seq)
	f, err := os.OpenFile(filepath.Join(rec.p.capDir, rec.spoolName),
		os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		rec.failReason, rec.spoolName = "spool-open", ""
		rec.p.setFatal()
		return
	}
	rec.file = f
}

func (rec *recorder) Write(b []byte) (int, error) {
	if rec.status == 0 {
		rec.WriteHeader(http.StatusOK)
	}
	if rec.armedStream() {
		return rec.writeFramed(b)
	}
	n, err := rec.w.Write(b)
	if n > 0 {
		rec.consume(b[:n], false)
	}
	if err != nil {
		rec.clientAbort = true
	} else if n < len(b) {
		rec.fail("short-write")
		err = io.ErrShortWrite
	}
	return n, err
}

// armedStream buffers partial frames so the client only ever receives whole
// flushed frames; the deliberate cut then lands between frames.
func (rec *recorder) armedStream() bool {
	return rec.ndjson && (rec.cutTarget || rec.cancelTarget)
}

// writeFramed forwards one complete frame at a time: bound and charge the
// bytes BEFORE buffering them, forward, flush (error-aware), only then commit
// the frame (spool, cursors) and decide the cut before any later frame is
// forwarded. Nothing is committed for a frame whose write or flush failed.
func (rec *recorder) writeFramed(b []byte) (int, error) {
	total := len(b)
	for len(b) > 0 {
		i := bytes.IndexByte(b, '\n')
		chunk := b
		if i >= 0 {
			chunk = b[:i+1]
		}
		b = b[len(chunk):]
		// Incremental bounds: a partial frame is checked and charged now.
		frameLen := int64(len(rec.pending)) + int64(len(chunk))
		if i >= 0 {
			frameLen-- // the newline is not frame content
		}
		if frameLen > rec.p.frameBound {
			rec.fail("harness-bound")
			rec.spoolStop, rec.pending = true, nil
			return 0, errHarnessBound
		}
		if !rec.chargeOK(len(chunk)) {
			rec.fail("total-spool")
			rec.spoolStop, rec.pending = true, nil
			return 0, errHarnessBound
		}
		rec.pending = append(rec.pending, chunk...)
		if i < 0 {
			continue // partial frame: wait for its newline
		}
		frame := rec.pending
		n, err := rec.w.Write(frame)
		switch {
		case err != nil:
			rec.clientAbort = true
			return 0, err
		case n < len(frame):
			rec.fail("short-write")
			return 0, io.ErrShortWrite
		}
		if err := http.NewResponseController(rec.w).Flush(); err != nil {
			rec.clientAbort, rec.flushFailed = true, true
			if !rec.cancelTarget {
				rec.fail("flush")
			}
			return 0, err
		}
		rec.consume(frame, true)
		rec.pending = rec.pending[:0]
		if rec.cutReady() {
			rec.cutDone = true
			rec.p.logger.Info("cut", "class", "cut")
			panic(http.ErrAbortHandler)
		}
	}
	return total, nil
}

// FlushError lets httputil.ReverseProxy flush through the recorder with the
// real error preserved (http.NewResponseController finds this method first).
func (rec *recorder) FlushError() error {
	return http.NewResponseController(rec.w).Flush()
}

// cutReady fires the armed cut at a complete flushed frame boundary - the
// committed frame was written and flushed, and evaluation happens between
// frames, so any still-unprocessed LATER bytes are simply never forwarded -
// once enough stdout frames were committed and no terminal frame was seen
// (the stream provably continues).
func (rec *recorder) cutReady() bool {
	return rec.cutTarget && !rec.cutDone && !rec.terminal && rec.stdout >= rec.p.cutMin
}

// consume streams delivered bytes into the bounded spool and splits NDJSON
// frames. precharged bytes were already charged against the total spool
// before they were buffered (armed path) and are not charged twice.
func (rec *recorder) consume(b []byte, precharged bool) {
	rec.bodyBytes += int64(len(b))
	if !rec.ndjson && len(rec.sniff) < 4096 {
		rec.sniff = append(rec.sniff, b...)
		if len(rec.sniff) > 4096 {
			rec.sniff = rec.sniff[:4096]
		}
	}
	if rec.file == nil || rec.failReason != "" || rec.spoolStop {
		return // failed open, already failed, or spool stopped: count only
	}
	if !precharged && !rec.chargeOK(len(b)) {
		rec.failReason, rec.spoolStop = "total-spool", true
		return
	}
	if !rec.ndjson {
		if rec.bodyBytes > rec.p.jsonBound {
			rec.failReason, rec.spoolStop = "harness-bound", true
			return
		}
		if n, err := rec.file.Write(b); err != nil {
			rec.failReason = "spool-write"
			rec.p.setFatal()
		} else {
			rec.complete += int64(n)
		}
		return
	}
	for len(b) > 0 {
		i := bytes.IndexByte(b, '\n')
		if i < 0 {
			rec.line = append(rec.line, b...)
			if int64(len(rec.line)) > rec.p.frameBound {
				rec.failReason, rec.spoolStop = "harness-bound", true
			}
			return
		}
		rec.line = append(rec.line, b[:i]...)
		b = b[i+1:]
		if int64(len(rec.line)) > rec.p.frameBound {
			rec.failReason, rec.spoolStop = "harness-bound", true
			return
		}
		rec.frameLine(string(rec.line))
		rec.line = rec.line[:0]
	}
}

func (rec *recorder) chargeOK(n int) bool {
	rec.p.mu.Lock()
	rec.p.spool += int64(n)
	over := rec.p.spool > rec.p.totalCap
	rec.p.mu.Unlock()
	return !over
}

// frameLine spools one complete NDJSON frame, tracks per-stream committed
// offsets and terminal frames.
func (rec *recorder) frameLine(line string) {
	var f struct {
		Type    string `json:"type"`
		NextOff int64  `json:"nextOffset"`
	}
	if err := json.Unmarshal([]byte(line), &f); err != nil || rec.terminal {
		rec.fail("bad-frame") // unparsable, or any frame after a terminal one
	} else {
		switch f.Type {
		case "stdout":
			rec.stdout++
			if f.NextOff > 0 {
				rec.lastOff = f.NextOff
			}
		case "stderr":
			if f.NextOff > 0 {
				rec.lastErrOff = f.NextOff
			}
		case "exit", "error", "reconnect":
			rec.terminal = true
		}
	}
	if n, err := rec.file.WriteString(line + "\n"); err != nil {
		rec.failReason = "spool-write"
		rec.p.setFatal()
	} else {
		rec.frames++
		rec.complete += int64(n)
	}
}

// finalize classifies the record, closes the spool and appends the index
// line. It runs even after an ErrAbortHandler panic, persists first and then
// re-panics the sentinel so the connection truly aborts.
func (rec *recorder) finalize() {
	aborted := false
	if e := recover(); e != nil {
		if e == http.ErrAbortHandler {
			aborted = true
		} else {
			rec.persist(aborted)
			panic(e)
		}
	}
	rec.persist(aborted)
	if aborted {
		panic(http.ErrAbortHandler)
	}
}

func (rec *recorder) persist(aborted bool) {
	if rec.file != nil {
		if err := rec.file.Close(); err != nil {
			rec.p.setFatal()
		}
	}
	// A client-side abandonment shows as the INBOUND request context being
	// canceled (ReverseProxy watches it) or a failed client write/flush; an
	// upstream fault leaves the inbound context alive.
	clientGone := rec.clientAbort || rec.r.Context().Err() != nil
	rec.tail = rec.bodyBytes - rec.complete
	class, reason := "json", ""
	switch {
	case rec.failReason != "":
		class, reason = "fail", rec.failReason
	case rec.status == 0:
		class, reason = "fail", "unexpected-abort"
	case aborted && rec.cutDone:
		class = "cut"
	case aborted && rec.cancelTarget && clientGone:
		class = "cancel"
	case aborted && clientGone:
		class, reason = "fail", "unexpected-abort"
	case aborted:
		class, reason = "fail", "upstream-abort"
	case rec.ndjson && (rec.tail != 0 || len(rec.pending) != 0):
		class, reason = "fail", "unexpected-eof"
	case rec.ndjson:
		class = "ndjson"
	}
	if !rec.ndjson {
		rec.trackDetached()
	}
	line := indexLine{
		Seq: rec.seq, Method: rec.r.Method, Path: rec.r.URL.RequestURI(),
		Status: rec.status, ContentType: rec.ct, Class: class, FailReason: reason,
		File: rec.spoolName, BodyBytes: rec.bodyBytes, Frames: rec.frames,
		StdoutFrames: rec.stdout, LastNextOff: rec.lastOff, LastStderrOff: rec.lastErrOff,
		TailBytes: rec.tail, Terminal: rec.terminal, ClientAborted: clientGone,
	}
	rec.writeIndex(line)
	rec.p.finish(class == "fail")
	rec.p.logger.Info("record", "seq", rec.seq, "method", rec.r.Method,
		"status", rec.status, "class", class, "bytes", rec.bodyBytes)
}

// writeIndex appends one index line; open, marshal, write and Close errors
// are all fatal for the run.
func (rec *recorder) writeIndex(line indexLine) {
	f, err := os.OpenFile(filepath.Join(rec.p.capDir, "index.ndjson"),
		os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		rec.p.setFatal()
		return
	}
	b, err := json.Marshal(line)
	if err != nil {
		_ = f.Close()
		rec.p.setFatal()
		return
	}
	if _, err := f.Write(append(b, '\n')); err != nil {
		rec.p.setFatal()
	}
	if err := f.Close(); err != nil {
		rec.p.setFatal()
	}
}
EOF
  cat >"$d/proxy/main_test.go" <<'EOF'
// Package main tests exercise the actual recorder against a real httptest
// upstream through the real proxy handler: complete and truncated streams,
// the deliberate cut (frame-bound forwarding, flush, cursor, no terminal in
// prefix, true abort), coalesced and split frames, the client cancel with
// tail accounting, per-write spool bounds, IO failure fatality, and the
// audit barrier. No native error, URL or body is asserted into evidence.
package main

import (
	"bufio"
	"encoding/json/v2"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// testClient bounds every client call so a wedged proxy fails the test fast.
var testClient = &http.Client{Timeout: 10 * time.Second}

type fixture struct {
	up      *httptest.Server
	proxy   *httptest.Server
	p       *proxy
	capDir  string
	mu      sync.Mutex
	handler http.HandlerFunc
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	capDir := t.TempDir()
	p := newProxy(nil, capDir, newDiscardLogger())
	f := &fixture{capDir: capDir, p: p}
	f.up = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.serveUpstream(w, r)
	}))
	t.Cleanup(f.up.Close)
	target := mustURL(t, f.up.URL)
	p.upstream = target
	f.proxy = httptest.NewServer(p)
	t.Cleanup(f.proxy.Close)
	return f
}

func mustURL(t *testing.T, raw string) *url.URL {
	t.Helper()
	u, err := parseURL(raw)
	if err != nil {
		t.Fatal(err)
	}
	return u
}

func (f *fixture) serveUpstream(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	handler := f.handler
	f.mu.Unlock()
	if handler != nil {
		handler(w, r)
		return
	}
	switch {
	case r.URL.Path == "/api/v1/me":
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true,"type":"user.info","user":{}}`))
	case strings.HasSuffix(r.URL.Path, "/commands") && r.Method == "POST":
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true,"processId":7,"commandId":"cmd_1"}`))
	case strings.HasSuffix(r.URL.Path, "/stream"):
		w.Header().Set("Content-Type", "application/x-ndjson")
		fl := w.(http.Flusher)
		_, _ = w.Write([]byte("{\"type\":\"attached\",\"processId\":7,\"stdoutOffset\":0,\"stderrOffset\":0}\n"))
		fl.Flush()
		for i := 0; i < 5; i++ {
			_, _ = w.Write([]byte("{\"type\":\"stdout\",\"data\":\"chunk" + itoa(i) + "\",\"nextOffset\":" + itoa((i+1)*7) + "}\n"))
			fl.Flush()
		}
		_, _ = w.Write([]byte("{\"type\":\"exit\",\"status\":\"exited\",\"exitCode\":0,\"stdoutOffset\":35,\"stderrOffset\":0}\n"))
	}
}

func itoa(i int) string {
	return strings.TrimSpace(strings.Replace(jsonOf(i), "\n", "", -1))
}

func jsonOf(v any) string { b, _ := json.Marshal(v); return string(b) }

func (f *fixture) setHandler(h http.HandlerFunc) {
	f.mu.Lock()
	f.handler = h
	f.mu.Unlock()
}

func (f *fixture) index(t *testing.T) []indexLine {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(f.capDir, "index.ndjson"))
	if err != nil {
		t.Fatalf("index: %v", err)
	}
	var lines []indexLine
	for _, l := range strings.Split(strings.TrimSpace(string(b)), "\n") {
		if l == "" {
			continue
		}
		var il indexLine
		if err := json.Unmarshal([]byte(l), &il); err != nil {
			t.Fatalf("index line: %v", err)
		}
		lines = append(lines, il)
	}
	return lines
}

func getBody(t *testing.T, url string) (int, []byte, error) {
	t.Helper()
	resp, err := testClient.Get(url)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	return resp.StatusCode, b, err
}

func TestJSONRecordHeadersAndIndex(t *testing.T) {
	f := newFixture(t)
	code, body, err := getBody(t, f.proxy.URL+"/api/v1/me")
	if err != nil || code != 200 {
		t.Fatalf("get: code=%d err=%v", code, err)
	}
	if !strings.Contains(string(body), "user.info") {
		t.Fatal("body not proxied")
	}
	ix := f.index(t)
	if len(ix) != 1 || ix[0].Class != "json" || ix[0].Status != 200 {
		t.Fatalf("index: %+v", ix)
	}
	hb, err := os.ReadFile(filepath.Join(f.capDir, "rec-000001.headers.json"))
	if err != nil {
		t.Fatalf("headers file: %v", err)
	}
	if !strings.Contains(string(hb), "application/json") {
		t.Fatal("headers not captured")
	}
	bb, err := os.ReadFile(filepath.Join(f.capDir, ix[0].File))
	if err != nil || len(bb) != int(ix[0].BodyBytes) {
		t.Fatalf("spool mismatch: %v %d %d", err, len(bb), ix[0].BodyBytes)
	}
}

func TestNDJSONCompleteStreamTerminalState(t *testing.T) {
	f := newFixture(t)
	code, body, err := getBody(t, f.proxy.URL+"/api/v1/sandboxes/bx_t/commands/7/stream")
	if err != nil || code != 200 {
		t.Fatalf("stream: code=%d err=%v", code, err)
	}
	if got := strings.Count(string(body), "\n"); got != 7 {
		t.Fatalf("frames forwarded: %d", got)
	}
	ix := f.index(t)
	last := ix[len(ix)-1]
	if last.Class != "ndjson" || last.Frames != 7 || last.TailBytes != 0 || !last.Terminal {
		t.Fatalf("index: %+v", last)
	}
	if last.LastNextOff != 35 {
		t.Fatalf("cursor: %d", last.LastNextOff)
	}
}

func TestCoalescedAndSplitFrames(t *testing.T) {
	f := newFixture(t)
	f.setHandler(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/x-ndjson")
		fl := w.(http.Flusher)
		// three frames in ONE write (coalesced)
		_, _ = w.Write([]byte("{\"type\":\"stdout\",\"data\":\"a\",\"nextOffset\":1}\n" +
			"{\"type\":\"stdout\",\"data\":\"b\",\"nextOffset\":2}\n" +
			"{\"type\":\"exit\",\"exitCode\":0}\n"))
		fl.Flush()
	})
	code, _, err := getBody(t, f.proxy.URL+"/api/v1/sandboxes/bx_t/commands/9/stream")
	if err != nil || code != 200 {
		t.Fatalf("coalesced: %v", err)
	}
	ix := f.index(t)
	if ix[0].Frames != 3 || ix[0].Class != "ndjson" {
		t.Fatalf("coalesced index: %+v", ix[0])
	}

	f2 := newFixture(t)
	f2.setHandler(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/x-ndjson")
		fl := w.(http.Flusher)
		// one frame split across TWO writes
		_, _ = w.Write([]byte("{\"type\":\"stdout\",\"da"))
		fl.Flush()
		_, _ = w.Write([]byte("ta\":\"a\",\"nextOffset\":1}\n{\"type\":\"exit\",\"exitCode\":0}\n"))
		fl.Flush()
	})
	code, _, err = getBody(t, f2.proxy.URL+"/api/v1/sandboxes/bx_t/commands/9/stream")
	if err != nil || code != 200 {
		t.Fatalf("split: %v", err)
	}
	ix = f2.index(t)
	if ix[0].Frames != 2 || ix[0].TailBytes != 0 || ix[0].Class != "ndjson" {
		t.Fatalf("split index: %+v", ix[0])
	}
}

func TestUnexpectedTruncationFails(t *testing.T) {
	f := newFixture(t)
	f.setHandler(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/x-ndjson")
		_, _ = w.Write([]byte("{\"type\":\"stdout\",\"data\":\"partial-no-newline\""))
	})
	_, _, _ = getBody(t, f.proxy.URL+"/api/v1/sandboxes/bx_t/commands/9/stream")
	ix := f.index(t)
	if ix[0].Class != "fail" || ix[0].FailReason != "unexpected-eof" {
		t.Fatalf("index: %+v", ix[0])
	}
}

func TestCutFlushesFrameAndAbortsBetweenFrames(t *testing.T) {
	f := newFixture(t)
	// Bind the pid first: the default upstream POST returns processId 7.
	resp, err := testClient.Post(f.proxy.URL+"/api/v1/sandboxes/bx_t/commands",
		"application/json", strings.NewReader("{}"))
	if err != nil || resp.StatusCode != 200 {
		t.Fatalf("detach post: %v %v", err, resp)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	if err := os.WriteFile(filepath.Join(f.capDir, "arm-cut"), []byte("bx_t:2"), 0o600); err != nil {
		t.Fatal(err)
	}
	sresp, serr := http.Get(f.proxy.URL + "/api/v1/sandboxes/bx_t/commands/7/stream")
	if serr != nil {
		t.Fatal(serr)
	}
	br := bufio.NewReader(sresp.Body)
	var lines []string
	readErr := error(nil)
	for {
		l, err := br.ReadString('\n')
		if err != nil {
			readErr = err
			break
		}
		lines = append(lines, l)
	}
	if readErr == nil {
		t.Fatal("cut did not abort the client stream")
	}
	// The client saw only complete frames: attached + exactly 2 stdout.
	if len(lines) != 3 || !strings.HasPrefix(lines[2], "{\"type\":\"stdout\"") {
		t.Fatalf("client frames: %d", len(lines))
	}
	ix := f.index(t)
	var cut *indexLine
	for i := range ix {
		if ix[i].Class == "cut" {
			cut = &ix[i]
		}
	}
	if cut == nil {
		t.Fatalf("no cut record: %+v", ix)
	}
	if cut.TailBytes != 0 || cut.Terminal || cut.StdoutFrames != 2 || cut.LastNextOff != 14 {
		t.Fatalf("cut record: %+v", cut)
	}
	// The spool holds exactly the frames the client saw.
	b, _ := os.ReadFile(filepath.Join(f.capDir, cut.File))
	if got := strings.Count(string(b), "\n"); got != 3 {
		t.Fatalf("cut spool frames: %d", got)
	}
}

func TestCancelRequiresActualClientAbort(t *testing.T) {
	f := newFixture(t)
	// The upstream stays mid-stream until the proxy reports the client gone.
	gate := make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(gate) }) }
	t.Cleanup(release) // runs before the upstream server's Close (LIFO)
	f.setHandler(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/x-ndjson")
		fl := w.(http.Flusher)
		_, _ = w.Write([]byte("{\"type\":\"attached\",\"processId\":7,\"stdoutOffset\":0,\"stderrOffset\":0}\n"))
		fl.Flush()
		select {
		case <-gate:
		case <-r.Context().Done():
		}
		for range 3 {
			_, _ = w.Write([]byte("{\"type\":\"stdout\",\"data\":\"late\",\"nextOffset\":4}\n"))
			fl.Flush()
		}
		_, _ = w.Write([]byte("{\"type\":\"exit\",\"status\":\"exited\",\"exitCode\":0,\"stdoutOffset\":4,\"stderrOffset\":0}\n"))
	})
	if err := os.WriteFile(filepath.Join(f.capDir, "arm-cancel"),
		[]byte("/api/v1/sandboxes/bx_t/commands/7/stream"), 0o600); err != nil {
		t.Fatal(err)
	}
	resp, err := testClient.Get(f.proxy.URL + "/api/v1/sandboxes/bx_t/commands/7/stream")
	if err != nil {
		t.Fatal(err)
	}
	br := bufio.NewReader(resp.Body)
	if _, err := br.ReadString('\n'); err != nil { // attached frame
		t.Fatal(err)
	}
	_ = resp.Body.Close() // client abandons mid-stream
	// The upstream handler resumes only once the proxy has propagated the
	// abandonment (its request context ends), so no frame can race ahead of
	// the abort; the gate is just the bounded safety release.
	if !waitFor(t, f, "cancel") {
		ix := f.index(t)
		t.Fatalf("no cancel record: %+v", ix)
	}
	ix := f.index(t)
	var cancel *indexLine
	for i := range ix {
		if ix[i].Class == "cancel" {
			cancel = &ix[i]
		}
	}
	if cancel == nil {
		t.Fatalf("no cancel record: %+v", ix)
	}
	if !cancel.ClientAborted {
		t.Fatal("cancel without clientAborted flag")
	}
	// Explicit tail accounting: complete bytes + tail == body bytes.
	b, _ := os.ReadFile(filepath.Join(f.capDir, cancel.File))
	spooled := int64(0)
	for _, l := range strings.SplitAfter(string(b), "\n") {
		if strings.HasSuffix(l, "\n") {
			spooled += int64(len(l))
		}
	}
	if cancel.BodyBytes-spooled != cancel.TailBytes {
		t.Fatalf("cancel tail: %+v spooled=%d", cancel, spooled)
	}
}

func waitFor(t *testing.T, f *fixture, class string) bool {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		for _, l := range f.indexIfExists() {
			if l.Class == class {
				return true
			}
		}
		timeSleep()
	}
	return false
}

func (f *fixture) indexIfExists() []indexLine {
	b, err := os.ReadFile(filepath.Join(f.capDir, "index.ndjson"))
	if err != nil {
		return nil
	}
	var lines []indexLine
	for _, l := range strings.Split(strings.TrimSpace(string(b)), "\n") {
		if l == "" {
			continue
		}
		var il indexLine
		if json.Unmarshal([]byte(l), &il) == nil {
			lines = append(lines, il)
		}
	}
	return lines
}

func TestFrameBoundMidstreamFails(t *testing.T) {
	f := newFixture(t)
	f.p.frameBound = 64
	f.setHandler(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/x-ndjson")
		_, _ = w.Write([]byte("{\"type\":\"stdout\",\"data\":\"" + strings.Repeat("x", 200) + "\"}\n"))
	})
	_, _, _ = getBody(t, f.proxy.URL+"/api/v1/sandboxes/bx_t/commands/9/stream")
	ix := f.index(t)
	if ix[0].Class != "fail" || ix[0].FailReason != "harness-bound" {
		t.Fatalf("index: %+v", ix[0])
	}
}

func TestSpoolOpenFailureIsFatal(t *testing.T) {
	// A DIRECTORY where the spool file must be created makes the open fail
	// deterministically for every user, root included (chmod would not).
	capDir := t.TempDir()
	if err := os.Mkdir(filepath.Join(capDir, "rec-000001.body"), 0o700); err != nil {
		t.Fatal(err)
	}
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	t.Cleanup(up.Close)
	p := newProxy(mustURL(t, up.URL), capDir, newDiscardLogger())
	ts := httptest.NewServer(p)
	t.Cleanup(ts.Close)
	_, _, _ = getBody(t, ts.URL+"/api/v1/me")
	if !p.isFatal() {
		t.Fatal("spool-open failure was not fatal")
	}
	ix := waitIndex(t, &fixture{capDir: capDir}, 1)
	if ix[0].Class != "fail" || ix[0].FailReason != "spool-open" {
		t.Fatalf("index: %+v", ix[0])
	}
}

func TestTotalSpoolChargedPerWrite(t *testing.T) {
	f := newFixture(t)
	f.p.totalCap = 8 // smaller than one response
	_, _, _ = getBody(t, f.proxy.URL+"/api/v1/me")
	ix := f.index(t)
	if ix[0].Class != "fail" || ix[0].FailReason != "total-spool" {
		t.Fatalf("index: %+v", ix[0])
	}
}

func TestAuditBarrierCounts(t *testing.T) {
	f := newFixture(t)
	_, _, _ = getBody(t, f.proxy.URL+"/api/v1/me")
	_, _, _ = getBody(t, f.proxy.URL+"/api/v1/me")
	if got := f.p.accepted.Load(); got != 2 {
		t.Fatalf("accepted: %d", got)
	}
	if got := f.p.completed.Load(); got != 2 {
		t.Fatalf("completed: %d", got)
	}
	if got := f.p.failed.Load(); got != 0 {
		t.Fatalf("failed: %d", got)
	}
	if len(f.index(t)) != 2 {
		t.Fatal("index rows missing")
	}
	if f.p.seq.Load() != 2 {
		t.Fatalf("seq: %d", f.p.seq.Load())
	}
}
EOF
  cat >"$d/proxy/testsupport.go" <<'EOF'
package main

import (
	"io"
	"log/slog"
	"net/url"
	"time"
)

// Test support for main_test.go, kept out of the production file.

func newDiscardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func parseURL(raw string) (*url.URL, error) { return url.Parse(raw) }

func timeSleep() { time.Sleep(20 * time.Millisecond) }
EOF
  mkdir -p "$d/pgenv"
  cat >"$d/pgenv/main.go" <<'EOF'
// Command pgenv turns a PostgreSQL connection string into a 0600 shell env
// file for psql's environment - the password included there, never in argv or
// logs. It supports an explicit, documented libpq subset in BOTH the URI and
// the keyword/value form and REJECTS everything else before writing anything,
// so no option is ever silently dropped:
//
//	host port user password dbname sslmode sslrootcert sslcert sslkey
//	application_name options connect_timeout
//
// Rejected with a fixed diagnostic (never echoing DSN content): multi-host or
// multi-port lists, target_session_attrs, load_balance_hosts, service,
// passfile, channel_binding, gss/krb settings and every other or repeated
// key. host, port, user and dbname are OPTIONAL exactly as for pgx/libpq: an
// absent one is unset in the env file so psql applies the same defaults pgx
// does (unix socket or localhost, port 5432, the OS user, dbname = user); an
// empty DSN is refused as a harness guard. Percent-encoding (URI) and quoting
// and backslash escapes (keyword form) are decoded. Documented harness bounds:
// connect_timeout becomes min(value, 3) seconds (3 when absent or not
// positive) and PGOPTIONS always ends with "-c statement_timeout=2000" (the
// last -c wins, so a DSN statement_timeout cannot lift it). Every libpq PG*
// variable the DSN does not set is unset in the env file, so a stale ambient
// PGSSLMODE, PGSERVICE and friends cannot change the connection. The parse
// is cross-checked against pgx for the identity fields (host, port, user,
// password, dbname) and performs no network I/O.
package main

import (
	"errors"
	"flag"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
)

// dsnError is a fixed-class diagnostic: it never carries DSN content.
type dsnError string

func (e dsnError) Error() string { return "pgenv: " + string(e) }

const (
	errInvalid     = dsnError("invalid database URL")
	errUnsupported = dsnError("unsupported database URL option")
	errMultiHost   = dsnError("unsupported multi-host database URL")
	errEmpty       = dsnError("empty database URL")
	errValue       = dsnError("invalid database URL option value")
	errMismatch    = dsnError("database URL parse disagrees with pgx")
)

// supported maps each accepted key to the libpq environment variable psql
// reads for it.
var supported = map[string]string{
	"host": "PGHOST", "port": "PGPORT", "user": "PGUSER", "password": "PGPASSWORD",
	"dbname": "PGDATABASE", "sslmode": "PGSSLMODE", "sslrootcert": "PGSSLROOTCERT",
	"sslcert": "PGSSLCERT", "sslkey": "PGSSLKEY", "application_name": "PGAPPNAME",
	"options": "PGOPTIONS", "connect_timeout": "PGCONNECT_TIMEOUT",
}

// alwaysUnset are libpq variables with no supported DSN key: ambient values
// must not influence the connection.
var alwaysUnset = []string{
	"PGHOSTADDR", "PGSERVICE", "PGSERVICEFILE", "PGPASSFILE", "PGSSLCRL", "PGSSLCRLDIR",
	"PGSSLCOMPRESSION", "PGSSLSNI", "PGGSSENCMODE", "PGKRBSRVNAME", "PGGSSLIB",
	"PGCHANNELBINDING", "PGTARGETSESSIONATTRS", "PGLOADBALANCEHOSTS", "PGREQUIRESSL",
	"PGREQUIREAUTH", "PGSSLMINPROTOCOLVERSION", "PGSSLMAXPROTOCOLVERSION",
	"PGMINPROTOCOLVERSION", "PGMAXPROTOCOLVERSION", "PGCLIENTENCODING", "PGTZ",
	"PGDATESTYLE", "PGGEQO", "PGSYSCONFDIR", "PGREALM", "PGSSLKEYLOGFILE",
}

var sslModes = []string{"disable", "allow", "prefer", "require", "verify-ca", "verify-full"}

func shellQuote(v string) string {
	return "'" + strings.ReplaceAll(v, "'", `'\''`) + "'"
}

// parseKeywords parses the libpq keyword/value form.
func parseKeywords(s string) (map[string]string, error) {
	out := map[string]string{}
	i := 0
	skip := func() {
		for i < len(s) && (s[i] == ' ' || s[i] == '\t' || s[i] == '\n' || s[i] == '\r' || s[i] == '\f' || s[i] == '\v') {
			i++
		}
	}
	for {
		skip()
		if i >= len(s) {
			return out, nil
		}
		start := i
		for i < len(s) && s[i] != '=' && s[i] != ' ' && s[i] != '\t' && s[i] != '\n' {
			i++
		}
		key := s[start:i]
		skip()
		if key == "" || i >= len(s) || s[i] != '=' {
			return nil, errInvalid
		}
		i++
		skip()
		var val strings.Builder
		if i < len(s) && s[i] == '\'' {
			i++
			closed := false
			for i < len(s) {
				c := s[i]
				if c == '\\' && i+1 < len(s) {
					val.WriteByte(s[i+1])
					i += 2
					continue
				}
				if c == '\'' {
					closed = true
					i++
					break
				}
				val.WriteByte(c)
				i++
			}
			if !closed {
				return nil, errInvalid
			}
		} else {
			for i < len(s) && s[i] != ' ' && s[i] != '\t' && s[i] != '\n' && s[i] != '\r' {
				if s[i] == '\\' && i+1 < len(s) {
					i++
				}
				val.WriteByte(s[i])
				i++
			}
		}
		if _, dup := out[key]; dup {
			return nil, errUnsupported
		}
		out[key] = val.String()
	}
}

// parseURI parses postgres:// and postgresql:// URIs into the same map.
func parseURI(s string) (map[string]string, error) {
	rest := s[strings.Index(s, "://")+3:]
	authority, _, _ := strings.Cut(rest, "/")
	authority, _, _ = strings.Cut(authority, "?")
	if _, hostpart, ok := strings.Cut(authority, "@"); ok {
		authority = hostpart
	}
	if strings.Contains(authority, ",") {
		return nil, errMultiHost
	}
	u, err := url.Parse(s)
	if err != nil {
		return nil, errInvalid
	}
	out := map[string]string{}
	if h := u.Hostname(); h != "" {
		out["host"] = h
	}
	if p := u.Port(); p != "" {
		out["port"] = p
	}
	if u.User != nil {
		if name := u.User.Username(); name != "" {
			out["user"] = name
		}
		if pw, ok := u.User.Password(); ok {
			out["password"] = pw
		}
	}
	if db := strings.TrimPrefix(u.Path, "/"); db != "" {
		out["dbname"] = db
	}
	for k, vs := range u.Query() {
		if len(vs) != 1 {
			return nil, errUnsupported
		}
		if _, dup := out[k]; dup {
			return nil, errUnsupported
		}
		out[k] = vs[0]
	}
	return out, nil
}

// render parses dsn and returns the env file text; it performs no I/O.
func render(dsn string) (string, error) {
	env, _, err := renderOverride(dsn, "")
	return env, err
}

var dbNameRE = regexp.MustCompile(`^[A-Za-z0-9_]{1,63}$`)

// canonicalKeyword renders the parsed (and possibly overridden) parameter map
// as a single-line keyword/value DSN every value of which is single-quoted
// with backslash escapes, in sorted key order.
func canonicalKeyword(params map[string]string) string {
	keys := make([]string, 0, len(params))
	for k := range params {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	parts := make([]string, 0, len(keys))
	r := strings.NewReplacer(`\`, `\\`, `'`, `\'`)
	for _, k := range keys {
		parts = append(parts, k+"='"+r.Replace(params[k])+"'")
	}
	return strings.Join(parts, " ")
}

// renderOverride is render with an optional database-name override (used to
// point the harness at its own disposable database). It returns the env file
// text and the canonical keyword DSN of the same parameters (override
// applied), which a server can consume via pgx. No I/O.
func renderOverride(dsn, dbOverride string) (string, string, error) {
	dsn = strings.TrimSpace(dsn)
	if dsn == "" {
		return "", "", errEmpty
	}
	var params map[string]string
	var err error
	if strings.HasPrefix(dsn, "postgres://") || strings.HasPrefix(dsn, "postgresql://") {
		params, err = parseURI(dsn)
	} else {
		params, err = parseKeywords(dsn)
	}
	if err != nil {
		return "", "", err
	}
	if dbOverride != "" {
		if !dbNameRE.MatchString(dbOverride) {
			return "", "", errValue
		}
		params["dbname"] = dbOverride
	}
	for k, v := range params {
		if _, ok := supported[k]; !ok {
			return "", "", errUnsupported
		}
		if strings.ContainsRune(v, 0) {
			return "", "", errValue
		}
		if k != "password" && k != "options" && strings.ContainsAny(v, "\r\n") {
			return "", "", errValue
		}
	}
	if strings.Contains(params["host"], ",") || strings.Contains(params["port"], ",") {
		return "", "", errMultiHost
	}
	port := 0 // 0 = absent: PGPORT stays unset (libpq/pgx default)
	if v, ok := params["port"]; ok {
		n, perr := strconv.Atoi(v)
		if perr != nil || n < 1 || n > 65535 {
			return "", "", errValue
		}
		port = n
	}
	if m, ok := params["sslmode"]; ok && !slices.Contains(sslModes, m) {
		return "", "", errValue
	}
	timeout := 3
	if v, ok := params["connect_timeout"]; ok {
		n, perr := strconv.Atoi(v)
		if perr != nil {
			return "", "", errValue
		}
		if n > 0 {
			timeout = min(n, 3)
		}
	}
	if err := crossCheck(params, port); err != nil {
		return "", "", err
	}

	var b strings.Builder
	var unset []string
	for _, k := range sortedKeys() {
		if _, ok := params[k]; !ok && k != "options" && k != "connect_timeout" {
			unset = append(unset, supported[k])
		}
	}
	unset = append(unset, alwaysUnset...)
	b.WriteString("unset " + strings.Join(unset, " ") + "\n")
	if port != 0 {
		b.WriteString(fmt.Sprintf("export PGPORT=%d\n", port))
	}
	for _, k := range []string{"host", "user", "dbname", "password", "sslmode", "sslrootcert", "sslcert", "sslkey", "application_name"} {
		if v, ok := params[k]; ok {
			b.WriteString("export " + supported[k] + "=" + shellQuote(v) + "\n")
		}
	}
	pgoptions := "-c statement_timeout=2000"
	if o := strings.TrimSpace(params["options"]); o != "" {
		pgoptions = o + " " + pgoptions
	}
	b.WriteString("export PGOPTIONS=" + shellQuote(pgoptions) + "\n")
	b.WriteString(fmt.Sprintf("export PGCONNECT_TIMEOUT=%d\n", timeout))
	return b.String(), canonicalKeyword(params), nil
}

func sortedKeys() []string {
	keys := make([]string, 0, len(supported))
	for k := range supported {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}

// crossCheck rebuilds a TLS-file-free URI from the identity fields and
// requires pgx to decode exactly the same host, port, user, password and
// database (parse only; no connection is made).
func crossCheck(params map[string]string, port int) error {
	u := url.URL{Scheme: "postgres"}
	if dbname, ok := params["dbname"]; ok {
		u.Path = "/" + dbname
	}
	q := url.Values{"sslmode": {"disable"}}
	if pw, ok := params["password"]; ok {
		u.User = url.UserPassword(params["user"], pw)
	} else if user, ok := params["user"]; ok {
		u.User = url.User(user)
	}
	host := params["host"]
	switch {
	case strings.HasPrefix(host, "/"):
		q.Set("host", host)
	case strings.Contains(host, ":"):
		u.Host = "[" + host + "]" // IPv6 literal
	case host != "":
		u.Host = host
	}
	if port != 0 {
		q.Set("port", strconv.Itoa(port))
	}
	u.RawQuery = q.Encode()
	cfg, err := pgx.ParseConfig(u.String())
	if err != nil {
		return errInvalid
	}
	// Compare only what the DSN states; everything else is a default on both
	// sides (parse only; no connection is made).
	if (host != "" && cfg.Host != host) || (port != 0 && int(cfg.Port) != port) ||
		(params["user"] != "" && cfg.User != params["user"]) ||
		(params["dbname"] != "" && cfg.Database != params["dbname"]) ||
		(params["password"] != "" && cfg.Password != params["password"]) {
		return errMismatch
	}
	return nil
}

func main() {
	dsnFile := flag.String("dsn-file", "", "file holding the DSN (mode 0600)")
	outDir := flag.String("out", "", "directory for pgenv.sh")
	dbName := flag.String("dbname", "", "override the database name (letters, digits, underscore)")
	dsnOut := flag.String("dsn-out", "", "also write the canonical keyword DSN (0600) here")
	flag.Parse()
	if *dsnFile == "" || *outDir == "" {
		fmt.Fprintln(os.Stderr, "pgenv: -dsn-file and -out are required")
		os.Exit(2)
	}
	dsn, err := os.ReadFile(*dsnFile)
	if err != nil {
		fmt.Fprintln(os.Stderr, "pgenv: cannot read dsn file")
		os.Exit(1)
	}
	text, canon, err := renderOverride(string(dsn), *dbName)
	if err != nil {
		if de, ok := errors.AsType[dsnError](err); ok {
			fmt.Fprintln(os.Stderr, de.Error())
		} else {
			fmt.Fprintln(os.Stderr, "pgenv: unsupported database URL")
		}
		os.Exit(1)
	}
	if err := writeEnv(filepath.Join(*outDir, "pgenv.sh"), text); err != nil {
		fmt.Fprintln(os.Stderr, "pgenv: cannot write env file")
		os.Exit(1)
	}
	if *dsnOut != "" {
		if err := writeEnv(*dsnOut, canon+"\n"); err != nil {
			fmt.Fprintln(os.Stderr, "pgenv: cannot write dsn file")
			os.Exit(1)
		}
	}
}

// writeEnv creates the 0600 file and checks write and close errors.
func writeEnv(path, text string) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	if _, err := f.WriteString(text); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}
EOF
  cat >"$d/validator/validator_test.go" <<'EOF'
// Package validator is the generated compat proxy test of
// deploy/dev/e2e-p1.sh. It loads the v1 API document through
// internal/compat and validates EVERY captured /api/v1 response and every
// NDJSON frame, including the deliberate cut and cancel records, reading all
// capture files through os.Root with bounded streaming reads. Stream records
// are judged from their streamed frames (terminal presence and position,
// per-stream committed cursors, frame counts, byte accounting) and compared to
// the proxy's index, never trusted from metadata. Failure output carries only
// seq/op/status/class - never compat error text, never body values, paths,
// URLs, tokens or uncontrolled index strings. E2E_VALIDATE=final (the
// default) additionally REQUIRES the drained proxy audit; E2E_VALIDATE=live
// is the explicitly weaker preflight used while the proxy is still running.
package validator

import (
	"bufio"
	"bytes"
	"cmp"
	"encoding/json/v2"
	"errors"
	"io"
	"net/http"
	"net/url"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/nathabonfim59/bamboo-raft/internal/compat"
)

const (
	maxRecords    = 65536
	maxIndexBytes = 128 << 20
	maxAuditBytes = 4 << 10
	maxHeaderSize = 64 << 10
)

type record struct {
	Seq           int64  `json:"seq"`
	Method        string `json:"method"`
	Path          string `json:"path"`
	Status        int    `json:"status"`
	ContentType   string `json:"contentType"`
	Class         string `json:"class"`
	FailReason    string `json:"failReason"`
	File          string `json:"file"`
	BodyBytes     int64  `json:"bodyBytes"`
	Frames        int64  `json:"frames"`
	StdoutFrames  int64  `json:"stdoutFrames"`
	LastNextOff   int64  `json:"lastNextOffset"`
	LastStderrOff int64  `json:"lastStderrOffset"`
	TailBytes     int64  `json:"tailBytes"`
	Terminal      bool   `json:"terminalFrame"`
	ClientAborted bool   `json:"clientAborted"`
}

// reporter is the subset of testing.TB the checks use, so the same checks run
// under a collecting reporter in the negative-control tests.
type reporter interface {
	Errorf(format string, args ...any)
	Fatalf(format string, args ...any)
	Logf(format string, args ...any)
}

type config struct {
	jsonBound  int64
	frameBound int64
	final      bool // require the drained audit
}

var (
	knownClasses = []string{"json", "ndjson", "cut", "cancel", "fail"}
	knownReasons = []string{"harness-bound", "unexpected-abort", "unexpected-eof", "upstream-abort",
		"upstream-error", "total-spool", "spool-open", "spool-write", "bad-frame", "short-write",
		"flush", "index"}
	knownMethods = []string{"GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"}
)

func envInt(name string, def int64) int64 {
	if v := os.Getenv(name); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n > 0 {
			return n
		}
	}
	return def
}

// methodToken collapses an uncontrolled method string to a fixed token.
func methodToken(m string) string {
	if slices.Contains(knownMethods, m) {
		return m
	}
	return "other"
}

// pathOnly strips the query; segments splits into non-empty segments with
// the /api/v1 prefix removed.
func pathOnly(p string) string {
	if i := strings.Index(p, "?"); i >= 0 {
		return p[:i]
	}
	return p
}

func segments(path string) []string {
	path = strings.TrimPrefix(pathOnly(path), "/api/v1")
	parts := strings.Split(strings.Trim(path, "/"), "/")
	out := make([]string, 0, len(parts))
	for _, s := range parts {
		if s != "" {
			out = append(out, s)
		}
	}
	return out
}

// pathMatchesTemplate: equal length, literal segments equal, {param}
// matches one segment.
func pathMatchesTemplate(tpl, p string) bool {
	ts := strings.Split(tpl, "/")
	ps := strings.Split(p, "/")
	if len(ts) != len(ps) {
		return false
	}
	for i := range ts {
		templated := strings.HasPrefix(ts[i], "{") && strings.HasSuffix(ts[i], "}")
		if !templated && ts[i] != ps[i] {
			return false
		}
	}
	return true
}

// classify maps a compat error to a fixed diagnostic class without printing
// the error text.
func classify(err error) string {
	s := err.Error()
	switch {
	case strings.Contains(s, "is not documented"):
		return "undocumented-status"
	case strings.Contains(s, "no approved exception"), strings.Contains(s, "not approved"),
		strings.Contains(s, "must be false"), strings.Contains(s, "is not sandbox.error"):
		return "matrix-violation"
	default:
		return "schema-violation"
	}
}

var errTooBig = errors.New("file exceeds its bound")

// readBounded reads a whole capture file with a hard size bound, checked by
// Stat first and enforced again on the reader itself.
func readBounded(root *os.Root, name string, limit int64) ([]byte, error) {
	f, err := root.Open(name)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	if st, err := f.Stat(); err != nil {
		return nil, err
	} else if st.Size() > limit {
		return nil, errTooBig
	}
	b, err := io.ReadAll(io.LimitReader(f, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(b)) > limit {
		return nil, errTooBig
	}
	return b, nil
}

func TestE2ECompat(t *testing.T) {
	dir := os.Getenv("E2E_CAPTURE")
	if dir == "" {
		t.Fatal("E2E_CAPTURE not set")
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		t.Fatalf("open capture root: %v", err)
	}
	defer root.Close()
	mode := os.Getenv("E2E_VALIDATE")
	if mode == "" {
		mode = "final"
	}
	if mode != "final" && mode != "live" {
		t.Fatal("class=bad-mode")
	}
	v := compat.Load(t)
	checkCapture(t, v, root, config{
		jsonBound:  envInt("E2E_JSON_BOUND", 128<<20),
		frameBound: envInt("E2E_FRAME_BOUND", 16<<20),
		final:      mode == "final",
	})
}

func readIndex(t reporter, root *os.Root) []record {
	f, err := root.Open("index.ndjson")
	if err != nil {
		t.Fatalf("class=index-missing")
	}
	defer f.Close()
	var recs []record
	sc := bufio.NewScanner(io.LimitReader(f, maxIndexBytes+1))
	sc.Buffer(make([]byte, 64*1024), 1<<20)
	var total int64
	for sc.Scan() {
		line := sc.Bytes()
		total += int64(len(line)) + 1
		if total > maxIndexBytes {
			t.Fatalf("class=index-too-large")
		}
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		if len(recs) >= maxRecords {
			t.Fatalf("class=record-overflow")
		}
		var r record
		if err := json.Unmarshal(line, &r); err != nil {
			t.Fatalf("class=index-corrupt position=%d", len(recs)+1)
		}
		recs = append(recs, r)
	}
	if sc.Err() != nil {
		t.Fatalf("class=index-unreadable")
	}
	return recs
}

func checkAudit(t reporter, root *os.Root, recs []record, final bool) {
	ab, err := readBounded(root, "audit.json", maxAuditBytes)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) && !final {
			return // live preflight: the proxy has not drained yet
		}
		if errors.Is(err, os.ErrNotExist) {
			t.Fatalf("class=audit-missing")
		}
		t.Fatalf("class=audit-unreadable")
	}
	var a struct {
		Accepted  int64 `json:"accepted"`
		Completed int64 `json:"completed"`
		Failed    int64 `json:"failed"`
		Fatal     bool  `json:"fatal"`
	}
	if err := json.Unmarshal(ab, &a); err != nil {
		t.Fatalf("class=audit-unreadable")
	}
	if a.Fatal {
		t.Fatalf("class=proxy-fatal")
	}
	if a.Accepted != a.Completed {
		t.Errorf("class=audit-incomplete accepted %d completed %d", a.Accepted, a.Completed)
	}
	if a.Completed != int64(len(recs)) {
		t.Errorf("class=audit-completed %d records %d", a.Completed, len(recs))
	}
	var fails int64
	for _, r := range recs {
		if r.Class == "fail" {
			fails++
		}
	}
	if a.Failed != fails {
		t.Errorf("class=audit-failed %d records %d", a.Failed, fails)
	}
}

func checkCapture(t reporter, v *compat.Validator, root *os.Root, cfg config) {
	type opRoute struct {
		id     string
		segstr string
	}
	routes := map[string][]opRoute{}
	for _, op := range v.Operations() {
		segs := segments(op.Path)
		routes[op.Method] = append(routes[op.Method], opRoute{id: op.ID, segstr: strings.Join(segs, "/")})
	}

	if _, err := root.Stat("proxy-fatal"); err == nil {
		t.Fatalf("class=proxy-fatal")
	}
	recs := readIndex(t, root)
	if len(recs) == 0 {
		t.Fatalf("class=capture-empty")
	}
	// Concurrent finalizers append out of order: sort by sequence, then
	// require a unique, gapless 1..N sequence.
	slices.SortFunc(recs, func(a, b record) int { return cmp.Compare(a.Seq, b.Seq) })
	for i, r := range recs {
		if r.Seq != int64(i+1) {
			t.Fatalf("class=sequence-gap at position %d", i+1)
		}
	}
	checkAudit(t, root, recs, cfg.final)

	counts := map[string]int{}
	for i := range recs {
		r := recs[i]
		if !slices.Contains(knownClasses, r.Class) {
			counts["unknown"]++
			t.Errorf("seq=%d class=unknown-class", r.Seq)
			continue
		}
		counts[r.Class]++
		if r.Class == "fail" {
			reason := r.FailReason
			if !slices.Contains(knownReasons, reason) {
				reason = "unknown-reason"
			}
			t.Errorf("seq=%d op=%s status=%d class=harness:%s", r.Seq, methodToken(r.Method), r.Status, reason)
			continue
		}
		segs := segments(r.Path)
		var opID string
		matches := 0
		for _, rt := range routes[r.Method] {
			if pathMatchesTemplate(rt.segstr, strings.Join(segs, "/")) {
				matches++
				opID = rt.id
			}
		}
		if matches == 0 {
			t.Errorf("seq=%d class=no-op-match method=%s segs=%d", r.Seq, methodToken(r.Method), len(segs))
			continue
		}
		if matches > 1 {
			t.Errorf("seq=%d class=ambiguous-op", r.Seq)
			continue
		}
		report := func(class string) {
			t.Errorf("seq=%d op=%s status=%d class=%s", r.Seq, opID, r.Status, class)
		}
		if r.Status == 0 {
			report("zero-status")
			continue
		}
		ndjson := strings.Contains(r.ContentType, "application/x-ndjson")
		if ndjson != (r.Class != "json") {
			report("class-content-type")
			continue
		}
		header, ok := capturedHeader(root, r, report)
		if !ok {
			continue
		}
		if ndjson {
			d := validateStreamRecord(root, v, r, header, opID, cfg.frameBound, report)
			switch r.Class {
			case "cut":
				if d.terminal {
					report("cut-terminal-in-prefix")
					continue
				}
				if d.stdoutFrames < 1 || d.lastOff <= 0 {
					report("cut-precondition")
					continue
				}
				if r.TailBytes != 0 {
					report("cut-partial-tail")
					continue
				}
				if !reconnectsWithExactOffsets(recs[i+1:], r.Path, d) {
					report("reconnect-offset")
				}
			case "cancel":
				if !r.ClientAborted {
					report("cancel-without-client-abort")
				}
			}
			continue
		}
		// Whole-body JSON, size-checked BEFORE reading.
		st, err := root.Stat(r.File)
		if err != nil {
			report("missing-body")
			continue
		}
		if st.Size() > cfg.jsonBound || r.BodyBytes > cfg.jsonBound {
			report("harness:bound-json")
			continue
		}
		body, err := readBounded(root, r.File, cfg.jsonBound)
		if err != nil {
			report("missing-body")
			continue
		}
		if int64(len(body)) != r.BodyBytes {
			report("spool-size")
			continue
		}
		if err := v.Response(opID, r.Status, header, body); err != nil {
			report(classify(err))
		}
	}
	t.Logf("validated records: %d, classes: %v", len(recs), counts)
}

// derived is what the streamed frames themselves prove.
type derived struct {
	frames       int64
	stdoutFrames int64
	lastOff      int64
	lastErrOff   int64
	terminal     bool
}

// validateStreamRecord validates every frame of a stream record with bounded
// streaming reads, derives terminal state, cursors and counts from the frames
// and compares them to the index.
func validateStreamRecord(root *os.Root, v *compat.Validator, r record, header http.Header, opID string, frameBound int64, report func(string)) derived {
	var d derived
	f, err := root.Open(r.File)
	if err != nil {
		report("missing-body")
		return d
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		report("missing-body")
		return d
	}
	fs := bufio.NewScanner(f)
	fs.Buffer(make([]byte, 64*1024), int(frameBound)+1)
	var spooled int64
	var idx int64
	terminalAt := int64(-1)
	var postTerminal, badOrder, regression bool
	for fs.Scan() {
		l := fs.Bytes()
		spooled += int64(len(l)) + 1
		if len(bytes.TrimSpace(l)) == 0 {
			report("bad-frame")
			continue
		}
		if int64(len(l)) > frameBound {
			report("harness:bound-frame")
			continue
		}
		if err := v.Response(opID, r.Status, header, l); err != nil {
			report(classify(err))
		}
		var fr struct {
			Type    string `json:"type"`
			NextOff int64  `json:"nextOffset"`
		}
		if err := json.Unmarshal(l, &fr); err != nil {
			report("bad-frame")
			continue
		}
		if terminalAt >= 0 {
			postTerminal = true
		}
		switch fr.Type {
		case "attached", "started":
			if idx != 0 {
				badOrder = true
			}
		case "stdout":
			d.stdoutFrames++
			if fr.NextOff > 0 {
				if fr.NextOff <= d.lastOff {
					regression = true
				}
				d.lastOff = fr.NextOff
			}
		case "stderr":
			if fr.NextOff > 0 {
				if fr.NextOff <= d.lastErrOff {
					regression = true
				}
				d.lastErrOff = fr.NextOff
			}
		case "exit", "error", "reconnect":
			if terminalAt < 0 {
				terminalAt = idx
			}
		}
		idx++
		d.frames++
	}
	if fs.Err() != nil {
		report("harness:bound-frame-scan")
	}
	d.terminal = terminalAt >= 0
	if d.frames == 0 {
		report("empty-stream")
		return d
	}
	if postTerminal {
		report("post-terminal")
	}
	if badOrder {
		report("frame-order")
	}
	if regression {
		report("cursor-regression")
	}
	if st.Size() != spooled {
		report("spool-size")
	}
	// Index metadata must agree with what the frames prove.
	if d.frames != r.Frames {
		report("frame-count")
	}
	if d.stdoutFrames != r.StdoutFrames {
		report("index-stdout-frames")
	}
	if d.lastOff != r.LastNextOff || d.lastErrOff != r.LastStderrOff {
		report("index-cursor")
	}
	if d.terminal != r.Terminal {
		report("index-terminal")
	}
	switch r.Class {
	case "ndjson":
		if !d.terminal {
			report("terminal-state")
		}
		if r.BodyBytes != spooled || r.TailBytes != 0 {
			report("body-accounting")
		}
	case "cut":
		if r.BodyBytes != spooled {
			report("body-accounting")
		}
	case "cancel":
		// Explicit tail accounting via the same streamed count.
		if r.BodyBytes-spooled != r.TailBytes {
			report("cancel-tail-account")
		}
	}
	return d
}

// capturedHeader loads and validates the proxy-captured response headers.
// They are REQUIRED: a missing, oversized or corrupt file, or a Content-Type
// that differs from the index, is a failure; nothing is ever fabricated.
func capturedHeader(root *os.Root, r record, report func(string)) (http.Header, bool) {
	b, err := readBounded(root, strings.TrimSuffix(r.File, ".body")+".headers.json", maxHeaderSize)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			report("missing-headers")
		} else {
			report("corrupt-headers")
		}
		return nil, false
	}
	var m map[string][]string
	if err := json.Unmarshal(b, &m); err != nil {
		report("corrupt-headers")
		return nil, false
	}
	h := http.Header{}
	for k, vs := range m {
		for _, v := range vs {
			h.Add(k, v)
		}
	}
	if h.Get("Content-Type") == "" || h.Get("Content-Type") != r.ContentType {
		report("header-content-type")
		return nil, false
	}
	return h, true
}

// reconnectsWithExactOffsets proves the next stream GET for the same full
// path resumed at exactly the cut stream's DERIVED committed stdout AND
// stderr offsets (exact query parse, no substring matching).
func reconnectsWithExactOffsets(rest []record, cutPath string, cut derived) bool {
	for _, r := range rest {
		if r.Method != "GET" || r.Class == "cut" || r.Class == "cancel" {
			continue
		}
		if pathOnly(r.Path) != pathOnly(cutPath) {
			continue
		}
		q, err := url.ParseQuery(queryOf(r.Path))
		if err != nil {
			continue
		}
		out, err1 := strconv.ParseInt(q.Get("stdoutOffset"), 10, 64)
		errOff, err2 := strconv.ParseInt(q.Get("stderrOffset"), 10, 64)
		return err1 == nil && err2 == nil && out == cut.lastOff && errOff == cut.lastErrOff
	}
	return false
}

func queryOf(path string) string {
	if i := strings.Index(path, "?"); i >= 0 {
		return path[i+1:]
	}
	return ""
}
EOF
  cat >"$d/proxy/leadcut_test.go" <<'EOF'
// TestLeadActualCut is the lead's independent live cut proof
// (e2e-p1-cut-probe_test.go), adapted only to this proxy's constructor and
// "sandbox:minStdoutFrames" arm format. The three assertions are unchanged:
// the cut must abort the client body (not a clean EOF), must not forward any
// terminal frame beyond the intended stdout frame, and must commit exactly
// the first flushed stdout cursor.
package main

import (
	"bytes"
	"encoding/json/v2"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"
)

func TestLeadActualCut(t *testing.T) {
	capture := t.TempDir()
	if err := os.WriteFile(capture+"/arm-cut", []byte("bx_probe:1\n"), 0o600); err != nil {
		t.Fatal("prepare cut")
	}
	lines := []string{
		"{\"type\":\"attached\",\"processId\":7,\"stdoutOffset\":0,\"stderrOffset\":0}",
		"{\"type\":\"stdout\",\"data\":\"first\",\"nextOffset\":5}",
		"{\"type\":\"stdout\",\"data\":\"second\",\"nextOffset\":11}",
		"{\"type\":\"exit\",\"status\":\"exited\",\"exitCode\":0,\"stdoutOffset\":11,\"stderrOffset\":0}",
	}
	payload := strings.Join(lines, "\n") + "\n"
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/x-ndjson")
		_, _ = io.WriteString(w, payload)
		_ = http.NewResponseController(w).Flush()
	}))
	t.Cleanup(upstream.Close)
	// Pretend a detached start returned pid 7 for bx_probe, so the cut
	// claims this exact stream.
	p := newProxy(mustURL(t, upstream.URL), capture, newDiscardLogger())
	p.mu.Lock()
	p.detachedPID["bx_probe"] = "7"
	p.mu.Unlock()
	front := httptest.NewServer(p)
	t.Cleanup(front.Close)
	request, err := http.NewRequestWithContext(t.Context(), http.MethodGet,
		front.URL+"/api/v1/sandboxes/bx_probe/commands/7/stream?stdoutOffset=0&stderrOffset=0", nil)
	if err != nil {
		t.Fatal("build fixture request")
	}
	client := front.Client()
	client.Timeout = 3 * time.Second
	response, err := client.Do(request)
	if err != nil {
		t.Fatal("fixture request failed before body")
	}
	t.Cleanup(func() { _ = response.Body.Close() })
	body, readErr := io.ReadAll(response.Body)
	if readErr == nil {
		t.Error("cut completed as clean HTTP EOF; expected an aborted body")
	}
	if bytes.Contains(body, []byte("\"type\":\"exit\"")) {
		t.Error("cut forwarded terminal frame after intended first stdout")
	}
	front.Close()
	root, err := os.OpenRoot(capture)
	if err != nil {
		t.Fatal("open fixture capture")
	}
	defer root.Close()
	index, err := root.ReadFile("index.ndjson")
	if err != nil {
		t.Fatal("fixture capture index missing")
	}
	for line := range bytes.SplitSeq(index, []byte{'\n'}) {
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		var record struct {
			Class string `json:"class"`
			Last  int64  `json:"lastNextOffset"`
		}
		if err := json.Unmarshal(line, &record); err != nil {
			t.Fatal("fixture capture index invalid")
		}
		if record.Class != "cut" || record.Last != 5 {
			t.Errorf("cut cursor/class = %d/%s, want first flushed cursor 5/cut", record.Last, record.Class)
		}
	}
}
EOF
  cat >"$d/proxy/armed_test.go" <<'EOF'
// Armed-capture tests drive the actual recorder (newRecorder) with fake
// response writers: incremental partial-frame and total-spool bounds, flush
// and write failures, and the rule that a cut is never committed before a
// frame was fully written AND flushed.
package main

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// armedRecorder builds a cut-armed NDJSON recorder over w.
func armedRecorder(t *testing.T, w http.ResponseWriter, mutate func(*proxy)) (*proxy, *recorder) {
	t.Helper()
	p := newProxy(nil, t.TempDir(), newDiscardLogger())
	if mutate != nil {
		mutate(p)
	}
	r := httptest.NewRequest(http.MethodGet, "/api/v1/sandboxes/bx_t/commands/7/stream", nil)
	rec := newRecorder(w, p, r)
	rec.cutTarget = true
	rec.Header().Set("Content-Type", "application/x-ndjson")
	rec.WriteHeader(http.StatusOK)
	t.Cleanup(func() {
		if rec.file != nil {
			_ = rec.file.Close()
		}
	})
	return p, rec
}

// failWriter is a ResponseWriter with injectable write and flush behaviour.
type failWriter struct {
	header   http.Header
	writeN   func(n int) int // bytes reported written for a len-n write
	writeErr error
	flushErr error
	flushes  int
	written  int
}

func newFailWriter() *failWriter { return &failWriter{header: http.Header{}} }

func (w *failWriter) Header() http.Header { return w.header }
func (w *failWriter) WriteHeader(int)     {}
func (w *failWriter) Write(b []byte) (int, error) {
	n := len(b)
	if w.writeN != nil {
		n = w.writeN(len(b))
	}
	w.written += n
	return n, w.writeErr
}
func (w *failWriter) FlushError() error { w.flushes++; return w.flushErr }

func TestArmedPartialFrameBound(t *testing.T) {
	p, rec := armedRecorder(t, httptest.NewRecorder(), func(p *proxy) { p.frameBound = 32 })
	_, err := rec.Write([]byte(strings.Repeat("x", 128)))
	if err == nil {
		t.Error("armed newline-free write beyond the frame bound did not fail")
	}
	if rec.failReason != "harness-bound" {
		t.Errorf("failReason = %q, want harness-bound", rec.failReason)
	}
	if int64(len(rec.pending)) > p.frameBound {
		t.Errorf("pending grew to %d, bound %d", len(rec.pending), p.frameBound)
	}
}

func TestArmedPartialFrameBoundAcrossWrites(t *testing.T) {
	p, rec := armedRecorder(t, httptest.NewRecorder(), func(p *proxy) { p.frameBound = 32 })
	var err error
	for range 10 {
		if _, err = rec.Write([]byte(strings.Repeat("x", 8))); err != nil {
			break
		}
	}
	if err == nil || rec.failReason != "harness-bound" {
		t.Fatalf("slow partial growth was not bounded: err=%v reason=%q", err, rec.failReason)
	}
	if int64(len(rec.pending)) > p.frameBound {
		t.Errorf("pending grew to %d, bound %d", len(rec.pending), p.frameBound)
	}
}

func TestArmedTotalSpoolChargedBeforeBuffering(t *testing.T) {
	p, rec := armedRecorder(t, httptest.NewRecorder(), func(p *proxy) { p.totalCap = 40 })
	_, err := rec.Write([]byte(strings.Repeat("y", 100)))
	if err == nil || rec.failReason != "total-spool" {
		t.Fatalf("partial armed bytes were not charged: err=%v reason=%q", err, rec.failReason)
	}
	if len(rec.pending) != 0 {
		t.Errorf("pending kept %d bytes after total-spool failure", len(rec.pending))
	}
	if p.spool > p.totalCap+100 {
		t.Errorf("spool charged %d", p.spool)
	}
}

func TestArmedNoDoubleCharge(t *testing.T) {
	p, rec := armedRecorder(t, httptest.NewRecorder(), func(p *proxy) { p.cutMin = 1 << 30 })
	frame := "{\"type\":\"stdout\",\"data\":\"a\",\"nextOffset\":1}\n"
	// one frame delivered in two partial writes, then complete
	if _, err := rec.Write([]byte(frame[:10])); err != nil {
		t.Fatal(err)
	}
	if _, err := rec.Write([]byte(frame[10:])); err != nil {
		t.Fatal(err)
	}
	if p.spool != int64(len(frame)) {
		t.Errorf("spool charged %d for a %d-byte frame (double charge?)", p.spool, len(frame))
	}
	if rec.frames != 1 || rec.stdout != 1 || rec.lastOff != 1 {
		t.Errorf("frame accounting: frames=%d stdout=%d off=%d", rec.frames, rec.stdout, rec.lastOff)
	}
}

func TestArmedFlushErrorNeverCommitsCut(t *testing.T) {
	w := newFailWriter()
	w.flushErr = errors.New("flush broke")
	p, rec := armedRecorder(t, w, func(p *proxy) { p.cutMin = 1 })
	frame := "{\"type\":\"stdout\",\"data\":\"a\",\"nextOffset\":1}\n"
	_, err := rec.Write([]byte(frame)) // must NOT panic into a committed cut
	if err == nil {
		t.Fatal("flush failure was swallowed")
	}
	if rec.cutDone {
		t.Error("cut committed although the frame was not flushed")
	}
	if rec.failReason != "flush" {
		t.Errorf("failReason = %q, want flush", rec.failReason)
	}
	if rec.stdout != 0 || rec.lastOff != 0 {
		t.Errorf("cursor committed before flush: stdout=%d off=%d", rec.stdout, rec.lastOff)
	}
	if p.isFatal() {
		t.Error("a client flush failure must not be a fatal harness error")
	}
}

func TestArmedShortWriteFails(t *testing.T) {
	w := newFailWriter()
	w.writeN = func(n int) int { return n - 1 }
	_, rec := armedRecorder(t, w, func(p *proxy) { p.cutMin = 1 })
	frame := "{\"type\":\"stdout\",\"data\":\"a\",\"nextOffset\":1}\n"
	_, err := rec.Write([]byte(frame))
	if err == nil || rec.failReason != "short-write" {
		t.Fatalf("short nil write: err=%v reason=%q", err, rec.failReason)
	}
	if rec.cutDone {
		t.Error("cut committed after a short write")
	}
}

func TestUnarmedShortWriteFails(t *testing.T) {
	w := newFailWriter()
	w.writeN = func(n int) int { return n - 1 }
	p := newProxy(nil, t.TempDir(), newDiscardLogger())
	r := httptest.NewRequest(http.MethodGet, "/api/v1/me", nil)
	rec := newRecorder(w, p, r)
	rec.Header().Set("Content-Type", "application/json")
	rec.WriteHeader(http.StatusOK)
	t.Cleanup(func() {
		if rec.file != nil {
			_ = rec.file.Close()
		}
	})
	_, err := rec.Write([]byte(`{"ok":true}`))
	if err == nil || rec.failReason != "short-write" {
		t.Fatalf("unarmed short write: err=%v reason=%q", err, rec.failReason)
	}
}

func TestArmedWriteErrorIsClientAbortNotCommit(t *testing.T) {
	w := newFailWriter()
	w.writeErr = errors.New("conn reset")
	_, rec := armedRecorder(t, w, func(p *proxy) { p.cutMin = 1 })
	frame := "{\"type\":\"stdout\",\"data\":\"a\",\"nextOffset\":1}\n"
	if _, err := rec.Write([]byte(frame)); err == nil {
		t.Fatal("write error swallowed")
	}
	if rec.cutDone || rec.stdout != 0 {
		t.Errorf("committed on a failed write: cut=%v stdout=%d", rec.cutDone, rec.stdout)
	}
	if !rec.clientAbort {
		t.Error("write error not recorded as client abort")
	}
}

func TestArmedUnterminatedTailIsUnexpectedEOF(t *testing.T) {
	f := newFixture(t)
	f.p.cutClaimed = true // keep the arm file from binding; arm cancel instead
	f.setHandler(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/x-ndjson")
		_, _ = w.Write([]byte("{\"type\":\"stdout\",\"data\":\"a\",\"nextOffset\":1}\n{\"type\":\"stdout\",\"da"))
	})
	armCancel(t, f, "/api/v1/sandboxes/bx_t/commands/9/stream")
	_, _, _ = getBody(t, f.proxy.URL+"/api/v1/sandboxes/bx_t/commands/9/stream")
	ix := waitIndex(t, f, 1)
	if ix[0].Class != "fail" || ix[0].FailReason != "unexpected-eof" {
		t.Fatalf("index: %+v", ix[0])
	}
}

// armCancel arms the one-shot cancel target for an exact stream path.
func armCancel(t *testing.T, f *fixture, path string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(f.capDir, "arm-cancel"), []byte(path), 0o600); err != nil {
		t.Fatal(err)
	}
}

// waitIndex polls (bounded) until the index holds at least n records.
func waitIndex(t *testing.T, f *fixture, n int) []indexLine {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if ix := f.indexIfExists(); len(ix) >= n {
			return ix
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("index never reached %d records", n)
	return nil
}
EOF
  cat >"$d/proxy/audit_test.go" <<'EOF'
// Audit, drain, barrier and IO-failure tests of the actual proxy: bounded
// graceful shutdown with a blocked request, atomic audit publication, every
// persistence failure being fatal (injected deterministically by directory
// collisions, which also fail for root), and the loopback-only barrier.
package main

import (
	"encoding/json/v2"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func readAudit(t *testing.T, dir string) auditLine {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(dir, "audit.json"))
	if err != nil {
		t.Fatalf("audit missing: %v", err)
	}
	var a auditLine
	if err := json.Unmarshal(b, &a); err != nil {
		t.Fatalf("audit invalid: %v", err)
	}
	return a
}

func TestDrainCleanWritesFinalAudit(t *testing.T) {
	f := newFixture(t)
	if _, _, err := getBody(t, f.proxy.URL+"/api/v1/me"); err != nil {
		t.Fatal(err)
	}
	if err := f.p.drain(f.proxy.Config, 2*time.Second, 2*time.Second); err != nil {
		t.Fatalf("drain: %v", err)
	}
	a := readAudit(t, f.capDir)
	if a.Accepted != 1 || a.Completed != 1 || a.Failed != 0 || a.Fatal {
		t.Fatalf("audit: %+v", a)
	}
}

func TestDrainTimeoutForceClosesAndJoinsBlockedRequest(t *testing.T) {
	f := newFixture(t)
	gate := make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(gate) }) }
	t.Cleanup(release) // before the upstream Close (LIFO)
	f.setHandler(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-gate:
		case <-r.Context().Done():
		}
	})
	errc := make(chan error, 1)
	go func() {
		_, _, err := getBody(t, f.proxy.URL+"/api/v1/me")
		errc <- err
	}()
	deadline := time.Now().Add(5 * time.Second)
	for f.p.accepted.Load() != 1 {
		if time.Now().After(deadline) {
			t.Fatal("request never accepted")
		}
		timeSleep()
	}
	start := time.Now()
	err := f.p.drain(f.proxy.Config, 100*time.Millisecond, 3*time.Second)
	if err == nil {
		t.Fatal("drain with an active blocked request reported success")
	}
	if time.Since(start) > 4*time.Second {
		t.Fatalf("drain was not bounded: %v", time.Since(start))
	}
	if !f.p.isFatal() {
		t.Error("forced shutdown must be fatal")
	}
	if f.p.completed.Load() != f.p.accepted.Load() {
		t.Errorf("handlers not joined: accepted=%d completed=%d", f.p.accepted.Load(), f.p.completed.Load())
	}
	a := readAudit(t, f.capDir)
	if !a.Fatal || a.Accepted != a.Completed {
		t.Fatalf("audit after forced shutdown: %+v", a)
	}
	select {
	case <-errc:
	case <-time.After(5 * time.Second):
		t.Fatal("client request did not end after force-close")
	}
}

func TestAuditWriteFailureIsFatal(t *testing.T) {
	f := newFixture(t)
	if err := os.Mkdir(filepath.Join(f.capDir, "audit.json"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := f.p.drain(f.proxy.Config, time.Second, time.Second); err == nil {
		t.Fatal("audit write failure was ignored")
	}
	if !f.p.isFatal() {
		t.Error("audit write failure not fatal")
	}
}

func TestHeaderWriteFailureIsFatal(t *testing.T) {
	f := newFixture(t)
	if err := os.Mkdir(filepath.Join(f.capDir, "rec-000001.headers.json"), 0o700); err != nil {
		t.Fatal(err)
	}
	_, _, _ = getBody(t, f.proxy.URL+"/api/v1/me")
	if !f.p.isFatal() {
		t.Fatal("header write failure was not fatal")
	}
}

func TestIndexWriteFailureIsFatal(t *testing.T) {
	f := newFixture(t)
	if err := os.Mkdir(filepath.Join(f.capDir, "index.ndjson"), 0o700); err != nil {
		t.Fatal(err)
	}
	_, _, _ = getBody(t, f.proxy.URL+"/api/v1/me")
	if !f.p.isFatal() {
		t.Fatal("index write failure was not fatal")
	}
}

func TestSpoolCloseFailureIsFatal(t *testing.T) {
	_, rec := armedRecorder(t, httptest.NewRecorder(), nil)
	if err := rec.file.Close(); err != nil { // pre-close: persist's Close now errors
		t.Fatal(err)
	}
	rec.persist(false)
	if !rec.p.isFatal() {
		t.Fatal("spool Close failure was not fatal")
	}
}

type barrierSnap struct {
	Seq, Accepted, Completed, Failed int64
	Fatal                            bool
}

func barrierGet(t *testing.T, p *proxy, remote string, header map[string]string) (int, barrierSnap) {
	t.Helper()
	r := httptest.NewRequest(http.MethodGet, barrierPath, nil)
	r.RemoteAddr = remote
	for k, v := range header {
		r.Header.Set(k, v)
	}
	w := httptest.NewRecorder()
	p.ServeHTTP(w, r)
	var s barrierSnap
	if w.Code == http.StatusOK {
		var raw struct {
			Seq       int64 `json:"seq"`
			Accepted  int64 `json:"accepted"`
			Completed int64 `json:"completed"`
			Failed    int64 `json:"failed"`
			Fatal     bool  `json:"fatal"`
		}
		if err := json.Unmarshal(w.Body.Bytes(), &raw); err != nil {
			t.Fatalf("barrier body: %v", err)
		}
		s = barrierSnap(raw)
	}
	return w.Code, s
}

func TestBarrierLoopbackOnlyConsistentAndOutsideCapture(t *testing.T) {
	f := newFixture(t)
	if _, _, err := getBody(t, f.proxy.URL+"/api/v1/me"); err != nil {
		t.Fatal(err)
	}
	waitIndex(t, f, 1)
	code, s := barrierGet(t, f.p, "127.0.0.1:5555", nil)
	if code != http.StatusOK || s.Seq != 1 || s.Accepted != 1 || s.Completed != 1 {
		t.Fatalf("loopback snapshot: code=%d %+v", code, s)
	}
	if code, _ = barrierGet(t, f.p, "[::1]:5555", nil); code != http.StatusOK {
		t.Fatalf("ipv6 loopback: %d", code)
	}
	// Forwarded headers and Host never grant access to a remote peer.
	for _, remote := range []string{"203.0.113.9:4444", "192.0.2.1:80"} {
		hdr := map[string]string{"X-Forwarded-For": "127.0.0.1", "X-Real-IP": "127.0.0.1", "Forwarded": "for=127.0.0.1"}
		if code, _ = barrierGet(t, f.p, remote, hdr); code != http.StatusNotFound {
			t.Errorf("remote peer %s got %d", remote, code)
		}
	}
	// Reads never count as v1 records.
	if got := f.p.accepted.Load(); got != 1 {
		t.Errorf("barrier changed accepted: %d", got)
	}
	if len(f.index(t)) != 1 {
		t.Error("barrier request appeared in the capture index")
	}
}

func TestBarrierSnapshotWithInflightRequest(t *testing.T) {
	f := newFixture(t)
	gate := make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(gate) }) }
	t.Cleanup(release)
	f.setHandler(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-gate:
		case <-r.Context().Done():
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	})
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _, _ = getBody(t, f.proxy.URL+"/api/v1/me")
	}()
	deadline := time.Now().Add(5 * time.Second)
	for f.p.accepted.Load() != 1 {
		if time.Now().After(deadline) {
			t.Fatal("request never accepted")
		}
		timeSleep()
	}
	_, s := barrierGet(t, f.p, "127.0.0.1:1", nil)
	if s.Accepted != 1 || s.Completed != 0 || s.Seq != 1 {
		t.Fatalf("in-flight snapshot must show accepted>completed: %+v", s)
	}
	release()
	<-done
	waitIndex(t, f, 1)
	deadline = time.Now().Add(5 * time.Second)
	for {
		if _, s = barrierGet(t, f.p, "127.0.0.1:1", nil); s.Completed == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("never completed: %+v", s)
		}
		timeSleep()
	}
}
EOF
  cat >"$d/pgenv/main_test.go" <<'EOF'
// Offline DSN tests: preserved options, fixed rejections, stale-env unsets
// and secret hygiene. No network, no files outside t.TempDir().
package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRenderPreservesSupportedOptions(t *testing.T) {
	cases := []struct {
		name string
		dsn  string
		want []string
	}{
		{"uri basic", "postgres://u:p%40ss%27w@db.test:5433/app?sslmode=require",
			[]string{"export PGHOST='db.test'", "export PGPORT=5433", "export PGUSER='u'",
				`export PGPASSWORD='p@ss'\''w'`, "export PGDATABASE='app'", "export PGSSLMODE='require'"}},
		{"uri tls files", "postgresql://u@h/d?sslmode=verify-full&sslrootcert=/ca.pem&sslcert=/c.pem&sslkey=/k.pem&application_name=e2e",
			[]string{"export PGSSLROOTCERT='/ca.pem'", "export PGSSLCERT='/c.pem'", "export PGSSLKEY='/k.pem'", "export PGAPPNAME='e2e'"}},
		{"uri options", "postgres://u@h/d?options=-c%20search_path%3Dpublic",
			[]string{"export PGOPTIONS='-c search_path=public -c statement_timeout=2000'"}},
		{"keyword", "host=db.test user=user dbname=test sslmode=disable options='-c search_path=public'",
			[]string{"export PGHOST='db.test'", "export PGSSLMODE='disable'",
				"export PGOPTIONS='-c search_path=public -c statement_timeout=2000'"}},
		{"keyword quoted password", `host=h user=u password='a b\'c\\d' dbname=d`,
			[]string{`export PGPASSWORD='a b'\''c\d'`}},
		{"keyword spaced equals", "host = h user = u dbname = d port = 6543",
			[]string{"export PGPORT=6543"}},
		{"unix socket", "postgres://u@/d?host=/var/run/postgresql",
			[]string{"export PGHOST='/var/run/postgresql'"}},
		{"ipv6", "postgres://u@[::1]:5432/d", []string{"export PGHOST='::1'"}},
		{"timeout capped", "postgres://u@h/d?connect_timeout=30", []string{"export PGCONNECT_TIMEOUT=3"}},
		{"timeout lower kept", "postgres://u@h/d?connect_timeout=2", []string{"export PGCONNECT_TIMEOUT=2"}},
		{"timeout default", "postgres://u@h/d", []string{"export PGCONNECT_TIMEOUT=3"}},
		{"statement timeout forced last", "postgres://u@h/d?options=-c%20statement_timeout%3D99999",
			[]string{"export PGOPTIONS='-c statement_timeout=99999 -c statement_timeout=2000'"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out, err := render(tc.dsn)
			if err != nil {
				t.Fatalf("render: %v", err)
			}
			for _, want := range tc.want {
				if !strings.Contains(out, want) {
					t.Errorf("env lacks %q", want)
				}
			}
		})
	}
}

// pgx-valid default forms (socket or localhost, OS user, dbname = user, no
// port) must be accepted and leave those variables UNSET, not fabricated.
func TestRenderAcceptsPgxDefaultForms(t *testing.T) {
	for _, tc := range []struct {
		name, dsn string
		exported  []string
		unset     []string
	}{
		{"database only", "postgres:///app", []string{"export PGDATABASE='app'"}, []string{"PGHOST", "PGUSER", "PGPORT"}},
		{"host only", "postgres://db.test", []string{"export PGHOST='db.test'"}, []string{"PGUSER", "PGDATABASE", "PGPORT"}},
		{"user only", "postgres://u@/", []string{"export PGUSER='u'"}, []string{"PGHOST", "PGDATABASE"}},
		{"socket query", "postgres:///d?host=/var/run/postgresql", []string{"export PGHOST='/var/run/postgresql'"}, []string{"PGUSER"}},
		{"keyword db only", "dbname=app", []string{"export PGDATABASE='app'"}, []string{"PGHOST", "PGUSER"}},
		{"keyword socket", "host=/tmp user=u", []string{"export PGHOST='/tmp'"}, []string{"PGDATABASE"}},
		{"bare scheme with sslmode", "postgres://?sslmode=disable", []string{"export PGSSLMODE='disable'"}, []string{"PGHOST", "PGUSER", "PGDATABASE"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out, err := render(tc.dsn)
			if err != nil {
				t.Fatalf("pgx-valid default form rejected: %v", err)
			}
			first, rest, _ := strings.Cut(out, "\n")
			for _, want := range tc.exported {
				if !strings.Contains(rest, want) {
					t.Errorf("missing %q", want)
				}
			}
			for _, v := range tc.unset {
				if !strings.Contains(first, v) || strings.Contains(rest, "export "+v+"=") {
					t.Errorf("%s must be unset, not exported", v)
				}
			}
		})
	}
}

func TestRenderRejectsUnsupportedBeforeAnyOutput(t *testing.T) {
	cases := []struct {
		name string
		dsn  string
		want dsnError
	}{
		{"multi host uri", "postgres://u@h1:5432,h2:5432/d", errMultiHost},
		{"multi host keyword", "host=h1,h2 user=u dbname=d", errMultiHost},
		{"multi port", "host=h user=u dbname=d port=1,2", errMultiHost},
		{"target session attrs", "postgres://u@h/d?target_session_attrs=read-write", errUnsupported},
		{"load balance", "postgres://u@h/d?load_balance_hosts=random", errUnsupported},
		{"service", "service=foo user=u dbname=d host=h", errUnsupported},
		{"passfile", "postgres://u@h/d?passfile=/x", errUnsupported},
		{"unknown runtime param", "postgres://u@h/d?search_path=public", errUnsupported},
		{"channel binding", "host=h user=u dbname=d channel_binding=require", errUnsupported},
		{"repeated key", "postgres://u@h/d?sslmode=disable&sslmode=require", errUnsupported},
		{"duplicate keyword", "host=h host=i user=u dbname=d", errUnsupported},
		{"bad sslmode", "postgres://u@h/d?sslmode=sometimes", errValue},
		{"bad port", "postgres://u@h:70000/d", errValue},
		{"bad port keyword", "host=h user=u dbname=d port=abc", errValue},
		{"bad timeout", "postgres://u@h/d?connect_timeout=soon", errValue},
		{"unterminated quote", "host=h user=u dbname='d", errInvalid},
		{"no equals", "host", errInvalid},
		{"empty", "", errEmpty},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out, err := render(tc.dsn)
			if err == nil {
				t.Fatalf("accepted; env:\n%s", out)
			}
			if out != "" {
				t.Error("rejected DSN still produced env text")
			}
			if err != tc.want {
				t.Errorf("error %q, want %q", err, tc.want)
			}
		})
	}
}

func TestDiagnosticsEchoNothing(t *testing.T) {
	secret := "sup3r-s3cret-pw"
	for _, dsn := range []string{
		"postgres://user:" + secret + "@h1:1,h2:2/d",
		"postgres://user:" + secret + "@h/d?weird_" + secret + "=1",
		"host=h user=u password=" + secret + " dbname=d bogus=" + secret,
		"postgres://user:" + secret + "@h/d?sslmode=" + secret,
	} {
		_, err := render(dsn)
		if err == nil {
			t.Fatalf("accepted %q", dsn)
		}
		if strings.Contains(err.Error(), secret) || strings.Contains(err.Error(), "user") {
			t.Errorf("diagnostic echoes DSN content: %v", err)
		}
	}
}

func TestStaleEnvIsUnset(t *testing.T) {
	out, err := render("postgres://u:p@h/d")
	if err != nil {
		t.Fatal(err)
	}
	first, _, _ := strings.Cut(out, "\n")
	if !strings.HasPrefix(first, "unset ") {
		t.Fatalf("no unset line: %q", first)
	}
	for _, v := range []string{"PGSSLMODE", "PGSSLROOTCERT", "PGSSLCERT", "PGSSLKEY", "PGAPPNAME",
		"PGSERVICE", "PGPASSFILE", "PGTARGETSESSIONATTRS", "PGHOSTADDR"} {
		if !strings.Contains(first, v) {
			t.Errorf("stale %s not unset", v)
		}
	}
	// A variable the DSN sets is exported, never unset.
	out, err = render("postgres://u:p@h/d?sslmode=require")
	if err != nil {
		t.Fatal(err)
	}
	first, _, _ = strings.Cut(out, "\n")
	if strings.Contains(first, "PGSSLMODE") || strings.Contains(first, "PGPASSWORD") {
		t.Errorf("exported variable also unset: %q", first)
	}
	// Absent password: PGPASSWORD must be unset (ambient one ignored).
	out, _ = render("postgres://u@h/d")
	first, _, _ = strings.Cut(out, "\n")
	if !strings.Contains(first, "PGPASSWORD") {
		t.Error("ambient PGPASSWORD not unset when the DSN has none")
	}
}

func TestWriteEnvMode0600AndFailure(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "pgenv.sh")
	if err := writeEnv(path, "export A='b'\n"); err != nil {
		t.Fatal(err)
	}
	st, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if st.Mode().Perm() != 0o600 {
		t.Errorf("mode %v", st.Mode().Perm())
	}
	collide := filepath.Join(dir, "collide")
	if err := os.Mkdir(collide, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := writeEnv(collide, "x"); err == nil {
		t.Error("write into a directory path succeeded")
	}
}

// The canonical keyword DSN of an overridden parse must itself parse (same
// supported subset), carry the new database, and keep every other option.
func TestCanonicalDSNOverrideRoundTrips(t *testing.T) {
	for _, dsn := range []string{
		"postgres:///base?host=/var/run/postgresql",
		"postgres://u:p%40ss%27w@db.test:5433/base?sslmode=require&application_name=e2e&options=-c%20search_path%3Dpublic",
		"host=h user=u password='a b\\'c\\\\d' dbname=base sslmode=disable",
	} {
		env0, _, err := renderOverride(dsn, "")
		if err != nil {
			t.Fatalf("render base: %v", err)
		}
		env, canon, err := renderOverride(dsn, "bbraft_e2e_0123456789ab")
		if err != nil {
			t.Fatalf("override: %v", err)
		}
		if !strings.Contains(env, "export PGDATABASE='bbraft_e2e_0123456789ab'") {
			t.Errorf("override not exported")
		}
		if strings.Contains(canon, "base") && strings.Contains(canon, "dbname='base'") {
			t.Errorf("base database survived the override")
		}
		// idempotent: the canonical form parses to the same env
		env2, canon2, err := renderOverride(canon, "")
		if err != nil {
			t.Fatalf("canonical DSN does not parse: %v", err)
		}
		if env2 != env || canon2 != canon {
			t.Errorf("canonical DSN is not a fixed point")
		}
		// every non-database export of the base survives
		for _, line := range strings.Split(env0, "\n") {
			if strings.HasPrefix(line, "export PGDATABASE=") || line == "" {
				continue
			}
			if !strings.Contains(env, line) {
				t.Errorf("option lost by override: %q", strings.SplitN(line, "=", 2)[0])
			}
		}
	}
}

func TestDBOverrideRejectsUnsafeNames(t *testing.T) {
	for _, name := range []string{`a"b`, "a b", "x;drop", "a-b", strings.Repeat("a", 64), "\u00e9"} {
		if _, _, err := renderOverride("postgres:///d", name); err != errValue {
			t.Errorf("name %q: error %v", name, err)
		}
	}
}
EOF
  cat >"$d/validator/evidence_test.go" <<'EOF'
// Negative-control and positive tests of the capture validator itself, run in
// one go test so the pinned spec loads once: every case builds a tiny capture
// in a temp dir, runs the REAL checkCapture under a collecting reporter and
// asserts the exact fixed diagnostic class (or the absence of any error).
package validator

import (
	"encoding/json/v2"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/nathabonfim59/bamboo-raft/internal/compat"
)

type collectorStop struct{}

// collector is a reporter that records lines; Fatalf stops the check.
type collector struct{ lines []string }

func (c *collector) Errorf(f string, a ...any) { c.lines = append(c.lines, fmt.Sprintf(f, a...)) }
func (c *collector) Fatalf(f string, a ...any) {
	c.Errorf(f, a...)
	panic(collectorStop{})
}
func (c *collector) Logf(string, ...any) {}

func (c *collector) has(sub string) bool {
	for _, l := range c.lines {
		if strings.Contains(l, sub) {
			return true
		}
	}
	return false
}

func runCheck(t *testing.T, v *compat.Validator, dir string, cfg config) *collector {
	t.Helper()
	root, err := os.OpenRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	c := &collector{}
	func() {
		defer func() {
			if e := recover(); e != nil {
				if _, ok := e.(collectorStop); !ok {
					panic(e)
				}
			}
		}()
		checkCapture(c, v, root, cfg)
	}()
	return c
}

const (
	attached = `{"type":"attached","processId":9,"stdoutOffset":0,"stderrOffset":0}`
	stdout3  = `{"type":"stdout","data":"abc","nextOffset":3}`
	stdout6  = `{"type":"stdout","data":"def","nextOffset":6}`
	exit0    = `{"type":"exit","status":"exited","exitCode":0,"stdoutOffset":6,"stderrOffset":0}`
	streamP  = "/api/v1/sandboxes/bx_s/commands/9/stream"
	meBody   = `{"ok":true,"type":"user.info","user":{}}`
)

// capture builds capture directories.
type capture struct {
	t    *testing.T
	dir  string
	recs []record
}

func newCapture(t *testing.T) *capture {
	t.Helper()
	return &capture{t: t, dir: t.TempDir()}
}

func (c *capture) write(name, content string) {
	c.t.Helper()
	if err := os.WriteFile(filepath.Join(c.dir, name), []byte(content), 0o600); err != nil {
		c.t.Fatal(err)
	}
}

func (c *capture) headers(seq int64, ct string) {
	b, _ := json.Marshal(map[string][]string{"Content-Type": {ct}})
	c.write(fmt.Sprintf("rec-%06d.headers.json", seq), string(b))
}

// json adds an honest whole-body JSON record.
func (c *capture) json(method, path string, status int, body string) *record {
	seq := int64(len(c.recs) + 1)
	name := fmt.Sprintf("rec-%06d.body", seq)
	c.write(name, body)
	c.headers(seq, "application/json")
	c.recs = append(c.recs, record{Seq: seq, Method: method, Path: path, Status: status,
		ContentType: "application/json", Class: "json", File: name, BodyBytes: int64(len(body))})
	return &c.recs[len(c.recs)-1]
}

// stream adds an honest NDJSON record whose index is derived from frames
// exactly as the proxy would; callers mutate the returned record to plant a
// contradiction between body and index.
func (c *capture) stream(path, class string, frames ...string) *record {
	seq := int64(len(c.recs) + 1)
	name := fmt.Sprintf("rec-%06d.body", seq)
	body := strings.Join(frames, "\n") + "\n"
	c.write(name, body)
	c.headers(seq, "application/x-ndjson")
	r := record{Seq: seq, Method: "GET", Path: path, Status: 200, ContentType: "application/x-ndjson",
		Class: class, File: name, BodyBytes: int64(len(body)), Frames: int64(len(frames))}
	for _, f := range frames {
		var fr struct {
			Type    string `json:"type"`
			NextOff int64  `json:"nextOffset"`
		}
		_ = json.Unmarshal([]byte(f), &fr)
		switch fr.Type {
		case "stdout":
			r.StdoutFrames++
			r.LastNextOff = fr.NextOff
		case "stderr":
			r.LastStderrOff = fr.NextOff
		case "exit", "error", "reconnect":
			r.Terminal = true
		}
	}
	if class == "cancel" {
		r.ClientAborted = true
	}
	c.recs = append(c.recs, r)
	return &c.recs[len(c.recs)-1]
}

// seal writes index.ndjson from the (possibly mutated) records.
func (c *capture) seal(accepted, completed, failed int64) {
	var b strings.Builder
	for _, r := range c.recs {
		line, _ := json.Marshal(r)
		b.Write(line)
		b.WriteByte('\n')
	}
	c.write("index.ndjson", b.String())
	if accepted >= 0 {
		c.write("audit.json", fmt.Sprintf(`{"accepted":%d,"completed":%d,"failed":%d,"fatal":false}`, accepted, completed, failed))
	}
}

func (c *capture) sealAudited() { c.seal(int64(len(c.recs)), int64(len(c.recs)), 0) }

func TestValidatorEvidence(t *testing.T) {
	t.Setenv("BBRAFT_TEST_REQUIRE_ALL", "1")
	v := compat.Load(t)
	final := config{jsonBound: 1 << 20, frameBound: 1 << 20, final: true}
	live := final
	live.final = false

	cases := []struct {
		name  string
		cfg   config
		build func(c *capture)
		want  string // "" = must pass
	}{
		{"positive json+stream+cut+reconnect+cancel", final, func(c *capture) {
			c.json("GET", "/api/v1/me", 200, meBody)
			c.stream(streamP, "cut", attached, stdout3)
			c.stream(streamP+"?stdoutOffset=3&stderrOffset=0", "ndjson", attached, stdout6, exit0)
			c.stream("/api/v1/sandboxes/bx_s/commands/8/stream", "cancel", attached, stdout3)
			c.sealAudited()
		}, ""},
		{"live preflight without audit", live, func(c *capture) {
			c.json("GET", "/api/v1/me", 200, meBody)
			c.seal(-1, 0, 0)
		}, ""},
		{"final without audit", final, func(c *capture) {
			c.json("GET", "/api/v1/me", 200, meBody)
			c.seal(-1, 0, 0)
		}, "class=audit-missing"},
		{"accepted2 completed1 one record", final, func(c *capture) {
			c.json("GET", "/api/v1/me", 200, meBody)
			c.seal(2, 1, 0)
		}, "class=audit-incomplete"},
		{"audit completed differs from records", final, func(c *capture) {
			c.json("GET", "/api/v1/me", 200, meBody)
			c.seal(2, 2, 0)
		}, "class=audit-completed"},
		{"audit failed counter wrong", final, func(c *capture) {
			c.json("GET", "/api/v1/me", 200, meBody)
			c.seal(1, 1, 1)
		}, "class=audit-failed"},
		{"audit fatal", final, func(c *capture) {
			c.json("GET", "/api/v1/me", 200, meBody)
			c.seal(-1, 0, 0)
			c.write("audit.json", `{"accepted":1,"completed":1,"failed":0,"fatal":true}`)
		}, "class=proxy-fatal"},
		{"audit corrupt", final, func(c *capture) {
			c.json("GET", "/api/v1/me", 200, meBody)
			c.seal(-1, 0, 0)
			c.write("audit.json", `{not json`)
		}, "class=audit-unreadable"},
		{"audit oversized", final, func(c *capture) {
			c.json("GET", "/api/v1/me", 200, meBody)
			c.seal(-1, 0, 0)
			c.write("audit.json", `{"accepted":1,"completed":1,"failed":0,"fatal":false,"pad":"`+strings.Repeat("x", 8192)+`"}`)
		}, "class=audit-unreadable"},
		{"headers missing", final, func(c *capture) {
			c.json("GET", "/api/v1/me", 200, meBody)
			c.sealAudited()
			_ = os.Remove(filepath.Join(c.dir, "rec-000001.headers.json"))
		}, "class=missing-headers"},
		{"headers corrupt", final, func(c *capture) {
			c.json("GET", "/api/v1/me", 200, meBody)
			c.sealAudited()
			c.write("rec-000001.headers.json", "{")
		}, "class=corrupt-headers"},
		{"headers content-type differs", final, func(c *capture) {
			c.json("GET", "/api/v1/me", 200, meBody)
			c.sealAudited()
			c.headers(1, "text/plain")
		}, "class=header-content-type"},
		{"headers without content-type", final, func(c *capture) {
			c.json("GET", "/api/v1/me", 200, meBody)
			c.sealAudited()
			c.write("rec-000001.headers.json", `{"X-Other":["1"]}`)
		}, "class=header-content-type"},
		{"cut body holds exit but index says no terminal", final, func(c *capture) {
			r := c.stream(streamP, "cut", attached, stdout3, exit0)
			r.Terminal = false
			c.stream(streamP+"?stdoutOffset=3&stderrOffset=0", "ndjson", attached, stdout6, exit0)
			c.sealAudited()
		}, "class=cut-terminal-in-prefix"},
		{"cut cursor in index differs from body", final, func(c *capture) {
			r := c.stream(streamP, "cut", attached, stdout3)
			r.LastNextOff = 99
			c.stream(streamP+"?stdoutOffset=99&stderrOffset=0", "ndjson", attached, stdout6, exit0)
			c.sealAudited()
		}, "class=index-cursor"},
		{"cut reconnects at the wrong offset", final, func(c *capture) {
			c.stream(streamP, "cut", attached, stdout3)
			c.stream(streamP+"?stdoutOffset=2&stderrOffset=0", "ndjson", attached, stdout6, exit0)
			c.sealAudited()
		}, "class=reconnect-offset"},
		{"frame count lies", final, func(c *capture) {
			r := c.stream(streamP, "ndjson", attached, stdout3, exit0)
			r.Frames = 9
			c.sealAudited()
		}, "class=frame-count"},
		{"stdout frame count lies", final, func(c *capture) {
			r := c.stream(streamP, "ndjson", attached, stdout3, exit0)
			r.StdoutFrames = 5
			c.sealAudited()
		}, "class=index-stdout-frames"},
		{"index terminal flag lies", final, func(c *capture) {
			r := c.stream(streamP, "ndjson", attached, stdout3, exit0)
			r.Terminal = false
			c.sealAudited()
		}, "class=index-terminal"},
		{"normal stream with frames after exit", final, func(c *capture) {
			c.stream(streamP, "ndjson", attached, stdout3, exit0, stdout6)
			c.sealAudited()
		}, "class=post-terminal"},
		{"normal stream without terminal", final, func(c *capture) {
			c.stream(streamP, "ndjson", attached, stdout3)
			c.sealAudited()
		}, "class=terminal-state"},
		{"attached frame mid-stream", final, func(c *capture) {
			c.stream(streamP, "ndjson", stdout3, attached, exit0)
			c.sealAudited()
		}, "class=frame-order"},
		{"cursor regression", final, func(c *capture) {
			c.stream(streamP, "ndjson", attached, stdout6, stdout3, exit0)
			c.sealAudited()
		}, "class=cursor-regression"},
		{"body bytes disagree", final, func(c *capture) {
			r := c.stream(streamP, "ndjson", attached, stdout3, exit0)
			r.BodyBytes += 7
			c.sealAudited()
		}, "class=body-accounting"},
		{"spool lacks trailing newline", final, func(c *capture) {
			r := c.stream(streamP, "ndjson", attached, stdout3, exit0)
			b, _ := os.ReadFile(filepath.Join(c.dir, r.File))
			c.write(r.File, strings.TrimSuffix(string(b), "\n"))
			r.BodyBytes--
			c.sealAudited()
		}, "class=spool-size"},
		{"cancel tail unaccounted", final, func(c *capture) {
			r := c.stream("/api/v1/sandboxes/bx_s/commands/8/stream", "cancel", attached, stdout3)
			r.TailBytes = 3
			c.sealAudited()
		}, "class=cancel-tail-account"},
		{"cancel without client abort", final, func(c *capture) {
			r := c.stream("/api/v1/sandboxes/bx_s/commands/8/stream", "cancel", attached, stdout3)
			r.ClientAborted = false
			c.sealAudited()
		}, "class=cancel-without-client-abort"},
		{"class vs content type", final, func(c *capture) {
			r := c.json("GET", "/api/v1/me", 200, meBody)
			r.Class = "ndjson"
			c.sealAudited()
		}, "class=class-content-type"},
		{"fail record", final, func(c *capture) {
			r := c.json("GET", "/api/v1/me", 200, meBody)
			r.Class, r.FailReason = "fail", "unexpected-eof"
			c.seal(1, 1, 1)
		}, "class=harness:unexpected-eof"},
		{"index missing", final, func(c *capture) {}, "class=index-missing"},
		{"index corrupt", final, func(c *capture) {
			c.write("index.ndjson", "{oops\n")
		}, "class=index-corrupt"},
		{"sequence gap", final, func(c *capture) {
			c.json("GET", "/api/v1/me", 200, meBody)
			c.json("GET", "/api/v1/me", 200, meBody)
			c.recs[1].Seq = 3
			c.sealAudited()
		}, "class=sequence-gap"},
		{"matrix violation", final, func(c *capture) {
			c.json("GET", "/api/v1/sandboxes/bx_s", 500, `{"ok":false,"type":"sandbox.error","status":500,"code":"not_found","message":"x","error":{"code":"not_found","message":"x","status":500},"requestId":"r"}`)
			c.sealAudited()
		}, "class=matrix-violation"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := newCapture(t)
			tc.build(c)
			got := runCheck(t, v, c.dir, tc.cfg)
			switch {
			case tc.want == "" && len(got.lines) != 0:
				t.Fatalf("legal capture rejected: %v", got.lines)
			case tc.want != "" && !got.has(tc.want):
				t.Fatalf("want %q, got %v", tc.want, got.lines)
			}
		})
	}

	t.Run("diagnostics never echo uncontrolled index text", func(t *testing.T) {
		c := newCapture(t)
		r1 := c.json("BREW-SECRET-METHOD", "/api/v1/me", 200, meBody)
		r1.Class, r1.FailReason = "fail", "SECRET-REASON"
		r2 := c.json("GET", "/api/v1/me", 200, meBody)
		r2.Class = "SECRET-CLASS"
		r3 := c.json("BREW-SECRET-METHOD", "/api/v1/nowhere/at/all", 200, meBody)
		_ = r3
		c.sealAudited()
		got := runCheck(t, v, c.dir, final)
		for _, l := range got.lines {
			if strings.Contains(l, "SECRET") {
				t.Errorf("diagnostic echoes index text: %q", l)
			}
		}
		for _, want := range []string{"harness:unknown-reason", "class=unknown-class", "method=other"} {
			if !got.has(want) {
				t.Errorf("missing fixed token %q in %v", want, got.lines)
			}
		}
	})
}
EOF
  # The generated code is formatted at generation (like make proto), so every
  # consumer builds canonical Go regardless of heredoc alignment.
  gofmt -w "$d/proxy"/*.go "$d/validator"/*.go "$d/pgenv"/*.go
}

# ---------------------------------------------------------------------------
# Self-test. Everything here is cheap and local: no VM, no KVM, no server, no
# node, no sysctl writes, no real network beyond loopback httptest inside the
# generated Go tests. Artifacts persist under E2E_OUT.
# ---------------------------------------------------------------------------
e2e_selftest() {
  local repo=$PWD out self child rc bad keep
  keep() { echo "helpers kept: $helpers"; exit 1; }
  mkdir -p "$E2E_OUT"
  out="$E2E_OUT/e2e-p1-selftest"
  rm -rf "$out"; mkdir -p "$out"
  echo "selftest artifacts: $out"

  # 1. Embedded Go helpers: formatted at generation; proxy, pgenv and
  #    validator all build; the proxy package passes its LIVE httptest suite
  #    (real recorder, cut, cancel, bounds, IO failure, audit barrier).
  mkdir -p "$repo/.cache" 2>/dev/null || true
  helpers=$(mktemp -d "$repo/.cache/e2e-p1-selftest.XXXXXXXX")
  e2e_write_go_helpers "$helpers"
  bad=$(gofmt -l "$helpers" 2>&1) || true
  [ -z "$bad" ] || { echo "SELFTEST FAIL: generated Go not gofmt-clean: $bad"; keep; }
  go build -o "$out/e2e-p1-proxy" "./${helpers#"$repo"/}/proxy" || { echo "SELFTEST FAIL: proxy build"; keep; }
  go build -o "$out/pgenv" "./${helpers#"$repo"/}/pgenv" || { echo "SELFTEST FAIL: pgenv build"; keep; }
  go test -run TestNothing "./${helpers#"$repo"/}/validator" -count=1 >/dev/null 2>&1 \
    || { echo "SELFTEST FAIL: validator does not compile"; keep; }
  go test "./${helpers#"$repo"/}/proxy" -count=1 -v >"$out/proxy-live.log" 2>&1 \
    || { echo "SELFTEST FAIL: live proxy tests"; sed -n 1,25p "$out/proxy-live.log"; keep; }
  echo "PASS proxy-live-httptest ($(grep -c '^--- PASS' "$out/proxy-live.log") tests)"

  # 2. ERR/errexit semantics of the capture helpers, in a child bash using
  #    THIS script's own functions: expected nonzero codes never trip the
  #    fatal ERR path; an unexpected failure does trip ERR and cleanup.
  child=$out/err-semantics.sh
  {
    echo 'set -euo pipefail; set -E'
    echo "E2E_HELPERS_ONLY=1 source '${BASH_SOURCE[0]}'"
    echo 'sentinel=$1'
    echo 'export E2E_ERRLOG=$sentinel.err'
    echo 'e2e_run_rc /dev/null bash -c "exit 7"'
    echo '[ "$E2E_RC" = 7 ] || { echo bad-expected-rc; exit 30; }'
    echo 'e2e_expect_rc 124 /dev/null bash -c "exit 124"'
    echo 'echo expected-ok'
    echo 'trap "touch $sentinel.cleanup" EXIT'
    echo 'bash -c "exit 9"'
    echo 'echo unreachable'
  } >"$child"
  rm -f "$out/err-sentinel.err" "$out/err-sentinel.cleanup"
  e2e_run_rc "$out/err-semantics.out" bash "$child" "$out/err-sentinel"
  if [ "$E2E_RC" != 9 ]; then
    echo "SELFTEST FAIL: child exit $E2E_RC, want 9 (unexpected failure must fail)"; keep
  fi
  grep -q expected-ok "$out/err-semantics.out" || { echo "SELFTEST FAIL: expected-rc path did not run"; keep; }
  grep -q unreachable "$out/err-semantics.out" && { echo "SELFTEST FAIL: errexit did not stop the child"; keep; }
  [ -s "$out/err-sentinel.err" ] || { echo "SELFTEST FAIL: ERR trap did not record"; keep; }
  [ -e "$out/err-sentinel.cleanup" ] || { echo "SELFTEST FAIL: cleanup trap did not run"; keep; }
  echo "PASS err-semantics"

  # 3. Literal redactor: punctuation-heavy literals, tokens and DSNs vanish;
  #    negative control proves the OLD broken redactor fails the same checks.
  local red=$out/redact.list
  {
    printf '%s\n' 'S3cr3t./|*^$()[]{}+?-end'
    printf '%s\n' 'bmb_TOKEN_LITERAL_9'
    printf '%s\n' 'postgres://u:p%40ss@db.example.internal:5432/e2e'
  } >"$red"
  export E2E_REDACT_FILE=$red
  local sample
  sample=$(printf '%s\n' \
    'token S3cr3t./|*^$()[]{}+?-end here' \
    'Authorization: Bearer bmb_TOKEN_LITERAL_9' \
    'dsn postgres://u:p%40ss@db.example.internal:5432/e2e and /api/bbraft/downloads/Ab3-xyz._= tail' \
    'clean line stays' | e2e_redact)
  case $sample in
    *S3cr3t*|*bmb_TOKEN_LITERAL_9*|*p%40ss*|*downloads/Ab3*) echo "SELFTEST FAIL: redactor leaked"; keep ;;
  esac
  grep -q 'clean line stays' <<<"$sample" || { echo "SELFTEST FAIL: redactor ate clean text"; keep; }
  # Negative control: a non-redacting passthrough (the frozen source's sed -i
  # literal branch never redacted) must FAIL the same detection.
  child=$out/redact-negative-control.sh
  cat >"$child" <<'CHILD'
set -euo pipefail
E2E_HELPERS_ONLY=1 source "$1"
e2e_redact() { cat; }
printf 'token S3cr3t./|*^$()[]{}+?-end here' | e2e_redact
CHILD
  e2e_run_rc "$out/redact-negative.out" bash "$child" "${BASH_SOURCE[0]}"
  if [ "$E2E_RC" = 0 ]; then
    if ! grep -q 'S3cr3t' "$out/redact-negative.out"; then
      echo "SELFTEST FAIL: detection cannot see a passthrough redactor"; keep
    fi
    echo "PASS redactor-negative-control (passthrough detected as leaking)"
  else
    echo "PASS redactor-negative-control (passthrough failed loudly rc=$E2E_RC)"
  fi
  echo "PASS redactor"

  # 4. e2e_jq forwards the full argv (jq --arg support).
  printf '[{"id":"bx_probe"}]\n' >"$out/list.json"
  e2e_jq --arg id bx_probe '.[0].id == $id' "$out/list.json" \
    || { echo "SELFTEST FAIL: e2e_jq drops --arg"; keep; }
  echo "PASS jq-arg-forwarding"

  # 5. pg_query protocol: SQL on stdin, no -c anywhere, -v variables kept
  #    (mirrors the lead's probe), with a fake psql.
  child=$out/sql-protocol.sh
  cat >"$child" <<'CHILD'
set -euo pipefail
E2E_HELPERS_ONLY=1 source "$1"
psql() {
    local arg sql
    for arg in "$@"; do
        [[ "$arg" != -c ]] || return 44
    done
    sql=$(cat)
    [[ "$sql" == "SELECT :'nid';" ]] || return 45
    printf '1\n'
}
value=$(pg_query "SELECT :'nid';" -v nid=bx_probe)
[[ "$value" == 1 ]]
CHILD
  e2e_run_rc "$out/sql-protocol.out" bash "$child" "${BASH_SOURCE[0]}"
  [ "$E2E_RC" = 0 ] || { echo "SELFTEST FAIL: pg_query protocol rc=$E2E_RC"; keep; }
  echo "PASS sql-protocol-stdin"

  # 6. pgenv parses real DSNs offline: percent-decoded password, preserved
  #    sslmode and options, no network; unsupported forms fail; nothing
  #    secret is printed.
  printf '%s' 'postgres://u%73er:p%40ss%21@db.test:5433/e2edb?sslmode=require&options=-c%20search_path%3Dpublic' \
    >"$out/dsn-ok"
  "$out/pgenv" -dsn-file "$out/dsn-ok" -out "$out" 2>"$out/pgenv.err" \
    || { echo "SELFTEST FAIL: pgenv rejected a valid DSN"; keep; }
  grep -q "export PGHOST='db.test'" "$out/pgenv.sh" || { echo "SELFTEST FAIL: host"; keep; }
  grep -q 'export PGPORT=5433' "$out/pgenv.sh" || { echo "SELFTEST FAIL: port"; keep; }
  grep -q "export PGUSER='user'" "$out/pgenv.sh" || { echo "SELFTEST FAIL: decoded user"; keep; }
  grep -q "export PGPASSWORD='p@ss!'" "$out/pgenv.sh" || { echo "SELFTEST FAIL: decoded password"; keep; }
  grep -q "export PGDATABASE='e2edb'" "$out/pgenv.sh" || { echo "SELFTEST FAIL: database"; keep; }
  grep -q "export PGSSLMODE='require'" "$out/pgenv.sh" || { echo "SELFTEST FAIL: sslmode preserved"; keep; }
  grep -q 'statement_timeout=2000' "$out/pgenv.sh" || { echo "SELFTEST FAIL: statement bound"; keep; }
  grep -q 'search_path=public' "$out/pgenv.sh" || { echo "SELFTEST FAIL: options preserved"; keep; }
  [ -s "$out/pgenv.err" ] && { echo "SELFTEST FAIL: pgenv wrote diagnostics"; keep; }
  printf 'not a dsn' >"$out/dsn-bad"
  e2e_run_rc "$out/pgenv-bad.out" "$out/pgenv" -dsn-file "$out/dsn-bad" -out "$out"
  [ "$E2E_RC" = 1 ] || { echo "SELFTEST FAIL: unsupported DSN rc=$E2E_RC"; keep; }
  grep -qE 'p@ss|postgres://' "$out/pgenv-bad.out" && { echo "SELFTEST FAIL: pgenv echoed DSN content"; keep; }
  echo "PASS pgenv-parse"

  # 7. Validator, DSN and proxy evidence: the generated Go tests (positives
  #    above the old size caps, every negative control with its exact fixed
  #    class, required audit/headers, derived stream evidence, DSN subset).
  go test "./${helpers#"$repo"/}/pgenv" -count=1 >"$out/pgenv-go.log" 2>&1 \
    || { echo "SELFTEST FAIL: pgenv tests"; sed -n 1,25p "$out/pgenv-go.log"; keep; }
  echo "PASS pgenv-go-tests"
  BBRAFT_TEST_REQUIRE_ALL=1 go test -run TestValidatorEvidence "./${helpers#"$repo"/}/validator" -count=1 >"$out/validator-go.log" 2>&1 \
    || { echo "SELFTEST FAIL: validator evidence tests"; sed -n 1,30p "$out/validator-go.log" | cut -c1-200; keep; }
  echo "PASS validator-evidence-go-tests"

  # 8. The actual reconnect selection on an out-of-order index with decoys.
  self=$out/cap-select; mkdir -p "$self"
  cat >"$self/index.ndjson" <<'IDX'
{"seq":5,"method":"GET","path":"/api/v1/sandboxes/bx_a/commands/9/stream?stdoutOffset=7&stderrOffset=0","class":"ndjson"}
{"seq":3,"method":"GET","path":"/api/v1/sandboxes/bx_a/commands/9/stream","class":"cut","lastNextOffset":7,"lastStderrOffset":0}
{"seq":4,"method":"GET","path":"/api/v1/sandboxes/bx_b/commands/9/stream?stdoutOffset=1&stderrOffset=0","class":"ndjson"}
{"seq":1,"method":"POST","path":"/api/v1/sandboxes/bx_a/commands","class":"json"}
IDX
  cutrec=$(e2e_cut_record "$self/index.ndjson" 0) || { echo "SELFTEST FAIL: cut selection (jq)"; keep; }
  [ "$(jq -r .seq <<<"$cutrec")" = 3 ] || { echo "SELFTEST FAIL: wrong cut record"; keep; }
  e2e_resume_ok "$self/index.ndjson" 3 /api/v1/sandboxes/bx_a/commands/9/stream 7 0 \
    || { echo "SELFTEST FAIL: exact resume rejected"; keep; }
  if e2e_resume_ok "$self/index.ndjson" 3 /api/v1/sandboxes/bx_a/commands/9/stream 8 0; then
    echo "SELFTEST FAIL: wrong resume offset accepted"; keep
  fi
  echo "PASS reconnect-selection"

  # 9. Strict teardown with fakes (real code paths of the teardown and the
  #    daemon/helper wrappers): sweep, stale delete, node stop + proof through
  #    the standalone errexit-active helper runner, proxy drain, capture
  #    validation before deletion, leak scan, ip_forward restore, evidence in
  #    the ACTUAL random node_parent kept, capture/secrets removed. A helper
  #    whose first command fails must NEVER reach its later success marker and
  #    must fail the teardown while the remaining proofs still run.
  child=$out/teardown-fake.sh
  cat >"$child" <<'CHILD'
set -euo pipefail; set -E
E2E_HELPERS_ONLY=1 source "$1"
d=$2; CALLS=$3; MODE=$4; export CALLS
mkdir -p "$d/bin" "$d/secrets" "$d/capture" "$d/logs" "$d/.helpers/validator"
work=$d; helpers=$d/.helpers; B=$d/bin/bbraft
node_parent=$(mktemp -d "$d/node-parent.XXXXXXXX")
mkdir -p "$node_parent/node"
touch "$work/capture/index.ndjson" "$work/secrets/database-url" "$work/baseline.links" "$work/pgenv"; chmod +x "$work/pgenv"
printf 'stale-secret\n' >"$work/secrets/redact.list"
export E2E_REDACT_FILE=$work/secrets/redact.list
api_url=http://127.0.0.1:1
cat >"$B" <<'BB'
#!/usr/bin/env bash
echo "$*" >>"$CALLS"
case "$1" in
  list) printf '{"ok":true,"type":"sandbox.list","sandboxes":[{"id":"bx_stale1","state":"error"}]}\n' ;;
  delete) exit 0 ;;
  info) printf '{"ok":false}\n'; exit 1 ;;
esac
BB
chmod +x "$B"
e2e_node_stop() {
  echo node-stop >>"$CALLS"
  if [ "$MODE" = helperfail ]; then false; fi
  echo stop-reached >>"$CALLS"
}
e2e_node_leak_proof() { echo leak-proof >>"$CALLS"; }
e2e_ipf_write() { echo "ipf-write:$1" >>"$CALLS"; }
e2e_ipf_read() { echo 0; }
e2e_validate() { echo "validate:$1" >>"$CALLS"; }
e2e_leak_scan() { echo leak-scan >>"$CALLS"; }
e2e_reservation_proof() { echo res-proof >>"$CALLS"; }
# Signals to other-session processes are not deliverable in every local
# sandbox, so kill is shadowed (recorded; -0 probes answer "gone"). The fake
# daemons exit by themselves, so the REAL join/reap and exit-status logic runs.
kill() {
  case " $* " in
    *" -0 "*) return 1 ;;
  esac
  echo "kill:$*" >>"$CALLS"
  return 0
}
echo evidence >"$node_parent/node/evidence.log"
node_started=1; ip_forward_set=1; ip_forward_prev=0; api_started=1
setsid bash -c 'exit 0' & proxy_pid=$!
setsid bash -c 'exit 0' & server_pid=$!
sleep 0.5
set +e
e2e_teardown 0
echo "trc=$?" >>"$CALLS"
set -e
[ -e "$node_parent/node/evidence.log" ] || { echo "evidence removed"; exit 40; }
[ -z "$proxy_pid$server_pid" ] || { echo "daemon tracking not cleared"; exit 41; }
CHILD
  local mode calls
  for mode in ok helperfail; do
    calls=$out/teardown-calls-$mode.log
    rm -f "$calls"; rm -rf "$out/fake-work-$mode"
    e2e_run_rc "$out/teardown-fake-$mode.out" bash "$child" "${BASH_SOURCE[0]}" "$out/fake-work-$mode" "$calls" "$mode"
    [ "$E2E_RC" = 0 ] || { echo "SELFTEST FAIL: teardown($mode) child rc=$E2E_RC"; tail -5 "$out/teardown-fake-$mode.out"; keep; }
    for want in 'list --all --json' 'delete bx_stale1 --yes --wait' res-proof node-stop leak-proof validate:final leak-scan 'ipf-write:0'; do
      grep -q -- "$want" "$calls" || { echo "SELFTEST FAIL: teardown($mode) lacks '$want'"; keep; }
    done
    [ ! -e "$out/fake-work-$mode/capture/index.ndjson" ] || { echo "SELFTEST FAIL: capture not removed"; keep; }
    [ ! -e "$out/fake-work-$mode/secrets/database-url" ] || { echo "SELFTEST FAIL: secrets not removed"; keep; }
  done
  grep -q 'trc=0' "$out/teardown-calls-ok.log" || { echo "SELFTEST FAIL: clean teardown reported leaks"; keep; }
  grep -q stop-reached "$out/teardown-calls-ok.log" || { echo "SELFTEST FAIL: healthy helper did not complete"; keep; }
  grep -q 'trc=1' "$out/teardown-calls-helperfail.log" || { echo "SELFTEST FAIL: helper failure not reported by teardown"; keep; }
  if grep -q stop-reached "$out/teardown-calls-helperfail.log"; then
    echo "SELFTEST FAIL: failing helper continued past its first false (errexit lost)"; keep
  fi
  echo "PASS strict-teardown (fakes: ok + helper failure)"

  # 10. Deliberate-break path: E2E_BREAK=<stage name> fails that stage cleanly.
  e2e_run_rc "$out/break.out" env E2E_BREAK=probe E2E_HELPERS_ONLY=1 bash -c \
    'source "$1"; stage_name=""; e2e_stage probe' _ "${BASH_SOURCE[0]}"
  [ "$E2E_RC" = 1 ] && grep -q 'deliberate break at stage probe' "$out/break.out" \
    || { echo "SELFTEST FAIL: deliberate break path"; cat "$out/break.out"; keep; }
  echo "PASS deliberate-break"

  rm -rf "$helpers"
  echo "SELFTEST PASS"
}

# ---------------------------------------------------------------------------
# Child, capture and proof helpers shared by stage 14 and the teardown.
# Functions returning a status are called as plain statements or `if !`;
# their bodies check every command explicitly (errexit may be off there).
# ---------------------------------------------------------------------------

# e2e_proc_state PID prints the /proc state letter, or "gone".
e2e_proc_state() {
  local st
  st=$(cat "/proc/$1/stat" 2>/dev/null) || { echo gone; return 0; }
  st=${st##*) }
  echo "${st%% *}"
}

# e2e_reap_daemon PID GRACE_SECONDS TERMs the daemon's process group, joins and
# reaps it (bounded; exit status in E2E_DAEMON_RC), KILLs leftovers after the
# grace and succeeds only once the whole group is verifiably absent.
e2e_reap_daemon() {
  local pid=$1 grace=$2 phase i st reaped=0
  E2E_DAEMON_RC=""
  kill -TERM -- "-$pid" 2>/dev/null || true
  for phase in term kill; do
    for ((i = 0; i < grace * 10; i++)); do
      if [ "$reaped" = 0 ]; then
        st=$(e2e_proc_state "$pid")
        case $st in
          Z | gone)
            if wait "$pid" 2>/dev/null; then E2E_DAEMON_RC=0; else E2E_DAEMON_RC=$?; fi
            reaped=1 ;;
        esac
      fi
      if [ "$reaped" = 1 ] && ! kill -0 -- "-$pid" 2>/dev/null; then return 0; fi
      sleep 0.1
    done
    if [ "$phase" = term ]; then
      kill -KILL -- "-$pid" 2>/dev/null || true
      grace=5
    fi
  done
  return 1
}

# e2e_stop_proxy drains the recording proxy (SIGTERM = graceful drain plus the
# final audit) and requires exit status 0. Tracking is cleared only after the
# process group is verified absent.
e2e_stop_proxy() {
  local rc
  [ -n "$proxy_pid" ] || return 0
  if ! e2e_reap_daemon "$proxy_pid" 25; then
    echo "TEARDOWN LEAK: proxy process group survived"
    return 1
  fi
  rc=$E2E_DAEMON_RC
  proxy_pid=""
  if [ "$rc" != 0 ]; then
    echo "proxy exited with status $rc (drain failed or fatal capture error)"
    return 1
  fi
}

e2e_stop_server() {
  [ -n "$server_pid" ] || return 0
  if ! e2e_reap_daemon "$server_pid" 25; then
    echo "TEARDOWN LEAK: server process group survived"
    return 1
  fi
  server_pid=""
}

# e2e_run_helper SECONDS FUNC [ARG...] runs a node-helper function as a
# STANDALONE background subshell with errexit really active inside it (never
# under if/||, which would disable it), without the outer traps, and collects
# a bounded exit status in E2E_HELPER_RC (124 on timeout). Its output is
# printed redacted afterwards.
e2e_run_helper() {
  local secs=$1 pid i out="$work/logs/helper-${2}.out"
  shift
  (trap - EXIT ERR; set -eEuo pipefail; "$@") >"$out" 2>&1 &
  pid=$!
  for ((i = 0; i < secs * 5; i++)); do
    kill -0 "$pid" 2>/dev/null || break
    sleep 0.2
  done
  if kill -0 "$pid" 2>/dev/null; then
    kill -KILL "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
    E2E_HELPER_RC=124
  elif wait "$pid"; then
    E2E_HELPER_RC=0
  else
    E2E_HELPER_RC=$?
  fi
  tail -20 "$out" | e2e_redact | cut -c1-300
  return 0
}

# e2e_barrier waits (bounded) until the proxy reports accepted==completed and
# sets E2E_BARRIER_SEQ to the largest ASSIGNED sequence of that snapshot.
e2e_barrier() {
  local i snap
  for ((i = 0; i < 120; i++)); do
    if snap=$(curl -fsS --max-time 5 "http://127.0.0.1:$E2E_PROXY_PORT/__e2e/barrier" 2>/dev/null) &&
      jq -e '.accepted == .completed and .fatal == false' <<<"$snap" >/dev/null 2>&1; then
      E2E_BARRIER_SEQ=$(jq -r '.seq' <<<"$snap") || return 1
      return 0
    fi
    sleep 0.25
  done
  return 1
}

# e2e_cut_record INDEX FROM prints the ONE cut record after FROM (compact).
e2e_cut_record() {
  jq -c -s --argjson from "$2" '
    map(select(.seq > $from and .class == "cut")) as $c
    | if ($c | length) == 1 then $c[0] else error("cut count") end' "$1"
}

# e2e_resume_ok INDEX CUT_SEQ CUT_PATH OFF ERR succeeds iff the FIRST GET of the
# identical stream path after the cut resumed at exactly the committed
# offsets (query parsed, any parameter order).
e2e_resume_ok() {
  jq -e -s --argjson cs "$2" --arg p "$3" --arg o "$4" --arg e "$5" '
    map(select(.seq > $cs and .method == "GET" and ((.path | split("?")[0]) == $p)))
    | sort_by(.seq) | .[0]
    | ((.path | split("?")[1] // "" | split("&") | map(split("=") | {(.[0]): (.[1] // "")}) | add) // {}) as $q
    | $q.stdoutOffset == $o and $q.stderrOffset == $e' "$1" >/dev/null
}

# e2e_cli ARGS... runs the bbraft CLI as the EXTERNAL client (ssh/scp ingress
# is only reachable from the client netns) with the isolated config dir and
# API URL passed explicitly (nonsecret env; the key lives in that config).
e2e_cli() {
  e2e_client env "XDG_CONFIG_HOME=$XDG_CONFIG_HOME" "BBRAFT_API_URL=$BBRAFT_API_URL" "$B" "$@"
}

# e2e_pty_cli SECONDS ARGS... is e2e_cli under a bounded real local PTY.
e2e_pty_cli() {
  local secs=$1 cmd
  shift
  cmd=$(printf '%q ' env "XDG_CONFIG_HOME=$XDG_CONFIG_HOME" "BBRAFT_API_URL=$BBRAFT_API_URL" "$B" "$@")
  e2e_client timeout "$secs" script -qec "$cmd" /dev/null
}

# e2e_validate MODE LOG runs the pinned-spec validator over the capture: MODE
# "final" REQUIRES the drained proxy audit, "live" is the explicitly weaker
# preflight used while the proxy still runs.
e2e_validate() {
  E2E_VALIDATE=$1 E2E_CAPTURE="$work/capture" go test -v -run TestE2ECompat \
    "./${helpers#"$repo"/}/validator" -count=1 >"$2" 2>&1 || {
    e2e_redact <"$2" | cut -c1-300 | head -40
    return 1
  }
  grep -o 'validated records: [0-9]*' "$2" || true
}

e2e_unit_names() {
  local raw
  raw=$(systemctl list-units --all --no-legend 'bbraft*') || return 1
  printf '%s\n' "$raw" | awk '{ for (i = 1; i <= NF; i++) if ($i ~ /\.(service|socket|scope|slice|timer|mount|target)$/) { print $i; break } }' | sort
}

# e2e_leak_scan proves no process, link, nft table or unit appeared since the
# baseline (added entries are leaks; collection failure is a failure).
e2e_leak_scan() {
  local rc=0 new
  pgrep -af "$E2E_PROC_RE" >"$work/final.procs.raw" || true
  sort "$work/final.procs.raw" >"$work/final.procs"
  new=$(comm -13 <(sort "$work/baseline.procs") "$work/final.procs") || rc=1
  [ -z "$new" ] || { printf 'LEAK processes:\n%s\n' "$new" | e2e_redact | head -20; rc=1; }
  if ip -o link >"$work/final.links.raw"; then
    sort "$work/final.links.raw" >"$work/final.links"
    new=$(comm -13 <(sort "$work/baseline.links") "$work/final.links") || rc=1
    [ -z "$new" ] || { printf 'LEAK links:\n%s\n' "$new" | head -20; rc=1; }
  else
    echo "LEAK: final ip link collection failed"; rc=1
  fi
  if e2e_priv nft list tables >"$work/final.nft.raw"; then
    sort "$work/final.nft.raw" >"$work/final.nft"
    new=$(comm -13 <(sort "$work/baseline.nft") "$work/final.nft") || rc=1
    [ -z "$new" ] || { printf 'LEAK nft tables:\n%s\n' "$new" | head -20; rc=1; }
  else
    echo "LEAK: final nft collection failed"; rc=1
  fi
  if e2e_unit_names >"$work/final.units"; then
    new=$(comm -13 "$work/baseline.units" "$work/final.units") || rc=1
    [ -z "$new" ] || { printf 'LEAK units:\n%s\n' "$new" | head -20; rc=1; }
  else
    echo "LEAK: final unit collection failed"; rc=1
  fi
  return "$rc"
}

# e2e_reservation_proof: node counters are zero and no row of THIS owner keeps
# a reservation (owner-scoped; skipped for the owner part before the owner uuid
# is known).
e2e_reservation_proof() {
  local n
  e2e_pg_env || return 1
  n=$(pg_query "SELECT count(*) FROM public.nodes WHERE id='e2e-node' AND reserved_vcpus=0 AND reserved_memory_mib=0 AND reserved_disk_gib=0") || return 1
  [ "$(trim "$n")" = 1 ] || { echo "node reservations not zero"; return 1; }
  [ -n "$owner_uuid" ] || return 0
  n=$(pg_query "SELECT count(*) FROM public.sandboxes WHERE owner_id=:'owner'::uuid AND (reserved_vcpus>0 OR reserved_memory_mib>0 OR reserved_disk_gib>0)" \
    -v owner="$owner_uuid") || return 1
  [ "$(trim "$n")" = 0 ] || { echo "$n owner sandbox rows still hold reservations"; return 1; }
}

# e2e_new_database creates this run's OWN disposable database next to the base
# one (the base DSN's role must be allowed CREATE DATABASE) and points
# secrets/database-url at it: a fresh master key never meets a previous run's
# fingerprint, and the foreign/base database is never reset. The name is
# generated and validated, the identifier travels as a psql variable on a stdin
# statement, and no credential reaches argv or output. The server and the
# readiness/proof callbacks (public.nodes) then use the new database.
e2e_new_database() {
  local suffix
  suffix=$(head -c 6 /dev/urandom | od -An -tx1 | tr -d ' \n') || return 1
  e2e_db_name=bbraft_e2e_$suffix
  [[ $e2e_db_name =~ ^bbraft_e2e_[0-9a-f]{12}$ ]] || return 1
  e2e_pg_env "$work/secrets/database-url-base" || return 1
  e2e_db_created=1 # armed before CREATE: a half-created database is still dropped
  PGOPTIONS="$PGOPTIONS -c statement_timeout=120000" \
    pg_query 'CREATE DATABASE :"newdb"' -v newdb="$e2e_db_name" >/dev/null || return 1
  "$work/pgenv" -dsn-file "$work/secrets/database-url-base" -dbname "$e2e_db_name" \
    -dsn-out "$work/secrets/database-url" -out "$work/secrets" || return 1
  # The canonical DSN (and its password) are different literals than the base
  # one: add them to the redaction list.
  { cat "$work/secrets/database-url"
    if [ -n "${PGPASSWORD:-}" ]; then printf '%s\n' "$PGPASSWORD"; fi; } >>"$work/secrets/redact.list" || return 1
}

# e2e_drop_database drops ONLY this run's generated database, after every owned
# process stopped.
e2e_drop_database() {
  [[ $e2e_db_name =~ ^bbraft_e2e_[0-9a-f]{12}$ ]] || return 1
  e2e_pg_env "$work/secrets/database-url-base" || return 1
  PGOPTIONS="$PGOPTIONS -c statement_timeout=120000" \
    pg_query 'DROP DATABASE IF EXISTS :"newdb" WITH (FORCE)' -v newdb="$e2e_db_name" >/dev/null || return 1
  e2e_db_created=0
}

# e2e_write_ci_block FILE writes the documented "In scripts and CI" block as an
# EXTERNAL script (errexit stays active for its whole body). The API key
# arrives only through the environment (E2E_DOC_KEY) and is never written into
# the file or argv; the literal `login --key-stdin --json <<<` line signs in.
e2e_write_ci_block() {
  {
    echo '#!/usr/bin/env bash'
    echo 'set -euo pipefail'
    echo 'B=$1; fix=$2'
    echo 'BBRAFT_API_KEY=$E2E_DOC_KEY; unset E2E_DOC_KEY'
    echo '"$B" login --key-stdin --json <<< "$BBRAFT_API_KEY"'
    echo 'sandbox_id=$("$B" new --json | jq -r '"'"'select(.event == "ready") | .id'"'"')'
    echo 'test -n "$sandbox_id" && [[ $sandbox_id == bx_* ]]'
    echo 'trap '"'"'$B delete "$sandbox_id" --yes --json'"'"' EXIT'
    echo '"$B" push "$sandbox_id" "$fix" --to /home/user >/dev/null'
    echo '"$B" exec "$sandbox_id" --timeout 600 "npm test"'
  } >"$1"
}

# ---------------------------------------------------------------------------
# Strict teardown. Always best-effort, always after the original exit status
# was captured; it never masks the original error, runs the node proofs, the
# proxy drain + capture validation and the leak scan on every path that did not
# already finish them, reports (never hides) cleanup failures, reaps children,
# and removes only sensitive state - the node helper's retained evidence (the
# ACTUAL mktemp node_parent) stays.
# ---------------------------------------------------------------------------
e2e_teardown_wait_ops() {
  local i n=unknown
  e2e_pg_env || return 1
  for ((i = 0; i < 240; i++)); do
    n=$(pg_query "SELECT count(*) FROM public.deletion_operations WHERE owner_id=:'owner'::uuid AND status<>'completed'" \
      -v owner="$owner_uuid") || return 1
    [ "$(trim "$n")" = 0 ] && return 0
    sleep 0.5
  done
  echo "$(trim "$n") owner deletion operations still not completed after 120s"
  return 1
}

e2e_teardown() { # $1 = original rc
  local rc=$1 id pg listed gone_leaks=0
  set +e; trap - ERR
  # 1. Every own sandbox through the API (acceptance registry plus a full --all
  # sweep of this run's FRESH UNIQUE OWNER account), unless stage 14 already
  # proved them gone. A list failure after the API started is a leak report.
  if [ "$e2e_body_complete" != 1 ] && [ "$api_started" = 1 ] && [ -n "$api_url" ] && [ -x "${B:-}" ]; then
    if BBRAFT_API_KEY=$api_key e2e_run_rc "$work/logs/sweep.out" "$B" list --all --json && [ "$E2E_RC" = 0 ]; then
      for id in $(jq -r '.sandboxes[]?.id' "$work/logs/sweep.out" 2>/dev/null); do
        case " ${e2e_ids[*]:-} " in *" $id "*) ;; *) e2e_ids+=("$id");; esac
      done
    else
      echo "TEARDOWN LEAK: sweep list failed (API down?)" | e2e_redact
      gone_leaks=1
    fi
    # list --all hides OWNED soft-deleted rows, yet they may still hold a
    # reservation or a pending deletion operation: recover them from this
    # run's own database (owner-scoped) so they are waited on below, while
    # the server and node still run.
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
        # A replayed delete of an already-deleted sandbox is success if the
        # row is verifiably gone; anything else is a leak.
        if BBRAFT_API_KEY=$api_key e2e_run_rc "$work/logs/teardown-info.out" "$B" info "$id" --json && [ "$E2E_RC" = 1 ] &&
          grep -q 'not_found' "$work/logs/teardown-info.out"; then
          :
        else
          echo "TEARDOWN LEAK: sandbox $id not deleted" | e2e_redact
          gone_leaks=1
        fi
      fi
    done
    # A replayed delete of an already soft-deleted row returns before its
    # operation completes: wait every owned operation (DB, owner-scoped) before
    # the node is stopped.
    if [ -n "$owner_uuid" ] && [ -x "$work/pgenv" ] && [ -f "$work/secrets/database-url" ]; then
      listed=$(e2e_teardown_wait_ops 2>&1) || { echo "TEARDOWN LEAK: deletion operations: $listed" | e2e_redact; gone_leaks=1; }
    fi
    if [ -x "$work/pgenv" ] && [ -f "$work/secrets/database-url" ]; then
      listed=$(e2e_reservation_proof 2>&1) || { echo "TEARDOWN LEAK: reservation proof: $listed" | e2e_redact; gone_leaks=1; }
    fi
  fi
  # 2. Node half: stop and leak proof on every path (partial starts too). The
  # helpers run as standalone errexit-active children; failures are collected.
  if [ "$node_started" = 1 ] && declare -F e2e_node_stop >/dev/null 2>&1; then
    e2e_run_helper 300 e2e_node_stop
    [ "$E2E_HELPER_RC" = 0 ] || { echo "TEARDOWN LEAK: e2e_node_stop failed ($E2E_HELPER_RC)"; gone_leaks=1; }
    e2e_run_helper 180 e2e_node_leak_proof
    [ "$E2E_HELPER_RC" = 0 ] || { echo "TEARDOWN LEAK: e2e_node_leak_proof failed ($E2E_HELPER_RC)"; gone_leaks=1; }
  fi
  # 3. Children. The proxy drains first (final audit), the capture that
  # actually exists is validated BEFORE it is deleted, then the server stops.
  if [ -n "$proxy_pid" ]; then
    e2e_stop_proxy || gone_leaks=1
  fi
  if [ "$e2e_body_complete" != 1 ] && [ -f "$work/capture/index.ndjson" ] && [ -d "${helpers:-/nonexistent}/validator" ]; then
    e2e_validate final "$work/logs/validate-teardown.log" || { echo "TEARDOWN: capture validation failed"; gone_leaks=1; }
  fi
  if [ -n "$server_pid" ]; then
    e2e_stop_server || gone_leaks=1
  fi
  # Only now (every owned process stopped) drop this run's own database.
  if [ "$e2e_db_created" = 1 ] && [ -x "$work/pgenv" ] && [ -f "$work/secrets/database-url-base" ]; then
    e2e_drop_database || { echo "TEARDOWN LEAK: could not drop the owned e2e database"; gone_leaks=1; }
  fi
  for pg in "${e2e_bg[@]:-}"; do
    [ -n "$pg" ] || continue
    kill -KILL "$pg" 2>/dev/null
    wait "$pg" 2>/dev/null
  done
  e2e_bg=()
  # 4. Leak scan vs the baseline when stage 14 did not finish it.
  if [ "$e2e_body_complete" != 1 ] && [ -f "$work/baseline.links" ]; then
    e2e_leak_scan || gone_leaks=1
  fi
  [ -n "${helpers:-}" ] && rm -rf "$helpers"
  # Sensitive capture and secrets go; redacted evidence (logs, the node
  # helper's RETAINED work dir with its render/evidence) always stays.
  rm -rf "${work:-}/capture" "${work:-}/secrets" "${work:-}/cfg" "${work:-}/cfg-ci" 2>/dev/null
  if [ "$rc" != 0 ]; then
    echo "(work dir kept for post-mortem: ${work:-}; sensitive capture/secrets removed)"
  else
    # Keep logs and the helper's evidence (the ACTUAL node_parent); drop every
    # other scratch dir.
    find "${work:-}" -mindepth 1 -maxdepth 1 ! -name logs ${node_parent:+! -name "$(basename "$node_parent")"} \
      -exec rm -rf {} + 2>/dev/null
    echo "(evidence kept: ${work:-}/logs, ${node_parent:-no node parent})"
  fi
  # Outer teardown, after all owned node/network state is gone: restore and
  # verify the prior ip_forward. A failed restoration is a leak and fails an
  # otherwise-successful run; it never masks the original error.
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

e2e_cleanup() {
  local rc=$?
  trap - EXIT
  if [ "$rc" != 0 ]; then
    e2e_dump
  fi
  e2e_teardown "$rc"
  local trc=$?
  if [ "$trc" != 0 ] && [ "$rc" = 0 ]; then
    rc=1
  fi
  # E2E PASS is printed HERE, only after the full teardown and the
  # ip_forward restoration succeeded and the body completed all stages.
  if [ "$rc" = 0 ] && [ "$trc" = 0 ] && [ "${e2e_pass_pending:-0}" = 1 ]; then
    printf '\n'
    local line
    for line in "${e2e_results[@]:-}"; do printf '%s\n' "$line"; done
    echo "E2E PASS"
  elif [ "${e2e_pass_pending:-0}" = 1 ]; then
    printf '\n'
    local line
    for line in "${e2e_results[@]:-}"; do printf '%s\n' "$line"; done
    echo "E2E FAIL (teardown reported leaks)"
  fi
  exit "$rc"
}

# e2e_wait_for SECONDS DESCRIPTION CMD... polls CMD (as an if-condition) until
# it succeeds or the budget runs out.
e2e_wait_for() {
  local secs=$1 desc=$2; shift 2
  local i
  for ((i = 0; i < secs * 2; i++)); do
    if "$@" >/dev/null 2>&1; then return 0; fi
    sleep 0.5
  done
  e2e_fail "timeout after ${secs}s: $desc"
}

# api_curl METHOD PATH [JSON_BODY] — through the validating proxy; prints the
# body to $work/logs/last-api.out and the HTTP status code to stdout. Bodies
# larger than an argv can carry go through api_curl_file instead.
api_curl() {
  local method=$1 path=$2 body=${3:-} out
  out=$work/logs/last-api.out
  if [ -n "$body" ]; then
    curl -sS -X "$method" "$BBRAFT_API_URL/api/v1$path" \
      -H "Authorization: Bearer $api_key" -H 'Content-Type: application/json' \
      -d "$body" -o "$out" -w '%{http_code}'
  else
    curl -sS -X "$method" "$BBRAFT_API_URL/api/v1$path" \
      -H "Authorization: Bearer $api_key" -o "$out" -w '%{http_code}'
  fi
}

# api_curl_file METHOD PATH BODYFILE — like api_curl with the request body
# streamed from a file (no argv size limit).
api_curl_file() {
  local method=$1 path=$2 bodyfile=$3
  curl -sS -X "$method" "$BBRAFT_API_URL/api/v1$path" \
    -H "Authorization: Bearer $api_key" -H 'Content-Type: application/json' \
    --data-binary @"$bodyfile" -o "$work/logs/last-api.out" -w '%{http_code}'
}

# e2e_jq [jq options...] FILTER FILE asserts the jq expression is truthy;
# the full argv is forwarded so --arg/--argjson work.
e2e_jq() { jq -e "$@" >/dev/null; }

# e2e_wait_state ID 'ready|running|idle' polls info until the state matches.
e2e_wait_state() {
  local id=$1 pat=$2 i
  for ((i = 0; i < 360; i++)); do
    if "$B" info "$id" --json >"$work/logs/wait-state.out" 2>/dev/null; then
      if jq -e --arg p "^($pat)\$" '.sandbox.state | test($p)' \
        "$work/logs/wait-state.out" >/dev/null 2>&1; then
        return 0
      fi
    fi
    sleep 0.5
  done
  e2e_fail "timeout: sandbox $id not in state ($pat)"
}

if [ "${E2E_HELPERS_ONLY:-}" = 1 ]; then return 0 2>/dev/null || exit 0; fi
if [ "${E2E_SELFTEST:-}" = 1 ]; then e2e_selftest; exit 0; fi

# ===========================================================================
# Main run.
# ===========================================================================
repo=$PWD
[ -f "$repo/go.mod" ] || { echo "run e2e-p1.sh from the repository root" >&2; exit 2; }
B=$repo/bin/bbraft
# Parents exist before mktemp; the EXIT trap is armed between the two
# allocations so a failure in the second cannot leak the first.
mkdir -p "$HOME/.bbk"
work=$(mktemp -d "$HOME/.bbk/e2e-p1.XXXXXXXX") # short: QMP socket paths < 107 bytes
trap e2e_cleanup EXIT
mkdir -p "$repo/.cache"
helpers=$(mktemp -d "$repo/.cache/e2e-p1.XXXXXXXX")
mkdir -p "$work"/{secrets,logs,capture,cfg,fixtures,scp-up/d,scp-down}
chmod 700 "$work" "$work/secrets" "$work/capture"
E2E_ERRLOG=$work/logs/err.log
export E2E_ERRLOG
: >"$E2E_ERRLOG"
export XDG_CONFIG_HOME=$work/cfg

# --- stage 0: node-helper preflight (before any expensive work) -------------
e2e_stage "preflight"
node_helper=$repo/deploy/dev/e2e-p1-node.sh
[ -f "$node_helper" ] || e2e_fail "node helper $node_helper missing (Lead A merge pending)"
# shellcheck source=/dev/null
source "$node_helper"
for fn in e2e_node_start e2e_node_wait_registered e2e_node_wait_ready e2e_client e2e_node_stop e2e_node_leak_proof; do
  declare -F "$fn" >/dev/null || e2e_fail "node helper lacks $fn"
done
# External programs the CLI shells out to are harness prerequisites, not
# product behavior: provision openssh-client in this disposable VM if absent,
# then require ssh, scp, script and timeout to be present (clear failure).
if ! command -v ssh >/dev/null 2>&1 || ! command -v scp >/dev/null 2>&1; then
  echo "installing openssh-client (disposable VM prerequisite)"
  if ! { e2e_priv env DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends openssh-client ||
    { e2e_priv apt-get update && e2e_priv env DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends openssh-client; }; } \
    >"$work/logs/openssh-install.log" 2>&1; then
    tail -10 "$work/logs/openssh-install.log" | e2e_redact | cut -c1-300
    e2e_fail "could not install openssh-client (see logs/openssh-install.log)"
  fi
fi
for tool in ssh scp script timeout; do
  command -v "$tool" >/dev/null 2>&1 || e2e_fail "required external program '$tool' is not installed"
done
# At source time ONLY the planned fixed topology is exported (no side
# effects); runtime exports are gated after e2e_node_wait_ready below.
for ev in E2E_NODE_HOST_VETH_ADDR E2E_NODE_SSH_INGRESS_ADDR E2E_NODE_SSH_INGRESS_IFACE \
  E2E_NODE_CLIENT_ADDR E2E_NODE_CLIENT_NETNS; do
  [ -n "${!ev:-}" ] || e2e_fail "node helper did not export planned-topology $ev at source time"
done
node_helper_sourced=1
veth=${E2E_NODE_HOST_VETH_ADDR:?}
export BBRAFT_API_URL="http://$veth:$E2E_PROXY_PORT"
api_url=$BBRAFT_API_URL
e2e_pass

# --- stage 1: build --------------------------------------------------------
e2e_stage "build"
make build >"$work/logs/build.log" 2>&1 || e2e_fail "make build (see logs/build.log)"
for b in bbraft bbraft-server bbraft-node; do
  [ -x "$repo/bin/$b" ] || e2e_fail "bin/$b missing"
done
# Clean source copy, taken before the image build fills images/.cache and
# images/out (p05 pattern): nothing generated later is ever pushed.
src=$work/bamboo-raft
mkdir -p "$src"
tar -C "$repo" --exclude=./.cache --exclude=./images/.cache --exclude=./images/out \
  --exclude=./bin -cf - . | tar -C "$src" -xf -
e2e_pass

# --- stage 2: image (pinned version from images/pins.sh) --------------------
e2e_stage "image"
t0=$(date +%s)
# shellcheck source=/dev/null
source "$repo/images/pins.sh"
[ -n "${IMAGE_VERSION:-}" ] || e2e_fail "images/pins.sh did not set IMAGE_VERSION"
img_dir=$repo/images/out/$IMAGE_VERSION
if [ "${E2E_REUSE_IMAGE:-}" = 1 ]; then
  # DEBUG ONLY (kept-VM rerun): images/build.sh always rebuilds and publish.sh
  # is no-clobber, so reuse the pinned image built earlier in THIS VM, but only
  # after the exact pinned directory passes images/verify.sh. Never the default.
  [ -d "$img_dir" ] || e2e_fail "E2E_REUSE_IMAGE=1 but images/out/$IMAGE_VERSION does not exist"
  bash "$repo/images/verify.sh" "$img_dir" >"$work/logs/image-verify.log" 2>&1 \
    || e2e_fail "E2E_REUSE_IMAGE=1: pinned image failed verify.sh (see logs/image-verify.log)"
  echo "DEBUG: reusing verified pinned image $IMAGE_VERSION (E2E_REUSE_IMAGE=1, not rebuilt)"
else
  make -C images image >"$work/logs/image-build.log" 2>&1 || e2e_fail "image build (see logs/image-build.log)"
  [ -d "$img_dir" ] || e2e_fail "images/out/$IMAGE_VERSION missing"
fi
ls "$img_dir" >"$work/logs/image-manifest.out"
echo "image $IMAGE_VERSION ready in $(( $(date +%s) - t0 ))s"
e2e_pass

# --- stage 3: helpers + process baseline -----------------------------------
e2e_stage "helpers+baseline"
e2e_write_go_helpers "$helpers"
[ -z "$(gofmt -l "$helpers")" ] || e2e_fail "generated helpers not gofmt-clean"
go build -o "$work/e2e-p1-proxy" "./${helpers#"$repo"/}/proxy" || e2e_fail "proxy build"
go build -o "$work/pgenv" "./${helpers#"$repo"/}/pgenv" || e2e_fail "pgenv build"
go test -run TestNothing "./${helpers#"$repo"/}/validator" -count=1 >/dev/null || e2e_fail "validator compile"
# Baselines must COLLECT (an empty baseline is valid and still compared
# later); a collection failure is a privileges failure, not a skip.
pgrep -af "$E2E_PROC_RE" >"$work/baseline.procs.raw" || true
sort "$work/baseline.procs.raw" >"$work/baseline.procs"
ip -o link >"$work/baseline.links.raw" || e2e_fail "baseline ip link collection"
sort "$work/baseline.links.raw" >"$work/baseline.links"
e2e_priv nft list tables >"$work/baseline.nft.raw" || e2e_fail "baseline nft collection"
sort "$work/baseline.nft.raw" >"$work/baseline.nft"
e2e_unit_names >"$work/baseline.units" || e2e_fail "baseline unit collection"
e2e_pass

# --- stage 4: secrets, server, proxy, node ----------------------------------
e2e_stage "server+proxy+node"
umask 077
head -c 32 /dev/urandom >"$work/secrets/master.key"
head -c 32 /dev/urandom | od -An -tx1 | tr -d ' \n' >"$work/secrets/gateway.token"
node_token=$(head -c 32 /dev/urandom | od -An -tx1 | tr -d ' \n')
printf %s "$node_token" >"$work/secrets/node.token"
node_sha=$(printf %s "$node_token" | sha256sum | cut -d' ' -f1)
printf '{"nodes":[{"id":"e2e-node","tokenSha256":"%s"}]}' "$node_sha" >"$work/secrets/nodes.json"
api_key="bmb_$(head -c 24 /dev/urandom | od -An -tx1 | tr -d ' \n')"
owner="e2e-$(date +%s)-$RANDOM" # fresh unique account owner for this run
printf '{"keys":[{"id":"main","owner":"%s","key":"%s"}]}' "$owner" "$api_key" >"$work/secrets/keys.json"
[ -n "${BBRAFT_TEST_DATABASE_URL:-}" ] || e2e_fail "BBRAFT_TEST_DATABASE_URL not set"
printf %s "$BBRAFT_TEST_DATABASE_URL" >"$work/secrets/database-url-base"
# The redact list carries only text secrets; the raw master key never enters
# a command substitution - its hex form stands in for it.
master_hex=$(od -An -tx1 <"$work/secrets/master.key" | tr -d ' \n')
{ printf '%s\n' "$api_key" "$node_token" "$(cat "$work/secrets/gateway.token")" \
    "$BBRAFT_TEST_DATABASE_URL" "$master_hex"; } >"$work/secrets/redact.list"
chmod 600 "$work/secrets/redact.list"
export E2E_REDACT_FILE=$work/secrets/redact.list
unset master_hex
# Fresh OWNED database for this invocation; secrets/database-url now targets it.
e2e_new_database || e2e_fail "owned database creation"

setsid "$repo/bin/bbraft-server" -roles all \
  -server.listen 127.0.0.1:7710 -node.listen 127.0.0.1:7701 \
  -server.public-url "$api_url" \
  -database.url-file "$work/secrets/database-url" \
  -secrets.master-key-file "$work/secrets/master.key" \
  -node.gateway-token-file "$work/secrets/gateway.token" \
  -auth.keys-file "$work/secrets/keys.json" \
  -node.tokens-file "$work/secrets/nodes.json" \
  >"$work/logs/server.log" 2>&1 &
server_pid=$!
# 0.0.0.0 so the API answers on the planned host-veth address the moment the
# topology exists; e2e_node_start probes it after creating the topology.
setsid "$work/e2e-p1-proxy" -listen "0.0.0.0:$E2E_PROXY_PORT" -upstream 127.0.0.1:7710 \
  -capture "$work/capture" >"$work/logs/proxy.log" 2>&1 &
proxy_pid=$!
e2e_wait_for 60 "server answers /me (direct)" \
  curl -fsS -o /dev/null "http://127.0.0.1:7710/api/v1/me" -H "Authorization: Bearer $api_key"
e2e_wait_for 60 "proxy answers /me on loopback" \
  curl -fsS -o /dev/null "http://127.0.0.1:$E2E_PROXY_PORT/api/v1/me" -H "Authorization: Bearer $api_key"
# API readiness does not imply the node-hub listener is up: wait for the
# node-link port the helper TCP-probes right away in e2e_node_start.
e2e_wait_for 60 "node-link listener answers on 127.0.0.1:7701" \
  bash -c 'exec 3<>/dev/tcp/127.0.0.1/7701'

# Orchestrator-approved, disposable e2e VM only: remember the prior
# net.ipv4.ip_forward, enable forwarding for the node topology, and arm the
# restoration immediately (the EXIT trap runs on every path). The node helper
# keeps its own fail-closed check and never enables this itself. Nothing here
# runs under E2E_SELFTEST, which exits before main init.
ip_forward_prev=$(e2e_ipf_read) || e2e_fail "read net.ipv4.ip_forward"
ip_forward_set=1 # armed BEFORE the write: a write that changed state then failed is still restored
e2e_ipf_write 1 || e2e_fail "enable net.ipv4.ip_forward"
echo "ip_forward: prior=$ip_forward_prev enabled=1 (restored and verified in teardown)"

# FRESH_WORK_DIR must not exist yet; its parent does (A contract). Cleanup is
# armed (node_started) BEFORE the call so a partially failed start is cleaned;
# helper functions are called plainly so errexit stays active inside them.
node_parent=$(mktemp -d "$work/node-parent.XXXXXXXX")
# Explicit disposable-VM mutation gate required by the node helper (sudo -n,
# systemd, network and root-state changes). Main path only, never selftest.
export BBRAFT_TEST_VM_SETUP=1
node_started=1
e2e_node_start "$repo/bin/bbraft-node" "$img_dir" "e2e-node" \
  "127.0.0.1:7701" "$work/secrets/node.token" "$node_parent/node"
e2e_node_wait_ready 300
# Runtime exports appear after wait_ready; a missing one fails here.
for ev in E2E_NODE_ID E2E_NODE_IMAGE_ID E2E_NODE_EXPECTED_VCPUS \
  E2E_NODE_EXPECTED_MEMORY_MIB E2E_NODE_EXPECTED_DISK_GIB \
  E2E_NODE_ADMITTED_QEMU_VERSION E2E_NODE_START_EPOCH E2E_NODE_READY_EPOCH; do
  [ -n "${!ev:-}" ] || e2e_fail "node helper did not export runtime $ev after wait_ready"
done
e2e_node_wait_registered e2e_node_ready_cb
# Only now can a sandbox exist: before node registration no sandbox was
# allocated, so setup-phase cleanup skips the API sweep (its public-veth URL
# may not even be reachable yet) and the nodes-row reservation proof.
api_started=1
printf %s "$api_key" | e2e_ok "$B" login --key-stdin --json
# owner uuid for the owner-scoped reservation proof later.
code=$(api_curl GET /me)
[ "$code" = 200 ] || e2e_fail "GET /me: HTTP $code"
owner_uuid=$(jq -r '.user.id // empty' "$work/logs/last-api.out")
[ -n "$owner_uuid" ] || e2e_fail "no .user.id from /me"
e2e_run_rc "$work/logs/list-sanity.out" "$B" list --json
[ "$E2E_RC" = 0 ] || e2e_fail "list sanity through proxy"
e2e_pass

# --- stage 5: quickstart + In scripts and CI -------------------------------
e2e_stage "quickstart+scripts-ci"
# npm fixture: dependency-free, zero network (the image ships node/npm).
mkdir -p "$work/fixtures/npmf"
printf '{"name":"e2e-npm-fixture","private":true,"scripts":{"test":"node test.js"}}\n' >"$work/fixtures/npmf/package.json"
printf 'console.log("npm-test-ok");\n' >"$work/fixtures/npmf/test.js"
# files fixture for stage 8, scp fixture for stage 9.
mkdir -p "$work/fixtures/sub" "$work/scp-up/d"
printf 'file-a-content\n' >"$work/fixtures/a.txt"
printf 'file-b-content\n' >"$work/fixtures/sub/b.txt"
printf 'scp-f1\n' >"$work/scp-up/f1.txt"
printf 'scp-f2\n' >"$work/scp-up/d/f2.txt"

# Quickstart block (binary name and URL swapped; the id is this run's id).
# A bare `new` on a pipe emits the documented JSON events.
e2e_run_rc "$work/logs/qs-new.out" "$B" new
[ "$E2E_RC" = 0 ] || e2e_fail "bare new: $(tail -3 "$work/logs/qs-new.out" | e2e_redact)"
qid=$(jq -r 'select(.event == "ready") | .id' "$work/logs/qs-new.out" | tail -1)
[ -n "$qid" ] && [[ $qid == bx_* ]] || e2e_fail "no bx_ id from bare new"
e2e_ids+=("$qid")
e2e_ok "$B" exec "$qid" "echo hello"
e2e_have hello "$work/logs/last.out"
e2e_ok "$B" stop "$qid" --json
e2e_wait_state "$qid" 'archived'
e2e_ok "$B" resume "$qid" --json
e2e_wait_state "$qid" 'ready|running|idle'
# Interactive quickstart ssh through a real PTY inside the external-client
# netns, driven to exit by stdin. The marker must be EXECUTED output (the
# expanded $USER), never the terminal echo of the typed command.
printf 'echo SHELL-REACHED-$USER\nexit\n' | e2e_expect_rc 0 "$work/logs/qs-ssh-pty.out" \
  e2e_pty_cli 30 ssh "$qid"
grep -Eq 'SHELL-REACHED-[A-Za-z0-9_.-]+' "$work/logs/qs-ssh-pty.out" \
  || e2e_fail "quickstart ssh: no executed SHELL-REACHED marker"
# The default-type quickstart sandbox is deleted (durable) BEFORE the next
# default allocation; the outer VM fits one default at a time.
e2e_ok "$B" delete "$qid" --yes --wait

# Quickstart curl block, unchanged apart from the URL.
code=$(api_curl POST /sandboxes '{"ttlSeconds":3600}')
[ "$code" = 200 ] || [ "$code" = 202 ] || e2e_fail "curl create: HTTP $code"
cid=$(jq -r '.sandbox.id // empty' "$work/logs/last-api.out")
[ -n "$cid" ] || e2e_fail "curl create: no sandbox.id"
e2e_ids+=("$cid")
e2e_wait_state "$cid" 'ready|running|idle'
code=$(api_curl POST "/sandboxes/$cid/commands" '{"command":"echo hello"}')
[ "$code" = 200 ] || e2e_fail "curl command: HTTP $code"
e2e_have hello "$work/logs/last-api.out"
code=$(curl -sS -X DELETE "$BBRAFT_API_URL/api/v1/sandboxes/$cid" \
  -H "Authorization: Bearer $api_key" -H "X-Ascii-Confirm-Delete: $cid" \
  -o "$work/logs/last-api.out" -w '%{http_code}')
[ "$code" = 202 ] || e2e_fail "curl delete: HTTP $code"
dop=$(jq -r '.operation.id // empty' "$work/logs/last-api.out")
[ -n "$dop" ] || e2e_fail "curl delete: no operation id"
# Durable wait: the deletion operation must complete before the next default.
e2e_wait_for 120 "deletion operation completed" bash -c \
  "curl -fsS \"$api_url/api/v1/deletion-operations/$dop\" -H 'Authorization: Bearer $api_key' | jq -e '.operation.status == \"completed\"' >/dev/null"

# "In scripts and CI" block, verbatim apart from binary name, executed as an
# EXTERNAL script so errexit stays active for its whole body (a subshell on
# the left of || would suppress it). Its literal login line signs in from a
# FRESH config dir with the key arriving only through the environment, so the
# login is load-bearing for every later command of the block.
scripts_ci=$work/scripts-ci.sh
e2e_write_ci_block "$scripts_ci"
E2E_DOC_KEY=$api_key XDG_CONFIG_HOME=$work/cfg-ci \
  e2e_expect_rc 0 "$work/logs/scripts-ci.out" bash "$scripts_ci" "$B" "$work/fixtures/npmf"
if grep -qF "$api_key" "$work/logs/scripts-ci.out"; then e2e_fail "CI block output echoes the API key"; fi
e2e_have npm-test-ok "$work/logs/scripts-ci.out"
# The block's literal EXIT trap (`delete --yes --json`) only ACCEPTS the
# deletion (202, async): the row is soft-deleted at once, but its disk
# reservation is held until the deletion operation completes. A list is not
# durable (soft-deleted rows vanish from it early), so take the id and the
# operation from the envelope the trap printed, register the id for teardown,
# and wait for that exact operation before the next allocation.
ci_id=$(jq -rR 'fromjson? | objects | .operation.targetId? // empty' "$work/logs/scripts-ci.out" | tail -1)
ci_op=$(jq -rR 'fromjson? | objects | .operation.id? // empty' "$work/logs/scripts-ci.out" | tail -1)
[ -n "$ci_id" ] && [[ $ci_id == bx_* ]] || e2e_fail "CI block: no deletion envelope (operation.targetId) in its output"
[ -n "$ci_op" ] || e2e_fail "CI block: deletion envelope has no operation.id"
e2e_ids+=("$ci_id")
# Polling callback: returns failures explicitly, which e2e_wait_for retries.
e2e_ci_op_completed() {
  curl -fsS "$api_url/api/v1/deletion-operations/$ci_op" -H "Authorization: Bearer $api_key" |
    jq -e '.operation.status == "completed"' >/dev/null
}
e2e_wait_for 120 "scripts-and-CI deletion operation $ci_op completed (durable)" e2e_ci_op_completed
# JSON Lines error line shape on a refused request.
e2e_expect_rc 1 "$work/logs/err-line.out" "$B" info bx_e2edoesnotexist1 --json
tail -1 "$work/logs/err-line.out" >"$work/logs/err-line-last.json"
e2e_jq '.event == "error" and (.code != null) and (.status != null)' "$work/logs/err-line-last.json"
e2e_pass

# --- stage 6: CRUD, filters, rename, extend --------------------------------
e2e_stage "crud-rename-extend"
e2e_run_rc "$work/logs/crud-new.out" "$B" new --type small --name e2e-crud --ttl 3600 --json
[ "$E2E_RC" = 0 ] || e2e_fail "new: $(tail -3 "$work/logs/crud-new.out" | e2e_redact)"
c1=$(jq -r 'select(.event == "ready") | .id' "$work/logs/crud-new.out" | tail -1)
[ -n "$c1" ] && [[ $c1 == bx_* ]] || e2e_fail "no id from new"
e2e_ids+=("$c1")
e2e_wait_state "$c1" 'ready|running|idle'
# Capacity: the node offers ~94 GiB and every small/default sandbox reserves
# 51 GiB of disk (a stopped one keeps it), so a second concurrent sandbox can
# never be admitted. Everything here is sequential on ONE live sandbox; no
# multi-page merge is claimed.
e2e_ok "$B" info "$c1" --json
e2e_jq '.sandbox.name == "e2e-crud" and .sandbox.ttlSeconds == 3600' "$work/logs/last.out"
# list --limit 1 over one sandbox: one page holding it, and the final
# pageInfo.nextCursor of a full walk is null (internal/cli list.go).
e2e_ok "$B" list --limit 1 --json
e2e_jq --arg a "$c1" \
  '(.sandboxes | length) == 1 and .sandboxes[0].id == $a and .pageInfo.nextCursor == null' \
  "$work/logs/last.out"
e2e_ok "$B" list --filter r --json
e2e_jq --arg id "$c1" 'any(.sandboxes[]?; .id == $id)' "$work/logs/last.out"
e2e_ok "$B" list --filter s --json
e2e_jq --arg id "$c1" 'all(.sandboxes[]?; .id != $id)' "$work/logs/last.out"
# Wire filters use the ACTUAL current state of the row.
e2e_ok "$B" info "$c1" --json
c1_state=$(jq -r '.sandbox.state' "$work/logs/last.out")
code=$(api_curl GET "/sandboxes?state=$c1_state&name=e2e-crud&namePrefix=e2e-")
[ "$code" = 200 ] || e2e_fail "wire filter: HTTP $code"
e2e_jq --arg id "$c1" 'any(.sandboxes[]?; .id == $id)' "$work/logs/last-api.out"
code=$(api_curl GET "/sandboxes?state=archived&name=e2e-crud")
[ "$code" = 200 ] || e2e_fail "wire filter archived: HTTP $code"
e2e_jq --arg id "$c1" 'all(.sandboxes[]?; .id != $id)' "$work/logs/last-api.out"
# rename round trip, then the CLI's explicit refusal of an over-long name:
# usage exit 2, NO PATCH on the wire, name unchanged.
e2e_ok "$B" rename "$c1" e2e-crud2
e2e_ok "$B" info "$c1" --json
e2e_jq '.sandbox.name == "e2e-crud2"' "$work/logs/last.out"
patch_seq0=$(jq -s 'map(select(.method == "PATCH")) | length' "$work/capture/index.ndjson")
e2e_expect_rc 2 "$work/logs/rename-long.out" "$B" rename "$c1" "$(printf 'x%.0s' $(seq 121))"
patch_seq1=$(jq -s 'map(select(.method == "PATCH")) | length' "$work/capture/index.ndjson")
[ "$patch_seq0" = "$patch_seq1" ] || e2e_fail "121-char rename issued a PATCH"
e2e_ok "$B" info "$c1" --json
e2e_jq '.sandbox.name == "e2e-crud2"' "$work/logs/last.out"
# extend: hours, then seconds, then disable auto-stop (ttlSeconds null).
e2e_ok "$B" extend "$c1" --hours 2
e2e_ok "$B" info "$c1" --json
e2e_jq '.sandbox.ttlSeconds == 7200' "$work/logs/last.out"
e2e_ok "$B" extend "$c1" --ttl 1800
e2e_ok "$B" info "$c1" --json
e2e_jq '.sandbox.ttlSeconds == 1800' "$work/logs/last.out"
e2e_ok "$B" extend "$c1" --no-auto-stop
e2e_ok "$B" info "$c1" --json
e2e_jq '.sandbox.ttlSeconds == null' "$work/logs/last.out"
e2e_pass

# --- stage 7: exec suite (sync, stream, timeout, detach, SIGINT, reconnect) -
e2e_stage "exec-suite"
# a. synchronous: remote exit code and both streams.
e2e_expect_rc 7 "$work/logs/exec-sync.out" "$B" exec "$c1" --timeout 60 -- \
  bash -lc 'echo o1; echo e1 >&2; exit 7'
e2e_have o1 "$work/logs/exec-sync.out"
e2e_have e1 "$work/logs/exec-sync.out"
# b. the agent-harness streamed pipeline (legacy JSON shapes, remote exit 3).
e2e_run_rc "$work/logs/exec-stream.out" bash -o pipefail -c \
  "\"$B\" exec \"$c1\" --timeout 0 --stream --json -- bash -lc 'echo hello-from-guest; exit 3' | jq -rj --unbuffered 'if .type == \"stdout\" then .data elif .type == \"exit\" then \"\\nEXIT:\\(.exitCode // 1)\\n\" else empty end'"
[ "$E2E_RC" = 3 ] || e2e_fail "stream pipeline exit $E2E_RC, want 3: $(tail -3 "$work/logs/exec-stream.out" | e2e_redact)"
e2e_have hello-from-guest "$work/logs/exec-stream.out"
e2e_have 'EXIT:3' "$work/logs/exec-stream.out"
# c. command timeout: the CLI exits 124.
e2e_expect_rc 124 "$work/logs/exec-timeout.out" "$B" exec "$c1" --timeout 5 -- bash -lc 'sleep 60'
# d. detached lifecycle: pid, status, SIGINT-interrupted attach (CLI exit 130,
#    process keeps running), idempotent kill. The detached command prints a
#    marker BEFORE sleeping so the attach actually observes decoded output.
e2e_ok "$B" exec "$c1" --detach --json -- bash -lc 'echo attach-marker; sleep 600'
e2e_jq '.processId > 0' "$work/logs/last.out"
apid=$(jq -r '.processId' "$work/logs/last.out")
sleep 2
e2e_ok "$B" exec "$c1" --status "$apid" --json
e2e_jq '.status == "running"' "$work/logs/last.out"
# Arm the exact cancel target (full path), then SIGINT the attach CLI after a
# decoded stdout frame. The child is registered and unconditionally reaped.
printf '/api/v1/sandboxes/%s/commands/%s/stream' "$c1" "$apid" >"$work/capture/arm-cancel"
"$B" exec "$c1" --attach "$apid" >"$work/logs/attach.out" 2>&1 &
attach_cli=$!
e2e_bg+=("$attach_cli")
cancel_seen=0
for _ in $(seq 60); do
  if grep -q 'attach-marker' "$work/logs/attach.out" 2>/dev/null; then cancel_seen=1; break; fi
  sleep 0.5
done
if [ "$cancel_seen" != 1 ]; then
  e2e_fail "no decoded attach-marker observed"
fi
kill -INT "$attach_cli"
attach_rc=0
for _ in $(seq 30); do
  if ! kill -0 "$attach_cli" 2>/dev/null; then break; fi
  sleep 0.5
done
if kill -0 "$attach_cli" 2>/dev/null; then
  kill -KILL "$attach_cli" 2>/dev/null
  e2e_fail "attach CLI ignored SIGINT (no exit 130)"
fi
if wait "$attach_cli"; then attach_rc=0; else attach_rc=$?; fi
e2e_bg=() # reaped: never signal a possibly reused pid later
[ "$attach_rc" = 130 ] || e2e_fail "attach after SIGINT exited $attach_rc, want 130"
e2e_ok "$B" exec "$c1" --status "$apid" --json
e2e_jq '.status == "running"' "$work/logs/last.out"
e2e_ok "$B" exec "$c1" --kill "$apid" --json
e2e_ok "$B" exec "$c1" --kill "$apid" --json
e2e_jq '.signaled == false' "$work/logs/last.out"
# e. reconnect: one deliberate proxy cut mid-stream; the CLI must resume at
#    the exact committed offsets with byte-exact output and ONE start POST
#    inside this sub-stage's capture window. The deterministic payload is
#    > 2 MiB (250k lines) so the cut after 2 stdout frames provably lands
#    BEFORE the final output under any legal node frame batching, and the
#    command pauses mid-output to guarantee output remains pending.
e2e_barrier || e2e_fail "capture barrier before the reconnect window"
seq_win0=$E2E_BARRIER_SEQ
printf '%s:2' "$c1" >"$work/capture/arm-cut"
for i in $(seq 250000); do printf 'r-%06d\n' "$i"; done >"$work/logs/reconnect.expected"
total_bytes=$(stat -c%s "$work/logs/reconnect.expected")
e2e_run_rc "$work/logs/reconnect.json" "$B" exec "$c1" --timeout 0 --stream --json -- \
  bash -lc 'for i in $(seq 1 125000); do printf "r-%06d\n" $i; done; sleep 3; for i in $(seq 125001 250000); do printf "r-%06d\n" $i; done'
[ "$E2E_RC" = 0 ] || e2e_fail "reconnect exec exit $E2E_RC: $(tail -3 "$work/logs/reconnect.json" | e2e_redact)"
jq -rj 'select(.type == "stdout") | .data' "$work/logs/reconnect.json" >"$work/logs/reconnect.got"
sha_exp=$(sha256sum "$work/logs/reconnect.expected" | cut -d' ' -f1)
sha_got=$(sha256sum "$work/logs/reconnect.got" | cut -d' ' -f1)
[ "$sha_exp" = "$sha_got" ] || e2e_fail "reconnect output not byte-exact (sha mismatch)"
jq -e 'select(.type == "exit" and .exitCode == 0)' "$work/logs/reconnect.json" >/dev/null \
  || e2e_fail "reconnect exec: no exit frame"
e2e_barrier || e2e_fail "capture barrier after the reconnect window"
win_posts=$(jq -s --argjson from "$seq_win0" --arg p "/api/v1/sandboxes/$c1/commands" \
  'map(select(.seq > $from and .method == "POST" and (.path == $p))) | length' "$work/capture/index.ndjson")
[ "$win_posts" = 1 ] || e2e_fail "reconnect window saw $win_posts command POSTs, want exactly 1"
cutrec=$(e2e_cut_record "$work/capture/index.ndjson" "$seq_win0") || e2e_fail "not exactly one cut record in the reconnect window"
cut_seq=$(jq -r '.seq' <<<"$cutrec")
cut_path=$(jq -r '.path | split("?")[0]' <<<"$cutrec")
cut_off=$(jq -r '.lastNextOffset' <<<"$cutrec")
cut_err=$(jq -r '.lastStderrOffset' <<<"$cutrec")
grep -qE "^/api/v1/sandboxes/$c1/commands/[0-9]+/stream\$" <<<"$cut_path" || e2e_fail "the cut is not on this sandbox's attach stream"
[ "$cut_off" -gt 0 ] || e2e_fail "cut record has no committed offset"
# 0 < cursor < total: the cut is provably mid-stream, and its prefix holds
# no terminal frame (the validator enforces that on the spool too).
[ "$cut_off" -lt "$total_bytes" ] || e2e_fail "cut cursor $cut_off not before total $total_bytes"
if jq -e 'select(.terminalFrame == true)' <<<"$cutrec" >/dev/null; then e2e_fail "cut prefix contains a terminal frame"; fi
# The FIRST GET of the identical stream path after the cut resumed at exactly
# the committed offsets.
e2e_resume_ok "$work/capture/index.ndjson" "$cut_seq" "$cut_path" "$cut_off" "$cut_err" \
  || e2e_fail "reconnect after the cut did not resume at stdoutOffset=$cut_off stderrOffset=$cut_err"
e2e_pass

# --- stage 8: files ---------------------------------------------------------
e2e_stage "files"
e2e_ok "$B" push "$c1" "$work/fixtures" --to /home/user/fix
code=$(api_curl GET "/sandboxes/$c1/files/list?path=/home/user/fix")
[ "$code" = 200 ] || e2e_fail "listFiles: HTTP $code"
e2e_jq 'any(.files[]?; .name == "a.txt") and any(.files[]?; .name == "sub" and .isDir)' "$work/logs/last-api.out"
e2e_ok "$B" exec "$c1" -- cat /home/user/fix/a.txt
e2e_have file-a-content "$work/logs/last.out"
# utf8 round trip.
code=$(api_curl PUT "/sandboxes/$c1/files" '{"path":"e2e/w.txt","content":"write-round-trip"}')
[ "$code" = 200 ] || e2e_fail "writeFile: HTTP $code"
e2e_jq '.type == "file.written"' "$work/logs/last-api.out"
code=$(api_curl GET "/sandboxes/$c1/files?path=e2e/w.txt")
[ "$code" = 200 ] || e2e_fail "readFile: HTTP $code"
e2e_jq '.content == "write-round-trip" and .type == "file.read"' "$work/logs/last-api.out"
# base64 round trip on 16 random bytes.
rand16=$(head -c 16 /dev/urandom | base64 -w0)
code=$(api_curl PUT "/sandboxes/$c1/files" "{\"path\":\"e2e/bin.dat\",\"content\":\"$rand16\",\"encoding\":\"base64\"}")
[ "$code" = 200 ] || e2e_fail "writeFile base64: HTTP $code"
code=$(api_curl GET "/sandboxes/$c1/files?path=e2e/bin.dat&encoding=base64")
[ "$code" = 200 ] || e2e_fail "readFile base64: HTTP $code"
e2e_jq --arg c "$rand16" '.content == $c and .encoding == "base64"' "$work/logs/last-api.out"
# 5 MiB limits: exactly 5 MiB succeeds (asserted), above fails. Bodies are
# streamed from files (an argv this size cannot be passed).
{ printf '{"path":"e2e/exact.bin","encoding":"base64","content":"'
  head -c $((5 * 1024 * 1024)) /dev/zero | tr '\0' 'e' | base64 -w0
  printf '"}'; } >"$work/fixtures/exact.json"
code=$(api_curl_file PUT "/sandboxes/$c1/files" "$work/fixtures/exact.json")
[ "$code" = 200 ] || e2e_fail "writeFile exact 5MiB: HTTP $code, want 200"
e2e_jq '.type == "file.written" and .size == 5242880' "$work/logs/last-api.out"
{ printf '{"path":"e2e/too-big.txt","encoding":"base64","content":"'
  head -c $((5 * 1024 * 1024 + 1)) /dev/zero | tr '\0' 'z' | base64 -w0
  printf '"}'; } >"$work/fixtures/too-big.json"
code=$(api_curl_file PUT "/sandboxes/$c1/files" "$work/fixtures/too-big.json")
[ "$code" = 413 ] || e2e_fail "writeFile 5MiB+1: HTTP $code, want 413"
e2e_jq '.code == "file_too_large" and .ok == false' "$work/logs/last-api.out"
e2e_ok "$B" exec "$c1" -- bash -lc 'head -c 6291456 /dev/zero > /home/user/big.bin'
code=$(api_curl GET "/sandboxes/$c1/files?path=/home/user/big.bin&encoding=base64")
[ "$code" = 413 ] || e2e_fail "readFile 6MiB: HTTP $code, want 413"
e2e_jq '.code == "file_too_large"' "$work/logs/last-api.out"
# folder, info, move, permissions, delete; roots refused.
code=$(api_curl POST "/sandboxes/$c1/files/folder" '{"path":"e2e/dir","mode":"0755"}')
[ "$code" = 200 ] || e2e_fail "createFolder: HTTP $code"
code=$(api_curl GET "/sandboxes/$c1/files/info?path=e2e/dir")
[ "$code" = 200 ] || e2e_fail "getFileInfo dir: HTTP $code"
e2e_jq '.isDir == true and .mode == "0755"' "$work/logs/last-api.out"
code=$(api_curl POST "/sandboxes/$c1/files/move" '{"source":"e2e/w.txt","destination":"e2e/dir/w2.txt"}')
[ "$code" = 200 ] || e2e_fail "moveFile: HTTP $code"
e2e_jq '.type == "file.moved"' "$work/logs/last-api.out"
code=$(api_curl GET "/sandboxes/$c1/files?path=e2e/w.txt")
[ "$code" = 404 ] || e2e_fail "readFile old path: HTTP $code, want 404"
e2e_jq '.code == "not_found"' "$work/logs/last-api.out"
code=$(api_curl POST "/sandboxes/$c1/files/permissions" '{"path":"e2e/dir/w2.txt","mode":"0600"}')
[ "$code" = 200 ] || e2e_fail "setFilePermissions: HTTP $code"
code=$(api_curl GET "/sandboxes/$c1/files/info?path=e2e/dir/w2.txt")
[ "$code" = 200 ] || e2e_fail "getFileInfo w2: HTTP $code"
e2e_jq '.mode == "0600"' "$work/logs/last-api.out"
code=$(api_curl DELETE "/sandboxes/$c1/files?path=e2e/dir&recursive=true")
[ "$code" = 200 ] || e2e_fail "deleteFile recursive: HTTP $code"
code=$(api_curl DELETE "/sandboxes/$c1/files?path=/home/user")
[ "$code" = 400 ] || e2e_fail "deleteFile root: HTTP $code, want 400"
# signed download URL fetched with curl, no bearer header; bytes must match.
code=$(api_curl POST "/sandboxes/$c1/files/download-url" '{"path":"fix/a.txt"}')
[ "$code" = 200 ] || e2e_fail "createDownloadUrl: HTTP $code"
e2e_jq '.method == "GET" and (.url | startswith("http"))' "$work/logs/last-api.out"
dl_url=$(jq -r '.url' "$work/logs/last-api.out")
curl -fsS "$dl_url" -o "$work/logs/dl.out" || e2e_fail "download fetch failed"
cmp -s "$work/logs/dl.out" "$work/fixtures/a.txt" || e2e_fail "download bytes differ"
e2e_pass

# --- stage 9: ssh / scp through the external-client netns -------------------
e2e_stage "ssh-scp"
e2e_ok "$B" info "$c1" --json
e2e_jq '.sandbox.sshEndpoint == null' "$work/logs/last.out"
e2e_expect_rc 0 "$work/logs/ssh1.out" \
  e2e_cli ssh "$c1" 'echo ssh-ok; uname -s'
e2e_have ssh-ok "$work/logs/ssh1.out"
e2e_have Linux "$work/logs/ssh1.out"
e2e_ok "$B" info "$c1" --json
e2e_jq '.sandbox.sshEndpoint != null' "$work/logs/last.out"
# ssh -t needs a REAL local PTY (OpenSSH may decline allocation without
# one): the script wrapper supplies it through the netns, bounded.
e2e_expect_rc 0 "$work/logs/ssh2.out" \
  e2e_pty_cli 30 ssh -t "$c1" 'test -t 1 && echo isatty'
e2e_have isatty "$work/logs/ssh2.out"
e2e_expect_rc 0 "$work/logs/scp-up.out" \
  e2e_cli scp -r "$work/scp-up" "$c1:/home/user/scpdir"
e2e_ok "$B" exec "$c1" -- bash -lc 'ls /home/user/scpdir /home/user/scpdir/d'
e2e_have f1.txt "$work/logs/last.out"
e2e_have f2.txt "$work/logs/last.out"
rm -rf "$work/scp-down"; mkdir -p "$work/scp-down"
e2e_expect_rc 0 "$work/logs/scp-down.out" \
  e2e_cli scp -r "$c1:/home/user/scpdir" "$work/scp-down/scpdir"
diff -r "$work/scp-up" "$work/scp-down/scpdir" || e2e_fail "scp round trip differs"
e2e_pass

# --- stage 10: stop / resume with exact reservation accounting ----------------
e2e_stage "stop-resume"
e2e_ok "$B" exec "$c1" -- bash -lc 'echo keep-me > /home/user/keep.txt'
e2e_ok "$B" exec "$c1" --detach --json -- bash -lc 'sleep 600'
spid=$(jq -r '.processId' "$work/logs/last.out")
e2e_pg_env
sb_sql="SELECT node_id, reserved_vcpus, reserved_memory_mib, reserved_disk_gib, desired_vcpus, desired_memory_mib, desired_disk_gib FROM public.sandboxes WHERE id=:'sid'"
nd_sql="SELECT reserved_vcpus, reserved_memory_mib, reserved_disk_gib FROM public.nodes WHERE id=:'nid'"
res_pre=$(pg_query "$sb_sql" -v sid="$c1")
res_pre_node=$(pg_query "$nd_sql" -v nid="e2e-node")
node_id=$(cut -d'|' -f1 <<<"$res_pre")
pre_v=$(cut -d'|' -f2 <<<"$res_pre"); pre_m=$(cut -d'|' -f3 <<<"$res_pre"); pre_d=$(cut -d'|' -f4 <<<"$res_pre")
pre_desired=$(cut -d'|' -f5-7 <<<"$res_pre")
pre_nv=$(cut -d'|' -f1 <<<"$res_pre_node"); pre_nm=$(cut -d'|' -f2 <<<"$res_pre_node"); pre_nd=$(cut -d'|' -f3 <<<"$res_pre_node")
# Pre-stop reservations are positive and the compute reservation IS the
# sandbox's desired compute (read from the row, not hardcoded).
[ "$pre_v" -gt 0 ] && [ "$pre_m" -gt 0 ] && [ "$pre_d" -gt 0 ] \
  || e2e_fail "pre-stop reservations not positive ($pre_v/$pre_m/$pre_d)"
[ "$pre_desired" = "$pre_v|$pre_m|$pre_d" ] || e2e_fail "pre-stop reservation differs from desired resources ($pre_v/$pre_m/$pre_d vs $pre_desired)"
e2e_ok "$B" stop "$c1" --json
e2e_wait_state "$c1" 'archived'
e2e_ok "$B" info "$c1" --json
e2e_jq '.sandbox.stoppedAt != null' "$work/logs/last.out"
res_post=$(pg_query "$sb_sql" -v sid="$c1")
res_post_node=$(pg_query "$nd_sql" -v nid="e2e-node")
post_node_id=$(cut -d'|' -f1 <<<"$res_post")
post_v=$(cut -d'|' -f2 <<<"$res_post"); post_m=$(cut -d'|' -f3 <<<"$res_post"); post_d=$(cut -d'|' -f4 <<<"$res_post")
post_desired=$(cut -d'|' -f5-7 <<<"$res_post")
post_nv=$(cut -d'|' -f1 <<<"$res_post_node"); post_nm=$(cut -d'|' -f2 <<<"$res_post_node"); post_nd=$(cut -d'|' -f3 <<<"$res_post_node")
# compute released exactly; desired resources and disk reservations EXACTLY
# unchanged on both rows.
[ "$post_v" = 0 ] && [ "$post_m" = 0 ] || e2e_fail "sandbox compute reservation not zero after stop ($post_v/$post_m)"
[ "$post_desired" = "$pre_desired" ] || e2e_fail "desired resources changed across stop: $pre_desired -> $post_desired"
[ "$post_d" = "$pre_d" ] || e2e_fail "sandbox disk reservation changed: $pre_d -> $post_d"
[ "$post_nv" = "$((pre_nv - pre_v))" ] || e2e_fail "node vCPU reservation $pre_nv->$post_nv, want $((pre_nv - pre_v))"
[ "$post_nm" = "$((pre_nm - pre_m))" ] || e2e_fail "node memory reservation $pre_nm->$post_nm, want $((pre_nm - pre_m))"
[ "$post_nd" = "$pre_nd" ] || e2e_fail "node disk reservation changed: $pre_nd -> $post_nd"
[ "$post_node_id" = "$node_id" ] || e2e_fail "sandbox moved nodes across stop"
e2e_ok "$B" resume "$c1" --json
e2e_wait_state "$c1" 'ready|running|idle'
# Only after resume is the process record readable again: the pre-stop
# detached process is reported lost (lowercase).
e2e_ok "$B" exec "$c1" --status "$spid" --json
e2e_jq '.status == "lost"' "$work/logs/last.out"
e2e_ok "$B" exec "$c1" -- cat /home/user/keep.txt
e2e_have keep-me "$work/logs/last.out"
e2e_ok "$B" info "$c1" --json
e2e_jq '.sandbox.sshEndpoint == null' "$work/logs/last.out" # cleared by the stop
e2e_pass

# --- stage 11: snapshots -----------------------------------------------------
e2e_stage "snapshots"
e2e_ok "$B" exec "$c1" -- bash -lc 'echo warm > /home/user/warm.txt'
e2e_ok "$B" snapshot "$c1" e2e-base
e2e_ok "$B" delete "$c1" --yes --wait
# restore inherits the snapshot type; the artifact outlives its source.
e2e_run_rc "$work/logs/snap-new1.out" "$B" new --from e2e-base --json
[ "$E2E_RC" = 0 ] || e2e_fail "new --from: $(tail -3 "$work/logs/snap-new1.out" | e2e_redact)"
r1=$(jq -r 'select(.event == "ready") | .id' "$work/logs/snap-new1.out" | tail -1)
[ -n "$r1" ] || e2e_fail "no id from new --from"
e2e_ids+=("$r1")
e2e_ok "$B" exec "$r1" -- cat /home/user/warm.txt
e2e_have warm "$work/logs/last.out"
# Mutate the first restore; a second restore of the same artifact must still
# see the ORIGINAL content (immutable artifact) and none of the mutation.
e2e_ok "$B" exec "$r1" -- bash -lc 'echo mutation > /home/user/mutated.txt'
# Capacity: r1 is deleted (durably) BEFORE the second restore is admitted.
e2e_ok "$B" delete "$r1" --yes --wait
e2e_run_rc "$work/logs/snap-new2.out" "$B" new --from e2e-base --type default --json
[ "$E2E_RC" = 0 ] || e2e_fail "new --from --type default: $(tail -3 "$work/logs/snap-new2.out" | e2e_redact)"
r2=$(jq -r 'select(.event == "ready") | .id' "$work/logs/snap-new2.out" | tail -1)
[ -n "$r2" ] || e2e_fail "no id from new --from --type default"
e2e_ids+=("$r2")
e2e_ok "$B" exec "$r2" -- bash -lc 'cat /home/user/warm.txt; test ! -f /home/user/mutated.txt'
e2e_have warm "$work/logs/last.out"
e2e_ok "$B" delete "$r2" --yes --wait
e2e_ok "$B" snapshot rm e2e-base
e2e_expect_rc 1 "$work/logs/snap-gone.out" "$B" new --from e2e-base --json
tail -1 "$work/logs/snap-gone.out" >"$work/logs/snap-gone-last.json"
e2e_jq '.event == "error"' "$work/logs/snap-gone-last.json"
e2e_pass

# --- stage 12: dogfood --------------------------------------------------------
e2e_stage "dogfood-run-make-check"
t0=$(date +%s)
e2e_run_rc "$work/logs/run-make-check.out" \
  bash -c 'cd "$1" && exec "$2" run --push . -- make check' _ "$src" "$B"
rc_line="exit $E2E_RC in $(( $(date +%s) - t0 ))s"
tail -5 "$work/logs/run-make-check.out" | e2e_redact
[ "$E2E_RC" = 0 ] || e2e_fail "bbraft run --push . -- make check $rc_line"
echo "run $rc_line"
e2e_pass

# --- stage 13: pinned-spec validation of everything seen so far ---------------
# LIVE preflight: the proxy still runs, so there is no drained audit yet. The
# FINAL validation (audit required) runs in stage 14 after the proxy drained.
e2e_stage "validate-api-v1-live"
if ! e2e_validate live "$work/logs/validate-live.log"; then e2e_fail "compat validation (live preflight; see validate-live.log)"; fi
e2e_pass

# --- stage 14: delete all, stop everything, prove no leaks --------------------
e2e_stage "delete-all+leaks"
# Recovery sweep with --all (archived/error/pending included): the fresh
# unique owner means the list can only show this run's sandboxes.
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
# Durable: node counters zero and no row of THIS OWNER keeps a reservation.
if ! e2e_reservation_proof; then e2e_fail "reservation proof"; fi
# Node down, node half of the leak proof (plain calls: errexit stays active in
# the helpers). node_started is cleared only after BOTH succeeded, so a failed
# proof is still retried and reported by the outer cleanup.
e2e_node_stop
e2e_node_leak_proof
node_started=0
# Drain the proxy (final audit, exit status 0 required), then the FINAL
# validation that REQUIRES that audit and covers every delete/stop response.
if ! e2e_stop_proxy; then e2e_fail "proxy drain"; fi
if ! e2e_validate final "$work/logs/validate-final.log"; then e2e_fail "compat validation (final; see validate-final.log)"; fi
if ! e2e_stop_server; then e2e_fail "server did not stop"; fi
# ANY new matching process/link/table/unit since the baseline is a leak.
if ! e2e_leak_scan; then e2e_fail "leaks found (see above)"; fi
e2e_body_complete=1
e2e_pass

# All stages passed. The summary and E2E PASS itself are printed by the EXIT
# trap, only after the teardown and the ip_forward restoration verify clean.
printf '\nall stages passed; tearing down for the final leak proof\n'
e2e_pass_pending=1
