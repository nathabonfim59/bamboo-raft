# AGENTS.md

Instructions for coding agents (and humans) working on bamboo-raft. Read this
and [README.md](README.md) before changing code.

## What bamboo-raft is

Self-hosted persistent Linux VMs (QEMU/KVM) for AI agents. Binaries:

| binary | role |
|---|---|
| `bbraft-server` | control plane: `/api/v1` + `/api/bbraft` (extensions), Postgres, node hub |
| `bbraft-node` | one per KVM host: runs QEMU VMs, serves `nodev1.Node` over a link it dials to the server |
| `bbraft-guest` | inside every VM: exec, files, freeze/thaw, over gRPC on vsock |
| `bbraft` | CLI |

## Commands

```sh
mise install      # Go 1.27.3 and protoc, pinned in mise.toml
make check        # gofmt check, go mod tidy check, go vet, staticcheck, go test -race
make build        # every cmd/* into bin/
make proto        # regenerate internal/proto/** from proto/** (generated code is committed)
go test ./internal/nodelink/ -run TestReconnect -v   # one package / test
```

`make check` must pass before you report work as done. Heavy runs (the full
race suite, QEMU/KVM tests, image builds) belong on a throwaway VM with
`/dev/kvm`, `/dev/vhost-vsock`, Postgres and QEMU, for example a fresh
sandbox: `bbraft run --push . -- make check`. `BBRAFT_TEST_REQUIRE_ALL=1`
makes a missing service fail instead of skip.

staticcheck and the protoc plugins are module tools (`go tool ...`), so there
is nothing else to install.

## Layout

| path | owns |
|---|---|
| `proto/bbraft/node/v1/` | **server ⇄ node contract** (`nodev1.Node`), frozen |
| `internal/proto/nodev1/` | its generated code (`make proto`) |
| `internal/nodelink/` | transport: the node dials the server; `Hub` (server side), `Serve` (node side) |
| `internal/nodetest/` | conformance suite every `nodev1.Node` implementation passes |
| `internal/nodefake/` | in-process `nodev1.Node` without VMs, for control-plane tests |
| `internal/objstore/` | object store contract (`Store`), `memstore`, conformance suite `objstoretest`, `s3` |
| `internal/snapformat/` | snapshot chunks, manifests and their object keys, shared by node and server |
| `internal/ids/` | `bx_` sandbox ids |
| `internal/machinetype/` | sandbox sizes (small/default/large/xlarge) |
| `internal/version/` | build version |
| `cmd/bbraft-node/`, `internal/node/...` | node agent |
| `cmd/bbraft-guest/`, `internal/guest/...`, `proto/bbraft/guest/` | guest agent and node ⇄ guest protocol |
| `images/` | guest image build |
| `cmd/bbraft-server/`, `internal/{server,api,apierr,auth,db,lifecycle,scheduler,compat}/` | control plane |
| `cmd/bbraft/`, `internal/cli/` | CLI |
| `api/` | OpenAPI documents: `bbraft-v1.yaml` (`/api/v1`), `bbraft-ext.yaml` (`/api/bbraft`) |
| `infra/ansible/` | host provisioning; roles per binary (`bbraft_node`, `postgres`, `bbraft_server`) |

## Contracts you must not bend

- **`nodev1.Node` is the only way the server touches a node.** The proto
  comments are the spec (blocking `CreateVM`, idempotent deletes, Exec frame
  order, path rules, error codes). Every implementation passes
  `nodetest.Run`: `nodefake` in-process and through `nodelink`, the real
  `bbraft-node` on a KVM host. Changing the contract means changing the
  proto, the suite, the fake and every implementation in one change.
- **Nodes dial the server** (`nodelink`). The server never opens connections
  to nodes and never holds node credentials.
- **The guest never holds platform credentials**: no API keys, node tokens
  or object-store keys inside a VM.
- **Roles share nothing in-process.** Server roles talk through Postgres, the
  object store, gRPC and HTTP only. No package-level mutable state.
- **Tenant everywhere.** Every sandbox, snapshot and key row has an owner
  (account id), even while an install has one user.
- **API documents are the contract.** `/api/v1` responses validate against
  `api/bbraft-v1.yaml` (envelopes `{ok, type, ...}`, error shape
  `{ok:false, type:"sandbox.error", status, code, message, error:{...},
  requestId}`, status codes); `internal/compat` checks it in tests and
  `docs/api.md` records behavior the schema can't express. Extensions live
  under `/api/bbraft`, never as new fields in `/api/v1`.
- **No host details in the repository** (IPs, hostnames, key paths, disk
  serials, provider, region, account ids). They live only in the gitignored
  `infra/ansible/inventory/`. Examples use documentation addresses
  (203.0.113.0/24) and placeholders.
- **Secrets** never in logs, errors, test output or committed files. Tokens
  and keys are compared in constant time and stored hashed.

## Configuration

Each binary takes flags with a `BBRAFT_*` environment fallback:
`-node.data-dir` ⇄ `BBRAFT_NODE_DATA_DIR` (dots and dashes become
underscores, upper-cased). Secrets come from files (`-node.token-file`) or
the environment, never from flags. Object storage uses the `BBRAFT_S3_*`
variables.

## Go style

We write modern Go for the version in `go.mod`. Before editing Go code, load
the **use-modern-go** skill (vendored in `.agents/skills/`, linked from
`.claude/skills/` and `.devin/skills/`; without skill support run
`sh .agents/skills/use-modern-go/scripts/run-tool.sh list --file-path <file>`)
and follow every guideline it returns. The ones that come up most here:

- `encoding/json/v2` for all new JSON (`jsontext` for NDJSON streams). Not
  `encoding/json`.
- Standard library `uuid` (`uuid.NewV7()`). No third-party uuid/ulid.
- `wg.Go(...)`, `t.Context()`, `b.Loop()`, `for i := range n`, `min`/`max`,
  `slices`/`maps` helpers and iterators, `strings.CutLast`,
  `errors.AsType[T]`, `new(value)`, `cmp.Or`.
- `os.Root` for filesystem access under a configured or confined directory
  (guest file API, node data dir).
- `http.ServeMux` method patterns and `r.PathValue`. No router dependency.
- `log/slog` only, structured key/value pairs, logger passed in (no global
  logger).

And the usual rules:

- `context.Context` first on anything that does I/O or can block; respect
  cancellation. Child processes die with their context (process groups).
- Wrap errors with context (`fmt.Errorf("qemu: start %s: %w", id, err)`);
  compare with `errors.Is`/`errors.AsType`. gRPC errors use `status` codes as
  the proto documents.
- No panics on bad input; panics are for programmer errors only.
- Doc comments on every exported identifier; package doc on every package.
- Small interfaces, defined where they're consumed, unless they are a shared
  contract.
- gofmt'd, `go vet` and staticcheck clean. No `//nolint` without a comment
  saying why.

## Tests

- Table-driven, standard `testing` only (no testify). `t.Context()`,
  `t.TempDir()`.
- Race-clean: `make check` runs `-race`.
- Anything needing a real service is an integration test that **skips**
  unless its variable is set, documented in the test's doc comment:
  - `BBRAFT_TEST_KVM=1` plus `BBRAFT_TEST_IMAGE_DIR=<dir>`: QEMU/KVM tests
    (real VMs from a built image).
  - `BBRAFT_TEST_DATABASE_URL=postgres://...`: Postgres tests. Each test
    creates its own schema and drops it.
- Test helpers in a package shared with another area get a prefix naming
  your area (`nodeQMPHelper`, not `helper`) so merges don't collide.
- Fixing a bug comes with a test that fails before the fix.

## Documentation

The Markdown in the repository is the single source of the documentation site
(VitePress, `site/`): `docs/*.md` and the infra READMEs are read in place and
`scripts/doc-links.mjs` decides which files are pages and their URLs. Write
links relative to the file, as GitHub resolves them; the site rewrites them.
The HTTP API reference (from `api/*.yaml`) and the Go API reference (from the
source, gomarkdoc) are generated on every build into the gitignored
`docs/api-reference/`, so keep doc comments and spec descriptions current.

```sh
npm ci
npm run docs:dev     # generate the API reference and serve the site
npm run docs:build   # what CI runs; dead links fail the build
```

## Dependencies

Keep to the standard library and the modules already in `go.mod` (and, for
the docs site, the packages in `package.json`). Any other
module needs a one-line justification (what it replaces, why the stdlib isn't
enough). Pin versions; run `go mod tidy`.

## Safety

- Never touch real hosts from a development session. Deploys go through the
  Ansible roles with an inventory you own.
- Report done only after `make check` passes, with the command output
  summary and the list of files changed.
