# postgres

PostgreSQL 17+ for `bbraft-server`, for Debian 13 (trixie). The role installs
the distro package and adds one drop-in to the existing cluster. It **never**
runs `initdb`, drops a role or database, rewrites `postgresql.conf`, or
replaces `pg_hba.conf`; a missing cluster is an error, not something the role
creates.

The role is not wired into `site.yml` yet: the shared playbook, inventory and
`requirements.yml` are owned by the lead and change only with approval.

## Dependency

`tasks/main.yml` and `tasks/database.yml` use
`community.postgresql.postgresql_query`, `postgresql_user` and
`postgresql_db`. That collection is not in the shared `requirements.yml`, so
install the role-local file into the same `.collections` directory:

```sh
cd infra/ansible
mise exec -- ansible-galaxy collection install -r roles/postgres/requirements.yml
```

Syntax-checked with `community.postgresql` 5.1.1 on ansible-core 2.22.0. The
role-local `requirements.yml` pins that exact version; the lead can move the
dependency into the shared `requirements.yml` when wiring the role.

## Variables

| Variable | Default | Meaning |
|---|---|---|
| `postgres_password` | **required**, no default | password for `postgres_user`; supply it from a vault or the private inventory |
| `postgres_version` | `"17"` | major version; must be 17 or newer (`postgresql-<version>` package) |
| `postgres_cluster` | `main` | existing Debian cluster name |
| `postgres_user` | `bbraft` | owning role, created when absent |
| `postgres_database` | `bbraft` | database owned by the role, created when absent |
| `postgres_listen_addresses` | `127.0.0.1` | drop-in `listen_addresses`; loopback by default |
| `postgres_port` | `5432` | drop-in `port`; positive decimal integer 1-65535 (floats and non-decimal strings are rejected, never truncated) |
| `postgres_unix_socket` | `/var/run/postgresql` | socket directory used by the version probe and the user/database modules |
| `postgres_service` | `postgresql` | systemd unit started and enabled |
| `postgres_group` | `postgres` | group that reads the drop-in |
| `postgres_config_dir` | `/etc/postgresql/<version>/<cluster>` | existing cluster configuration directory |
| `postgres_dropin` | `<config_dir>/conf.d/bbraft.conf` | the only file changed |
| `postgres_file_owner` / `postgres_file_group` / `postgres_file_mode` | `root` / `postgres` / `0640` | drop-in ownership |
| `postgres_manage_service` | `true` | set `false` only in role-local render tests |

The drop-in also pins `password_encryption = 'scram-sha-256'`. Existing
password hashes keep working.

Changing `postgres_port` or `postgres_listen_addresses` restarts the cluster
before the version probe and before the role/database modules connect. The
probe and `community.postgresql` tasks use `postgres_unix_socket` and
`postgres_port`, so an override reaches the intended cluster rather than the
default 5432 one.

## Security

- No default or generated database password exists. An empty
  `postgres_password` fails before any package or database task.
- The password is passed to `community.postgresql.postgresql_user` as a module
  argument with `no_log: true`; it does not appear in Ansible output, a
  command line, or an error message.
- The role only creates or updates the role and database. It never deletes
  either, and it never resets a cluster. Re-running with the same password is
  a no-op (the module compares the stored hash).
- `pg_hba.conf` is untouched. Loopback TCP uses Debian's default
  `scram-sha-256` rules. Any non-loopback listener needs an explicit
  `pg_hba.conf` rule added by the operator.

## Database URL for bbraft-server

The server role takes a connection URL, not the pieces. For the loopback
default, build it with the same password (URL-encode special characters):

```text
postgresql://bbraft:<url-encoded-password>@127.0.0.1:5432/bbraft?sslmode=disable
```

`sslmode=disable` is acceptable only on loopback. For a remote database use
TLS and the provider's CA settings.

## Tests

Role-local, localhost only, no live connection; they render into a temporary
directory and never install or restart PostgreSQL:

```sh
cd infra/ansible
mise exec -- sh roles/postgres/tests/run.sh
```

The suite checks:

- the `postgres_positive_int` filter unit tests
- `--syntax-check` of the full role, validation and render plays
- task order: validation runs before the apt task, and the pending config
  restart runs before the version probe and the user/database modules
- exact rendered `listen_addresses`, `port`, `password_encryption` and mode
- a custom loopback address/port pair renders literally
- an ASCII-decimal string port is accepted
- rejection of an empty password, port 0, a float port, a non-decimal port,
  PostgreSQL 16, an invalid SQL identifier and a quote in
  `listen_addresses`
- a verbose validation/render pass that prints no database password
  placeholder

Not covered locally: package installation, cluster discovery, role/database
creation against a real PostgreSQL and the restart handler. Those need a
disposable Debian 13 host or test VM; the lead runs that. Never run the role
in apply or check mode against a real inventory from here.
