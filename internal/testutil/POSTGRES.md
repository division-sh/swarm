# Postgres Test Environment

Swarm's Postgres-backed tests support PostgreSQL 16 as the parity target. The
preferred local setup is a host Postgres instance selected explicitly with
`SWARM_TEST_POSTGRES_DSN`; this avoids the Docker or Colima memory cost during
normal test iteration.

The test role must use password authentication and have `CREATEDB`. The harness
creates isolated databases, initializes one canonical schema template, clones
that template for each test, and removes the databases during cleanup. Custom
`pqgo-*` TLS registrations, GSS authentication, passfiles, service files, and
empty passwords are intentionally unsupported because cleanup runs in a separate
process and must receive a self-contained connection value.

## Existing Host Postgres

For a local PostgreSQL 16 server whose current administrator can create roles:

```bash
psql postgres <<'SQL'
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'swarm_test') THEN
    CREATE ROLE swarm_test LOGIN CREATEDB PASSWORD 'swarm-test';
  ELSE
    ALTER ROLE swarm_test LOGIN CREATEDB PASSWORD 'swarm-test';
  END IF;
END
$$;
SQL
```

Verify the client connection. `PGPASSWORD` is used only by this manual
`pg_isready` command; the Go harness receives its password in the explicit DSN.

```bash
PGPASSWORD='swarm-test' pg_isready \
  -h 127.0.0.1 -p 5432 -U swarm_test -d postgres
```

Run tests with an invocation-scoped DSN instead of a persistent shell export:

```bash
SWARM_TEST_POSTGRES_DSN='host=127.0.0.1 port=5432 user=swarm_test password=swarm-test dbname=postgres sslmode=disable' \
  go test ./...
```

URL DSNs are equally supported:

```bash
SWARM_TEST_POSTGRES_DSN='postgres://swarm_test:swarm-test@127.0.0.1:5432/postgres?sslmode=disable' \
  go test ./internal/testutil -run '^TestStartPostgres' -count=1
```

A set but invalid DSN fails closed and never falls through to Docker.

## Dedicated Fast Instance

The durability settings below are safe only for a disposable test cluster. Do
not apply them to a shared development or production server.

On macOS with Homebrew PostgreSQL 16:

```bash
PG_BIN="$(brew --prefix postgresql@16)/bin"
PG_DATA="$HOME/Library/Application Support/swarm/postgres-test"
mkdir -p "$PG_DATA"
printf '%s\n' 'swarm-test' > "$PG_DATA/.pwfile"
"$PG_BIN/initdb" \
  -D "$PG_DATA" \
  -U swarm_test \
  --auth-host=scram-sha-256 \
  --pwfile="$PG_DATA/.pwfile"
rm "$PG_DATA/.pwfile"
"$PG_BIN/pg_ctl" \
  -D "$PG_DATA" \
  -l "$PG_DATA/postgres.log" \
  -o "-p 55432 -c max_connections=300 -c fsync=off -c synchronous_commit=off -c full_page_writes=off" \
  start
```

Use port `55432` in `SWARM_TEST_POSTGRES_DSN`. Stop the dedicated instance with:

```bash
"$(brew --prefix postgresql@16)/bin/pg_ctl" \
  -D "$HOME/Library/Application Support/swarm/postgres-test" stop
```

## Docker Fallback

When `SWARM_TEST_POSTGRES_DSN` is absent, the harness announces and uses a Docker
fallback if a daemon is already running. It does not start Docker Desktop or
Colima. The owned container uses tmpfs plus `fsync=off`,
`synchronous_commit=off`, and `full_page_writes=off`.

If Docker is missing or unavailable, the failure points back to this guide and
retains the underlying executable, socket, startup, or readiness error.
