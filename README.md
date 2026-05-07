# nbd-server

Experimental Rust workspace for a Network Block Device server, local control
plane, storage engines, CLI, and integration test harness.

The project is a systems prototype. It has real protocol, catalog, runtime,
WAL, COW read-view, compaction, and kernel smoke coverage, but it is not yet a
production NBD daemon. The current focus is making the server's state
boundaries explicit and testable.

## What Is Here

- `nbd-server`: TCP NBD server, connection runtime, export runtime, admission,
  local export registry, memory storage, simple durable storage, and WAL-backed
  durable storage.
- `nbd-control-plane`: catalog domain model and SQLite-backed catalog.
- `nbd-config`: default config generation and config loading.
- `nbd-protocol`: NBD wire constants, parsing, and encoding.
- `nbd-us-client`: userspace NBD client used by integration tests.
- `nbdcli`: operator CLI for export catalog operations.
- `nbd-test-support`: shared test fixtures.

The server currently supports three export engine kinds:

- `memory`: volatile in-memory export.
- `simple_durable`: sparse local blob chunks with mutable tree metadata.
- `wal_durable`: WAL-backed durable export with COW committed roots, retained
  WAL overlay reads, close compaction, and write-pressure compaction.

## Status

Implemented and tested:

- NBD handshake, option negotiation, read/write/flush/disconnect, pipelining,
  error replies, and userspace TCP integration coverage.
- Per-export queue depth, serial and concurrent export runtimes, admission
  ordering for read/write/flush, and queue-slot lifetime through reply write or
  reply drop.
- Server-owned connection supervision and cooperative graceful shutdown.
- SQLite catalog with `exports`, `export_heads`, and tree metadata.
- Durable WAL append/replay, COW read view, close compaction, and
  write-pressure compaction.
- `nbdcli create`, `list`, `inspect`, `clone`, and `delete`.
- Docker-based Linux kernel NBD smoke scenarios for memory, simple durable,
  and WAL durable paths.

Important limitations:

- This does not implement durable serving leases, fencing, auth/client identity,
  or multi-connection serving semantics.
- The server does not advertise `NBD_FLAG_CAN_MULTI_CONN`.
- Graceful shutdown has no timeout escalation; a stuck engine operation can
  delay shutdown.
- `simple_durable` is direct-commit mutable storage. It is not WAL-backed,
  COW, clone-safe, or crash-atomic across multi-chunk failures.
- `nbdcli clone` is a committed COW checkpoint clone. It does not include
  uncheckpointed source WAL records.
- Postgres catalog URLs are parsed but not implemented.

## Requirements

- Rust 1.85 or newer.
- `make`.
- Node.js/npm through `npx` for Prisma migration commands.
- Docker with privileged container support for kernel NBD smoke tests.
- Linux with kernel NBD tooling for manual `/dev/nbd*` attachment.

macOS is fine for Rust unit and integration tests. Kernel NBD attachment needs
Linux, which this repo exercises through Docker.

## Build

```sh
cargo build -p nbd-server -p nbdcli
```

Useful check commands:

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

Protocol-focused test:

```sh
make test-protocol
```

Kernel smoke test:

```sh
make docker-smoke
```

The default Docker smoke scenario is `wal-durable-basic`. Other scenarios:

```sh
KERNEL_SMOKE_SCENARIO=memory-basic make docker-smoke
KERNEL_SMOKE_SCENARIO=simple-durable-basic make docker-smoke
KERNEL_SMOKE_SCENARIO=wal-durable-basic make docker-smoke
```

Smoke artifacts are written under `.tmp/docker-smoke` by default.

## Local Config And Catalog

Default user config paths:

- config: `~/.nbd/config.toml`
- catalog: `~/.nbd/catalog.db`
- blobs: `~/.cache/nbd/blobs`
- WAL files: `~/.cache/nbd/wal`
- server log: `/tmp/nbd/current.log`

Generated config shape:

```toml
[catalog]
url = "file:/path/to/catalog.db"

[runtime]
state_dir = "/path/to/state"
blob_dir = "/path/to/blobs"
wal_dir = "/path/to/wal"

[server]
export_runtime = "concurrent"
export_queue_depth = 64

[logging]
file_path = "/tmp/nbd/current.log"
```

The SQLite catalog file must be migrated before normal use:

```sh
mkdir -p "$HOME/.nbd"
DATABASE_URL="file:$HOME/.nbd/catalog.db" make -C prisma db-migrate
```

The server can create the SQLite file if it is missing, but it does not apply
the schema migration itself.

For throwaway local testing, use an explicit config under `.tmp`:

```sh
mkdir -p .tmp/local-state .tmp/local-blobs .tmp/local-wal
cat > .tmp/local.toml <<'EOF'
[catalog]
url = "file:.tmp/local-catalog.db"

[runtime]
state_dir = ".tmp/local-state"
blob_dir = ".tmp/local-blobs"
wal_dir = ".tmp/local-wal"

[server]
export_runtime = "concurrent"
export_queue_depth = 64

[logging]
file_path = ".tmp/nbd-server.log"
EOF

DATABASE_URL="file:../.tmp/local-catalog.db" make -C prisma db-migrate
```

The `DATABASE_URL` example above is relative to the `prisma/` directory because
`make -C prisma` changes the working directory. The paths in `.tmp/local.toml`
are relative to the process that runs `nbd-server` or `nbdcli`.

## Basic Usage

Create an export:

```sh
cargo run -p nbdcli -- --config .tmp/local.toml create disk-a \
  --size 67108864 \
  --engine wal_durable
```

Inspect or list exports:

```sh
cargo run -p nbdcli -- --config .tmp/local.toml list
cargo run -p nbdcli -- --config .tmp/local.toml inspect disk-a
cargo run -p nbdcli -- --config .tmp/local.toml inspect disk-a --json
```

Clone a committed COW checkpoint:

```sh
cargo run -p nbdcli -- --config .tmp/local.toml clone disk-a disk-b
```

`clone` only copies the source export's committed COW root. It does not copy
source WAL records newer than that committed checkpoint.

Run the server:

```sh
cargo run -p nbd-server -- serve \
  --config .tmp/local.toml \
  --listen 127.0.0.1:10809 \
  --log-stdout
```

Stop the server with Ctrl-C. The binary waits for `NbdServer::shutdown()` so
accepted connection tasks can stop cooperatively and close active exports.

On Linux, a manual kernel client flow looks like this. The `mkfs.ext4` command
formats the attached block device.

```sh
sudo modprobe nbd
sudo nbd-client 127.0.0.1 10809 /dev/nbd0 -N disk-a
sudo mkfs.ext4 /dev/nbd0
sudo mount /dev/nbd0 /mnt/nbd-demo
```

Before stopping a manual kernel test, detach cleanly:

```sh
sudo umount /mnt/nbd-demo
sudo nbd-client -d /dev/nbd0
```

## Architecture

The main runtime path is:

```text
TCP connection
  -> connection reader / reply writer
  -> LocalExportRegistry
  -> ExportRuntime
  -> ExportAdmissionCtl
  -> ExportEngine
```

Key ownership rules:

- Connection code owns socket I/O, negotiation, request decoding, reply
  encoding, and per-connection reply serialization.
- `LocalExportRegistry` owns process-local active export state and final-owner
  close.
- `ExportRuntime` owns queue slots, accepted job lifecycle, runtime close, and
  engine close.
- `ExportAdmissionCtl` owns semantic read/write/flush ordering.
- Storage engines execute only admitted requests.
- Catalog code owns durable export metadata and schema interpretation.

Useful architecture docs:

- `docs/architecture/nbd-protocol-architecture.md`
- `docs/architecture/export-admission-control.md`
- `docs/architecture/local-export-registry-architecture.md`
- `docs/architecture/storage-engine-architecture.md`
- `docs/architecture/wal-architecture.md`
- `docs/architecture/export-read-view-architecture.md`
- `docs/architecture/compaction-manager-architecture.md`
- `docs/architecture/export-catalog-architecture.md`

## Development Workflow

Use the smallest meaningful validation first, then broaden before handoff.

Common commands:

```sh
cargo fmt --all --check
make test-protocol
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
make docker-smoke
```

Catalog migration check:

```sh
make -C prisma db-migrate-check
```

Build server and CLI:

```sh
make build
```

The repository uses durable planning docs under `docs/plans/` and execution
contracts under `docs/execution/`. Treat active docs as source of truth when a
feature has one.

## Logging

`nbd-server serve` initializes structured `tracing` output. By default it
writes JSON lines to the configured log file. Use `--log-stdout` to mirror logs
to stdout.

`RUST_LOG` controls filtering. If unset, the server uses its built-in default
filter with operational events enabled and request internals quieter.

The request path logs structured identifiers such as connection id, request
sequence, cookie, command, offset, and length. It does not log payload bytes.
