Title: Catalog SDK V1
Date: 2026-05-01
Status: approved

# Problem

The first server checkpoint needs a control plane before the NBD data path can
open named exports. `nbdcli` should not own business logic, and integration
tests should not shell out to create exports. Both should use the same Rust SDK
over the same catalog/lifecycle boundary.

This slice also needs the first database schema and migration workflow. Prisma
should define and migrate the schema, while Rust runtime code should talk to
SQLite through explicit catalog traits.

# Goal

Implement the M1 control-plane slice:

- Prisma schema and SQLite migration for exports and export generations;
- `nbd-control-plane` Rust crate;
- SQLite-backed `ExportCatalog`;
- thin `nbdcli` binary;
- integration tests using temporary databases through `nbd-test-support`.

# Constraints

- Runtime code must be Rust.
- Prisma is used for schema and migrations, not as a Rust runtime ORM.
- SQLite is the first runtime database.
- The schema should stay friendly to a future Postgres implementation.
- `nbdcli` must be a thin wrapper over `nbd-control-plane`.
- Tests must use isolated temp databases and explicit config.
- No NBD protocol, WAL, `ExportReadView`, tree metadata, or storage engine is
  implemented in this slice.

# Non-Goals

- Implementing clone.
- Implementing tree nodes or node edges.
- Implementing compaction checkpoint publication.
- Implementing real etcd leases.
- Implementing open/delete race prevention.
- Implementing `ExportLifecycleManager`.
- Implementing the NBD server open path.
- Implementing Docker/kernel NBD tests.

# End State

After this slice:

- Prisma can create a SQLite catalog schema with stable export identity rows
  and append-only committed-root generation rows.
- `nbd-control-plane` can create/list/inspect/delete exports.
- `nbdcli` exposes create/list/inspect/delete by calling the SDK.
- Integration tests can create a temp database, apply migrations, exercise the
  SDK, and drop all artifacts afterward.
- Deleted exports cannot be loaded as active exports by the SDK.
- The API shape leaves room for clone, lifecycle leases, and tree metadata
  without requiring them in M1.

# Proposed Approach

Add two runtime crates:

```text
crates/nbd-control-plane
  ExportCatalog trait, SQLiteExportCatalog, request/response structs,
  catalog errors

crates/nbdcli
  CLI binary that loads config and calls nbd-control-plane
```

Add Prisma schema and migrations:

```text
prisma/schema.prisma
prisma/migrations/...
prisma/Makefile
```

The migration command should be handled by `prisma/Makefile` in this slice, but
Rust runtime code should only require a migrated database.

M1 should use `sqlx` for the Rust runtime database client. `sqlx` supports both
SQLite and Postgres, maps naturally to explicit SQL behind `ExportCatalog`, and
keeps the runtime path independent of Prisma-generated clients.

# Catalog URL Boundary

M1 should introduce a `CatalogUrl` type as the runtime parser for
`catalog.url`. The config file can keep a plain string field, but runtime code
should not pass raw catalog URL strings directly into database clients.

For local SQLite, use `file:` URLs:

```text
file:/Users/example/.nbd/catalog.db
file:relative/catalog.db
```

`CatalogUrl` should interpret `file:` as SQLite and expose the connection
string shape required by the runtime database client. If `sqlx` needs a
`sqlite:` URL, the conversion belongs in `CatalogUrl`.

M1 should update the M0 config and test-support helpers so newly bootstrapped
operator configs and test runtimes write `file:` catalog URLs.

Prisma can consume the `file:` URL directly for SQLite migrations. A future
Postgres catalog can use `postgres://...` and route to a future Postgres
catalog implementation behind the same SDK boundary.

Unsupported schemes should fail loudly. The SDK should not silently guess a
database provider from an arbitrary URL shape.

# Database Schema V1

V1 should define `exports` and `export_generations`. Tree node/edge metadata
and lifecycle lease state land later.

`exports` owns the stable identity and lifecycle of a named disk:

Conceptual fields:

```text
exports
  id             text primary key
  name           text not null unique
  size_bytes     integer not null
  block_size     integer not null
  state          text not null
  created_at     text not null
  updated_at     text not null
  deleted_at     text null
```

`export_generations` owns the published committed-root history for an export:

```text
export_generations
  id                  text primary key
  export_id           text not null references exports(id)
  generation          integer not null
  root_node_id        text null
  checkpoint_wal_seq  integer not null
  created_at          text not null

  unique(export_id, generation)
```

Initial values for create are inserted transactionally:

```text
exports.state = active
exports.deleted_at = null

export_generations.generation = 0
export_generations.root_node_id = null
export_generations.checkpoint_wal_seq = 0
```

Use text for IDs and timestamps initially. This keeps SQLite simple and leaves
room for UUID/timestamp mapping in a future Postgres implementation.

`state` should be represented as text with values:

```text
active
deleted
```

Rust should parse state into an enum at the SDK boundary.

The latest committed root for an export is the row with the highest
`generation` in `export_generations`. Generation rows are append-only; future
compaction should publish a new generation instead of mutating an existing
generation row. This gives clone a clean future model: create a new export and
insert generation `0` copied from the source export's latest committed root.

# API Shape

Use structured request/response types.

```rust
struct CreateExport {
    name: ExportName,
    size_bytes: u64,
    block_size: u64,
}

struct DeleteExport {
    name: ExportName,
}

struct InspectExport {
    name: ExportName,
}

struct ListExports {
    include_deleted: bool,
}

struct ExportMeta {
    id: ExportId,
    name: ExportName,
    size_bytes: u64,
    block_size: u64,
    state: ExportState,
    committed: CommittedRoot,
    created_at: Timestamp,
    updated_at: Timestamp,
}

struct CommittedRoot {
    root_node_id: Option<NodeId>,
    checkpoint_wal_seq: WalSeq,
    generation: ExportGeneration,
}
```

Catalog trait:

```rust
trait ExportCatalog {
    async fn create_export(&self, request: CreateExport)
        -> Result<ExportMeta>;

    async fn delete_export(&self, request: DeleteExport)
        -> Result<()>;

    async fn load_export(&self, name: ExportName)
        -> Result<ExportMeta>;

    async fn inspect_export(&self, request: InspectExport)
        -> Result<ExportMeta>;

    async fn list_exports(&self, request: ListExports)
        -> Result<Vec<ExportMeta>>;
}
```

`load_export` is for serving/open paths and must reject deleted exports.
`inspect_export` is for operator visibility and may return deleted exports.

# CLI Shape

`nbdcli` should load config, connect to the control-plane SDK, and delegate.

Initial commands:

```text
nbdcli create <name> --size <bytes> [--block-size <bytes>]
nbdcli list [--include-deleted] [--json]
nbdcli inspect <name> [--json]
nbdcli delete <name>
```

Defaults:

```text
--block-size 4096
```

Global option:

```text
--config <path>
```

Default output should be human-readable text. `--json` should be available for
scripts and for stable CLI smoke tests that need structured output.

If `--config` is omitted, `nbdcli` uses `ConfigSource::DefaultUserPath`, which
may bootstrap `~/.nbd/config.toml`.

# Prisma Makefile Commands

M1 should put Prisma workflow wrappers under `prisma/Makefile` so database
schema/migration commands stay next to the Prisma schema and migrations.
Contributors should not need to remember raw `npx prisma ...` commands.

Expected commands:

```text
make -C prisma db-migrate
make -C prisma db-migrate-check
make -C prisma db-reset
```

`make -C prisma db-migrate` should require an explicit `DATABASE_URL` and apply
migrations to that database. Validation should use
`make -C prisma db-migrate-check`, which creates and removes a temporary
SQLite database instead of touching operator or developer catalog state.
`make -C prisma db-reset` should require both an explicit `DATABASE_URL` and
`ALLOW_DB_RESET=1` so it does not silently destroy an operator database.

# Source Of Truth

- Prisma schema/migrations are the database schema source of truth.
- `ExportCatalog` is the Rust runtime metadata boundary.
- `CatalogUrl` is the runtime interpretation boundary for `catalog.url`.
- `nbdcli` owns command parsing and output formatting only.
- Test fixtures own test database paths.

# Invariants

- Runtime catalog code parses `catalog.url` through `CatalogUrl`.
- `file:` URLs mean local SQLite.
- New local SQLite configs and test fixtures emit `file:` catalog URLs.
- Unsupported catalog URL schemes fail loudly.
- `nbdcli` does not directly issue SQL.
- SDK integration tests do not shell out to `nbdcli`.
- Deleted exports are not returned by `load_export`.
- `inspect_export` can return deleted exports for operator visibility.
- `list_exports` excludes deleted exports unless requested.
- Every export has at least one export generation.
- `size_bytes` and `block_size` must both be greater than zero.
- Create initializes generation `0` to the all-zero committed state in the same
  database transaction as the export row.
- `export_generations` rows are append-only.
- The latest committed root is the highest generation for the export.
- `checkpoint_wal_seq` belongs to a generation, not to the export row.
- `root_node_id = null` means an empty/all-zero committed disk.
- Delete is logical and does not remove rows.
- Deleted exports keep their generation history for inspect/debugging.
- M1 does not create tree node or edge metadata tables.
- M1 does not prevent open/delete races.

# Alternatives Considered

## Shell Out To `nbdcli` In Tests

This would exercise the CLI but make integration tests slower and more brittle.
The SDK should own behavior; CLI tests can separately prove argument parsing and
formatting.

## Rust ORM From Prisma

Using an unofficial Rust Prisma client would couple runtime behavior to a less
standard Rust path. Prisma stays valuable as schema/migration tooling while
Rust uses a direct database client behind `ExportCatalog`.

## Defer Lifecycle Leases

Open/delete exclusion is real architecture, but it is not important for the toy
example. Adding a SQLite lease table now would create a temporary mechanism
that does not match the eventual etcd lease model. M1 keeps delete simple and
logical; lifecycle leases are deferred until they are needed by a later server
slice.

# Migration / Rollout

This is the first catalog schema. No data migration is needed.

# Validation Strategy

Expected checks:

- `make test`
- `make fmt`
- `make clippy`
- `make -C prisma db-migrate-check`
- Prisma migration command/check for SQLite
- SDK integration tests against a temp SQLite database
- CLI smoke tests against a temp SQLite database and explicit config

High-value SDK tests:

- create export initializes expected metadata;
- duplicate create fails clearly;
- list excludes deleted exports by default;
- inspect returns active and deleted exports;
- delete marks state deleted;
- `load_export` rejects deleted exports;
- test database lives under temp runtime and is removed with the fixture.

# Risks

- Prisma migration workflow may introduce a Node/npm dependency that needs to
  be easy to run locally.
- SQLite integer handling needs explicit Rust bounds checks for `u64` fields.
- CLI output can become a test dependency too early if SDK tests shell out.
- Schema choices that are convenient in SQLite may not map cleanly to Postgres.
- `make -C prisma db-reset` could be dangerous if it is not clearly scoped to
  local/test databases.

# Open Questions

None.

# Design Exit Criteria

This design is ready for `$review-plan` when:

- the Prisma-as-migrations-only boundary is accepted;
- the `nbd-control-plane` SDK boundary is accepted;
- the `nbdcli` command shape is accepted;
- the M1 schema is accepted as exports plus append-only export generations;
- deferring open/delete race prevention is accepted.

# Recommended Next Step

Run `$review-plan` before execution planning.
