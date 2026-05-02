Title: Initial Integration Roadmap
Date: 2026-05-01
Status: draft

# Product Input

Not needed. This roadmap is for the first implementation checkpoints after the
architecture discussion.

# Objective

Build the first NBD server slices in Rust through small, provable integration
checkpoints:

- a SQLite-backed control plane with Prisma-managed schema/migrations;
- a Rust control-plane SDK used by both `nbdcli` and tests;
- a toy in-memory NBD server tested through a userspace validation client;
- a Docker/kernel-NBD smoke path only after userspace TCP integration tests
  pass.

# Scope And Assumptions

Runtime code should be Rust. Prisma is used for schema and migrations, not as
the Rust runtime ORM. The Rust runtime database implementation should sit
behind `ExportCatalog` / `ExportLifecycleManager`, with SQLite first and a
future Postgres implementation possible behind the same traits.

Integration tests must not use the developer's default database or config.
Every test should create its own temporary database and remove it with the temp
directory. Tests should be able to configure the catalog path explicitly rather
than depending on `~/.nbd`.

The default operator config can live under:

```text
~/.nbd/config.toml
```

Tests should override that with an explicit config path, temp home, or direct
SDK configuration.

# Components / Capability Areas

- Rust workspace and crate layout
- Prisma schema and migration workflow
- `nbd-control-plane` Rust SDK
- `nbdcli` CLI wrapper
- config loading and path resolution
- temporary test database harness
- root `Makefile` for local developer commands
- toy in-memory `Export` implementation
- NBD protocol server and userspace validation client
- Docker/kernel-NBD smoke test path

# Slice Matrix

- `workspace` foundation v1:
  crates build and share errors/config types.
- `config` foundation v1:
  load explicit config and default `~/.nbd/config.toml`.
- `migrations` foundation v1:
  define export catalog schema and SQLite migrations with Prisma.
- `nbd-control-plane` component v1:
  create/list/inspect/delete exports without shelling out.
- `nbdcli` component v1:
  thin CLI over the SDK.
- `catalog harness` test v1:
  each integration test owns and deletes its database.
- `Makefile` foundation v1:
  provide `make test`, `make fmt`, and `make clippy`.
- `NBD protocol` component v1:
  fixed newstyle, `GO`, `ABORT`, read, write, flush, and disconnect.
- `toy export` component v1:
  in-memory byte vector, no WAL/read-view/storage.
- `userspace validation client` test v1:
  exercise real TCP protocol from tests.
- `toy server` integration v1:
  create export, serve it, write/read/flush.
- `Docker image` operational v1:
  run server in a Linux container.
- `kernel NBD` operational v1:
  Linux kernel NBD tooling connects and basic I/O works as manual/ignored
  proof.

# Milestone Map

- M0:
  workspace, config, temp database harness, and local Makefile commands.
  Exit when tests can create isolated runtime config and temp SQLite DBs, and
  `make test` works.
- M1:
  Prisma schema, migrations, SDK, and `nbdcli`.
  Exit when SDK and CLI can create/list/inspect/delete exports in SQLite.
- M2:
  protocol, toy export, and userspace validation client.
  Exit when a small Rust validation client proves handshake/read/write/flush
  /disc over TCP.
- M3:
  toy server plus control plane.
  Exit when a test creates an export through the SDK, starts the server, and
  verifies I/O through the validation client.
- M4:
  Docker image and kernel NBD smoke.
  Exit when a privileged container can serve an export to a real NBD client.

# Dependencies

- `nbdcli` v1 depends on `nbd-control-plane` v1:
  CLI should be a thin wrapper, not the source of behavior.
- SDK integration tests depend on the temp DB harness:
  tests must not leave local database artifacts.
- Toy server catalog open depends on SDK/catalog v1:
  the server needs export size/name metadata before serving.
- Userspace validation client tests depend on protocol v1:
  tests should exercise real wire framing.
- Docker smoke depends on userspace TCP integration passing:
  kernel testing should validate packaging, not debug basic protocol.

# Parity / Migration Requirements

Schema design should stay in the common SQLite/Postgres subset where practical:

- avoid SQLite-only behavior in catalog invariants;
- keep timestamps, enum-like states, and IDs explicit;
- keep database-specific SQL behind migrations and catalog implementations;
- treat Postgres support as a future `ExportCatalog` implementation, not a
  data-path change.

# First Shippable Slice

The first useful vertical slice is:

```text
temp config + temp SQLite DB
  -> SDK creates export
  -> toy NBD server opens export metadata
  -> userspace validation client writes, reads, flushes, disconnects
```

This proves the control plane and protocol boundary without WAL, `ReadView`,
S3, compaction, or Docker.

# Design-Doc Backlog

- Initial Rust workspace and test harness:
  dedicated design doc required.
  Suggested path:
  `docs/plans/initial-integration/2026-05-01-rust-workspace-testing.md`.
  This should set crate boundaries, config override rules, temp DB cleanup,
  and test commands.
- Catalog schema and control-plane SDK:
  dedicated design doc required.
  Suggested path:
  `docs/plans/initial-integration/2026-05-01-catalog-sdk-v1.md`.
  This should define Prisma schema, migration workflow, SDK API, and CLI
  behavior.
- Toy NBD protocol server:
  dedicated design doc required.
  Suggested path:
  `docs/plans/initial-integration/2026-05-01-toy-nbd-server.md`.
  This should define userspace validation client coverage, protocol subset,
  in-memory export semantics, and server lifecycle.
- Docker/kernel NBD smoke:
  dedicated design doc exists.
  Path:
  `docs/plans/initial-integration/2026-05-02-docker-kernel-smoke.md`.
  This needs Linux privileges, device handling, and manual/ignored test
  policy.

# Risk Hotspots

- Prisma is a schema/migration tool here, not the Rust runtime API.
- Tests must never silently use `~/.nbd` or a developer database.
- The validation client must use real TCP framing instead of calling server
  internals.
- The first server should stay toy-like; WAL and `ExportReadView` come later.
- Docker/kernel NBD tests should not become the normal inner-loop proof.

# What Not To Design Yet

- WAL format and replay
- `ExportReadView`
- S3/MinIO storage
- compaction/tree metadata implementation
- writer fencing
- authenticated multi-connection support
- advanced admission/range-lock policy

# Recommended Next Design Tasks

1. Design the initial Rust workspace, config model, and test harness.
2. Design the Prisma schema plus `nbd-control-plane` SDK / `nbdcli` boundary.
3. Design the toy NBD server and validation client integration contract.

# Roadmap Exit Criteria

This roadmap is ready when:

- the SDK-first `nbdcli` direction is accepted;
- test database isolation is accepted as mandatory;
- `~/.nbd/config.toml` is accepted as the operator default, not a test default;
- Docker/kernel NBD is treated as a later smoke test; and
- the first vertical slice can be designed without adding WAL or storage.
