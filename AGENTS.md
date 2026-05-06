# AGENTS.md

This is the repo-local guide for future coding agents working in `nbd-server`.
It should help an agent get oriented quickly, make correct edits, and avoid
relearning project-specific pitfalls. Follow the global agent instructions too;
this file only records local facts and conventions.

## Quick Orientation

- This is a Rust workspace for an NBD server, control plane, userspace test
  client, and CLI.
- Main workspace crates:
  - `crates/nbd-server`: TCP NBD server, connection runtime, export runtime,
    admission, registry, memory engine, simple durable engine.
  - `crates/nbd-control-plane`: catalog domain model, SQLite catalog, tree
    metadata APIs.
  - `crates/nbd-config`: runtime config loading and default config generation.
  - `crates/nbd-protocol`: wire constants, parsing, and encoding.
  - `crates/nbd-us-client`: userspace validation client.
  - `crates/nbdcli`: operator/catalog CLI.
- Docker and kernel smoke support lives in `Makefile`, `docker/`, and
  `scripts/docker/`.

## Where To Look First

- Protocol or wire-format work:
  - `docs/architecture/nbd-protocol-architecture.md`
  - `crates/nbd-protocol/src/`
  - `crates/nbd-server/src/connection.rs`
  - `crates/nbd-server/tests/tcp_integration.rs`
- Runtime, queue depth, completion, or admission work:
  - `docs/plans/2026-05-03-connection-admission-concurrency.md`
  - `docs/architecture/export-admission-control.md`
  - `crates/nbd-server/src/runtime.rs`
  - `crates/nbd-server/src/admission.rs`
  - `crates/nbd-server/tests/export_runtime.rs`
  - `crates/nbd-server/tests/admission.rs`
- Catalog or schema work:
  - `docs/architecture/export-catalog-architecture.md`
  - `crates/nbd-control-plane/src/model.rs`
  - `crates/nbd-control-plane/src/sqlite.rs`
  - `crates/nbd-control-plane/tests/sqlite_catalog.rs`
  - `prisma/schema.prisma`
  - `prisma/migrations/`
- Simple durable engine work:
  - `docs/plans/2026-05-04-simple-durable-engine.md`
  - `docs/architecture/export-tree-metadata.md`
  - `docs/architecture/storage-engine-architecture.md`
  - `crates/nbd-server/src/simple_durable.rs`
  - `crates/nbd-server/tests/simple_durable.rs`
- Config work:
  - `crates/nbd-config/src/lib.rs`
  - `crates/nbd-config/tests/config_loading.rs`
- Docker/kernel smoke work:
  - `Makefile`
  - `scripts/docker/kernel-smoke.sh`

## Source Of Truth

- `docs/architecture/` contains durable architecture direction.
- `docs/plans/` contains active or historical design plans.
- `docs/execution/` contains staged implementation contracts.
- Treat active docs as the source of truth once they exist. Do not let chat-only
  updates silently replace them.
- If docs and code disagree, call out the drift and fix or revise the relevant
  doc before continuing substantial implementation.
- Historical docs are useful context, but the current active plan/execution doc
  for the effort wins.

## Current Core Model

- Catalog metadata chooses the export engine through `exports.engine_kind`.
- Server process config chooses runtime policy and queue depths.
- `LocalExportRegistry` opens exports from the catalog and constructs the
  engine/runtime pair.
- `ExportRuntime` is the connection-facing execution boundary.
- `ConcurrentExportRuntime` owns per-export queue depth and submits admitted
  work through Tokio tasks.
- `ExportAdmissionCtl` owns semantic read/write/flush admission and logical
  range ordering.
- `ExportEngine` executes only `AdmittedExportRequest`; storage access should
  not bypass admission.
- `MemoryExportEngine` relies on admission for range safety.
- `SimpleDurableEngine` stores sparse 32 MiB chunks in a local blob directory
  and stores simple mutable tree metadata in SQLite.

## Current Sharp Edges

- `ExportQueueSlot` must stay occupied until the connection reply is written to
  the socket or dropped during connection cleanup. Do not release it at engine
  completion.
- `ExportAdmissionCtl` admission order is semantic correctness. Do not replace
  it with Tokio task race order.
- Simple durable writes use chunk-aligned admission. A small write can block
  the whole 32 MiB chunk by design.
- Simple durable is direct-commit and mutable. It is not WAL-backed, COW,
  clone-ready, garbage-collected, or atomic across multi-chunk failures.
- `SimpleMutableTree` is the only request-path owner that should update simple
  tree metadata. Keep random catalog writes out of `SimpleDurableEngine`.
- The current protocol path uses one synthetic owner per connection. Same-owner
  multi-connection support is a future auth/client-identity feature.
- The server does not advertise `NBD_FLAG_CAN_MULTI_CONN`.
- `SerialExportRuntime` still exists as a baseline and test oracle even though
  concurrent runtime is the default.

## Boundary Rules

- `nbd-protocol` must not depend on server, catalog, or storage behavior.
- Connection code owns socket I/O, negotiation, request decoding, reply
  encoding, and per-connection reply serialization.
- Export runtime code owns queue slots, admission registration, request task
  lifecycle, and completion handoff.
- Admission code owns logical byte-range and flush ordering. It must not know
  blob paths, tree nodes, WAL records, or sockets.
- Engine/storage code owns data behavior after admission. It must not know NBD
  cookies, sockets, reply queues, or connection task lifecycle.
- Catalog code owns durable metadata and schema interpretation. Runtime code
  should go through catalog traits rather than direct SQL.
- Keep new policy decisions behind explicit boundaries. Avoid scattering config,
  logging, scheduling, or storage policy through request-path call sites.

## Important Commands

Use the smallest meaningful validation first, then broaden before handoff.

```text
cargo fmt --all --check
make test-protocol
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
make docker-smoke
make docker-smoke KERNEL_SMOKE_SCENARIO=memory-basic \
  KERNEL_SMOKE_OUTPUT=memory-basic
```

- `make test-protocol` runs the userspace TCP protocol integration baseline.
- `make docker-smoke` runs the privileged kernel NBD smoke path. It defaults
  to the `wal-durable-basic` scenario, verifies reattach and close compaction,
  and exports logs and inspect snapshots under `./.tmp/docker-smoke`.
- `KERNEL_SMOKE_SCENARIO=memory-basic make docker-smoke` checks the volatile
  engine path. Current scenarios are `memory-basic`, `simple-durable-basic`,
  and `wal-durable-basic`.
- `make docker-smoke KERNEL_SMOKE_OUTPUT=wal-basic` writes artifacts under
  `./.tmp/wal-basic`.
- Set `DOCKER_KERNEL_SMOKE_ARTIFACT_DIR=/path` to override the full host
  artifact path.
- `KERNEL_SMOKE_ENGINE=memory make docker-smoke` remains a compatibility
  shortcut for the matching basic scenario.
- `make build` builds `nbd-server` and `nbdcli`.
- `make -C prisma db-migrate` applies Prisma migrations to `DATABASE_URL`.
- If Docker/kernel smoke cannot run, say exactly why and what was run instead.

## Docker And Manual NBD Notes

- `make docker-kernel-shell` starts a privileged named container.
- `make docker-attach` opens another shell in that container.
- Before stopping a container used for manual NBD testing, clean up the device:

```text
umount /mnt/nbd-demo || true
nbd-client -d /dev/nbd0 || true
```

- A mounted or connected `/dev/nbd0` can wedge Docker container shutdown. Check
  the mount and device before repeatedly killing Docker.
- `mkfs.ext4` can write metadata beyond the first 32 MiB chunk. Seeing multiple
  simple durable blobs after formatting a block device is expected.

## Config And Runtime Defaults

- Default config is generated by `crates/nbd-config`.
- Default catalog path is under `~/.nbd/catalog.db`.
- Default blob directory is `~/.cache/nbd/blobs`.
- Default export runtime is concurrent.
- Tokio worker threads are fixed in the server binary with
  `#[tokio::main(flavor = "multi_thread", worker_threads = 4)]`.
- `nbdcli create` defaults to `--engine memory`; `simple_durable` is opt-in.

## Catalog And Schema

- Prisma schema and migrations live under `prisma/`.
- The active head table is `export_heads`; do not reintroduce the old
  `export_generations` model.
- Simple durable uses `layout_kind = simple_mutable_tree`.
- Future WAL/COW work should use a different layout meaning rather than
  reinterpret simple mutable rows as immutable history.
- Catalog schema changes need focused tests in
  `crates/nbd-control-plane/tests/sqlite_catalog.rs`.

## Testing Guidance

- Prefer integration tests for protocol-visible behavior.
- Prefer unit tests for small primitives such as admission, range validation,
  config parsing, and catalog domain types.
- When changing runtime/admission behavior, include tests that prove queue-slot
  lifetime, admission ordering, shutdown, or reply handoff as appropriate.
- When changing durable storage, test both the metadata path and restart or
  reload behavior.
- Do not make tests assert incidental log lines, internal task polling order, or
  exhaustive implementation detail unless that is the contract being added.

## Documentation Workflow

- Docs under `docs/` should wrap prose around 80 columns.
- New design docs go under `docs/plans/YYYY-MM-DD-topic.md`.
- Multi-series execution docs go under `docs/execution/YYYY-MM-DD-topic.md`.
- Each durable doc should include `Title`, `Date`, and `Status`.
- `Status: draft` means not ready for implementation unless the user explicitly
  asks for a prototype.
- `Status: approved` means ready for review/series planning.
- Update docs in the same series as behavior changes that alter config,
  commands, runtime contracts, catalog semantics, or storage invariants.

## Git And Scratch Space

- Always inspect `git status --short` before editing.
- Preserve unrelated changes and untracked files.
- Use `./.tmp/` for repo-local scratch files.
- Avoid `/private/tmp` for project scratch unless a tool specifically requires
  it.
- Commit messages should use subsystem prefixes and `git commit -F` for bodies.
- Do not use `--no-verify`.

## Parallel Work Note

- Parallel agents should use separate branches or worktrees and avoid
  overlapping write sets.
- If two efforts need the same file, coordinate the order rather than racing
  edits.
- Never assume an untracked or modified file belongs to you.

## Review Before Handoff

- Check the active design/execution doc still matches the result.
- Confirm the changed boundary stayed contained.
- Run relevant formatter/tests/lints or report why not.
- Summarize remaining risks plainly.
- Keep final responses concise but include the commands actually run.

# Project Agents

See `.rust-skills/AGENTS.md` for Rust development guidelines.
