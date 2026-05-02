Title: Initial Integration Execution
Date: 2026-05-01
Status: approved
Approval:
- overall doc approved: yes
- current state: Series 1 approved
Completion:
- execution complete: no

## Goal

Execute the first NBD server integration checkpoints from the approved initial
integration designs.

The target is a sequence of small, provable Rust slices:

- workspace, config, and temporary test harness;
- SQLite catalog schema, control-plane SDK, and `nbdcli`;
- NBD protocol framing and mock client;
- toy in-memory server integrated with the catalog;
- later Docker/kernel-NBD smoke validation.

## Roadmap Context

This execution plan follows:

- `docs/roadmaps/2026-05-01-initial-integration-roadmap.md`

The first shippable vertical slice is:

```text
temp config + temp SQLite DB
  -> SDK creates export
  -> toy NBD server opens export metadata
  -> mock NBD client writes, reads, flushes, disconnects
```

Docker and kernel-NBD validation stay behind the mock-client proof path.

## Design Inputs

- `docs/plans/initial-integration/2026-05-01-rust-workspace-testing.md`
- `docs/plans/initial-integration/2026-05-01-catalog-sdk-v1.md`
- `docs/plans/initial-integration/2026-05-01-toy-nbd-server.md`

## Why Split

This effort spans three approved design docs and a later Docker smoke design.
It also has natural stable checkpoints: runtime/test foundation, catalog
metadata, protocol framing, server integration, and privileged smoke testing.

Each series should leave the repository buildable and should prove the
boundary it introduces. Future series should not reopen earlier architecture
unless implementation exposes a real mismatch.

## Series 1: Workspace, Config, And Test Harness

Depends on: none

Roadmap milestone: M0

Design coverage:
`docs/plans/initial-integration/2026-05-01-rust-workspace-testing.md`

Stable checkpoint: the Rust workspace builds; config loading has explicit and
default-user paths; tests can create isolated runtime state and temp SQLite
URLs; root `make` commands work.

Review focus: crate boundaries, config source-of-truth rules, and test
isolation.

Done means: `cargo test --workspace`, formatter check, clippy, and the root
Makefile targets all work. Tests prove that explicit config paths do not use
developer `~/.nbd` state.

Approval: approved

Verification plan:

```text
cargo test --workspace
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
make test
make fmt
make clippy
```

Not included: Prisma schema, catalog migrations, `nbdcli`, NBD protocol,
server sockets, WAL, storage, compaction, or Docker.

Commit 1/6: docs/plans: add initial integration designs

  Type:             docs
  Required:         yes
  Summary:          Add the roadmap and approved design docs that define the
                    initial implementation checkpoints.
  Invariant focus:  Architecture and execution planning have a committed
                    source of truth before code changes begin.
  Test level:       none
  Review gate:      structures
  Files:            docs/roadmaps/2026-05-01-initial-integration-roadmap.md
                    docs/plans/initial-integration/2026-05-01-rust-workspace-testing.md
                    docs/plans/initial-integration/2026-05-01-catalog-sdk-v1.md
                    docs/plans/initial-integration/2026-05-01-toy-nbd-server.md
  Preconditions:    Long-term architecture docs are already committed.
  Postconditions:   The approved initial integration roadmap and design inputs
                    are present in the repository.
  Verify:           git diff --cached --check
  Risks:            Low; this is a planning-only commit.
  Not included:     Execution-series approval or implementation code.
  Depends on:       none

Commit 2/6: docs/execution: add initial integration execution plan

  Type:             docs
  Required:         yes
  Summary:          Add the execution artifact that splits the approved design
                    work into stable implementation series.
  Invariant focus:  Execution state is tracked separately from architecture
                    intent.
  Test level:       none
  Review gate:      structures
  Files:            docs/execution/2026-05-01-initial-integration.md
  Preconditions:    Commit 1 has landed the approved design inputs.
  Postconditions:   Series boundaries, checkpoints, approval state, and the
                    Series 1 commit contract are explicit.
  Verify:           git diff --cached --check
  Risks:            Low; the main risk is overplanning future series.
  Not included:     Any code, schema, CLI, or protocol implementation.
  Depends on:       1

Commit 3/6: workspace: initialize Rust crates

  Type:             preparatory
  Required:         yes
  Summary:          Add the root Cargo workspace, initial crate skeletons, and
                    target ignore rules for build artifacts.
  Invariant focus:  Only crates with an M0 contract exist in the workspace.
  Test level:       functional
  Review gate:      structures
  Files:            .gitignore
                    Cargo.lock
                    Cargo.toml
                    crates/nbd-config/Cargo.toml
                    crates/nbd-config/src/lib.rs
                    crates/nbd-test-support/Cargo.toml
                    crates/nbd-test-support/src/lib.rs
  Preconditions:    Commit 2 has landed the execution plan.
  Postconditions:   The workspace builds with `nbd-config` and
                    `nbd-test-support` as the only initial crates.
  Verify:           cargo test --workspace
  Risks:            Low; keep crate APIs minimal and avoid dormant future
                    plumbing.
  Not included:     Config parsing, temp fixtures, catalog code, or protocol
                    code.
  Depends on:       2

Commit 4/6: config: add runtime config loading

  Type:             semantic
  Required:         yes
  Summary:          Implement structured config loading for explicit paths and
                    default-user bootstrap behavior.
  Invariant focus:  Runtime config is explicit after startup, and default
                    bootstrap is the only path that writes under user state.
  Test level:       unit
  Review gate:      code
  Files:            Cargo.lock
                    crates/nbd-config/Cargo.toml
                    crates/nbd-config/src/lib.rs
                    crates/nbd-config/tests/config_loading.rs
  Preconditions:    Commit 3 has created the workspace and `nbd-config` crate.
  Postconditions:   Explicit config files load without touching `~/.nbd`; the
                    default-user path can create an absolute-path TOML config.
  Verify:           cargo test -p nbd-config
  Risks:            Path expansion and home-directory tests must not depend on
                    the developer's real home directory.
  Not included:     Database schema creation or test runtime fixture helpers.
  Depends on:       3

Commit 5/6: test-support: add isolated runtime fixture

  Type:             semantic
  Required:         yes
  Summary:          Add a reusable temporary runtime fixture for integration
                    tests that need isolated config and SQLite paths.
  Invariant focus:  Test-owned config, state, and catalog paths stay under the
                    fixture root and are removed when the fixture is dropped.
  Test level:       integration
  Review gate:      code
  Files:            Cargo.lock
                    crates/nbd-test-support/Cargo.toml
                    crates/nbd-test-support/src/lib.rs
                    crates/nbd-test-support/tests/runtime_fixture.rs
  Preconditions:    Commit 4 has landed the public `nbd-config` API.
  Postconditions:   Tests can construct `TestRuntime`, load its explicit
                    config, inspect its SQLite URL, and observe cleanup.
  Verify:           cargo test -p nbd-test-support
  Risks:            Cleanup assertions should avoid OS-specific timing or open
                    handle assumptions.
  Not included:     Applying Prisma migrations or creating schema tables.
  Depends on:       4

Commit 6/6: build: add local Makefile targets

  Type:             semantic
  Required:         yes
  Summary:          Add root Makefile commands for the normal local test,
                    format, and clippy loop.
  Invariant focus:  The documented developer entry points run the same checks
                    as the direct cargo commands.
  Test level:       functional
  Review gate:      none
  Files:            Makefile
  Preconditions:    Commit 5 has landed the M0 crates and tests.
  Postconditions:   `make test`, `make fmt`, and `make clippy` work from the
                    repository root.
  Verify:           make test
                    make fmt
                    make clippy
  Risks:            Low; keep targets thin wrappers around cargo commands.
  Not included:     Docker commands or Prisma Makefile commands.
  Depends on:       5

## Series 2: Catalog SDK And `nbdcli`

Depends on: Series 1

Roadmap milestone: M1

Design coverage:
`docs/plans/initial-integration/2026-05-01-catalog-sdk-v1.md`

Stable checkpoint: Prisma creates the SQLite `exports` schema; the Rust SDK
and `nbdcli` can create, list, inspect, and logically delete exports against a
temp database.

Review focus: schema shape, SQL/runtime boundary, SDK ownership, and CLI as a
thin wrapper.

Done means: SDK integration tests use temp databases and do not shell out to
the CLI; CLI smoke tests use explicit config and structured output where
needed.

Approval: pending

Verification plan:

```text
make test
make fmt
make clippy
make -C prisma db-migrate
```

Not included: real leases, open/delete race prevention, tree metadata, clone,
or NBD server open paths.

## Series 3: NBD Protocol And Mock Client

Depends on: Series 2

Roadmap milestone: M2

Design coverage:
`docs/plans/initial-integration/2026-05-01-toy-nbd-server.md`

Stable checkpoint: `nbd-protocol` can encode/decode the fixed-newstyle
handshake, `NBD_OPT_GO`, `NBD_OPT_ABORT`, and read/write/flush/disconnect
command framing. The mock client exercises real TCP framing helpers without
depending on server internals.

Review focus: protocol constants, endian handling, error mapping, and keeping
protocol code independent of catalog/server crates.

Done means: protocol unit tests and mock-client framing tests pass without a
kernel NBD client.

Approval: pending

Verification plan:

```text
make test
make fmt
make clippy
```

Not included: listener lifecycle, catalog export opening, persistence,
concurrency, workqueues, or Docker.

## Series 4: Toy Server And Catalog Integration

Depends on: Series 3

Roadmap milestone: M3

Design coverage:
`docs/plans/initial-integration/2026-05-01-toy-nbd-server.md`

Stable checkpoint: a test creates export metadata through
`nbd-control-plane`, starts `nbd-server` on `127.0.0.1:0`, negotiates with the
mock client, and proves read zeroes, write/readback, flush, disconnect, and
missing/deleted export failures.

Review focus: server lifecycle, catalog open path, toy `MemoryExport`
semantics, and honest non-durability.

Done means: the first vertical slice passes through temp config, temp SQLite,
SDK-created export metadata, toy server, and mock TCP client.

Approval: pending

Verification plan:

```text
make test
make fmt
make clippy
```

Not included: WAL, `ExportReadView`, storage engine, compaction, admission
control, concurrent request execution, or kernel NBD.

## Series 5: Docker And Kernel-NBD Smoke

Depends on: Series 4 and a future approved Docker/kernel smoke design

Roadmap milestone: M4

Design coverage: pending future design doc

Stable checkpoint: a privileged Linux container can run the toy server and a
real NBD client can perform basic I/O.

Review focus: privilege boundaries, device cleanup, Makefile ergonomics, and
keeping Docker smoke outside the normal inner-loop proof.

Done means: manual or ignored smoke commands are documented and runnable in the
intended Linux/Docker environment.

Approval: pending

Verification plan:

```text
make docker-build
make docker-smoke
```

Not included: this series is not approved until its design doc exists.
