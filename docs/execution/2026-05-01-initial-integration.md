Title: Initial Integration Execution
Date: 2026-05-01
Status: completed
Approval:
- overall doc approved: yes
- current state: Series 5 and runtime/registry follow-up finished; initial
  integration complete
Completion:
- execution complete: yes

## Goal

Execute the first NBD server integration checkpoints from the approved initial
integration designs.

The target is a sequence of small, provable Rust slices:

- workspace, config, and temporary test harness;
- SQLite catalog schema, control-plane SDK, and `nbdcli`;
- NBD protocol framing and userspace validation client;
- in-memory NBD server integrated with the catalog;
- later Docker/kernel-NBD smoke validation.

## Roadmap Context

This execution plan follows:

- `docs/roadmaps/2026-05-01-initial-integration-roadmap.md`

The first shippable vertical slice is:

```text
temp config + temp SQLite DB
  -> SDK creates export
  -> NBD server opens export metadata
  -> userspace validation client writes, reads, flushes, disconnects
```

Docker and kernel-NBD validation stay behind the userspace TCP proof path.

## Design Inputs

- `docs/plans/initial-integration/2026-05-01-rust-workspace-testing.md`
- `docs/plans/initial-integration/2026-05-01-catalog-sdk-v1.md`
- `docs/plans/initial-integration/2026-05-01-in-memory-nbd-server.md`
- `docs/plans/initial-integration/2026-05-02-docker-kernel-smoke.md`
- `docs/plans/initial-integration/2026-05-02-export-runtime-registry.md`

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

Approval: finished

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
                    docs/plans/initial-integration/2026-05-01-in-memory-nbd-server.md
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

Stable checkpoint: Prisma creates the SQLite `exports` and
`export_generations` schema; the Rust SDK and `nbdcli` can create, list,
inspect, and logically delete exports against a temp database.

Review focus: schema shape, SQL/runtime boundary, SDK ownership, and CLI as a
thin wrapper.

Done means: SDK integration tests use temp databases and do not shell out to
the CLI; CLI smoke tests use explicit config and structured output where
needed.

Approval: finished

Verification plan:

```text
make test
make fmt
make clippy
make -C prisma db-migrate-check
```

Not included: real leases, open/delete race prevention, tree metadata, clone,
or NBD server open paths.

Commit 1/6: docs/execution: plan catalog SDK series

  Type:             docs
  Required:         yes
  Summary:          Record the Series 2 commit contract now that the M0
                    workspace checkpoint is finished.
  Invariant focus:  Catalog execution work has an approved source of truth
                    before schema, SDK, or CLI changes begin.
  Test level:       none
  Review gate:      structures
  Files:            docs/execution/2026-05-01-initial-integration.md
                    docs/plans/initial-integration/2026-05-01-catalog-sdk-v1.md
  Preconditions:    Series 1 is finished and the M1 catalog SDK design is
                    approved.
  Postconditions:   Series 2 has explicit commit boundaries, verification
                    commands, review gates, and deferred scope.
  Verify:           git diff --cached --check
  Risks:            Low; this is a planning-only commit.
  Not included:     Prisma schema, Rust SDK code, CLI code, or migrations.
  Depends on:       none

Commit 2/6: config: make catalog file URLs canonical

  Type:             semantic
  Required:         yes
  Summary:          Update config and test-support helpers so local SQLite
                    catalog URLs use the canonical `file:` form.
  Invariant focus:  New local SQLite configs and test fixtures emit catalog
                    URLs that Prisma can consume directly.
  Test level:       integration
  Review gate:      code
  Files:            crates/nbd-config/src/lib.rs
                    crates/nbd-config/tests/config_loading.rs
                    crates/nbd-test-support/src/lib.rs
                    crates/nbd-test-support/tests/runtime_fixture.rs
  Preconditions:    Commit 1 has recorded the Series 2 execution contract.
  Postconditions:   Default config bootstrap and `TestRuntime` produce `file:`
                    catalog URLs for local SQLite paths.
  Verify:           cargo test -p nbd-config
                    cargo test -p nbd-test-support
  Risks:            Existing M0 code used `sqlite:` URLs; tests must prove the
                    new canonical local format is used consistently.
  Not included:     Prisma schema, Rust catalog APIs, `nbdcli`, or database
                    connections.
  Depends on:       1

Commit 3/6: prisma: add export catalog schema

  Type:             semantic
  Required:         yes
  Summary:          Add the Prisma schema, initial SQLite migration, and
                    migration Makefile for the V1 catalog tables.
  Invariant focus:  Prisma schema and migrations are the database schema source
                    of truth.
  Test level:       integration
  Review gate:      migration
  Files:            prisma/schema.prisma
                    prisma/migrations/20260501000000_init/migration.sql
                    prisma/Makefile
  Preconditions:    Commit 2 has made `file:` URLs canonical for local SQLite
                    configs.
  Postconditions:   Prisma can create the V1 `exports` and
                    `export_generations` tables from a `file:` SQLite URL,
                    with active/deleted export state, append-only committed
                    root generations, and no tree node/edge tables.
  Verify:           make -C prisma db-migrate-check
  Risks:            Prisma consumes `file:` URLs directly; Rust runtime code
                    must still go through `CatalogUrl` before opening a
                    database connection.
  Not included:     Rust catalog APIs, `nbdcli`, tree node/edge metadata,
                    leases, or clone support.
  Depends on:       2

Commit 4/6: catalog: add control-plane API

  Type:             preparatory
  Required:         yes
  Summary:          Add the `nbd-control-plane` crate with `CatalogUrl`, export
                    metadata types, request/response structs, errors, and the
                    `ExportCatalog` trait.
  Invariant focus:  The SDK boundary owns catalog semantics and catalog URL
                    interpretation; callers do not construct raw SQL or depend
                    on Prisma runtime clients.
  Test level:       unit
  Review gate:      structures
  Files:            Cargo.lock
                    Cargo.toml
                    crates/nbd-control-plane/Cargo.toml
                    crates/nbd-control-plane/src/lib.rs
                    crates/nbd-control-plane/src/catalog_url.rs
                    crates/nbd-control-plane/src/error.rs
                    crates/nbd-control-plane/src/model.rs
                    crates/nbd-control-plane/tests/model.rs
  Preconditions:    Commit 3 has landed the schema shape the API represents.
  Postconditions:   The control-plane crate builds, parses `file:` catalog URLs
                    as SQLite, validates basic domain values, represents the
                    latest committed generation in `ExportMeta`, and exposes
                    the catalog API without an SQLite implementation.
  Verify:           cargo test -p nbd-control-plane
  Risks:            The API should stay narrow enough for M1 while leaving room
                    for clone, lifecycle leases, tree metadata, and explicit
                    generation history operations later.
  Not included:     SQLite queries, migrations, CLI commands, or server open
                    paths.
  Depends on:       3

Commit 5/6: catalog: implement SQLite exports

  Type:             semantic
  Required:         yes
  Summary:          Implement `SQLiteExportCatalog` with create, list, inspect,
                    load, and logical delete behavior.
  Invariant focus:  `ExportCatalog` is the runtime metadata boundary, and
                    deleted exports are never returned by `load_export`.
  Test level:       integration
  Review gate:      code
  Files:            Cargo.lock
                    crates/nbd-control-plane/Cargo.toml
                    crates/nbd-control-plane/src/lib.rs
                    crates/nbd-control-plane/src/sqlite.rs
                    crates/nbd-control-plane/tests/sqlite_catalog.rs
  Preconditions:    Commit 4 has landed the catalog API and Commit 3 has landed
                    the migration SQL used by tests.
  Postconditions:   SDK tests create a temp SQLite database through
                    `CatalogUrl`, apply the export migration, and prove
                    create/list/inspect/delete/load semantics including the
                    transactional generation `0` row.
  Verify:           cargo test -p nbd-control-plane
  Risks:            SQLite integer values need explicit range handling before
                    mapping into Rust `u64` domain types.
  Not included:     `nbdcli`, real leases, open/delete race prevention, clone,
                    or tree metadata.
  Depends on:       4

Commit 6/6: nbdcli: add catalog commands

  Type:             semantic
  Required:         yes
  Summary:          Add the `nbdcli` binary as a thin wrapper over
                    `nbd-control-plane` for create, list, inspect, and delete.
  Invariant focus:  CLI code owns argument parsing and output formatting only;
                    catalog behavior remains in the SDK.
  Test level:       integration
  Review gate:      code
  Files:            Cargo.lock
                    Cargo.toml
                    crates/nbdcli/Cargo.toml
                    crates/nbdcli/src/main.rs
                    crates/nbdcli/tests/cli.rs
  Preconditions:    Commit 5 has landed the SQLite catalog implementation.
  Postconditions:   CLI smoke tests parse explicit temp config through
                    `CatalogUrl` and use temp SQLite catalogs to create, list,
                    inspect, and delete exports.
  Verify:           cargo test -p nbdcli
                    make test
                    make fmt
                    make clippy
  Risks:            CLI tests should not become the only proof of SDK behavior
                    and must not use the developer default config.
  Not included:     Shelling out from SDK tests, NBD server open paths, real
                    leases, clone, or tree metadata.
  Depends on:       5

## Series 3: NBD Protocol

Depends on: Series 2

Roadmap milestone: M2 protocol sub-slice

Design coverage:
`docs/plans/initial-integration/2026-05-01-in-memory-nbd-server.md`

Stable checkpoint: `nbd-protocol` can encode/decode the fixed-newstyle
handshake, `NBD_OPT_GO`, `NBD_OPT_ABORT`, and read/write/flush/disconnect
command framing against byte fixtures. No real TCP peer is introduced in this
series.

Review focus: protocol constants, endian handling, error mapping, public API
shape, and keeping protocol code independent of catalog/server crates.

Done means: boundary-style protocol fixture tests prove the supported
handshake, option, and transmission wire shapes through the public
`nbd-protocol` API without a TCP validation client, server, or kernel NBD
client.

Approval: finished

Verification plan:

```text
make test
make fmt
make clippy
```

Not included: listener lifecycle, catalog export opening, persistence,
concurrency, workqueues, validation-client TCP behavior, scripted NBD peers, or
Docker.

Initial protocol policy: use a small explicit maximum write payload constant;
reject unsupported nonzero command flags; reject unknown client flags except
`NBD_FLAG_C_NO_ZEROES`, which is accepted and ignored because Series 3 does not
write trailing handshake zeroes after client flags. Zero-length read/write
requests are rejected as invalid for the in-memory protocol path; flush and
disconnect have no payload.

Commit 1/5: docs/execution: plan NBD protocol series

  Type:             docs
  Required:         yes
  Summary:          Record the narrowed Series 3 contract after deciding that
                    the first real TCP validation proof belongs with the
                    in-memory server in Series 4.
  Invariant focus:  The execution source of truth separates protocol byte-layout
                    correctness from server lifecycle and export behavior.
  Test level:       none
  Review gate:      structures
  Files:            docs/execution/2026-05-01-initial-integration.md
  Preconditions:    Series 2 is finished and the in-memory NBD server design
                    remains approved for the broader M2/M3 slice.
  Postconditions:   Series 3 has explicit protocol-only commit boundaries,
                    verification commands, and deferred validation-client/server
                    scope.
  Verify:           git diff --cached --check
  Risks:            Low; this is a planning-only commit, but it must not
                    silently redesign the approved in-memory server
                    architecture.
  Not included:     Protocol implementation, validation client, server
                    lifecycle, catalog opening, or MemoryExport.
  Depends on:       none

Commit 2/5: protocol: add NBD wire crate

  Type:             preparatory
  Required:         yes
  Summary:          Add the nbd-protocol crate with public wire constants,
                    endian helpers, small typed wrappers, and the first
                    boundary-style fixture test through the public API.
  Invariant focus:  Protocol constants and primitive wire types live in a crate
                    that has no catalog or server dependencies.
  Test level:       integration
  Review gate:      structures
  Files:            Cargo.lock
                    Cargo.toml
                    crates/nbd-protocol/Cargo.toml (new)
                    crates/nbd-protocol/src/lib.rs (new)
                    crates/nbd-protocol/src/constants.rs (new)
                    crates/nbd-protocol/src/error.rs (new)
                    crates/nbd-protocol/src/wire.rs (new)
                    crates/nbd-protocol/tests/protocol_fixtures.rs (new)
  Preconditions:    Commit 1 has recorded the Series 3 protocol-only boundary.
  Postconditions:   The workspace builds with nbd-protocol, the crate exposes
                    only protocol-level primitives, and the fixture test proves
                    basic big-endian integer layout and known magic values.
  Verify:           cargo test -p nbd-protocol --test protocol_fixtures
  Risks:            Constants copied incorrectly from the public protocol would
                    poison later parsing; keep this commit small and
                    fixture-driven.
  Not included:     Handshake parsing, option negotiation, transmission request
                    parsing, validation client helpers, server code, or catalog
                    integration.
  Depends on:       1

Commit 3/5: protocol: implement handshake framing

  Type:             semantic
  Required:         yes
  Summary:          Implement fixed-newstyle server handshake encoding and
                    client-flag decoding for the supported handshake path,
                    extending the public fixture coverage.
  Invariant focus:  Handshake code accepts only fixed-newstyle client
                    negotiation and rejects unsupported client flags explicitly.
  Test level:       integration
  Review gate:      code
  Files:            crates/nbd-protocol/src/lib.rs
                    crates/nbd-protocol/src/handshake.rs (new)
                    crates/nbd-protocol/src/error.rs
                    crates/nbd-protocol/tests/protocol_fixtures.rs
  Preconditions:    Commit 2 has introduced constants, errors, and primitive
                    wire helpers.
  Postconditions:   The fixture test can encode the server initial handshake,
                    decode valid fixed-newstyle client flags, accept
                    `NBD_FLAG_C_NO_ZEROES`, and reject missing or unknown
                    client flags without any server crate.
  Verify:           cargo test -p nbd-protocol --test protocol_fixtures
  Risks:            The NBD handshake is public wire protocol; byte order, magic
                    values, and flag rejection need direct fixture coverage.
  Not included:     Option negotiation, transmission commands,
                    validation-client TCP behavior, or export metadata.
  Depends on:       2

Commit 4/5: protocol: implement option negotiation framing

  Type:             semantic
  Required:         yes
  Summary:          Implement fixed-newstyle option request parsing and option
                    reply encoding for NBD_OPT_GO, NBD_OPT_ABORT, export info,
                    ACK, and unsupported-option errors, extending the same
                    fixture file.
  Invariant focus:  NBD_OPT_GO wire handling can represent export name,
                    requested info ids, export size, transmission flags, and
                    final ACK without opening an export.
  Test level:       integration
  Review gate:      code
  Files:            crates/nbd-protocol/src/lib.rs
                    crates/nbd-protocol/src/option.rs (new)
                    crates/nbd-protocol/src/error.rs
                    crates/nbd-protocol/tests/protocol_fixtures.rs
  Preconditions:    Commit 3 has established fixed-newstyle handshake primitives
                    and shared wire helpers.
  Postconditions:   The fixture test parses GO and ABORT option requests,
                    encodes export-info and ACK replies, and encodes
                    unsupported-option errors with the original option code.
  Verify:           cargo test -p nbd-protocol --test protocol_fixtures
  Risks:            GO payload layout is easy to blur with server policy; keep
                    this commit limited to syntax and reply framing.
  Not included:     Catalog lookup, missing/deleted export policy, transmission
                    request parsing, or standalone NBD_OPT_INFO.
  Depends on:       3

Commit 5/5: protocol: implement transmission framing

  Type:             semantic
  Required:         yes
  Summary:          Implement transmission request parsing, write payload
                    sizing, simple reply encoding, read payload replies, and
                    protocol-level validation for the supported commands.
  Invariant focus:  READ, WRITE, FLUSH, and DISC are represented as typed
                    protocol requests with cookies preserved and unsupported
                    flags rejected before server logic exists.
  Test level:       integration
  Review gate:      code
  Files:            crates/nbd-protocol/src/lib.rs
                    crates/nbd-protocol/src/transmission.rs (new)
                    crates/nbd-protocol/src/error.rs
                    crates/nbd-protocol/tests/protocol_fixtures.rs
  Preconditions:    Commit 4 has landed option negotiation framing and shared
                    error handling.
  Postconditions:   The fixture test has one happy-path protocol script and one
                    invalid-input table covering bad magic, unsupported command
                    flags, zero-length read/write, length overflow, oversized
                    payloads, and simple replies with matching cookies.
  Verify:           cargo test -p nbd-protocol --test protocol_fixtures
                    cargo test -p nbd-protocol
                    make test
                    make fmt
                    make clippy
  Risks:            Transmission parsing determines later socket behavior; keep
                    the fixture coverage holistic and avoid adding overlapping
                    microtests that mostly restate implementation details.
  Not included:     A validation client, scripted peer, listener lifecycle,
                    MemoryExport, catalog opening, concurrency, or kernel NBD
                    validation.
  Depends on:       4

## Series 4: In-Memory Server, Validation Client, And Catalog Integration

Depends on: Series 3

Roadmap milestone: M2 userspace-validation/in-memory-export completion and M3
server/catalog integration

Design coverage:
`docs/plans/initial-integration/2026-05-01-in-memory-nbd-server.md`

Stable checkpoint: a test creates export metadata through `nbd-control-plane`,
starts the NBD server on `127.0.0.1:0`, connects with the small userspace
validation client, negotiates `NBD_OPT_GO`, and proves read zeroes,
write/readback, flush, disconnect, independent export contents, and
missing/deleted export failures.

Review focus: server lifecycle, validation-client/server TCP boundary, catalog
open path, `MemoryExport` semantics, and honest non-durability.

Done means: the first vertical slice passes through temp config, temp SQLite,
SDK-created export metadata, NBD server, and userspace validation client.

Approval: finished

Closeout: Series 4 completed the userspace TCP in-memory server checkpoint. The
final reviewed stack includes the review-fix commit that bounds wire
allocations before buffers are allocated and keeps transmission handling behind
the `Export` boundary. `$review-series` verdict was acceptable after those
fixes, and `$polish-series` normalized the local commit history without
changing final content.

Verification plan:

```text
make test
make fmt
make clippy
```

Not included: WAL, `ExportReadView`, storage engine, compaction, admission
control, concurrent request execution beyond one task per accepted connection,
scripted protocol peers, same-export multi-connection visibility, operator-ready
binary packaging, or kernel NBD.

Closeout verification:

```text
make test
make fmt
make clippy
```

Commit 1/6: docs/execution: plan in-memory server series

  Type:             docs
  Required:         yes
  Summary:          Mark the in-memory NBD server design approved for Series 4
                    and record the current commit contract in the execution
                    artifact.
  Invariant focus:  The execution source of truth reflects the validation-client
                    design before code changes begin.
  Test level:       none
  Review gate:      structures
  Files:            docs/execution/2026-05-01-initial-integration.md
                    docs/plans/initial-integration/2026-05-01-in-memory-nbd-server.md
  Preconditions:    Series 3 is finished and the validation-client in-memory
                    server design has been discussed.
  Postconditions:   Series 4 has explicit commit boundaries, verification
                    commands, approval state, and accepted design guardrails.
  Verify:           git diff --cached --check
  Risks:            Low; this is planning-only, but it must not silently expand
                    Series 4 beyond the in-memory server checkpoint.
  Not included:     Protocol helpers, validation-client code, server sockets,
                    MemoryExport, or transmission I/O.
  Depends on:       none

Commit 2/6: protocol: add client wire helpers

  Type:             semantic
  Required:         yes
  Summary:          Add client-side framing helpers for fixed-newstyle
                    handshake, NBD_OPT_GO replies, and simple transmission
                    requests/replies.
  Invariant focus:  Both the validation client and NBD server use nbd-protocol
                    for public NBD wire shapes instead of private byte layouts.
  Test level:       integration
  Review gate:      code
  Files:            crates/nbd-protocol/src/constants.rs
                    crates/nbd-protocol/src/error.rs
                    crates/nbd-protocol/src/handshake.rs
                    crates/nbd-protocol/src/lib.rs
                    crates/nbd-protocol/src/option.rs
                    crates/nbd-protocol/src/transmission.rs
                    crates/nbd-protocol/tests/protocol_fixtures.rs
  Preconditions:    Commit 1 has recorded the Series 4 execution contract and
                    the existing protocol crate already parses the server-facing
                    wire subset.
  Postconditions:   The protocol crate can encode client flags, encode GO/ABORT
                    option requests, decode option replies including export
                    info, encode READ/WRITE/FLUSH/DISC requests, and decode
                    simple/read replies with cookies preserved.
  Verify:           cargo test -p nbd-protocol --test protocol_fixtures
                    cargo test -p nbd-protocol
  Risks:            Client-side helpers must stay syntax-only; catalog lookup,
                    socket I/O, and server policy still belong outside
                    nbd-protocol.
  Not included:     Validation-client networking, server listener lifecycle,
                    MemoryExport, catalog opening, or transmission command
                    execution.
  Depends on:       1

Commit 3/6: server: add memory export

  Type:             semantic
  Required:         yes
  Summary:          Introduce the nbd-server crate with the Export trait and the
                    MemoryExport implementation.
  Invariant focus:  In-memory byte contents live behind the Export boundary and
                    remain explicitly non-durable.
  Test level:       unit
  Review gate:      structures
  Files:            Cargo.lock
                    Cargo.toml
                    crates/nbd-server/Cargo.toml (new)
                    crates/nbd-server/src/error.rs (new)
                    crates/nbd-server/src/export.rs (new)
                    crates/nbd-server/src/lib.rs (new)
                    crates/nbd-server/src/memory.rs (new)
                    crates/nbd-server/tests/memory_export.rs (new)
  Preconditions:    Commit 2 has kept protocol helpers independent of server
                    state.
  Postconditions:   MemoryExport validates export metadata, rejects oversized
                    in-memory allocations, returns zero-filled reads before
                    writes, persists writes only in process memory, enforces
                    bounds, and treats flush as a completed-write barrier.
  Verify:           cargo test -p nbd-server --test memory_export
  Risks:            The in-memory size limit must make accidental OOM
                    unlikely while preserving enough space for integration
                    tests.
  Not included:     TCP listener lifecycle, NBD handshake handling, catalog
                    opening, validation-client networking, or multi-connection
                    shared export state.
  Depends on:       2

Commit 4/6: server: implement TCP option negotiation

  Type:             semantic
  Required:         yes
  Summary:          Add the small nbd-us-client validation crate and the NBD
                    server listener/connection path through fixed-newstyle
                    handshake and NBD_OPT_GO.
  Invariant focus:  Integration tests observe only real TCP framing while the
                    server opens exports through ExportCatalog.
  Test level:       integration
  Review gate:      code
  Files:            Cargo.lock
                    Cargo.toml
                    crates/nbd-us-client/Cargo.toml (new)
                    crates/nbd-us-client/src/client.rs (new)
                    crates/nbd-us-client/src/error.rs (new)
                    crates/nbd-us-client/src/lib.rs (new)
                    crates/nbd-server/Cargo.toml
                    crates/nbd-server/src/connection.rs (new)
                    crates/nbd-server/src/error.rs
                    crates/nbd-server/src/lib.rs
                    crates/nbd-server/src/server.rs (new)
                    crates/nbd-server/tests/tcp_integration.rs (new)
  Preconditions:    Commit 3 has established the Export boundary and
                    MemoryExport; Commit 2 has client-side option framing
                    helpers.
  Postconditions:   Tests can load temp config, create temp catalog exports,
                    start the NBD server on 127.0.0.1:0, connect through
                    nbd-us-client, negotiate NBD_OPT_GO, inspect negotiated
                    export size/flags, and see missing or deleted exports fail
                    before transmission mode.
  Verify:           cargo test -p nbd-us-client
                    cargo test -p nbd-server --test tcp_integration
  Risks:            This is the first async TCP boundary; keep the server task
                    lifecycle simple and keep nbd-us-client independent of
                    server internals.
  Not included:     READ/WRITE/FLUSH/DISC execution, workqueues, same-export
                    multi-connection visibility, server binary packaging, or
                    kernel NBD.
  Depends on:       3

Commit 5/6: server: handle in-memory I/O commands

  Type:             semantic
  Required:         yes
  Summary:          Extend the validation client and NBD server transmission
                    path for READ, WRITE, FLUSH, and DISC against MemoryExport.
  Invariant focus:  Completed in-memory writes are visible to later reads on
                    the same connection, and flush returns only after earlier
                    sequential writes complete.
  Test level:       integration
  Review gate:      code
  Files:            crates/nbd-us-client/src/client.rs
                    crates/nbd-us-client/src/error.rs
                    crates/nbd-us-client/src/lib.rs
                    crates/nbd-server/src/connection.rs
                    crates/nbd-server/src/error.rs
                    crates/nbd-server/src/lib.rs
                    crates/nbd-server/tests/tcp_integration.rs
  Preconditions:    Commit 4 has proven fixed-newstyle TCP negotiation and
                    catalog export opening through nbd-us-client.
  Postconditions:   TCP integration tests prove zero reads from new exports,
                    write/readback, flush, clean disconnect, out-of-bounds error
                    reporting, and independent contents for different catalog
                    exports served by the same process.
  Verify:           cargo test -p nbd-server --test tcp_integration
                    make test
                    make fmt
                    make clippy
  Risks:            Sequential I/O must remain honest about non-durability and
                    must not accidentally advertise FUA, multi-conn, or other
                    unsupported transmission features.
  Not included:     WAL, ExportReadView, StorageEngine, compaction, admission
                    control, pipelining, same-export shared state across
                    connections, or Docker/kernel NBD.
  Depends on:       4

Commit 6/6: protocol: bound in-memory wire allocations

  Type:             semantic
  Required:         yes
  Summary:          Address review findings by adding protocol-owned supported
                    payload limits and keeping socket transmission behind an
                    Export trait handle.
  Invariant focus:  Wire lengths are capped before allocation, and the
                    connection loop depends on the Export boundary rather than
                    MemoryExport directly.
  Test level:       integration
  Review gate:      code
  Files:            crates/nbd-us-client/src/client.rs
                    crates/nbd-protocol/src/lib.rs
                    crates/nbd-protocol/src/option.rs
                    crates/nbd-protocol/src/transmission.rs
                    crates/nbd-protocol/tests/protocol_fixtures.rs
                    crates/nbd-server/src/connection.rs
                    crates/nbd-server/src/export.rs
                    crates/nbd-server/src/lib.rs
                    docs/execution/2026-05-01-initial-integration.md
                    docs/plans/initial-integration/2026-05-01-in-memory-nbd-server.md
  Preconditions:    Commit 5 has completed the in-memory TCP command path and
                    review has identified the allocation and concrete-type
                    boundary gaps.
  Postconditions:   Option payload allocation is capped at 64 KiB, read/write
                    I/O is capped at 64 MiB, and successful negotiation returns
                    an ExportHandle to the transmission loop.
  Verify:           cargo test -p nbd-protocol --test protocol_fixtures
                    cargo test -p nbd-server --test tcp_integration
                    make test
                    make fmt
                    make clippy
  Risks:            This is still a sequential in-memory server; the cap is a
                    supported-subset limit, not a full resource-control model.
  Not included:     Per-export quotas, admission control, workqueues, pipelined
                    command execution, or durable backing state.
  Depends on:       5

## Series 5: Docker And Kernel-NBD Smoke

Depends on: Series 4 and an approved Docker/kernel smoke design

Roadmap milestone: M4

Design coverage:
`docs/plans/initial-integration/2026-05-02-docker-kernel-smoke.md`

Stable checkpoint: a privileged Linux container can run the NBD server and a
real NBD client can perform basic I/O.

Review focus: privilege boundaries, device cleanup, Makefile ergonomics, and
keeping Docker smoke outside the normal inner-loop proof.

Done means: manual or ignored smoke commands are documented and runnable in the
intended Linux/Docker environment.

Approval: finished

Closeout: Series 5 completed the Docker/kernel-NBD smoke checkpoint. The final
reviewed stack adds the Docker smoke design, renames the Rust userspace
validation crate to `nbd-us-client`, exposes the NBD server through
`nbd-server serve`, adds the Linux development container and Makefile workflow,
and adds a privileged kernel smoke script that attaches the real Debian
`/usr/sbin/nbd-client` to `/dev/nbd0`.

The review pass found and fixed one cleanup ownership issue: the smoke script
now refuses to run when the selected NBD device or mount point is already in
use, and cleanup only unmounts or disconnects resources created by that smoke
run.

Verification plan:

```text
make docker-build
make docker-smoke
```

Not included: CI-required privileged kernel testing, Docker Desktop
installations whose Linux VM lacks NBD support, production image hardening,
runtime durability, WAL, `ExportReadView`, storage engines, compaction,
admission control, or multi-connection kernel NBD testing.

Closeout verification:

```text
make -n docker-test
make -n docker-smoke
make -n docker-shell
make -n docker-kernel-shell
make -n docker-attach
git --no-pager diff --check HEAD~5..HEAD
make docker-smoke
```

Post-closeout follow-up: the kernel smoke script now writes a larger probe
file, runs `sync`, drops Linux page cache in the privileged container, and
compares the file through the mounted filesystem. This strengthens the
readback check without changing the smoke test's intended scope.

## Post-Completion Follow-Up: Export Runtime And Local Registry

Depends on: completed initial integration through Series 5

Design coverage:
`docs/plans/initial-integration/2026-05-02-export-runtime-registry.md`

Stable checkpoint: NBD connections open exports through `LocalExportRegistry`,
active exports own a shared `ExportRuntime`, and decoded read/write/flush work
flows through runtime jobs before replies are encoded on the socket path.

Review focus: local active-export ownership, runtime/engine responsibility
boundaries, socket/protocol separation, and keeping durable WAL/read-view work
out of this follow-up.

Approval: finished

Closeout: this follow-up introduced the long-term runtime boundary behind the
existing in-memory server. `NbdServer` now owns a `LocalExportRegistry`,
connections call `registry.open(...)` during `NBD_OPT_GO`, competing active
mounters receive `NBD_REP_ERR_POLICY`, and transmission requests submit
`ExportJob`s to the active export runtime. `MemoryExportEngine` is the shared
in-memory backend, while `MemoryExport` remains only as the compatibility name
for the older direct export path.

The stack also adds the small server backend config surface, the protocol
policy-error encoder, a process-local registry with open/close accounting, a
serial runtime with an opened metadata snapshot, and tests proving runtime
execution, registry exclusion/reopen behavior, TCP protocol behavior, and
kernel smoke readback.

Verification:

```text
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
make docker-smoke
```

Deferred follow-up: the long-term architecture now says logical export size
belongs to export generations, but the implemented Prisma schema and catalog
queries still store `size_bytes` on `exports`. The next catalog cleanup should
move implemented size ownership to `export_generations` while preserving the
serving-facing `ExportMeta.size_bytes()` API.

Not included: WAL, `ExportReadView`, durable storage engines, compaction,
range-aware `ExportAdmissionCtl`, split reader/writer connection runtime,
auth-based same-owner multi-connection mounts, etcd leases, or cross-process
writer fencing.
