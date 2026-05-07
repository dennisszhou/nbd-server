Title: NBD Server Module Topology
Date: 2026-05-07
Status: approved

# Problem

`crates/nbd-server/src` has grown from a vertical prototype into a server with
several real boundaries: protocol sessions, export admission, export runtimes,
engine implementations, blob storage, WAL durability, COW read views, and
server lifecycle. The source tree still looks mostly like a flat prototype.

That flat shape makes important boundaries easy to miss:

- `connection.rs` owns NBD wire adaptation, option negotiation, request
  routing, reply serialization, shutdown I/O helpers, and tests in one large
  file.
- `export.rs`, `admission.rs`, `runtime.rs`, and `registry.rs` are peers even
  though only some of them define the export execution contract. The registry
  also composes catalog, engines, storage, WAL, and runtime policy.
- engine implementations and engine-internal helpers sit next to server
  lifecycle modules.
- `wal.rs` mixes the WAL service API, local provider, record codec, replay, and
  scan behavior.
- protocol crate types leak into server-internal export and observability
  types, which weakens the intended boundary between wire protocol and export
  execution.

The result is review friction and a real risk that future S3, clone,
multi-connection, or lifecycle work will attach behavior to whichever file is
nearby instead of to the correct owner.

# Goal

Define one durable module topology for `nbd-server` that can support the next
round of storage and protocol work without another broad source-tree cleanup.

The design should:

- make the request path easy to follow from socket to export runtime to engine
  and back to reply writer;
- keep NBD wire types at the connection adapter boundary;
- keep export admission and runtime contracts independent of engine
  implementations;
- keep engine implementations below explicit export, storage, and WAL
  boundaries;
- keep shared storage and WAL provider APIs extractable later;
- preserve current public re-exports while internal imports move toward the
  owning module paths;
- let the cleanup execute in batches under one approved topology.

# Constraints

- This is a source organization and internal boundary cleanup. It must not
  change protocol behavior, export behavior, catalog schema, storage layout, WAL
  format, compaction behavior, config shape, or operator commands.
- `nbd-server` remains the crate boundary for now.
- Existing tests and external imports through the root `nbd_server::...` facade
  should keep working unless a later approved design explicitly changes the
  public API.
- Rust modules should stay private by default. Use `pub(crate)` for internal
  cross-module contracts and root `pub use` only for intentional compatibility
  or public crate API.
- New module boundaries must follow the existing architecture docs for protocol,
  admission, storage, WAL, read view, lifecycle, and registry ownership.
- `storage/` already exists and remains the shared blob-store boundary.
- Unsafe memory access stays isolated to the memory engine module and remains
  protected by the admitted request capability.

# Non-goals

- Extracting `nbd-storage`, `nbd-wal`, or another new crate.
- Implementing S3 storage, storage worker queues, delete/GC, leases, auth, or
  multi-connection serving semantics.
- Rewriting the NBD protocol crate.
- Replacing Tokio task structure, export admission policy, queue depth behavior,
  WAL format, read cache policy, or compaction policy.
- Changing `engine_kind`, `layout_kind`, or catalog metadata.
- Designing every future feature that may use these modules.
- Producing the execution commit stack in this design.

# End state

The library source tree has a small set of top-level ownership boundaries:

```text
crates/nbd-server/src/
  lib.rs
  error.rs
  range.rs
  server.rs
  observability.rs
  connection/
    mod.rs
    shutdown.rs
    io.rs
    handshake.rs
    options.rs
    transmission.rs
    replies.rs
  export/
    mod.rs
    request.rs
    completion.rs
    engine.rs
    admission.rs
    runtime.rs
  registry/
    mod.rs
    factory.rs
    active.rs
  engines/
    mod.rs
    memory.rs
    tree/
      mod.rs
      read.rs
    simple_durable/
      mod.rs
      mutable_tree.rs
      reader.rs
    wal_durable/
      mod.rs
      admission.rs
      read_view.rs
      overlay.rs
      read_cache.rs
      extent_map.rs
      compaction.rs
  storage/
    mod.rs
    local.rs
  wal/
    mod.rs
    local.rs
    codec.rs
    replay.rs
```

The binary-side files stay separate from the library topology:

```text
crates/nbd-server/src/main.rs
crates/nbd-server/src/logging.rs
```

The exact private helper file names may change during implementation if the
reviewed ownership stays the same. The top-level boundaries should not change
without revising this design.

# Proposed approach

## Root facade

`lib.rs` remains the compatibility facade. It declares the top-level modules
and re-exports the crate API that tests and external users already import.

Internal code should prefer paths from the owning module, such as
`crate::export::ExportRequest` or `crate::storage::BlobStoreHandle`, instead of
using the root facade as a catch-all import surface. Root re-exports are for
public API compatibility, not for hiding ownership inside the crate.

No new crate-wide prelude should be introduced. It would make dependencies less
visible and would work against the purpose of this cleanup.

## Shared range primitive

Move `ByteRange` out of admission into `range.rs`.

`ByteRange` is a pure logical byte-range primitive used by admission, WAL,
engines, read views, caches, and compaction. Keeping it inside export admission
would make WAL and storage-tree support appear to depend on the export
scheduler. That is the wrong source-of-truth boundary.

`range.rs` should contain only range construction, accessors, and validation
helpers that are not tied to admission policy, WAL durability, or storage
layout.

## Connection boundary

`connection/` owns one client socket session and all NBD wire adaptation.

Allowed dependencies:

- `nbd_protocol`;
- `export` request, completion, and runtime handles;
- `registry` for `NBD_OPT_GO` export open and close;
- `observability` for connection and request diagnostics;
- `error` and `range`.

Forbidden dependencies:

- concrete engines;
- blob storage backends;
- WAL provider internals;
- read cache, COW tree, or compaction internals;
- direct catalog SQL or storage metadata mutation.

`connection` converts from `nbd_protocol` request and reply structures into
server-internal export structures. After conversion, export runtime and engine
code should not need `nbd_protocol` imports.

The split responsibilities are:

- `shutdown.rs`: cooperative server-to-connection shutdown handles.
- `io.rs`: read and write helpers that race socket I/O with shutdown.
- `handshake.rs`: fixed-newstyle handshake.
- `options.rs`: option request loop, `NBD_OPT_GO`, `NBD_OPT_ABORT`, and option
  replies.
- `transmission.rs`: transmission request decoding and conversion to
  `ExportRequest`.
- `replies.rs`: `ConnectionReply`, reply kinds, completion sink, and serialized
  wire reply writing.
- `mod.rs`: public `serve_with_shutdown` entry point and task orchestration.

`connection` continues to own the original NBD cookie for wire replies, but
exports should see only a server-local request cookie value.

## Export boundary

`export/` owns the server-internal request execution contract.

It defines:

- `ExportRequest` and `ExportReply`;
- `RequestCookie` and `ExportJobContext` server-local context values needed by
  runtime and observability;
- `ExportCompletion`, `CompletedExport`, and reply handoff contracts;
- `ExportEngine` and `ExportAdmissionPolicy`;
- `AdmittedExportRequest` and `OwnedAdmittedExportRequest`;
- `ExportAdmissionCtl`, tickets, waiters, and permits;
- `ExportRuntime`, queue slots, serial runtime, and concurrent runtime.

`export` must not depend on `nbd_protocol`, concrete engine modules, storage
backends, WAL provider implementations, or connection socket types. It may
depend on catalog domain types such as `ExportRecord` because runtime policy
uses the active export metadata, but it must not perform catalog persistence.

The admitted request capability remains the storage access proof. Engines that
can observe or mutate export data execute through `AdmittedExportRequest`, not
through a bare `ExportRequest`.

Queue-slot lifetime remains part of the export/connection contract:

```text
reserve queue slot
  -> submit export job
  -> engine completes
  -> completion sends connection reply
  -> queue slot is held until the reply is written or dropped
```

## Registry boundary

`registry/` is a top-level orchestration boundary, not a child of `export/`.

`LocalExportRegistry` and `ExportFactory` compose several lower-level owners:

- catalog metadata loading;
- active export owner tracking;
- shared blob-store handles;
- WAL provider handles;
- concrete engine construction;
- export runtime selection.

Keeping registry separate avoids a misleading dependency cycle where
`export/` both defines the engine/runtime contracts and also knows every engine
implementation. Instead:

```text
export/ defines contracts
engines/ implement contracts
registry/ composes catalog + engines + storage + WAL + runtime policy
```

The registry may import concrete engines. Concrete engines must not import the
registry.

## Engine boundary

`engines/` owns concrete export data behavior after admission.

Allowed dependencies:

- `export` request, reply, engine, admission policy, and admitted request
  contracts;
- `range`;
- `storage` blob-store traits;
- `wal` provider traits and records when implementing WAL durable behavior;
- catalog domain and tree metadata traits needed to load or publish engine
  state;
- observability events for engine and storage diagnostics.

Forbidden dependencies:

- `nbd_protocol`;
- connection socket types, reply queues, or shutdown handles;
- server listener lifecycle;
- registry active-owner maps.

The memory engine remains a single explicit unsafe island. Its module gets the
only `#[allow(unsafe_code)]` needed under `#![deny(unsafe_code)]`, and the
safety comment continues to tie unsynchronized memory access to admission
permits.

`engines/tree/` holds shared committed-tree read primitives used by simple and
WAL durable engines, such as `Block`, `BlockPart`, and `TreeReader`. These are
engine read helpers, not storage backends and not protocol structures.

`engines/simple_durable/` owns mutable simple-tree behavior and the direct
commit path that intentionally requires `MutableBlobStoreHandle`.

`engines/wal_durable/` owns the WAL-backed engine, read view, WAL overlay,
read cache, extent map, admission policy, and COW compaction coordinator. The
extent map can stay private here until another subsystem has a real need for
it.

## Storage boundary

`storage/` stays top-level and keeps the existing approved blob-store cleanup
shape.

It owns opaque blob byte storage:

- `BlobStore`;
- `MutableBlobStore`;
- handle aliases;
- `put_random_blob`;
- `LocalBlobStore`.

It must not learn about exports, engines, WAL records, tree nodes, compaction
policy, sockets, catalog transactions, or admission tickets. `BlobKey` is an
opaque key type from the control-plane model; using it does not make storage a
catalog owner.

## WAL boundary

`wal/` owns the WAL service/provider contract and the local WAL backend.

It defines:

- `WalDomain`;
- `OpenWal`;
- `WalRequest`;
- `WalRecord`;
- `WalBounds`;
- `WalReplay`;
- `WalPruneResult`;
- `WalProvider`;
- `ExportWal`;
- `LocalWalProvider`;
- `LocalExportWal`.

The local backend split should isolate record and segment encoding from
provider lifecycle:

- `codec.rs`: segment and record constants, encode, decode, checksum, and
  partial/corrupt record handling.
- `local.rs`: local provider and export WAL implementation.
- `replay.rs`: replay cursor and scan summaries.
- `mod.rs`: public WAL contract and re-exports.

`wal/` may depend on `range`, `error`, control-plane identity types, and local
filesystem APIs for the local backend. It must not depend on connection,
export runtime, concrete engines, storage blob stores, or catalog tree
metadata.

## Observability boundary

`observability.rs` can remain top-level for this cleanup, but protocol wire
types should be removed from its public context shape.

`RequestCookie` and `ExportJobContext` belong to `export::request`, because
they are part of the export execution contract. `observability` consumes those
values when building spans and events; it does not own request identity.

Introduce a server-local request cookie type, for example:

```rust
pub struct RequestCookie(u64);
```

`connection/` converts between `NbdCookie` and `RequestCookie`. Export runtime
and engine code use `RequestCookie` only for diagnostics and completion
correlation. This keeps wire protocol identity out of export-facing APIs while
preserving log fields and reply correlation.

A later observability-only cleanup may split events, targets, ids, and macros,
but that is not required for this topology refactor.

# Data model / API shape

The cleanup does not change durable data models. It changes module ownership
and a small set of internal type locations.

Stable internal primitives:

```rust
pub struct ByteRange {
    start: u64,
    len: u64,
}

pub struct RequestCookie(u64);

pub struct ExportJobContext {
    cookie: RequestCookie,
    // connection id, request sequence, command, range, and timing metadata
}

pub enum ExportRequest {
    Read { offset: u64, len: u32 },
    Write { offset: u64, data: Vec<u8> },
    Flush,
}

pub trait ExportEngine: Send + Sync {
    fn admission_policy(&self) -> ExportAdmissionPolicyHandle;

    async fn execute_admitted(
        &self,
        request: AdmittedExportRequest,
    ) -> ExportResult;

    async fn close(&self) -> Result<()> {
        Ok(())
    }
}
```

The desired request-path dependencies are:

```text
server
  -> connection
      -> registry.open during option negotiation
      -> export runtime submit during transmission
  -> registry
      -> engines + storage + wal + catalog + export runtime

export runtime
  -> export admission
  -> export engine trait
  -> completion sink

engines
  -> export contracts
  -> storage and/or wal contracts
  -> catalog metadata traits when needed
```

The desired reply-path dependency is:

```text
engine result
  -> export completion
  -> connection reply queue
  -> NBD wire reply encoding
```

Only the connection reply writer converts back to NBD wire replies.

# Invariants

- `nbd_protocol` imports are confined to `connection/`, protocol-focused
  tests, and small conversion points approved by this design.
- `export/` does not know concrete engine types.
- concrete engines do not know connection sockets, reply queues, or NBD wire
  structs.
- `registry/` is the composition owner for catalog metadata, concrete engine
  opening, storage handles, WAL handles, and runtime policy.
- `storage/` stores opaque blob bytes and never interprets export, tree, WAL,
  admission, or compaction semantics.
- `wal/` owns WAL sequencing, persistence, replay, and pruning contracts. It
  does not know storage blobs or connection state.
- `ByteRange` is a shared logical range primitive, not an admission-owned type.
- request cookies used outside `connection/` are server-local values, not
  `nbd_protocol::NbdCookie`.
- queue slots stay occupied until the connection reply is written or dropped.
- admitted export requests remain the only mutable engine data access
  capability.
- unsafe code remains isolated to the memory engine and justified by admission
  invariants.
- root public re-exports preserve compatibility, while internal imports should
  name the owning module.
- modules are private by default; internal exposure uses `pub(crate)` rather
  than broad `pub mod` visibility.

# Alternatives considered

## Move `registry` under `export/`

This would group active exports with runtime and admission, but it would make
`export/` know concrete engines, storage, WAL, and catalog opening details.
That weakens the engine trait boundary. Keeping `registry/` top-level makes it
clear that it is composition and lifecycle orchestration.

## Keep `ByteRange` in admission

This preserves the current location, but it forces WAL, read cache, compaction,
and storage-tree code to depend on an admission module for a plain range type.
Moving `ByteRange` to `range.rs` gives shared low-level code the right owner.

## Extract crates now

Extracting storage, WAL, or engine support into new crates could enforce
boundaries harder, but it adds Cargo and API churn before the internal module
ownership is settled. A clean internal topology is the lower-risk step and
keeps later extraction possible.

## Only split large files

Splitting `connection.rs` and `wal.rs` by size alone would reduce file length
but would not fix ownership leaks. The cleanup should be driven by dependency
direction and source-of-truth state, not by line count.

# Migration / rollout

This refactor can execute in batches under one approved design. A new design
review is required only if implementation reveals that an approved boundary is
wrong or if behavior changes become necessary.

Each batch should preserve behavior and public root re-exports. Internal
imports should move toward owning-module paths as the relevant modules are
introduced.

Recommended rollout shape:

- establish `range.rs` and server-local request cookie conversion;
- split `connection/` while keeping request, reply, and shutdown behavior
  unchanged;
- move export request, completion, engine trait, admission, and runtime into
  `export/`;
- move active export registry and factory into `registry/`;
- move memory, simple durable, WAL durable, tree-read, read-cache, extent-map,
  and compaction support into `engines/`;
- split `wal/` provider, codec, replay, and local backend internals;
- update documentation and internal imports to match the final ownership.

The exact execution batches and commit boundaries belong in `$plan-series`,
not in this design.

# Validation strategy

Validation should prove that the topology changes did not change request-path
or storage behavior.

For move-only or visibility-only batches:

- `cargo fmt --all --check`;
- `cargo test -p nbd-server`.

For the connection/protocol split and cookie decoupling:

- `make test-protocol`;
- `cargo test -p nbd-server --test tcp_integration`.

For export runtime, admission, and queue-slot moves:

- `cargo test -p nbd-server --test export_runtime`;
- `cargo test -p nbd-server --test admission`;
- `make test-protocol`.

For engine, storage, WAL, read-view, or compaction moves:

- `cargo test -p nbd-server --test memory_export`;
- `cargo test -p nbd-server --test simple_durable`;
- `cargo test -p nbd-server --test wal`;
- `cargo test -p nbd-server --test wal_durable`;
- `cargo test -p nbd-server --test compaction`.

Before handoff for the full cleanup, run:

- `cargo fmt --all --check`;
- `cargo clippy --workspace --all-targets -- -D warnings`;
- `cargo test --workspace`;
- `make test-protocol`.

Docker kernel smoke is not required for every move-only batch. It should run
before final closeout if the implementation changes request-path behavior,
engine behavior, storage I/O behavior, WAL behavior, or shutdown behavior
beyond pure module moves.

# Risks

- Large file moves can hide behavior changes. Mitigation: keep early batches
  move-only and review with rename-aware diffs.
- Root re-exports can mask poor internal ownership. Mitigation: use owning
  module paths internally once each boundary exists.
- Over-splitting can make navigation worse. Mitigation: split only along named
  responsibilities and keep helper modules private.
- Protocol type decoupling can accidentally change cookie handling. Mitigation:
  convert at the connection boundary and prove behavior with TCP protocol tests.
- Moving unsafe memory code can weaken review of its safety boundary.
  Mitigation: preserve the explicit unsafe allowance and admitted-request safety
  comments in the memory engine module.
- WAL/read-view moves can blur authoritative, cached, and derived state.
  Mitigation: keep read view, overlay, cache, and compaction ownership explicit
  under the WAL durable engine.

# Open questions

- None.

# Design exit criteria

This design is ready for `$review-plan` when:

- the top-level module owners are accepted;
- `registry/` as a separate orchestration boundary is accepted;
- `ByteRange` moving to `range.rs` is accepted;
- NBD cookie conversion at `connection/` is accepted;
- the batching approach is accepted as one topology design with multiple
  implementation batches.

# Recommended next step

Run `$review-plan` after the draft design is accepted. A
`ready for series planning` result should be treated as permission to ask
whether to start `$plan-series`, not as permission to start execution.
