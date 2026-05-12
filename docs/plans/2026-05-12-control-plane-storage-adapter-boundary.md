Title: Control Plane Storage Adapter Boundary
Date: 2026-05-12
Status: draft

# Problem

The current catalog implementation mixes three different concerns:

- storage-neutral export and tree metadata contracts;
- concrete SQLite persistence, row mapping, and migration assumptions;
- tree materialization and copy-on-write update algorithms used by storage
  engines.

That makes `nbd-control-plane` an ambiguous owner. It is useful as the shared
domain and control-plane API crate for `nbdcli`, `nbd-server`, tests, and future
tools, but it should not also be the SQLite adapter. Keeping SQLite inside that
crate makes it harder to swap PostgreSQL later and causes server/runtime code to
see database-specific concepts that should be below the catalog boundary.

The recent bounded tree work made this more visible. `sqlite_tree.rs` and the
large imports around it are a sign that shared catalog types, concrete database
code, and tree algorithms are being pulled through the same module boundary.

Large import lists are not the root bug, but they are a useful smell. A module
that needs many unrelated names from `crate::model` or from a root facade is
often doing too many jobs, importing from the wrong owner, or compensating for a
domain module that has grown into a grab bag.

# Goals

- Keep `nbd-control-plane` as the storage-neutral shared crate for catalog
  domain types, traits, and request/response contracts.
- Move SQLite implementation details into one concrete adapter crate or adapter
  module that can later have a PostgreSQL sibling.
- Keep SQL, `sqlx`, table names, row structs, and migration details out of
  server storage engines and runtime code.
- Move tree geometry and tree update semantics to the storage-engine side of the
  boundary, where the read/write behavior is owned.
- Preserve `export_heads` as the serving source of truth for the current export
  head.
- Make backend swaps possible without changing `nbd-server` engine logic.
- Treat large import lists from broad model or facade modules as a source
  topology warning during review.

# Constraints

- `nbdcli` and `nbd-server` both need to use the same durable catalog state.
- The active catalog schema uses `export_heads` as the serving source of truth.
- Runtime code must keep loading exports through catalog traits rather than
  bypassing the catalog boundary.
- The design must preserve a path to both SQLite and PostgreSQL adapters.
- The current package layout has a combined `nbd-server` library and binary, so
  provider-specific startup wiring may need an intermediate containment step.
- Existing tests that use SQLite directly are useful integration proof, but
  they should not force SQLite into pure engine tests.

# Non-Goals

- Implement PostgreSQL in this slice.
- Redesign the catalog schema only for naming or aesthetics.
- Remove `nbd-server`'s dependency on storage-neutral `nbd-control-plane` types.
- Change NBD protocol behavior, export runtime behavior, or blob storage
  semantics.
- Add legacy compatibility for pre-tree-format data. The current development
  database can move with the active schema.
- Solve tree garbage collection, historical checkpoints, or cross-format clone.

# Current State

`crates/nbd-control-plane` currently owns both the public catalog API and the
SQLite implementation:

- `src/lib.rs` exposes domain modules and re-exports many model types.
- `src/lib.rs` also exposes `SQLiteExportCatalog` and `open_catalog`.
- `Cargo.toml` depends on `sqlx` with SQLite support.
- `src/sqlite.rs` and `src/sqlite_tree.rs` contain concrete database behavior.
- `src/tree_geometry.rs` and `src/tree_format.rs` contain tree-shape decisions.

`crates/nbd-server` depends on `nbd-control-plane` for catalog traits and domain
types. Its library/runtime path should be allowed to do that. The problem is
that the same dependency also brings the SQLite adapter and tree persistence
implementation into the shared crate.

`crates/nbd-server/src/server.rs` currently opens the catalog from a config URL.
That makes the server crate aware of catalog providers. The storage engines
also have tests that import `SQLiteExportCatalog` directly. Those are useful
tests, but they should not define the production boundary.

# End State

The desired end state is:

- `nbd-control-plane` has no `sqlx` dependency and exposes only storage-neutral
  catalog contracts, domain types, and errors.
- one SQLite adapter owns all SQLite-specific code and implements the shared
  catalog traits;
- server engine, registry, runtime, connection, and storage modules import no
  concrete database adapter types;
- tree write algorithms live with the server storage engines that own the data
  behavior;
- CLI and process startup construct concrete catalog services at the edge and
  pass storage-neutral handles inward;
- internal implementation modules import from focused owner modules rather than
  broad root facades or a giant `model.rs`.

# Target Ownership

## `nbd-control-plane`

Owns storage-neutral domain and API contracts:

- export ids, names, heads, layout kinds, engine kinds, WAL sequence ids, and
  tree format ids;
- catalog request and response types;
- storage-neutral catalog traits;
- storage-neutral tree record types;
- validation for domain values;
- errors that describe catalog contract failures rather than a specific
  database engine.

It must not own:

- `sqlx`, SQLite, PostgreSQL, connection pools, migration SQL, or table-specific
  row structs;
- SQL transaction boundaries;
- tree path-copy algorithms;
- simple mutable tree materialization algorithms;
- compaction changed-chunk selection.

## `nbd-control-plane-sqlite`

Owns the concrete SQLite adapter:

- SQL queries and transactions;
- row structs and conversion between rows and storage-neutral domain types;
- SQLite catalog URL opening;
- SQLite migration assumptions;
- SQLite-specific tests and diagnostics;
- implementation of the storage-neutral catalog traits.

This crate is the place that would later get a PostgreSQL sibling, for example
`nbd-control-plane-postgres`.

## `nbd-server`

Owns NBD serving behavior and storage-engine semantics:

- export runtime, admission, connection lifecycle, and request execution;
- simple durable tree updates and zero-fill behavior;
- WAL durable read views, compaction planning, path-copy publication, and
  changed-chunk decisions;
- blob store and WAL integration.

The server library should depend only on storage-neutral control-plane traits
and records. It should not import `SQLiteExportCatalog`, `sqlx`, migration SQL,
SQLite table names, or adapter row types.

The process entrypoint still needs a place to construct concrete dependencies
from config. The clean target is a thin wiring boundary that opens the selected
catalog adapter and passes trait objects into `NbdServer`. If keeping the
current package layout is cheaper initially, provider-specific imports must stay
confined to binary/doctor wiring and must not leak into engine, registry, or
runtime modules.

## `nbdcli`

Owns operator commands and output. It should depend on the storage-neutral
control-plane API plus the catalog adapter opener used by operator tooling.
Provider-specific wording, such as SQLite file-permission advice, belongs in
adapter diagnostics or command wiring, not in shared server engine code.

# API Shape

The shared control-plane crate should expose records and traits that describe
the durable catalog contract without describing how the rows are stored.

```rust
pub enum TreeFormat {
    Bounded32V1,
}

pub struct TreeFormatSpec {
    pub fanout: u16,
    pub leaf_bytes: u64,
}

pub enum ExportHead {
    MemoryEmpty(MemoryExportHead),
    SimpleMutableTree(SimpleMutableTreeHead),
    CowImmutableTree(CowImmutableTreeHead),
}

pub struct TreeNodeRecord {
    pub id: NodeId,
    pub layout_kind: ExportLayoutKind,
    pub owner_export_id: Option<ExportId>,
    pub kind: TreeNodeKind,
    pub level: u16,
    pub span_start_bytes: u64,
    pub span_len_bytes: u64,
}

pub struct TreeEdgeRecord {
    pub parent_node_id: NodeId,
    pub slot: u16,
    pub child_node_id: NodeId,
}

pub struct TreeLeafRefRecord {
    pub node_id: NodeId,
    pub storage_kind: TreeStorageKind,
    pub storage_key: BlobKey,
    pub len_bytes: u64,
}

pub struct TreeRecordBatch {
    pub nodes: Vec<TreeNodeRecord>,
    pub edges: Vec<TreeEdgeRecord>,
    pub leaf_refs: Vec<TreeLeafRefRecord>,
}

pub struct TreeRecordSet {
    pub nodes: Vec<TreeNodeRecord>,
    pub edges: Vec<TreeEdgeRecord>,
    pub leaf_refs: Vec<TreeLeafRefRecord>,
}
```

The catalog boundary should keep lifecycle and head publication explicit:

```rust
#[async_trait::async_trait]
pub trait ExportCatalog: Send + Sync {
    async fn create_export(&self, request: CreateExport) -> Result<ExportRecord>;
    async fn clone_export(&self, request: CloneExport) -> Result<CloneExportResult>;
    async fn delete_export(&self, request: DeleteExport) -> Result<()>;
    async fn load_export_descriptor(
        &self,
        name: ExportName,
    ) -> Result<ActiveExportDescriptor>;
    async fn load_export_head(&self, export_id: &ExportId) -> Result<ExportHead>;
    async fn compare_and_swap_head(
        &self,
        request: CompareAndSwapHead,
    ) -> Result<CompareAndSwapOutcome>;
    async fn inspect_export(&self, request: InspectExport) -> Result<ExportRecord>;
    async fn list_exports(&self, request: ListExports) -> Result<Vec<ExportRecord>>;
}

#[async_trait::async_trait]
pub trait TreeRecordStore: Send + Sync {
    async fn load_tree_records(&self, root: &NodeId) -> Result<TreeRecordSet>;
    async fn insert_tree_records(&self, batch: TreeRecordBatch) -> Result<()>;
}
```

This shape is deliberately lower level than today's `SimpleTreeMetadataStore`
and `CowTreeMetadataStore`. The adapter persists records and publishes heads;
the server-side tree owners decide which records to create and which head to
publish.

The exact trait split can change during implementation, but the direction
should not: database adapters persist neutral records and atomic head changes;
storage engines own tree algorithms.

# Tree Algorithm Ownership

Tree geometry can remain in `nbd-control-plane` only if it is a pure format
description that both CLI output and server code need. Any code that decides
which nodes to create, which path to copy, or which chunks changed belongs with
the server storage engines.

Expected server-side owners:

- `crates/nbd-server/src/engines/tree/geometry.rs` or equivalent shared tree
  module for runtime tree paths and spans;
- `crates/nbd-server/src/engines/simple_durable/mutable_tree.rs` for lazy
  simple tree materialization and mutable chunk commits;
- `crates/nbd-server/src/engines/wal_durable/compaction.rs` for COW path-copy,
  changed-chunk publication, and compare-and-swap head advancement;
- `crates/nbd-server/src/engines/tree/read.rs` for storage-neutral read view
  materialization from `TreeRecordSet`.

The control-plane adapter may validate row-level invariants, such as duplicate
edge slots in one parent, but it must not become the owner of the sparse-tree
write algorithm.

# Import Hygiene

Large import lists are a review signal, not a lint by themselves.

Allowed:

- root-level re-exports for external compatibility and ergonomic public APIs;
- short imports from focused owner modules;
- aggregate request/response structs when a function genuinely needs a bundle
  of related values.

Smells:

- implementation modules importing a long list from `crate::model`;
- implementation modules importing many names from a root facade of their own
  crate;
- test helpers that require concrete adapter types to exercise pure engine
  behavior;
- adding a `prelude` to hide a broad dependency instead of narrowing the
  boundary.

Review rule:

If a new or changed module needs a large list of domain imports, stop and ask
which owner should provide the smaller abstraction. The answer may be to split
`model.rs`, introduce a focused aggregate type, or move the code to the module
that owns the behavior.

# Proposed Source Topology

```text
crates/nbd-control-plane/
  src/
    lib.rs                 # storage-neutral exports and compatibility facade
    catalog.rs             # ExportCatalog and catalog handle traits
    export.rs              # export identity, descriptor, head, lifecycle types
    tree.rs                # storage-neutral tree records and tree store traits
    tree_format.rs         # TreeFormat ids and pure format specs
    error.rs               # storage-neutral catalog errors
    catalog_url.rs         # parsed provider/url value, no adapter opening

crates/nbd-control-plane-sqlite/
  src/
    lib.rs                 # SQLite adapter construction
    export_catalog.rs      # ExportCatalog implementation
    tree_records.rs        # TreeRecordStore implementation
    row.rs                 # SQLite row mapping
    error.rs               # SQLite-to-catalog error mapping

crates/nbd-server/
  src/
    server.rs              # accepts opened catalog services, no adapter open
    registry/
    engines/
      tree/
        geometry.rs        # runtime path/span helpers if not shared
        read.rs
      simple_durable/
        mutable_tree.rs
      wal_durable/
        compaction.rs
```

The final file names can change, but each directory should have one obvious
owner. A reviewer should not look at a large file or directory and see the next
unrelated feature's natural dumping ground.

# Invariants

- `export_heads` remains the serving source of truth for each export's current
  head.
- A tree-backed head carries its `TreeFormat`; readers do not infer tree shape
  from adapter defaults.
- Missing tree paths represent zero-filled data.
- `simple_mutable_tree` records are export-private and may refer to mutable
  blobs.
- `cow_immutable_tree` records are immutable after publication and may be shared
  by cloned exports.
- COW publication is atomic with respect to the expected prior head.
- The database adapter may reject malformed record batches, but it does not
  choose the sparse tree update plan.
- Server engine code does not import concrete catalog adapters.
- Storage-neutral control-plane code does not depend on `sqlx`.

# Migration Plan

1. Split storage-neutral control-plane modules from the concrete SQLite adapter
   without changing behavior.
2. Add `nbd-control-plane-sqlite` and move `SQLiteExportCatalog`, SQL queries,
   row mapping, migration assumptions, and SQLite tests into it.
3. Replace `open_catalog` in `nbd-control-plane` with an adapter-neutral
   construction boundary. CLI and process wiring can call an adapter opener;
   server runtime receives already-opened catalog services.
4. Move tree write algorithms out of the SQLite adapter. Introduce neutral tree
   records and let simple durable and WAL durable code build record batches.
5. Update tests so pure engine behavior uses fake or in-memory trait
   implementations. Keep SQLite integration tests in the SQLite adapter crate.
6. Remove compatibility imports once callers use focused owner modules.

The paused bounded-tree implementation should be treated as implementation
evidence, not as the final topology. The refactor can reuse its tests and
logic, but the final series should place behavior under the ownership model in
this document.

# Validation

- `cargo test -p nbd-control-plane` proves storage-neutral domain behavior.
- `cargo test -p nbd-control-plane-sqlite` proves SQLite adapter behavior.
- `cargo test -p nbd-server` proves engine behavior without requiring direct
  SQLite imports in engine modules.
- `cargo test --workspace` proves the integrated workspace.
- `cargo clippy --workspace --all-targets -- -D warnings` catches dependency
  and import cleanup issues.
- `rg "SQLite|Sqlite|sqlx|tree_nodes|export_heads" crates/nbd-server/src`
  should have no hits in engine, registry, runtime, connection, or storage
  modules. Any remaining hits must be confined to process wiring or removed by
  a follow-up binary split.
- `cargo tree -p nbd-control-plane` should not include `sqlx` after the split.

# Risks

- A trait that is too high level will keep tree algorithms hidden in the
  adapter and recreate the current problem.
- A trait that is too low level will leak table shape into server code and make
  PostgreSQL harder instead of easier.
- Moving adapter construction out of `nbd-control-plane` may create temporary
  churn in `nbdcli`, `nbd-server`, and tests.
- Splitting `model.rs` only for import neatness could become noise. The split
  should follow ownership boundaries, not line-count aesthetics.
- If process wiring remains inside the current `nbd-server` package, dependency
  checks need to distinguish library/runtime modules from binary-only wiring.

# Alternatives Considered

## Keep `nbd-control-plane` as the SQLite-backed catalog crate

This preserves the current shape but keeps the storage backend and domain API
coupled. It also makes PostgreSQL a disruptive later change because the server
and CLI already see the SQLite-backed entry points.

## Move SQLite into `nbd-server`

This would remove SQLite from `nbd-control-plane`, but it puts database
knowledge in the runtime crate and makes `nbdcli` need a different path for the
same catalog. It also violates the goal that the database implementation live in
one adapter place.

## Rename `nbd-control-plane`

A new name such as `nbd-catalog` may eventually be clearer, but renaming does
not solve the boundary by itself. The important first step is to separate
storage-neutral contracts from concrete persistence.

# Open Questions

- Should concrete adapter opening live in a new helper crate, or should
  `nbdcli` and the `nbd-server` binary depend on `nbd-control-plane-sqlite`
  directly?
- Should the current `nbd-server` package be split into a library crate and a
  thin binary/wiring crate so the library has no provider-specific imports at
  all?
- Should `TreeFormat` pure geometry stay in `nbd-control-plane`, or should only
  the format id stay shared while runtime geometry lives in `nbd-server`?
- What is the smallest compatibility facade worth keeping while existing code
  moves from `model.rs` root imports to focused owner modules?

# Design Exit Criteria

The design is ready for `$review-plan` when:

- the team agrees that `nbd-control-plane` remains the storage-neutral shared
  crate;
- the concrete SQLite implementation has a named adapter owner;
- the server-side owners for simple mutable tree and COW tree algorithms are
  accepted;
- the intended treatment of provider-specific process wiring is clear enough to
  plan a commit series;
- the import-list smell is captured as a topology review rule, not a mechanical
  style limit.

# Recommended Next Step

Run `$review-plan` on this draft before any execution planning. The review
should focus on the adapter boundary, the tree-record trait level, and whether
the current `nbd-server` package needs a binary/library split now or only a
temporary wiring containment step.
