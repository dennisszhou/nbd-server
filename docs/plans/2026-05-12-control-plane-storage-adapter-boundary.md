Title: Control Plane Storage Adapter Boundary
Date: 2026-05-12
Status: approved

# Problem

The current catalog implementation mixes three different concerns:

- storage-neutral export and tree metadata contracts;
- concrete SQLite persistence, row mapping, and migration assumptions;
- tree materialization and copy-on-write update algorithms used by storage
  engines.

That makes `nbd-control-plane` an ambiguous owner. It is useful as the public
control-plane facade for `nbdcli`, `nbd-server`, tests, and future tools, but
it should not also contain SQLite row logic or tree algorithms. Keeping SQLite
implementation code in the same source modules makes it harder to swap
PostgreSQL later and causes server/runtime code to see database-specific
concepts that should be below the catalog boundary.

The recent bounded tree work made this more visible. `sqlite_tree.rs` and the
large imports around it are a sign that shared catalog types, concrete database
code, and tree algorithms are being pulled through the same module boundary.

Large import lists are not the root bug, but they are a useful smell. A module
that needs many unrelated names from `crate::model` or from a root facade is
often doing too many jobs, importing from the wrong owner, or compensating for a
domain module that has grown into a grab bag.

Splitting the storage adapter is therefore not only about replacing SQLite
later. It is also about preventing one shared crate module from becoming the
place every export, tree, catalog, and adapter type has to pass through.

# Goals

- Keep `nbd-control-plane` as the frontend facade and factory used by
  `nbdcli` and `nbd-server`.
- Move storage-neutral catalog domain types, traits, request/response
  contracts, and errors into a core control-plane API crate that the facade
  re-exports.
- Move SQLite implementation details into one concrete adapter crate that can
  later have a PostgreSQL sibling.
- Keep SQL, `sqlx`, table names, row structs, and migration details out of
  server source code. The `nbd-server` crate may depend on the
  `nbd-control-plane` facade, and that facade may choose a SQLite adapter, but
  modules such as `wal_durable.rs` must not import or name SQLite concepts.
- Move tree geometry, lazy tree traversal, and tree update semantics to the
  storage-engine side of the boundary, where the read/write behavior is owned.
- Preserve `export_heads` as the serving source of truth for the current export
  head.
- Make backend swaps possible without changing `nbd-server` engine logic.
- Load tree metadata on demand. Opening or reading a large sparse export must
  not require loading every tree row reachable from the root.
- Split broad domain modules by ownership so implementation modules can import
  focused types instead of long lists from one shared model namespace.
- Treat large import lists from broad model or facade modules as a source
  topology warning during review.

# Constraints

- `nbdcli` and `nbd-server` both need to use the same durable catalog state.
- The active catalog schema uses `export_heads` as the serving source of truth.
- Runtime code must keep loading exports through the `nbd-control-plane`
  facade and storage-neutral catalog traits rather than bypassing the catalog
  boundary.
- The design must preserve a path to both SQLite and PostgreSQL adapters.
- Breaking catalog compatibility is allowed in this development slice. The
  durability rule is that every commit in the eventual series must build, pass
  its relevant checks, and describe the schema/API it actually supports.
- The current `nbd-server` package may keep calling the `nbd-control-plane`
  facade. The boundary is that it must not import or name concrete backing
  database details.
- Existing tests that use SQLite directly are useful integration proof, but
  they should not force SQLite into pure engine tests.

# Non-Goals

- Implement PostgreSQL in this slice.
- Redesign the catalog schema only for naming or aesthetics.
- Remove `nbd-server`'s dependency on the `nbd-control-plane` facade.
- Change NBD protocol behavior, export runtime behavior, or blob storage
  semantics.
- Add legacy compatibility for pre-tree-format data. The current development
  database can move with the active schema.
- Solve tree garbage collection, historical checkpoints, or cross-format clone.

# Current State

`crates/nbd-control-plane` currently owns the public catalog API, the factory
function, and the SQLite implementation:

- `src/lib.rs` exposes domain modules and re-exports many model types.
- `src/lib.rs` also exposes `SQLiteExportCatalog` and `open_catalog`.
- `Cargo.toml` depends on `sqlx` with SQLite support.
- `src/sqlite.rs` and `src/sqlite_tree.rs` contain concrete database behavior.
- `src/tree_geometry.rs` and `src/tree_format.rs` contain tree-shape decisions.

`crates/nbd-server` depends on `nbd-control-plane` for catalog opening, catalog
traits, and domain types. That dependency is acceptable as the frontend
abstraction. The problem is that the same source crate currently contains the
SQLite adapter and tree persistence implementation, so concrete database code is
too easy to import from server modules.

`crates/nbd-server/src/server.rs` currently opens the catalog from a config URL.
That makes the server crate aware of catalog providers. The storage engines
also have tests that import `SQLiteExportCatalog` directly. Those are useful
tests, but they should not define the production boundary.

# End State

The desired end state is:

- `nbd-control-plane` is the public facade. It owns provider selection,
  `open_catalog`, and re-exports storage-neutral contracts from the core API.
- `nbd-control-plane-core` owns storage-neutral catalog contracts, domain
  types, and errors. It exists to avoid a Rust dependency cycle: the public
  facade depends on concrete adapters, and concrete adapters need a neutral
  crate that defines the traits they implement.
- one SQLite adapter crate owns all SQLite-specific code and implements the
  core catalog traits;
- server engine, registry, runtime, connection, and storage modules import no
  concrete database adapter types;
- tree geometry, lazy tree readers, and tree write algorithms live with the
  server storage engines that own the data behavior;
- CLI and process startup call the `nbd-control-plane` facade. They do not
  select or import concrete adapter crates directly.
- the shared domain surface is split into focused owner modules, so internal
  implementation modules import from the owner they actually need rather than
  from broad root facades or a giant `model.rs`.

# Target Ownership

## `nbd-control-plane`

Owns the public frontend abstraction:

- `open_catalog` and provider selection from config/catalog URLs;
- public re-exports of storage-neutral control-plane API types;
- construction of catalog service handles from concrete adapter crates;
- conversion from `CatalogUrl` into adapter-specific connection inputs;
- operator-facing provider diagnostics that are not specific to one binary.

It may depend on adapter crates. It must not contain adapter implementation
logic itself. Adapter crates must not depend on this facade; they depend on
`nbd-control-plane-core` instead.

It must not own:

- SQL queries, row structs, connection pools, or migration SQL;
- tree geometry and span/path math;
- in-memory tree objects or caches;
- tree path-copy algorithms;
- simple mutable tree materialization algorithms;
- compaction changed-chunk selection.

## `nbd-control-plane-core`

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

- `sqlx`, SQLite, PostgreSQL, connection pools, migration SQL, or
  table-specific row structs;
- SQL transaction boundaries;
- tree geometry and span/path math;
- in-memory tree objects or caches;
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
- tree geometry derived from stored `TreeFormat` ids;
- lazy in-memory tree readers/editors and their metadata caches;
- simple durable tree updates and zero-fill behavior;
- WAL durable read views, compaction planning, path-copy publication, and
  changed-chunk decisions;
- blob store and WAL integration.

Server source should depend on the `nbd-control-plane` facade and
storage-neutral catalog handles. It should not import `SQLiteExportCatalog`,
`sqlx`, migration SQL, SQLite table names, or adapter row types.

The server process may call `nbd_control_plane::open_catalog`. The backing
database remains hidden behind that frontend abstraction.

## `nbdcli`

Owns operator commands and output. It should depend on the storage-neutral
control-plane facade. Provider-specific wording, such as SQLite
file-permission advice, belongs in the facade or adapter diagnostics, not in
server engine code.

# API Shape

The core control-plane API should expose records and traits that describe the
durable catalog contract without describing how the rows are stored. The public
`nbd-control-plane` facade should re-export these types and expose
`open_catalog`.

```rust
pub enum TreeFormat {
    Bounded32V1,
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

pub struct TreeEdgeLookup {
    pub parent_node_id: NodeId,
    pub slots: Vec<u16>,
}

pub struct PublishTreeUpdate {
    pub export_id: ExportId,
    pub expected_head: ExportHead,
    pub next_head: ExportHead,
    pub records: TreeRecordBatch,
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
    async fn load_node(&self, node_id: &NodeId) -> Result<Option<TreeNodeRecord>>;
    async fn load_nodes(&self, node_ids: &[NodeId]) -> Result<Vec<TreeNodeRecord>>;
    async fn load_child_edges(
        &self,
        lookups: &[TreeEdgeLookup],
    ) -> Result<Vec<TreeEdgeRecord>>;
    async fn load_leaf_refs(
        &self,
        node_ids: &[NodeId],
    ) -> Result<Vec<TreeLeafRefRecord>>;
    async fn publish_tree_update(
        &self,
        request: PublishTreeUpdate,
    ) -> Result<PublishTreeUpdateOutcome>;
}
```

This shape is deliberately lower level than today's `SimpleTreeMetadataStore`
and `CowTreeMetadataStore`, but it is not a full-tree load API. The adapter
reads bounded sets of rows by caller-supplied node ids and parent/slot lookup
lists. It does not expose "all descendants" or "all children according to this
format" operations. The server-side tree owners decide which paths to traverse,
which records to create, and which head to publish.

`publish_tree_update` is the adapter's transaction boundary. It inserts the
new tree rows and advances `export_heads` with the expected prior head in one
transaction. If the expected head is stale, no new tree rows from that request
become reachable from a published head.

The exact trait split can change during implementation, but the direction
should not: database adapters persist neutral records and atomic head changes;
storage engines own tree algorithms.

# Tree Algorithm Ownership

`TreeFormat` ids remain in `nbd-control-plane` because they are stored in
`export_heads`. `TreeGeometry`, path/span math, lazy traversal, and mutation
planning live in the server tree code. Any code that decides which nodes to
read, which nodes to create, which path to copy, or which chunks changed
belongs with the server storage engines.

Expected server-side owners:

- `crates/nbd-server/src/engines/tree/geometry.rs` or equivalent shared tree
  module for runtime tree paths, spans, and `TreeFormat` interpretation;
- `crates/nbd-server/src/engines/tree/read.rs` for lazy tree readers that load
  only the metadata paths needed by a read, write, or compaction operation;
- `crates/nbd-server/src/engines/simple_durable/mutable_tree.rs` for lazy
  simple tree materialization and mutable chunk commits;
- `crates/nbd-server/src/engines/wal_durable/compaction.rs` for COW path-copy,
  changed-chunk publication, and compare-and-swap head advancement.

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
Dependency direction:

nbdcli -> nbd-control-plane -> nbd-control-plane-core
                            -> nbd-control-plane-sqlite
nbd-server -> nbd-control-plane -> nbd-control-plane-core
                                -> nbd-control-plane-sqlite
nbd-control-plane-sqlite -> nbd-control-plane-core

There is no dependency from an adapter crate back to the public facade.

crates/nbd-control-plane/
  src/
    lib.rs                 # public facade, core re-exports, open_catalog
    catalog_url.rs         # parsed provider/url value
    diagnostics.rs         # provider-level diagnostics shared by CLIs

crates/nbd-control-plane-core/
  src/
    lib.rs                 # storage-neutral API facade
    service.rs             # catalog service traits and handle bundle
    export.rs              # export identity, descriptor, head, lifecycle types
    tree.rs                # storage-neutral tree rows and lazy row-read traits
    tree_format.rs         # stored TreeFormat ids, no runtime geometry
    error.rs               # storage-neutral catalog errors

crates/nbd-control-plane-sqlite/
  src/
    lib.rs                 # SQLite adapter construction
    adapter.rs             # SQLiteCatalog implementation and opening
    export_rows.rs         # SQLite export row mapping
    tree_rows.rs           # SQLite tree row mapping
    transaction.rs         # SQLite transaction helpers
    error.rs               # SQLite-to-catalog error mapping

crates/nbd-server/
  src/
    server.rs              # opens through facade, no adapter imports
    registry/
    engines/
      tree/
        geometry.rs        # runtime TreeFormat interpretation and path math
        read.rs            # lazy metadata reader/editor helpers
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
- Tree metadata is loaded on demand by path or by bounded child/leaf batches.
  Export open and ordinary reads must not scan every row reachable from the
  current root.
- Row-read APIs are bounded by explicit caller-supplied node ids and
  parent/slot lookup lists. The adapter does not infer fanout or expand a
  subtree.
- `simple_mutable_tree` records are export-private and may refer to mutable
  blobs.
- `cow_immutable_tree` records are immutable after publication and may be shared
  by cloned exports.
- Tree record insertion and head publication are atomic with respect to the
  expected prior head.
- The database adapter may reject malformed record batches, but it does not
  choose the sparse tree update plan.
- Server engine code does not import concrete catalog adapters.
- Storage-neutral control-plane core code does not depend on `sqlx`.
- `nbd-server` source code does not import or name SQLite, PostgreSQL, `sqlx`,
  adapter row types, or database table names. It may call the public
  `nbd-control-plane` facade.

# Implementation Constraints

This is a refactor with permission to break unreleased catalog compatibility.
It does not need a compatibility migration plan.

The eventual execution series still has to preserve commit correctness:

- each commit should build and pass its relevant tests;
- each commit should leave docs and code truthful for that checkout;
- schema/API changes can be breaking, but the same commit must update the
  callers, tests, and docs that rely on the changed contract;
- code motion should not hide semantic changes;
- proof for a moved or newly exposed primitive should travel with the commit
  that introduces that primitive.

The implementation should still proceed in ownership order:

- split storage-neutral control-plane core modules from the concrete SQLite
  adapter;
- keep `nbd-control-plane` as the public facade and factory. It re-exports the
  core API and delegates provider opening to adapter crates;
- add `nbd-control-plane-sqlite` and move `SQLiteExportCatalog`, SQL queries,
  row mapping, schema assumptions, and SQLite tests into it;
- keep `open_catalog` in `nbd-control-plane`, but make it a facade call that
  returns storage-neutral service handles instead of exposing adapter types;
- move tree read/write algorithms out of the SQLite adapter. Introduce lazy
  row-read primitives plus atomic publish, and let simple durable and WAL
  durable code build record batches;
- update tests so pure engine behavior uses fake or in-memory trait
  implementations. Keep SQLite integration tests in the SQLite adapter crate;
- remove compatibility imports once callers use focused owner modules.

The paused bounded-tree implementation should be treated as implementation
evidence, not as the final topology. The refactor can reuse its tests and
logic, but the final series should place behavior under the ownership model in
this document.

# Validation

- `cargo test -p nbd-control-plane-core` proves storage-neutral domain
  behavior.
- `cargo test -p nbd-control-plane` proves facade provider selection and
  shared diagnostics.
- `cargo test -p nbd-control-plane-sqlite` proves SQLite adapter behavior.
- `cargo test -p nbd-server` proves engine behavior without requiring direct
  SQLite imports in engine modules.
- server tree tests should include an instrumented store proving a large sparse
  export read or write loads only the touched metadata path and bounded sibling
  batches, not the whole tree.
- `cargo test --workspace` proves the integrated workspace.
- `cargo clippy --workspace --all-targets -- -D warnings` catches dependency
  and import cleanup issues.
- `rg "SQLite|Sqlite|sqlx|tree_nodes|export_heads" crates/nbd-server/src`
  should have no hits. `nbd-server` should talk to the
  `nbd-control-plane` facade, not to concrete adapters or schema details.
- `cargo tree -p nbd-control-plane-core` should not include `sqlx` after the
  split.
- `cargo tree -p nbd-server` may include adapter dependencies transitively
  through `nbd-control-plane`; that is acceptable. Source imports, public API
  exposure, and module ownership are the boundary being enforced.

# Risks

- A trait that is too high level will keep tree algorithms hidden in the
  adapter and recreate the current problem.
- A trait that is too low level will leak table shape into server code and make
  PostgreSQL harder instead of easier.
- Keeping adapter construction in the `nbd-control-plane` facade means the
  facade may have transitive database dependencies. Reviewers must enforce that
  concrete adapter names do not leak through the facade API or into
  `nbd-server` source modules.
- Splitting `model.rs` only for import neatness could become noise. The split
  should follow ownership boundaries, not line-count aesthetics.
- Lazy metadata reads can become too chatty if every edge lookup is a separate
  database round trip. The row-read API should support bounded batching by node
  id, parent id, and parent slot selection.
- The facade/core split adds one crate. That is justified only because it
  prevents adapter/facade dependency cycles while keeping `nbd-server` source
  behind a single public control-plane abstraction.

# Alternatives Considered

## Keep all code in `nbd-control-plane`

This preserves the current shape but keeps the storage backend, domain API, and
facade coupled in one source crate. It also keeps large import lists and
concrete adapter types too close to server-facing APIs.

## Move SQLite into `nbd-server`

This would remove SQLite from `nbd-control-plane`, but it puts database
knowledge in the runtime crate and makes `nbdcli` need a different path for the
same catalog. It also violates the goal that the database implementation live in
one adapter place.

## Rename `nbd-control-plane`

A new name such as `nbd-catalog` may eventually be clearer, but renaming does
not solve the boundary by itself. The important first step is to separate
storage-neutral contracts from concrete persistence.

## Make `nbdcli` and `nbd-server` depend on adapters directly

This keeps the facade thinner but makes every frontend repeat provider
selection and risks leaking concrete adapter names into server source. The
chosen design keeps provider selection in `nbd-control-plane` instead.

# Open Questions

- What is the smallest compatibility facade worth keeping while existing code
  moves from `model.rs` root imports to focused owner modules?

# Design Exit Criteria

The design is ready for `$review-plan` when:

- the team agrees that `nbd-control-plane` is the public facade/factory and
  `nbd-control-plane-core` is the storage-neutral API owner;
- the concrete SQLite implementation has a named adapter owner;
- the server-side owners for simple mutable tree and COW tree algorithms are
  accepted;
- lazy tree metadata loading and atomic publish semantics are accepted;
- the import-list smell is captured as a topology review rule, not a mechanical
  style limit.

# Recommended Next Step

Run `$review-plan` on this draft before any execution planning. The review
should focus on the facade/core/adapter boundary, the tree-record trait level,
and whether source-level leakage checks are strong enough to keep `nbd-server`
core code independent of the backing database.
