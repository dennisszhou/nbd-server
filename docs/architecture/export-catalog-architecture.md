Title: Export Catalog Architecture
Date: 2026-05-12
Status: approved

# Problem

The system needs a durable metadata source for export lifecycle, current
serving heads, tree metadata, and WAL checkpoints. That metadata is consumed by
operator commands, `LocalExportRegistry`, simple durable storage, and WAL
durable compaction.

The public catalog API must hide the backing database. Production callers use
the `nbd-control-plane` facade and storage-neutral traits; concrete SQLite
details stay inside `nbd-control-plane-sqlite` so a future PostgreSQL adapter
can implement the same contracts.

# Ownership

`nbd-control-plane` is the public facade. It parses `CatalogUrl`, exposes
`open_catalog` and `doctor_catalog`, re-exports storage-neutral API types, and
chooses a concrete adapter.

`nbd-control-plane-core` owns storage-neutral domain values, request and
response structs, service traits, diagnostic records, and catalog errors. It
must not depend on `sqlx`, SQLite, PostgreSQL, migration SQL, table-specific
row structs, or runtime tree geometry.

`nbd-control-plane-sqlite` owns SQLite connection handling, SQL statements, row
mapping, transaction boundaries, SQLite diagnostics, schema assumptions, and
SQLite integration tests.

`nbd-server` owns runtime behavior: export admission, engine execution, tree
geometry derived from stored `TreeFormat` ids, lazy tree traversal, simple
mutable writes, WAL read views, COW compaction planning, and blob/WAL I/O.
Server production source must not import concrete catalog adapters, `sqlx`,
catalog table names, or adapter row types.

# Terminology

The NBD protocol calls a named network block device an export. The
architecture keeps `Export`, `ExportCatalog`, and the `exports` table for that
protocol-aligned concept.

`exports` owns stable identity and lifecycle. `export_heads` owns the current
serving view for each export. There is no separate export generation table in
the active catalog model.

# Catalog-Owned State

The catalog owns:

```text
exports
  id
  name
  engine_kind
  block_size
  state
  created_at
  updated_at
  deleted_at

export_heads
  export_id primary key
  layout_kind
  root_node_id
  tree_format
  size_bytes
  base_wal_seq
  updated_at

tree_nodes
  id
  layout_kind
  owner_export_id/null
  kind
  level
  span_start_bytes
  span_len_bytes
  created_at

tree_edges
  parent_node_id
  slot
  child_node_id

tree_leaf_refs
  node_id
  storage_kind
  storage_key
  len_bytes
  created_at
```

`root_node_id = null` on `export_heads` represents an all-zero current tree.
`tree_format = null` is valid only for `memory_empty` heads. Tree-backed heads
must carry a stored `TreeFormat`, currently `bounded_32_v1`.

`export_heads` is the serving source of truth. Simple mutable writes and WAL
compaction advance the current head rather than appending generation rows.

The catalog stores one-component blob references. Blob bytes live behind the
configured `BlobStore`, which resolves those ids to local files or S3 objects
from process config.

# Data Structures

Use explicit structs at API boundaries.

```rust
enum TreeFormat {
    Bounded32V1,
}

enum ExportHead {
    MemoryEmpty(MemoryExportHead),
    SimpleMutableTree(SimpleMutableTreeHead),
    CowImmutableTree(CowImmutableTreeHead),
}

struct MemoryExportHead {
    size_bytes: u64,
}

struct SimpleMutableTreeHead {
    size_bytes: u64,
    root_node_id: Option<NodeId>,
    tree_format: TreeFormat,
}

struct CowImmutableTreeHead {
    size_bytes: u64,
    root_node_id: Option<NodeId>,
    base_wal_seq: WalSeq,
    tree_format: TreeFormat,
}
```

`ExportHead` is typed by layout so memory heads cannot carry root nodes or WAL
state, simple mutable heads cannot carry WAL state, and COW immutable heads
carry the committed `base_wal_seq`. Tree-backed heads carry `tree_format`
because tree shape is durable state, not an adapter default.

Tree record structs are storage-neutral rows. They describe what should be
persisted, not how to traverse or update the tree.

```rust
struct TreeNodeRecord {
    id: NodeId,
    layout_kind: ExportLayoutKind,
    owner_export_id: Option<ExportId>,
    kind: TreeNodeKind,
    level: u16,
    span_start_bytes: u64,
    span_len_bytes: u64,
}

struct TreeEdgeRecord {
    parent_node_id: NodeId,
    slot: u16,
    child_node_id: NodeId,
}

struct TreeLeafRefRecord {
    node_id: NodeId,
    storage_kind: TreeStorageKind,
    storage_key: BlobKey,
    len_bytes: u64,
}

struct TreeRecordBatch {
    nodes: Vec<TreeNodeRecord>,
    edges: Vec<TreeEdgeRecord>,
    leaf_refs: Vec<TreeLeafRefRecord>,
}
```

# API Shape

The facade opens storage-neutral service handles:

```rust
async fn open_catalog(url: &CatalogUrl) -> Result<CatalogHandle>;
async fn doctor_catalog(url: &CatalogUrl) -> Vec<CatalogDoctorCheck>;

struct CatalogHandle {
    export_catalog: Arc<dyn ExportCatalog>,
    tree_record_store: Arc<dyn TreeRecordStore>,
}
```

`ExportCatalog` owns export lifecycle and current-head reads:

```rust
trait ExportCatalog {
    async fn create_export(&self, request: CreateExport)
        -> Result<ExportRecord>;
    async fn clone_export(&self, request: CloneExport)
        -> Result<CloneExportResult>;
    async fn delete_export(&self, request: DeleteExport)
        -> Result<()>;
    async fn load_export(&self, name: ExportName)
        -> Result<ExportRecord>;
    async fn load_export_descriptor(&self, name: ExportName)
        -> Result<ActiveExportDescriptor>;
    async fn load_export_head(&self, export_id: &ExportId)
        -> Result<ExportHead>;
    async fn inspect_export(&self, request: InspectExport)
        -> Result<ExportRecord>;
    async fn list_exports(&self, request: ListExports)
        -> Result<Vec<ExportRecord>>;
}
```

`TreeRecordStore` owns bounded row reads and atomic publication:

```rust
trait TreeRecordStore {
    async fn load_node(&self, node_id: &NodeId)
        -> Result<Option<TreeNodeRecord>>;
    async fn load_nodes(&self, node_ids: &[NodeId])
        -> Result<Vec<TreeNodeRecord>>;
    async fn load_child_edges(&self, lookups: &[TreeEdgeLookup])
        -> Result<Vec<TreeEdgeRecord>>;
    async fn load_leaf_refs(&self, node_ids: &[NodeId])
        -> Result<Vec<TreeLeafRefRecord>>;
    async fn publish_tree_update(&self, request: PublishTreeUpdate)
        -> Result<PublishTreeUpdateOutcome>;
}
```

The adapter reads only bounded sets of rows requested by the caller. It does
not expose "load all descendants", "load whole tree", or "expand this format"
operations. Server tree code decides which paths to traverse, which records to
create, and which head to publish.

# Create Export

Create initializes a new export and current head in one catalog transaction:

```text
insert exports row:
  state = active
  deleted_at = null
  engine_kind = requested engine kind

insert export_heads row:
  layout_kind = layout implied by engine_kind
  root_node_id = null
  tree_format = null for memory_empty
  tree_format = bounded_32_v1 for tree-backed layouts
  base_wal_seq = 0
  size_bytes = requested size
```

`memory` creates `layout_kind = memory_empty`. `simple_durable` creates
`layout_kind = simple_mutable_tree`. `wal_durable` creates
`layout_kind = cow_immutable_tree`.

# Clone Export

Clone is a `cow_immutable_tree` operation. It copies the source export's latest
committed COW root and tree format. It does not include the source export's
uncheckpointed WAL.

Clone requires the source head to have a non-null `root_node_id`. A null root
is the all-zero committed tree, so clone rejects it with an operator error
instead of silently creating another empty export.

# Tree Publication

`publish_tree_update` is the catalog transaction boundary for both simple
mutable tree commits and WAL/COW compaction. It inserts new tree records and
advances `export_heads` with an expected prior head in one transaction.

```text
begin transaction
  load latest export row and head
  verify state = active
  if current head != expected_head:
    rollback and return StaleHead(current)
  insert tree node, edge, and leaf-ref records
  update export_heads where current columns still match expected_head
  if no head row was updated:
    rollback and return StaleHead(current)
  update exports.updated_at
commit
```

If a publish sees a stale head, none of that request's new tree rows become
reachable from a published head. Already-written blob files may remain as
future-GC garbage, but a failed or racing publication must not expose partial
tree metadata through the catalog head.

# Server Tree Ownership

Tree format ids are catalog state. Runtime geometry and algorithms are server
behavior.

Current owners:

- `crates/nbd-server/src/engines/tree/geometry.rs` interprets stored
  `TreeFormat` ids as runtime fanout, levels, spans, and paths.
- `crates/nbd-server/src/engines/tree/read.rs` loads metadata lazily for the
  requested paths and treats missing paths as zero-filled data.
- `crates/nbd-server/src/engines/simple_durable/mutable_tree.rs` owns simple
  mutable tree commits and cache refresh.
- `crates/nbd-server/src/engines/wal_durable/compaction.rs` owns COW path-copy,
  unchanged-subtree reuse, changed-chunk selection, and checkpoint publication.

# Doctor

`doctor_catalog` is the facade entry point for catalog diagnostics. Binaries
parse their config and translate `CatalogDoctorCheck` records into their local
report format.

SQLite diagnostics live in `nbd-control-plane-sqlite`: the adapter checks that
the catalog file exists, that it is a regular file, that it can be opened, and
that the expected schema can answer a lightweight catalog query. SQLite opening
uses `create_if_missing(false)` because Prisma migrations own database
creation and schema setup.

# Invariants

- `exports` owns stable identity and lifecycle.
- `export_heads` owns the current serving layout, root, size, tree format, and
  checkpoint.
- Every export has exactly one current head.
- New exports create the current head transactionally with the export row.
- Tree-backed heads carry a stored `TreeFormat`.
- `root_node_id = null` means an all-zero current tree.
- Tree metadata is loaded on demand by path or bounded lookup lists.
- The adapter does not infer fanout, expand subtrees, or choose sparse tree
  update plans.
- `simple_mutable_tree` records are export-private and may reference mutable
  blobs.
- `cow_immutable_tree` records are immutable after publication and may be
  shared by cloned exports.
- Tree record insertion and head publication are atomic with respect to the
  expected prior head.
- Clone copies the latest non-empty committed COW root and source tree format.
- Clone does not include source uncheckpointed WAL records.
- Delete race prevention belongs to future lifecycle orchestration.
- Delete is logical; physical cleanup belongs to future GC.
- Blob bytes live behind the configured `BlobStore`.
- Production server and CLI source do not import concrete catalog adapters,
  `sqlx`, adapter row types, or catalog table names.

# Open Questions

- PostgreSQL adapter implementation and migrations.
- Tree garbage collection for unreachable rows and blobs.
- Historical checkpoint browsing.
- Cross-format clone, once more than one tree format exists.
- Lifecycle lease integration for open/delete race prevention.
