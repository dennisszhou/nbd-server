Title: Export Catalog Architecture
Date: 2026-05-01
Status: draft

# Problem

The system needs a durable metadata source for export lifecycle, current
serving heads, WAL checkpoints, simple mutable tree metadata, and future
immutable COW tree metadata. This metadata is shared by
`ExportLifecycleManager`, `LocalExportRegistry`, `CommittedTreeReader`,
`SimpleMutableTree`, and `CompactionManager`.

The API should avoid long parameter lists. Callers should pass structured
requests and receive structured records.

# Terminology

The NBD protocol calls a named network block device an export. The
architecture keeps `Export`, `ExportCatalog`, and the `exports` table for that
protocol-aligned concept. In product language, an export is the durable network
block device that clients mount by name.

`exports` owns stable identity and lifecycle. `export_heads` owns the current
serving view for each export. There is no separate export generation table in
the active catalog model.

# Goal

Define `ExportCatalog` as the durable metadata API for:

- creating exports;
- cloning exports from the latest committed checkpoint;
- loading exports for NBD open;
- listing and inspecting exports;
- marking exports deleted when lifecycle orchestration has acquired the
  per-export lease;
- storing the current export head;
- inserting simple mutable tree metadata for `SimpleDurableEngine`;
- inserting immutable tree metadata for WAL/COW compaction;
- publishing root/checkpoint updates by advancing `export_heads`.

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
  checkpoint_wal_seq
  size_bytes
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
This avoids creating malformed empty internal nodes.

`export_heads` is the serving source of truth. Normal `simple_mutable_tree`
writes do not append root history, and future COW checkpoint publication
should advance the current head rather than reintroducing a generation table.

The catalog stores blob references. Blob bytes live behind `StorageEngine`.

# Data Structures

Use explicit structs at API boundaries.

```rust
struct ExportDescriptor {
    id: ExportId,
    name: ExportName,
    block_size: u64,
    engine_kind: ExportEngineKind,
    state: ExportState,
    created_at: Timestamp,
    updated_at: Timestamp,
    deleted_at: Option<Timestamp>,
}

struct ExportMeta {
    id: ExportId,
    name: ExportName,
    block_size: u64,
    engine_kind: ExportEngineKind,
    state: ExportState,
    head: ExportHead,
    created_at: Timestamp,
    updated_at: Timestamp,
    deleted_at: Option<Timestamp>,
}

struct ExportHead {
    layout_kind: ExportLayoutKind,
    root_node_id: Option<NodeId>,
    size_bytes: u64,
    checkpoint_wal_seq: WalSeq,
}

enum ExportLayoutKind {
    MemoryEmpty,
    SimpleMutableTree,
    CowImmutableTree,
}

enum ExportState {
    Active,
    Deleted,
}
```

`ExportDescriptor` is exports-only metadata for open paths. It must not carry
`export_heads` root or checkpoint state. Durable engines load their current
head/tree snapshot separately so compaction can advance `export_heads` without
invalidating an open already holding a descriptor.

`ExportMeta` remains the operator-facing joined view used by create, inspect,
list, and publication outcomes.

Lifecycle request structs:

```rust
struct CreateExport {
    name: ExportName,
    size_bytes: u64,
    block_size: u64,
    engine_kind: ExportEngineKind,
}

struct CloneExport {
    source: ExportName,
    destination: ExportName,
}

struct DeleteExport {
    name: ExportName,
}
```

Tree publication structs:

```rust
struct SimpleChunkRef {
    chunk_index: ChunkIndex,
    blob_key: BlobKey,
    len_bytes: u64,
}

struct TreeNodeRecord {
    id: NodeId,
    layout_kind: ExportLayoutKind,
    kind: NodeKind,
    level: u8,
    span_start_bytes: u64,
    span_len_bytes: u64,
    blob_key: Option<BlobKey>,
}

struct TreeEdgeRecord {
    parent_node_id: NodeId,
    slot: u8,
    child_node_id: NodeId,
}

struct TreeBatch {
    nodes: Vec<TreeNodeRecord>,
    edges: Vec<TreeEdgeRecord>,
}

struct PublishCompaction {
    export_id: ExportId,
    expected_base: ExportHead,
    tree_batch: TreeBatch,
    new_root_node_id: Option<NodeId>,
    compacted_through: WalSeq,
}

enum PublishCompactionOutcome {
    Published(ExportMeta),
    AlreadyCovered(ExportMeta),
    StalePlan(ExportMeta),
}
```

# API Shape

Conceptual API:

```rust
trait ExportCatalog {
    async fn create_export(&self, request: CreateExport)
        -> Result<ExportMeta>;

    async fn clone_export(&self, request: CloneExport)
        -> Result<ExportMeta>;

    async fn delete_export(&self, request: DeleteExport)
        -> Result<()>;

    async fn load_export(&self, name: ExportName)
        -> Result<ExportMeta>;

    async fn load_export_descriptor(&self, name: ExportName)
        -> Result<ExportDescriptor>;

    async fn load_export_head(&self, export_id: &ExportId)
        -> Result<ExportHead>;

    async fn list_exports(&self, filter: ExportListFilter)
        -> Result<Vec<ExportMeta>>;

    async fn publish_compaction(&self, update: PublishCompaction)
        -> Result<PublishCompactionOutcome>;
}
```

The simple durable request path should use a narrower metadata boundary, such
as `SimpleTreeMetadataStore`, so only `SimpleMutableTree` mutates simple tree
rows on behalf of the engine.

# Create Export

Create initializes a new export with a current head and empty WAL checkpoint:

```text
insert exports row:
  state = active
  deleted_at = null
  engine_kind = requested engine kind

insert export_heads row:
  layout_kind = layout implied by engine_kind
  root_node_id = null
  checkpoint_wal_seq = 0
  size_bytes = requested size
```

`root_node_id = null` means the current tree is all zeroes.

`memory` creates `layout_kind = memory_empty`. `simple_durable` creates
`layout_kind = simple_mutable_tree`.

# Clone Export

Clone is a `cow_immutable_tree` operation. It copies the source export's latest
committed COW root. It does not include the source export's uncheckpointed WAL.

Clone requires the source head to have a non-null `root_node_id`. A null root
is the all-zero committed tree, so clone should reject it with an operator
error that the source snapshot is empty rather than silently creating another
empty export.

```text
source = load active source export
verify source.root_node_id is not null
insert destination exports row
insert destination export_heads row:
  layout_kind = cow_immutable_tree
  root_node_id = source.root_node_id
  checkpoint_wal_seq = 0
```

The destination has a new export identity and its own WAL. Future writes to the
destination replay from its own WAL on top of the shared committed root.

# Delete Export

`ExportCatalog.delete_export` marks catalog state deleted. It does not inspect
etcd leases by itself.

The current local prototype lets `nbdcli delete` call this catalog primitive
directly because the lease/lifecycle layer is not implemented yet. That is a
prototype shortcut. The target delete path goes through
`ExportLifecycleManager` before calling the catalog.

Delete flow:

```text
ExportLifecycleManager acquires the per-export delete lease
  -> if lease acquisition fails, return ExportBusy
  -> ExportCatalog marks state = deleted
  -> ExportLifecycleManager releases the delete lease
```

This keeps the catalog as a metadata primitive. Open/delete race prevention
belongs to `ExportLifecycleManager`, which composes the catalog with
`ExportLeaseStore`.

Physical deletion of tree nodes, blobs, and WAL records belongs to future GC.
Delete never immediately removes committed data because future COW exports may
share the same immutable nodes and blobs, and simple durable may leave orphan
mutable blobs after file-first failures.

# Root And Checkpoint Publication

`publish_compaction` is a WAL/COW operation. It is called after compaction has
written any new blobs. It inserts immutable tree metadata and
advances the current head in one transaction so the catalog never exposes a
partially published tree. Publication must preserve the size from the head it
compacted unless compaction explicitly observes and incorporates a newer resize
head.

Publication advances the current head in a single catalog transaction:

```text
begin transaction
  load latest export row
  load current export_heads row
  verify state = active
  if current.checkpoint_wal_seq >= compacted_through:
    return AlreadyCovered
  if current root/checkpoint/size/layout != expected_base:
    return StalePlan
  insert tree metadata batch
  update export_heads:
    root_node_id = new_root_node_id
    checkpoint_wal_seq = compacted_through
    size_bytes = current.size_bytes
commit
```

`AlreadyCovered` is a successful no-op for duplicate or slower compaction jobs.
`StalePlan` tells the caller to discard unpublished output and replan from the
current database head if the original target is still useful. This keeps racing
compaction attempts idempotent without adding a separate generation table.

Callers should not insert immutable compaction tree metadata separately from
head publication. A failed or racing publication may leave already-written
blob files as future-GC garbage, but it must not expose partial tree metadata
through the catalog head.

Future resize should update the current head with the new `size_bytes` and may
need its own fencing against checkpoint publication.
If resize and compaction can run concurrently, checkpoint publication must avoid
publishing a head update that rolls the size backward. That conflict policy
needs a dedicated resize design before online resize is implemented.

# Close-Time Compaction

Close-time compaction is an intended feature. When an export mount closes
cleanly, the server should enqueue background compaction for that export as
part of the close workflow.

Close-time compaction goal:

- reduce WAL replay work for the next open;
- move recent writes into the committed copy-on-write tree;
- advance `checkpoint_wal_seq` when compaction succeeds.

Close does not wait for compaction to finish. If close-time compaction fails,
the export can stay closed as long as acknowledged writes remain durable in the
WAL. Startup recovery will replay records after the last catalog checkpoint.

This makes close-time compaction an operational quality feature, not part of
the write durability contract.

# Invariants

- `ExportCatalog` is the durable metadata source.
- API calls use structured request/response types.
- `exports` owns stable identity and lifecycle.
- `export_heads` owns the current serving layout, root, size, and checkpoint.
- Every export has exactly one current head.
- New exports create the current head transactionally with the export row.
- `simple_mutable_tree` writes update simple tree metadata through
  `SimpleMutableTree`, not by appending generations.
- Cloned COW exports create their own head and checkpoint zero.
- Clone copies the latest non-empty committed root only.
- Clone rejects a source with `root_node_id = null` as an empty committed
  snapshot.
- Clone does not include source uncheckpointed WAL records.
- `root_node_id = null` means an all-zero current tree.
- Root/checkpoint publication advances the current head.
- `publish_compaction` inserts immutable tree metadata and advances the current
  head transactionally.
- `checkpoint_wal_seq` advances monotonically.
- Clean export close enqueues background close-time compaction.
- Delete race prevention belongs to `ExportLifecycleManager`.
- Delete is logical; physical cleanup belongs to GC.
- The catalog stores blob references, not blob bytes.
- Blob bytes live behind `StorageEngine`.

# Open Questions

- Whether the first catalog implementation should use SQLite or structured
  files.
- Exact catalog error type for stale or no-op checkpoint publication attempts.
