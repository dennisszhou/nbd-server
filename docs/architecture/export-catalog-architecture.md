Title: Export Catalog Architecture
Date: 2026-05-01
Status: draft

# Problem

The system needs a durable metadata source for export lifecycle, committed tree
roots, WAL checkpoints, and immutable tree metadata. This metadata is shared by
`ExportLifecycleManager`, `ExportOpener`, `CommittedTreeReader`, and
`CompactionManager`.

The API should avoid long parameter lists. Callers should pass structured
requests and receive structured records.

# Terminology

The NBD protocol calls a named network block device an export. The
architecture keeps `Export`, `ExportCatalog`, and the `exports` table for that
protocol-aligned concept. In product language, an export is the durable network
block device that clients mount by name.

`exports` owns stable identity and lifecycle. `export_generations` owns the
append-only committed-root history for that export.

# Goal

Define `ExportCatalog` as the durable metadata API for:

- creating exports;
- cloning exports from the latest committed checkpoint;
- loading exports for NBD open;
- listing and inspecting exports;
- marking exports deleted when lifecycle orchestration has acquired the
  per-export lease;
- inserting immutable tree metadata;
- publishing new root/checkpoint generations after compaction.

# Catalog-Owned State

The catalog owns:

```text
exports
  id
  name
  size_bytes
  block_size
  state
  created_at
  updated_at
  deleted_at

export_generations
  id
  export_id
  generation
  root_node_id
  checkpoint_wal_seq
  created_at

  unique(export_id, generation)

nodes
  id
  kind
  level
  span_start_bytes
  span_len_bytes
  blob_key/null
  created_at

node_edges
  parent_node_id
  slot
  child_node_id
```

`root_node_id = null` on an export generation represents an all-zero committed
tree. This avoids creating malformed empty internal nodes.

The catalog stores blob references. Blob bytes live behind `StorageEngine`.

# Data Structures

Use explicit structs at API boundaries.

```rust
struct ExportMeta {
    id: ExportId,
    name: ExportName,
    size_bytes: u64,
    block_size: u64,
    state: ExportState,
    committed: CommittedRoot,
    created_at: Timestamp,
    updated_at: Timestamp,
    deleted_at: Option<Timestamp>,
}

struct CommittedRoot {
    root_node_id: Option<NodeId>,
    checkpoint_wal_seq: WalSeq,
    generation: ExportGeneration,
}

enum ExportState {
    Active,
    Deleted,
}
```

Lifecycle request structs:

```rust
struct CreateExport {
    name: ExportName,
    size_bytes: u64,
    block_size: u64,
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
struct TreeNodeRecord {
    id: NodeId,
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

struct PublishCheckpoint {
    export_id: ExportId,
    new_root_node_id: Option<NodeId>,
    compacted_through: WalSeq,
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

    async fn list_exports(&self, filter: ExportListFilter)
        -> Result<Vec<ExportMeta>>;

    async fn insert_tree_batch(&self, batch: TreeBatch)
        -> Result<()>;

    async fn publish_checkpoint(&self, update: PublishCheckpoint)
        -> Result<ExportMeta>;
}
```

# Create Export

Create initializes a new export with its own generation history and empty WAL
checkpoint:

```text
insert exports row:
  state = active
  deleted_at = null

insert export_generations row:
  generation = 0
  root_node_id = null
  checkpoint_wal_seq = 0
```

`root_node_id = null` means the committed tree is all zeroes.

# Clone Export

Clone copies the source export's latest committed root. It does not include the
source export's uncheckpointed WAL.

```text
source = load active source export
insert destination exports row
insert destination export_generations row:
  generation = 0
  root_node_id = source.root_node_id
  checkpoint_wal_seq = 0
```

The destination has a new export identity and its own WAL. Future writes to the
destination replay from its own WAL on top of the shared committed root.

# Delete Export

`ExportCatalog.delete_export` marks catalog state deleted. It does not inspect
etcd leases by itself.

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
Delete never immediately removes committed data because other exports may share
the same immutable nodes and blobs.

# Root And Checkpoint Publication

`publish_checkpoint` is called after compaction has written any new blobs and
inserted the corresponding immutable tree metadata.

Publication appends a new export generation in a single catalog transaction:

```text
begin transaction
  load latest export row
  load latest export_generation row
  verify state = active
  verify compacted_through > checkpoint_wal_seq
  insert export_generations row:
    generation = previous_generation + 1
    root_node_id = new_root_node_id
    checkpoint_wal_seq = compacted_through
commit
```

Callers do not pass `expected_generation`. The catalog owns loading the latest
generation. The architecture relies on a single checkpoint publisher per export
until writer fencing or multi-publisher compaction is designed.

# Close-Time Compaction

Close-time compaction is an intended feature. When an export mount closes
cleanly, the server should try to compact that export before completing close
or as part of the close workflow.

Close-time compaction goal:

- reduce WAL replay work for the next open;
- move recent writes into the committed copy-on-write tree;
- advance `checkpoint_wal_seq` when compaction succeeds.

If close-time compaction fails, the export can still close as long as
acknowledged writes remain durable in the WAL. Startup recovery will replay
records after the last catalog checkpoint.

This makes close-time compaction an operational quality feature, not part of
the write durability contract.

# Invariants

- `ExportCatalog` is the durable metadata source.
- API calls use structured request/response types.
- `exports` owns stable identity and lifecycle.
- `export_generations` owns committed roots, checkpoints, and generation
  numbers.
- Every export has at least one export generation.
- New exports create generation zero transactionally with the export row.
- Cloned exports create their own generation zero and checkpoint zero.
- Export generations are append-only.
- Clone copies the latest committed root only.
- Clone does not include source uncheckpointed WAL records.
- `root_node_id = null` means an all-zero committed tree.
- `publish_checkpoint` appends the next generation.
- `checkpoint_wal_seq` advances monotonically.
- Clean export close attempts close-time compaction.
- Delete race prevention belongs to `ExportLifecycleManager`.
- Delete is logical; physical cleanup belongs to GC.
- The catalog stores blob references, not blob bytes.
- Blob bytes live behind `StorageEngine`.

# Open Questions

- Whether the first catalog implementation should use SQLite or structured
  files.
- Whether root history should be recorded now for debugging or deferred to GC.
- Exact catalog error type for stale or no-op checkpoint publication attempts.
