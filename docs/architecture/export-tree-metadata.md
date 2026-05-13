Title: Export Tree Metadata
Date: 2026-05-12
Status: approved

# Problem

The system needs durable metadata for two related but different tree layouts:

- `simple_mutable_tree`, the current direct-commit local layout used by
  `SimpleDurableEngine`;
- `cow_immutable_tree`, the WAL/compaction layout used for clone, checkpoints,
  and immutable local/S3-compatible blobs.

The old version of this document described only the future immutable COW model.
That made it too easy to read immutable-node and generation rules as if they
also applied to `simple_mutable_tree`.

# Goal

Use the control-plane catalog traits to track export lifecycle, the current
export head, and layout-specific sparse tree metadata without conflating
mutable direct-commit state with immutable checkpoint history.

The current serving source of truth is `export_heads`.

For `simple_mutable_tree`, updates mutate export-private metadata under the
current head through `SimpleMutableTree` and `TreeRecordStore`.

For `cow_immutable_tree`, compaction creates immutable nodes and publishes a
new root/checkpoint by advancing `export_heads`.

# Control-Plane Responsibilities

`ExportCatalog` owns durable export lifecycle and current-head metadata:

- create exports;
- clone exports;
- inspect/list exports;
- logically delete exports;
- load exports-only descriptors on NBD open;
- store export size, block size, and lifecycle state;
- load one current `export_heads` row per export.

`TreeRecordStore` owns tree row persistence:

- read bounded sets of nodes, edges, and leaf refs by caller-supplied ids or
  parent/slot lookups;
- insert tree record batches;
- publish a new current head atomically with an expected prior head.

It is not the local active export registry.

# Conceptual Schema

The exact database can evolve, but the model should preserve these concepts:

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
  layout_kind        -- memory_empty | simple_mutable_tree | cow_immutable_tree
  root_node_id
  tree_format        -- null for memory_empty, bounded_32_v1 for tree-backed
  base_wal_seq
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
  storage_kind       -- mutable_blob | immutable_blob
  storage_key        -- one-component BlobKey
  len_bytes
  created_at
```

`export_heads` is the serving source of truth for every layout. Future COW
checkpoints advance the current head; they do not create a separate generation
history.

`tree_format` is durable head state. Runtime tree geometry is derived from it
inside `nbd-server`, not inferred from database adapter defaults.

Open paths should not treat a previously joined `exports` + `export_heads`
view as stable. They load an exports-only descriptor first, then let the
engine-specific opener load the latest `export_heads` state by export id and
use `TreeRecordStore` for lazy tree metadata reads.

# Layout Semantics

## `simple_mutable_tree`

`simple_mutable_tree` is the direct-commit local layout.

- tree nodes are export-private;
- leaf refs use `storage_kind = mutable_blob`;
- blob files may be replaced in place by `LocalBlobStore`;
- normal writes do not create root history;
- `SimpleMutableTree` is the request-path owner that mutates tree metadata;
- clone and COW sharing are not supported.

The simple tree can use the same sparse geometry and table names as the future
COW tree, but its rows are not immutable COW publication artifacts.

## `cow_immutable_tree`

`cow_immutable_tree` is the WAL/compaction layout.

Published nodes are immutable. Updating an export creates new nodes and moves
the current head to a new immutable root/checkpoint.

The export checkpoint is a global WAL prefix. If the current head has
`root_node_id = R` and `base_wal_seq = S`, root `R` represents every WAL
record with sequence `<= S`. Startup recovery must replay every durable WAL
record with sequence `> S`.

`root_node_id = null` means the current tree is all zeroes.

Leaf refs store blob ids only. They do not store local roots, S3 buckets,
prefixes, endpoints, credentials, or full object URIs. The configured
`BlobStore` resolves each `storage_key` to the active backend location.

# Sparse Tree Shape

Use a sparse tree over logical disk offsets.

Target fanout:

```text
leaf:       32 MiB data blob
level 1:     1 GiB = 32 leaves
level 2:    32 GiB
level 3:     1 TiB
level 4:    32 TiB
```

Internal nodes:

- metadata only;
- sparse child pointers;
- immutable once published for `cow_immutable_tree`;
- export-private for `simple_mutable_tree`.

A materialized internal node should have at least one reachable leaf descendant.
Sparse missing child pointers are valid, but an internal node with no reachable
leaf data is malformed metadata.

Leaf nodes:

- point to full 32 MiB data blobs;
- represent a dense logical leaf range;
- are mutable blob refs for `simple_mutable_tree`;
- are immutable blob refs for `cow_immutable_tree`.

Missing committed data zero-fills.

The current implementation uses this bounded 32-wide shape for both simple
mutable trees and COW immutable trees. Older simple-durable planning text
described a one-level root with direct leaf edges as the first implementation;
that historical detail is no longer the current tree geometry.

# Copy-On-Write Roots And Clone

This section applies only to `cow_immutable_tree`.

The committed tree is a persistent copy-on-write tree. Edges are immutable once
published. Updates create new leaf blobs and new nodes along changed paths to a
new root. Unchanged subtrees are shared by reference.

Clone is O(1) because it copies the source export's current root pointer into a
new export head. Clone requires a non-null source root. A null root represents
the all-zero committed tree, so clone should fail with an operator-visible
empty-snapshot error instead of creating another empty export.

```text
clone src -> dst
  -> require src.root_node_id is not null
  -> create dst export metadata
  -> create dst export_head:
       layout_kind = cow_immutable_tree
       root_node_id = src.root_node_id
       tree_format = src.tree_format
       base_wal_seq = 0
  -> copy no leaf blobs
```

`base_wal_seq` is per export WAL state. The cloned root already contains
the source export's committed data as of the source's latest catalog
checkpoint. The destination export has a new WAL, so it starts with checkpoint
zero and replays only its own future WAL records. Clone does not include the
source export's uncheckpointed WAL.

When the child later compacts writes, it creates a new root for the child only.
The source export keeps pointing at its prior root. Both exports may continue
sharing unchanged immutable nodes and leaf blobs.

`simple_mutable_tree` is intentionally not clone-ready. Its blob files are
mutable and export-private, so copying only a root pointer would not produce a
stable snapshot.

# Root Identity

Roots must be identifiable even if the prototype keeps every old root forever.
The identifying tuple for a committed export view is:

```text
root_node_id
base_wal_seq
size_bytes
tree_format
```

`root_node_id` identifies the immutable tree root. `base_wal_seq`
identifies which prefix of the export's WAL is represented by that root.
`size_bytes` identifies the logical device size for that committed serving
view. `tree_format` identifies the stored tree geometry used to interpret
node spans and slots.

For `simple_mutable_tree`, the serving identity is the current `export_heads`
row plus export-private tree rows. Normal writes keep the same current head and
do not create a new generation.

The prototype may keep old roots and blobs physically present after head
movement. Future GC can add pinning and retention policy without making old
roots part of the normal serving source of truth.

# Root Publication

Simple mutable commits and WAL/COW compaction publish new tree records through
`TreeRecordStore::publish_tree_update` in a single catalog transaction. WAL
checkpoints are global, not per range:

```rust
struct PublishTreeUpdate {
    export_id: ExportId,
    expected_head: ExportHead,
    next_head: ExportHead,
    records: TreeRecordBatch,
}

enum PublishTreeUpdateOutcome {
    Published(ExportRecord),
    StaleHead(ExportRecord),
}
```

The adapter loads the current head inside the publication transaction. On
success, it inserts the tree metadata batch and advances `export_heads`.
For COW compaction, `next_head.root_node_id` must represent every WAL record
with sequence `<= next_head.base_wal_seq`.

If the current head no longer matches `expected_head`, publication is stale and
the caller discards unpublished output. WAL compaction may replan from the
current database head if the original target is still useful. Stale publication
attempts must not leave new tree rows reachable from a published head.

# Invariants

- `ExportCatalog` is the durable export lifecycle and head metadata source.
- `TreeRecordStore` is the durable tree row and tree-publication boundary.
- `LocalExportRegistry` is not used for durable metadata.
- `exports` owns export identity and lifecycle.
- `export_heads` owns the current serving root, size, layout, tree format, and
  checkpoint.
- Tree-backed heads carry a stored `tree_format`.
- Tree geometry and sparse update planning live in server engine code.
- Tree metadata reads are bounded by explicit node ids or parent/slot lookups.
- `simple_mutable_tree` rows are export-private direct-commit metadata.
- `cow_immutable_tree` rows are immutable publication metadata.
- Published COW nodes and leaf blobs are immutable.
- COW child pointers are immutable once published.
- COW root publication advances the current head.
- Tree record insertion and head publication are atomic with respect to the
  expected prior head.
- Checkpoints advance monotonically as a global WAL prefix.
- Clones copy a COW root pointer and do not copy leaf blobs.
- Clones include only the source export's latest committed checkpoint.
- Clones preserve the source tree format.
- New simple durable exports start with one current head and no materialized
  tree.
- New COW exports start with one current head.
- Cloned COW exports start with one current head copied from a non-null source
  root.
- Delete is logical first; physical deletion belongs to GC.
- Missing tree data means zero-fill, never uninitialized bytes.
- Materialized internal nodes with no reachable leaf descendants are
  corruption.

# Open Questions

- Whether a final short export leaf is stored as full 32 MiB or a shorter blob.
- How much tree metadata should be cached in memory per active export.
- Whether future GC should keep N old roots, time-based roots, pinned roots, or
  only roots reachable from current exports and active readers.
