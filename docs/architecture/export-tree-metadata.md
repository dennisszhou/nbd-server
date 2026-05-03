Title: Export Tree Metadata
Date: 2026-05-01
Status: draft

# Problem

The system needs durable metadata that can represent large sparse disks, cheap
clone/fork, committed checkpoints, and immutable S3-friendly data blobs without
copying whole exports on update.

# Goal

Use `ExportCatalog` to track export lifecycle, root pointers, checkpoints, and
a sparse tree of committed data. Updates create new immutable nodes and move an
export root by appending a new generation instead of copying or rewriting the
full tree.

# ExportCatalog Responsibilities

`ExportCatalog` owns durable export metadata:

- create exports;
- clone exports;
- inspect/list exports;
- logically delete exports;
- load export metadata on NBD open;
- store export size, block size, and lifecycle state;
- store append-only committed-root generations;
- publish root/checkpoint updates by appending a generation.

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

export_generations
  id
  export_id
  generation
  root_node_id
  checkpoint_wal_seq
  size_bytes
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

child pointers
  immutable edges from internal tree nodes to child nodes
```

The implementation may choose normalized edge rows, embedded child pointers, or
serialized node objects. The architectural invariant is more important than the
initial table shape:

Published nodes are immutable. Updating an export creates new nodes and moves
the export root by appending a new export generation.

The export checkpoint is a global WAL prefix. If the latest export generation
has `root_node_id = R` and `checkpoint_wal_seq = S`, root `R` represents every
WAL record with sequence `<= S`. Startup recovery must replay every durable
WAL record with sequence `> S`.

`generation` is the committed-root generation. It starts at zero when an export
is created or cloned. Every successful root/checkpoint publication appends a
new row with the next generation number. Generation is per export; a cloned
export starts its own generation history even when it initially shares the
source root node.

`root_node_id = null` means the committed tree is all zeroes.

# Sparse Tree Shape

Use a sparse tree over logical disk offsets.

Target fanout:

```text
leaf:       32 MiB immutable data blob
level 1:     1 GiB = 32 leaves
level 2:    32 GiB
level 3:     1 TiB
level 4:    32 TiB
```

Internal nodes:

- metadata only;
- sparse child pointers;
- immutable once published.

A materialized internal node should have at least one reachable leaf descendant.
Sparse missing child pointers are valid, but an internal node with no reachable
leaf data is malformed metadata.

Leaf nodes:

- point to full 32 MiB immutable data blobs;
- represent a dense logical leaf range;
- are immutable once published.

Missing committed data zero-fills. Clone/fork sharing is represented by shared
immutable tree nodes, not by parent-root fallback during reads.

# Copy-On-Write Roots And Clone

The committed tree is a persistent copy-on-write tree. Edges are immutable once
published. Updates create new leaf blobs and new nodes along changed paths to a
new root. Unchanged subtrees are shared by reference.

Clone is O(1) because it copies the source export's current root pointer into a
new export generation.

```text
clone src -> dst
  -> create dst export metadata
  -> create dst export_generation:
       generation = 0
       root_node_id = src.root_node_id
       checkpoint_wal_seq = 0
  -> copy no leaf blobs
```

`checkpoint_wal_seq` is per export WAL state. The cloned root already contains
the source export's committed data as of the source's latest catalog
checkpoint. The destination export has a new WAL, so it starts with checkpoint
zero and replays only its own future WAL records. Clone does not include the
source export's uncheckpointed WAL.

When the child later compacts writes, it creates a new root for the child only.
The source export keeps pointing at its prior root. Both exports may continue
sharing unchanged immutable nodes and leaf blobs.

# Root Identity

Roots must be identifiable even if the prototype keeps every old root forever.
The identifying tuple for a committed export view is:

```text
root_node_id
checkpoint_wal_seq
generation
size_bytes
```

`root_node_id` identifies the immutable tree root. `checkpoint_wal_seq`
identifies which prefix of the export's WAL is represented by that root.
`generation` identifies the committed-root version used for ordering and
debugging. `size_bytes` identifies the logical device size for that committed
serving view.

The prototype may keep all old roots and blobs. Future GC can add root history,
pinning, and retention policy.

# Root Publication

Compaction publishes a new root/checkpoint through `ExportCatalog` in a single
catalog transaction. The checkpoint is global, not per range:

```rust
struct PublishCheckpoint {
    export_id: ExportId,
    new_root_node_id: Option<NodeId>,
    compacted_through: WalSeq,
}
```

`ExportCatalog` loads the latest export generation inside the publication
transaction. On success, `new_root_node_id` must represent every WAL record
with sequence `<= compacted_through`, and the catalog appends the next export
generation row.
This relies on a single checkpoint publisher per export until writer fencing or
multi-publisher compaction is designed.

# Invariants

- `ExportCatalog` is the durable export metadata source.
- `LocalExportRegistry` is not used for durable metadata.
- `exports` owns export identity and lifecycle.
- `export_generations` owns committed roots and checkpoints.
- Export generations are append-only.
- Published nodes and leaf blobs are immutable.
- Child pointers are immutable once published.
- Export root publication appends a generation.
- Checkpoints advance monotonically as a global WAL prefix.
- Clones copy a root pointer and do not copy leaf blobs.
- Clones include only the source export's latest committed checkpoint.
- New and cloned exports start at generation zero.
- Delete is logical first; physical deletion belongs to GC.
- Missing tree data means zero-fill, never uninitialized bytes.
- Materialized internal nodes with no reachable leaf descendants are
  corruption.

# Open Questions

- Whether child pointers live in DB rows or object-serialized node metadata.
- Whether a final short export leaf is stored as full 32 MiB or a shorter blob.
- How much tree metadata should be cached in memory per active export.
- Whether future GC should keep N old roots, time-based roots, pinned roots, or
  only roots reachable from current exports and active readers.
