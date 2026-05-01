Title: Compaction Manager Architecture
Date: 2026-05-01
Status: draft

# Problem

WAL records make writes durable and immediately visible through
`ExportReadView`, but replay cost and memory pressure grow if WAL entries are
never folded into committed tree state. The system needs a component that
turns a global WAL prefix into immutable leaf blobs and a new committed
copy-on-write root.

# Goal

Define `CompactionManager` as the component responsible for:

- choosing a global WAL prefix checkpoint;
- reading WAL records for that prefix;
- grouping records by 32 MiB leaf;
- creating immutable replacement leaf blobs;
- creating immutable copy-on-write tree metadata;
- inserting tree metadata into `ExportCatalog`;
- publishing a new checkpoint through `ExportCatalog`;
- notifying active `ExportReadView` instances after publication;
- supporting close-time compaction as an intended feature.

# Scope

Compaction is not part of the write durability path. A write is durable once
its WAL record is durable. Compaction improves recovery time and moves data
from WAL overlay state into committed tree state.

Compaction must preserve the global WAL prefix invariant:

```text
root R at checkpoint S represents every WAL record with seq <= S
```

# API Shape

Use request and result structs rather than long parameter lists.

```rust
struct CompactExport {
    export_id: ExportId,
    reason: CompactionReason,
    target: CompactionTarget,
}

enum CompactionReason {
    Background,
    MemoryPressure,
    CloseTime,
    Manual,
}

enum CompactionTarget {
    Through(WalSeq),
    BestEffort,
}

struct CompactionResult {
    export_id: ExportId,
    old_root: CommittedRoot,
    new_root: CommittedRoot,
    compacted_records: u64,
    written_leaf_blobs: u64,
    changed: bool,
}

struct CompactionPlan {
    export_id: ExportId,
    base_root: CommittedRoot,
    target_checkpoint: WalSeq,
}
```

Conceptual API:

```rust
impl CompactionManager {
    async fn compact_export(&self, request: CompactExport)
        -> Result<CompactionResult>;
}
```

# Workflow

```text
load latest export metadata from ExportCatalog
  -> capture base_root and base_checkpoint
  -> choose target checkpoint S
  -> verify S is durable and contiguous after base_checkpoint
  -> build CompactionPlan(base_root, S)
  -> read WAL records (base_checkpoint + 1)..S
  -> group records by 32 MiB leaf range
  -> for each affected leaf:
       read committed leaf through base_root only, or zero buffer
       apply WAL records in sequence order
       write new immutable leaf blob through StorageEngine
       create new leaf node metadata
  -> create new internal nodes along affected paths
  -> insert tree metadata batch into ExportCatalog
  -> ExportCatalog.publish_checkpoint(new_root, S)
  -> notify active Export/ExportReadView of the checkpoint
```

If no WAL records are available after the current checkpoint, compaction should
return a no-op result with `changed = false` and should not append a new export
generation.

# Checkpoint Selection

Compaction publishes only global WAL prefix checkpoints.

For the first implementation, target selection can be simple:

- close-time compaction targets the current durable WAL high watermark;
- manual compaction may target a supplied `WalSeq`;
- background compaction may compact when WAL size or replay count crosses a
  threshold.

The target checkpoint must be less than or equal to the WAL's durable
contiguous high watermark. Compaction must not include speculative writes,
in-flight writes, or a sequence range with gaps.

Compaction may internally process by leaf, but it may not publish checkpoint
`S` unless the new root includes all WAL records from the previous checkpoint
through `S`.

# Active And Inactive Exports

Compaction can run while an export is active or inactive.

Active export:

- writes may continue during background compaction;
- compaction captures `base_root` and target checkpoint `S`;
- writes that land after `S` remain in WAL overlay state;
- after publication, the active `ExportReadView` is notified.

Inactive export:

- compaction uses `ExportCatalog`, `WALManager`, `CommittedTreeReader`, and
  `StorageEngine`;
- no read-view notification is needed;
- the next open loads the new root/checkpoint from `ExportCatalog`.

Close-time compaction:

- the connection/export stops accepting new writes;
- admitted writes finish or fail according to shutdown policy;
- compaction targets the durable WAL high watermark;
- timeout or failure falls back to WAL replay on next open.

# Tree Construction

The committed tree is persistent and copy-on-write:

- leaf blobs are immutable;
- tree nodes are immutable;
- child pointers are immutable;
- unchanged subtrees are shared;
- changed leaves create new leaf nodes;
- changed paths create new internal nodes up to a new root.

`root_node_id = null` represents the all-zero committed tree. Compaction from
an empty root creates only the nodes needed for affected leaves.

Compaction must read committed base leaf data from `base_root` only. It must
not read through `ExportReadView`, because the read view may include WAL
records newer than the selected target checkpoint.

# Catalog Publication

`CompactionManager` inserts the new tree metadata before publishing the root.

Publication uses:

```rust
ExportCatalog::publish_checkpoint(PublishCheckpoint)
```

`ExportCatalog` loads the latest generation internally and appends the next
`export_generations` row on success. The architecture assumes one checkpoint
publisher per export until writer fencing or multi-publisher compaction is
designed.

If publication fails after blobs or metadata were written, those objects are
unpublished garbage. Future GC is responsible for deleting them.

# ReadView Notification

After catalog publication succeeds, `CompactionManager` notifies the active
export if it is mounted locally:

```text
new_root/checkpoint
  -> ExportReadView.install_checkpoint(...)
```

The read view decides when now-committed WAL overlay entries can be downgraded
or retired. Compaction does not directly mutate read-view internals.

If no active export exists, no notification is needed. The next open loads the
new root/checkpoint from `ExportCatalog`.

If notification fails after catalog publication, the durable checkpoint remains
published. The active read view may continue serving correctly from its old root
plus WAL overlay, and it can catch up by reloading the catalog checkpoint or by
receiving a later notification. Notification failure delays memory cleanup; it
does not roll back catalog publication.

# Close-Time Compaction

Close-time compaction is an intended feature. On clean close, the server should
request compaction for that export, usually targeting the current durable WAL
high watermark.

Close remains correct if close-time compaction fails because acknowledged
writes are still durable in WAL and will replay on next open.

The exact close timeout and fallback policy remain design details. Timeout must
leave the export recoverable from the previous catalog checkpoint plus WAL.

# Invariants

- Compaction is not required for write durability.
- Published checkpoints are global WAL prefixes.
- A published root at checkpoint `S` represents every WAL record with
  `seq <= S`.
- The target checkpoint is durable and contiguous.
- Compaction input is exactly `(base_checkpoint + 1)..target_checkpoint`.
- Committed base reads during compaction use `base_root`, not
  `ExportReadView`.
- Compaction applies WAL records in sequence order.
- Leaf blobs and tree nodes created by compaction are immutable.
- Unchanged subtrees are shared.
- Catalog root publication happens after blobs and metadata are written.
- Compaction does not directly retire `ExportReadView` overlay entries.
- Read-view notification failure delays cleanup but does not invalidate the
  published catalog checkpoint.
- No-op compaction does not publish a new generation.
- Failed unpublished compaction output is garbage-collectable later.
- Only one checkpoint publisher per export is assumed until fencing is
  designed.

# Open Questions

- Exact background compaction thresholds.
- Exact close-time compaction timeout.
- Whether compaction should stream leaf construction or materialize grouped WAL
  records first.
- How much parallelism to allow while building leaf blobs.
- Exact read-view catch-up mechanism after notification failure.
