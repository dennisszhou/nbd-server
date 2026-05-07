Title: Compaction Manager Architecture
Date: 2026-05-01
Status: superseded

Superseded by `docs/plans/2026-05-06-export-head-ownership-compaction.md`.
The live implementation uses an engine-owned compaction coordinator over a
direct `CowCompactor`; it no longer has a global `CompactionManager` queue or
background worker shutdown lifecycle. This document is retained only as
historical context for the earlier queue-based design.

# Problem

WAL records make writes durable and immediately visible through
`ExportReadView`, but replay cost and memory pressure grow if WAL entries are
never folded into committed tree state. The system needs a component that
turns a global WAL prefix into immutable leaf blobs and a new committed
copy-on-write root.

# Goal

Define `CompactionManager` as the component responsible for:

- accepting idempotent compaction jobs;
- choosing a global WAL prefix checkpoint;
- rereading current catalog state when a job runs;
- reading WAL records for that prefix;
- grouping records by 32 MiB leaf;
- creating immutable replacement leaf blobs;
- creating immutable copy-on-write tree metadata;
- publishing tree metadata and a new checkpoint through `ExportCatalog`;
- optionally notifying active `ExportReadView` instances after publication;
- supporting close-time compaction as an intended feature.

# Scope

Compaction is not part of the write durability path. A write is durable once
its WAL record is durable. Compaction improves recovery time and moves data
from WAL overlay state into committed tree state.

Compaction must preserve the global WAL prefix invariant:

```text
root R at checkpoint S represents every WAL record with seq <= S
```

# In-Process And External Compaction

Compaction may run in the serving process or as an outside process. The
publication contract is the same in both cases:

```text
write immutable compaction output
  -> publish catalog root/checkpoint
  -> active read views catch up by notification or refresh
```

Correctness must not depend on synchronous notification. An active server that
misses the notification can continue serving from its older root plus WAL
overlay as long as the WAL needed by that older view remains retained.

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
    base_root: CommittedRoot,
    published_root: Option<CommittedRoot>,
    target_wal_seq: WalSeq,
    compacted_records: u64,
    written_leaf_blobs: u64,
    outcome: CompactionOutcome,
}

enum CompactionOutcome {
    Published,
    AlreadyCovered,
    StalePlan,
    NoRecords,
}

struct CompactionPlan {
    export_id: ExportId,
    base_root: CommittedRoot,
    base_wal_seq: WalSeq,
    base_size_bytes: u64,
    target_wal_seq: WalSeq,
}
```

Conceptual API:

```rust
impl CompactionManager {
    async fn enqueue(&self, request: CompactExport) -> Result<()>;

    async fn compact_export(&self, request: CompactExport)
        -> Result<CompactionResult>;
}
```

`enqueue` is the normal serving-process lifecycle API. `compact_export` is the
worker operation and is also useful for tests and future operator-triggered
compaction.

# Queue Ownership

Compaction work runs on a separate compaction queue owned by
`CompactionManager`. Tokio may be the process executor, but it is not the
logical workqueue policy.

```rust
struct CompactionQueue {
    // bounded queue, worker handles, shutdown state, metrics
}

struct CompactionManager {
    queue: CompactionQueue,
    // catalog, wal provider, committed tree reader/writer, blob store
}

enum CompactionEnqueueOutcome {
    Queued,
    DroppedFull,
    ShuttingDown,
}
```

The compaction queue is distinct from:

- `ConcurrentExportRuntime` and export queue-depth slots;
- per-connection reply queues;
- admission wait queues;
- future foreground storage queues.

The first implementation should use a small fixed worker count, likely one
worker, and a bounded pending queue. This limits background catalog, WAL replay,
and blob construction pressure. Correctness must not rely on that
serialization: duplicate jobs, racing in-process workers, or a future external
compactor must still be safe through catalog compare/publish semantics.

The queue API hides the executor choice. V1 may run the worker as a Tokio task
on the server's existing runtime. Later, the same `CompactionQueue` boundary can
move to a dedicated current-thread Tokio runtime on one OS thread, or to a
dedicated multi-thread runtime, without changing close-path or operator-facing
callers.

If enqueue fails because the queue is full or shutting down, close still
finishes after acknowledged writes are WAL-durable. The skipped compaction is
logged, WAL records remain retained, and a later close, manual trigger, or
future scheduler can retry.

Manager shutdown is explicit. Shutdown stops new enqueues, signals the worker,
lets the current compaction job finish, drops any jobs still pending in the
bounded queue, and joins the worker before shutdown completion is reported.
This keeps the active write/checkpoint operation coherent without pretending
queued cleanup is required for durability.

Cleanup safety is a narrow policy boundary. The planned policy is time-based
retention plus periodic read-view refresh:

```rust
struct WalRetentionPolicy {
    min_wall_clock_age: Duration,
    refresh_interval: Duration,
}
```

Active exports must refresh catalog head/read-view state more frequently than
`min_wall_clock_age`. Background cleanup may prune checkpointed WAL segments
only after they are older than the retention window. The refresh worker and
full retention cleanup loop are future work; close-triggered compaction can
publish checkpoints without physically pruning WAL.

# Workflow

```text
receive CompactExport(export_id, requested target)
  -> load latest export metadata from ExportCatalog
  -> capture base_root, base_wal_seq, size, and layout from the DB head
  -> open the export WAL through WalProvider
  -> clamp target WAL sequence S to the durable WAL high watermark
  -> if S <= base_wal_seq: return AlreadyCovered or NoRecords
  -> verify S is durable and contiguous after base_wal_seq
  -> build CompactionPlan(base head, S)
  -> read WAL records (base_wal_seq + 1)..S
  -> group records by 32 MiB leaf range
  -> for each affected leaf:
       read committed leaf through base_root only, or zero buffer
       apply WAL records in sequence order
       write new immutable leaf blob through BlobStore
       create new leaf node metadata
  -> create new internal nodes along affected paths
  -> ExportCatalog.publish_compaction(plan base, new tree batch, S)
  -> optionally notify active Export/ExportReadView of the checkpoint
```

If no WAL records are available after the current checkpoint, compaction should
return `NoRecords` or `AlreadyCovered` and should not advance the export head.

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
- after publication, the active `ExportReadView` catches up by notification or
  periodic refresh.

Inactive export:

- compaction uses `ExportCatalog`, `WalProvider`, `CommittedTreeReader`, and
  `BlobStore`;
- no read-view notification is needed;
- the next open loads the new root/checkpoint from `ExportCatalog`.

Clean-close enqueue:

- the connection/export stops accepting new writes;
- admitted writes finish or fail according to shutdown policy;
- the close path captures the durable WAL high watermark as a target hint;
- it enqueues `CompactExport { target: Through(H) }`;
- close finishes without waiting for the compaction job;
- timeout or failure in the background job falls back to WAL replay on next
  open.

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

`CompactionManager` writes immutable blob files before catalog publication.
Tree metadata insertion and export head advancement should happen in one
catalog transaction so the database never exposes a partial tree.

Publication uses a compare/publish request:

```rust
ExportCatalog::publish_compaction(PublishCompaction)
```

`ExportCatalog` loads the current export head internally. The request carries
the base root, checkpoint, size, and layout that the compaction plan used.
Publication outcomes are:

```text
current.base_wal_seq >= compacted_through
  -> AlreadyCovered, no head change

current root/checkpoint/size/layout != expected base
  -> StalePlan, no head change

current root/checkpoint/size/layout == expected base
  -> insert immutable tree metadata
  -> advance export_heads to new_root/checkpoint
```

`AlreadyCovered` is a successful no-op. `StalePlan` is retryable by discarding
unpublished output and replanning from the current database head through the
original target. This makes duplicate and racing compaction attempts safe.

If publication fails after blobs or metadata were written, those objects are
unpublished garbage. Future GC is responsible for deleting them.

# ReadView Notification

After catalog publication succeeds, `CompactionManager` may notify the active
export if it is mounted locally:

```text
new_root/checkpoint
  -> ExportReadView.install_checkpoint(...)
```

The read view decides when now-committed WAL overlay entries can be downgraded
or retired. Compaction does not directly mutate read-view internals.

If no active export exists, no notification is needed. The next open loads the
new root/checkpoint from `ExportCatalog`. The first close-triggered
implementation can skip active read-view notification if it only enqueues work
after the close path has drained the request path and released the export.

If notification fails after catalog publication, the durable checkpoint remains
published. The active read view may continue serving correctly from its old root
plus WAL overlay, and it can catch up by reloading the catalog checkpoint or by
receiving a later notification. Notification failure delays memory cleanup; it
does not roll back catalog publication.

If compaction runs outside the serving process, notification may be replaced by
polling or periodic head refresh. The active `ExportReadView` is still the only
owner that demotes authoritative WAL overlay entries for that process.

# WAL Cleanup Handoff

Compaction publication makes WAL records at or below the checkpoint represented
by the committed tree. It does not immediately delete those WAL records.

Cleanup may prune a WAL segment only after both are true:

```text
segment.max_seq <= published_base_wal_seq
segment.closed_at <= now - wal_retention_window
```

The retention window gives active serving processes time to refresh their read
views to the newer checkpoint. A process that cannot refresh before its view
becomes older than the retention window must stop serving that export and force
a reopen.

This keeps external cleanup asynchronous and avoids requiring the compactor to
coordinate directly with every connection. A future lease protocol can tighten
the rule for multi-host serving, but time-based retention is enough for the
first lifecycle model.

# Close-Time Compaction

Close-time compaction is the first implemented compaction trigger. On clean
close, the server requests compaction for that export, usually targeting the
current durable WAL high watermark.

Close does not wait for compaction to finish. Close remains correct if
close-time compaction fails because acknowledged writes are still durable in
WAL and will replay on next open.

The exact background worker retry policy remains a design detail. Failure must
leave the export recoverable from the previous catalog checkpoint plus WAL.

# Invariants

- Compaction is not required for write durability.
- Close does not wait for background compaction to finish.
- Published checkpoints are global WAL prefixes.
- A published root at checkpoint `S` represents every WAL record with
  `seq <= S`.
- The target WAL sequence is durable and contiguous.
- Compaction input is exactly `(base_wal_seq + 1)..target_wal_seq`.
- Committed base reads during compaction use `base_root`, not
  `ExportReadView`.
- Compaction applies WAL records in sequence order.
- Leaf blobs and tree nodes created by compaction are immutable.
- Unchanged subtrees are shared.
- Catalog root publication happens after blobs and metadata are written.
- Catalog publication compares against the current database head before
  advancing `export_heads`.
- Compaction does not directly retire `ExportReadView` overlay entries.
- Read-view notification failure delays cleanup but does not invalidate the
  published catalog checkpoint.
- External compaction is valid because active read views can refresh from the
  catalog checkpoint and retain WAL overlay until then.
- WAL physical cleanup is asynchronous and gated by checkpoint publication plus
  the retention policy.
- Physical cleanup can be skipped until retention cleanup is implemented or
  retention safety is otherwise proven.
- No-op compaction does not advance the export head.
- Failed unpublished compaction output is garbage-collectable later.
- Duplicate and racing compactions are safe because stale publication attempts
  no-op or retry from the current database head.

# Open Questions

- Exact background compaction thresholds.
- Exact background worker retry/backoff policy.
- Whether compaction should stream leaf construction or materialize grouped WAL
  records first.
- How much parallelism to allow while building leaf blobs.
- Exact read-view catch-up mechanism after notification failure.
- Default WAL retention window and refresh interval.
- Whether external cleanup needs serving leases before multi-host serving.
