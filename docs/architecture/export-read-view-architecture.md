Title: Export Read View Architecture
Date: 2026-05-12
Status: approved

# Problem

The server needs a correctness-facing in-memory serving view for each active
export. That view must always include durable WAL entries that are newer than
the committed catalog checkpoint, while still allowing optional read-through
caching from committed object storage.

This is more than a normal cache. Some in-memory state is required for
correctness because it represents durable writes that have not yet been
compacted into the committed tree. Other in-memory state is optional and may be
evicted under memory pressure.

# Goal

Define `ExportReadView` so that:

- acknowledged writes are visible before compaction;
- WAL entries after the committed checkpoint are retained as required serving
  overlay state;
- reads can fill misses from committed backing state;
- optional read-through/blob cache entries can be evicted safely;
- compaction can advance the committed root and checkpoint through an explicit
  cutover event;
- in-flight reads using an old root remain correct because they capture the
  overlay/cache slices needed for that read before the root changes;
- logical range caches remain owned by `ExportReadView` and cannot become an
  independent source of truth.

# Serving Model

There is one authoritative `ExportReadView` owner per active export in a
serving process. Individual reads may pin lightweight snapshots from it, but
they should not create independent long-lived tree readers with their own
metadata truth.

The view is the cache and the arbiter of what is authoritative for reads:

```rust
struct ExportReadView {
    state: RwLock<ReadViewState>,
    tree_reader: Arc<dyn TreeReader<RootSnapshot>>,
}

struct ReadViewState {
    root: RootSnapshot,
    last_applied_seq: WalSeq,
    wal_debt_bytes: u64,
    overlay: OverlayExtentMap,
    cache: ReadCache,
}

struct RootSnapshot {
    root_node_id: Option<NodeId>,
    base_wal_seq: WalSeq,
    size_bytes: u64,
    tree_format: Option<TreeFormat>,
}

struct WalEntry {
    seq: WalSeq,
    range: ByteRange,
    data_ref: WalDataRef,
}

struct CacheEntry {
    range: ByteRange,
    data_ref: CacheDataRef,
}
```

`wal_overlay` is required correctness state. `cache` is optional
acceleration. The two may contain data for similar logical ranges, but only
the WAL overlay is authoritative for durable writes not represented by the
published tree checkpoint.

Read order:

```text
1. required WAL overlay entries in ExportReadView
2. optional ReadCache entries for gaps not covered by overlay
3. committed tree/blob state through the current or captured tree reader
4. zero-fill committed tree holes
```

The WAL overlay is required correctness state. It is not evictable until a
committed checkpoint proves those WAL records are represented by the committed
tree.

Optional cache state is separate:

```text
required:
  WAL overlay entries where seq > base_wal_seq

optional:
  logical ReadCache entries owned by ExportReadView
  tree lookup metadata, if added, scoped to root/checkpoint
  immutable blob cache entries keyed by BlobKey, if added
```

The current `ReadCache` is a logical extent cache, not an independent
versioned source of truth. It is safe only because it is owned inside
`ExportReadView` state:

- reads capture cache slices while holding the same state lock used for the
  current root and overlay lookup;
- WAL writes trim overlapping cache ranges before they become visible through
  the overlay;
- tree fills are inserted only if the read-view root did not change while the
  tree miss was being filled;
- checkpoint advancement inserts now-committed WAL slices into the cache before
  removing those ranges from the authoritative overlay.

A cache entry backed by WAL storage must not outlive the WAL segment it
references; before external WAL pruning, such entries must be dropped or
converted to cache data that no longer depends on the WAL file.

# Global WAL Prefix Checkpoint

`base_wal_seq` is a global prefix checkpoint for one export.

If the current export head says:

```text
root_node_id = R
base_wal_seq = S
```

then root `R` represents every WAL record with sequence `<= S`. Startup must
replay every durable WAL record with sequence `> S`.

This deliberately avoids per-range checkpoints. Compaction may internally group
work by 32 MiB leaf, but it may publish checkpoint `S` only after the new root
represents every WAL record from the old checkpoint through `S`.

# API Shape

`ExportReadView` owns the current committed root snapshot used for new reads,
plus the required WAL overlay and optional memory caches.

Conceptual API:

```rust
trait TreeReader<R> {
    async fn read_committed(
        &self,
        root: &R,
        range: ByteRange,
    ) -> Result<Block>;
}

impl ExportReadView {
    async fn read(&self, range: ByteRange) -> Result<Vec<u8>>;

    async fn apply_wal_record(&self, record: WalRecord) -> Result<()>;

    async fn advance_root(&self, new_root: RootSnapshot) -> Result<()>;
}
```

`TreeReader<RootSnapshot>` is intentionally above `BlobStore`. `ExportReadView`
should not know how to walk sparse tree nodes, but it may own the current
`RootSnapshot` and pass that snapshot to the tree reader.

# Read Snapshots And Checkpoint Advancement

A read captures the root snapshot, overlapping WAL overlay slices, optional
cache hits, and remaining tree misses while holding the read-view state lock.
Captured overlay slices own references to their WAL records, so the read can
remain correct even if a checkpoint is installed before the tree miss reads
finish.

Read flow:

```text
capture root R/S plus overlay/cache slices for the requested range
read remaining holes from committed tree using R
copy tree fills and cache hits
overlay captured WAL slices last
assemble result
```

If compaction publishes root `R2` while a read is still using old root `R1`,
the read remains correct because it already captured the WAL overlay slices
needed with `R1`.

That is the key cutover invariant:

> The read view must never drop WAL overlay entries for a sequence unless every
> root snapshot still usable by in-flight reads represents that sequence, or
> the read captured an overlay snapshot that still includes it.

The current implementation satisfies this without a separate root-guard type:
it captures the overlay slices before releasing the read lock. Reads remain
correct because each read combines:

```text
captured root/checkpoint
  + WAL overlay entries newer than the captured checkpoint
```

Checkpoint installation may make a newer root available for new reads, but it
must not invalidate the overlay slices already captured by in-flight reads.

# Tree Reader

`CowTreeReader` implements `TreeReader<RootSnapshot>` by:

- resolving the supplied committed root snapshot;
- walking sparse internal nodes;
- locating 32 MiB leaf blobs;
- reading blob ranges through the configured `BlobStore`;
- zero-filling holes.

The tree reader may cache immutable node/blob lookups internally. Any future
logical range cache outside `ExportReadView` state must be tagged by
root/checkpoint; the current logical `ReadCache` is safe only because it is
owned by the read view and maintained under that state lock.

# Checkpoint Events

Compaction creates a new committed root for a global WAL prefix.

Current checkpoint advancement entry points:

```rust
impl ExportReadView {
    async fn advance_root(&self, new_root: RootSnapshot) -> Result<()>;

    async fn advance_after_compaction(
        &self,
        new_root: RootSnapshot,
        snapshot: &ReadViewCompactionSnapshot,
    ) -> Result<()>;
}
```

On checkpoint installation, `ExportReadView` should:

- atomically make `new_root` the root used for new read snapshots;
- record that the committed checkpoint advanced to `compacted_through`;
- keep WAL overlay entries with `seq > compacted_through`;
- demote or retire WAL overlay index entries with `seq <= compacted_through`;
- keep optional cache entries that remain correct for the new read view.

Demotion means an entry stops being authoritative WAL overlay state because
the published tree now includes it. The implementation may:

- drop the entry from the live overlay index after captured read slices own
  their record references;
- keep a copy as optional cache if it no longer references prunable WAL
  storage;
- keep a WAL-backed cache entry only while the referenced WAL segment is still
  retained.

Demotion is not required for correctness. It is a memory and recovery-time
optimization after the durable catalog checkpoint has advanced.

# Refresh And Staleness

An active read view can remain correct even if another process publishes a
newer committed root, as long as the WAL it still needs has not been pruned.

Example:

```text
active view:
  base tree checkpoint = 100
  WAL overlay = 101..250

catalog after compaction:
  base tree checkpoint = 200

active view remains correct:
  old base tree checkpoint 100
  + WAL overlay 101..250
```

The view may catch up by notification, polling, or refresh on open/close. The
catch-up path is:

```text
load newer export head/root checkpoint C
  -> advance the read-view root/checkpoint for new reads
  -> keep overlay entries with seq > C authoritative
  -> demote/drop overlay entries with seq <= C after captured read slices own
     any record references they still need
```

If WAL cleanup uses a time-based retention window, a read view older than that
window is invalid. A serving process must refresh within the retention window
or fail closed for that export before cleanup can make its old WAL dependencies
disappear.

# Compaction Interaction

Compaction publishes committed tree state before read-view overlay entries are
retired:

```text
choose global checkpoint S
  -> read WAL records (old_checkpoint + 1)..S
  -> upload new leaf blobs
  -> create new tree nodes
  -> TreeRecordStore.publish_tree_update(expected_head, next_head, records)
  -> optionally notify active Export
  -> ExportReadView.advance_after_compaction(new_root, snapshot), if active
  -> retire now-committed WAL overlay entries when safe
```

If notification fails after catalog publication, the durable checkpoint remains
published. The read view may continue serving correctly from its old root plus
WAL overlay. It can catch up by reloading the catalog checkpoint or by
receiving a later notification. WAL cleanup must preserve enough retained WAL
for stale-but-valid read views according to the retention contract.

# WAL Retention Interaction

The current local engine prunes checkpointed WAL only after the active
`ExportReadView` has installed the newly published checkpoint. That is a
single-process cleanup policy, not a cross-process retention protocol.

A future external cleanup contract can be time based rather than lease based.

```rust
struct WalRetentionPolicy {
    min_wall_clock_age: Duration,
    refresh_interval: Duration,
}
```

The serving contract is:

```text
refresh_interval < min_wall_clock_age
```

Every live serving process must refresh export head/read-view state within the
retention window. If it cannot refresh in time, it must stop serving the export
and force a reopen from the current catalog head plus retained WAL.

This allows asynchronous external cleanup:

```text
published checkpoint C exists
WAL segment max_seq <= C
WAL segment closed_at <= now - min_wall_clock_age
  -> segment is eligible for pruning
```

Future multi-host serving can replace or strengthen this with leases:

```text
wal_retention_leases
  owner
  export_id
  min_required_wal_seq
  expires_at
```

The read-view model does not require leases on day one, but it must keep the
single-owner read-view boundary so a stronger lease protocol can be added
without changing request-path read semantics.

# Startup Recovery

Startup recovery uses the catalog checkpoint:

```text
load export metadata from ExportCatalog
  -> load current export head
  -> root = root_node_id
  -> checkpoint = base_wal_seq
  -> initialize ExportReadView at root/checkpoint
  -> replay WAL records where seq > checkpoint
  -> apply replayed records to ExportReadView
```

After replay, `ExportReadView` again contains every durable WAL record not
represented by the committed root.

# Invariants

- `ExportReadView` is the authoritative in-process serving view for an active
  export.
- `ExportReadView` is reconstructable from `ExportCatalog` plus durable WAL.
- WAL overlay entries with `seq > base_wal_seq` are required state.
- Required WAL overlay entries are not evicted for memory pressure.
- WAL entries with `seq <= base_wal_seq` may be demoted from
  authoritative overlay state after captured read slices own any record
  references they still need.
- Optional blob/read-through cache entries may be evicted for memory pressure.
- Optional cache entries that reference WAL storage must be dropped or copied
  before the referenced WAL segment is pruned.
- `base_wal_seq` is a global WAL prefix, not a per-range frontier.
- Root `R` at checkpoint `S` represents every WAL record with `seq <= S`.
- Startup replays every durable WAL record with `seq > base_wal_seq`.
- Checkpoint installation never drops WAL overlay entries newer than the
  checkpoint.
- Checkpoint installation does not invalidate overlay slices already captured
  by in-flight reads.
- Logical `ReadCache` entries are valid only while owned by `ExportReadView`
  state and maintained by write trimming, root-checked tree fill insertion, and
  checkpoint demotion.
- Under a future external retention policy, a serving read view older than the
  WAL retention window is invalid and must refresh or stop serving.

# Open Questions

- Whether cache memory pressure can ever spill WAL overlay state to a local
  durable cache while preserving the same read-view invariant.
- Exact default WAL retention window and refresh interval.
- Whether external cleanup starts with time-based retention only or also writes
  optional serving leases.
