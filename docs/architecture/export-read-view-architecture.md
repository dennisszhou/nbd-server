Title: Export Read View Architecture
Date: 2026-05-01
Status: draft

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
- in-flight reads using an old root remain correct because the WAL overlay
  still contains every write not represented by that root;
- untagged logical range caches are forbidden.

# Serving Model

There is one authoritative `ExportReadView` owner per active export in a
serving process. Individual reads may pin lightweight snapshots from it, but
they should not create independent long-lived tree readers with their own
metadata truth.

The view is the cache and the arbiter of what is authoritative for reads:

```rust
struct ExportReadView {
    state: RwLock<ReadViewState>,
    active_reads: ReadEpochTracker,
}

struct ReadViewState {
    base_tree: PublishedTree,
    wal_overlay: RangeIndex<WalEntry>,
    cache: RangeCache<CacheEntry>,
}

struct PublishedTree {
    root_node_id: Option<NodeId>,
    base_wal_seq: WalSeq,
}

struct WalEntry {
    seq: WalSeq,
    range: ByteRange,
    data_ref: WalDataRef,
}

struct CacheEntry {
    range: ByteRange,
    visible_at_or_before: WalSeq,
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
2. committed tree/blob state through the current or captured BackingReader
3. zero-fill
```

The WAL overlay is required correctness state. It is not evictable until a
committed checkpoint proves those WAL records are represented by the committed
tree.

Optional cache state is separate:

```text
required:
  WAL overlay entries where seq > base_wal_seq

optional:
  immutable blob cache entries keyed by BlobKey
  tagged logical read-through cache entries, if ever added
  tree lookup metadata tagged by root/checkpoint
```

Cache entries are valid only when their version tag proves they are no newer
than the current visible WAL boundary for the read. A cache entry backed by
WAL storage must not outlive the WAL segment it references; before WAL pruning,
such entries must be dropped or converted to cache data that no longer depends
on the WAL file.

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
trait BackingReader {
    async fn read_committed(
        &self,
        root: RootSnapshot,
        range: ByteRange,
    ) -> Result<Bytes>;
}

impl ExportReadView {
    async fn read(&self, range: ByteRange) -> Result<Bytes>;

    fn apply_wal_record(&self, record: WalRecord) -> Result<()>;

    fn install_checkpoint(&self, checkpoint: ReadViewCheckpoint)
        -> Result<()>;
}
```

`BackingReader` is intentionally above `StorageEngine`. `ExportReadView`
should not know how to walk sparse tree nodes, but it may own the current
`RootSnapshot` and pass that snapshot to the backing reader.

# Read Epochs And Root Guards

A read should capture a root guard before filling misses. The guard identifies
the committed tree root and WAL checkpoint used by that read.

```rust
struct RootGuard {
    root: RootSnapshot,
    checkpoint: WalSeq,
    epoch: ReadEpoch,
}
```

The guard is a lightweight active-reader reference, not a correctness lock.
Its job is to prevent WAL overlay downgrade while a read may still use an
older root.

Read flow:

```text
capture root guard R/S
read WAL overlay entries that cover the request
read remaining holes from committed tree using R
assemble result
drop root guard
```

If compaction publishes root `R2` while a read is still using old root `R1`,
the read remains correct as long as `ExportReadView` has not dropped WAL
overlay entries that are not represented by `R1`.

That is the key cutover invariant:

> The read view must never drop WAL overlay entries for a sequence unless every
> root snapshot still usable by in-flight reads represents that sequence, or
> the read captured an overlay snapshot that still includes it.

The first implementation can satisfy this conservatively by holding a read-view
lock across root-guard capture and overlay lookup, then delaying overlay
downgrade until no root guards with older checkpoints remain.

Root guards protect downgrade eligibility only. Reads remain correct because
each read combines:

```text
captured root/checkpoint
  + WAL overlay entries newer than the captured checkpoint
```

The read epoch tracker records the oldest checkpoint still usable by in-flight
reads. Checkpoint installation may make a newer root available for new reads,
but it must not retire authoritative WAL entries needed by any active epoch.

# BackingReader

`CommittedTreeReader` implements `BackingReader` by:

- resolving the supplied committed root snapshot;
- walking sparse internal nodes;
- locating 32 MiB leaf blobs;
- reading blob ranges through `StorageWorkQueue`;
- zero-filling holes.

The tree reader may cache immutable node/blob lookups internally, but logical
range caching must be tagged by root/checkpoint.

# Checkpoint Events

Compaction creates a new committed root for a global WAL prefix.

Checkpoint event:

```rust
struct ReadViewCheckpoint {
    old_root: RootId,
    new_root: RootId,
    compacted_through: WalSeq,
}
```

On checkpoint installation, `ExportReadView` should:

- atomically make `new_root` the root used for new read snapshots;
- record that the committed checkpoint advanced to `compacted_through`;
- keep WAL overlay entries with `seq > compacted_through`;
- demote or retire WAL overlay entries with `seq <= compacted_through` only
  when no root guard with an older checkpoint can still need them;
- keep immutable blob cache entries by `BlobKey`;
- invalidate logical read-through cache entries tagged with older roots.

Demotion means an entry stops being authoritative WAL overlay state because
the published tree now includes it. The implementation may:

- drop the entry immediately when no active read can need it;
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
  -> install root/checkpoint C for new reads
  -> keep overlay entries with seq > C authoritative
  -> demote/drop overlay entries with seq <= C after older read epochs drain
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
  -> ExportCatalog.publish_compaction(expected_base, new_root, S)
  -> optionally notify active Export
  -> ExportReadView.install_checkpoint(new_root, S), if active
  -> retire now-committed WAL overlay entries when safe
```

If notification fails after catalog publication, the durable checkpoint remains
published. The read view may continue serving correctly from its old root plus
WAL overlay. It can catch up by reloading the catalog checkpoint or by
receiving a later notification. WAL cleanup must preserve enough retained WAL
for stale-but-valid read views according to the retention contract.

# WAL Retention Interaction

The first pruning contract can be time based rather than lease based.

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
  authoritative overlay state after active older read epochs drain.
- Optional blob/read-through cache entries may be evicted for memory pressure.
- Optional cache entries that reference WAL storage must be dropped or copied
  before the referenced WAL segment is pruned.
- `base_wal_seq` is a global WAL prefix, not a per-range frontier.
- Root `R` at checkpoint `S` represents every WAL record with `seq <= S`.
- Startup replays every durable WAL record with `seq > base_wal_seq`.
- Checkpoint installation never drops WAL overlay entries newer than the
  checkpoint.
- Checkpoint installation does not retire older overlay entries until root
  guards with older checkpoints no longer need them.
- Immutable blob cache entries do not become stale when roots move.
- Untagged logical read caches are forbidden.
- Cache invalidation is driven by checkpoint/root advancement, not object
  deletion.
- A serving read view older than the WAL retention window is invalid and must
  refresh or stop serving.

# Open Questions

- Whether first implementation should delay overlay retirement with a root
  guard count or by serializing reads during checkpoint install.
- Whether the first read view should include immutable blob caching or only the
  required WAL overlay.
- Whether cache memory pressure can ever spill WAL overlay state to a local
  durable cache while preserving the same read-view invariant.
- Exact default WAL retention window and refresh interval.
- Whether external cleanup starts with time-based retention only or also writes
  optional serving leases.
