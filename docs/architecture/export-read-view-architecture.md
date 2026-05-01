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
  WAL overlay entries where seq > checkpoint_wal_seq

optional:
  immutable blob cache entries keyed by BlobKey
  tagged logical read-through cache entries, if ever added
  tree lookup metadata tagged by root/generation
```

# Global WAL Prefix Checkpoint

`checkpoint_wal_seq` is a global prefix checkpoint for one export.

If the latest catalog generation says:

```text
root_node_id = R
checkpoint_wal_seq = S
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

# Root Guards

A read should capture a root guard before filling misses. The guard identifies
the committed tree root and WAL checkpoint used by that read.

```rust
struct RootGuard {
    root: RootSnapshot,
    checkpoint: WalSeq,
}
```

The guard is a lightweight active-reader reference, not a correctness lock. Its
job is to prevent WAL overlay downgrade while a read may still use an older
root.

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

# BackingReader

`CommittedTreeReader` implements `BackingReader` by:

- resolving the supplied committed root snapshot;
- walking sparse internal nodes;
- locating 32 MiB leaf blobs;
- reading blob ranges through `StorageWorkQueue`;
- zero-filling holes.

The tree reader may cache immutable node/blob lookups internally, but logical
range caching must be tagged by root/generation.

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
- retire WAL overlay entries with `seq <= compacted_through` only when no
  root guard with an older checkpoint can still need them;
- keep immutable blob cache entries by `BlobKey`;
- invalidate logical read-through cache entries tagged with older roots.

# Compaction Interaction

Compaction publishes committed tree state before read-view overlay entries are
retired:

```text
choose global checkpoint S
  -> read WAL records (old_checkpoint + 1)..S
  -> upload new leaf blobs
  -> create new tree nodes
  -> ExportCatalog.publish_checkpoint(new_root, S)
  -> notify active Export
  -> ExportReadView.install_checkpoint(new_root, S)
  -> retire now-committed WAL overlay entries when safe
```

If notification fails after catalog publication, the durable checkpoint remains
published. The read view may continue serving correctly from its old root plus
WAL overlay. It can catch up by reloading the catalog checkpoint or by
receiving a later notification. GC must not delete checkpointed WAL until
active read views have installed the checkpoint or closed.

# Startup Recovery

Startup recovery uses the catalog checkpoint:

```text
load export metadata from ExportCatalog
  -> load latest export generation
  -> root = root_node_id
  -> checkpoint = checkpoint_wal_seq
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
- WAL overlay entries with `seq > checkpoint_wal_seq` are required state.
- Required WAL overlay entries are not evicted for memory pressure.
- Optional blob/read-through cache entries may be evicted for memory pressure.
- `checkpoint_wal_seq` is a global WAL prefix, not a per-range frontier.
- Root `R` at checkpoint `S` represents every WAL record with `seq <= S`.
- Startup replays every durable WAL record with `seq > checkpoint_wal_seq`.
- Checkpoint installation never drops WAL overlay entries newer than the
  checkpoint.
- Checkpoint installation does not retire older overlay entries until root
  guards with older checkpoints no longer need them.
- Immutable blob cache entries do not become stale when roots move.
- Untagged logical read caches are forbidden.
- Cache invalidation is driven by checkpoint/root advancement, not object
  deletion.

# Open Questions

- Whether first implementation should delay overlay retirement with a
  generation refcount or by serializing reads during checkpoint install.
- Whether the first read view should include immutable blob caching or only the
  required WAL overlay.
- How active exports acknowledge checkpoint installation back to compaction or
  GC.
- Whether cache memory pressure can ever spill WAL overlay state to a local
  durable cache while preserving the same read-view invariant.
