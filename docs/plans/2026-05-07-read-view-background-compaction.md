Title: Read-View Background Compaction
Date: 2026-05-07
Status: approved

# Problem

`wal_durable` currently compacts in two places:

- close-time compaction, which is best effort;
- write-pressure compaction, which runs when retained WAL debt reaches the
  internal hard threshold of 2 GiB.

The hard threshold protects the server from allowing unbounded WAL growth, but
it waits until the export is already under pressure. Close-time compaction helps
when an export is detached cleanly, but an actively written export may stay open
for a long time and retain a large WAL prefix between hard-threshold events.

The current compactor also rebuilds checkpoint output by replaying WAL records
from disk. That is correct, but it makes compaction cost proportional to the
number of retained WAL records. A hot block rewritten many times should still
count toward WAL retention pressure, but compaction should write the latest
visible chunk image rather than replaying every obsolete intermediate write.

# Goal

Add background compaction for active `wal_durable` exports while preserving the
existing durability and backpressure model.

The design should:

- keep `wal_debt_bytes` as retained WAL payload bytes after the committed base;
- add a soft background trigger around 512 MiB of WAL debt;
- keep the existing 2 GiB hard write-pressure threshold;
- compact from a captured read-view snapshot instead of replaying WAL records;
- keep background compaction opportunistic and non-blocking for writes below
  the hard threshold;
- keep hard-threshold compaction as write backpressure;
- keep close-time compaction as a final best-effort cleanup attempt.

# Constraints

- WAL append remains the durability boundary for acknowledged writes.
- The catalog head remains the durable source of truth for the committed base.
- `ExportReadView` remains the serving source of truth for active exports.
- A checkpoint may advance to `S` only when the published tree represents every
  WAL record from the previous base through `S`.
- Compaction output remains immutable 32 MiB leaf blobs plus COW tree metadata.
- Reads must remain correct while background compaction is running.
- Writes must not include data with sequence greater than the compaction target
  in a checkpoint for that target.
- Tests should land with the commits that introduce the behavior they prove,
  not as one trailing proof commit.

# Non-goals

- Public operator configuration for compaction thresholds or intervals.
- A global compaction worker shared by all exports.
- A full arbitrary historical read-version tree.
- Multi-generation write admission.
- S3 or local blob garbage collection.
- WAL deletion safety beyond the existing prune-through behavior.
- External compaction outside the serving process.
- Changing WAL encoding or catalog schema.

# End State

Each active `wal_durable` export owns:

- the existing `ExportReadView`;
- the existing `CompactionCoordinator`;
- one background compaction task;
- a shutdown handle for that task.

The lifecycle becomes:

```text
open
  -> load committed COW root
  -> replay retained WAL into ExportReadView
  -> start background compaction task

write
  -> append WAL
  -> apply record to ExportReadView
  -> if wal_debt_bytes >= hard threshold:
       compact while holding write_lock

background tick
  -> if wal_debt_bytes >= soft threshold and below hard threshold:
       try to compact from a captured read-view snapshot

close
  -> stop background task
  -> wait for it to finish or observe shutdown
  -> run close-time best-effort compaction
```

Background compaction checks every 30 seconds. The first soft threshold is an
internal 512 MiB constant. The hard threshold remains the existing internal
2 GiB constant.

Compaction no longer needs to replay every WAL record in the normal active
export path. It captures a metadata snapshot of the read view at target sequence
`S`, groups visible dirty overlay extents by 32 MiB chunk, materializes those
chunks from the committed base plus the captured overlay, writes immutable
chunk blobs, publishes a COW tree checkpoint at `S`, advances the live read
view, and prunes WAL through the published base.

# Proposed Approach

## Retained WAL Debt

Keep `wal_debt_bytes` as a physical retention metric:

```text
wal_debt_bytes = sum(payload bytes for applied WAL records after base_wal_seq)
```

If block 1 is written 100 times, all 100 payloads count toward WAL debt. That
is intentional because compaction is primarily about bounding retained WAL size
and replay cost.

The compactor should still use the read-view overlay so that the output work is
based on the latest visible logical state. The same hot block written 100 times
should normally produce one dirty chunk image, not replay 100 record payloads
into that chunk.

## Read-View Snapshot Capture

Add an `ExportReadView` method that captures the current compaction state:

```rust
pub(super) async fn capture_compaction_snapshot(
    &self,
) -> Result<Option<ReadViewCompactionSnapshot>>;
```

The returned snapshot represents the current live view at
`target_wal_seq == last_applied_seq`:

```rust
pub(super) struct ReadViewCompactionSnapshot {
    root: RootSnapshot,
    target_wal_seq: WalSeq,
    wal_debt_bytes: u64,
    overlay: OverlayExtentMap,
}
```

`wal_debt_bytes` is the retained WAL payload represented by this snapshot:
payload bytes for applied records with sequence greater than
`root.base_wal_seq()` and less than or equal to `target_wal_seq`. At capture
time, `target_wal_seq == last_applied_seq`, so this is the current read-view
debt. If newer writes arrive while compaction is running, the live read view may
have more debt than the snapshot.

The overlay clone is a metadata clone. `OverlayExtentMap` entries contain
`Arc<WalRecord>`, so cloning the snapshot does not copy WAL payload bytes.
It copies the B-tree metadata and increments record references. The snapshot
keeps the payloads it needs alive even if later writes mutate the live overlay.

If `target_wal_seq <= root.base_wal_seq()` or the overlay is empty, the method
returns `Ok(None)`.

The first implementation captures only the current view. It does not support
asking for an arbitrary old sequence after newer writes have already mutated
the live overlay.

## Snapshot-Based Compaction

Teach the compactor to compact from `ReadViewCompactionSnapshot`:

```rust
pub async fn compact_snapshot(
    &self,
    export_id: &ExportId,
    snapshot: ReadViewCompactionSnapshot,
) -> Result<CompactionResult>;
```

The compactor uses the snapshot's committed root and captured overlay:

1. Group overlay extents by `ChunkIndex`.
2. For each dirty chunk, load the committed chunk from `snapshot.root` or start
   from a zero-filled 32 MiB buffer.
3. Apply captured overlay slices for that chunk in sequence order.
4. Write one immutable blob for each dirty chunk.
5. Publish a compaction checkpoint with expected base derived from
   `snapshot.root` and target sequence `snapshot.target_wal_seq`.

When publication succeeds, live read-view advancement must subtract the
snapshot's WAL debt from the current live debt instead of blindly resetting to
zero. If no newer writes arrived and the new checkpoint equals
`last_applied_seq`, the subtraction leaves zero. If newer writes did arrive,
the remaining debt is the payload bytes for records after the new checkpoint.
This keeps `wal_debt_bytes` equal to retained WAL payload bytes after the
current base even when background compaction publishes an older captured target.

The existing WAL-replay compaction helper may remain temporarily for tests or
fallback while the snapshot path is introduced, but the active engine should use
the snapshot path for background, close, and hard-threshold compaction once the
snapshot path is available.

## Background Task

`WalDurableEngine` should own a background task only when it has a COW tree
compaction coordinator. The legacy zero-backed `open()` path without COW tree
support should not start one.

The task loops on a Tokio interval and a shutdown signal:

```text
loop:
  wait for 30s tick or shutdown
  if shutdown: exit
  coordinator.compact_background_tick().await
```

The background tick is opportunistic:

1. Read `wal_debt_bytes`.
2. Skip if debt is below the 512 MiB soft threshold.
3. Skip if debt is at or above the 2 GiB hard threshold.
4. Try to acquire `compaction_lock`.
5. If the lock is busy, skip this tick.
6. Capture a read-view snapshot.
7. If there is no snapshot, skip.
8. Compact the snapshot with phase `"background"`.

Background compaction does not hold `write_lock`. Writes below the hard
threshold continue while background compaction works against its captured
snapshot.

## Hard-Threshold Compaction

The existing hard path remains intentionally blocking:

```text
write_lock held
  append WAL
  apply record to read view
  if wal_debt_bytes >= 2 GiB:
    wait for compaction_lock
    capture latest read-view snapshot
    compact snapshot
```

After acquiring `compaction_lock`, the hard path should recheck
`wal_debt_bytes`. A background compaction may have just completed. If the debt
is now below the hard threshold, the writer can skip hard compaction and release
`write_lock`.

If the debt is still at or above the hard threshold, the writer compacts through
the latest captured `last_applied_seq` before releasing `write_lock`. Later
writes queue behind this lock. Reads continue against the read view except for
brief root advancement.

This gives the main policy invariant:

```text
below hard threshold: compaction is maintenance
at or above hard threshold: compaction is write backpressure
```

## Close-Time Compaction

Close should stop the background loop before running close compaction. This
avoids close racing a new background tick and makes shutdown behavior easier to
reason about.

Close sequence:

1. Signal background task shutdown.
2. Wait for the task to exit.
3. Run close-time best-effort compaction through a fresh read-view snapshot.
4. Log close compaction failure and return success.

If the task is already inside a background compaction when close starts, close
waits for that task. The compaction is cleanup, not write durability, so future
work may add a timeout if shutdown latency becomes a problem. The first version
should prefer a simple ordered lifecycle.

# Data Model / API Shape

New or changed structures:

```rust
pub(super) struct ReadViewCompactionSnapshot {
    root: RootSnapshot,
    target_wal_seq: WalSeq,
    wal_debt_bytes: u64,
    overlay: OverlayExtentMap,
}
```

```rust
pub(super) struct CompactionPolicy {
    background_interval: Duration,
    background_wal_debt_threshold_bytes: u64,
    hard_wal_debt_threshold_bytes: u64,
}
```

```rust
struct BackgroundCompactionTask {
    shutdown: watch::Sender<bool>,
    task: Mutex<Option<JoinHandle<()>>>,
}
```

`WalDurableEngine` owns `BackgroundCompactionTask`. The `Mutex<Option<_>>`
shape does not mean some other component owns the task. It exists because the
export engine close API is `close(&self)`, while awaiting a Tokio
`JoinHandle` consumes the handle. The option lets close take the handle exactly
once through interior mutability:

```text
first close caller:
  signal shutdown
  take Some(handle)
  await handle

later close caller:
  signal shutdown
  take None
  observe that the task is already joined or being joined
```

The normal registry/runtime lifecycle should still close an active engine once.
The join-once guard is a local safety boundary for shared-reference Rust APIs,
tests, and future shutdown paths.

The exact shutdown primitive can be `watch`, `oneshot`, or a cancellation
token. The important API contract is that `WalDurableEngine::close` can request
shutdown and wait for the background task before close compaction even though
the engine close API takes `&self`. The task handle therefore needs interior
mutability, or an equivalent helper that can join at most once.

The background task must not keep the `WalDurableEngine` alive through a
reference cycle. It can own an `Arc<CompactionCoordinator>` or an explicit
worker handle made from the same underlying `Arc` fields. The engine owns the
task handle and remains responsible for stopping it on close and drop.

`CompactionCoordinator` should expose separate methods for the three phases:

```rust
async fn compact_background_tick(&self, policy: &CompactionPolicy);
async fn compact_hard_threshold(&self, policy: &CompactionPolicy);
async fn compact_close_best_effort(&self);
```

All three converge on a shared snapshot compaction helper after they decide
whether they should run.

Read-view advancement should expose a compaction-specific API rather than reuse
a plain root setter:

```rust
async fn advance_after_compaction(
    &self,
    new_root: RootSnapshot,
    snapshot: &ReadViewCompactionSnapshot,
) -> Result<()>;
```

That API verifies the new root advances from `snapshot.root.base_wal_seq()` to
`snapshot.target_wal_seq`, retires visible overlay entries through the new
checkpoint, and subtracts `snapshot.wal_debt_bytes` from the live debt. If the
live root has already advanced past the snapshot base, the call should reject
the stale advancement or treat it as already covered through the catalog
publication outcome.

Source-of-truth boundaries:

- SQLite catalog: durable committed export head and COW tree metadata.
- WAL files: durable acknowledged writes not yet represented by the catalog
  checkpoint.
- `ExportReadView`: live serving state for active export reads.
- `wal_debt_bytes`: derived runtime metric rebuilt from retained WAL replay and
  new writes.
- background task state: runtime lifecycle only, never durable source of truth.

# Invariants

- A write is acknowledged only after its WAL append is durable and its record
  is applied to the read view.
- `wal_debt_bytes` counts retained WAL payload bytes after the current base.
- Partial background publication subtracts only the WAL debt represented by the
  published snapshot; it must not reset debt for later writes.
- Background compaction must not include writes with sequence greater than the
  captured snapshot target.
- A checkpoint at sequence `S` may publish only when its root represents every
  WAL record from the previous base through `S`.
- `ExportReadView::advance_root` is the only path that moves the live read view
  to a newer committed base.
- Background, close, and hard-threshold compaction attempts are serialized by
  `compaction_lock`.
- Background compaction uses `try_lock` semantics and skips if another
  compaction is active.
- Hard-threshold compaction waits for `compaction_lock` while holding
  `write_lock`.
- Close stops the background task before close-time compaction.
- The background task is owned by `WalDurableEngine` and its join handle is
  consumed at most once.
- Compaction failure never loses acknowledged writes.
- Publication failure may leave orphaned blobs, but the catalog remains the
  source of truth and later compaction can retry.
- WAL prune failure does not invalidate the published checkpoint.

# Operational / Lifecycle Contracts

Startup:

- Replay WAL after the catalog base before serving.
- Rebuild `wal_debt_bytes` from replayed WAL payload bytes.
- Start the background task only after replay and read-view construction
  succeed.

Runtime:

- Background ticks are best effort and may skip when below threshold, above the
  hard threshold, or when another compaction is active.
- The hard threshold is the only compaction trigger allowed to block writes.
- Reads do not wait for background compaction except for normal short read-view
  lock interactions.

Close:

- Stop accepting new export work through the existing runtime close path.
- Signal background task shutdown.
- Wait for the task to exit.
- Run close-time best-effort compaction.
- Return close success even if compaction fails.

Observability:

- Continue logging `wal.compaction.completed` and `wal.compaction.failed`.
- Use phase values that distinguish `"background"`, `"write_pressure"`, and
  `"engine_close"`.
- Log skipped background ticks only at trace/debug level if logging is added.
  Do not spam info logs every 30 seconds.

# Alternatives Considered

## Continue WAL-Replay Compaction

This is already correct and simpler, but compaction cost grows with retained
WAL records. A hot block rewritten many times would force compaction to replay
obsolete writes even though only the latest visible state matters. Snapshot
compaction keeps the WAL debt trigger while making the output work depend on
visible dirty state.

## Full Read-Version Tree

A persistent read-version tree would make snapshots cheap and support arbitrary
historical reads. It also adds allocation and version-retention complexity to
the write path. The first background compactor only needs to capture the current
view at compaction start, so a metadata clone of the overlay is enough.

## Dirty Logical Bytes As The Trigger

Counting visible dirty logical bytes would prevent hot rewrites from triggering
background compaction. That is the wrong primary policy because compaction also
exists to bound retained WAL size and replay cost. Keep the trigger tied to WAL
debt and make the compaction implementation cheaper with read-view snapshots.

## Global Compaction Worker

A process-wide worker may eventually manage backend-wide fairness, but the
active engine currently owns read-view advancement, write pressure, and close
lifecycle. Per-export background tasks match the current ownership model and
keep the first implementation narrow.

# Migration / Rollout

No catalog migration is needed.

Rollout should preserve local and S3 blob-store compatibility because the
compactor still writes through `BlobStore`.

The implementation should be staged so each behavior arrives with its proof:

- introduce read-view snapshot capture with tests for repeated hot writes,
  metadata cloning, and snapshot isolation;
- introduce snapshot-based compaction with tests that prove it writes latest
  visible chunk state without replaying obsolete WAL records;
- move close and hard-threshold compaction to the snapshot path with tests for
  close success, hard backpressure, and read-view advancement;
- add background task lifecycle with tests for threshold skipping, trigger,
  busy-lock skipping, and shutdown before close compaction;
- keep Docker smoke validation for the integrated WAL durable path.

# Validation Strategy

Focused tests should be placed with the commits that introduce each contract.

Read-view snapshot validation:

- repeated writes to the same range increase `wal_debt_bytes` by every payload
  while the captured overlay contains only the latest visible extent;
- a captured snapshot keeps old visible data while later writes mutate the live
  overlay;
- snapshot capture returns `None` when no WAL is newer than the root.

Snapshot compaction validation:

- compaction from a snapshot writes the latest visible data for a hot rewritten
  chunk;
- compaction groups multiple visible extents in the same 32 MiB chunk into one
  blob write;
- publication advances the catalog base to the snapshot target;
- publication of a snapshot while newer writes exist subtracts only the
  snapshot debt and leaves later WAL debt visible;
- stale publication remains safe if another compaction already advanced the
  head.

Lifecycle validation:

- background tick skips below the soft threshold;
- background tick triggers at or above the soft threshold and below the hard
  threshold;
- background tick skips when `compaction_lock` is busy;
- hard-threshold compaction waits for the active compaction and blocks later
  writes through `write_lock`;
- hard-threshold compaction rechecks debt after acquiring the compaction lock;
- close stops background work before close compaction.

Integration validation:

```text
cargo fmt --all --check
cargo test -p nbd-server --lib
cargo test -p nbd-server --test wal_durable
cargo test -p nbd-server --test compaction
cargo test --workspace
make docker-smoke
make docker-smoke-s3
```

# Risks

- Snapshotting a large overlay clones metadata for every visible extent. This
  should be much cheaper than replaying large WAL payloads, but many tiny
  extents can still make the clone visible in profiles.
- Holding `write_lock` while waiting for background compaction at the hard
  threshold can stall writes. This is intentional pressure behavior, but a very
  slow backend can make the stall noticeable.
- Background compaction can create new 32 MiB blobs every time it publishes.
  Without GC, old blobs remain orphaned after later checkpoints. The WAL debt
  threshold should prevent overly frequent tiny compactions.
- Waiting for an in-progress background compaction during close may increase
  close latency.
- The first implementation has no public knobs. If defaults are wrong for small
  or very large disks, later work should add config after operational evidence.

# Open Questions

none

# Design Exit Criteria

- Retained WAL bytes are accepted as the compaction trigger metric.
- Snapshot-based compaction from the current read view is accepted.
- A metadata clone of the overlay is accepted instead of a full read-version
  tree.
- Background compaction is accepted as per-export Tokio task ownership.
- The initial 30 second interval, 512 MiB soft threshold, and 2 GiB hard
  threshold are accepted as internal constants.
- Hard-threshold compaction is accepted as write backpressure that may wait for
  the compaction lock while holding the write lock.
- Close-before-background-shutdown ordering is accepted.
- Behavior-specific tests landing with the behavior commits is accepted.

# Recommended Next Step

Run `$review-plan` after the draft design is accepted. A `ready for series
planning` result should lead to asking whether to start `$plan-series`, not to
implementation automatically.
