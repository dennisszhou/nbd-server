Title: Export Head Ownership And Compaction
Date: 2026-05-06
Status: approved

# Problem

The current catalog model lets `ExportMeta` carry an `engine_kind` and a
separate `ExportHead`. That shape allows invalid combinations to exist in Rust
values until an engine rejects them later. It also blurs durable catalog state
with live runtime state: the same `ExportHead` name is used both for the
database row and for the base version that a running engine serves.

WAL durable compaction has a similar ownership problem. The current
`CompactionManager` is owned outside the engine and close-triggered compaction
reopens WAL/catalog state after the runtime closes. The component that best
understands the serving base, WAL overlay, and read-view advancement is the
open `WalDurableEngine`, not the registry.

# Planning Inputs

`docs/plans/2026-05-04-local-wal.md` remains the source of truth for the WAL
primitive, replay boundary, and checkpoint semantics. This design supersedes
that plan's Part 3 compaction ownership shape where it describes close
enqueueing a registry-owned or background-only `CompactionManager` after the
engine has been dropped.

`docs/architecture/export-catalog-architecture.md` is an older draft catalog
architecture. This design supersedes its `ExportMeta` joined type, struct
`ExportHead`, and `checkpoint_wal_seq` naming for this effort.

# Goal

Refactor export metadata and WAL compaction so that:

- `ExportDescriptor` describes only the `exports` row;
- `ExportHead` is a typed durable catalog head decoded from `export_heads`;
- operator-facing joined state is named `ExportRecord`;
- serving/open paths receive an `ActiveExportDescriptor`;
- engines own live serving state derived from catalog heads;
- `TreeReader` remains stateless with respect to read versions;
- `WalDurableEngine` owns a `CompactionCoordinator`;
- close attempts compaction through the engine's applied WAL high watermark;
- compaction failure on close is logged but does not make close fail;
- stop-the-world compaction can block WAL durable writes when WAL debt exceeds
  a hard threshold;
- retry, backoff, generational compaction, and async write admission policy are
  left for future work.

# Constraints

- Backward compatibility with existing local development catalog data is not a
  requirement for this refactor.
- Existing Prisma migrations may be replaced or rebaselined instead of carrying
  a compatibility migration stack for every previous prototype schema.
- Prototype Prisma migration history and local catalog state may be discarded
  rather than preserved through compatibility migrations.
- The SQLite catalog remains the durable source of truth for export rows,
  current heads, and committed COW tree metadata.
- WAL sequence numbers remain per export.
- The local WAL remains append-only from the engine's perspective.
- Published COW heads are complete snapshots through their `base_wal_seq`.
- Compaction publication remains idempotent behind the transactional catalog
  head update.
- Reads should continue during stop-the-world compaction when possible.
- The first write-pressure threshold is an internal 2 GiB WAL debt constant,
  with test-only construction hooks for smaller thresholds.
- New write backoff, retry scheduling, group commit, or generational commit
  policy is out of scope for the first implementation.

# Non-goals

- Introducing a remote WAL service.
- Implementing btrfs-style multi-generation transaction commits.
- Implementing a write backoff scheme.
- Making old prototype SQLite databases migrate forward without data loss.
- Changing NBD protocol behavior or adding new client-visible export states.
- Adding a general-purpose storage engine abstraction as part of this refactor.
- Solving active read-view retention for multi-process serving.

# End State

Catalog APIs expose a clear durable model:

```rust
pub struct ExportDescriptor { ... }       // exports row
pub struct ActiveExportDescriptor(...);   // serving-safe exports row
pub enum ExportHead { ... }               // typed export_heads row
pub struct ExportRecord { ... }           // descriptor + head
```

The `ExportHead` enum encodes the layout-specific head variants. Engine kind
and layout kind no longer drift as independent fields inside a joined metadata
value.

WAL durable opens from an active descriptor plus a typed COW head/tree snapshot.
The engine converts that catalog state into a runtime `RootSnapshot` owned by
`ExportReadView`. The read view owns the current serving base, overlay, and
cache. The catalog still owns the durable head.

`WalDurableEngine` owns a `CompactionCoordinator`. The coordinator can compact
through a chosen WAL sequence, publish a new catalog head transactionally,
advance the engine read view to the new root, and prune eligible WAL records.
On close, the engine attempts this sequence through its latest applied WAL
sequence. Failure is observable but non-fatal because replay from the old
catalog head plus retained WAL remains correct.

# Proposed Approach

## Catalog Model

Keep `ExportDescriptor` as the exports-row type. It should include identity,
name, block size, engine kind, lifecycle state, and timestamps. It should not
include head/root/checkpoint state.

Introduce `ActiveExportDescriptor` as a newtype wrapper around
`ExportDescriptor`. Catalog open/load methods that are safe for serving return
this wrapper only after rejecting deleted exports. The active/deleted invariant
belongs to the export row, not to the head.

Replace the struct-shaped `ExportHead` with typed variants:

```rust
pub enum ExportHead {
    MemoryEmpty(MemoryExportHead),
    SimpleMutableTree(SimpleMutableTreeHead),
    CowImmutableTree(CowImmutableTreeHead),
}

pub struct MemoryExportHead {
    size_bytes: ExportSize,
}

pub struct SimpleMutableTreeHead {
    size_bytes: ExportSize,
    root_node_id: Option<NodeId>,
}

pub struct CowImmutableTreeHead {
    size_bytes: ExportSize,
    root_node_id: Option<NodeId>,
    base_wal_seq: WalSeq,
}
```

Use `base_wal_seq` instead of `checkpoint_wal_seq` for the COW head field. It
means that the committed base already contains WAL records through that
sequence. Code that publishes compaction can still use `compacted_through` for
the target sequence being materialized.

Use `ExportRecord` for joined descriptor-plus-head views returned by create,
inspect, list, clone, and compaction publication outcomes. Avoid
`ActiveExportRecord` unless a real API later needs descriptor-plus-head state
that is guaranteed active.

## Runtime Ownership

The catalog owns durable records. Engines own serving state derived from those
records. `TreeReader` owns no version state.

For WAL durable:

```rust
pub struct WalDurableEngine {
    descriptor: ActiveExportDescriptor,
    wal: ExportWalHandle,
    read_view: Arc<ExportReadView>,
    compaction: CompactionCoordinator,
    write_lock: Mutex<()>,
}

pub struct ExportReadView {
    state: RwLock<ExportReadViewState>,
    tree_reader: Arc<dyn TreeReader<RootSnapshot>>,
}

struct ExportReadViewState {
    root: RootSnapshot,
    last_applied_seq: WalSeq,
    wal_debt_bytes: u64,
    overlay: OverlayExtentMap,
    cache: ReadCache,
}
```

`RootSnapshot` is the runtime read base derived from `ExportHead` and COW tree
metadata. It is not the catalog source of truth. `ExportReadView::advance_root`
is the only path that moves a live read view to a newer committed base.

The engine and coordinator should share `Arc<ExportReadView>` rather than
borrowing between sibling fields. This avoids self-referential structs and
keeps ownership idiomatic.

## Compaction Coordinator

Move WAL durable compaction ownership into the active engine:

```rust
pub struct CompactionCoordinator {
    export_id: ExportId,
    wal: ExportWalHandle,
    catalog: Arc<dyn CowTreeMetadataStore>,
    blob_store: LocalBlobStore,
    read_view: Arc<ExportReadView>,
    policy: CompactionPolicy,
    state: Mutex<CompactionState>,
}

pub struct CompactionPolicy {
    wal_debt_threshold_bytes: u64,
}
```

The coordinator owns the state machine for one open export. A stateless helper
may still own the mechanics of reading WAL records and writing COW blobs, but
the active engine owns target selection, write gating, publication, read-view
advancement, and close behavior.

`wal_debt_bytes` is engine-local derived state, not durable source of truth.
It is the sum of payload bytes for applied WAL records after the current
runtime base. Replay initializes it from records with `seq > base_wal_seq`.
Each newly appended and applied write adds its payload length. Successful
compaction through the current `last_applied_seq` advances the base and resets
debt to zero. If a future partial compaction advances to a sequence before
`last_applied_seq`, the first implementation may leave debt conservatively high
rather than subtracting per-record byte lengths.

Stop-the-world compaction is the first write-pressure policy. When WAL debt is
below the hard threshold, writes append to WAL and update the read view. The
production threshold starts as an internal 2 GiB constant rather than a public
config surface. When debt reaches the hard threshold, the writer that observes
the threshold compacts through the latest applied WAL sequence while holding
the engine write lock. New writes wait behind that lock. Reads continue
against the current read view except for the brief `advance_root` update.

Close compaction uses the same coordinator path but should be best effort:

```text
runtime drains accepted jobs
runtime calls engine.close()
WalDurableEngine asks coordinator to compact through last_applied_seq
on success: catalog head advances, read view advances, WAL prefix may prune
on failure: log and return Ok(())
```

This behavior is correct because the catalog head update is the authoritative
commit point. If compaction writes blobs but fails before publication, those
blobs are orphaned scratch outputs. If publication succeeds but pruning fails,
future opens can still recover from the newer head plus retained WAL.

## Runtime Close API

Extend the engine/runtime boundary so the runtime can close the engine after it
has drained jobs:

```rust
#[async_trait::async_trait]
pub trait ExportEngine: Send + Sync {
    fn admission_policy(&self) -> ExportAdmissionPolicyHandle;

    async fn execute_admitted(&self, request: AdmittedExportRequest)
        -> ExportResult;

    async fn close(&self) -> Result<()> {
        Ok(())
    }
}
```

`ConcurrentExportRuntime` already owns an engine handle. `SerialExportRuntime`
should retain an engine handle in addition to the worker task path so
`close()` can call `engine.close().await` after accepted work is complete.

`LocalExportRegistry` should stop reopening WALs or directly enqueueing
close-triggered compaction. It should close the runtime and let the engine own
engine-specific cleanup.

# Data Model / API Shape

Catalog-facing API shape:

```rust
#[async_trait::async_trait]
pub trait ExportCatalog: Send + Sync {
    async fn create_export(&self, request: CreateExport)
        -> Result<ExportRecord>;

    async fn load_export_descriptor(&self, name: ExportName)
        -> Result<ActiveExportDescriptor>;

    async fn load_export_head(&self, export_id: &ExportId)
        -> Result<ExportHead>;

    async fn inspect_export(&self, request: InspectExport)
        -> Result<ExportRecord>;

    async fn list_exports(&self, request: ListExports)
        -> Result<Vec<ExportRecord>>;
}
```

Engine construction shape:

```rust
impl ExportFactory {
    pub async fn open_export(
        &self,
        descriptor: ActiveExportDescriptor,
    ) -> Result<ExportRuntimeHandle>;
}
```

The factory loads the typed head or engine-specific tree snapshot after it
knows the active descriptor's engine kind. Engines receive only the typed state
they can legally serve.

Compaction-facing API shape:

```rust
impl CompactionCoordinator {
    pub async fn maybe_compact_for_write_pressure(
        &self,
        target: WalSeq,
    ) -> Result<CompactionDecision>;

    pub async fn compact_through(&self, target: WalSeq)
        -> Result<CompactionResult>;

    pub async fn close_best_effort(&self);
}
```

Exact method names may change during implementation, but the ownership should
not: the coordinator is engine-local and advances the engine read view after a
successful catalog publication.

# Invariants

- `ExportDescriptor` carries no head/root/checkpoint state.
- `ActiveExportDescriptor` is never deleted.
- `ExportHead` variants make engine/layout-specific head state explicit.
- Memory heads cannot carry root nodes or WAL sequence state.
- Simple mutable heads cannot carry WAL sequence state.
- COW immutable heads carry `base_wal_seq`.
- `ExportRecord` is a catalog/operator view, not live serving state.
- The catalog head is the durable source of truth.
- `RootSnapshot` is runtime serving state derived from catalog state.
- `TreeReader` receives the read version as an argument and owns no version.
- `wal_debt_bytes` is derived pressure state; it must not be required for
  correctness.
- `WalDurableEngine` owns the read view and compaction coordinator.
- Only successful catalog publication advances the durable base.
- `ExportReadView::advance_root` must not advance beyond `last_applied_seq`.
- Close compaction failure must not lose acknowledged writes.
- WAL pruning is safe only after the catalog head represents the pruned prefix.

# Operational And Lifecycle Contracts

Opening a WAL durable export:

1. Load an `ActiveExportDescriptor`.
2. Load the COW tree/head state for the descriptor.
3. Construct `RootSnapshot` from the durable base.
4. Replay WAL records after `base_wal_seq`.
5. Apply replayed records to `ExportReadView`.
6. Start serving only after replay succeeds.

Writing to a WAL durable export:

1. Write admission authorizes the logical range.
2. The engine write lock serializes WAL append and read-view application.
3. Append returns only after the WAL record is durable.
4. The record is applied to the read view.
5. If WAL debt exceeds the hard threshold, the coordinator compacts through a
   stable applied target before releasing the write lock.

If write-pressure compaction fails after the WAL append has become durable and
the read view has been updated, the write still succeeds. The failure leaves
the retained WAL debt in place, logs the failure, and lets a later write or
close attempt compaction again.

Closing a WAL durable export:

1. Runtime stops accepting new jobs and drains accepted jobs.
2. Runtime calls `engine.close().await`.
3. WAL durable close attempts best-effort compaction.
4. Failure is logged and close returns success.
5. The next open recovers from the last published head plus retained WAL.

# Alternatives Considered

## Prefix Catalog Types With `Catalog`

Names such as `CatalogExportDescriptor` and `CatalogExportHead` make database
origin explicit, but they are noisy. The better distinction is structural:
`ExportDescriptor` is an exports row, `ExportHead` is a durable head row, and
runtime state uses names such as `RootSnapshot` and `ExportReadView`.

## Use Generic Typestate For Every Export State

Types such as `ExportMeta<Active, WalDurable>` would encode more state at
compile time, but engine kind and head layout are selected from the database at
runtime. A closed `ExportHead` enum plus `ActiveExportDescriptor` gives the
important safety without making the runtime open path generic-heavy.

## Keep Global `CompactionManager`

A global worker is useful for fire-and-forget cleanup, but it is the wrong
owner for serving-base advancement and write gating. The active engine already
owns the WAL handle, read view, and write serialization. Engine-local
coordination is simpler and makes lifecycle ownership explicit.

## Implement Generational Compaction Now

Generational commit would allow writes to continue into a new generation while
an older generation compacts. That is closer to mature filesystem behavior,
but it needs more policy: write backoff, repeated threshold handling, target
rotation, and active-reader retention. Stop-the-world compaction is sufficient
for the first correctness boundary.

# Migration / Rollout

Backward compatibility with existing local prototype catalogs is not required.
The Prisma schema and migrations may be rebaselined to the new model by
replacing the current prototype migration stack with one fresh baseline
migration. Existing development databases may be discarded and recreated.
There is no requirement to preserve the current saved prototype migrations in
Prisma history.

Rollout should still be staged in code:

1. Introduce the typed catalog model and update SQLite row decoding.
2. Update catalog tests to prove invalid engine/head combinations are rejected
   at the catalog boundary.
3. Update registry/factory open paths to consume `ActiveExportDescriptor`.
4. Move WAL durable compaction ownership into `WalDurableEngine`.
5. Add engine close hooks and make close compaction best effort.
6. Add stop-the-world write-pressure compaction behind explicit thresholds.
7. Remove the old registry-owned close compaction path.

# Validation Strategy

- Catalog model tests:
  - memory heads reject root and WAL state;
  - simple mutable heads reject WAL state;
  - COW heads decode `base_wal_seq`;
  - serving descriptor loads reject deleted exports;
  - inspect/list still return deleted export records when requested.

- WAL durable read-view tests:
  - replay starts after `base_wal_seq`;
  - `advance_root` retires overlay through the published base;
  - advancing beyond `last_applied_seq` is rejected.

- Compaction coordinator tests:
  - compacting through an applied target publishes a new head;
  - repeated compaction through the same target is idempotent;
  - failed publication leaves the old head serving and replayable;
  - close compaction failure is logged/observable but returns success;
  - hard-threshold compaction blocks a second write behind the write lock.

- Integration tests:
  - WAL durable writes survive restart when close compaction fails;
  - WAL durable close compaction reduces replay debt on reopen;
  - clone reads from a published COW base and starts with its own WAL.

# Risks

- Rebaselining Prisma migrations can simplify implementation but may hide
  accidental schema drift unless tests cover fresh database creation.
- Stop-the-world compaction may produce long write stalls for large WAL debt.
- Holding the engine write lock during compaction is simple but can tie up
  runtime queue slots and write admission longer than ideal.
- Orphaned compaction blobs are possible when publication fails before the
  catalog head advances. This is acceptable until garbage collection exists.
- Close best-effort compaction must be observable enough that silent repeated
  failures do not leave replay debt growing unnoticed.

# Open Questions

None.

# Design Exit Criteria

- The descriptor/head/record naming is accepted.
- `base_wal_seq` is accepted as the replacement for `checkpoint_wal_seq`.
- Engine-local `CompactionCoordinator` ownership is accepted.
- Close best-effort compaction semantics are accepted.
- Stop-the-world write-pressure compaction is accepted as the first pressure
  policy, with backoff and generational compaction deferred.
- Prisma migration rebaseline/discard behavior is accepted.

# Recommended Next Step

Produce a staged implementation series with `$plan-series`.
