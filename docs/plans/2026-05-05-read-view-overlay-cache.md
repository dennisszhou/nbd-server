Title: Read View Overlay And Cache
Date: 2026-05-05
Status: approved

## Problem

`ExportReadView` is still served by a retained WAL sequence map. Reads scan all
retained records, clone the records that overlap the read, read committed bytes
from the tree, and overlay WAL bytes afterward.

That is correct, but it is the wrong serving model:

- repeated writes to the same logical range keep shadowed WAL records live in
  the read view even though only the newest bytes can be visible;
- reads scale with retained WAL record count instead of the ranges that overlap
  the read;
- repeated reads of committed blob-backed ranges still traverse the tree and
  read blob bytes;
- checkpointed WAL bytes have no explicit demotion path into an optional cache;
- cache entries cannot be aged out under byte pressure; and
- partial tree fills do not have an ownership policy.

The durable WAL remains the source of truth for recovery and compaction. The
read view should be the in-process serving view: latest visible WAL bytes
first, optional cached committed bytes second, and the committed tree on cache
miss.

## Current Baseline

The branch already has the cleanup that this design depends on:

```rust
pub struct RootSnapshot {
    backing: RootBacking,
}

enum RootBacking {
    Zero {
        root_node_id: Option<NodeId>,
        checkpoint_wal_seq: WalSeq,
        size_bytes: u64,
    },
    CowTree(Arc<CowTreeSnapshot>),
}
```

`RootSnapshot` is a readable committed root, not only an `ExportHead` summary.
For COW roots it owns the immutable `CowTreeSnapshot`; for the legacy empty-root
path it carries zero backing metadata. A cloned `RootSnapshot` is sufficient to
serve a committed read after the read-view lock is released.

The durable engines also share a tree-reading boundary:

```rust
#[async_trait::async_trait]
pub(crate) trait TreeReader<R>: fmt::Debug + Send + Sync {
    async fn read_committed(&self, root: &R, range: ByteRange)
        -> Result<Block>;
}

pub(crate) struct Block {
    range: ByteRange,
    parts: Vec<BlockPart>,
}

pub(crate) enum BlockPart {
    Data { range: ByteRange, bytes: Bytes },
    Zero { range: ByteRange },
}
```

`TreeReader` means "read the tree metadata, then read the referenced blocks".
It does not own the current root. Callers pass the captured root or tree
snapshot into `read_committed`.

`Block` is the returned committed byte description. It is not a cache object.
It contains absolute export ranges. `BlockPart::Data` owns bytes with
`Bytes`; `BlockPart::Zero` represents sparse committed ranges without payload.
Current callers still call `Block::materialize()` to produce `Vec<u8>` replies.

The COW and simple tree readers split returned blocks on their natural tree
chunk boundaries. With the current layout that means a read of `[0, 48 MiB)`
returns parts shaped like:

```text
[0, 32 MiB)
[32 MiB, 48 MiB)
```

The cache layer introduced by this design sits below `ExportReadView` and above
`TreeReader`. It consumes `Block` parts and decides how to split them into
smaller cache objects.

## Goal

Replace the linear retained-WAL overlay with a normalized range model that:

- keeps only the latest visible WAL bytes in the authoritative overlay;
- frees WAL records from the read view once no visible overlay or cache extent
  references them;
- adds a real committed-byte cache for retired WAL bytes and tree fills;
- bounds cache memory with byte-based object LRU eviction;
- splits cache ownership on fixed 2 MiB aligned cache-block windows;
- preserves `TreeReader` natural block boundaries separately from cache-block
  boundaries;
- defines source-limited cache-object merge and ownership rules; and
- keeps export replies as owned contiguous `Vec<u8>` values for now.

## Constraints

- V1 keeps one `RwLock` around `ExportReadView` state.
- `ExportAdmissionCtl` remains responsible for logical read/write/flush
  ordering. The read-view lock protects data structures, not semantic request
  admission.
- `checkpoint_wal_seq` is a global WAL prefix. A root at checkpoint `S`
  represents every WAL record with sequence `<= S`.
- `advance_root` is valid only when the read view has already applied every WAL
  record from the current checkpoint through the new checkpoint.
- The serving model remains one active read view for an export in one process.
- `ExportReply::Read` continues to own a contiguous `Vec<u8>`.
- The protocol path rejects oversized requests before constructing an
  `ExportJob`. The read view may split internal work, but it must not split one
  export request into multiple completion replies.
- V1 uses a 1 GiB per-export internal cache byte budget. Construction must
  accept an explicit budget so config can replace the default later.
- WAL files remain the durable recovery and compaction source of truth.
- Cache entries are optional serving state and may be evicted at any time.

## Non-goals

- Inline compaction.
- Reloading evicted WAL payloads from WAL segment offsets.
- Scatter/gather export replies or vectored socket writes.
- Installing a catalog root that was not reached through WAL already applied to
  this read view.
- Multi-host read-view refresh or lease-based WAL retention.
- A benchmark harness in the first implementation series.
- Replacing the COW tree or blob storage format.
- A process-wide cache coordinator.

## End State

`ExportReadView` owns one mutable state object:

```rust
pub struct ExportReadView {
    state: RwLock<ReadViewState>,
    tree_reader: Arc<dyn TreeReader<RootSnapshot>>,
}

struct ReadViewState {
    root: RootSnapshot,
    last_applied_seq: WalSeq,
    overlay: OverlayExtentMap,
    cache: ReadCache,
}
```

Read priority is:

```text
1. OverlayExtentMap
2. ReadCache
3. TreeReader
```

The overlay is authoritative for WAL bytes newer than the current root
checkpoint. The cache is a real serving source, but optional. Any cached byte
is reconstructable from the current root plus newer overlay entries.

`TreeReader` returns natural tree-backed `Block` parts. The read cache then
splits `BlockPart::Data` into aligned 2 MiB cache-block windows for ownership,
LRU, and byte accounting. `TreeReader` does not know about the 2 MiB cache
policy.

## Proposed Approach

### Shared Extent Map Primitive

The overlay and cache both need range overwrite mechanics. V1 should introduce
a small local extent-map primitive backed by `BTreeMap<u64, Extent<V>>`, not a
new `rangemap` dependency.

The primitive owns:

- half-open `[start, end)` ranges;
- checked end-offset arithmetic;
- lookup of extents overlapping a range;
- overwrite insertion with left and right tail preservation;
- range removal and coverage calculation;
- optional coalescing when payloads are mergeable; and
- debug/test invariant checks for non-empty, non-overlapping extents.

Overlay and cache then layer payload ownership, WAL reference updates, cache
object backrefs, and LRU updates on top of this range primitive.

### Overlay Extent Map

The overlay is a normalized latest-byte map:

```rust
struct OverlayExtentMap {
    extents: BTreeMap<u64, OverlayExtent>,
}

struct OverlayExtent {
    end: u64,
    seq: WalSeq,
    record: Arc<WalRecord>,
    record_offset: u64,
}
```

An extent means:

```text
logical [start, end) is served from record.data()
starting at record_offset
```

Applying a WAL record inserts the record's range and splits or removes older
overlapping extents.

Example:

```text
seq 1 writes [0, 8) = A
seq 2 writes [4, 6) = B

overlay:
[0, 4) -> seq 1, record_offset 0
[4, 6) -> seq 2, record_offset 0
[6, 8) -> seq 1, record_offset 6
```

Repeated writes to the same block keep only the last visible extent:

```text
seq 1 writes [0, 4096)
seq 2 writes [0, 4096)
seq 3 writes [0, 4096)

overlay:
[0, 4096) -> seq 3
```

The durable WAL may still retain `seq 1` and `seq 2` for recovery and
compaction. The read view must not keep their payloads live just because the
durable WAL still exists.

### TreeReader Blocks

`TreeReader` reads committed bytes from a captured root or snapshot:

```rust
trait TreeReader<R> {
    async fn read_committed(&self, root: &R, range: ByteRange)
        -> Result<Block>;
}
```

`Block` is a read result, not a cache entry:

```rust
struct Block {
    range: ByteRange,
    parts: Vec<BlockPart>,
}

enum BlockPart {
    Data { range: ByteRange, bytes: Bytes },
    Zero { range: ByteRange },
}
```

`Block` invariants:

- `Block.range` is the requested range.
- parts are ordered by logical offset;
- parts are contiguous and exactly cover `Block.range`;
- data part byte length equals the part range length; and
- zero parts carry no payload.

Tree readers split on natural tree chunk boundaries. They do not split on cache
block boundaries. For the current tree layout, a read of `[0, 48 MiB)` returns:

```text
Block {
  range: [0, 48 MiB),
  parts: [
    [0, 32 MiB),
    [32 MiB, 48 MiB),
  ],
}
```

If the second tree chunk is sparse, the second part is `BlockPart::Zero`.

The current engine path may call `Block::materialize()` to produce a `Vec<u8>`.
The cache insertion path must consume `BlockPart::Data` directly so owned
`Bytes` can move into cache objects without copying the whole read result.

### Read Cache

The read cache is a logical extent map whose extents reference cache objects.
The cache is source-agnostic: a cache extent represents logical bytes whether
the bytes came from a retired WAL extent or a tree fill.

Cache priority is below overlay and above `TreeReader`.

```rust
const CACHE_BLOCK_BYTES: u64 = 2 * 1024 * 1024;

struct ReadCache {
    extents: BTreeMap<u64, CacheExtent>,
    objects: BTreeMap<CacheObjectId, CacheObject>,
    lru: CacheObjectLru,
    max_bytes: usize,
    charged_bytes: usize,
}

struct CacheExtent {
    end: u64,
    object_id: CacheObjectId,
    object_offset: u64,
}

struct CacheObject {
    block_index: u64,
    logical_start: u64,
    len: u64,
    payload: CachePayload,
    extents: BTreeSet<u64>,
    charged_bytes: usize,
}

enum CachePayload {
    Bytes(Bytes),
    WalRecord {
        record: Arc<WalRecord>,
    },
}
```

`CACHE_BLOCK_BYTES` defines fixed aligned cache-block windows:

```text
[0, 2 MiB)
[2 MiB, 4 MiB)
[4 MiB, 6 MiB)
...
```

A cache object must be contained in exactly one aligned cache-block window. It
may be smaller than the full 2 MiB window. This avoids pinning 2 MiB for a
10-byte retained hole while still bounding object locality, LRU churn, and
eviction granularity.

Cache insertion works one aligned cache-block window at a time. Within one
window, insertion may merge newly inserted bytes with existing cached bytes
into one new `CachePayload::Bytes` object. Merge work is allowed in v1 because
the copy cost is capped by `CACHE_BLOCK_BYTES`.

Merging is source-limited. It may only materialize contiguous byte spans for
which the insertion has a source, either from the new bytes or from existing
cache objects. It must not create cached bytes for holes that neither source
covers.

The cache may temporarily contain multiple objects in the same aligned 2 MiB
window. When an insertion merges them, replaced extents point into the new
object and old objects with no extents are removed immediately.

`BlockPart::Zero` is not inserted into the read cache in v1. It satisfies the
current read as zeros. A future zero-extent cache can be added with explicit
metadata budgeting if sparse-read tree traversal becomes material.

### LRU And Budgeting

The LRU is object-based:

```text
head = most recently used / hottest object
tail = least recently used / eviction candidate
```

The LRU is exact:

- every live cache object has exactly one LRU node;
- promoting an object moves that node;
- removing an object removes that node; and
- lazy duplicate LRU entries are not allowed.

A merge that replaces existing objects inherits the coldest LRU position among
the replaced inputs. If the insertion does not merge with an existing object,
tree fills enter at the LRU head and retired WAL fills enter at the LRU tail.

Eviction pressure is byte-based:

```text
while charged_bytes > max_bytes:
  evict tail object O
  remove every cache extent in O.extents
  remove O from objects and LRU
  subtract O.charged_bytes
```

`CachePayload::Bytes` charges `bytes.len()`.

`CachePayload::WalRecord` charges the full pinned `record.data().len()`.
Because the full WAL record payload is pinned, WAL-record cache objects are
allowed only when that full pinned payload is small enough for the v1 policy.
Otherwise retired WAL bytes are copied into `Bytes` objects split by aligned
2 MiB cache-block windows.

This keeps the cache budget honest and keeps the normal LRU unit bounded by the
2 MiB cache-block policy. A future WAL payload representation based on
`Bytes` could allow zero-copy slicing without pinning an oversized record.

### Tree Fill Policy

Tree fills are the lowest-priority cache insertion. A tree fill may only insert
bytes that are uncovered by both the current overlay and the current cache at
insertion time:

```text
holes = block_part.range
holes -= overlay.coverage(block_part.range)
holes -= cache.coverage(block_part.range)
insert only remaining holes
```

This rule makes in-flight backing reads safe. If another read filled the cache
before this `TreeReader` call returned, this fill can only populate ranges that
are still empty.

Only the newly returned tree bytes are admitted for empty holes. A merge may
re-home existing cache bytes into a new object, but it must preserve their
logical byte values exactly.

Tree fill insertion also checks that the active root still equals the root
captured by the read plan. If the root advanced while the tree read was in
flight, the read reply still uses the captured root, but returned tree bytes are
not inserted into cache.

For each `BlockPart::Data`, insertion first splits by aligned 2 MiB cache-block
windows. Each window builds source spans from:

- the returned `BlockPart::Data` bytes that are still eligible to cache; and
- existing cache bytes in that same aligned window.

Insertion partitions those sources into contiguous covered spans. For each
covered span:

1. If the span includes existing cache bytes or more than one source, copy the
   span into one merged `Bytes` object.
2. If the span is only newly returned tree bytes, keep that window slice as one
   `Bytes` object.
3. If sources do not cover a gap between two spans, leave the gap uncached and
   create separate objects for the separate covered spans.

This preserves locality when a tree read returns enough bytes to bridge nearby
cached slices, while avoiding a rule that pins or invents bytes for unknown
holes. No tree fill cache object may cross an aligned 2 MiB cache-block
boundary.

Example:

```text
TreeReader returns BlockPart::Data [0, 32 MiB)
only [0, 10) is still empty

policy:
keep one 10-byte Bytes object in cache block [0, 2 MiB)
```

Example:

```text
cache already has [0, 8 KiB)
TreeReader returns BlockPart::Data [8 KiB, 12 KiB)

policy:
merge into one Bytes object for [0, 12 KiB)
new object inherits the older object's LRU position
```

Example:

```text
cache already has [0, 8 KiB)
TreeReader returns BlockPart::Data [12 KiB, 16 KiB)

policy:
do not merge across the unknown [8 KiB, 12 KiB) gap
keep [0, 8 KiB) and [12 KiB, 16 KiB) as separate cache objects
```

### Retired Overlay Cache Policy

`advance_root(new_root)` moves the committed root forward. It is valid only if
the read view has applied all WAL records through
`new_root.checkpoint_wal_seq`.

When the root advances, overlay extents with
`seq <= new_root.checkpoint_wal_seq` are no longer authoritative because the
new root represents them. Those extents are retired from the overlay and may be
inserted into the read cache.

Retirement operates on visible overlay extents, not raw WAL records. A repeated
write sequence must not insert shadowed bytes into cache.

Retired extents are processed deterministically:

```text
retired = visible overlay extents with seq <= checkpoint
sort by (seq ascending, logical_start ascending)
insert in that order
```

If an external API uses "prune before S" semantics, its retained prefix is
`seq < S`. For `advance_root` with checkpoint `S`, the checkpoint is inclusive
and the retired prefix is `seq <= S`.

Retired WAL insertion overwrites older cache entries for the same logical
range. Correctness is still preserved if pressure immediately evicts the new
cache object because the new root now represents those bytes.

V1 retired WAL ownership policy:

```rust
const RETIRED_WAL_KEEP_MAX_PINNED_BYTES: usize = CACHE_BLOCK_BYTES as usize;
```

Retired visible bytes are split by aligned 2 MiB cache-block windows before
cache insertion.

For each retired window slice:

1. If it overlaps or is adjacent to existing cache bytes in the same window,
   build contiguous covered spans from the retired bytes and existing cached
   bytes, then materialize each span as one merged `Bytes` object.
2. Else if the full WAL payload is at most
   `RETIRED_WAL_KEEP_MAX_PINNED_BYTES`, the retired slice covers the whole WAL
   payload, and the payload fits within one aligned cache-block window, keep the
   `Arc<WalRecord>` as one `CachePayload::WalRecord`.
3. Otherwise copy the retired slice into a right-sized `Bytes` object for that
   aligned cache-block window.

Merging retired WAL bytes with existing cache bytes always produces
`CachePayload::Bytes`. If the merge absorbs an older `CachePayload::WalRecord`
object, the old WAL pin is released once its old extents are removed.

New retired WAL objects are inserted at the LRU tail. A retired WAL merge that
replaces existing objects inherits the coldest LRU position among the replaced
inputs. Later reads promote it normally.

Within one root-advancement batch, lower sequence numbers are colder than
higher sequence numbers.

### Read Flow

Read execution still returns an owned `Vec<u8>`:

```text
read(range):
  acquire read lock
  validate range against state.root
  build a read plan:
    overlay slices for overlay-covered bytes
    cache slices for cache-covered holes
    tree miss ranges for remaining holes
  clone Arc payload references needed by the plan
  capture state.root for tree misses
  release read lock

  read miss ranges with TreeReader using captured root
  assemble output Vec<u8>

  acquire write lock if cache objects were hit or tree fills returned
  promote cache-hit objects that still exist
  insert/merge BlockPart::Data fills into uncovered cache holes
  evict cache objects until charged_bytes <= max_bytes
  release write lock

  return Vec<u8>
```

`ExportReadView::read` is not a request splitter. One admitted
`ExportRequest::Read` produces one `ExportReply::Read`. The read view may split
internal overlay/cache/tree work, but the export boundary remains one request,
one reply.

### Write Flow

Writes keep the durable ordering:

```text
append WAL record
apply durable record to ExportReadView
reply success
```

`apply_wal_record` validates bounds, requires contiguous WAL application,
updates `last_applied_seq`, and inserts the record into `OverlayExtentMap`.
It also trims overlapping cache bytes from the written range. Overlay priority
would make stale cache safe, but trimming keeps memory behavior predictable.

### Root Advancement

The read-view API is:

```rust
impl ExportReadView {
    pub async fn advance_root(&self, new_root: RootSnapshot) -> Result<()>;
}
```

`advance_root` validates:

- `new_root.size_bytes == current_root.size_bytes`;
- `new_root.checkpoint_wal_seq >= current_root.checkpoint_wal_seq`;
- equal checkpoints are no-ops;
- `new_root.checkpoint_wal_seq <= last_applied_seq`; and
- WAL records have been applied contiguously since the current root checkpoint.

Then it retires visible overlay extents with
`seq <= new_root.checkpoint_wal_seq`, inserts eligible retired bytes into cache,
removes retired extents from the overlay, and installs `new_root`.

## Data Model / API Shape

Core state:

```rust
struct ReadViewState {
    root: RootSnapshot,
    last_applied_seq: WalSeq,
    overlay: OverlayExtentMap,
    cache: ReadCache,
}
```

Tree-reader API:

```rust
#[async_trait::async_trait]
trait TreeReader<R>: fmt::Debug + Send + Sync {
    async fn read_committed(&self, root: &R, range: ByteRange)
        -> Result<Block>;
}

struct Block {
    range: ByteRange,
    parts: Vec<BlockPart>,
}

enum BlockPart {
    Data { range: ByteRange, bytes: Bytes },
    Zero { range: ByteRange },
}
```

Read-view operations:

```rust
impl ExportReadView {
    pub async fn read(&self, range: ByteRange) -> Result<Vec<u8>>;

    pub async fn apply_wal_record(&self, record: WalRecord) -> Result<()>;

    pub async fn advance_root(&self, new_root: RootSnapshot) -> Result<()>;
}
```

Cache operations:

```rust
insert_overlay_record(record: Arc<WalRecord>) -> Result<()>
retire_overlay_through(seq: WalSeq) -> Vec<RetiredOverlayExtent>
insert_retired_overlay(retired: RetiredOverlayExtent)
insert_tree_block_parts_if_uncovered(root: &RootSnapshot, block: Block)
promote_cache_hits(hits: &[CacheObjectId])
evict_cache_to_budget()
```

The cache max-byte value is an explicit construction parameter. V1 may pass a
1 GiB default until operator configuration exists.

## Invariants

- Overlay extents are non-empty and non-overlapping.
- Cache extents are non-empty and non-overlapping.
- Every cache extent references an existing cache object.
- Every cache object tracks all cache extent start keys that reference it.
- Every cache object is contained in one aligned 2 MiB cache-block window.
- Cache objects may be smaller than one 2 MiB window.
- Cache lookup priority is independent of original byte source.
- The LRU has exactly one node per live cache object.
- Cache insertion may merge existing same-window objects into a new
  `CachePayload::Bytes` object.
- A merged cache object never crosses one aligned 2 MiB cache-block window.
- A merged cache object covers only contiguous bytes for which insertion has a
  source.
- A merge that replaces existing objects inherits the coldest LRU position among
  those inputs.
- Eviction is driven by `charged_bytes > max_bytes`.
- Removing or splitting a cache extent updates the owning object's extent set.
- A cache object with no extents is removed immediately.
- Cache eviction removes all extents for the evicted object.
- `ReadCache.charged_bytes` counts cache-owned objects, not transient read
  clones.
- Overlay bytes always beat cache bytes.
- Cache bytes always beat tree reads.
- Tree fills never change the logical value of overlay or cache bytes. Cache
  insertion may re-home existing cache bytes into a merged object.
- Tree fills are inserted only if the active root still equals the captured
  read-plan root.
- `Block` parts exactly cover `Block.range` in order.
- `BlockPart::Data.bytes.len() == BlockPart::Data.range.len()`.
- `BlockPart::Zero` has no payload and is not cached in v1.
- Retired overlay bytes may overwrite older cache bytes.
- Retired overlay extents are processed in `(seq, logical_start)` order.
- Retired WAL cache objects are inserted at the LRU tail.
- Retired WAL bytes merged with existing cache bytes become
  `CachePayload::Bytes`.
- Tree fill cache objects are inserted at the LRU head.
- Cache hits promote touched cache objects to the LRU head.
- `CachePayload::WalRecord` charges the full pinned WAL payload length.
- Oversized or cross-block retired WAL payloads are copied into `Bytes` objects
  or skipped; they are not kept as one oversized LRU object.
- `apply_wal_record` applies WAL records contiguously.
- `last_applied_seq` is never lower than `root.checkpoint_wal_seq`.
- `advance_root` never installs a checkpoint greater than `last_applied_seq`.
- Shadowed WAL records are not retained by the overlay once no overlay extent
  references them.
- Read replies own their returned bytes and do not borrow from the read view.
- A logical cache without generation tags is valid only under the contract that
  roots advance through WAL already applied to this read view.

## Alternatives Considered

### Keep Records By Sequence And Add A Range Index

This would speed lookup, but it would still keep shadowed WAL records in memory
unless a separate visibility model existed. The latest-byte extent map is both
the visibility model and the lookup structure.

### Tag Cache Entries By Root Generation

Generation tags are necessary if a read view can install a root that did not
arrive through locally applied overlay records. This design keeps the narrower
single-server contract: roots advance only through applied WAL. Under that
contract, same-root tree fills are safe because they only fill empty holes, and
retired overlay entries overwrite older cache.

If root advancement occurs while a tree read is in flight, the returned bytes
are not inserted into cache.

### Cache By BlobKey Only

Blob-key caching is simpler for immutable COW blobs, but it does not cache
retired WAL records and does not avoid logical tree traversal for repeated
small ranges. This design uses logical cache extents first. A future immutable
blob cache can be added behind `TreeReader` if needed.

### Never Merge Cache Objects

Keeping every partial fill as its own object would minimize copy work at
insertion time. It would also fragment same-window locality and make later LRU
behavior harder to reason about. This design allows bounded local merging
because the worst-case copy is capped at one 2 MiB cache-block window and all
merged bytes have explicit sources.

### Use The `rangemap` Crate

`rangemap` would provide a ready-made range map, but the read view needs tight
control over overwrite side effects. Every split, trim, and removal must update
WAL references, cache object backrefs, and exact LRU state. A small local
`BTreeMap` primitive keeps those effects visible and testable.

### Use Linux's `Folio` Term

The design borrows the idea of bounded cache units from page-cache folios, but
does not use the term. `Folio` carries Linux memory-management meaning that is
not quite this API. `Block` and `BlockPart` describe the tree-read result.
`CacheObject` and aligned 2 MiB cache-block windows describe cache ownership.

### Eagerly Materialize Every Retired WAL Byte

Copying retired WAL bytes into `Bytes` objects everywhere would make cache
payloads uniform. It would also make root advancement copy-heavy. V1 keeps
small, block-contained WAL records by `Arc<WalRecord>` when that is honest for
budgeting, and copies larger or cross-block retired bytes into cache-block
bounded `Bytes` objects.

### Add Scatter/Gather Read Replies Now

Scatter/gather could avoid materializing a contiguous reply buffer. That would
change the export and connection reply contract. This design keeps `Vec<u8>`
replies so overlay and cache correctness can be reviewed independently.

## Migration / Rollout

No durable data migration is needed. Catalog, WAL, COW tree, and blob formats
do not change.

Rollout is in-process:

- startup constructs the read view from the current root and WAL replay;
- replay applies records into the overlay extent map instead of a sequence map;
- reads use overlay, cache, then `TreeReader`;
- writes still append to WAL before applying to the read view; and
- root advancement remains constrained by applied WAL sequence continuity.

The committed `TreeReader` and `RootSnapshot` cleanup is compatible with this
plan and remains the baseline.

## Validation Strategy

Primitive and read-view tests should prove:

- `Block` rejects missing, overlapping, out-of-order, or wrong-length parts;
- COW and simple tree readers split `[0, 48 MiB)` into natural tree parts;
- sparse tree chunks return `BlockPart::Zero`;
- the extent-map primitive preserves non-overlap after insert, split, trim,
  remove, and coalesce operations;
- repeated same-block writes retain only the newest visible overlay extent;
- middle splits preserve correct source offsets;
- reads assemble overlay, cache, and tree bytes in priority order;
- tree fills insert only uncovered holes;
- tree fills from an inactive root are not cached;
- tiny partial fills retain only known bytes instead of pinning a large slice;
- same-window adjacent fills merge into one `Bytes` object;
- same-window fills separated by an unknown gap do not merge across the gap;
- cache-object merge inherits the coldest LRU position among replaced inputs;
- merging an existing WAL-backed cache object materializes `Bytes` and releases
  the old WAL pin after its extents are removed;
- tree fills crossing aligned 2 MiB cache-block windows create separate cache
  objects per window;
- `BlockPart::Zero` is not inserted into cache in v1;
- retired WAL objects enter the LRU tail;
- tree fill objects enter the LRU head;
- root advancement does not cache shadowed raw WAL records;
- retired WAL merges with existing objects inherit the coldest LRU position;
- cache hits promote touched objects;
- eviction is based on charged bytes rather than object count;
- WAL-record cache objects charge the full pinned WAL payload length;
- oversized retired WAL payloads are copied or skipped instead of pinned as one
  oversized cache object;
- evicting a cache object removes all referenced extents;
- trimming cache extents removes unreferenced objects;
- `apply_wal_record` trims cache entries under the written range;
- equal-checkpoint `advance_root` is a no-op;
- `advance_root` rejects checkpoints beyond `last_applied_seq`; and
- replay plus restart still reconstructs the same visible bytes.

Relevant existing integration coverage should continue to pass:

```text
cargo test -p nbd-server --test wal_durable
cargo test -p nbd-server --test compaction
cargo test -p nbd-server --test local_export_registry
make test-protocol
```

Before handoff, run formatter and broader workspace checks as the implementation
scope warrants.

## Risks

- Extent splitting bugs can silently return wrong byte ranges.
- Cache object GC must update both the extent map and object table every time
  an extent changes.
- The untagged logical cache depends on applied-overlay root advancement.
  Future direct catalog refresh must not reuse this cache unchecked.
- Cache budget accounting intentionally excludes temporary in-flight read
  clones. That must be documented in code.
- Bounded merge copies are capped at 2 MiB, but merge policy must avoid copying
  while a read lock is held.
- LRU inheritance for merges must be exact so cache aging does not drift.
- Copying large retired WAL records into cache-block bounded `Bytes` objects can
  add checkpoint-time copy cost.
- Keeping small retired WAL records by `Arc<WalRecord>` can still pin bytes
  that are no longer all visible, so the keep-whole policy must charge the full
  pinned payload.
- A single read-view `RwLock` keeps the concurrency model simple, but cache LRU
  touches and fill insertion may add write-lock traffic on read-heavy exports.
- The 1 GiB cache budget is per active export. Many active exports will need an
  operator-facing setting or process-wide cache coordinator later.

## Open Questions

- none

## Design Exit Criteria

- `TreeReader` remains the shared root/snapshot-in, committed-block-out API.
- `Block` / `BlockPart` remains the tree-read result API.
- The overlay extent map is accepted as the authoritative latest-byte WAL view.
- The read cache object and extent ownership model is accepted.
- The 2 MiB aligned cache-block window policy is accepted.
- The bounded same-window cache-object merge policy is accepted.
- The tree fill and retired WAL ownership policies are accepted.
- The root advancement contract is accepted without generation tags under the
  single-server applied-overlay contract.
- The 1 GiB internal cache default is accepted for v1.

## Recommended Next Step

Use `$plan-series` to update the implementation series around the current
`RootSnapshot`, `TreeReader`, `Block`, and `BlockPart` baseline.
