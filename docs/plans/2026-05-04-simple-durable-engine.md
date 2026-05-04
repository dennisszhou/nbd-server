Title: Simple Durable Engine
Date: 2026-05-04
Status: approved

# Problem

The current server can persist export metadata, but the only implemented data
engine is `MemoryExportEngine`. Data is lost when the process exits. The next
storage milestone needs an opt-in durable local backing store that preserves
data across clean restart without taking on the full WAL, read-view,
compaction, clone, and immutable copy-on-write tree design all at once.

The hard part is keeping the first durable engine honest. It should use the
same broad catalog and tree vocabulary that future WAL durability will need,
but it must not pretend that mutable local chunk files are immutable COW blob
snapshots.

# Goal

Add `SimpleDurableEngine`, an opt-in direct-commit local engine that stores
export data in sparse 32 MiB local blobs and records the sparse tree metadata
in SQLite.

The engine should:

- use 32 MiB blob-sized chunks;
- store chunk bytes under `LocalBlobStore`;
- store tree metadata in the catalog database;
- update catalog metadata only after all blob writes for a request are durable;
- support sparse zero reads when a chunk has no tree leaf;
- allow reads when no active write overlaps the read byte range;
- allow one writer per 32 MiB chunk at a time;
- keep `memory` as the `nbdcli create` default engine;
- leave WAL, immutable COW roots, clone, compaction, and S3 for later.

# Constraints

- This design builds on the existing runtime/completion/admission boundary.
- `ExportAdmissionCtl` remains storage-agnostic. It must not know node IDs,
  blob paths, SQLite rows, or tree shape.
- `LocalBlobStore` owns single-blob file I/O only. It must not know export
  IDs, chunk indexes, admission, tree metadata, or multi-chunk requests.
- SQLite remains the metadata source of truth.
- A loaded `SimpleMutableTree` may cache metadata for one active export, but
  the cache is not durable truth.
- Tree metadata for the simple engine is export-private and mutable by
  insertion. It is not clone-shareable.
- Normal simple durable writes do not create historical generations.
- Existing memory exports and the default memory create path must keep working.
- The implementation must remain testable with temp blob directories.
- Local blob file operations must use async-safe file APIs or explicit blocking
  offload. Full-blob writes and fsyncs must not block Tokio core worker
  threads with synchronous filesystem calls.

# Non-goals

- WAL append, WAL replay, WAL durability, or WAL-backed flush semantics.
- `WALDurableEngine`.
- Immutable COW tree publication.
- Clone or snapshot support.
- S3 or other remote object storage.
- Garbage collection for orphan local blobs.
- Atomic all-or-nothing semantics across multi-chunk write requests.
- Online resize.
- Historical export generations or root history.
- Advertising `NBD_FLAG_CAN_MULTI_CONN`.

# End State

Operators can create an opt-in simple durable export:

```text
nbdcli create disk-a --size 1G --engine simple_durable
```

The server opens that export through `LocalExportRegistry`, constructs a
`SimpleDurableEngine`, and serves reads, writes, and flushes through the
existing `ExportRuntime` path.

Data for materialized chunks lives under a local blob directory. The generated
default config should place that directory under:

```text
~/.cache/nbd/blobs
```

Explicit test and operator configs may set `runtime.blob_dir` to isolate data.

SQLite stores the current export head and sparse tree metadata. Missing tree
children read as zeroes. Existing chunk overwrites update only the blob file.
First writes to sparse chunks create blob files first, fsync them, then commit
the new tree metadata in one database transaction.

# Proposed Approach

Use one metadata database with explicit layout semantics.

`SimpleDurableEngine` uses:

- `layout_kind = simple_mutable_tree`;
- export-private tree nodes and edges;
- `storage_kind = mutable_blob`;
- stable blob keys whose full contents may be replaced by later writes.

Future WAL/COW durability should use:

- `layout_kind = cow_immutable_tree`;
- immutable tree nodes and edges;
- `storage_kind = immutable_blob`;
- root publication through compaction.

The two layouts may share table names, but rows must carry explicit layout and
storage kind values so readers cannot silently interpret mutable simple chunks
as immutable COW blobs.

## Component Responsibilities

`ExportAdmissionCtl`

- owns active/waiting reads, writes, and flushes;
- validates ranges against the export size;
- grants permits for visible byte ranges and write chunk spans;
- does not inspect tree or blob metadata.

`SimpleDurableAdmissionPolicy`

- maps `ExportRequest` to admission operations;
- computes 32 MiB chunk spans for writes;
- needs only export size and chunk size.

`SimpleDurableEngine`

- holds admitted requests through the existing `AdmittedExportRequest` type;
- paginates reads and writes into 32 MiB chunk operations;
- coordinates `SimpleMutableTree` and `LocalBlobStore`;
- keeps admission permits live through file I/O and metadata commits.

`SimpleMutableTree`

- owns tree interpretation for one active simple durable export;
- loads the current head and tree rows from SQLite on open;
- resolves chunk indexes to blob keys or sparse zeroes;
- commits new chunk metadata after blob files are durable;
- updates its in-memory view only after the database transaction commits.

`LocalBlobStore`

- maps blob keys to files under `runtime.blob_dir`;
- reads ranges from one blob;
- creates one new full blob with a random key;
- replaces one existing full blob by temp-file, fsync, rename, and directory
  fsync;
- uses Tokio file APIs or explicit blocking offload for file and fsync work;
- does not know export-level request boundaries.

# Data Model / API Shape

Replace the current latest-generation-as-head model with an explicit current
head table.

```text
exports
  id
  name
  engine_kind        -- memory | simple_durable
  block_size
  state
  created_at
  updated_at
  deleted_at
```

```text
export_heads
  export_id primary key
  layout_kind        -- memory_empty | simple_mutable_tree
  root_node_id null
  size_bytes
  checkpoint_wal_seq
  updated_at
```

`root_node_id = null` means the committed/current tree is all zeroes.
`checkpoint_wal_seq` remains `0` for `SimpleDurableEngine`.

```text
tree_nodes
  id primary key
  layout_kind        -- simple_mutable_tree
  owner_export_id    -- set for simple mutable nodes
  kind               -- internal | leaf
  level
  span_start_bytes
  span_len_bytes
  created_at
```

```text
tree_edges
  parent_node_id
  slot
  child_node_id

  primary key (parent_node_id, slot)
```

```text
tree_leaf_refs
  node_id primary key
  storage_kind       -- mutable_blob
  storage_key
  len_bytes          -- 32 MiB for v1
  created_at
```

The old `export_generations` name should not remain the serving source of
truth. If a compatibility migration needs to preserve the old table during the
transition, runtime code should still load current state from `export_heads`.

The catalog domain API should mirror that source of truth:

```rust
pub struct ExportHead {
    layout_kind: ExportLayoutKind,
    root_node_id: Option<NodeId>,
    size_bytes: u64,
    checkpoint_wal_seq: WalSeq,
}

pub enum ExportLayoutKind {
    MemoryEmpty,
    SimpleMutableTree,
}
```

`ExportMeta` should expose the current head through head-oriented naming.
Serving paths should not expose normal simple durable writes as generation
advancement. Any historical generation API belongs with the future immutable
WAL/COW root model.

The v1 simple mutable tree is a sparse database tree with one internal root
node and direct leaf edges keyed by chunk index. All nodes and edges are rows
in SQLite; there are no node files. The `level` and span fields leave room for
future multi-level COW trees, but v1 does not need intermediate nodes.

## Rust Structure

The exact module split can follow local style, but the core structures should
look like:

```rust
pub struct LocalBlobStore {
    root: PathBuf,
}

pub struct BlobKey(String);

impl LocalBlobStore {
    async fn create_blob(&self, data: &[u8]) -> Result<BlobKey>;
    async fn write_blob(&self, key: &BlobKey, data: &[u8]) -> Result<()>;
    async fn read_blob(&self, key: &BlobKey, offset: u64, len: u64)
        -> Result<Vec<u8>>;
}
```

`write_blob` replaces the full blob contents. It is not a partial write API.

```rust
pub struct SimpleMutableTree {
    export_id: ExportId,
    size_bytes: u64,
    root_node_id: Option<NodeId>,
    catalog: Arc<dyn ExportCatalog>,
}

impl SimpleMutableTree {
    async fn load(catalog: Arc<dyn ExportCatalog>, meta: &ExportMeta)
        -> Result<Self>;

    async fn lookup_chunk(&self, chunk_index: u64)
        -> Result<Option<BlobKey>>;

    async fn commit_new_chunks(&self, chunks: Vec<NewSimpleChunk>)
        -> Result<()>;
}
```

`SimpleMutableTree` should guard metadata commits with an internal async
mutex. Two writes to different sparse chunks may perform file work in
parallel, but root creation and tree row insertion must serialize so both
writes are represented in the current head.

```rust
pub struct SimpleDurableEngine {
    meta: ExportMeta,
    tree: SimpleMutableTree,
    blob_store: Arc<LocalBlobStore>,
}
```

Admission should gain a chunk-aware write shape:

```rust
pub struct ChunkSpan {
    first: u64,
    count: u64,
}

pub struct AdmissionWrite {
    visible_range: ByteRange,
    write_chunks: Option<ChunkSpan>,
}

pub enum AdmissionOp {
    Read(ByteRange),
    Write(AdmissionWrite),
    Flush,
}
```

`ExportAdmissionPolicy` should replace the current `ExportAdmissionProfile`
naming during the admission series. `MemoryAdmissionPolicy` can set
`write_chunks = None`. The simple durable policy sets the touched 32 MiB chunk
span.

# Data Paths

## Read Path

```text
ExportRuntime
  -> SimpleDurableAdmissionPolicy maps read to Read(range)
  -> ExportAdmissionCtl grants read permit
  -> SimpleDurableEngine keeps permit alive
  -> SimpleMutableTree resolves each touched chunk
  -> missing chunk: zero-fill
  -> present chunk: LocalBlobStore.read_blob(...)
  -> assemble reply bytes
```

Reads may run while a write is active when the read byte range does not
overlap the write's `visible_range`.

## Write Path

The engine paginates the request into chunk-sized operations.

```text
write offset=16MiB len=64MiB

chunk 0:
  blob offset 16MiB, data slice 0..16MiB

chunk 1:
  blob offset 0, data slice 16MiB..48MiB

chunk 2:
  blob offset 0, data slice 48MiB..64MiB
```

Request flow:

```text
ExportRuntime
  -> policy maps write to Write(visible_range, write_chunks)
  -> ExportAdmissionCtl grants write permit
  -> SimpleDurableEngine keeps permit alive
  -> for each touched chunk:
       lookup existing blob key
       read old full blob or allocate zero-filled 32 MiB buffer
       patch request bytes into the buffer
       create or replace exactly one full blob
  -> after all blob operations and fsyncs succeed:
       commit metadata for newly materialized chunks in one DB transaction
  -> reply success
```

Existing chunk overwrites do not update the database. New chunk materialization
adds leaf/ref/edge rows after file durability is established.

If file work succeeds for some chunks and then later file work fails, the
simple engine may expose a partial multi-chunk write. That limitation is
explicit for this engine. WAL durability is the future path for stronger
request-level recovery behavior.

## Flush Path

```text
ExportRuntime
  -> policy maps flush to Flush
  -> ExportAdmissionCtl grants global barrier permit
  -> SimpleDurableEngine flush is a no-op
  -> reply success
```

Flush is a no-op because successful writes reply only after their blob file
work and any needed metadata commits have completed.

# Invariants

- SQLite is the metadata source of truth.
- `SimpleMutableTree` in-memory state is a cache loaded from SQLite.
- In-memory tree state is updated only after the corresponding DB transaction
  commits.
- `LocalBlobStore` never updates metadata.
- `ExportAdmissionCtl` never reads or writes storage metadata.
- DB metadata for a newly materialized chunk is committed only after its blob
  file is fully written and fsynced.
- DB may lag the filesystem after an error or crash; orphan blobs are safe.
- DB must not point at a blob file that was never fully written.
- Simple durable tree nodes are export-private.
- Simple durable normal writes do not create generations or history.
- Missing tree children mean sparse zeroes.
- Existing chunk overwrites preserve the same `storage_key`.
- One active write may touch a given 32 MiB chunk at a time.
- Reads conflict with writes only by visible byte range, not whole chunk span.
- Flush conflicts globally.
- `memory` remains the default create engine.
- `simple_durable` remains opt-in.

# Alternatives Considered

Use immutable COW tree metadata for the simple engine:

- Rejected. It would make the direct-commit engine look clone-ready even though
  its chunk files are mutable. WAL/compaction should own immutable COW roots.

Use flat chunk-map metadata:

- Plausible, but rejected for v1 because the tree shape lets us exercise the
  reader and metadata model that the future COW tree will reuse.

Let `ExportAdmissionCtl` own the tree:

- Rejected. Admission owns scheduling and semantic permits. The tree owns
  logical range resolution to metadata and storage keys. Combining them would
  make admission a catalog/cache component.

Append a generation per simple write:

- Rejected. Old roots would not represent old data because chunk files are
  mutable.

Add a `metadata_version` now:

- Rejected for v1. No metadata cache invalidation protocol needs it yet.

# Migration / Rollout

Add catalog migrations that create `export_heads` and the tree metadata
tables. The head migration should populate `export_heads` from the latest
existing `export_generations` row for each export.

Stage the public `simple_durable` engine kind with the engine rollout, not the
schema foundation. Today `nbdcli create` parses `ExportEngineKind` directly, so
adding a domain enum variant also exposes a new `--engine` value.

Runtime code should load export head state from `export_heads` after the
migration. Existing memory exports should continue to load as all-zero heads
with `layout_kind = memory_empty` and `root_node_id = null`.

Add `runtime.blob_dir` to config. Generated default config should use
`~/.cache/nbd/blobs`. Explicit configs that omit the field should derive a
safe default from `runtime.state_dir`, and tests should set a temp directory
explicitly.

Expose `simple_durable` through `nbdcli create --engine simple_durable` when
`SimpleDurableEngine` is wired into the registry, while leaving the CLI
default as `memory`.

# Validation Strategy

Validation should prove each boundary at the layer where it matters:

- catalog tests for migration, `export_heads`, tree nodes, edges, leaf refs,
  and simple durable engine kind parsing once the engine kind is exposed;
- admission tests for chunk-aware write conflicts, non-overlapping reads, and
  flush barriers;
- local blob store tests for create, full replacement, ranged read, fsync/rename
  behavior as far as unit tests can observe, and path containment;
- simple mutable tree tests for sparse zero resolution, root creation, leaf
  insertion, and metadata commit after blob work is staged;
- engine tests for read/write/flush behavior over sparse and existing chunks;
- protocol integration tests for simple durable exports across server restart;
- existing memory protocol, workspace, clippy, and Docker smoke checks to prove
  no regression to the default path.

# Risks

- The database and filesystem are separate durability domains. The file-first,
  DB-second order prevents DB pointers to unwritten blobs, but it can leave
  orphan blobs.
- Multi-chunk writes are not atomic across all touched chunks on failure.
- Whole-blob replacement writes 32 MiB for small overwrites, which is simple
  and safe but not efficient.
- A mutable tree layout cannot support clone sharing.
- If tree cache updates happen before DB commit, reads may observe metadata
  that is not durable. The implementation must update memory after commit.
- If `runtime.blob_dir` defaults are unclear, tests can accidentally write to
  operator state. Tests must use explicit temp config.

# Open Questions

None.

# Design Exit Criteria

- The database tables and layout semantics are explicit.
- `SimpleDurableEngine` is clearly direct-commit and mutable, not COW.
- `LocalBlobStore` owns only single-blob file I/O.
- `SimpleMutableTree` owns metadata interpretation and DB commits.
- `ExportAdmissionCtl` remains storage-agnostic while supporting chunk-aware
  write locks.
- Crash and multi-chunk atomicity limitations are documented.

# Recommended Next Step

Review the staged execution artifact for this design with `$review-execution`.
