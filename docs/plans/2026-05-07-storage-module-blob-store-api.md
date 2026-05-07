Title: Storage Module And Blob Store API Cleanup
Date: 2026-05-07
Status: approved

# Problem

`LocalBlobStore` currently lives in `simple_durable.rs`, but it is no longer a
simple-durable-only detail. `SimpleDurableEngine`, `WalDurableEngine`, and
`CowCompactor` all use it. Keeping the blob store implementation inside one
engine file makes `crates/nbd-server/src` harder to navigate and obscures the
storage boundary that a future S3-compatible backend should implement.

The current method names also mix storage operations with higher-level
semantics:

- `create_blob` allocates a random `BlobKey` and writes bytes;
- `replace_blob` expresses simple durable's current mutable full-object update
  path;
- `read_blob` is the only operation whose name maps directly to backend blob
  I/O.

The upcoming S3 work should not inherit this shape. S3 and local storage should
share a simple get/put blob API, while callers remain responsible for the
semantic difference between immutable COW blob creation and simple durable
read-modify-write updates.

Today, blob-store handles are constructed directly by engine-opening paths.
That means each opened export path effectively owns its own `LocalBlobStore`
value, even though those values usually point at the same blob directory. This
cleanup should move blob-store ownership up to the shared export factory so
active exports clone one long-lived store handle instead of constructing their
own stores.

# Goal

Create an internal `storage` module under `crates/nbd-server/src` and move the
local blob implementation behind explicit blob-store traits.

The cleanup should:

- make blob storage easy to find in the source tree;
- give local storage and future S3 storage the same get/put API surface;
- keep mutable blob reuse visible through the dependency type;
- make create-only writes the base blob-store contract;
- move blob-store ownership out of per-export open paths and into the shared
  server/export-factory setup path;
- avoid changing export behavior, catalog metadata, WAL behavior, or protocol
  behavior;
- preserve the option to extract storage into a separate crate later.

# Constraints

- This is an internal organization and API cleanup, not the S3 implementation.
- `nbd-server` remains the crate boundary for now.
- Storage code must not learn about exports, WAL records, tree nodes,
  admission, compaction policy, sockets, or catalog transactions.
- `LocalBlobStore` must continue using async-safe local file operations or
  explicit blocking offload where needed.
- The API shape must not assume one storage client or request queue per export.
  A later S3 backend should be able to share one SDK client, HTTP connector,
  connection reuse, and concurrency limits across all active exports.
- The first cleanup should share one local blob-store handle through
  `ExportFactory`, even if local file I/O still executes directly without a
  storage queue.
- Simple durable remains the only current path that intentionally reuses a blob
  key for later full-object writes.
- COW committed blobs remain logically immutable. Compaction must continue to
  write fresh blob keys for committed COW chunks.
- Public re-exports should preserve existing external test imports where doing
  so keeps the refactor low-churn.

# Non-goals

- Implementing `S3BlobStore`.
- Adding S3 or AWS SDK dependencies.
- Introducing `crates/nbd-storage`.
- Changing config shape for selecting a storage backend.
- Changing `engine_kind`, `layout_kind`, or catalog schema.
- Changing WAL provider storage.
- Making `simple_durable` work with S3-compatible storage.
- Adding storage worker tasks, a producer/consumer storage queue, retry policy,
  or a backend connection pool implementation.
- Adding garbage collection or delete behavior.
- Changing chunk sizes or tree metadata semantics.

# End state

The server source tree has a focused storage module:

```text
crates/nbd-server/src/storage/
  mod.rs
  local.rs
```

`storage/mod.rs` defines the shared blob-store traits and handle aliases.
`storage/local.rs` owns the local file-backed implementation.

`LocalBlobStore` implements both the base blob-store trait and the mutable
extension trait. COW paths depend only on the base blob-store trait. Simple
durable depends on the mutable extension trait or on `LocalBlobStore` where
tests need local-specific access such as `root()`.

`ExportFactory` owns one shared local blob-store handle and clones it when
opening engines. It no longer constructs a fresh `LocalBlobStore` inside every
engine-open branch.

The handle-based shape leaves room for a later `S3BlobStore` to implement
`BlobStore` while owning one shared SDK client and HTTP connector. That is the
first intended S3 ownership model: active exports call the shared backend
object directly, and connection reuse comes from the shared client/connector.
This cleanup does not introduce storage worker queues.

The existing top-level `nbd_server::LocalBlobStore` re-export remains available
for tests and callers during this refactor.

# Proposed approach

Move `LocalBlobStore` and its local file helpers out of `simple_durable.rs` and
into `storage/local.rs`.

Change `ExportFactory` so it constructs one shared local blob-store handle
during factory setup. Engine-open code receives cloned trait handles instead of
calling `LocalBlobStore::new(...)` for each opened export.

Introduce a base blob-store trait with S3-shaped operation names:

```rust
#[async_trait::async_trait]
pub trait BlobStore: fmt::Debug + Send + Sync {
    async fn get_blob(
        &self,
        key: &BlobKey,
        offset: u64,
        len: u64,
    ) -> Result<Vec<u8>>;

    async fn put_blob(&self, key: &BlobKey, data: &[u8]) -> Result<()>;
}
```

`get_blob` reads a byte range from one blob. `put_blob` stores a full new blob
for one key and fails if that key already exists. The trait does not allocate
keys and does not know why a caller is writing a given key.

Introduce an extension trait for stores that support intentional mutable key
reuse:

```rust
#[async_trait::async_trait]
pub trait MutableBlobStore: BlobStore {
    async fn overwrite_blob(&self, key: &BlobKey, data: &[u8]) -> Result<()>;
}
```

The mutable extension trait owns the overwrite operation. It exists so call
sites can express their required storage semantics through dependency type:

- COW readers and compaction require `BlobStore` and can only create fresh
  blobs;
- simple durable requires `MutableBlobStore` because it intentionally updates
  existing full-chunk blobs.

Add shared handle aliases:

```rust
pub type BlobStoreHandle = Arc<dyn BlobStore>;
pub type MutableBlobStoreHandle = Arc<dyn MutableBlobStore>;
```

Keep random blob-key allocation outside the trait:

```rust
pub async fn put_random_blob<S: BlobStore + ?Sized>(
    store: &S,
    data: &[u8],
) -> Result<BlobKey>;
```

`put_random_blob` owns the retry loop over `BlobKey::random()`. This keeps the
base trait backend-shaped while preserving the existing random-key allocation
behavior for local compaction and sparse chunk creation.

For the local backend, `put_blob` keeps create-new behavior and
`overwrite_blob` uses the existing temp-file, rename, and directory-sync
strategy for full-object replacement. `overwrite_blob` must fail when the key
does not already exist. The exact local helper split can stay private to
`storage/local.rs`.

# Data model / API shape

No catalog data model changes are required.

The intended dependencies are:

```text
SimpleDurableEngine
  owns read-modify-write over full chunks
  requires MutableBlobStoreHandle
  calls get_blob for existing chunk bytes
  calls overwrite_blob for the resulting existing full chunk image
  calls put_random_blob for first writes to sparse chunks

CowTreeReader
  requires BlobStoreHandle
  calls get_blob for committed COW chunks

CowCompactor
  requires BlobStoreHandle
  calls get_blob for old committed chunks when needed
  calls put_random_blob for newly compacted committed chunks

LocalExportRegistry / ExportFactory
  owns one shared Arc<LocalBlobStore>
  passes it as BlobStoreHandle or MutableBlobStoreHandle by engine path

Future S3BlobStore
  may implement BlobStore
  may implement MutableBlobStore only if a future design accepts mutable use
  owns one shared SDK client and HTTP connector
  may enforce backend-wide concurrency limits
  remains below export, WAL, tree, and admission semantics
```

The future S3 backend should be able to implement:

```rust
impl BlobStore for S3BlobStore
```

It should not implement `MutableBlobStore` unless a later design explicitly
decides S3 is a valid backend for mutable key reuse.

# Invariants

- Blob storage is opaque byte storage only.
- Blob storage does not own catalog metadata or export lifecycle state.
- Blob storage does not assign logical chunk indexes or interpret tree shape.
- Base `BlobStore::put_blob` creates a new blob and must fail if the key
  already exists.
- Existing-key replacement is available only through `MutableBlobStore`, and
  must fail if the key does not already exist.
- Blob-store handles are shared across active exports by construction.
- A future S3 backend may reuse sockets and enforce backend-wide limits, but it
  must not become the source of truth for export ordering or write visibility.
- Simple durable's read-modify-write remains above the blob-store boundary.
- COW committed blobs are written under fresh keys and are never intentionally
  overwritten by COW readers or compaction.
- Missing sparse tree metadata continues to read as zeroes.
- Metadata that points at a missing or unreadable blob remains a storage error
  or corruption signal, not a sparse zero.
- The mutable extension trait is a dependency declaration, not a runtime
  permission system.

# Alternatives considered

## Separate `nbd-storage` crate

A separate crate would improve long-term dependency isolation, especially once
AWS SDK dependencies arrive. It is premature for this cleanup because the
boundary is still settling and current users all live in `nbd-server`.

The module layout should still avoid dependencies that would make later crate
extraction hard.

## `BlobWriteMode`

A single `write_blob` method with `CreateNew` and `OverwriteExisting` modes
would make overwrite intent explicit at every call site. It is heavier than the
current need and less idiomatic than a simple get/put object-store surface.

The chosen design keeps the API S3-shaped and expresses mutable-key reuse by
requiring `MutableBlobStore`.

## Marker-only `MutableBlobStore`

An empty `MutableBlobStore: BlobStore` marker would make mutable capability
visible in dependency types but would leave no separate method for existing-key
replacement. That would force `BlobStore::put_blob` to become create-or-replace,
which weakens the immutable COW contract.

The chosen design keeps `put_blob` create-only and puts full-object replacement
on `MutableBlobStore::overwrite_blob`.

## Producer/consumer storage runtime

A central queue with storage worker tasks would give the server explicit
backend-wide scheduling, fairness, retry shaping, and observability. It is more
structure than this cleanup needs, and it is not required to share S3
connections. A shared `S3BlobStore` can reuse sockets by owning one long-lived
SDK client and HTTP connector.

The chosen design moves ownership up to `ExportFactory` now. That supports the
first S3 backend as a shared client object. A producer/consumer storage runtime
can still replace or wrap that object later if scheduling needs become real.

# Migration / rollout

No runtime migration is needed. Existing blob files remain under
`runtime.blob_dir` and keep the same file names.

The rollout is a source-level refactor:

- add `storage/`;
- move local blob-store code into `storage/local.rs`;
- move local blob-store construction to `ExportFactory` setup;
- update engine and compaction imports;
- update method names from `read_blob` / `create_blob` / `replace_blob` to the
  new get/put/overwrite helpers;
- preserve existing public re-exports where practical.

# Validation strategy

Run focused tests that exercise every current blob-store caller:

```text
cargo fmt --all --check
cargo test -p nbd-server --test simple_durable
cargo test -p nbd-server --test compaction
cargo test -p nbd-server --test wal_durable
cargo test -p nbd-server --test local_export_registry
```

If the refactor touches only imports and method names in some paths, narrower
checks can run first, but the tests above are the minimum handoff evidence.

# Risks

- Trait objects can make local-only methods such as `root()` unavailable at
  call sites. Tests that need local path assertions should keep concrete
  `LocalBlobStore` values.
- Over-generalizing the storage module now could accidentally turn this cleanup
  into an S3 design. The module should stay minimal until the S3 design is
  reviewed.
- Sharing one local store handle now does not provide S3-style request
  coalescing by itself. It creates the ownership shape needed for a later
  shared S3 client, and a queue can still be added later if shared-client
  concurrency limits are not enough.
- Keeping `overwrite_blob` available for local storage means reviewers must
  still check that COW paths depend only on `BlobStore`, not
  `MutableBlobStore`.

# Open questions

None for this cleanup.

# Design exit criteria

This design is ready for `$review-plan` when:

- `BlobStore` with create-only `get_blob` / `put_blob` is accepted as the
  shared API;
- `MutableBlobStore: BlobStore` with `overwrite_blob` is accepted for
  existing-key full-object replacement;
- `storage/` as an internal `nbd-server` module is accepted;
- `ExportFactory` owning one shared local store handle is accepted;
- future S3 connection reuse through a shared backend object is accepted;
- producer/consumer storage queueing is acknowledged as compatible but
  intentionally out-of-scope;
- S3 implementation, S3 config, and crate extraction are confirmed as
  out-of-scope for this refactor.

# Recommended next step

Run `$review-plan` on this draft. Move to `$plan-series` only after review
returns `ready for series planning`.
