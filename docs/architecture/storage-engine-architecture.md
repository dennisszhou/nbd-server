Title: Storage Engine Architecture
Date: 2026-05-12
Status: approved

# Problem

The server needs one blob-storage boundary that can use local files during
development and S3-compatible object storage for the demo path. That boundary
must stay below exports, WAL records, tree nodes, compaction policy, and NBD
request semantics.

# Goal

Define blob storage as an opaque byte-store API:

- read byte ranges from blobs;
- create full blobs by key;
- never overwrite through the base blob-store contract;
- expose the same create/read contract for local and S3-compatible backends;
- make intentional mutable-key reuse visible through a separate capability.

# Scope

Blob storage stores opaque bytes. It does not store or interpret catalog
metadata.

`ExportCatalog` owns export metadata, current export heads, tree node metadata,
child pointers, root pointers, checkpoints, and blob references.

`ExportWal` does not use blob storage. The current WAL provider stores local
WAL files. A future remote WAL backend belongs behind the WAL provider
contract, not behind `BlobStore`.

The code boundary is currently the `storage` module in `nbd-server`:

```text
crates/nbd-server/src/storage/
  mod.rs
  local.rs
  s3.rs
```

`LocalBlobStore` is the local backend. `S3BlobStore` is compiled only when the
`nbd-server/s3` Cargo feature is enabled.

# API Shape

The base trait is create-only:

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

`get_blob` reads one byte range from one blob. `len == 0` returns an empty
buffer without touching the backend.

`put_blob` writes a complete new blob and must fail if the key already exists.
Random key allocation is owned by the helper above the trait:

```rust
pub async fn put_random_blob<S: BlobStore + ?Sized>(
    store: &S,
    data: &[u8],
) -> Result<BlobKey>;
```

Backends classify create collisions in a shared way so `put_random_blob` can
retry random-key collisions without retrying unrelated storage failures.

# Mutable Extension

Some local prototype paths intentionally reuse a blob key for full-object
replacement. That capability is separate from the base contract:

```rust
#[async_trait::async_trait]
pub trait MutableBlobStore: BlobStore {
    async fn overwrite_blob(&self, key: &BlobKey, data: &[u8]) -> Result<()>;
}
```

`LocalBlobStore` implements both `BlobStore` and `MutableBlobStore`.
`S3BlobStore` implements only `BlobStore`.

`simple_durable` requires `MutableBlobStore` because it performs
read-modify-write updates of export-private mutable chunk blobs. COW readers
and compaction require only `BlobStore`, so they cannot intentionally overwrite
committed blobs.

# Configured Backend

Process config selects one blob backend:

```toml
[blob_store]
kind = "local"
```

or:

```toml
[blob_store]
kind = "s3"
endpoint_url = "http://rustfs:9000"
region = "us-east-1"
bucket = "everstore"
access_key_id = "rustfsadmin"
secret_access_key = "rustfsadmin"
force_path_style = true
key_prefix = "v0.1/blobs/"
auto_create_bucket = true
```

`ConfiguredBlobStore::open` constructs one process-level backend handle.
`ExportFactory` clones that handle for active exports. This is the current S3
connection-reuse model: the shared `S3BlobStore` owns one AWS SDK client, and
active exports call it directly. A future storage runtime can wrap this
boundary if backend-wide scheduling, retries, or explicit worker queues become
necessary.

# Blob Identity And Location

Catalog leaf metadata stores a one-component `BlobKey`. It does not store S3
buckets, prefixes, endpoints, credentials, or full URIs.

The configured backend maps the blob id to a concrete location. For S3:

```text
bucket = "everstore"
key_prefix = "v0.1/blobs/"
storage_key = "abc123"

resolved object = "s3://everstore/v0.1/blobs/abc123"
```

For the current local backend:

```text
runtime.blob_dir = "/var/lib/nbd/blobs"
storage_key = "abc123"

resolved file = "/var/lib/nbd/blobs/abc123"
```

Switching an existing catalog between local and S3 is not a transparent
migration. The catalog keys are the same, but the configured backend resolves
those keys in a different namespace.

# Local Backend

The local backend:

- maps blob keys to local files under `runtime.blob_dir`;
- uses create-new file semantics for `put_blob`;
- uses ranged file reads for `get_blob`;
- supports existing-key full replacement through `MutableBlobStore`;
- is the only backend that can serve `simple_durable`.

Local overwrite is a local-filesystem contract. It does not imply that all
blob backends can safely support mutable blob replacement.

# S3-Compatible Backend

The S3 backend:

- maps blob keys to `key_prefix + BlobKey` object keys;
- normalizes and validates relative key prefixes;
- uses `PutObject` with `If-None-Match: *` for create-only writes;
- maps S3 create collisions to `BlobAlreadyExists`;
- uses ranged `GetObject` for reads;
- can optionally create the bucket at startup for demo/test environments;
- does not implement `MutableBlobStore`.

Missing referenced S3 objects are storage errors, not sparse holes. Auth,
endpoint, bucket, and range errors are not create collisions.

RustFS is the current S3-compatible endpoint used by Docker smoke tests. It is
replaceable by any endpoint that honors the same create-only and ranged-read
contract.

# Tree Reader Interaction

Server tree readers own logical read resolution above `BlobStore`.
`SimpleTreeReader` resolves simple mutable chunks through `MutableBlobStore`.
`CowTreeReader` resolves committed COW chunks through the base `BlobStore`.
Both share lazy sparse-tree metadata helpers.

- walks sparse catalog/tree metadata;
- locates the leaf blob for a logical range;
- calls `BlobStore::get_blob` for the needed bytes;
- zero-fills ranges that are absent from the sparse tree by design;
- treats referenced-but-missing blobs as corruption or storage failure.

The distinction is important:

```text
missing sparse child pointer:
  valid hole, read as zeroes

tree metadata points to blob key but storage cannot read it:
  corruption or storage failure
```

# Sparsity And Corruption

Tree nodes may be sparse. A missing child pointer means the corresponding
logical range has no committed data and should zero-fill.

Malformed metadata is corruption. Examples:

- a materialized internal node has no reachable leaf descendants;
- an internal node child pointer references a missing node;
- a leaf node references a missing blob;
- node span/level metadata does not match the expected tree shape;
- blob length is inconsistent with the leaf metadata.

# Invariants

- `BlobStore` is a blobstore API, not an export API.
- `BlobStore::put_blob` never overwrites an existing key.
- Existing-key replacement is only available through `MutableBlobStore`.
- `S3BlobStore` does not implement `MutableBlobStore`.
- `simple_durable` requires a mutable local backend.
- WAL does not use `BlobStore`.
- Tree/catalog metadata owns blob references.
- Blob bytes are opaque to the blob store.
- Leaf metadata stores one-component blob ids.
- Missing sparse tree children zero-fill.
- Missing referenced nodes or blobs are corruption/storage errors.
- Local and S3-compatible backends share the same visible create/read
  contract for COW committed blobs.

# Open Questions

- Whether blob keys should become content-addressed later.
- Whether the local backend should checksum blobs on read.
- Whether future blob deletion should be idempotent or report missing keys.
- Whether temporary compaction blobs should use a separate key namespace before
  root publication.
- Whether backend-wide storage queues become necessary after S3 load testing.
