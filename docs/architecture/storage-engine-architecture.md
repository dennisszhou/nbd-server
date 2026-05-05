Title: Storage Engine Architecture
Date: 2026-05-01
Status: draft

# Problem

The system needs one storage abstraction that can be backed by local files
during prototyping and S3-compatible object storage later. That abstraction
should not know about exports, WAL records, tree nodes, compaction policy, or
read/write semantics.

# Goal

Define `StorageEngine` as a blobstore-like API:

- store immutable blobs by key;
- read whole blobs or byte ranges;
- delete blobs when a higher-level GC says they are unreachable;
- never overwrite an existing key;
- expose the same contract for local and S3-compatible implementations.

# Scope

`StorageEngine` stores opaque blob bytes. It does not store or interpret
catalog metadata.

`ExportCatalog` owns export metadata, current export heads, tree node metadata,
child pointers, root pointers, checkpoints, and blob references.

`ExportWal` does not use `StorageEngine`. The WAL has its own replaceable
backend behind `WalProvider`.

# API Shape

Conceptual API:

```rust
trait StorageEngine {
    async fn put_blob(&self, key: BlobKey, data: Bytes) -> Result<()>;

    async fn get_blob(&self, key: BlobKey) -> Result<Bytes>;

    async fn get_blob_range(
        &self,
        key: BlobKey,
        offset_bytes: u64,
        len_bytes: u64,
    ) -> Result<Bytes>;

    async fn delete_blob(&self, key: BlobKey) -> Result<()>;
}
```

`put_blob` must fail if the key already exists. The storage engine never
overwrites keys.

# Blob Identity

Blob keys are allocated by higher layers. `StorageEngine` treats them as opaque
names.

Higher layers decide whether keys are:

- content-addressed;
- random IDs;
- namespace-prefixed by object purpose;
- temporary compaction outputs.

The storage engine enforces only the no-overwrite blobstore contract.

# Leaf Blob Relationship

Committed tree metadata references 32 MiB leaf blobs by `BlobKey`. The
metadata lives in `ExportCatalog` or catalog-managed tree-node records. The
blob bytes live in `StorageEngine`.

`StorageEngine` does not know that a blob is a leaf. It only stores and reads
bytes by key.

# Local And S3 Backends

Both backends implement the same API.

Local backend:

- maps blob keys to local files;
- uses create-new semantics for `put_blob`;
- supports ranged reads from files;
- is sufficient for local prototype validation.

S3-compatible backend:

- maps blob keys to object keys;
- uses conditional create semantics where available;
- supports ranged GET;
- reports overwrite attempts as errors.

Backend-specific retry, multipart upload, and provider details stay behind the
same API.

# CommittedTreeReader Interaction

`CommittedTreeReader` owns logical read resolution:

- walks sparse catalog/tree metadata;
- locates the leaf blob for a logical range;
- calls `StorageEngine.get_blob_range` for the needed bytes;
- zero-fills ranges that are absent from the sparse tree by design;
- treats referenced-but-missing blobs as corruption.

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

However, malformed metadata is corruption. Examples:

- a materialized internal node has no reachable leaf descendants;
- an internal node child pointer references a missing node;
- a leaf node references a missing blob;
- node span/level metadata does not match the expected tree shape;
- blob length is inconsistent with the leaf metadata.

# Invariants

- `StorageEngine` is a blobstore API, not an export API.
- `StorageEngine` never overwrites an existing key.
- WAL does not use `StorageEngine`.
- Tree/catalog metadata owns blob references.
- Blob bytes are opaque to `StorageEngine`.
- Missing sparse tree children zero-fill.
- Materialized internal nodes with no reachable leaf descendants are
  corruption.
- Missing referenced nodes or blobs are corruption/storage errors.
- Local and S3-compatible backends share the same visible contract.

# Open Questions

- Whether blob keys should be content-addressed or random IDs initially.
- Whether the first local backend should checksum blobs on read.
- Whether `delete_blob` should be idempotent or report missing keys.
- Whether temporary compaction blobs should use a separate key namespace before
  root publication.
