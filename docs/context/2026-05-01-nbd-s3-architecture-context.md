Title: NBD S3 Architecture Context
Date: 2026-05-01
Status: discussion context

# Problem

Build an NBD server backed by S3-compatible storage with arbitrary named
exports, durable `read`, `write`, and `flush` behavior, and a design that can
grow into clone/fork support, compaction, garbage collection, writer fencing,
cache policy, and gateway lease updates.

The first implementation should validate the core data path before optimizing
the full long-term architecture.

# Core Invariant

A successful write is durable in the WAL and visible to later reads. A flush
waits for all prior writes to reach that same durable and visible state.

# Major Components

## NBDServer

Protocol boundary. Handles fixed newstyle NBD handshake, `NBD_OPT_GO`,
`NBD_OPT_ABORT`, `read`, `write`, and `flush`. The NBD layer should stay thin
and delegate export behavior to `Export`.

## Export

Per-mounted disk object. Owns the read, write, and flush behavior for one
export.

## ExportAdmissionCtl

Admission and scheduling layer. Controls operation ordering, range conflicts,
flush barriers, and eventually rate limiting. Its API should hide the
scheduling policy.

## WALManager

Durable write history. Owns WAL sequence numbers. WAL sequence is per export
and survives restart.

## ExportMemoryCache

Live serving overlay reconstructed from WAL. Reads check this first, then
committed tree/blob state.

## StorageEngine

Object-storage abstraction. Starts with local filesystem storage, then grows to
S3 or MinIO.

## ExportCatalog

Durable database/catalog of exports, tree roots, clone lineage, state, size,
block size, and related metadata. Used by `nbdcli` and `nbdserver`.

## LocalExportRegistry

In-process registry of exports currently active on this NBD server. Also owns
periodic gateway/etcd lease renewal for active exports.

## CompactionManager

Converts WAL ranges into immutable tree/leaf objects and updates
`ExportCatalog` with a new root/checkpoint.

# Source Of Truth

Avoid treating memory as the durable source of truth.

Durable truth:

- WAL records
- committed tree/blob state in object storage
- export root/checkpoint metadata in `ExportCatalog`

Serving truth:

- `ExportMemoryCache` overlay
- committed tree/blob state

`ExportMemoryCache` is authoritative for serving reads while the server is
running, but it must be reconstructable from durable WAL.

# Read, Write, And Flush Contract

## Write Path

1. `ExportAdmissionCtl` admits the write for its range.
2. `WALManager` appends the write and assigns a durable WAL sequence.
3. Once the WAL append is durable, apply the write to `ExportMemoryCache`.
4. Send NBD write success.

## Read Path

1. `ExportAdmissionCtl` admits the read for its range.
2. Read from `ExportMemoryCache` overlay.
3. For uncovered bytes, read from committed tree/S3 blobs.
4. Missing data is zero-filled.

## Flush Path

1. `ExportAdmissionCtl` admits flush as a barrier.
2. Flush waits for all writes admitted before the flush to become WAL-durable
   and visible in `ExportMemoryCache`.
3. Flush replies success.

Flush does not need to compact WAL into base blobs. WAL durability is enough.

# Admission Control

Admission should be exposed through a stable API, with implementation policy
hidden.

Conceptual API:

```rust
acquire_read(range) -> AdmissionPermit
acquire_write(range) -> AdmissionPermit
acquire_flush() -> AdmissionPermit
```

Important behavior:

- Reads may run concurrently when compatible.
- Writes require exclusive access to overlapping ranges.
- If a write is waiting for a range, later reads on that same range wait behind
  it.
- Flush is an export-wide barrier, at least in v1.

Admission tickets are volatile scheduling/debug IDs only. WAL sequence numbers
belong to WAL, not admission.

# ExportCatalog And Database Model

Use `ExportCatalog` for durable metadata. Use `LocalExportRegistry` for active
exports on the current machine.

The catalog should track a tree per export.

Initial conceptual model:

```text
exports
  id
  name
  size_bytes
  block_size
  root_node_id
  state
  created_at
  updated_at

nodes
  id
  kind
  level
  span_start
  span_len
  blob_key/null
  created_at

edges or child pointers
  parent/child relationships between tree nodes
```

The exact table shape can evolve, but the core invariant is:

Published nodes are immutable. Updating an export creates new nodes and moves
the export root pointer.

A separate leaf-node table is probably unnecessary unless later optimization
requires it.

# Tree And Blob Layout

Use a sparse tree over logical disk offsets.

Fanout idea:

```text
leaf:       32 MiB data blob
level 1:     1 GiB  = 32 leaves
level 2:    32 GiB  = 32 level-1 nodes
level 3:     1 TiB
level 4:    32 TiB
...
```

Internal nodes:

- sparse child pointers
- metadata only
- immutable once published

Leaf nodes:

- full 32 MiB immutable S3 blob
- dense representation of that logical range

This is acceptable because S3 favors larger immutable objects, and small writes
are absorbed by WAL first.

Missing internal child or missing leaf means falling back to a parent
export/tree, or zero-filling if no parent has data.

# Clone And Fork

Clone should be constant-time.

Conceptually:

```text
clone src -> dst
  dst gets a new export record
  dst root references src current root / parent tree
  no blob copy
```

Future writes to the child go through WAL. Compaction later creates new leaf
blobs only for changed 32 MiB ranges.

# Compaction

Compaction workflow:

1. Pick a WAL sequence range to compact.
2. Group writes by 32 MiB leaf range.
3. Load the current base leaf blob or a zero buffer.
4. Apply WAL writes.
5. Upload a new immutable 32 MiB leaf blob.
6. Create new tree nodes along affected paths.
7. Atomically update the `ExportCatalog` root/checkpoint.
8. Mark the WAL sequence range compacted/reclaimable.

`CompactionManager` can call `ExportCatalog` directly. No separate
`ExportCommitManager` is needed unless repeated commit logic emerges later.

# nbdcli

Need a management CLI for export lifecycle:

```text
nbdcli create <name> --size <bytes>
nbdcli clone <src> <dst>
nbdcli delete <name>
nbdcli list
nbdcli inspect <name>
```

`nbdcli` talks to `ExportCatalog`, not to `LocalExportRegistry` and not to the
NBD protocol.

# LocalExportRegistry

Tracks exports active on this server process.

Responsibilities:

- register export on mount/open
- unregister export on disconnect/close
- periodically update gateway/etcd lease
- release or stop renewing leases on close/shutdown

It is not the durable export database.

# Deferred Long-Term Pieces

Do not build all of these immediately:

- cross-server writer fencing
- etcd writer locks
- gateway lease robustness
- garbage collection
- advanced compaction scheduling
- IOPS limits
- range-lock interval tree optimization
- S3 CAS optimizations
- full crash recovery edge cases

The architecture should leave room for them.

# Recommended First Prototype Slice

Build only enough to validate the core model:

- `NBDServer`
- `Export`
- simple `ExportAdmissionCtl`
- `WALManager`
- `ExportMemoryCache`
- `LocalStorageEngine`
- `ExportCatalog` with local DB/files
- `nbdcli create/list/delete`

Then add, in likely order:

1. restart replay from WAL
2. S3StorageEngine / MinIO
3. clone
4. tree compaction
5. LocalExportRegistry lease behavior
6. garbage collection
7. writer fencing
