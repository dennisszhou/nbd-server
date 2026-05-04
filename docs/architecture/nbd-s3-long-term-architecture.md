Title: NBD S3 Long-Term Architecture
Date: 2026-05-01
Status: draft

# Purpose

This document is the umbrella architecture for the NBD server backed by
S3-compatible storage. It defines the system shape, source-of-truth boundaries,
component responsibilities, and the focused architecture documents that should
be discussed separately before roadmap planning.

Detailed design is intentionally split into focused documents so the tree
metadata model, WAL durability model, workqueue infrastructure, and read-view
checkpoint behavior can evolve independently behind stable APIs.

# Objective

Build an NBD server that:

- supports fixed newstyle NBD negotiation, `NBD_OPT_GO`, `NBD_OPT_ABORT`,
  `read`, `write`, and `flush`;
- supports arbitrary named exports persisted independently;
- durably stores data in S3-compatible object storage;
- acknowledges writes only after they are durable in the WAL and visible to
  later reads;
- implements flush as a barrier over writes ordered before the flush becoming
  WAL-durable and read-view-visible;
- can grow into clone/fork, compaction, garbage collection, serving lease
  renewal, and cross-server writer fencing.

Cross-server writer fencing is a future hardening feature, not part of the
current architecture scope.

# Architecture Doc Set

This umbrella doc should stay relatively small. Use the focused documents for
deep discussion:

- `docs/architecture/workqueue-architecture.md`
  - request boundaries, worker queues, backpressure, cancellation, and shutdown
- `docs/architecture/nbd-protocol-architecture.md`
  - fixed newstyle handshake, `NBD_OPT_GO`, transmission requests, replies, and
    protocol-visible flush semantics
- `docs/architecture/export-admission-control.md`
  - logical byte-range permits, read/write conflict rules, and flush barriers
- `docs/architecture/wal-architecture.md`
  - WAL ownership, sequence numbers, durability, replay, and flush interaction
- `docs/architecture/export-tree-metadata.md`
  - export catalog, sparse tree metadata, 32 MiB leaves, copy-on-write clone,
    and root publication
- `docs/architecture/export-read-view-architecture.md`
  - `ExportReadView`, read-through behavior, backing readers, and
    compaction/checkpoint invalidation
- `docs/architecture/storage-engine-architecture.md`
  - blobstore API, immutable blobs, local/S3 backend contract, and corruption
    boundaries
- `docs/architecture/export-catalog-architecture.md`
  - export lifecycle, catalog data structures, clone/delete behavior, and
    checkpoint publication
- `docs/architecture/export-lifecycle-architecture.md`
  - open/delete orchestration across catalog metadata and per-export leases
- `docs/architecture/compaction-manager-architecture.md`
  - WAL prefix compaction, copy-on-write tree construction, close-time
    compaction, and read-view notification
- `docs/architecture/local-export-registry-architecture.md`
  - active local exports, etcd lease renewal, delete interaction, and close
    lifecycle

Later docs should be added for GC, writer fencing, and detailed active export
lease protocols when those topics become active. Writer fencing should be
handled as its own future design rather than folded into the current lease
model.

# System Planes

The mature system has three planes:

```text
data plane:
  NBDServer -> Export -> admission/WAL/cache/tree/storage

management plane:
  nbdcli -> ExportLifecycleManager -> ExportCatalog / ExportLeaseStore

local control plane:
  LocalExportRegistry -> active export etcd lease renewal
```

The data plane serves block-device operations. The management plane owns durable
export lifecycle operations. The local control plane advertises which exports
this server is currently serving.

# Source Of Truth

The architecture separates durable truth from serving truth.

Durable truth:

- durable WAL records that have not been compacted;
- committed tree/blob state in object storage;
- export root, checkpoint, and lifecycle metadata in `ExportCatalog`.

Serving truth:

- `ExportReadView` WAL overlay reconstructed from WAL;
- committed tree/blob state resolved through a backing reader.

`ExportReadView` is authoritative for serving acknowledged recent writes while
the server is running, but it is not durable by itself. Every required WAL
overlay entry that affects acknowledged write visibility must be
reconstructable from durable WAL records or committed tree state.

# Component Responsibilities

## NBDServer

Owns protocol handling only: listener setup, handshake, option negotiation,
request decoding, request enqueueing, per-connection reply writing, and global
shutdown. It owns a process-local connection registry or task set for accepted
connections so shutdown can signal, drain or cancel, and join active connection
tasks before reporting completion. It must not know about S3 keys, WAL layout,
tree nodes, or compaction.

## NBDConnection

Owns one client connection. In the long-term runtime split, inbound protocol
handling and outbound reply serialization are separate per-connection
responsibilities. Its read path decodes NBD requests and enqueues work. It does
not perform WAL append, storage reads, compaction, or catalog transactions
inline.

Replies are serialized per connection. A slow connection must not block reply
writes for other connections.

## Open Path

`LocalExportRegistry.open` is the connection-facing open boundary. It bridges
NBD negotiation to the data path by coordinating lifecycle checks, active local
state, runtime construction, engine construction, and future WAL/read-view
recovery. This is not a separate component in the plan of record.

## Export

Public data-path API for one opened export.

Conceptual API:

```rust
impl Export {
    async fn read(&self, range: ByteRange) -> Result<Bytes>;
    async fn write(&self, range: ByteRange, data: Bytes) -> Result<()>;
    async fn flush(&self) -> Result<()>;
    async fn close(&self) -> Result<()>;
}
```

The broad architecture's `Export` means the active serving boundary for one
opened export. In code, this can be split into `ExportRuntime` plus
`ExportEngine`: the runtime owns request queueing, admission, and execution
policy, while the engine owns data behavior. Engine execution may be guarded
by an admitted request capability so storage access cannot bypass the
per-export admission boundary. The boundary does not own WAL format, object
I/O, or catalog schema.

## ExportAdmissionCtl

Owns correctness ordering for reads, writes, and flushes. It exposes a stable
permit API over logical byte ranges while hiding the scheduling
implementation. A permit allows an operation to observe or mutate its protected
range. The first policy can be conservative; later policies can add fair
range-aware scheduling.

## WALManager

Owns per-export durable write history. It assigns WAL sequence numbers,
persists write records before acknowledgement, supports replay, and exposes
checkpoint state for compaction and GC. The first backend can be a local
per-export WAL. The long-term backend can be a WAL service behind the same
`WALManager` contract.

## ExportReadView

Owns the authoritative in-process serving view for acknowledged writes and
optional read-through cache state. It retains required WAL overlay entries
newer than the committed catalog checkpoint, exposes `read`, and may fill
misses through a committed backing reader. Overlay retirement is driven by
global WAL prefix checkpoint/root advancement events.

## CommittedTreeReader

Resolves reads from committed catalog/tree/blob state. It walks sparse internal
nodes, reads immutable 32 MiB leaf blobs, and zero-fills holes. Clone/fork is
represented by shared immutable tree nodes, not parent-root fallback during
reads.

## StorageEngine

Owns blob I/O only: create, read, ranged read, and delete. It never overwrites
keys. It does not own export lifecycle, WAL sequencing, compaction policy, tree
semantics, or metadata interpretation.

The storage runtime owns backend resource pooling and concurrency limits. S3
backends should reuse client/config objects rather than create per-request
clients.

## ExportCatalog

Owns durable export metadata: create, clone, inspect, list, delete, stable
export identity/lifecycle rows, append-only committed-root generations,
immutable tree metadata, and checkpoint publication.

## ExportLifecycleManager

Owns control-plane orchestration for operations that need both catalog metadata
and per-export leases. Open and delete contend on the same per-export lease:
open holds it while serving, and `nbdcli delete` holds it while marking the
catalog deleted. It does not store metadata itself.

## LocalExportRegistry

Owns active exports on this server process and serving-lease renewal. It is not
the durable export database. Per-export leases are the cross-process lifecycle
exclusion truth used by open/delete orchestration and future routing/fencing
behavior. If an active export observes that its lease expired, it must halt;
recovery from lease loss is out of scope.

## CompactionManager

Turns WAL records into committed tree state, publishes new roots through
`ExportCatalog`, and notifies active exports that checkpoints advanced.
Close-time compaction is an intended feature to reduce future WAL replay, but
close remains correct if compaction fails and acknowledged writes remain durable
in WAL.

## nbdcli

Operator-facing management CLI for export lifecycle. For delete, it goes
through `ExportLifecycleManager` so it acquires the per-export lease before
marking catalog state deleted. It does not talk to NBD and does not call into
the process-local `LocalExportRegistry`.

# Core Operation Contracts

## Write

```text
decode NBD write
  -> enqueue request job
  -> Export.write(range, data)
  -> acquire write admission permit
  -> WALManager.append_write(range, data)
  -> apply durable WAL record to ExportReadView
  -> reply success
```

A write response is sent only after the WAL record is durable and the write is
visible to later reads.

## Read

```text
decode NBD read
  -> enqueue request job
  -> Export.read(range)
  -> acquire read admission permit
  -> ExportReadView.read(range)
  -> fill misses through committed tree/storage as needed
  -> reply with bytes
```

Reads after a successful write on the same export must observe that write
unless a later write overwrote the same bytes.

## Flush

```text
decode NBD flush
  -> enqueue request job
  -> Export.flush()
  -> acquire flush admission permit
  -> wait for prior writes to be WAL-durable and read-view-visible
  -> reply success
```

Flush does not need to compact WAL records into committed leaf blobs.

# Global Invariants

- A write response is sent only after its WAL record is durable.
- A write response is sent only after the write is visible in
  `ExportReadView`.
- A flush response is sent only after all writes ordered before the flush by
  admission are WAL-durable and read-view-visible.
- WAL sequence numbers are assigned by `WALManager`, not admission.
- Admission tickets are volatile and never used for recovery.
- `StorageEngine` does not own export semantics.
- `ExportCatalog` is the durable export metadata source.
- Open and delete contend on the same per-export lease through
  `ExportLifecycleManager`.
- `LocalExportRegistry` is only the active local export registry.
- Per-export leases are the cross-process lifecycle exclusion truth.
- Lease loss halts the active export.
- Published tree nodes and leaf blobs are immutable.
- Tree child pointers are immutable once published.
- Publishing an export root/checkpoint appends a new generation through
  `ExportCatalog`.
- `checkpoint_wal_seq` is a global WAL prefix: the committed root represents
  every WAL record with `seq <= checkpoint_wal_seq`.
- Read-view overlay retirement after compaction is driven by checkpoint/root
  advancement.
- Untagged logical read caches are forbidden.
- Workqueue admission, completion, cancellation, and shutdown are explicit.

# First Prototype Boundary

The first implementation should prove the central data-path invariant without
requiring the full long-term storage system.

Include:

- NBD protocol subset;
- `Export` API;
- conservative `ExportAdmissionCtl`;
- `WALManager` with durable local WAL;
- `ExportReadView` read serving;
- local `StorageEngine`;
- minimal `ExportCatalog`;
- minimal `ExportLifecycleManager`;
- minimal serving lease renewal;
- basic `nbdcli` create/list/delete/inspect.

Defer:

- sparse tree compaction;
- clone/fork;
- S3/MinIO backend;
- garbage collection;
- writer fencing;
- advanced lease protocol hardening;
- authenticated same-client multi-connection support;
- optimized range-lock scheduling.

# Open Questions

- Exact first WAL segment/object format.
- Exact future catalog schema for tree nodes, edges, and GC metadata.
- Whether first read-through view should store only WAL overlay state or also
  immutable blob objects.
- Exact queue shutdown behavior for in-flight writes after connection close.
- Exact close-time compaction timeout and fallback policy.

# Architecture Exit Criteria

This architecture is ready to drive roadmap planning when:

- the split between umbrella and focused docs is accepted;
- component responsibilities are accepted;
- workqueue boundaries are accepted as part of the architecture;
- read-view read-through and checkpoint invalidation responsibilities are
  accepted;
- the durable/serving source-of-truth distinction is accepted; and
- the first prototype boundary can be derived without changing the long-term
  component model.
