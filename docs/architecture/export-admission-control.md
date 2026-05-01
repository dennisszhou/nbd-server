Title: Export Admission Control
Date: 2026-05-01
Status: draft

# Problem

NBD clients can issue concurrent or pipelined requests. The export data path
needs one component that decides when reads, writes, and flushes may execute so
the rest of the system can rely on stable ordering and visibility semantics.

Admission control should provide a clear correctness contract without forcing
the first implementation to use the final range-lock data structure.

# Goal

Define `ExportAdmissionCtl` as the per-export request admission boundary for:

- logical byte-range read/write conflicts;
- read-after-write visibility;
- write/write exclusion;
- flush as an export-wide barrier;
- future replacement of the scheduling policy behind the same API.

# Scope

`ExportAdmissionCtl` protects logical byte ranges, not tree nodes, blob keys, or
WAL segments. The mapping from byte range to storage layout is below this
layer.

The first implementation may serialize more than necessary. The API should
still express the final semantics: reader/writer range permits over logical
byte ranges plus a global flush barrier.

# API Shape

Conceptual API:

```rust
enum AdmissionOp {
    Read(ByteRange),
    Write(ByteRange),
    Flush,
}

struct AdmissionPermit {
    op: AdmissionOp,
    ticket: u64,
}

impl ExportAdmissionCtl {
    async fn acquire(&self, op: AdmissionOp) -> Result<AdmissionPermit>;
}
```

The permit is RAII-style. While held, the operation is allowed to observe or
mutate the protected range according to its mode.

The `ticket` is a volatile diagnostic/scheduling value only. It is not a WAL
sequence number and is not used for replay or compaction.

# Permit Semantics

## Read Permit

A read permit allows the holder to observe the requested logical byte range.
Multiple reads may hold overlapping permits concurrently unless a conflicting
write or flush barrier is active or earlier in the admission order.

## Write Permit

A write permit allows the holder to mutate the requested logical byte range.
Writes conflict with overlapping reads and writes.

The write response is still controlled by the export write path:

```text
acquire write permit
  -> append WAL record durably
  -> apply record to ExportReadView
  -> reply success
```

Admission gives permission to run. It does not make the write durable.

## Flush Permit

Flush is a global export barrier.

The first implementation should make flush conflict with all active and earlier
queued operations. This is conservative and easy to reason about.

Once admitted, `Export.flush()` waits for all writes covered by the protocol
contract to be WAL-durable and read-view-visible before replying.

# Read-After-Write Correctness

This is a correctness rule, not only a fairness rule:

> A read admitted after an earlier overlapping write has completed must observe
> that write unless a later write overwrote the same bytes.

Admission supports this by ensuring a later overlapping read cannot pass an
earlier overlapping write that has not finished the durable write/read-view
apply path.

Example:

```text
R1 block A active
W2 block A waiting
R3 block A arrives
```

`R3` must not pass `W2`. This preserves the ordering the export presents to
overlapping operations. It also prevents a continuous stream of reads from
starving the write.

# Conflict Rules

Two operations conflict when:

```text
their logical byte ranges overlap
and at least one operation is a write
```

Flush conflicts with the whole export.

Non-overlapping reads and writes may run concurrently in the long-term policy,
but the first implementation may choose a coarser policy as long as it
preserves these semantics.

# Policy Evolution

The stable API is more important than the first scheduling data structure.

Possible policies behind the same API:

- global export mutex;
- global reader/writer lock;
- fair byte-range lock with a FIFO wait queue;
- interval tree plus fair wait queue;
- sharded range locks;
- admission policy with IOPS or bandwidth limits.

Changing policy must not change the observable read/write/flush contract.

# Workqueue Boundary

Admission is not the same as generic workqueue execution.

Generic workqueues move jobs off hot paths and bound concurrency.
`ExportAdmissionCtl` grants semantic permission to observe or mutate export
byte ranges.

Request workers may block waiting for admission. The NBD socket read path
should not.

# Invariants

- Admission protects logical byte ranges.
- A permit authorizes observation or mutation only while it is held.
- Overlapping writes are exclusive.
- Reads do not run concurrently with overlapping active writes.
- Later overlapping reads do not pass earlier overlapping writes.
- Flush is an export-wide barrier in the first implementation.
- Admission tickets are volatile and are not WAL sequence numbers.
- Admission does not perform WAL append, read-view apply, or storage I/O.
- Policy can become more concurrent without changing the `Export` API.

# Open Questions

- Whether the first implementation should use a global mutex or global
  reader/writer lock behind the range-oriented API.
- Whether flush must conflict with reads forever or only with writes after the
  first implementation.
- How admission should expose queue depth, wait time, and per-export
  backpressure metrics.
- Whether future IOPS limits belong directly in `ExportAdmissionCtl` or in a
  wrapper policy around it.
