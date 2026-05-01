Title: WAL Architecture
Date: 2026-05-01
Status: draft

# Problem

The WAL is the durability boundary for acknowledged writes. It must own durable
ordering, support replay after restart, and provide the basis for flush
semantics without forcing immediate compaction into committed tree blobs.

# Goal

Define a WAL model where:

- WAL sequence numbers are per export and durable;
- writes are acknowledged only after their WAL record is durable;
- read-view visibility follows WAL durability;
- flush waits for prior writes to become WAL-durable and read-view-visible;
- replay rebuilds `ExportReadView` after restart;
- compaction can later checkpoint a global WAL prefix into committed tree state;
- the first implementation can use a local per-export WAL;
- the long-term implementation can replace the local WAL with a WAL service
  behind the same API.

# Ownership

`WALManager` owns the export-facing WAL contract. It assigns WAL sequence
numbers when using the local WAL implementation, and it delegates sequence
assignment to a remote WAL service if that service becomes the durable
sequencer later.

`ExportAdmissionCtl` may assign volatile admission tickets for scheduling, but
those tickets are not durable and are not used for replay.

Conceptual API:

```rust
struct WalRecord {
    seq: WalSeq,
    export_id: ExportId,
    range: ByteRange,
    data_ref: WalDataRef,
    checksum: Checksum,
}

impl WALManager {
    async fn append_write(&self, range: ByteRange, data: Bytes)
        -> Result<WalRecord>;

    async fn wait_durable_through(&self, seq: WalSeq) -> Result<()>;

    async fn replay_from(&self, checkpoint: Option<WalSeq>)
        -> Result<WalReplayStream>;
}
```

`WALManager` should be backed by a replaceable lower-level WAL backend:

```rust
trait WalStore {
    async fn append(&self, request: WalAppend) -> Result<DurableWalRecord>;

    async fn scan_from(&self, after: Option<WalSeq>)
        -> Result<WalReplayStream>;

    async fn durable_high_watermark(&self) -> Result<Option<WalSeq>>;
}
```

The first backend is `LocalWalStore`. The long-term backend can be
`RemoteWalServiceStore` without changing `Export`.

# Backend Strategy

## LocalWalStore

The initial WAL backend is local and per export.

Responsibilities:

- persist records to a local per-export WAL directory;
- `fsync` enough state before returning append success;
- recover the highest complete durable sequence on startup;
- reject or truncate a final partial/corrupt record during replay;
- expose replay in sequence order.

Local WAL durability is enough for the first prototype. It is not the final
cross-machine durability model.

## RemoteWalServiceStore

The long-term target is a WAL service.

Responsibilities:

- provide durable per-export sequencing;
- persist records to infrastructure whose durability is independent of the NBD
  server process;
- expose replay from a sequence boundary;
- provide durability acknowledgements that preserve the same `WALManager`
  contract.

The WAL service should replace the backend implementation, not the `Export`
read/write/flush contract.

## Per-Export Scope

The WAL is scoped per export. This keeps replay, compaction checkpoints, local
file lifetimes, and delete/GC rules easier to reason about.

Cross-export ordering is not part of the WAL contract.

# Write Interaction

```text
Export.write(range, data)
  -> acquire write admission permit
  -> WALManager.append_write(range, data)
  -> apply durable WalRecord to ExportReadView
  -> reply success
```

A write response is sent only after both durability and read-view visibility are
true.

# Flush Interaction

Flush does not need to compact WAL records.

```text
Export.flush()
  -> acquire flush admission permit
  -> wait for writes ordered before the flush by admission to finish WAL
     append and read-view apply
  -> reply success
```

For a conservative first implementation, the flush admission permit can act as
the barrier. Later, `wait_durable_through(seq)` can support more concurrent
write scheduling.

# Replay

Startup recovery:

```text
load export metadata and checkpoint
  -> initialize committed root snapshot
  -> replay durable WAL records with seq > checkpoint_wal_seq
  -> apply records to ExportReadView in sequence order
  -> set next WAL sequence after highest durable record
```

Replay must tolerate a final partial or corrupt record by rejecting it and
keeping only verified durable records.

With `LocalWalStore`, startup scans the local export WAL. With a remote WAL
service, startup asks the service for records after `checkpoint_wal_seq`. Both
paths produce the same ordered `WalReplayStream`.

# Compaction Checkpoints

Compaction consumes a global WAL sequence prefix and publishes committed tree
state.

After catalog publication succeeds:

```text
checkpoint.compacted_through = wal_seq
```

The published root must represent every WAL record with sequence
`<= checkpoint.compacted_through`. WAL records at or below the checkpoint
remain needed until active read views have installed the checkpoint and GC
decides they are unreachable.

# Invariants

- `WALManager` assigns durable sequence numbers.
- If a remote WAL service owns sequencing later, `WALManager` preserves the
  export-facing sequencing contract while delegating assignment to the service.
- WAL sequence is scoped per export.
- A write response implies the corresponding WAL record is durable.
- Read-view apply happens only after WAL append succeeds.
- Replay applies records in WAL sequence order.
- Checkpoints advance monotonically as a global WAL prefix.
- WAL truncation or deletion is a GC decision, not a write-path side effect.
- `Export` depends on `WALManager`, not on a specific local or remote WAL
  backend.

# Open Questions

- Local WAL record framing and segment size.
- Whether local WAL payloads are always inline for the first implementation.
- Remote WAL service API and object/storage layout.
- Checksum scheme and record framing.
- Whether flush should write an explicit flush marker for debugging or recovery
  evidence.
- How to represent failed or abandoned partial append attempts.
