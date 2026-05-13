Title: WAL Architecture
Date: 2026-05-12
Status: approved

# Problem

The WAL is the durability boundary for acknowledged writes. It must own durable
ordering, support replay after restart, and provide the basis for flush
semantics without forcing immediate compaction into committed tree blobs.

# Goal

Define a WAL model where:

- WAL sequence numbers are scoped to one WAL domain and durable;
- writes are acknowledged only after their WAL record is durable;
- read-view visibility follows WAL durability;
- flush waits for prior writes to become WAL-durable and read-view-visible;
- replay rebuilds `ExportReadView` after restart;
- compaction can later checkpoint a WAL domain prefix into committed tree
  state;
- the first implementation can use a local WAL domain backed by `export_id`;
- the long-term implementation can replace the local WAL with a WAL service
  behind the same API.

# Ownership

`WalProvider` is the replaceable WAL service facade. It opens an `ExportWal`
handle for one `WalDomain`. `ExportWal` owns the request-facing WAL contract
for that domain: append, bounds, replay, and prefix pruning. It assigns WAL
sequence numbers when using the local WAL implementation, and it can delegate
sequence assignment to a remote WAL service if that service becomes the
durable sequencer later.

`ExportAdmissionCtl` may assign volatile admission tickets for scheduling, but
those tickets are not durable and are not used for replay.

Conceptual API:

```rust
struct WalRecord {
    seq: WalSeq,
    range: ByteRange,
    data: Vec<u8>,
}

struct WalDomain {
    export_id: ExportId,
}

struct OpenWal {
    domain: WalDomain,
}

trait WalProvider {
    async fn open_export(&self, request: OpenWal) -> Result<ExportWalHandle>;
}

trait ExportWal {
    async fn append(&self, request: WalRequest) -> Result<WalRecord>;

    async fn bounds(&self) -> Result<WalBounds>;

    async fn replay_after(&self, after: WalSeq) -> Result<WalReplay>;

    async fn replay_range(
        &self,
        after: WalSeq,
        through: WalSeq,
    ) -> Result<WalReplay>;

    async fn prune_through(&self, seq: WalSeq) -> Result<WalPruneResult>;
}
```

The first provider is `LocalWalProvider`, which returns `LocalExportWal`
handles. The long-term provider can be remote without changing
`WalDurableEngine`.

`WalDomain` is a facade over the WAL namespace. The first local implementation
stores it as `export_id`. A future WAL service can change the domain internals
to `(owner, export_name)` when auth/client identity exists, without changing
`WalDurableEngine`.

# Backend Strategy

## LocalWalProvider

The implemented WAL backend is local and keyed by `export_id`.

Responsibilities:

- persist records to a local per-export WAL directory;
- `fsync` enough state before returning append success;
- recover the highest complete durable sequence on startup;
- reject or truncate a final partial/corrupt record during replay;
- expose replay in sequence order.

Local WAL durability is enough for the first prototype. It is not the final
cross-machine durability model.

The local backend stores segment files under the configured WAL root, uses a
128 MiB target segment size, writes framed records, syncs the segment before
returning append success, scans complete records on open, and exposes explicit
`prune_through` cleanup for checkpointed prefixes.

## RemoteWalProvider

The long-term target is a WAL service.

Responsibilities:

- provide durable domain sequencing;
- persist records to infrastructure whose durability is independent of the NBD
  server process;
- expose replay from a sequence boundary;
- provide durability acknowledgements that preserve the same `ExportWal`
  contract.

The WAL service should replace `WalProvider` / `ExportWal`, not the NBD
read/write/flush contract.

## Domain Scope

The WAL is scoped per `WalDomain`. This keeps replay, compaction checkpoints,
local file lifetimes, and delete/GC rules easier to reason about.

The v1 domain is `export_id`, which is already stable catalog identity. The
facade keeps the service boundary ready for a later `(owner, export_name)` key.

Cross-export ordering is not part of the WAL contract.

# Write Interaction

```text
Export.write(range, data)
  -> acquire write admission permit
  -> ExportWal.append(WalRequest)
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
the barrier.

# Replay

Startup recovery:

```text
load export metadata and checkpoint
  -> initialize committed root snapshot
  -> replay durable WAL records with seq > base_wal_seq
  -> apply records to ExportReadView in sequence order
  -> set next WAL sequence after highest durable record
```

Replay must tolerate a final partial or corrupt record by rejecting it and
keeping only verified durable records.

With `LocalExportWal`, startup scans the local per-export WAL. With a remote
WAL service, startup asks the service for records after `base_wal_seq`.
Both paths produce the same ordered `WalReplay`.

# Compaction Checkpoints

Compaction consumes a WAL domain sequence prefix and publishes committed tree
state.

After catalog publication succeeds:

```text
export_heads.base_wal_seq = wal_seq
```

The published root must represent every WAL record with sequence
`<= export_heads.base_wal_seq`. WAL records at or below the checkpoint
remain needed until active read views have installed the checkpoint and GC
decides they are unreachable.

# WAL Lifecycle And Pruning

WAL records move through these states:

```text
appended
  -> durable
  -> applied to ExportReadView overlay
  -> represented by a published tree checkpoint
  -> demoted from authoritative overlay state
  -> eligible for physical pruning
  -> deleted
```

Durability and visibility are write-path requirements. Pruning is not.
Deletion is asynchronous cleanup and must not be required for write, flush, or
read correctness.

The authoritative read truth is:

```text
published tree at checkpoint C
  + durable WAL overlay entries where seq > C
```

After compaction publishes checkpoint `C`, WAL records `<= C` are no longer
needed by a fresh read view. The current single-process engine prunes local WAL
only after its own `ExportReadView` installs the checkpoint. Future external
cleanup or multi-process serving must add a retention policy or leases before
deleting WAL that another stale-but-valid read view may still need.

One future pruning policy can be time based:

```text
WAL segment is prune-eligible when:
  segment.max_seq <= published_base_wal_seq
  AND segment.closed_at <= now - wal_retention_window
```

Serving processes must refresh export head/read-view state more frequently
than `wal_retention_window`. A process that cannot refresh in time must fail
closed for the export and force a reopen. This keeps external cleanup simple:
it can delete old checkpointed WAL asynchronously without coordinating with
every connection.

Future multi-host or long-stalled serving may add explicit retention leases:

```text
wal_retention_leases
  owner
  export_id
  min_required_wal_seq
  expires_at
```

Until leases exist, time-based retention is an operational contract. It relies
on bounded serving staleness and reasonably correct cleanup clocks.

# ReadView Interaction

`ExportReadView` is the single in-process owner of materialized WAL overlay
state for an active export. `ExportWal` appends and replays durable records,
but request-path reads consult the read view.

Write flow:

```text
append WAL record seq S
  -> fsync according to policy
  -> insert S into ExportReadView.wal_overlay
  -> reply success
```

Checkpoint refresh:

```text
catalog publishes checkpoint C
  -> ExportReadView installs root/checkpoint C
  -> overlay entries with seq > C remain authoritative
  -> overlay entries with seq <= C are demoted or dropped after old read
     epochs drain
```

Demoted entries may become optional cache entries only if they do not rely on
WAL storage that can be pruned. Otherwise they must be removed before their WAL
segment is deleted.

# Invariants

- `ExportWal` assigns durable sequence numbers for one `WalDomain`.
- If a remote WAL service owns sequencing later, `ExportWal` preserves the
  domain sequencing contract while delegating assignment to the service.
- WAL sequence is scoped per `WalDomain`.
- A write response implies the corresponding WAL record is durable.
- Read-view apply happens only after WAL append succeeds.
- Replay applies records in WAL sequence order.
- Checkpoints advance monotonically as a WAL domain prefix.
- WAL truncation or deletion is a GC decision, not a write-path side effect.
- `WalDurableEngine` depends on `ExportWal`, not on a specific local or remote
  WAL backend.
- Pruning requires a published checkpoint that represents the WAL records being
  pruned.
- Time-based pruning requires serving read views to refresh or close before
  they become older than the retention window.
- WAL-backed cache entries must not outlive the WAL segment they reference.

# Open Questions

- Remote WAL service API and object/storage layout.
- Whether flush should write an explicit flush marker for debugging or recovery
  evidence.
- How to represent failed or abandoned partial append attempts.
- Default retention window and refresh interval.
- Whether leases are needed before multi-host serving or can remain a later
  strengthening.
