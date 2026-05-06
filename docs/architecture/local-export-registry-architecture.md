Title: Local Export Registry Architecture
Date: 2026-05-01
Status: draft

# Problem

The durable `ExportCatalog` records what exports exist, but the server also
needs process-local truth about which exports are currently open on this
machine. That local truth is needed for request routing, clean close, lease
renewal, and a future writer-fencing design.

# Goal

Define `LocalExportRegistry` as the in-process registry for active exports:

- register exports on successful open;
- unregister exports after close;
- maintain serving lease state for active exports;
- expose local active export state for request routing and debugging;
- support serving lease renewal through etcd;
- leave room for future single-writer/fencing integration.

# Scope

`LocalExportRegistry` is not durable metadata. It does not replace
`ExportCatalog`.

It may be empty after process restart. Durable recovery comes from
`ExportCatalog` plus WAL.

Writer fencing is out of scope for this document. The architecture targets
etcd leases as the active-export signal and relies on lease loss halting the
export once leases exist. The current prototype uses only process-local active
export state and one active writable owner per export. A future fencing design
can add stronger durable mutation checks behind the lease/catalog/WAL
boundaries.

# Data Structures

Use structured records instead of passing many loose fields.

```rust
struct ActiveExportRecord {
    export_id: ExportId,
    name: ExportName,
    layout_kind: ExportLayoutKind,
    root_node_id: Option<NodeId>,
    size_bytes: u64,
    base_wal_seq: WalSeq,
    connection_id: ConnectionId,
    lease: ExportLeaseSnapshot,
    opened_at: Timestamp,
    state: ActiveExportState,
}

struct ExportLeaseSnapshot {
    lease_id: LeaseId,
    holder: ExportLeaseHolder,
    purpose: ExportLeasePurpose,
    expires_at: Timestamp,
    last_refreshed_at: Timestamp,
}

enum ExportLeaseHolder {
    Server(ServerId),
    Management(ManagementClientId),
}

enum ExportLeasePurpose {
    Serve,
    Delete,
}

enum ActiveExportState {
    Opening,
    Active,
    Closing,
    LeaseLost,
}

struct RegisterExport {
    export: ExportRecord,
    connection_id: ConnectionId,
    lease: ExportLeaseSnapshot,
}

struct UnregisterExport {
    export_id: ExportId,
    connection_id: ConnectionId,
    reason: CloseReason,
}

struct AcquireExportLease {
    name: ExportName,
    export_id: Option<ExportId>,
    holder: ExportLeaseHolder,
    purpose: ExportLeasePurpose,
    ttl: Duration,
}

const ACTIVE_EXPORT_LEASE_TTL: Duration = Duration::from_secs(60);
const ACTIVE_EXPORT_LEASE_REFRESH_INTERVAL: Duration = Duration::from_secs(30);
```

# API Shape

Conceptual API:

```rust
impl LocalExportRegistry {
    async fn register(&self, request: RegisterExport)
        -> Result<ActiveExportHandle>;

    async fn unregister(&self, request: UnregisterExport)
        -> Result<()>;

    async fn get_local_active(&self, name: ExportName)
        -> Result<Option<ActiveExportRecord>>;

    async fn list_local_active(&self) -> Result<Vec<ActiveExportRecord>>;

    async fn current_local_lease(&self, name: ExportName)
        -> Result<Option<ExportLeaseSnapshot>>;
}
```

`ActiveExportHandle` should be RAII-style so close/shutdown paths are less
likely to leak local active state.

# Lease Model

Etcd leases are the per-export lifecycle exclusion truth that other processes
use. `nbdcli delete` should acquire the lease through the lifecycle model
rather than checking a Unix domain socket or process-local registry API.

This is the target lifecycle model, not a statement that the first local
prototype already has distributed exclusion. Until `ExportLeaseStore` is
implemented, `LocalExportRegistry` is a single-process routing and close
boundary only.

The lease store is a boundary separate from the in-process registry:

```rust
trait ExportLeaseStore {
    async fn acquire(&self, request: AcquireExportLease)
        -> Result<ExportLeaseSnapshot>;

    async fn renew(&self, lease_id: LeaseId)
        -> Result<ExportLeaseSnapshot>;

    async fn release(&self, lease_id: LeaseId) -> Result<()>;

    async fn lookup(&self, name: ExportName)
        -> Result<Option<ExportLeaseSnapshot>>;
}
```

`LocalExportRegistry` uses this store to renew serving leases for local
exports. `nbdcli delete` uses the same lease model through
`ExportLifecycleManager`, not by calling into a running NBD server process.

Open and delete coordination belongs to `ExportLifecycleManager`. The registry
is responsible only after the server has acquired a serving lease and needs to
track/renew it locally.

The serving lease should include a timestamp/deadline:

```text
expires_at
last_refreshed_at
```

`LocalExportRegistry` renews the lease while the export is active and updates
the in-memory lease timestamp after each successful renewal. The active
`Export` must also hold an observable lease snapshot that is updated after each
successful renewal. If `now > expires_at`, the server has lost the lease and
the export must halt rather than continue serving or writing.

When the lease layer is implemented, the initial policy should use a one
minute serving lease and renew it every 30 seconds. This gives the renewal
worker one missed refresh before the lease expires.

Recovery after lease loss is out of scope for this architecture pass.

# Open Flow

```text
NBD_OPT_GO(export_name)
  -> ExportLifecycleManager.begin_open(export_name)
       -> acquire serving lease
       -> load and validate exports-only descriptor
  -> LocalExportRegistry.register(..., lease, state = Opening)
  -> initialize Export components from latest head/tree snapshot
  -> replay WAL into ExportReadView
  -> transition local record to Active
  -> enter transmission phase
```

Registering the local record before replay lets the renewal worker refresh the
lease while potentially slow recovery work is running. If initialization or WAL
replay fails after `begin_open` succeeds, the open path must unregister the
local record and release the lease before returning failure.

The descriptor loaded during `begin_open` is stable export identity and
configuration from `exports`; it is not the current serving head. Engines that
depend on durable state load the latest head/tree snapshot while constructing
their serving state. This keeps background compaction or future resize work
from making a previously loaded head stale during open.

The long-term system may support multiple connections for the same active
serving domain only when they belong to the same authenticated client/host.
That requires auth to differentiate hosts and is out of scope for the first
implementation.

The long-term serving domain key is `(owner, export)`: owner namespace first,
export name inside that namespace. Filesystem and backing-store layout may use
the same ordering. Until `owner` is backed by real client identity, the first
implementation should allow only one active writable connection per export.
Runtime and admission boundaries should still be written so multiple
same-owner connections can share one `(owner, export)` ordering domain once
the registry has a real client identity to compare.

# Close Flow

```text
connection close or NBD_CMD_DISC
  -> stop accepting new requests on the connection
  -> finish/cancel outstanding work according to shutdown policy
  -> enqueue close-time compaction on clean close
  -> unregister export from LocalExportRegistry
  -> release or stop renewing the serving lease
```

Close-time compaction is intended but not required for correctness. The close
path submits a background compaction job after acknowledged writes remain
durable in WAL, then lets the export close without waiting for that job. If the
job fails, the next open replays retained WAL after the last catalog
checkpoint.

Background compaction must reread catalog state when it runs. If delete races
after close and marks the export deleted, compaction publication should observe
that durable state and no-op or fail without advancing the head. Delete does
not need to wait for best-effort compaction.

# Delete Interaction

`nbdcli delete` must acquire the same per-export lease used by open before it
marks the export deleted.

```text
nbdcli delete name
  -> ExportLifecycleManager.delete_export(name)
       -> acquire delete lease
       -> if lease acquisition fails: return ExportBusy
       -> ExportCatalog.delete_export(...)
       -> release delete lease
```

The process-local registry is useful for the server's own lifecycle, but it is
not the cross-process active check for CLI operations.

# Active Lease Renewal

`LocalExportRegistry` owns periodic etcd lease renewal for active exports.

A dedicated renewal worker thread or task should scan the local active export
registry every 30 seconds and refresh each serving lease.

The lease layer should derive from active registry state:

```text
ActiveExportRecord -> lease renewal task
unregister/close -> stop renewing lease
```

Lease state is operational routing truth, not durable export recorddata.
After each successful renewal, the lease snapshot in both
`LocalExportRegistry` and the active `Export` must be updated.

# Lease Loss Is Fatal

An active `Export` must treat lease loss as an unrecoverable serving condition:

```text
if now > lease.expires_at:
  transition to LeaseLost
  stop trusting this process as the writer
```

This design intentionally does not define a graceful recovery path or detailed
in-flight request behavior after lease loss. The only required guarantee is
that the export must not continue serving writes after its lease has expired.

# Invariants

- `LocalExportRegistry` is process-local active export lifecycle truth.
- `ExportCatalog` remains durable export recorddata truth.
- Etcd per-export leases are cross-process lifecycle exclusion truth.
- A local record may be registered as `Opening` after lease acquisition and
  before export initialization.
- `Opening` transitions to `Active` only after initialization succeeds.
- Failed initialization unregisters the local record and releases the lease.
- Unregister happens on close/shutdown.
- Closing exports remain registered until close cleanup finishes.
- `nbdcli delete` fails if it cannot acquire the per-export lease.
- `nbdcli delete` uses `ExportLifecycleManager`, not a local NBD server socket.
- First implementation allows at most one active writable connection per
  export.
- Multiple connections for the same export require same-client authentication
  and are out of scope for the first implementation.
- Lease renewal is derived from local active records.
- Serving leases use a 60 second TTL.
- The renewal worker refreshes serving leases every 30 seconds.
- Successful lease renewal updates the active export's observed lease
  timestamp/deadline.
- Export lease loss halts the active export.
- Local registry state is not used for crash recovery.

# Design-Phase Details

These are intentionally not architecture blockers for this component boundary:

- exact etcd lease key layout;
- exact close/shutdown timeout policy.

Future distributed writer fencing should get its own architecture/design doc
when that work becomes active.
