Title: Export Runtime And Local Registry
Date: 2026-05-02
Status: approved

# Problem

The current in-memory NBD server proves the first protocol path, but the socket
connection still opens `MemoryExport` directly and executes export work inline.
That shortcut makes it unclear which component owns export request execution,
where `ExportAdmissionCtl` will live, how multiple connections will share one
active export, and where a future durable backend will be selected.

The next design should introduce the long-lived runtime boundary without
jumping all the way to the final WAL, read-view, storage, and split-socket
implementation.

# Goal

Introduce a small v0 runtime model that:

- moves backend construction out of `connection.rs`;
- keeps `NbdConnection` responsible for protocol and socket I/O only;
- gives every active export one shared `ExportRuntime`;
- makes `ExportRuntime` the owner of export work execution;
- leaves `ExportAdmissionCtl` inside `ExportRuntime`, not inside the engine;
- makes `MemoryExportEngine` the shared in-memory data implementation;
- keeps `MemoryExport` as a compatibility adapter for the current
  `ExportHandle` path until connection adoption switches to `ExportRuntime`;
- enforces one active mounter per export for now;
- adds config hooks for choosing export runtime and export engine.

# Constraints

- Runtime code remains Rust.
- Existing userspace TCP tests and Docker kernel smoke should continue to pass.
- The first implementation can remain conservative and serial.
- The socket read path should submit export work rather than execute backend
  logic directly.
- `MemoryExport` remains non-durable and bounded by its in-memory size limit.
- The design must align with the long-term architecture docs without forcing
  all long-term components into the next implementation.
- Existing configs should continue to load through serde defaults.

# Non-Goals

- Implementing WAL, `ExportReadView`, `StorageEngine`, or compaction.
- Implementing range-aware `ExportAdmissionCtl` in the v0 runtime.
- Implementing the final split reader/writer `ConnectionRuntime`.
- Supporting authenticated same-owner multi-connection mounts immediately.
- Implementing etcd leases or cross-process writer fencing.
- Adding a generic workqueue framework before there is a concrete second queue.
- Supporting runtime switching while an export is active.

# End State

After this slice:

- `NbdServer` owns one `LocalExportRegistry`.
- `LocalExportRegistry` owns active export records on this process.
- `connection.rs` calls `registry.open(...)` during `NBD_OPT_GO`.
- `connection.rs` submits `ExportJob`s to the returned `ExportRuntime`.
- `ExportRuntime` owns a bounded queue and a worker task for one active export.
- `MemoryExportEngine` is the first `ExportEngine`.
- one active mounter per export is enforced by the local active map.
- config can choose the v0 runtime and engine, both defaulting to serial and
  memory behavior.

# Proposed Approach

Use three explicit roles.

```text
LocalExportRegistry
  owns active export lifecycle on this process
  loads export metadata from ExportCatalog
  creates one ExportRuntime per active export
  enforces active export owner rules

ExportRuntime
  owns export request queueing and execution
  owns the future ExportAdmissionCtl insertion point
  calls ExportEngine
  sends replies to the connection reply sink

ExportEngine
  owns concrete data behavior
  MemoryExportEngine now
  DurableExportEngine later
```

This keeps the workqueue owner unambiguous:

```text
NBDConnection submits work.
ExportRuntime executes work.
ExportEngine performs data operations.
```

For v0, the existing connection loop may still be one task that reads and writes
the socket sequentially. The important change is that export work goes through
the `ExportRuntime.submit` boundary. The connection loop can use a one-shot
reply sink and wait for the reply before reading the next request. A later
split reader/writer connection can use the same `submit` API with a per-
connection reply queue.

# State Boundaries

This design keeps source-of-truth responsibilities narrow:

- `ExportCatalog` remains durable export metadata truth.
- `LocalExportRegistry.active` is process-local active export lifecycle truth.
- `ExportRuntime` owns accepted export jobs and execution policy for one active
  export.
- `ExportAdmissionCtl` will own semantic read/write/flush ordering inside the
  runtime when it is introduced.
- `MemoryExportEngine` owns v0 export bytes for an active in-memory export.
- `MemoryExport` is only a compatibility adapter while the old `ExportHandle`
  connection path still exists.
- `NbdConnection` owns wire state: cookies, request parsing, and reply encoding.

Config selects the runtime and engine used for a newly opened export. It does
not mutate an already active export.

# Data Model / API Shape

## Configuration

Add a server config section with defaults:

```toml
[server]
export_runtime = "serial"
export_engine = "memory"
```

Conceptual Rust shape:

```rust
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServerConfig {
    #[serde(default)]
    pub export_runtime: ExportRuntimeKind,
    #[serde(default)]
    pub export_engine: ExportEngineKind,
}

pub enum ExportRuntimeKind {
    Serial,
}

pub enum ExportEngineKind {
    Memory,
}
```

Future config values can add `range_admission`, `durable`, `local_wal`, or S3
specific engine settings without changing the connection-facing API.

## Request And Reply Types

Introduce data-path request and reply values independent of NBD wire encoding:

```rust
enum ExportRequest {
    Read { offset: u64, len: u32 },
    Write { offset: u64, data: Bytes },
    Flush,
}

enum ExportReply {
    Read { data: Bytes },
    Done,
}

struct ExportError {
    kind: ExportErrorKind,
    message: String,
}

type ExportResult = Result<ExportReply, ExportError>;
```

The NBD layer keeps cookies and wire encoding:

```text
NBD request -> ExportRequest
ExportResult + cookie -> NBD simple/read reply
```

The protocol layer maps `ExportError` to NBD error codes. The export runtime
and engine should not need to know about wire-level reply codes.

## Export Engine

`ExportEngine` is data behavior only:

```rust
#[async_trait::async_trait]
trait ExportEngine: Send + Sync {
    async fn execute(&self, request: ExportRequest) -> ExportResult;
}
```

`MemoryExportEngine` owns the current in-memory byte vector and implements
`ExportEngine`.

During the migration, `MemoryExport` remains as the compatibility type used by
the old `ExportHandle` path. It should delegate to `MemoryExportEngine` so
there is one in-memory data implementation while the connection path is being
rewired.

## Export Runtime

`ExportRuntime` is the work execution boundary:

```rust
struct ExportJob {
    request: ExportRequest,
    reply: ReplySink,
}

#[async_trait::async_trait]
trait ExportRuntime: Send + Sync {
    async fn submit(&self, job: ExportJob) -> Result<()>;
}
```

The v0 implementation is a serial runtime:

```text
SerialExportRuntime
  bounded mpsc queue
  one worker task
  optional no-op admission point
  Arc<dyn ExportEngine>
```

Worker flow:

```text
recv ExportJob
  -> future: acquire ExportAdmissionCtl permit
  -> engine.execute(request)
  -> job.reply.send(result)
```

The connection sees only `submit`. It never calls `MemoryExportEngine` and does
not own export scheduling.

## Local Export Registry

`LocalExportRegistry` owns active export state:

```rust
struct LocalExportRegistry {
    catalog: Arc<dyn ExportCatalog>,
    runtime_factory: ExportRuntimeFactory,
    active: Mutex<HashMap<ExportName, ActiveExportState>>,
}

enum ActiveExportState {
    Opening { owner: ExportOwner },
    Open(ActiveExport),
    Closing {
        owner: ExportOwner,
        runtime: Arc<dyn ExportRuntime>,
    },
}

struct ActiveExport {
    owner: ExportOwner,
    runtime: Arc<dyn ExportRuntime>,
    connections: usize,
}

struct ExportOwner {
    id: ExportOwnerId,
}
```

V0 owner policy:

```text
ExportOwner::unique_connection(...)
```

Because each owner is unique, a second connection to the same export fails.
Later auth can provide a stable owner identity so same-owner connections share
the same active runtime and increment `connections`.

Conceptual API:

```rust
impl LocalExportRegistry {
    async fn open(
        &self,
        name: ExportName,
        owner: ExportOwner,
    ) -> Result<Arc<dyn ExportRuntime>>;

    async fn close(
        &self,
        name: &ExportName,
        owner: &ExportOwner,
    ) -> Result<()>;
}
```

Open flow:

```text
open(name, owner)
  -> lock active map
  -> if active is Open with same owner: connections += 1, return runtime
  -> if active exists in any other state: reject
  -> insert Opening(owner)
  -> unlock active map
  -> catalog.load_export(name)
  -> create engine from config and metadata
  -> create runtime from config and engine
  -> if load or creation fails: remove Opening before returning failure
  -> lock active map
  -> replace Opening with Open { owner, runtime, connections: 1 }
  -> return runtime
```

`NbdServer` can still construct the current `SQLiteExportCatalog` from
`CatalogUrl`, but `LocalExportRegistry` should depend on the `ExportCatalog`
trait so a future Postgres catalog does not change the registry API.

Close flow:

```text
close(name, owner)
  -> lock active map
  -> validate owner
  -> connections -= 1
  -> if connections == 0:
       transition record to Closing and take runtime handle
       unlock active map
       ask runtime to shutdown/drain according to v0 policy
       remove active export after runtime close completes
```

The v0 close policy can be simple because the current connection loop is
sequential. When split connections and pipelining are introduced, close must
wait until that connection has no accepted jobs left before calling registry
close.

# Request Lifetime

V0 sequential connection:

```text
socket read
  -> parse NBD request
  -> convert to ExportRequest
  -> create one-shot ReplySink
  -> runtime.submit(job)
  -> wait for one reply
  -> encode/write NBD reply
  -> read next request
```

Future split connection:

```text
reader task
  -> parse NBD request
  -> create ExportJob with connection reply_tx
  -> runtime.submit(job)
  -> continue reading after submit is accepted

export runtime worker
  -> admission/engine execution
  -> reply_tx.send(reply)

writer task
  -> recv reply
  -> write NBD reply
```

The export runtime owns the workqueue in both cases. The difference is only the
reply sink used by the connection.

# Reconciliation With Architecture Docs

This design aligns with the architecture docs with one terminology
clarification.

- `LocalExportRegistry` keeps its existing architecture meaning: process-local
  truth for active exports and, later, serving lease renewal.
- The architecture's broad `Export` responsibility maps to the new
  `ExportRuntime` plus `ExportEngine` split.
- `ExportRuntime` is the architecture's per-export admission/order boundary.
- `ExportEngine` is the concrete data implementation. `MemoryExportEngine` is
  the current implementation; `DurableExportEngine` will later compose WAL,
  `ExportReadView`, `CommittedStore`, and `StorageEngine`.
- The workqueue architecture says export workers do not write sockets. This
  design follows that by returning replies through a `ReplySink`.
- The protocol architecture already has an `Export Runtime` section. This
  design makes that section concrete for v0.

The main architecture doc still uses `NBDConnection -> Export -> ...` in the
umbrella diagram. Treat `ExportRuntime` as the concrete form of that active
serving export boundary for implementation purposes.

# Invariants

- `connection.rs` does not construct `MemoryExportEngine` directly.
- `NbdConnection` submits export work; it does not execute backend work.
- Every active export has one `ExportRuntime`.
- `MemoryExportEngine` is the single v0 in-memory data implementation.
- `MemoryExport` exists only as a compatibility adapter until the socket path
  uses `ExportRuntime`.
- Multiple connections to the same active export share the same runtime only
  when their `ExportOwner` matches.
- V0 uses unique owners, so only one active mounter per export succeeds.
- `Opening` reserves the export name before async catalog/factory work begins.
- `ExportRuntime.submit` returns after the job is accepted, not after the job is
  complete.
- Export workers send replies through a reply sink; they do not write sockets.
- Export errors are data-path errors until the protocol layer maps them to NBD
  reply error codes.
- `ExportAdmissionCtl` belongs inside `ExportRuntime`, not inside
  `ExportEngine`.
- `ExportEngine` does not know about sockets, NBD cookies, reply queues, or
  active export ownership.
- Config selects runtime policy and engine policy separately.

# Alternatives Considered

## Connection Runtime Spawns Request Tasks

This keeps the immediate implementation small, but it makes the connection
runtime act like the export workqueue. That blurs ownership and makes it harder
to explain where `ExportAdmissionCtl` and future worker pools live.

## Public `execute` And `submit`

Supporting both APIs lets inline code call `execute` and split socket code call
`submit`, but it makes the connection/runtime boundary mode-dependent. The
cleaner rule is that connection code always submits work to the export runtime.

## Separate Factory Type

This is likely useful later when lease acquisition, WAL replay, read-view
construction, and durable engine setup are larger. For the next slice it
creates more names than responsibility. `LocalExportRegistry.open` can be the
connection-facing open boundary while runtime/engine construction stays
internal.

## Put Admission In The Engine

This couples scheduling policy to storage behavior. `MemoryExportEngine` and
`DurableExportEngine` should not own admission. The runtime owns ordering and
calls the engine only when a request may execute.

# Migration / Rollout

No data migration is needed.

Code migration should be structural:

- add config defaults for runtime/engine choice;
- introduce `ExportRequest`, `ExportReply`, and reply sink helpers;
- move the in-memory data behavior into `MemoryExportEngine`;
- keep `MemoryExport` as an `ExportHandle` compatibility adapter until the
  connection path is rewired;
- add `SerialExportRuntime`;
- add `LocalExportRegistry`;
- update connection option negotiation to call registry open;
- update transmission handling to submit jobs and encode replies;
- add close path that calls registry close after transmission ends.

# Validation Strategy

Keep validation high-signal and boundary-focused:

- existing `MemoryExport` behavior tests move to or remain around
  `MemoryExportEngine`;
- TCP integration still proves read/write/flush/disconnect over real protocol;
- add an integration test for active export exclusion:
  - first client opens an export;
  - second client opening the same export fails during `NBD_OPT_GO`;
  - after the first disconnects, a later client can open the export again;
- run `cargo test --workspace`;
- run Docker smoke after the connection path changes.

# Risks

- The serial runtime adds async queue machinery earlier than strictly
  necessary. The tradeoff is clearer ownership and a stable future boundary.
- Close ordering can become subtle once split reader/writer connections exist.
  V0 should stay sequential and document that pipelined close requires an
  in-flight request tracker before registry close.
- If `Opening` cleanup is wrong, a failed open could leave an export stuck busy.
  Open failure paths must remove the opening reservation.
- Reply ordering remains sequential in v0. Future pipelining must decide whether
  to preserve submission order or allow cookie-correlated out-of-order replies.
- Config axes can become premature if not kept small. V0 should accept only
  `serial` and `memory`.

# Open Questions

None for this slice.

Resolved policy decisions:

- V0 uses a fixed bounded queue capacity in code, not a config knob.
- Same-owner multiple connections stay disabled until auth or another stable
  owner identity exists.
- Busy export during `NBD_OPT_GO` should map to `NBD_REP_ERR_POLICY`. The
  implementation may need to add that protocol constant and encoder helper.

# Design Exit Criteria

- The `LocalExportRegistry` name and responsibility are accepted.
- `ExportRuntime` is accepted as the export-owned workqueue boundary.
- `ExportEngine` is accepted as the data implementation boundary.
- The v0 serial runtime is accepted over connection-owned spawned work.
- Config separates runtime choice from engine choice.
- The active export owner model is good enough for future auth and etcd leases.

# Recommended Next Step

Proceed to `$plan-series`.
