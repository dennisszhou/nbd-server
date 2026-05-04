Title: Connection Admission And Concurrent Runtime
Date: 2026-05-03
Status: approved

# Problem

The server now has the first export runtime boundary: connections open exports
through `LocalExportRegistry`, `ExportRuntime.submit` accepts `ExportJob`s, and
`SerialExportRuntime` executes jobs for one active export through
`MemoryExportEngine`.

That foundation is intentionally conservative. The transmission loop still
reads one request, waits for export completion, writes one reply, and only then
reads the next request. That shape cannot exercise NBD pipelining, cannot keep
socket reads independent from export work, and leaves the future admission
boundary as an implementation note instead of a concrete API.

The next design needs to settle three related boundaries:

- `ConnectionRuntime`: split the transmission request read path from the reply
  write path while keeping protocol ownership in the connection layer.
- `ExportAdmissionCtl`: provide ordered logical range permits for read, write,
  and flush correctness.
- `ConcurrentExportRuntime`: move accepted export jobs off the connection
  runtime, own the shared per-export queue depth, and execute jobs behind the
  existing runtime boundary without moving storage semantics into socket
  handling.

# Goal

Define a small, implementable runtime design that:

- keeps `connection.rs` responsible for NBD wire parsing and reply encoding;
- lets the socket read path enqueue accepted requests without waiting for
  export completion;
- serializes writes to each connection socket through one reply writer;
- uses NBD cookies as the reply correlation key;
- introduces `ExportAdmissionCtl` as the per-export read/write/flush ordering
  source of truth and owner of dynamic range-lock state;
- makes admitted export requests the only engine execution path, so memory
  access cannot bypass admission;
- routes request-to-admission mapping through an admission profile so backing
  stores can define the storage-touch range they require;
- adds `ConcurrentExportRuntime` behind an export-runtime reservation and
  submission boundary;
- preserves `SerialExportRuntime` as the simple baseline and test oracle;
- keeps WAL, read-view, and durable storage behavior out of this slice.

# Constraints

- Runtime code remains Rust and Tokio based.
- The current `ExportRuntime` boundary should remain the connection-facing
  boundary.
- `ExportEngine` must not learn about sockets, NBD cookies, reply queues, or
  protocol error codes.
- `MemoryExportEngine` remains the only concrete engine in this design.
- The first admission implementation may be simple, but its API must express
  logical byte ranges and flush barriers.
- Export queue depth and reply queue capacity must be bounded. Spawned request
  tasks are bounded by export queue depth.
- Accepted jobs must complete, fail, or be canceled explicitly.
- Slow reply writing should apply backpressure through the shared export queue
  depth instead of being hidden behind unbounded reply buffering.
- The server should continue to avoid advertising `NBD_FLAG_CAN_MULTI_CONN`.
- Existing userspace protocol tests and Docker kernel smoke should continue to
  pass.

# Non-Goals

- Implementing `DurableExportEngine`, WAL, `ExportReadView`, object storage,
  compaction, or checkpoint publication.
- Implementing cross-process serving leases or writer fencing.
- Implementing authentication, client identity, or advertising
  `NBD_FLAG_CAN_MULTI_CONN`. Runtime ordering should still be correct for
  multiple same-owner connections sharing one active export once the registry
  can identify them.
- Adding a general-purpose workqueue framework before there are multiple real
  queue classes in code.
- Changing the catalog schema.
- Making concurrent runtime the default before it has explicit regression
  coverage.
- Supporting runtime policy changes for an already active export.

# End State

After this design is implemented:

- transmission mode is owned by an explicit `ConnectionRuntime`;
- each connection has one read task and one reply writer task;
- the read task copies write payloads, validates requests, and submits export
  jobs without waiting for export completion;
- the reply writer is the only task that writes transmission replies to that
  connection socket;
- each active export can be served by `SerialExportRuntime` or
  `ConcurrentExportRuntime`;
- `ConcurrentExportRuntime` owns a shared per-export queue-depth limit across
  all connections using that active export;
- `ConnectionRuntime` pauses request reads when the export runtime has no
  queue-depth capacity;
- admission order is assigned by the export runtime's accepted-job order, not
  by Tokio task polling order;
- every engine execution observes an `AdmittedExportRequest` that carries an
  `ExportAdmissionCtl` permit for the operation's semantic duration;
- `ExportAdmissionCtl` permits compatible operations concurrently and enforces
  ordered conflicts for overlapping writes and flush barriers;
- the export-wide `MemoryExportEngine` mutex is no longer the semantic
  synchronization point for read/write correctness;
- `SerialExportRuntime` remains available as a conservative runtime kind.

# Proposed Approach

Use three explicit layers.

```text
ConnectionRuntime
  owns one negotiated NBD transmission connection
  reads requests, submits ExportJobs, writes replies
  owns cookies, request validation, reply encoding, and cleanup

ExportRuntime
  owns accepted jobs for one active export
  owns shared queue depth across connections
  moves export work off the connection runtime
  chooses serial or concurrent execution policy
  owns ExportAdmissionCtl, admission profile, and range-lock table

ExportEngine
  owns data behavior for one active export
  MemoryExportEngine now
  DurableExportEngine later
```

The connection layer submits work and receives completions. The runtime layer
owns shared export queue depth and moves work away from the connection tasks.
Admission then decides which operations may run. The engine performs reads,
writes, and flushes against the current backend.

## Connection Runtime

`ConnectionRuntime` starts after `NBD_OPT_GO` succeeds and the server enters
transmission mode. Negotiation can stay in the current connection code for this
slice; the new runtime owns only the transmission phase.

Conceptual shape:

```rust
struct ConnectionRuntime {
    runtime: ExportRuntimeHandle,
    reply_capacity: usize,
}

impl ConnectionRuntime {
    async fn run(self, stream: TcpStream) -> Result<()>;
}
```

`run` splits the socket and starts two joined tasks:

```text
request reader task:
  read NBD request header
  validate command shape, payload length, and export range
  reserve one shared export-runtime queue slot
  read payload when the command carries one
  validate payload
  build ExportRequest
  build connection-owned ExportCompletion
  build ExportJob with ExportQueueSlot
  submit ExportJob
  return to socket read loop

reply writer task:
  receive completed replies from this connection's reply queue
  map ExportResult to NBD wire reply
  write exactly one reply at a time to the socket
  release the ExportQueueSlot after write completion or drop
```

The read task may await shared export-runtime queue capacity. It must not await
export execution. This keeps socket reads independent from WAL, storage, cache
misses, or admission waits once those paths exist.

The reservation happens after all header-only validation, including export
range validation, and before a variable-length write payload is read. That
keeps the shared export queue-depth limit meaningful across connections: when
the active export is full, connection readers pause before buffering more
request bodies outside the runtime.

The reply queue is per connection. It is the in-process queue feeding the
connection socket writer, not the request queue and not the kernel socket send
buffer. There is no global reply writer, and export workers never write
directly to sockets.

Completed replies carry their `ExportQueueSlot` through the connection reply
queue. The slot is released only after the reply writer finishes the socket
`write_all` for that reply or drops the reply during shutdown. This makes
shared queue depth mean outstanding export requests until protocol completion,
not merely until engine completion.

## Export Completion

`ExportJob` carries the export request, completion target, and queue slot. This
keeps the shared queue-depth token attached to the accepted request until the
request is converted into a connection reply.

Conceptual shape:

```rust
struct ExportJob {
    request: ExportRequest,
    completion: ExportCompletion,
    queue_slot: ExportQueueSlot,
}

struct ExportCompletion {
    target: ExportCompletionTarget,
}

enum ExportCompletionTarget {
    OneShot(oneshot::Sender<CompletedExport>),
    Sink(Box<dyn ExportCompletionSink>),
}

impl ExportCompletion {
    async fn complete(
        self,
        result: ExportResult,
        slot: ExportQueueSlot,
    );
}
```

`ExportRuntime` completes work by calling
`completion.complete(result, slot).await`. `ExportEngine` only returns
`ExportResult`; it does not know about completion targets, cookies, queue
slots, reply queues, or NBD error encoding.

Connection-specific completion state lives behind an opaque completion sink,
not in the export module:

```rust
trait ExportCompletionSink: Send {
    async fn complete(self: Box<Self>, completed: CompletedExport);
}

struct ConnectionExportCompletion {
    cookie: u64,
    expected: ReplyKind,
    replies: mpsc::Sender<ConnectionReply>,
}
```

The connection-owned sink packages the completed export result with the
request cookie and the expected reply kind:

```rust
enum ReplyKind {
    Read,
    Simple,
}

struct ConnectionReply {
    cookie: u64,
    expected: ReplyKind,
    result: ExportResult,
    _queue_slot: ExportQueueSlot,
}
```

The underscore on `_queue_slot` is intentional: the reply writer does not need
to inspect the slot. It only needs to keep the slot alive until the wire reply
has completed or the reply is dropped during shutdown.

The reply writer owns the protocol mapping:

```text
Read + Ok(Read { data }) -> NBD read reply
Read + other result      -> NBD simple error reply
Write/Flush + Ok(Done)  -> NBD simple success reply
Write/Flush + other     -> NBD simple error reply
```

For this slice, internal export failures can continue to map to `NBD_EINVAL`
where no more specific server error mapping exists. The protocol architecture
already defines the future direction for mapping storage and shutdown failures
to more specific NBD errors.

The connection-backed completion sends the result and `ExportQueueSlot` into a
bounded reply queue. If that queue is full, sending may wait. The
`ExportQueueSlot` moves with the reply and remains held until the reply writer
finishes writing the reply to the socket or drops it during shutdown. A slow
writer therefore consumes shared export queue depth and slows request intake.

## Connection Lifecycle

The connection runtime has one authoritative lifecycle for transmission mode:

```text
negotiated
running
reader_done
draining_replies
closed
failed
```

Client disconnect / EOF:

```text
reader receives NBD_CMD_DISC or EOF
  -> stop accepting new requests
  -> drop any reserved but unsubmitted ExportQueueSlot
  -> mark the connection reply path closed
  -> let accepted jobs finish engine work or be dropped by runtime shutdown
  -> drop replies instead of writing them to a closed socket
  -> dropping ConnectionReply releases ExportQueueSlot
  -> join reader and writer tasks
  -> return to serve(), which closes the LocalExportRegistry owner
```

For this path, the client has ended transmission. The server does not need to
write replies that can no longer be observed. The correctness requirement is
that accepted writes either complete according to the engine contract or are
dropped before they start, and that every queued job or reply releases its
`ExportQueueSlot`.

Protocol error:

```text
reader observes malformed mandatory input
  -> enqueue a protocol error reply if the protocol allows one
  -> stop accepting new requests
  -> close or drain according to the error's severity
```

Socket write failure:

```text
reply writer fails
  -> signal reader shutdown
  -> stop accepting new requests
  -> drain accepted writes to engine completion when they have started
  -> drop queued replies and their ExportQueueSlots
  -> return the write error
```

Server shutdown while the socket is still writable:

```text
shutdown requested
  -> stop accepting new requests
  -> let already accepted jobs complete, or synthesize shutdown errors for
     jobs that have not started
  -> write replies while the socket remains usable
  -> drop unwritten replies if the socket fails
  -> release every ExportQueueSlot through reply write or drop
  -> close the LocalExportRegistry owner after connection tasks settle
```

The first implementation does not need a separate public cancellation API. Once
a write has started engine execution, it should be allowed to run to completion
even if the reply cannot be written. Later durable write cancellation can be
revisited once WAL state exists.

Connection close is not sufficient by itself to close an active export. The
export runtime may still have accepted tasks that are waiting on admission or
executing engine work. `LocalExportRegistry.close` must mark the export as
closing, close the runtime to new reservations, and wait for accepted runtime
jobs to settle before removing the active export record.

## Export Admission Control

`ExportAdmissionCtl` is the per-export semantic ordering boundary. It protects
logical byte ranges, not storage layout, WAL records, tree nodes, or blob keys.

Conceptual API:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ByteRange {
    start: u64,
    len: u32,
}

enum AdmissionOp {
    Read(ByteRange),
    Write(ByteRange),
    Flush,
}

struct AdmissionTicket(u64);

struct AdmissionPermit {
    inner: Arc<AdmissionInner>,
    ticket: AdmissionTicket,
    op: AdmissionOp,
}

struct AdmissionWaiter {
    inner: Arc<AdmissionInner>,
    ticket: AdmissionTicket,
    op: AdmissionOp,
    grant: oneshot::Receiver<AdmissionPermit>,
}

impl ExportAdmissionCtl {
    fn new(extent_bytes: u64) -> Self;

    fn register(&self, op: AdmissionOp) -> Result<AdmissionWaiter>;
}

impl AdmissionWaiter {
    async fn wait(self) -> Result<AdmissionPermit>;
}
```

Registration assigns the admission ticket and inserts the operation into the
ordered admission state. Waiting turns that registered operation into an active
permit once its conflicts clear. The permit is RAII-style. The operation is
admitted only while the permit is held. Dropping the permit releases the active
operation and wakes blocked waiters. Waiters are already owned by request
tasks, so admission wakeup does not need a separate dispatcher to spawn newly
permitted work.

Registration also validates range operations against the admission controller's
current `extent_bytes` before assigning a ticket. Offset overflow or an end
past the current extent fails immediately and does not enter the waiting
queue. That keeps bounds enforcement in the same component that owns the
operation's storage-touch range and gives future resize a single update point.

Registration and promotion must avoid a check-then-wait race. The first
implementation should create a concrete per-waiter grant channel while holding
the admission state mutex. If the operation is already admissible, promotion
sends the grant before `wait()` is ever polled. If it is not admissible, the
waiting record is already visible to the permit-release path. That makes a
read registering while a write is finishing deterministic:

```text
read registers before write release takes the mutex
  -> read is in waiting
  -> write release promotes the read

write release takes the mutex before read registers
  -> write is removed from active
  -> read registration promotes itself immediately
```

There is no transient notification window where the waiter can miss the wakeup
and remain blocked forever.

The first implementation should use an accepted-order conflict queue with an
O(n) conflict scan. That is simpler than an interval tree and still implements
the real contract.

This is not strict FIFO execution. A later non-conflicting request may be
admitted before an earlier waiting request. The ticket exists so conflicting
operations and flush barriers are ordered by export-runtime acceptance, not by
Tokio task scheduling.

Conflict order must be assigned before request tasks can race with each other
on the Tokio scheduler. `ConcurrentExportRuntime::submit` registers the job
with `ExportAdmissionCtl` before spawning the request task. A design where each
spawned task receives a fresh ticket when it first polls is incorrect, because
a later conflicting request could become visible first and be admitted before
an earlier conflicting request or flush.

Conceptual state:

```rust
struct ExportAdmissionCtl {
    inner: Arc<AdmissionInner>,
}

struct AdmissionInner {
    state: Mutex<AdmissionState>,
}

struct AdmissionState {
    next_ticket: u64,
    extent_bytes: u64,
    waiting: VecDeque<WaitingAdmission>,
    active: Vec<ActiveAdmission>,
}

struct WaitingAdmission {
    ticket: AdmissionTicket,
    op: AdmissionOp,
    grant: oneshot::Sender<AdmissionPermit>,
}

struct ActiveAdmission {
    ticket: AdmissionTicket,
    op: AdmissionOp,
}
```

This state is a dynamic range-lock table, not a fixed set of block or chunk
locks. Ranges are inserted for the operations that actually arrive. That keeps
the admission model compatible with a future durable engine where export size
can change.

Resize is out of scope for this implementation, but the admission structure
must not assume an immutable extent. A future resize operation should be a
full-export barrier that updates `extent_bytes` under admission control before
later reads and writes are validated against the new extent.

An operation is admissible when both checks pass:

```text
1. it does not conflict with any active operation
2. no earlier waiting operation conflicts with it
```

Conflict rules:

```text
read(range A)  conflicts with write(range B) when A overlaps B
write(range A) conflicts with read(range B) or write(range B) when A overlaps B
flush          conflicts with every operation
```

These rules allow non-overlapping work to pass earlier non-conflicting waiters,
while preventing the important illegal reorderings:

```text
R1 block A active
W2 block A waiting
R3 block A arrives
```

`R3` must wait behind `W2` because `W2` is an earlier conflicting waiter. A
stream of reads cannot starve an overlapping write.

Flush is an export-wide barrier in this slice. Once a flush is waiting, later
reads and writes wait behind it. The flush is admitted only after earlier
active and earlier conflicting queued work has cleared. With the conservative
conflict rule, that means the flush runs alone.

Admission tickets are volatile scheduling and diagnostic values. They are not
WAL sequence numbers, are not durable, and must never be used for replay,
checkpointing, or recovery.

## Concurrent Export Runtime

`ConcurrentExportRuntime` is another implementation of the export runtime
boundary. This slice should evolve `ExportRuntime` so connections can reserve
shared export queue depth before reading or buffering a full request body.

Conceptual shape:

```rust
struct ConcurrentExportRuntime {
    meta: ExportMeta,
    engine: ExportEngineHandle,
    queue_depth: Arc<Semaphore>,
    admission: Arc<ExportAdmissionCtl>,
    lifecycle: Arc<ExportRuntimeLifecycle>,
}

struct ExportRuntimeLifecycle {
    state: Mutex<ExportRuntimeState>,
    empty: Notify,
}

struct ExportRuntimeState {
    closed: bool,
    active_jobs: usize,
}
```

Conceptual API shape:

```rust
#[async_trait::async_trait]
trait ExportRuntime: Send + Sync {
    fn export_meta(&self) -> ExportMeta;

    async fn reserve(&self) -> Result<ExportQueueSlot>;

    async fn submit(&self, job: ExportJob) -> Result<()>;

    async fn close(&self) -> Result<()>;
}

struct ExportQueueSlot {
    _queue_depth: OwnedSemaphorePermit,
}
```

`reserve` waits only for shared export queue-depth capacity.
`submit(job)` enqueues a job that already owns an `ExportQueueSlot`. Neither
call waits for admission, engine execution, or reply writing.
`close()` stops new reservations and waits for already accepted jobs to finish,
fail, or be canceled according to the runtime shutdown policy.

Both runtime implementations register admission before calling the engine.
`SerialExportRuntime` still executes queued jobs one at a time, but it uses the
same admitted engine path as the concurrent runtime. This keeps the engine
safety contract uniform: there is no serial-only bypass around admission.

`ConcurrentExportRuntime::submit` registers the job with admission and spawns
one Tokio request task. The runtime records the accepted job in
`ExportRuntimeLifecycle` before the task can run, and the task removes itself
on every completion, cancellation, or panic-unwind path. The job owns the slot
for the lifetime of the accepted request. The slot is moved into the
connection reply after engine completion and released only after the reply
writer finishes the socket write or drops the reply during shutdown. This
makes queue depth the shared outstanding request budget for one active export
across same-owner connections.

If the connection drops the request before submission, dropping the
`ExportQueueSlot` releases queue depth. If submission fails because the runtime
closed, the slot is also dropped and the connection path may encode the
appropriate shutdown/error reply when the socket is still usable.

Runtime flow:

```text
reserve()
  -> acquire shared ExportQueueSlot

runtime.submit(job)
  derive AdmissionOp from ExportAdmissionProfile
  register job with ExportAdmissionCtl
  register active job guard
  spawn request task with AdmissionWaiter and active job guard

request task:
  wait for its already-registered ExportAdmissionCtl permit
  build AdmittedExportRequest from request and permit
  call engine.execute_admitted(admitted_request)
  move ExportQueueSlot from ExportJob into ExportCompletion
  complete job ExportCompletion with ExportResult
  drop active job guard

reply writer task:
  write NBD reply to socket
  release ExportQueueSlot
```

Runtime close flow:

```text
LocalExportRegistry.close
  mark active export Closing
  runtime.close()
    close queue_depth semaphore
    reject later reserve/submit calls with RuntimeClosed
    cancel or neutralize admission waiters for jobs that will not start
    allow started writes to finish engine execution
    wait until active_jobs == 0
  remove active export record
```

This keeps spawned request tasks from becoming detached export mutations after
the process-local active export record has been removed.

Queue depth bounds accepted outstanding export jobs and therefore also bounds
the number of spawned request tasks for one active export. A separate engine
execution limit is intentionally not part of this slice. Storage/backend
concurrency belongs to `DurableExportEngine` or a future `StorageWorkQueue`,
not to the memory-engine concurrency proof.

Request tasks may wait asynchronously on `AdmissionWaiter`. That wait does not
block an OS thread. `ExportAdmissionCtl` remains responsible for promoting
compatible waiters, such as allowing a later read on block 2 to run while an
earlier write on block 1 is waiting behind an active read on block 1.

No separate admission dispatcher is introduced for the concurrent runtime.
Queue depth already bounds the number of accepted jobs and therefore the
number of spawned request tasks. Each task has registered its admission
position before spawning and simply awaits its own `AdmissionWaiter`. When an
active permit is dropped, `ExportAdmissionCtl` wakes compatible waiters, and
those already-spawned tasks resume.

`SerialExportRuntime` remains useful. It has no admission controller in the
current implementation, but the admitted engine boundary adds one before
unsafe memory access is introduced. Tests should continue to cover it because
it is the simplest runtime and a useful comparison point for concurrent
behavior. A later cleanup may collapse the serial runtime into the concurrent
runtime with queue depth one, but that is not part of this design's risky
transition.

## Memory Engine Synchronization

`ExportAdmissionCtl` should own semantic read/write/flush synchronization.
`MemoryExportEngine` should not use a single export-wide `Mutex<Vec<u8>>` as
the real correctness mechanism once concurrent runtime exists, because that
would hide admission bugs and serialize compatible non-overlapping requests.

The range locks that decide whether an operation may run belong to
`ExportAdmissionCtl`, not to `MemoryExportEngine`.

```text
ExportAdmissionCtl
  dynamic logical range-lock state
  active and waiting read/write/flush records
  cacheline/storage-touch aware admitted ranges
  future resize barrier and extent update point

MemoryExportEngine
  byte storage only
  no export-wide ordering mutex
  no range scheduling policy
```

The concurrent memory implementation should remove the coarse
`Mutex<Vec<u8>>`, but it should not replace that mutex with per-byte atomics.
Per-byte atomics make the proof easy but make large memory exports
unrepresentative and unnecessarily slow. Instead, unsafe memory access is
acceptable if the engine API makes admitted access a type-level capability.

```rust
trait ExportAdmissionProfile: Send + Sync {
    fn operation_for(&self, request: &ExportRequest) -> Result<AdmissionOp>;
}

struct AdmittedExportRequest {
    request: ExportRequest,
    _permit: AdmissionPermit,
}

#[async_trait::async_trait]
trait ExportEngine: Send + Sync {
    async fn execute_admitted(
        &self,
        request: AdmittedExportRequest,
    ) -> ExportResult;
}
```

`ExportAdmissionProfile` is the backing-store-specific mapping from a request
to the operation admission must protect. The first `MemoryAdmissionProfile`
can map read/write requests to exact byte ranges, unless the unsafe memory
implementation expands its actual touch range. Future file-backed, S3-backed,
WAL, compaction, or resize-aware engines can replace that mapping without
moving backend geometry into connection code or making admission tickets
durable.

`AdmittedExportRequest` is not a scheduler and does not expose admission
policy to the engine. It is an unforgeable capability proving that an export
runtime acquired an `AdmissionPermit` for the exact storage range the request
may touch. `SerialExportRuntime` also acquires admission before calling
`execute_admitted`; serial execution remains stricter than admission, but it
does not provide a separate engine-access capability.

The constructor should stay private to the runtime/admission boundary. Once
unsafe memory storage is introduced, safe callers must not be able to invoke
memory access with a bare `ExportRequest`.

If the memory backend touches cacheline-rounded, word-rounded, or otherwise
expanded ranges internally, `MemoryAdmissionProfile` must return an
`AdmissionOp` that covers that expanded storage-touch range. Admission remains
the operation-level lock: overlapping read/write and write/write operations
are excluded before the engine touches storage, while non-overlapping
operations can proceed without an export-wide memory mutex.

The current crate forbids unsafe code. Introducing unsafe raw memory storage
therefore requires either deliberately relaxing that crate-level policy with a
small reviewed unsafe boundary, or moving the unsafe storage type into a small
separate crate that documents the admitted-access safety contract.

This separates two proofs:

```text
ExportAdmissionCtl
  proves operation ordering, flush barriers, waiter wakeup, and starvation
  avoidance

MemoryExportEngine storage
  proves raw byte access is reachable only through an admitted request whose
  permit excludes overlapping storage touches
```

Tests should not depend on a memory-engine mutex for correctness. The
concurrency proof should use admission instrumentation, a controllable engine,
or unsafe memory storage behind admitted access so ordered range-lock behavior
is observable without a coarse engine mutex masking it.

## Configuration

Extend the server runtime enum without changing the catalog engine selection:

```rust
pub enum ExportRuntimeKind {
    Serial,
    Concurrent,
}
```

The existing default should remain `Serial`. Concurrent runtime should be
opt-in until its behavior is covered by protocol, runtime, and smoke tests.

Runtime sizing should be process config, not catalog metadata:

```toml
[server]
export_runtime = "concurrent"
export_queue_depth = 128
tokio_worker_threads = "auto"

[server.connection]
reply_queue_capacity = 128
```

Exact serde structure can stay small in the first implementation. The important
boundary is that these are process scheduling limits. They must not change the
meaning of an existing export's stored data.

`export_queue_depth` is not an OS-thread count. It is accepted outstanding
request budget. Actual OS-thread parallelism comes from the Tokio runtime. The
current server binary uses Tokio's `current_thread` runtime; the concurrent
runtime can prove async ordering on that runtime, but true parallel engine
execution requires switching the server binary to Tokio's multi-thread runtime
or constructing an explicit multi-thread runtime in `main`.

# Data Model / API Shape

## Source Of Truth

- `ExportCatalog` remains the durable metadata source of truth.
- `LocalExportRegistry.active` remains process-local active export lifecycle
  truth.
- `ConnectionRuntime` owns per-connection protocol state and reply state.
- `ExportAdmissionCtl` owns volatile per-export admission order, dynamic range
  locks, active permit state, current extent size, and the future extent update
  point.
- `ExportRuntime` owns shared per-export queue depth, accepted export jobs, and
  runtime execution policy.
- `ExportRuntimeLifecycle` owns volatile runtime close state and accepted job
  settlement tracking.
- `ExportAdmissionProfile` owns the backing-store-specific request-to-admission
  mapping for the active export.
- `ExportEngine` owns data behavior.
- `AdmittedExportRequest` owns the volatile engine-access capability proving
  the request was admitted before engine execution.
- WAL sequence and durability remain future `WALManager` responsibilities.

## Derived State

- NBD wire replies are derived from `ExportResult`, the original request kind,
  and the request cookie.
- Admission operations are derived from `ExportRequest` and the active export's
  admission profile.
- Admission conflict decisions are derived from dynamic range-lock state,
  active permits, earlier queued waiters, and the current admitted extent.
- Queue depth and reply queue occupancy are runtime diagnostics, not durable
  state.

## Cached State

No new durable or serving cache is introduced by this design. Future
`ExportReadView` state belongs to the durable engine design, not to
`ConnectionRuntime` or `ExportAdmissionCtl`.

# Invariants

- Only the connection reply writer writes transmission replies to its socket.
- The connection request reader never calls `ExportEngine` directly.
- The connection request reader does not wait for export execution to finish.
- Export queue depth is shared across connections for one active export.
- Connection readers reserve export queue depth before reading write payloads.
- Connection readers pause when the export runtime has no queue-depth capacity.
- `ExportRuntime.submit(job)` means accepted into the runtime, not completed.
- `ExportRuntime.close()` rejects new reservations and waits for accepted jobs
  to settle before the active export is removed from `LocalExportRegistry`.
- Export queue depth remains occupied until the reply writer finishes the
  socket write or drops the reply during shutdown.
- `ExportQueueSlot` and `AdmissionPermit` are distinct. Queue slots track
  outstanding accepted requests; admission permits track active range locks.
- `ConcurrentExportRuntime` moves request work off connection tasks before
  admission or engine execution can block.
- `ExportEngine` never observes NBD cookies or writes NBD replies.
- Once unsafe memory storage exists, `ExportEngine` does not expose a safe
  raw `ExportRequest` execution path for memory access; callers execute
  through an `AdmittedExportRequest`.
- Runtime admission registration uses the active `ExportAdmissionProfile`;
  runtimes do not hard-code memory, file, S3, or WAL geometry.
- `ExportAdmissionCtl` rejects out-of-bounds read/write admission operations
  before assigning tickets or inserting waiters.
- Every read, write, and flush holds an admission permit while its engine
  operation is executing, including requests served by `SerialExportRuntime`.
- Overlapping writes never execute concurrently.
- Reads do not execute concurrently with overlapping active writes.
- Later overlapping reads do not pass earlier waiting writes.
- Flush is an export-wide barrier in this slice.
- Admission tickets are volatile and never used as WAL sequence numbers.
- Admission ranges are dynamic operation ranges, not static memory chunks.
- Admission ranges cover the full storage-touch range, including any
  cacheline or word expansion required by the active admission profile.
- The admission extent is the volatile serving extent for the active export.
  Resize is a future full-export admission barrier that updates that extent
  before later operation validation.
- Admission waiter grants cannot be lost: registration and promotion happen
  under the same admission state mutex.
- Dropping an admission permit releases the active operation and wakes waiters.
- Dropping a registered admission waiter removes it or makes it inert.
- `MemoryExportEngine` does not use an export-wide mutex as the semantic
  read/write ordering mechanism in concurrent runtime tests.
- Queue depth and reply queues are bounded. Spawned request tasks are bounded
  by queue depth.
- Spawned request tasks are tracked by the export runtime lifecycle; they are
  not detached mutations after local export close.
- Serial runtime remains semantically valid because serial execution is stricter
  than the admission contract.

# Alternatives Considered

## Keep The Sequential Connection Loop

This preserves the current simple behavior, but it means the socket read path
continues to wait for export completion. It cannot validate pipelining,
per-connection reply queues, or concurrent export execution.

## Let Export Workers Write Replies

This would remove the reply queue, but it couples export scheduling to socket
I/O and makes slow clients block worker tasks while holding data-path resources.
It also spreads protocol error mapping outside the protocol layer.

## Add A Generic Workqueue Framework First

The architecture calls for multiple queue classes long term, but the next slice
only needs concrete connection, admission, and export-runtime queues. A generic
framework would add review surface before there is enough code pressure to
justify it.

## Add A Separate Admission Dispatcher

A dispatcher could receive jobs, ask admission which operation is next, and
spawn work only after admission grants a permit. That adds another lifecycle
owner without reducing the core synchronization problem: when a permit is
released, the system still has to resume or spawn newly admitted work.

The chosen model is smaller. `submit(job)` registers the admission waiter in
accepted-job order and spawns one bounded request task. The task awaits the
waiter. Permit release wakes compatible waiters directly, so no dispatcher owns
an additional job queue or task-spawn policy.

## Let Spawned Tasks Race Into Admission

Spawning request tasks before they are visible to admission is attractive
because each task can simply block on its own permit. The blocking part is
fine; the ordering part is not. If the task that reaches `ExportAdmissionCtl`
first receives the earlier ticket, Tokio scheduling can reorder conflicting
operations and flush barriers.

For example, if a connection submits `write block 1` followed by `flush`, but
the flush task reaches admission first, the flush could run before the write is
visible. Likewise, `read block 1`, `write block 1`, `read block 1` could admit
the second read before the write if that read task wins the scheduler race.

The chosen model still lets tasks await permits. It only requires
`submit(job)` to make the job visible to admission, with its accepted-order
ticket, before spawning the task that waits for the permit.

## Use A Global Mutex For Admission

A global mutex would be correct for the first runtime, but it would not prove
the ordered range permit API. The proposed O(n) accepted-order conflict queue
is still simple while exercising the real read/write/flush semantics.

## Rely On The Memory Engine Mutex

The current `MemoryExportEngine` mutex is a data-structure lock, not the
runtime correctness model. Keeping it as the only real synchronization point
would make concurrent runtime tests pass for the wrong reason and would not
carry over to durable engines, resize, WAL replay, or storage-backed reads.

## Derive Admission Directly From ExportRequest

Deriving `AdmissionOp` directly from `ExportRequest` is enough for the first
byte-addressed memory engine, but it would make the runtime the owner of
backend geometry. File-backed and S3-backed engines may need block alignment,
extent barriers, object/leaf boundaries, or WAL/read-view barriers. The chosen
model keeps that mapping behind `ExportAdmissionProfile` while
`ExportAdmissionCtl` still owns dynamic waiting and active permit state.

## Add A Serial-Only Admission Bypass

An earlier shape let the serial runtime call admitted engine methods without
an admission permit. That is correct only by convention and adds a second
safety proof for memory access. The chosen model makes `SerialExportRuntime`
acquire admission too. Serial execution is still a simple baseline, but the
engine sees the same admitted capability in every runtime.

## Build An Interval Tree Immediately

An interval tree can improve admission scans later, but it is not needed to
establish the contract. The O(n) queue is easier to review and can be replaced
behind the same API when benchmarks or scale tests justify it.

# Migration / Rollout

No data migration is needed.

Rollout should be conservative:

- keep `SerialExportRuntime` as the default runtime kind;
- add `ConcurrentExportRuntime` as an opt-in process setting;
- keep `MemoryExportEngine` as the only engine for this design;
- preserve existing `nbdcli create --engine memory` behavior;
- preserve existing NBD transmission flags and do not advertise multi-conn;
- keep Docker kernel smoke on the default serial path first;
- add at least one opt-in userspace smoke or integration check for the
  concurrent path before making it default;
- keep Docker kernel smoke in the validation path as a regression check even
  when userspace smoke is the primary concurrent-runtime proof.

# Validation Strategy

Admission control should have focused unit tests:

- out-of-bounds read/write ranges are rejected before a ticket is assigned;
- read block 1, write block 1, read block 1 admits in ticket order and the
  second read waits behind the write;
- a read registered while a conflicting write permit is being released cannot
  miss its grant and block forever;
- overlapping writes do not hold permits concurrently;
- overlapping read and write do not hold permits concurrently;
- non-overlapping read/write operations can hold permits concurrently;
- later overlapping reads wait behind earlier waiting writes;
- flush waits for active work and runs alone;
- operations after a waiting flush wait behind it;
- dropping a registered waiter removes or neutralizes the waiter;
- dropping a permit wakes compatible waiters.

`ConcurrentExportRuntime` should have runtime tests with a controllable test
engine:

- `reserve` blocks when shared export queue depth is exhausted;
- `submit` returns after queue acceptance, before engine completion;
- queue-depth capacity remains held while a completed reply waits in or before
  the connection reply queue;
- queue-depth capacity is released only after the reply writer finishes the
  socket write or drops the reply during shutdown;
- compatible operations can execute concurrently;
- conflicting operations are serialized by admission;
- flush forms a barrier;
- queue closure reports `RuntimeClosed`;
- runtime close rejects new reservations and waits for accepted jobs to settle;
- runtime close does not remove the active export while request tasks can still
  mutate the engine;
- export completions receive exactly one result for each accepted job.

`MemoryExportEngine` should have storage-safety regression tests:

- `MemoryAdmissionProfile` maps read/write/flush requests to the expected
  admission operations;
- unsafe/raw memory access is possible only through `AdmittedExportRequest`;
- direct admitted non-overlapping reads and writes can overlap without relying
  on an export-wide memory mutex;
- serial runtime execution reaches memory only through an admitted request;
- concurrent non-overlapping writes through `ConcurrentExportRuntime`
  complete without relying on an export-wide memory mutex once that runtime
  exists;
- a read after an admitted write observes the completed write bytes;
- overlapping memory operations are serialized by admission, not by storage;
- flush waits for earlier writes even though the memory engine flush is a
  no-op.

`ConnectionRuntime` should have protocol-level tests:

- pipelined requests are submitted before earlier export work completes;
- replies carry the correct NBD cookies when completions are out of order;
- the reply writer serializes socket writes for one connection;
- export queue-depth exhaustion backpressures connection readers;
- write payload reads pause behind export queue-depth reservation;
- disconnect and EOF close the local export owner after accepted work settles;
- malformed requests still receive the same protocol errors or disconnect
  behavior as the current implementation.

Repository validation for the implementation series should include:

```text
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
make docker-smoke
```

An additional concurrent-runtime smoke target can be added once config wiring
exists. It can use the userspace client as the primary proof because the
connection/runtime/admission path is the same server path exercised by the
kernel smoke. Docker kernel smoke should still run as a compatibility
regression check before handoff.

# Risks

- Close ordering becomes more subtle once the read task can have accepted work
  that has not yet replied. The connection runtime must join its tasks before
  `serve()` closes the registry owner.
- Runtime task tracking is required; detached `tokio::spawn` handles would let
  engine mutations outlive the active export registry record.
- Bounded reply queues intentionally feed back into export queue depth. Tests
  should prove slow reply writing eventually blocks new export reservations.
- If reply sending into a bounded connection queue waits while an admission
  permit is still held, one slow writer can block unrelated compatible work.
  Runtime tasks should drop admission permits before awaiting reply enqueue.
- Admission cancellation must remove or neutralize abandoned waiters, otherwise
  a dropped request can block later compatible work.
- Admission promotion must be state-based, not notification-based. A lost wake
  between register and permit release would deadlock an otherwise admissible
  request.
- Unsafe memory storage is acceptable only if safe callers cannot reach raw
  memory access without an admitted request. An informal "runtime promises to
  call admission first" convention behind a safe `execute(ExportRequest)` API
  would be unsound.
- The admitted storage range must match every byte/cacheline/word the memory
  engine may touch. If that geometry changes, admission range derivation must
  change with it.
- Concurrent runtime can expose ordering assumptions hidden by
  `SerialExportRuntime`. Admission tests need to cover waiting conflicts, not
  only active conflicts.

# Open Questions

None.

# Design Exit Criteria

This design is ready for `$review-plan` when:

- `ConnectionRuntime` ownership of request reads, reply writes, and cleanup is
  accepted;
- `ExportCompletion` is accepted as the completion boundary that hides NBD
  cookies from export runtime and engine code except in connection-specific
  completion state;
- the O(n) accepted-order conflict queue in `ExportAdmissionCtl` is accepted as
  the first range-permit implementation;
- `ExportAdmissionProfile` is accepted as the backing-store-specific source of
  request-to-admission mapping;
- `AdmittedExportRequest` is accepted as the only engine execution capability,
  including for `SerialExportRuntime`;
- `ConcurrentExportRuntime` shared queue-depth bound is accepted;
- `SerialExportRuntime` remaining as default is accepted;
- the validation strategy is considered sufficient for the concurrency risk.

# Recommended Next Step

Run `$review-plan` against this design and the upstream architecture docs.
After approval, use `$plan-series` to split the implementation into reviewable
commits.
