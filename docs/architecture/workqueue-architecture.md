Title: Workqueue Architecture
Date: 2026-05-01
Status: draft

# Problem

The NBD socket read path must not block on storage I/O, WAL append, cache miss
handling, compaction, or catalog operations. The server also needs bounded
concurrency, cancellation, shutdown, and observability for request work.

Workqueues should be explicit request boundaries rather than incidental
implementation details.

# Goal

Define a shared workqueue model that can support:

- NBD request offload from the socket read path;
- serialized replies per connection;
- export admission scheduling;
- bounded storage I/O work;
- background compaction;
- lease renewal and shutdown cleanup;
- cancellation and backpressure.

# Queue Classes

```text
connection request queue:
  offloads decoded NBD requests from the socket read path

reply queue:
  one bounded queue per NBD connection; serializes replies back to that
  connection's socket writer

export admission queue:
  grants permits for read/write/flush operations

storage work queue:
  bounds and executes object I/O against local/S3 storage; owns storage-side
  concurrency limits and shared backend client resources

compaction queue:
  runs background checkpointing work at lower priority

lease queue:
  scans local active exports every 30 seconds, renews etcd leases, and runs
  shutdown cleanup
```

# Common API Shape

```rust
struct WorkQueue<J> {
    // bounded queue, worker pool, cancellation, metrics
}

impl<J> WorkQueue<J> {
    async fn submit(&self, job: J) -> Result<JobHandle<J::Output>>;
    async fn shutdown(&self, mode: ShutdownMode) -> Result<()>;
}

struct JobHandle<T> {
    async fn wait(self) -> Result<T>;
    fn cancel(&self);
}
```

Each job should carry context:

```rust
struct JobContext {
    export_id: Option<ExportId>,
    connection_id: Option<ConnectionId>,
    request_handle: Option<NbdHandle>,
    cancellation: CancellationToken,
}
```

# Request Boundary

The NBD socket read path boundary is:

```text
read bytes from socket
  -> decode request
  -> copy or retain request payload
  -> enqueue request job
  -> return to socket read loop
```

No WAL append, storage read, catalog update, or compaction work should happen on
the socket read path.

# Socket And Reply Boundary

The long-term socket architecture separates inbound request handling from
outbound reply serialization for each connection. The exact task layout is
flexible; the ownership boundary is not.

```text
connection A input -> ExportRequestQueue -> admitted work
                                      -> connection A reply queue
                                      -> connection A output
```

There is no global reply writer. A slow or blocked client should apply
backpressure to its own bounded reply path without blocking replies for other
connections.

Export workers do not write sockets. They complete by returning a reply to the
reply path attached to the original request.

# Storage Boundary

Storage callers should normally submit object work to `StorageWorkQueue` rather
than call the backend directly from request workers.

This keeps storage concurrency and backpressure policy out of:

- NBD protocol code;
- `Export`;
- `ExportReadView`;
- `CommittedTreeReader`;
- `CompactionManager`.

The queue may reorder independent storage I/O, but it must not define export
correctness ordering. Correctness ordering belongs to `ExportAdmissionCtl` and
WAL durability rules.

For S3-compatible backends, the storage runtime should reuse backend client or
configuration objects so connection pools, credential caches, retries, and
timeouts are shared. The storage queue owns backend concurrency policy; export
correctness is still defined above it.

# Invariants

- Queue capacity is bounded.
- Enqueue failure is explicit.
- Accepted jobs complete, fail, or are canceled explicitly.
- Shutdown behavior is declared per queue.
- Storage queues may reorder independent I/O but do not define read/write/flush
  correctness.
- Admission defines conflicting operation order.
- Reply queues are per connection.
- Export workers never write directly to sockets.
- Job context is available for tracing, cleanup, and diagnostics.
- Cancellation must not make an acknowledged write disappear.

# Shutdown Modes

The queue abstraction should support at least:

```rust
enum ShutdownMode {
    Drain,
    CancelPending,
    CancelAll,
}
```

Data-path queues should be conservative:

- pending reads can usually be canceled after connection close;
- pending writes that have not started can be canceled;
- started writes either complete durably and update cache or fail before
  acknowledgement;
- completed writes are never rolled back by cancellation.

# Open Questions

- Whether the first implementation needs one shared worker pool or per-queue
  worker pools.
- How much priority scheduling is needed between foreground reads/writes and
  background compaction.
- Whether storage queue jobs should include retry policy or leave retries to
  callers.
- Exact metrics/tracing shape.
