Title: Connection Task Ownership And Shutdown
Date: 2026-05-06
Status: approved

Problem
- The current server accept loop spawns connection tasks and discards their
  `JoinHandle`s. `NbdServer::shutdown()` stops accepting new sockets and joins
  the accept loop, but it does not own or await active connection tasks.
- Because connection tasks own the active export close path, shutdown can log
  completion while active exports, accepted runtime jobs, or close-time
  compaction are still running.
- `LocalExportRegistry::close()` moves an export to `Closing` before awaiting
  `runtime.close()`. If close returns an error, the active map is not cleaned
  up and the export can remain locally busy forever.
- The CLI `serve` path starts `NbdServer` and then waits forever, so normal
  process signals do not exercise the graceful shutdown API.
- The request path already has useful RAII primitives, but the task ownership
  and shutdown contract above those primitives is still implicit.

Goal
- Make `NbdServer` the owner of accepted connection tasks for the lifetime of
  the server.
- Make graceful shutdown mean:
  - stop accepting new connections;
  - signal active connections to stop reading new requests;
  - let each connection run its export cleanup path;
  - let each active export runtime drain accepted work before engine close;
  - report server shutdown completion only after owned connection tasks have
    finished.
- Keep the existing request-path safety model:
  - `ExportQueueSlot` remains live until reply write or reply drop;
  - `AdmissionPermit` remains live for the semantic engine operation;
  - accepted writes are not canceled after admission merely because the socket
    is going away.
- Make registry close idempotent and cleanup-safe even when engine close or
  best-effort close work reports an error.
- Add high-signal tests around active shutdown and close failure cleanup.

Constraints
- Do not add a new dependency just to model cancellation. Tokio's existing
  `watch`, `oneshot`, and task APIs are sufficient for this slice.
- Do not change NBD protocol semantics or advertise multi-connection support.
- Do not change export admission ordering, queue-depth meaning, WAL durability,
  compaction semantics, or clone behavior.
- Do not make close-time compaction a durability boundary. Acknowledged writes
  must already be durable before close starts.
- Do not implement a full backoff, timeout, or hard-abort policy in this slice.
  A stuck engine job may still delay graceful shutdown.
- Preserve the current conservative behavior that accepted work drains before
  `ExportEngine::close()` runs.

Non-goals
- Distributed serving leases, fencing, or lease-loss shutdown.
- A general-purpose workqueue framework.
- Per-request cancellation tokens inside storage engines.
- Timed shutdown escalation from graceful drain to hard abort.
- Changing the one-active-owner-per-export policy.
- Making `Drop` a fully graceful async cleanup path. Rust cannot await in
  `Drop`; deterministic cleanup belongs to `NbdServer::shutdown().await`.

End state
- `NbdServer` owns an accept/supervisor task.
- The supervisor owns all spawned connection tasks through a join set or an
  equivalent tracked task collection.
- A server shutdown signal is broadcast to every active and future connection
  task owned by that supervisor.
- Connection tasks observe shutdown cooperatively and return through their
  normal cleanup path instead of being aborted in the ordinary shutdown path.
- `NbdServer::shutdown().await` joins the supervisor after it has stopped
  accepting and after all tracked connection tasks have completed.
- The CLI `serve` command waits on `tokio::signal::ctrl_c()` and then calls
  `server.shutdown().await`.
- `LocalExportRegistry::close()` removes or truthfully finalizes local active
  state after a close attempt even when `runtime.close()` returns an error.
- Shutdown completion logs are truthful: after `server.shutdown()` returns,
  the server no longer owns active connection tasks or active local exports for
  that server instance.

Proposed approach
- Add a server-owned connection supervisor inside `NbdServer::start_on`.
- Keep the supervisor inside the spawned server task, because it owns the
  listener, the shutdown signal, and connection task joins as one lifecycle.
- Replace fire-and-forget connection spawning with tracked spawning:

```text
NbdServer
  -> accept/supervisor task
       owns TcpListener
       owns connection shutdown broadcast
       owns JoinSet<ConnectionTaskOutcome>
       accepts new sockets until server shutdown starts
       reaps completed connection tasks while accepting
       on shutdown:
         stop accepting
         broadcast connection shutdown
         await every tracked connection task
```

- Pass a connection shutdown receiver into `connection::serve`.
- Race the shutdown receiver against every connection socket I/O operation
  that may wait: handshake reads and writes, option negotiation reads and
  writes, transmission request reads, and transmission reply writes. An
  accepted connection that has not yet opened an export must still finish
  promptly during server shutdown.
- Thread that receiver into `ConnectionRuntime` so the transmission reader and
  reply writer can stop cooperatively after `NBD_OPT_GO`.
- Treat server shutdown as a connection close reason that stops reading new
  requests. Already-submitted export jobs still complete or fail through the
  export runtime. If the reply queue receiver is dropped, later completions
  drop their `ConnectionReply` and release the `ExportQueueSlot`.
- Keep client disconnect and protocol-error behavior separate from server
  shutdown. The existing "send one error then drain" behavior for request
  decode failures should remain explicit.
- Replace the boolean `drain_replies` shape with a small enum if the
  implementation touches that path. The enum should name the reply policy
  rather than encoding shutdown behavior in a bool.

```rust
enum ConnectionReplyDrain {
    DropPending,
    DrainQueued,
}

struct RequestReaderExit {
    result: Result<()>,
    reply_drain: ConnectionReplyDrain,
}
```

- Normal graceful server shutdown should use `DropPending`: the server is
  closing the connection and should not wait on socket replies before export
  cleanup. Accepted jobs still drain in `runtime.close()`.
- Protocol decode errors that have already enqueued an error reply should use
  `DrainQueued`, preserving the existing behavior where the connection writes
  the error response before closing.
- Update `LocalExportRegistry::close()` to always clean local active state for
  the matching owner after the close attempt reaches the final connection.
  The close result should still be returned and logged, but the export should
  not remain stuck in `Closing`.

```text
close(name, owner)
  -> if this is not the final connection: decrement and return
  -> mark Open -> Closing
  -> await runtime.close()
  -> remove Closing entry for the same owner, regardless of close result
  -> return runtime.close() result
```

- This cleanup rule relies on the invariant that `ExportEngine::close()` is
  cleanup/finalization work, not the durability boundary for acknowledged
  writes. WAL replay remains the recovery path if close-time compaction fails.
- Change `NbdServer::Drop` to be best-effort only. It should signal shutdown
  and let the supervisor task complete detached if possible. It should not be
  the normal path that aborts active connection cleanup. Callers that need a
  truthful lifecycle must call `shutdown().await`.

Data model / API shape
- New internal shutdown signal:

```rust
#[derive(Clone)]
struct ServerConnectionShutdown {
    tx: watch::Sender<bool>,
}

struct ConnectionShutdown {
    rx: watch::Receiver<bool>,
}

impl ConnectionShutdown {
    async fn cancelled(&mut self);
    fn is_cancelled(&self) -> bool;
}
```

- The concrete names can change during implementation, but the meaning should
  stay narrow: this signal is process-local server shutdown, not export close,
  client disconnect, lease loss, or request cancellation.
- New internal supervisor state:

```rust
struct ConnectionSupervisor {
    shutdown: ServerConnectionShutdown,
    tasks: JoinSet<ConnectionTaskOutcome>,
}

struct ConnectionTaskOutcome {
    connection_id: ConnectionId,
    peer_addr: SocketAddr,
    result: Result<()>,
}
```

- `ConnectionSupervisor` is not durable state and is not the active export
  source of truth. It owns task lifecycle only.
- `LocalExportRegistry.active` remains the process-local source of truth for
  open exports and final-owner close.
- `ExportRuntime` lifecycle state remains the source of truth for accepted
  export jobs, closed runtimes, and engine close idempotence.
- Connection reply queues remain transient per-connection buffers. They are not
  public status and are not a durability boundary.

Invariants
- The server accept loop must not spawn an untracked connection task.
- `NbdServer::shutdown().await` must not return before all tracked connection
  tasks have finished.
- A tracked connection task must observe shutdown even if it is blocked before
  export open in handshake or option negotiation.
- Normal graceful shutdown must signal connection tasks cooperatively rather
  than aborting them after an export has opened.
- Every successful export open in `connection::serve` must be paired with
  exactly one registry close attempt on every normal return path after open.
- `LocalExportRegistry` must not leave a matching `Closing` entry behind after
  a final-owner close attempt completes.
- A close error may be returned and logged, but it must not keep the export
  locally busy after the runtime has been closed to new work.
- Accepted export jobs must either complete, fail, or drop their completion
  target in a way that releases their queue slot.
- Connection shutdown may drop replies, but it must not release an admission
  permit before the admitted engine operation ends.
- `ExportEngine::close()` must run only after the runtime has stopped accepting
  new work and accepted active jobs have drained.
- Server shutdown is distinct from client disconnect, protocol validation
  failure, runtime closed, and future lease loss.

Alternatives considered
- Keep fire-and-forget connection tasks and rely on client disconnect:
  rejected because server shutdown would remain observationally false and
  tests could pass while active exports are still closing in the background.
- Abort connection tasks during shutdown:
  rejected for the normal path because aborting after export open can skip the
  async registry close path. Hard abort remains a process-exit reality, but it
  should not be the graceful shutdown model.
- Add per-request cancellation inside engines:
  rejected for this slice. The immediate problem is task ownership and export
  close cleanup. Canceling storage operations changes durability and reply
  semantics and needs a separate design.
- Add shutdown timeouts now:
  rejected for this slice. A timeout policy needs decisions about whether
  acknowledged writes may continue in the background, how errors surface to
  operators, and whether close-time compaction can be skipped. The first step
  should make ownership explicit.
- Use a new cancellation-token dependency:
  rejected because Tokio `watch` is enough for a single broadcast shutdown
  signal.

Migration / rollout
- No durable migration is needed.
- The changes are internal to the server runtime, connection runtime, CLI
  serving loop, and registry cleanup behavior.
- Existing exports and WAL files remain compatible.
- Existing tests that call `server.shutdown().await` should continue to work,
  but shutdown may now wait for active connection cleanup instead of only the
  accept loop.

Validation strategy
- Add a TCP integration test where a client opens an export and remains
  connected while `server.shutdown().await` is called. The test should prove
  shutdown completes and that a restarted server can reopen the same export.
- Add a WAL-durable integration test or extend an existing one so an
  acknowledged write followed by server shutdown remains readable after
  restart. This proves shutdown still routes through export close/drain.
- Add a connection-runtime test showing a shutdown signal stops a reader that
  is blocked waiting for the next request and releases the connection task.
- Add a TCP integration test where a client opens a TCP connection but does not
  complete negotiation, then `server.shutdown().await` completes without
  waiting for client input.
- Add a registry-focused test for final-owner close cleanup when runtime close
  returns an error. The test should prove a later open is not blocked by a
  stale `Closing` entry.
- Keep the existing runtime tests that prove close rejects new work, waits for
  accepted jobs, and calls engine close once.
- Run at least:

```text
cargo fmt --all --check
cargo test -p nbd-server --test export_runtime
cargo test -p nbd-server --test local_export_registry
cargo test -p nbd-server --test tcp_integration
make docker-smoke
```

Risks
- Graceful shutdown can still wait forever if an engine operation never
  returns. This design makes ownership explicit but does not add timeout or
  hard-abort policy.
- Dropping replies during server shutdown means a write can become durable
  while the client does not receive its reply. That is already possible with a
  broken TCP connection and is acceptable for shutdown; recovery must come from
  durable WAL state.
- If `NbdServer` is dropped without calling `shutdown().await`, completion and
  error reporting are best-effort. The CLI and tests should use the graceful
  API.
- A connection task panic after export open can still bypass async cleanup.
  The code should avoid panics on protocol and I/O paths; panic containment can
  be addressed separately if it becomes important.

Open questions
- none

Design exit criteria
- The design is ready for `$review-plan` when the shutdown owner, connection
  signal, registry cleanup rule, and validation expectations are accepted.
- It is not ready if shutdown must include timeout escalation, request-level
  engine cancellation, or distributed lease loss in the same slice.

Recommended next step
- Use `$plan-series` to split the approved design into a small implementation
  stack.
