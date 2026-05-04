Title: Connection Admission Concurrency Execution
Date: 2026-05-03
Status: completed
Approval:
- overall doc approved: yes
- current state: Series 5 and the approved concurrent-runtime-default follow-up
  are finished; connection admission concurrency execution complete
Completion:
- execution complete: yes
- completed series: Series 1, Series 2, Series 3, Series 4, Series 5
- completed follow-up: concurrent runtime default and fixed four-worker
  `nbd-server` Tokio startup policy
- next series: none; durable engine work should start a new design and
  execution artifact

## Goal

Implement the approved connection admission and concurrent export runtime
design in staged, reviewable checkpoints.

The target end state is:

- an explicit runtime reservation and completion boundary;
- a tested `ExportAdmissionCtl` primitive for read/write/flush ordering;
- a comprehensive userspace protocol integration suite that defines the
  transmission contract before the connection runtime is split;
- a `ConnectionRuntime` with independent request read and reply write paths;
- `MemoryExportEngine` storage that does not hide correctness behind an
  export-wide semantic mutex;
- an opt-in `ConcurrentExportRuntime` behind the existing runtime trait;
- userspace concurrent-runtime validation plus Docker kernel smoke regression.

## Design Inputs

- `docs/plans/2026-05-03-connection-admission-concurrency.md`
- `docs/plans/2026-05-03-concurrent-runtime-default.md`
  (post-completion follow-up)

## Why Split

This effort changes concurrency, lifecycle, protocol transmission flow, and
runtime shutdown behavior. Splitting at stable checkpoints keeps the series
reviewable and avoids validating the whole design only after every moving part
has changed.

The execution checkpoints are:

1. add a comprehensive userspace protocol integration baseline for the current
   serial server behavior;
2. establish live runtime API and admission primitives while the server remains
   on the serial runtime path;
3. split connection transmission into reader and writer tasks using the new
   runtime/completion boundary;
4. make admitted export requests the only memory-engine execution path and
   move memory synchronization behind `ExportAdmissionCtl`;
5. add the concurrent runtime, config wiring, multi-thread Tokio policy, and
   opt-in validation.

## Series 1: Userspace Protocol Integration Baseline

Depends on: none

Design coverage:
`docs/plans/2026-05-03-connection-admission-concurrency.md`

Stable checkpoint: the current serial server behavior is covered by a
comprehensive userspace protocol integration suite before runtime internals
change.

Review focus: NBD transmission contract coverage, userspace client harness
ergonomics, test determinism, malformed request/error coverage, and avoiding
test assertions that depend on current sequential implementation internals.

Done means: userspace integration tests cover handshake, `NBD_OPT_GO`,
unsupported options, read/write/flush/disconnect, out-of-bounds and malformed
requests, sequential visibility, payload sizing, cookie echoing, server cleanup
after disconnect or EOF, and a matrix of pipelined visibility scenarios whose
assertions are valid for both serial and future concurrent runtimes. The shared
harness exposes an engine-under-test profile so the same protocol scenarios can
run against memory now and durable later without duplicating the suite.

Approval: finished

Verification plan:

```text
make test-protocol
cargo fmt --all --check
```

Not included: runtime API changes, admission unit tests, connection
reader/writer split, connection reply queues, `ConcurrentExportRuntime`,
admitted memory-engine storage, multi-thread Tokio runtime wiring, durable
engine execution before a durable engine kind exists, userspace concurrent
smoke, or Docker smoke.

Commit 1/6: docs/plans: add connection concurrency design

  Type:             docs
  Required:         yes
  Summary:          Commit the approved design for ConnectionRuntime,
                    ExportAdmissionCtl, ConcurrentExportRuntime, queue-depth
                    slots, and memory-engine synchronization.
  Invariant focus:  Architecture intent is committed before concurrency-facing
                    code changes begin.
  Test level:       none
  Review gate:      structures
  Files:            docs/plans/2026-05-03-connection-admission-concurrency.md
  Preconditions:    The design has been approved in planning discussion.
  Postconditions:   The approved design doc is present in the repository with
                    Status: approved.
  Verify:           git diff --cached --check
  Risks:            Low; this is a planning-only commit.
  Not included:     Execution planning or implementation code.
  Depends on:       none

Commit 2/6: docs/execution: add connection concurrency plan

  Type:             docs
  Required:         yes
  Summary:          Add the execution artifact that splits the approved
                    concurrency design into stable implementation series and
                    defines the Series 1 protocol-baseline contract.
  Invariant focus:  Execution state is tracked separately from architecture
                    intent and the protocol baseline is established before
                    runtime internals change.
  Test level:       none
  Review gate:      structures
  Files:            docs/execution/2026-05-03-connection-admission-concurrency.md
  Preconditions:    Commit 1 has landed the approved design input.
  Postconditions:   Series boundaries, checkpoints, approval state, and the
                    Series 1 commit contract are explicit.
  Verify:           git diff --cached --check
  Risks:            Low; this is execution planning only, but future-series
                    boundaries should not be mistaken for implementation
                    approval.
  Not included:     Runtime, admission, connection, or memory-engine code.
  Depends on:       1

Commit 3/6: tests: add protocol integration harness

  Type:             preparatory
  Required:         yes
  Summary:          Introduce shared userspace TCP integration helpers for
                    starting a migrated server with an engine-under-test
                    profile, opening raw protocol connections, sending
                    explicit-cookie requests, reading replies, and invoking
                    the suite through `make test-protocol`.
  Invariant focus:  Protocol integration tests can exercise wire behavior
                    without depending on the high-level client enforcing
                    sequential request/reply order.
  Test level:       integration
  Review gate:      structures
  Files:            crates/nbd-server/tests/tcp_integration.rs
                    crates/nbd-server/tests/support/mod.rs (new)
                    crates/nbd-server/tests/support/nbd.rs (new)
                    Makefile
  Preconditions:    Commits 1 and 2 have landed the approved design and
                    execution contract.
  Postconditions:   Existing TCP integration tests run through shared setup
                    helpers, export creation does not hardcode
                    `ExportEngineKind::Memory` at every test callsite, and at
                    least one live test uses the raw protocol helper so the
                    harness is not dormant. Series 1 runs the memory profile;
                    adding durable later should be a profile/matrix expansion.
  Verify:           make test-protocol
  Risks:            The harness must stay deterministic and should not duplicate
                    protocol parser logic when existing nbd-protocol helpers can
                    parse replies.
  Not included:     New protocol coverage beyond the harness proof, connection
                    runtime changes, or runtime API changes.
  Depends on:       2

Commit 4/6: tests: cover negotiation edge cases

  Type:             semantic
  Required:         yes
  Summary:          Expand userspace integration coverage for handshake and
                    option haggling, including GO success, unknown/deleted
                    exports, unsupported options, abort, and active-export
                    policy errors.
  Invariant focus:  The server's negotiation contract is stable before
                    transmission runtime internals change.
  Test level:       integration
  Review gate:      code
  Files:            crates/nbd-server/tests/tcp_integration.rs
                    crates/nbd-server/tests/support/nbd.rs
  Preconditions:    Commit 3 has introduced the protocol integration harness.
  Postconditions:   Userspace integration tests cover the expected successful
                    and failing option negotiation paths without requiring
                    kernel NBD.
  Verify:           make test-protocol
  Risks:            Tests should assert protocol reply types and cleanup
                    behavior, not incidental error strings.
  Not included:     Transmission request edge cases or pipelined ordering
                    scenarios.
  Depends on:       3

Commit 5/6: tests: cover transmission edge cases

  Type:             semantic
  Required:         yes
  Summary:          Expand userspace transmission coverage for
                    read/write/flush/disconnect, out-of-bounds requests,
                    unsupported or malformed request shapes, payload sizing,
                    cookie echoing, and cleanup after EOF.
  Invariant focus:  The server preserves the documented NBD transmission
                    contract for valid requests and fails invalid requests
                    predictably.
  Test level:       integration
  Review gate:      code
  Files:            crates/nbd-server/tests/tcp_integration.rs
                    crates/nbd-server/tests/support/nbd.rs
  Preconditions:    Commit 4 has established the negotiation baseline.
  Postconditions:   Userspace integration tests cover transmission success and
                    error behavior through the wire protocol rather than only
                    through subsystem unit tests.
  Verify:           make test-protocol
  Risks:            Malformed-request tests must not bake in behavior the
                    protocol leaves flexible; when disconnect is valid, assert
                    clean cleanup rather than a specific internal error path.
  Not included:     Pipelined ordering and visibility scenarios that require
                    accepting out-of-order replies.
  Depends on:       4

Commit 6/6: tests: cover pipelined visibility semantics

  Type:             semantic
  Required:         yes
  Summary:          Add order-tolerant userspace tests for a matrix of
                    pipelined read/write/read, overlapping-write, flush, and
                    non-conflicting-read visibility scenarios.
  Invariant focus:  Tests assert required externally visible read/write/flush
                    semantics by cookie, not incidental serial completion order
                    for non-conflicting requests.
  Test level:       integration
  Review gate:      code
  Files:            crates/nbd-server/tests/tcp_integration.rs
                    crates/nbd-server/tests/support/nbd.rs
  Preconditions:    Commit 5 has established the broad transmission baseline and
                    raw reply collection helpers.
  Postconditions:   Pipelined scenarios validate by cookie and visible data:
                    read block 1, write block 1, read block 1, read block 2;
                    write block 1 value A, write block 1 value B, read block 1;
                    write block 1, flush, read block 1;
                    read block 1, write block 1, read block 2, read block 1;
                    and write block 1, read block 2, read block 1.
                    Non-conflicting reads may complete in any protocol-valid
                    order, while later conflicting reads must observe earlier
                    accepted writes. The scenarios are expressed through the
                    shared engine-under-test profile so durable can join the
                    same suite when the engine exists.
  Verify:           make test-protocol
  Risks:            The test must accept all protocol-correct reply orders so it
                    remains valid for both serial and future concurrent
                    runtimes.
  Not included:     ConnectionRuntime implementation changes, admission unit
                    tests, or concurrent runtime adoption.
  Depends on:       5

## Series 2: Runtime API And Admission Foundation

Depends on: Series 1

Design coverage:
`docs/plans/2026-05-03-connection-admission-concurrency.md`

Stable checkpoint: the current serial runtime path still works, but the code
has explicit queue slots, export completions, runtime close/drain semantics,
and a tested admission primitive ready for the concurrent runtime.

Review focus: runtime API shape, queue-slot ownership, admission state machine
liveness, and active export close/drain ownership.

Done means: serial runtime tests and local registry tests pass; admission tests
prove accepted-order conflicts, flush barriers, cancellation cleanup, and
no-lost-wake behavior. The Series 1 protocol integration suite continues to
pass.

Approval: finished

Verification plan:

```text
cargo test -p nbd-server --test export_runtime
cargo test -p nbd-server admission
cargo test -p nbd-server --test local_export_registry
make test-protocol
cargo fmt --all --check
```

Not included: connection reader/writer split, connection reply queues,
`ConcurrentExportRuntime`, admitted memory-engine storage, multi-thread Tokio
runtime wiring, userspace concurrent smoke, or Docker smoke.

Commit 1/5: docs/execution: plan runtime API series

  Type:             docs
  Required:         yes
  Summary:          Record the approved Series 2 commit contract for live
                    export queue slots, export completions, serial runtime
                    close/drain behavior, and the admission primitive.
  Invariant focus:  Series 2 scope is explicit before concurrency-facing
                    runtime API changes start.
  Test level:       none
  Review gate:      structures
  Files:            docs/execution/2026-05-03-connection-admission-concurrency.md
  Preconditions:    Series 1 is finished and the overall execution doc remains
                    approved.
  Postconditions:   The current execution artifact constrains the Series 2
                    implementation stack and keeps Series 3/4 work out of
                    scope.
  Verify:           git diff --cached --check
  Risks:            Low; this is execution planning only. It must not be
                    mistaken for implementation approval.
  Not included:     Runtime, admission, connection, or memory-engine code.
  Depends on:       Series 1

Commit 2/5: runtime: reserve export queue slots

  Type:             semantic
  Required:         yes
  Summary:          Add `ExportQueueSlot`, extend `ExportRuntime` with
                    `reserve`, and make `SerialExportRuntime` provide bounded
                    per-export queue-depth capacity through that API.
  Invariant focus:  Export queue depth is a runtime-owned reservation contract,
                    separate from admission and engine execution.
  Test level:       unit
  Review gate:      structures
  Files:            crates/nbd-server/src/runtime.rs
                    crates/nbd-server/src/lib.rs
                    crates/nbd-server/tests/export_runtime.rs
  Preconditions:    Commit 1 has landed the Series 2 execution contract.
  Postconditions:   Callers can reserve and drop queue slots explicitly; a
                    dropped slot releases capacity; queue-depth exhaustion
                    backpressures later reservations; the serial runtime still
                    executes existing jobs one at a time.
  Verify:           cargo test -p nbd-server --test export_runtime
  Risks:            The slot API must not conflate queue-depth permits with
                    admission permits or with the serial worker's internal
                    execution order.
  Not included:     Carrying slots through job completion, connection reply
                    queues, admission, close/drain, or concurrent execution.
  Depends on:       1

Commit 3/5: export: complete jobs with queue slots

  Type:             semantic
  Required:         yes
  Summary:          Replace the one-shot-only `ReplySink` with
                    `ExportCompletion`, make `ExportJob` carry an
                    `ExportQueueSlot`, and return a completed export envelope
                    that keeps the slot alive through the current serial wire
                    reply path.
  Invariant focus:  Accepted jobs own queue-depth slots until the request has a
                    completion owner; the export runtime and engine still do
                    not know about NBD cookies or socket writes.
  Test level:       integration
  Review gate:      code
  Files:            crates/nbd-server/src/export.rs
                    crates/nbd-server/src/runtime.rs
                    crates/nbd-server/src/connection.rs
                    crates/nbd-server/src/lib.rs
                    crates/nbd-server/tests/export_runtime.rs
                    crates/nbd-server/tests/tcp_integration.rs
  Preconditions:    Commit 2 has introduced live runtime queue-slot
                    reservation.
  Postconditions:   Current connection handling reserves a slot before
                    submission, builds jobs with that slot, receives exactly
                    one completion result per accepted job, and drops the slot
                    only after the existing sequential reply write completes
                    or the reply result is dropped. The completion target is
                    one-shot only in this series.
  Verify:           cargo test -p nbd-server --test export_runtime
                    make test-protocol
  Risks:            This touches the protocol execution path without yet
                    splitting reader and writer tasks, so the change must
                    preserve all Series 1 wire behavior.
  Not included:     `ConnectionReply`, bounded per-connection reply queues,
                    connection completion targets, admission, close/drain, or
                    concurrent execution.
  Depends on:       2

Commit 4/5: runtime: drain serial runtime on close

  Type:             semantic
  Required:         yes
  Summary:          Add `ExportRuntime.close`, make the serial runtime reject
                    new reservations/submissions after close starts, track
                    accepted jobs until completion handoff, and have
                    `LocalExportRegistry.close` await that drain before
                    removing the active export record.
  Invariant focus:  Local export close does not remove the active runtime while
                    accepted serial jobs can still mutate the engine.
  Test level:       functional
  Review gate:      structures
  Files:            crates/nbd-server/src/runtime.rs
                    crates/nbd-server/src/registry.rs
                    crates/nbd-server/src/error.rs
                    crates/nbd-server/tests/export_runtime.rs
                    crates/nbd-server/tests/local_export_registry.rs
  Preconditions:    Commit 3 has made accepted jobs and completion handoff
                    explicit.
  Postconditions:   Closing a local export transitions the runtime into a
                    closed state, later reservations and submissions fail with
                    `RuntimeClosed`, accepted jobs are allowed to hand off
                    exactly one completion, and registry removal happens only
                    after the runtime drain completes.
  Verify:           cargo test -p nbd-server --test export_runtime
                    cargo test -p nbd-server --test local_export_registry
                    make test-protocol
  Risks:            Close must not hold the registry mutex while waiting for
                    runtime drain, and runtime drain must account for every
                    accepted job on success, failure, and dropped-completion
                    paths.
  Not included:     Connection task join order, connection reply queue drain,
                    canceling in-flight connection replies, admission, or
                    concurrent runtime task tracking.
  Depends on:       3

Commit 5/5: admission: add export admission control

  Type:             semantic
  Required:         yes
  Summary:          Add `ExportAdmissionCtl` with accepted-order
                    read/write/flush permits, RAII permit release, waiter
                    cancellation cleanup, and state-based promotion tests.
  Invariant focus:  Admission order is assigned at registration, compatible
                    work can be admitted concurrently, conflicting work cannot
                    pass earlier conflicting waiters, and waiter grants cannot
                    be lost.
  Test level:       unit
  Review gate:      structures
  Files:            crates/nbd-server/src/admission.rs (new)
                    crates/nbd-server/src/lib.rs
                    crates/nbd-server/tests/admission.rs (new)
  Preconditions:    Commit 4 has established the runtime lifecycle foundation
                    that will later own admission.
  Postconditions:   Admission tests prove read/write conflicts,
                    non-overlapping compatibility, later reads waiting behind
                    earlier writes, flush barriers, waiter cancellation, permit
                    drop wakeups, and the read-registering-while-write-releases
                    no-lost-wake case.
  Verify:           cargo test -p nbd-server admission
                    cargo test -p nbd-server --test export_runtime
                    cargo test -p nbd-server --test local_export_registry
                    make test-protocol
                    cargo fmt --all --check
  Risks:            The O(n) queue must preserve accepted-order conflict
                    semantics without becoming strict FIFO for non-conflicting
                    ranges, and cancellation must not leave inert waiters that
                    block later work.
  Not included:     Wiring admission into `SerialExportRuntime`,
                    `ConcurrentExportRuntime`, admitted memory-engine storage,
                    resize admission, WAL ordering, or durable-engine read
                    views.
  Depends on:       4

## Series 3: Connection Runtime Split

Depends on: Series 2

Design coverage:
`docs/plans/2026-05-03-connection-admission-concurrency.md`

Stable checkpoint: transmission mode is owned by `ConnectionRuntime`; each
connection has one reader task and one reply writer task; export completions
flow through bounded per-connection reply queues; queue slots live until socket
write completion or reply drop.

Review focus: protocol ownership, reply serialization, queue-slot lifetime,
disconnect/error cleanup, and avoiding admission or engine work on the socket
read path.

Done means: protocol-level userspace tests prove pipelined request submission,
cookie-correct out-of-order completion handling, reply serialization, and
disconnect cleanup while the export runtime remains serial by default. The
Series 1 protocol integration suite continues to pass without weakening
coverage.

Approval: finished

Verification plan:

```text
make test-protocol
cargo test -p nbd-server connection_runtime
cargo test -p nbd-server connection::tests
cargo test -p nbd-server --test export_runtime
cargo fmt --all --check
```

Not included: `ConcurrentExportRuntime`, admitted memory-engine storage,
multi-thread Tokio runtime wiring, or concurrent-runtime smoke.

Closeout notes: review follow-up landed an explicit queue-slot lifetime fix for
socket writes and a cleanup that keeps connection-specific completion sinks out
of `export.rs`. `ExportCompletion` now targets an opaque completion sink;
`ConnectionRuntime` owns the sink that carries NBD cookies, reply kind, and the
bounded reply queue.

Commit 1/5: docs/execution: plan connection runtime series

  Type:             docs
  Required:         yes
  Summary:          Record the Series 3 commit contract for splitting
                    transmission mode into a connection request reader and
                    reply writer using the Series 2 runtime/completion
                    boundary.
  Invariant focus:  Series 3 scope is explicit before connection task
                    lifecycle and protocol ownership change.
  Test level:       none
  Review gate:      structures
  Files:            docs/execution/2026-05-03-connection-admission-concurrency.md
  Preconditions:    Series 2 is finished and its queue slot, completion, and
                    runtime close/drain contracts are committed.
  Postconditions:   The execution artifact constrains Series 3 implementation
                    and keeps concurrent runtime, admitted memory storage, and
                    Tokio worker-thread policy deferred.
  Verify:           git diff --cached --check
  Risks:            Low; this is execution planning only.
  Not included:     Connection, runtime, admission, or memory-engine code.
  Depends on:       Series 2

Commit 2/5: connection: model transmission replies

  Type:             preparatory
  Required:         yes
  Summary:          Introduce the connection reply envelope and reply-kind
                    helpers that map export results plus cookies into NBD wire
                    replies, then route the current sequential transmission
                    loop through that helper.
  Invariant focus:  Reply serialization has one explicit representation and
                    keeps the export queue slot alive until the write helper
                    finishes or drops the reply.
  Test level:       integration
  Review gate:      code
  Files:            crates/nbd-server/src/connection.rs
                    crates/nbd-server/tests/tcp_integration.rs
  Preconditions:    Commit 1 has landed the Series 3 execution contract and
                    Series 2 completions already return queue slots.
  Postconditions:   Existing sequential transmission behavior is unchanged,
                    but reads, writes, flushes, and protocol errors all flow
                    through a single connection reply encoding path.
  Verify:           make test-protocol
  Risks:            This touches wire reply mapping; tests must prove cookie
                    echoing, read payloads, simple replies, and existing error
                    behavior are preserved.
  Not included:     Connection reply queues, reader/writer task split,
                    connection completion targets, concurrent runtime, or
                    admitted memory storage.
  Depends on:       1

Commit 3/5: export: send completions to reply queues

  Type:             semantic
  Required:         yes
  Summary:          Extend `ExportCompletion` with a connection-backed target
                    that sends `ConnectionReply` values into a bounded reply
                    queue and make runtime completion handoff await that send.
  Invariant focus:  Export runtime completion still knows only the completion
                    target, while completed replies carry their queue slot
                    until the connection writer finishes or drops them.
  Test level:       unit
  Review gate:      structures
  Files:            crates/nbd-server/src/export.rs
                    crates/nbd-server/src/runtime.rs
                    crates/nbd-server/src/connection.rs
                    crates/nbd-server/tests/export_runtime.rs
  Preconditions:    Commit 2 has established the connection reply envelope used
                    by queued completions.
  Postconditions:   One-shot completions still work, connection completions can
                    hand off exactly one reply through a bounded queue, and a
                    full reply queue keeps the queue slot occupied until the
                    reply is received or dropped.
  Verify:           cargo test -p nbd-server --test export_runtime
                    make test-protocol
  Risks:            Awaiting reply queue handoff must not hold an admission
                    permit in future concurrent runtime work; Series 3 has no
                    admitted engine execution yet, but the boundary should not
                    make that mistake easy later.
  Not included:     Reader/writer task split, admission runtime wiring,
                    concurrent runtime, admitted memory storage, or config
                    knobs for reply queue capacity.
  Depends on:       2

Commit 4/5: connection: split transmission tasks

  Type:             semantic
  Required:         yes
  Summary:          Add `ConnectionRuntime` for transmission mode, split the
                    socket into a request reader and reply writer, reserve
                    queue slots before write payload buffering, and submit
                    jobs without waiting for export execution.
  Invariant focus:  Only the reply writer writes transmission replies, the
                    request reader does not wait for export completion, and
                    queue slots survive until socket write completion or reply
                    drop during shutdown.
  Test level:       integration
  Review gate:      code
  Files:            crates/nbd-server/src/connection.rs
                    crates/nbd-server/src/export.rs
                    crates/nbd-server/tests/tcp_integration.rs
  Preconditions:    Commit 3 has made connection completion handoff available
                    to export runtime workers.
  Postconditions:   `serve` runs transmission through `ConnectionRuntime`; the
                    request reader and reply writer are joined on disconnect,
                    EOF, or error; accepted work is drained or dropped through
                    the reply path; and the Series 1 protocol suite still
                    passes on the default serial runtime.
  Verify:           make test-protocol
                    cargo test -p nbd-server --test export_runtime
  Risks:            Shutdown ordering is the main risk: dropped readers,
                    pending completions, queued replies, and registry close
                    must release every queue slot without double-writing or
                    detaching accepted work.
  Not included:     `ConcurrentExportRuntime`, admission runtime wiring,
                    admitted memory storage, multi-thread Tokio runtime, or
                    making reply queue capacity configurable.
  Depends on:       3

Commit 5/5: tests: cover connection runtime pipelining

  Type:             semantic
  Required:         yes
  Summary:          Add targeted protocol/runtime tests that prove pipelined
                    request submission, cookie-correct out-of-order completion
                    handling, reply serialization, queue-depth backpressure,
                    and disconnect cleanup for `ConnectionRuntime`.
  Invariant focus:  The split connection path is validated by externally
                    visible protocol behavior and by a controllable runtime
                    that can complete accepted jobs out of order.
  Test level:       integration
  Review gate:      code
  Files:            crates/nbd-server/src/connection.rs
                    crates/nbd-server/tests/tcp_integration.rs
                    crates/nbd-server/tests/support/nbd.rs
  Preconditions:    Commit 4 has introduced the split connection runtime.
  Postconditions:   Tests prove the request reader can submit later requests
                    before earlier work completes, replies preserve NBD cookies
                    under out-of-order completion, one connection writer
                    serializes replies, queue-depth exhaustion backpressures
                    reads before write payload buffering, and disconnect/EOF
                    cleanup does not leak active export ownership. The
                    controllable runtime coverage may live in private
                    connection-module tests rather than exposing
                    `ConnectionRuntime` as public API.
  Verify:           make test-protocol
                    cargo test -p nbd-server connection_runtime
                    cargo test -p nbd-server --test export_runtime
                    cargo fmt --all --check
  Risks:            The controllable runtime harness must assert protocol
                    behavior rather than implementation scheduling details.
                    This standalone test commit is justified because the
                    default serial runtime cannot produce out-of-order
                    completions by itself.
  Not included:     Concurrent runtime implementation, admission runtime
                    wiring, admitted memory storage, Docker smoke, or making
                    concurrent runtime the default.
  Depends on:       4

## Series 4: Admission-Backed Memory Boundary

Depends on: Series 3

Design coverage:
`docs/plans/2026-05-03-connection-admission-concurrency.md`

Stable checkpoint: every `ExportEngine` execution goes through
`AdmittedExportRequest`, `SerialExportRuntime` acquires
`ExportAdmissionCtl` permits before engine execution, and
`ExportAdmissionCtl` owns active extent validation before admitted execution.
`MemoryExportEngine` storage safety no longer depends on an export-wide
semantic mutex or any safe raw `ExportRequest` execution path.

Review focus: admitted request API shape, removal of safe engine bypasses,
admission extent validation, serial runtime admission wiring, unsafe memory
boundary size, range coverage for every byte the memory engine may touch, and
tests that prove admission rather than memory locking owns read/write/flush
correctness.

Done means: the memory engine cannot be safely called with a bare
`ExportRequest`, out-of-bounds admission operations fail before receiving
tickets, direct admitted memory tests prove compatible operations can overlap
while conflicting operations and flush barriers are ordered by admission,
serial runtime and protocol tests still pass through the admitted engine path,
and Docker kernel smoke still passes on the default serial path.

Approval: finished

Verification plan:

```text
cargo test -p nbd-server admission
cargo test -p nbd-server --test export_runtime
cargo test -p nbd-server --test memory_export
make test-protocol
cargo test --workspace
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
make docker-smoke
```

Not included: `ConcurrentExportRuntime`, runtime config rollout,
multi-thread Tokio policy, `DurableExportEngine`, WAL, read views, storage
work queues, cross-process serving leases, authentication/client identity, or
advertising `NBD_FLAG_CAN_MULTI_CONN`.

Commit 1/5: docs/plans: revise admitted memory boundary

  Type:             docs
  Required:         yes
  Summary:          Commit the approved design and execution deltas that split
                    admitted memory from concurrent scheduling and align the
                    architecture docs with the admitted engine boundary.
  Invariant focus:  Series 4 owns the admitted memory safety boundary; Series
                    5 owns concurrent runtime scheduling and config rollout.
  Test level:       none
  Review gate:      structures
  Files:            docs/plans/2026-05-03-connection-admission-concurrency.md
                    docs/execution/2026-05-03-connection-admission-concurrency.md
                    docs/architecture/export-admission-control.md
                    docs/architecture/local-export-registry-architecture.md
                    docs/architecture/nbd-protocol-architecture.md
                    docs/architecture/nbd-s3-long-term-architecture.md
  Preconditions:    Series 3 is finished and the revised Series 4/5 split has
                    been accepted for execution planning.
  Postconditions:   The active docs contain no engine-guard API path, Series 4
                    is scoped to admitted memory, Series 5 is scoped to
                    concurrent runtime rollout, and architecture docs match
                    the single-domain and future `(owner, export)` stance.
  Verify:           git diff --cached --check
  Risks:            Low; this is planning and architecture alignment only.
  Not included:     Any runtime, engine, memory storage, or test code changes.
  Depends on:       Series 3

Commit 2/5: admission: validate ranges against extent

  Type:             semantic
  Required:         yes
  Summary:          Add active extent size to `ExportAdmissionCtl` and reject
                    read/write admission operations whose ranges overflow or
                    extend past that extent before assigning tickets.
  Invariant focus:  Admission tickets are assigned only to operations within
                    the current active export extent; future resize has a
                    single admission-owned extent update point.
  Test level:       unit
  Review gate:      structures
  Files:            crates/nbd-server/src/admission.rs
                    crates/nbd-server/tests/admission.rs
  Preconditions:    Commit 1 has landed the approved Series 4 execution
                    contract; current admission tests cover ordering and
                    waiter liveness without extent validation.
  Postconditions:   `ExportAdmissionCtl` is constructed with an active extent,
                    range operations outside that extent fail without tickets
                    or waiters, and existing admission ordering tests still
                    pass inside the extent.
  Verify:           cargo test -p nbd-server admission
  Risks:            Existing callers and tests must not silently fall back to
                    an unbounded admission controller.
  Not included:     Resize implementation, admitted engine execution, memory
                    storage changes, or concurrent runtime scheduling.
  Depends on:       1

Commit 3/5: export: require admitted engine execution

  Type:             semantic
  Required:         yes
  Summary:          Add `ExportAdmissionProfile` and
                    `AdmittedExportRequest`, replace
                    `ExportEngine::execute` with `execute_admitted`, and make
                    `SerialExportRuntime` acquire profile-derived admission
                    before engine execution.
  Invariant focus:  Engine execution receives a real `AdmissionPermit` for the
                    active profile's storage-touch operation; serial execution
                    is stricter than admission but is not a bypass around the
                    admitted engine contract.
  Test level:       integration
  Review gate:      structures
  Files:            crates/nbd-server/src/export.rs
                    crates/nbd-server/src/runtime.rs
                    crates/nbd-server/src/memory.rs
                    crates/nbd-server/src/lib.rs
                    crates/nbd-server/src/connection.rs
                    crates/nbd-server/tests/export_runtime.rs
                    crates/nbd-server/tests/memory_export.rs
  Preconditions:    Commit 2 has made `ExportAdmissionCtl` own active extent
                    validation and already provides tested permits.
  Postconditions:   All runtime-driven engine calls ask the active admission
                    profile for an `AdmissionOp`, construct an admitted
                    request from the original request and permit, and preserve
                    externally visible serial runtime behavior.
  Verify:           cargo test -p nbd-server admission
                    cargo test -p nbd-server --test export_runtime
                    cargo test -p nbd-server connection::tests
                    make test-protocol
  Risks:            This changes the core engine trait and every test engine;
                    reviewers should confirm profile-derived admission ranges
                    cover the full storage-touch range, permits are held for
                    the full engine execution, and permits drop before
                    completion handoff can wait on a reply queue.
  Not included:     Removing direct memory read/write bypasses, unsafe memory
                    storage, `ConcurrentExportRuntime`, or config rollout.
  Depends on:       2

Commit 4/5: memory: remove raw access bypasses

  Type:             semantic
  Required:         yes
  Summary:          Remove the public direct memory access path and make
                    memory read/write helpers private implementation details
                    behind admitted engine execution.
  Invariant focus:  Safe callers cannot observe or mutate memory storage with
                    a bare `ExportRequest` or direct memory export API.
  Test level:       integration
  Review gate:      code
  Files:            crates/nbd-server/src/export.rs
                    crates/nbd-server/src/memory.rs
                    crates/nbd-server/src/lib.rs
                    crates/nbd-server/tests/memory_export.rs
                    crates/nbd-server/tests/export_runtime.rs
  Preconditions:    Commit 3 has made the runtime and engine path use
                    `AdmittedExportRequest`.
  Postconditions:   External tests and callers exercise memory behavior
                    through runtime/admitted execution; the old direct
                    `Export`/`ExportHandle` memory path is gone or no longer
                    exposes storage mutation.
  Verify:           cargo test -p nbd-server --test memory_export
                    cargo test -p nbd-server --test export_runtime
                    make test-protocol
  Risks:            This is an API cleanup inside the crate boundary; the main
                    risk is leaving a convenient public helper that still
                    bypasses admission.
  Not included:     Removing the memory storage mutex, adding unsafe storage,
                    or introducing concurrent runtime scheduling.
  Depends on:       3

Commit 5/5: memory: use admitted unsafe storage

  Type:             semantic
  Required:         yes
  Summary:          Replace the memory engine's export-wide storage mutex with
                    a small audited unsafe storage boundary reachable only
                    through `AdmittedExportRequest`.
  Invariant focus:  `ExportAdmissionCtl` owns read/write/flush correctness;
                    memory storage trusts the `MemoryAdmissionProfile` and
                    admitted request permit to exclude overlapping storage
                    touches.
  Test level:       functional
  Review gate:      code
  Files:            crates/nbd-server/src/lib.rs
                    crates/nbd-server/src/memory.rs
                    crates/nbd-server/tests/memory_export.rs
                    crates/nbd-server/tests/export_runtime.rs
  Preconditions:    Commit 4 has removed safe raw memory bypasses, so unsafe
                    storage cannot be reached without admitted execution.
  Postconditions:   `MemoryExportEngine` no longer uses an export-wide
                    semantic `Mutex<Vec<u8>>`; tests prove admitted
                    non-overlapping operations can coexist, overlapping
                    operations are blocked by admission, read-after-write
                    visibility holds, and flush remains ordered.
  Verify:           cargo test -p nbd-server admission
                    cargo test -p nbd-server --test memory_export
                    cargo test -p nbd-server --test export_runtime
                    make test-protocol
                    cargo test --workspace
                    cargo fmt --all --check
                    cargo clippy --workspace --all-targets -- -D warnings
                    make docker-smoke
  Risks:            High-risk correctness boundary: unsafe code must stay
                    small, documented, and tied to exact
                    `MemoryAdmissionProfile` byte ranges. Any future cacheline
                    or word expansion must update that profile in the same
                    change.
  Not included:     `ConcurrentExportRuntime`, Tokio worker-thread changes,
                    runtime config rollout, durable storage, WAL, or
                    advertising `NBD_FLAG_CAN_MULTI_CONN`.
  Depends on:       4

## Series 5: Concurrent Runtime And Config Rollout

Depends on: Series 4

Design coverage:
`docs/plans/2026-05-03-connection-admission-concurrency.md`

Stable checkpoint: `ConcurrentExportRuntime` is opt-in through process config,
uses the admitted engine boundary established in Series 4, registers
admission before spawning request tasks, shares queue depth across same-owner
opens inside the Series 5 single-domain model, tracks runtime close/drain, and
runs under a Tokio runtime policy capable of real parallel request execution.
Production protocol multi-connection remains disabled until a same-client
identity policy can decide when multiple sockets belong to the same
`(owner, export)` serving and backing-store domain.

Review focus: admission registration before spawn, task lifecycle tracking,
waiting-admission cancellation, flush/read/write ordering, queue-depth
backpressure through reply write/drop, same-owner ordering inside the current
single-domain model, future `(owner, export)` domain compatibility, config
rollout, and Tokio multi-thread startup policy.

Done means: concurrent runtime tests prove compatible operations can overlap,
conflicting operations and flush barriers are ordered by admission, queue depth
is held through reply write/drop, close/drain settles waiting and executing
jobs without detached mutations, config can opt newly opened exports into the
concurrent runtime with nonzero queue sizing, userspace concurrent smoke
passes, and Docker kernel smoke still passes on the default serial path.

Approval: finished

Verification plan:

```text
cargo test -p nbd-server admission
cargo test -p nbd-server --test export_runtime
make test-protocol
cargo test --workspace
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
make docker-smoke
```

Not included: `DurableExportEngine`, WAL, read views, storage work queues,
cross-process serving leases, authentication/client identity, advertising
`NBD_FLAG_CAN_MULTI_CONN`, dynamic Tokio worker-thread sizing from config, or
making the concurrent runtime the default.

Closeout notes: review found no blocking findings. Verification passed:
`make test-protocol`, `cargo test --workspace`, `cargo fmt --all --check`,
`cargo clippy --workspace --all-targets -- -D warnings`, and
`make docker-smoke`. Docker smoke remained on the default serial kernel path
and confirmed mounted NBD write/readback. Concurrent runtime remains opt-in;
durable storage, real client identity for production multi-connection,
concurrent kernel smoke, dynamic Tokio worker sizing, and making concurrent
runtime the default are deferred to future design and execution work.

Post-completion follow-up: the approved concurrent-runtime-default design
landed in commits `9994e89` through `41d69c0`. The follow-up pinned the
`nbd-server` binary to four Tokio worker threads, added explicit serial
registry and userspace protocol coverage, and changed missing
`server.export_runtime` config to select `ConcurrentExportRuntime`. Review
found no blocking findings. Verification passed: `cargo test -p nbd-config`,
`cargo test -p nbd-server --test local_export_registry`,
`make test-protocol`, `cargo fmt --all --check`, `cargo test --workspace`,
`cargo clippy --workspace --all-targets -- -D warnings`, and
`make docker-smoke`. Docker smoke now remains config-minimal and validates the
default concurrent kernel path by mounting, writing through NBD, dropping
caches, and reading the probe file back.

Commit 1/7: docs/execution: plan concurrent runtime series

  Type:             docs
  Required:         yes
  Summary:          Record the approved Series 5 commit contract and align
                    active design and architecture docs around the current
                    single-domain model and future `(owner, export)` domain
                    key.
  Invariant focus:  Series 5 begins with the execution contract and domain
                    identity model committed before runtime rollout code
                    changes start.
  Test level:       none
  Review gate:      structures
  Files:            docs/plans/2026-05-03-connection-admission-concurrency.md
                    docs/execution/2026-05-03-connection-admission-concurrency.md
                    docs/architecture/local-export-registry-architecture.md
                    docs/architecture/nbd-protocol-architecture.md
  Preconditions:    Series 4 is finished and the Series 5 design review has
                    accepted the single-domain now, `(owner, export)` later
                    boundary.
  Postconditions:   The active docs state that Series 5 keeps production
                    protocol multi-connection disabled, tests same-owner
                    sharing only below the protocol layer, and treats
                    `(owner, export)` as the future serving and backing-store
                    namespace.
  Verify:           git diff --cached --check
  Risks:            Low; this is planning and architecture alignment only.
  Not included:     Runtime, config, registry, server startup, or protocol test
                    code changes.
  Depends on:       Series 4

Commit 2/7: config: apply runtime queue sizing

  Type:             semantic
  Required:         yes
  Summary:          Add nonzero server queue-depth and connection reply
                    capacity settings, preserve the existing serial default,
                    and apply the settings to the live serial runtime and
                    connection reply queue paths.
  Invariant focus:  Queue depth and reply queue capacity are process-local
                    runtime policy for newly opened exports and connections,
                    not catalog metadata or durable export state.
  Test level:       integration
  Review gate:      code
  Files:            crates/nbd-config/src/lib.rs
                    crates/nbd-config/tests/config_loading.rs
                    crates/nbd-server/src/connection.rs
                    crates/nbd-server/src/registry.rs
                    crates/nbd-server/src/server.rs
                    crates/nbd-server/tests/local_export_registry.rs
  Preconditions:    Commit 1 has landed the Series 5 execution contract.
  Postconditions:   Default config still yields serial runtime behavior with
                    nonzero default queue sizing; explicit config can set
                    queue and reply capacities; serial exports opened after
                    config load use the configured export queue depth; new
                    connections use the configured reply queue capacity.
  Verify:           cargo test -p nbd-config
                    cargo test -p nbd-server --test local_export_registry
                    cargo test -p nbd-server connection::tests
                    make test-protocol
  Risks:            This touches live config and connection construction, so
                    defaults and explicit config parsing must remain backward
                    compatible.
  Not included:     `ExportRuntimeKind::Concurrent`,
                    `ConcurrentExportRuntime`, Tokio multi-thread startup, or
                    concurrent userspace smoke.
  Depends on:       1

Commit 3/7: runtime: share admitted job execution

  Type:             preparatory
  Required:         yes
  Summary:          Extract the serial worker's admission/profile/engine
                    execution path into a small helper that is immediately used
                    by `SerialExportRuntime`.
  Invariant focus:  Serial execution still registers admission before engine
                    access and completes exactly one result per accepted job;
                    the helper is live in the same commit that introduces it.
  Test level:       functional
  Review gate:      code
  Files:            crates/nbd-server/src/runtime.rs
                    crates/nbd-server/tests/export_runtime.rs
  Preconditions:    Commit 2 has preserved serial runtime behavior under live
                    queue sizing.
  Postconditions:   `SerialExportRuntime` behavior is unchanged, but the
                    admitted job execution sequence is factored for reuse by
                    the concurrent runtime without adding a dormant API.
  Verify:           cargo test -p nbd-server --test export_runtime
                    make test-protocol
  Risks:            The refactor must not accidentally change when admission
                    permits are held or when queue slots move into completion.
  Not included:     Concurrent spawning, runtime config selection, task
                    lifecycle changes, or Tokio runtime changes.
  Depends on:       2

Commit 4/7: runtime: add concurrent export runtime

  Type:             semantic
  Required:         yes
  Summary:          Add `ConcurrentExportRuntime` behind the existing
                    `ExportRuntime` trait, with queue-depth-bounded request
                    tasks, admitted execution, lifecycle close/drain tracking,
                    and runtime tests.
  Invariant focus:  Accepted concurrent jobs are tracked until one completion
                    is handed off; admission order is assigned before spawn;
                    compatible work may overlap while conflicting work and
                    flushes obey `ExportAdmissionCtl`.
  Test level:       functional
  Review gate:      structures
  Files:            crates/nbd-server/src/runtime.rs
                    crates/nbd-server/src/lib.rs
                    crates/nbd-server/tests/export_runtime.rs
  Preconditions:    Commit 3 has a shared live admitted execution path used by
                    the serial runtime.
  Postconditions:   `ConcurrentExportRuntime` can be constructed directly in
                    tests, `reserve` enforces queue depth, `submit` returns
                    after acceptance, compatible requests can execute
                    concurrently, conflicting requests and flushes are ordered
                    by admission, close rejects new work, and accepted waiting
                    or executing jobs drain without detached mutations.
  Verify:           cargo test -p nbd-server --test export_runtime
                    cargo test -p nbd-server admission
  Risks:            Task lifecycle and close/drain accounting are the main
                    correctness risks; dropped waiters and post-acceptance
                    admission errors must still produce exactly one
                    completion.
  Not included:     Process config selection, registry rollout, Tokio
                    multi-thread startup, protocol multi-connection, or
                    userspace concurrent smoke.
  Depends on:       3

Commit 5/7: server: run on multi-thread Tokio

  Type:             semantic
  Required:         yes
  Summary:          Enable Tokio's normal `rt-multi-thread` feature for the
                    server crate and switch the server binary startup to the
                    multi-thread runtime policy.
  Invariant focus:  The server binary can execute spawned export request tasks
                    on more than one Tokio worker before concurrent runtime is
                    exposed through config.
  Test level:       functional
  Review gate:      code
  Files:            crates/nbd-server/Cargo.toml
                    crates/nbd-server/src/main.rs
  Preconditions:    Commit 4 has introduced the concurrent runtime primitive,
                    but production config does not select it yet.
  Postconditions:   The `nbd-server` binary starts under Tokio's multi-thread
                    scheduler; dynamic worker-thread count remains deferred and
                    the default runtime kind remains serial.
  Verify:           cargo test -p nbd-server --bin nbd-server
                    cargo clippy -p nbd-server --bin nbd-server -- -D warnings
  Risks:            This is a startup policy change; it must not pretend that
                    worker-thread count is dynamically configurable from the
                    loaded server config.
  Not included:     `tokio_worker_threads` config, concurrent runtime
                    selection, or making concurrent runtime the default.
  Depends on:       4

Commit 6/7: registry: opt into concurrent runtime

  Type:             semantic
  Required:         yes
  Summary:          Add `ExportRuntimeKind::Concurrent`, wire
                    `LocalExportRegistry` to construct the concurrent runtime
                    when configured, and test same-owner sharing below the
                    protocol layer.
  Invariant focus:  Runtime kind is process-local policy for newly opened
                    exports; serial remains the default; same-owner opens
                    share one active runtime and one queue-depth domain.
  Test level:       integration
  Review gate:      migration
  Files:            crates/nbd-config/src/lib.rs
                    crates/nbd-config/tests/config_loading.rs
                    crates/nbd-server/src/registry.rs
                    crates/nbd-server/tests/local_export_registry.rs
  Preconditions:    Commit 5 has made the server binary capable of real Tokio
                    parallelism before config can expose the concurrent
                    runtime.
  Postconditions:   Config parsing accepts `export_runtime = "concurrent"`;
                    default config still selects serial; newly opened exports
                    choose serial or concurrent runtime from process config;
                    same-owner registry opens share the configured active
                    runtime; different synthetic owners remain rejected.
  Verify:           cargo test -p nbd-config
                    cargo test -p nbd-server --test local_export_registry
                    cargo test -p nbd-server --test export_runtime
  Risks:            This is the public opt-in cutover. It must not advertise
                    NBD multi-connection or let different synthetic owners
                    create independent serving domains for one export.
  Not included:     Concurrent protocol smoke, durable storage, production
                    client identity, or changing the default runtime kind.
  Depends on:       5

Commit 7/7: tests: smoke concurrent protocol path

  Type:             semantic
  Required:         yes
  Summary:          Extend the userspace protocol harness to start the server
                    with concurrent runtime config and add an opt-in
                    end-to-end smoke that exercises pipelined transmission
                    through the concurrent path.
  Invariant focus:  The concurrent runtime is validated through the same NBD
                    userspace protocol path as the serial baseline while Docker
                    smoke continues to prove the default serial kernel path.
  Test level:       integration
  Review gate:      code
  Files:            crates/nbd-test-support/src/lib.rs
                    crates/nbd-server/tests/support/nbd.rs
                    crates/nbd-server/tests/tcp_integration.rs
  Preconditions:    Commit 6 has exposed the concurrent runtime through
                    process config while keeping production protocol
                    multi-connection disabled.
  Postconditions:   `make test-protocol` includes at least one concurrent
                    runtime userspace scenario that sends pipelined requests
                    and validates replies by cookie and visible data; existing
                    serial protocol scenarios still pass; Docker smoke remains
                    on the default serial path.
  Verify:           make test-protocol
                    cargo test --workspace
                    cargo fmt --all --check
                    cargo clippy --workspace --all-targets -- -D warnings
                    make docker-smoke
  Risks:            The smoke must prove the config-selected concurrent path is
                    actually used, not merely rerun the default serial fixture.
                    It must also keep order-tolerant assertions for
                    non-conflicting pipelined replies.
  Not included:     A new make target, kernel concurrent smoke, production
                    multi-connection advertisement, durable engine coverage,
                    or making concurrent runtime the default.
  Depends on:       6
