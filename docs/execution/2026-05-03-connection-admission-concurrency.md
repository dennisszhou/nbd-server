Title: Connection Admission Concurrency Execution
Date: 2026-05-03
Status: approved
Approval:
- overall doc approved: yes
- current state: Series 2 finished; Series 3 approved for implementation
Completion:
- execution complete: no
- completed series: Series 1, Series 2
- next series: Series 3 in progress

## Goal

Implement the approved connection admission and concurrent export runtime
design in staged, reviewable checkpoints.

The target end state is:

- an explicit runtime reservation and completion boundary;
- a tested `ExportAdmissionCtl` primitive for read/write/flush ordering;
- a comprehensive userspace protocol integration suite that defines the
  transmission contract before the connection runtime is split;
- a `ConnectionRuntime` with independent request read and reply write paths;
- an opt-in `ConcurrentExportRuntime` behind the existing runtime trait;
- `MemoryExportEngine` storage that does not hide correctness behind an
  export-wide semantic mutex;
- userspace concurrent-runtime validation plus Docker kernel smoke regression.

## Design Inputs

- `docs/plans/2026-05-03-connection-admission-concurrency.md`

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
4. add the concurrent runtime, memory-engine synchronization change, config
   wiring, and opt-in validation.

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

Approval: approved

Verification plan:

```text
make test-protocol
cargo test -p nbd-server connection_runtime
cargo test -p nbd-server --test export_runtime
cargo fmt --all --check
```

Not included: `ConcurrentExportRuntime`, admitted memory-engine storage,
multi-thread Tokio runtime wiring, or concurrent-runtime smoke.

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

## Series 4: Concurrent Runtime And Memory Synchronization

Depends on: Series 3

Design coverage:
`docs/plans/2026-05-03-connection-admission-concurrency.md`

Stable checkpoint: `ConcurrentExportRuntime` is opt-in through process config,
uses `ExportAdmissionCtl`, shares queue depth across active connections for one
export, tracks runtime close/drain, and runs against a memory engine whose
storage safety no longer depends on an export-wide semantic mutex.

Review focus: admission registration before spawn, task lifecycle tracking,
flush/read/write ordering, queue-depth backpressure, memory-engine data-race
safety, config rollout, and Tokio runtime worker-thread policy.

Done means: concurrent runtime tests prove compatible operations can overlap,
conflicting operations and flush barriers are ordered by admission, queue depth
is held through reply write/drop, userspace concurrent smoke passes, and Docker
kernel smoke still passes on the default serial path.

Approval: pending

Verification plan:

```text
cargo test -p nbd-server --test export_runtime
cargo test -p nbd-server --test tcp_integration
cargo test --workspace
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
make docker-smoke
```

Not included: `DurableExportEngine`, WAL, read views, storage work queues,
cross-process serving leases, authenticated multi-connection support, or making
the concurrent runtime the default.
