Title: Connection Admission Concurrency Execution
Date: 2026-05-03
Status: approved
Approval:
- overall doc approved: yes
- current state: Series 1 approved for implementation
Completion:
- execution complete: no

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

Approval: approved

Verification plan:

```text
make test-protocol
cargo fmt --all --check
```

Not included: runtime API changes, admission unit tests, connection
reader/writer split, connection reply queues, `ConcurrentExportRuntime`,
memory-engine atomic storage, multi-thread Tokio runtime wiring, durable engine
execution before a durable engine kind exists, userspace concurrent smoke, or
Docker smoke.

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

Approval: pending

Verification plan:

```text
cargo test -p nbd-server --test export_runtime
cargo test -p nbd-server admission
cargo test -p nbd-server --test local_export_registry
cargo test -p nbd-server --test tcp_integration
cargo fmt --all --check
```

Not included: connection reader/writer split, connection reply queues,
`ConcurrentExportRuntime`, memory-engine atomic storage, multi-thread Tokio
runtime wiring, userspace concurrent smoke, or Docker smoke.

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

Approval: pending

Verification plan:

```text
cargo test -p nbd-server --test tcp_integration
cargo test -p nbd-server --test export_runtime
cargo fmt --all --check
```

Not included: `ConcurrentExportRuntime`, memory-engine atomic storage,
multi-thread Tokio runtime wiring, or concurrent-runtime smoke.

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
