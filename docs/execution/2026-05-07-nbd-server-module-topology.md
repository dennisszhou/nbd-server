Title: NBD Server Module Topology Execution
Date: 2026-05-07
Status: approved
Approval:
- overall doc approved: yes
- current state: Series 1 approved
Completion:
- execution complete: no

## Goal

Reorganize `crates/nbd-server/src` around the approved module-topology design
without changing NBD protocol behavior, export behavior, catalog schema,
storage layout, WAL format, compaction behavior, config shape, or operator
commands.

The end state should make the request path and ownership boundaries explicit:

```text
connection -> export runtime/admission -> engines -> storage and/or WAL
registry -> catalog + engines + storage + WAL + runtime policy
```

## Design Inputs

- `docs/plans/2026-05-07-nbd-server-module-topology.md`

## Why One Series

The topology cleanup is broad, but each step is a self-contained ownership or
move boundary with a stable intermediate build. Keeping the work in one series
preserves review of the final dependency direction while still using small
commits for bisectability.

This remains a durable execution artifact because the stack is long and may
span more than one implementation session. The single series is approved or
revised as one execution contract.

## Series 1: NBD Server Module Topology

Depends on: none

Design coverage: implements the approved source-tree topology end to end:
shared range ownership, server-local request identity, connection submodules,
export contract submodules, registry orchestration submodules, engine
submodules, and WAL provider/backend submodules.

Stable checkpoint: `nbd-server/src` follows the approved topology, root public
re-exports preserve compatibility, `nbd_protocol` types stay at the connection
boundary, `export/` defines request/runtime/admission contracts, `registry/`
composes concrete engines and shared infrastructure, concrete engines sit
under `engines/`, and WAL provider internals sit under `wal/`.

Review focus: dependency direction, Rust visibility, move-only correctness,
request cookie conversion, queue-slot lifetime, unsafe memory isolation,
read-view authoritative/cache separation, and WAL format compatibility.

Done means: all commits in this series are landed and the final verification
plan passes.

Approval: approved

Implementation approval was recorded before implementation started.

Verification plan:

```text
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
make test-protocol
```

Not included: S3 storage, storage worker queues, storage deletion or GC,
leases, auth, multi-connection serving semantics, WAL format changes, remote
WAL providers, or behavior changes outside the approved topology cleanup.

### Current-Series Commit Plan

```text
Commit 1/19: docs/plans: add nbd-server topology design

  Type:             docs
  Required:         yes
  Summary:          Add the approved module-topology design as the architecture
                    source of truth for the nbd-server source cleanup.
  Invariant focus:  Implementation proceeds from the approved module owners and
                    dependency boundaries rather than from chat-only agreement.
  Test level:       none
  Review gate:      structures
  Files:            docs/plans/2026-05-07-nbd-server-module-topology.md
  Preconditions:    The design has passed review-plan with result ready for
                    series planning.
  Postconditions:   The approved topology design is present in the repository
                    and can be referenced by later execution commits.
  Verify:           awk 'length($0) > 80 { print FNR ":" length($0) ":" $0 }'
                    docs/plans/2026-05-07-nbd-server-module-topology.md
  Risks:            low
  Not included:     No implementation files or execution approval state are
                    changed.
  Depends on:       none
```

```text
Commit 2/19: docs/execution: add topology execution plan

  Type:             docs
  Required:         yes
  Summary:          Add the durable single-series execution contract for the
                    approved topology cleanup.
  Invariant focus:  Execution has one durable source of truth with explicit
                    commit boundaries, approval state, and completion state.
  Test level:       none
  Review gate:      structures
  Files:            docs/execution/2026-05-07-nbd-server-module-topology.md
  Preconditions:    Commit 1 has made the approved design available as the
                    planning input.
  Postconditions:   The one-series execution contract exists with implementation
                    approval recorded.
  Verify:           awk 'length($0) > 80 { print FNR ":" length($0) ":" $0 }'
                    docs/execution/2026-05-07-nbd-server-module-topology.md
  Risks:            low
  Not included:     No code movement starts in the execution-plan commit.
  Depends on:       Commit 1
```

```text
Commit 3/19: range: move byte range primitive

  Type:             preparatory
  Required:         yes
  Summary:          Move ByteRange out of admission into a shared range module
                    while preserving the existing root re-export and behavior.
  Invariant focus:  Logical byte ranges are a shared primitive and do not imply
                    a dependency on export admission policy.
  Test level:       none
  Review gate:      structures
  Files:            crates/nbd-server/src/range.rs (new)
                    crates/nbd-server/src/admission.rs
                    crates/nbd-server/src/lib.rs
  Preconditions:    The approved design and execution plan are committed.
  Postconditions:   ByteRange is defined in range.rs, admission imports it from
                    the shared owner, and existing callers continue to compile
                    through the root re-export.
  Verify:           cargo test -p nbd-server --lib
  Risks:            Low; this should be a move-only ownership change with no
                    range semantics change.
  Not included:     No admission scheduling, validation, WAL, or engine behavior
                    changes.
  Depends on:       Commit 2
```

```text
Commit 4/19: export: own request context

  Type:             preparatory
  Required:         yes
  Summary:          Move ExportJobContext from observability into the export
                    boundary while leaving observability as a consumer.
  Invariant focus:  Request identity and request execution context belong to the
                    export contract, not to the logging subsystem.
  Test level:       none
  Review gate:      structures
  Files:            crates/nbd-server/src/export.rs
                    crates/nbd-server/src/observability.rs
                    crates/nbd-server/src/lib.rs
  Preconditions:    Commit 3 has established the shared range owner and the
                    crate still builds.
  Postconditions:   ExportJobContext is owned by export.rs, observability
                    imports it, and root re-exports preserve existing external
                    imports.
  Verify:           cargo test -p nbd-server --lib
  Risks:            Moderate review risk because observability and runtime
                    imports change, but behavior should remain unchanged.
  Not included:     No protocol cookie conversion, connection split, or export
                    directory split.
  Depends on:       Commit 3
```

```text
Commit 5/19: connection: localize request cookies

  Type:             semantic
  Required:         yes
  Summary:          Introduce RequestCookie at the export boundary and convert
                    to and from NBD cookies only in the connection path.
  Invariant focus:  NBD wire cookie types do not leak into export runtime,
                    engine, or observability request-context APIs.
  Test level:       integration
  Review gate:      code
  Files:            crates/nbd-server/src/connection.rs
                    crates/nbd-server/src/export.rs
                    crates/nbd-server/src/observability.rs
                    crates/nbd-server/src/lib.rs
                    crates/nbd-server/src/memory.rs
  Preconditions:    Commit 4 has moved ExportJobContext to the export boundary.
  Postconditions:   ExportJobContext stores RequestCookie, export-facing code no
                    longer imports nbd_protocol::NbdCookie, and TCP protocol
                    tests still observe the original reply cookies.
  Verify:           cargo test -p nbd-server --test tcp_integration
                    make test-protocol
  Risks:            Cookie conversion mistakes would break reply correlation, so
                    this commit needs protocol-level verification.
  Not included:     No connection file split, no option negotiation changes, and
                    no reply ordering changes.
  Depends on:       Commit 4
```

```text
Commit 6/19: connection: create module shell

  Type:             preparatory
  Required:         yes
  Summary:          Move connection.rs to connection/mod.rs without changing
                    behavior so later commits can split focused submodules.
  Invariant focus:  The connection boundary remains the socket and protocol
                    adapter owner during mechanical file movement.
  Test level:       none
  Review gate:      none
  Files:            crates/nbd-server/src/connection.rs
                    crates/nbd-server/src/connection/mod.rs (new)
  Preconditions:    Commit 5 has localized protocol cookies at the connection
                    boundary.
  Postconditions:   The connection module compiles from connection/mod.rs and
                    behavior is otherwise unchanged.
  Verify:           cargo test -p nbd-server --test tcp_integration
  Risks:            low
  Not included:     No helper extraction or protocol logic changes.
  Depends on:       Commit 5
```

```text
Commit 7/19: connection: split shutdown and I/O helpers

  Type:             preparatory
  Required:         yes
  Summary:          Move cooperative shutdown handles and shutdown-aware socket
                    I/O helpers into focused connection submodules.
  Invariant focus:  Shutdown signaling and socket I/O cancellation stay owned by
                    the connection boundary.
  Test level:       integration
  Review gate:      code
  Files:            crates/nbd-server/src/connection/mod.rs
                    crates/nbd-server/src/connection/shutdown.rs (new)
                    crates/nbd-server/src/connection/io.rs (new)
  Preconditions:    Commit 6 has created the connection module shell.
  Postconditions:   Shutdown and I/O helpers are private connection submodules
                    and existing connection tests still pass.
  Verify:           cargo test -p nbd-server --test tcp_integration
  Risks:            Moving shutdown-aware helpers can change cancellation
                    behavior if call sites are rewired incorrectly.
  Not included:     No handshake, option, transmission, or reply split.
  Depends on:       Commit 6
```

```text
Commit 8/19: connection: split handshake and options

  Type:             preparatory
  Required:         yes
  Summary:          Move fixed-newstyle handshake and option negotiation into
                    dedicated connection submodules.
  Invariant focus:  NBD option negotiation remains wire-protocol adaptation and
                    does not move into export, registry, or engine code.
  Test level:       integration
  Review gate:      code
  Files:            crates/nbd-server/src/connection/mod.rs
                    crates/nbd-server/src/connection/handshake.rs (new)
                    crates/nbd-server/src/connection/options.rs (new)
  Preconditions:    Commit 7 has isolated shared connection shutdown and I/O
                    helpers.
  Postconditions:   Handshake and option negotiation compile from focused
                    submodules with unchanged TCP integration behavior.
  Verify:           cargo test -p nbd-server --test tcp_integration
                    make test-protocol
  Risks:            Option negotiation touches export open/close paths, so
                    protocol integration coverage is required.
  Not included:     No transmission request or reply writer split.
  Depends on:       Commit 7
```

```text
Commit 9/19: connection: split transmission and replies

  Type:             preparatory
  Required:         yes
  Summary:          Move transmission request decoding and reply writing into
                    dedicated connection submodules.
  Invariant focus:  Wire request/reply conversion stays at connection and export
                    workers never write sockets.
  Test level:       integration
  Review gate:      code
  Files:            crates/nbd-server/src/connection/mod.rs
                    crates/nbd-server/src/connection/transmission.rs (new)
                    crates/nbd-server/src/connection/replies.rs (new)
  Preconditions:    Commit 8 has split handshake and option negotiation.
  Postconditions:   Connection submodules own request decoding and reply
                    serialization, and protocol tests still pass.
  Verify:           cargo test -p nbd-server --test tcp_integration
                    make test-protocol
  Risks:            Reply handoff can affect queue-slot lifetime and cookie
                    correlation if moved carelessly.
  Not included:     No export contract directory split.
  Depends on:       Commit 8
```

```text
Commit 10/19: export: create module shell

  Type:             preparatory
  Required:         yes
  Summary:          Move export.rs to export/mod.rs without changing behavior
                    so export contracts can be split incrementally.
  Invariant focus:  Export remains the request execution contract owner while
                    root re-exports preserve public compatibility.
  Test level:       none
  Review gate:      none
  Files:            crates/nbd-server/src/export.rs
                    crates/nbd-server/src/export/mod.rs (new)
                    crates/nbd-server/src/lib.rs
  Preconditions:    Commit 9 has completed the connection split.
  Postconditions:   The export module compiles from export/mod.rs and root
                    public exports remain available.
  Verify:           cargo test -p nbd-server --lib
  Risks:            low
  Not included:     No admission or runtime movement.
  Depends on:       Commit 9
```

```text
Commit 11/19: export: split request and engine contracts

  Type:             preparatory
  Required:         yes
  Summary:          Split export request/context, completion, and engine trait
                    contracts into focused export submodules.
  Invariant focus:  Export request, completion, and engine contracts are
                    explicit API boundaries independent of concrete engines.
  Test level:       integration
  Review gate:      structures
  Files:            crates/nbd-server/src/export/mod.rs
                    crates/nbd-server/src/export/request.rs (new)
                    crates/nbd-server/src/export/completion.rs (new)
                    crates/nbd-server/src/export/engine.rs (new)
  Preconditions:    Commit 10 has created the export module shell.
  Postconditions:   Export contract types live in focused submodules and all
                    existing root re-exports still compile.
  Verify:           cargo test -p nbd-server --lib
                    cargo test -p nbd-server --test export_runtime
  Risks:            Moderate review risk because many imports move, but the
                    intended behavior remains unchanged.
  Not included:     No admission scheduling or runtime policy changes.
  Depends on:       Commit 10
```

```text
Commit 12/19: export: move admission and runtime

  Type:             preparatory
  Required:         yes
  Summary:          Move admission and runtime modules under export while
                    preserving queue-slot and admitted-request behavior.
  Invariant focus:  Export owns semantic admission, runtime queue depth, and
                    admitted request capabilities.
  Test level:       integration
  Review gate:      structures
  Files:            crates/nbd-server/src/admission.rs
                    crates/nbd-server/src/runtime.rs
                    crates/nbd-server/src/export/admission.rs (new)
                    crates/nbd-server/src/export/runtime.rs (new)
                    crates/nbd-server/src/export/mod.rs
                    crates/nbd-server/src/lib.rs
  Preconditions:    Commit 11 has split export request and engine contracts.
  Postconditions:   Admission and runtime compile under export/, queue-slot
                    lifetime tests pass, and admission tests pass.
  Verify:           cargo test -p nbd-server --test export_runtime
                    cargo test -p nbd-server --test admission
                    make test-protocol
  Risks:            Queue-slot lifetime and admission ordering are correctness
                    boundaries and need focused review.
  Not included:     No registry or concrete engine movement.
  Depends on:       Commit 11
```

```text
Commit 13/19: registry: split export orchestration

  Type:             preparatory
  Required:         yes
  Summary:          Move active export state and export factory construction
                    into registry submodules.
  Invariant focus:  Registry composes catalog, engines, storage, WAL, and
                    runtime policy without making export know concrete engines.
  Test level:       integration
  Review gate:      structures
  Files:            crates/nbd-server/src/registry.rs
                    crates/nbd-server/src/registry/mod.rs (new)
                    crates/nbd-server/src/registry/active.rs (new)
                    crates/nbd-server/src/registry/factory.rs (new)
                    crates/nbd-server/src/lib.rs
                    crates/nbd-server/src/server.rs
  Preconditions:    Commit 12 has moved export contracts under export/.
  Postconditions:   Registry orchestration is split into focused submodules,
                    active owner behavior is unchanged, and export/ still does
                    not import concrete engines.
  Verify:           cargo test -p nbd-server --test local_export_registry
                    make test-protocol
  Risks:            Active open/close state transitions must not change during
                    file movement.
  Not included:     No engine implementation movement.
  Depends on:       Commit 12
```

```text
Commit 14/19: engines: move memory engine

  Type:             preparatory
  Required:         yes
  Summary:          Move the memory engine under engines/ while preserving its
                    explicit unsafe boundary and public re-exports.
  Invariant focus:  Unsafe memory access remains isolated to the memory engine
                    and justified by admitted request permits.
  Test level:       integration
  Review gate:      code
  Files:            crates/nbd-server/src/memory.rs
                    crates/nbd-server/src/engines/mod.rs (new)
                    crates/nbd-server/src/engines/memory.rs (new)
                    crates/nbd-server/src/lib.rs
                    crates/nbd-server/src/registry/factory.rs
  Preconditions:    Commit 13 has isolated registry as the engine composition
                    owner.
  Postconditions:   Memory engine compiles under engines/, root re-exports
                    remain available, and unsafe allowance remains local.
  Verify:           cargo test -p nbd-server --test memory_export
                    cargo test -p nbd-server --lib
  Risks:            Moving the unsafe island can weaken review if the explicit
                    safety comments or allow attribute are lost.
  Not included:     No simple durable or WAL durable engine movement.
  Depends on:       Commit 13
```

```text
Commit 15/19: engines: move simple durable engine

  Type:             preparatory
  Required:         yes
  Summary:          Move simple durable and shared tree read helpers under
                    engines/ while preserving mutable blob-store semantics.
  Invariant focus:  Simple durable remains the only current engine requiring
                    MutableBlobStoreHandle for full-chunk replacement.
  Test level:       integration
  Review gate:      structures
  Files:            crates/nbd-server/src/simple_durable.rs
                    crates/nbd-server/src/tree_reader.rs
                    crates/nbd-server/src/engines/mod.rs
                    crates/nbd-server/src/engines/tree/mod.rs (new)
                    crates/nbd-server/src/engines/tree/read.rs (new)
                    crates/nbd-server/src/engines/simple_durable/mod.rs (new)
                    crates/nbd-server/src/engines/simple_durable/reader.rs (new)
                    crates/nbd-server/src/engines/simple_durable/tree.rs (new)
                    crates/nbd-server/src/lib.rs
                    crates/nbd-server/src/registry/factory.rs
  Preconditions:    Commit 14 has introduced engines/ and moved memory.
  Postconditions:   Simple durable code compiles under engines/, shared tree
                    read helpers are engine-internal, and simple durable tests
                    pass.
  Verify:           cargo test -p nbd-server --test simple_durable
                    make test-protocol
  Risks:            Tree read helper visibility and mutable blob semantics must
                    not drift during the split.
  Not included:     No WAL durable, read-cache, extent-map, or compaction moves.
  Depends on:       Commit 14
```

```text
Commit 16/19: engines: move wal durable support

  Type:             preparatory
  Required:         yes
  Summary:          Move WAL durable engine internals, read cache, extent map,
                    and COW compaction support under engines/wal_durable/.
  Invariant focus:  WAL durable keeps authoritative read-view overlay state
                    separate from optional cache state.
  Test level:       integration
  Review gate:      structures
  Files:            crates/nbd-server/src/wal_durable.rs
                    crates/nbd-server/src/read_cache.rs
                    crates/nbd-server/src/extent_map.rs
                    crates/nbd-server/src/compaction.rs
                    crates/nbd-server/src/engines/mod.rs
                    crates/nbd-server/src/engines/wal_durable/mod.rs (new)
                    crates/nbd-server/src/engines/wal_durable/admission.rs (new)
                    crates/nbd-server/src/engines/wal_durable/read_view.rs (new)
                    crates/nbd-server/src/engines/wal_durable/overlay.rs (new)
                    crates/nbd-server/src/engines/wal_durable/cache.rs (new)
                    crates/nbd-server/src/engines/wal_durable/extents.rs (new)
                    crates/nbd-server/src/engines/wal_durable/compact.rs (new)
                    crates/nbd-server/src/lib.rs
                    crates/nbd-server/src/registry/factory.rs
  Preconditions:    Commit 15 has moved shared tree read helpers under engines/.
  Postconditions:   WAL durable support compiles under engines/wal_durable/,
                    root re-exports remain available, and durable/compaction
                    tests pass.
  Verify:           cargo test -p nbd-server --test wal_durable
                    cargo test -p nbd-server --test compaction
  Risks:            Read-view correctness depends on preserving overlay and
                    cache ownership during the move.
  Not included:     No WAL provider/backend split or WAL format changes.
  Depends on:       Commit 15
```

```text
Commit 17/19: wal: create provider module shell

  Type:             preparatory
  Required:         yes
  Summary:          Move wal.rs to wal/mod.rs without changing WAL behavior so
                    provider internals can be split incrementally.
  Invariant focus:  WAL remains the sequencing, persistence, replay, and pruning
                    contract owner during mechanical file movement.
  Test level:       integration
  Review gate:      none
  Files:            crates/nbd-server/src/wal.rs
                    crates/nbd-server/src/wal/mod.rs (new)
                    crates/nbd-server/src/lib.rs
  Preconditions:    Commit 16 has moved WAL durable engine code out of crate
                    root.
  Postconditions:   WAL compiles from wal/mod.rs and behavior is unchanged.
  Verify:           cargo test -p nbd-server --test wal
                    cargo test -p nbd-server --test wal_durable
  Risks:            low
  Not included:     No codec, replay, or local backend extraction.
  Depends on:       Commit 16
```

```text
Commit 18/19: wal: split codec and replay internals

  Type:             preparatory
  Required:         yes
  Summary:          Move WAL record/segment encoding and replay cursor support
                    into focused wal submodules.
  Invariant focus:  WAL format compatibility and replay ordering are preserved
                    while implementation details move behind wal/.
  Test level:       integration
  Review gate:      code
  Files:            crates/nbd-server/src/wal/mod.rs
                    crates/nbd-server/src/wal/codec.rs (new)
                    crates/nbd-server/src/wal/replay.rs (new)
  Preconditions:    Commit 17 has created the WAL module shell.
  Postconditions:   Codec and replay helpers are private WAL submodules and WAL
                    tests still pass.
  Verify:           cargo test -p nbd-server --test wal
  Risks:            WAL decode behavior must keep tolerating final partial or
                    corrupt records exactly as before.
  Not included:     No local provider lifecycle extraction.
  Depends on:       Commit 17
```

```text
Commit 19/19: wal: split local backend

  Type:             preparatory
  Required:         yes
  Summary:          Move LocalWalProvider and LocalExportWal into wal/local.rs
                    while keeping the public WAL facade stable.
  Invariant focus:  WAL provider/backend internals do not depend on connection,
                    export runtime, engines, storage blobs, or catalog trees.
  Test level:       integration
  Review gate:      code
  Files:            crates/nbd-server/src/wal/mod.rs
                    crates/nbd-server/src/wal/local.rs (new)
                    crates/nbd-server/src/lib.rs
  Preconditions:    Commit 18 has isolated WAL codec and replay support.
  Postconditions:   Local WAL backend lives in wal/local.rs, public re-exports
                    remain stable, and final workspace verification passes.
  Verify:           cargo fmt --all --check
                    cargo clippy --workspace --all-targets -- -D warnings
                    cargo test --workspace
                    make test-protocol
  Risks:            Provider split touches WAL persistence and replay plumbing;
                    final verification must cover both WAL and protocol paths.
  Not included:     No WAL format changes, remote WAL provider, or storage
                    backend changes.
  Depends on:       Commit 18
```
