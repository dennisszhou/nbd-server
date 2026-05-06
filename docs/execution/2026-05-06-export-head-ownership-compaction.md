Title: Export Head Ownership And Compaction Execution
Date: 2026-05-06
Status: approved
Approval:
- overall doc approved: yes
- current state: Series 1 and Series 2 approved; Series 1 is current
Completion:
- execution complete: no

## Goal

Implement the approved export head ownership and compaction design in staged
checkpoints that keep catalog state modeling, runtime lifecycle, close
compaction, and write-pressure compaction independently reviewable.

The target end state is:

- `ExportDescriptor` describes only the `exports` row;
- `ActiveExportDescriptor` is required for serving/open paths;
- typed `ExportHead` variants represent durable `export_heads` state;
- `ExportRecord` is the joined descriptor-plus-head operator/catalog view;
- COW heads use `base_wal_seq` for the durable serving base;
- prototype Prisma migration history is replaced by a fresh baseline;
- runtimes drain accepted jobs and then call `ExportEngine::close`;
- `WalDurableEngine` owns an engine-local `CompactionCoordinator`;
- clean close attempts best-effort compaction through the applied WAL high
  watermark;
- stop-the-world write-pressure compaction runs behind an internal 2 GiB WAL
  debt threshold, with smaller test-only thresholds.

## Design Inputs

- `docs/plans/2026-05-06-export-head-ownership-compaction.md`
- `docs/plans/2026-05-04-local-wal.md`
- `docs/architecture/export-catalog-architecture.md`
- `docs/architecture/export-read-view-architecture.md`
- `docs/architecture/export-tree-metadata.md`
- `docs/architecture/local-export-registry-architecture.md`
- `docs/architecture/compaction-manager-architecture.md`

## Why Split

This effort changes catalog data structures, Prisma migration history, runtime
close semantics, compaction ownership, WAL replay debt policy, and integration
tests. A single implementation series would mix migration review with runtime
and compaction correctness.

The execution checkpoints are:

1. cut catalog state over to the descriptor/head/record model;
2. add the runtime-to-engine close hook;
3. move close compaction into `WalDurableEngine`;
4. add stop-the-world write-pressure compaction.

## Series 1: Catalog State Model

Depends on: none

Design coverage:
`docs/plans/2026-05-06-export-head-ownership-compaction.md`, catalog model,
schema naming, Prisma rebaseline, and active descriptor serving boundary.

Stable checkpoint: the repository builds and tests with the new catalog model.
Joined catalog views are named `ExportRecord`, durable heads are typed by
layout, COW heads use `base_wal_seq`, serving loads require
`ActiveExportDescriptor`, and fresh SQLite catalogs are created from one
baseline Prisma migration. Existing runtime compaction behavior remains in
place for this checkpoint.

Review focus: catalog source-of-truth boundaries, mechanical rename hygiene,
layout-specific illegal state handling, Prisma rebaseline correctness, active
versus deleted descriptor ownership, and avoiding compaction behavior drift in
this series.

Done means: no live Rust code depends on `ExportMeta`; no struct-shaped
`ExportHead` can carry impossible layout state; old `checkpoint_wal_seq`
schema/API names are replaced by `base_wal_seq`; `ExportCatalog` serving loads
return active descriptors; create/inspect/list/clone and compaction publication
outcomes return `ExportRecord`; targeted catalog, server, Prisma, formatting,
and clippy verification pass.

Approval: approved

Verification plan:

```text
awk 'length($0)>100 { print FILENAME ":" FNR; bad=1 } \
  END { exit bad }' docs/*/2026-05-06-*.md
make -C prisma db-migrate-check
cargo test -p nbd-control-plane
cargo test -p nbd-server --test compaction
cargo test -p nbd-server --test export_runtime
cargo test -p nbd-server --test local_export_registry
cargo test -p nbd-server --test memory_export
cargo test -p nbd-server --test simple_durable
cargo test -p nbd-server --test tcp_integration
cargo test -p nbd-server --test wal_durable
cargo test -p nbdcli
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
```

Not included: runtime engine close hooks, engine-owned compaction coordinator,
registry compaction removal, close compaction behavior changes, and
write-pressure compaction.

### Series 1 Commit Plan

```text
Commit 1/4: docs/plans: add export head ownership design

  Type:             docs
  Required:         yes
  Summary:          Commit the approved design and the execution contract before
                    the multi-commit implementation begins. The execution
                    artifact is included because it constrains the series that
                    follows.
  Invariant focus:  The approved state model is recorded before code changes
                    depend on it.
  Test level:       none
  Review gate:      structures
  Files:            docs/plans/2026-05-06-export-head-ownership-compaction.md
                    docs/execution/2026-05-06-export-head-ownership-compaction.md (new)
  Preconditions:    The design has passed review-plan and is ready for execution
                    planning.
  Postconditions:   The repository contains the approved design and the current
                    execution source of truth for this effort.
  Verify:           awk 'length($0)>100 { print FILENAME ":" FNR; bad=1 } \
                    END { exit bad }' docs/*/2026-05-06-*.md
  Risks:            Low; the only risk is committing a plan that no longer
                    matches the implementation direction.
  Not included:     No code, schema, migration, or runtime behavior changes.
  Depends on:       none

Commit 2/4: catalog: rename export metadata records

  Type:             preparatory
  Required:         yes
  Summary:          Rename the joined descriptor-plus-head API from ExportMeta
                    to ExportRecord across live code and tests without changing
                    behavior. This clears the misleading old name before
                    changing the head model underneath it.
  Invariant focus:  Joined export state is named as a catalog/operator record,
                    not as live engine metadata.
  Test level:       functional
  Review gate:      code
  Files:            crates/nbd-control-plane/src/lib.rs
                    crates/nbd-control-plane/src/model.rs
                    crates/nbd-control-plane/src/sqlite.rs
                    crates/nbd-control-plane/tests/sqlite_catalog.rs
                    crates/nbd-server/src/connection.rs
                    crates/nbd-server/src/export.rs
                    crates/nbd-server/src/memory.rs
                    crates/nbd-server/src/observability.rs
                    crates/nbd-server/src/registry.rs
                    crates/nbd-server/src/runtime.rs
                    crates/nbd-server/src/wal_durable.rs
                    crates/nbd-server/tests/compaction.rs
                    crates/nbd-server/tests/export_runtime.rs
                    crates/nbd-server/tests/memory_export.rs
                    crates/nbd-server/tests/simple_durable.rs
                    crates/nbd-server/tests/support/nbd.rs
                    crates/nbd-server/tests/wal_durable.rs
                    crates/nbdcli/src/main.rs
  Preconditions:    Commit 1 has recorded the approved design and execution
                    contract.
  Postconditions:   The workspace builds against ExportRecord for joined catalog
                    views, and no live code depends on the ExportMeta type name.
  Verify:           cargo test -p nbd-control-plane
                    cargo test -p nbd-server --test export_runtime
                    cargo test -p nbd-server --test memory_export
                    cargo test -p nbd-server --test wal_durable
                    cargo test -p nbdcli
  Risks:            Moderate rename churn; review should check for semantic
                    edits hidden in mechanical call-site updates.
  Not included:     No typed head variants, active descriptor wrapper, Prisma
                    migration rebaseline, or compaction ownership changes.
  Depends on:       Commit 1

Commit 3/4: catalog: type export heads by layout

  Type:             semantic
  Required:         yes
  Summary:          Replace the struct-shaped ExportHead with layout-specific
                    variants, rename the COW base sequence to base_wal_seq, and
                    rebaseline Prisma to the new schema. Update catalog
                    decoding, engine callers, tests, and current architecture
                    docs in the same semantic step.
  Invariant focus:  Invalid layout/head combinations are rejected at the catalog
                    boundary or made unrepresentable in Rust values.
  Test level:       integration
  Review gate:      migration
  Files:            crates/nbd-control-plane/src/lib.rs
                    crates/nbd-control-plane/src/model.rs
                    crates/nbd-control-plane/src/sqlite.rs
                    crates/nbd-control-plane/tests/model.rs
                    crates/nbd-control-plane/tests/sqlite_catalog.rs
                    crates/nbd-server/src/compaction.rs
                    crates/nbd-server/src/memory.rs
                    crates/nbd-server/src/simple_durable.rs
                    crates/nbd-server/src/wal_durable.rs
                    crates/nbd-server/tests/compaction.rs
                    crates/nbd-server/tests/local_export_registry.rs
                    crates/nbd-server/tests/memory_export.rs
                    crates/nbd-server/tests/simple_durable.rs
                    crates/nbd-server/tests/tcp_integration.rs
                    crates/nbd-server/tests/wal_durable.rs
                    docs/architecture/compaction-manager-architecture.md
                    docs/architecture/export-catalog-architecture.md
                    docs/architecture/export-read-view-architecture.md
                    docs/architecture/export-tree-metadata.md
                    docs/architecture/local-export-registry-architecture.md
                    docs/architecture/wal-architecture.md
                    prisma/schema.prisma
                    prisma/migrations/20260501000000_init/migration.sql (delete)
                    prisma/migrations/20260504000000_export_heads_tree_metadata/ (delete)
                    prisma/migrations/20260504010000_simple_durable_engine_kind/ (delete)
                    prisma/migrations/20260505000000_wal_durable_engine_kind/ (delete)
                    prisma/migrations/20260505010000_cow_tree_metadata/ (delete)
                    prisma/migrations/20260506000000_baseline/migration.sql (new)
  Preconditions:    Commit 2 has made joined export state use the ExportRecord
                    name everywhere.
  Postconditions:   ExportHead is layout typed, COW heads expose base_wal_seq,
                    SQLite row decoding enforces the layout-specific invariants,
                    and a fresh Prisma baseline creates the new schema from
                    scratch.
  Verify:           make -C prisma db-migrate-check
                    cargo test -p nbd-control-plane
                    cargo test -p nbd-server --test compaction
                    cargo test -p nbd-server --test local_export_registry
                    cargo test -p nbd-server --test wal_durable
                    cargo test -p nbd-server --test tcp_integration
  Risks:            High migration/review churn; the fresh baseline
                    intentionally discards prototype migration history, so tests
                    must prove new database creation and row decoding.
  Not included:     No ActiveExportDescriptor serving wrapper, runtime close
                    hook, engine-owned coordinator, or write-pressure compaction
                    behavior.
  Depends on:       Commit 2

Commit 4/4: catalog: require active descriptors for serving

  Type:             semantic
  Required:         yes
  Summary:          Introduce ActiveExportDescriptor for open/serving paths and
                    update the registry and engine factory to consume the active
                    wrapper. Operator paths continue to use ExportRecord so
                    deleted exports remain inspectable when requested.
  Invariant focus:  Serving code cannot open a deleted export without first
                    crossing the catalog's active-export check.
  Test level:       integration
  Review gate:      structures
  Files:            crates/nbd-control-plane/src/lib.rs
                    crates/nbd-control-plane/src/model.rs
                    crates/nbd-control-plane/src/sqlite.rs
                    crates/nbd-control-plane/tests/model.rs
                    crates/nbd-control-plane/tests/sqlite_catalog.rs
                    crates/nbd-server/src/registry.rs
                    crates/nbd-server/src/memory.rs
                    crates/nbd-server/src/simple_durable.rs
                    crates/nbd-server/src/wal_durable.rs
                    crates/nbd-server/tests/local_export_registry.rs
                    crates/nbd-server/tests/memory_export.rs
                    crates/nbd-server/tests/simple_durable.rs
                    crates/nbd-server/tests/wal_durable.rs
                    docs/architecture/export-catalog-architecture.md
                    docs/architecture/local-export-registry-architecture.md
  Preconditions:    Commit 3 has typed export heads and renamed the durable COW
                    base sequence.
  Postconditions:   Catalog serving loads return ActiveExportDescriptor, open
                    paths and engines use the active wrapper, and
                    inspect/list/create/clone remain ExportRecord views.
  Verify:           cargo test -p nbd-control-plane
                    cargo test -p nbd-server --test local_export_registry
                    cargo test -p nbd-server --test memory_export
                    cargo test -p nbd-server --test simple_durable
                    cargo test -p nbd-server --test wal_durable
                    cargo fmt --all --check
                    cargo clippy --workspace --all-targets -- -D warnings
  Risks:            Moderate API churn across the registry and engine
                    constructors; review should check that active/deleted state
                    stays descriptor-owned, not head-owned.
  Not included:     No runtime engine close hook, compaction coordinator
                    ownership change, or write-pressure threshold behavior.
  Depends on:       Commit 3
```

## Series 2: Runtime Engine Close Hook

Depends on: Series 1

Design coverage:
`docs/plans/2026-05-06-export-head-ownership-compaction.md`, runtime close API.

Stable checkpoint: `ExportEngine` has a default close hook, serial and
concurrent runtimes close queue admission, drain accepted jobs, and then call
the engine close hook exactly once. Existing engines keep no-op close behavior.

Review focus: queue-slot lifetime, accepted-job drain ordering, serial runtime
engine ownership, repeated close behavior, and preserving connection-visible
close semantics.

Done means: runtime tests prove close waits for accepted jobs before calling
the engine hook; serial runtime retains an engine handle for close; concurrent
runtime calls close after its lifecycle is empty; no compaction behavior is
moved yet.

Approval: approved

Verification plan:

```text
cargo test -p nbd-server --test export_runtime
cargo test -p nbd-server --test local_export_registry
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
```

Not included: engine-owned compaction, registry compaction removal, and
write-pressure compaction.

### Series 2 Commit Plan

```text
Commit 1/1: runtime: call engine close after drain

  Type:             semantic
  Required:         yes
  Summary:          Add a default ExportEngine close hook and make serial and
                    concurrent runtimes call it after closing queue admission
                    and draining accepted jobs. Existing engines keep the
                    default no-op behavior.
  Invariant focus:  Engine shutdown runs only after accepted runtime work has
                    completed, and it runs at most once per runtime.
  Test level:       integration
  Review gate:      code
  Files:            crates/nbd-server/src/export.rs
                    crates/nbd-server/src/runtime.rs
                    crates/nbd-server/tests/export_runtime.rs
                    crates/nbd-server/tests/local_export_registry.rs
  Preconditions:    Series 1 has finished and runtime metadata now uses
                    ExportRecord naming.
  Postconditions:   Runtime close rejects new work, waits for accepted jobs,
                    invokes ExportEngine::close exactly once, and preserves the
                    existing close behavior for engines that do not override
                    the hook.
  Verify:           cargo test -p nbd-server --test export_runtime
                    cargo test -p nbd-server --test local_export_registry
                    cargo fmt --all --check
                    cargo clippy --workspace --all-targets -- -D warnings
  Risks:            Close idempotence and serial runtime ownership are the
                    main risks; review should check that engine.close is not
                    called before queued accepted work finishes.
  Not included:     No compaction coordinator, registry compaction removal,
                    WAL close compaction, or write-pressure compaction.
  Depends on:       Series 1
```

## Series 3: Engine-Owned Close Compaction

Depends on: Series 2

Design coverage:
`docs/plans/2026-05-06-export-head-ownership-compaction.md`, compaction
coordinator ownership and close best-effort semantics.

Stable checkpoint: `WalDurableEngine` owns a `CompactionCoordinator` that can
compact through the engine read view's applied WAL high watermark on close.
Successful close compaction publishes a new durable head and advances the live
read view. Failed close compaction is logged but does not fail close because
retained WAL replay remains correct.

Review focus: coordinator ownership, avoiding self-referential borrows with
`Arc<ExportReadView>`, idempotent publication, failure ordering, registry
cleanup, observability, deleting the current compaction queue/worker lifecycle,
and not running compaction on request queue slots.

Done means: the registry no longer reopens WALs or enqueues close compaction;
the old global `CompactionManager`, `CompactionQueue`,
`CompactionEnqueueOutcome`, and background worker shutdown path are gone;
any retained compaction code is a direct async helper owned by
`CompactionCoordinator`, not a queue or manager; close compaction tests prove
success, already-covered/stale outcomes, failure recovery, and reopen replay
after failure.

Approval: pending

Verification plan:

```text
cargo test -p nbd-server --test compaction
cargo test -p nbd-server --test local_export_registry
cargo test -p nbd-server --test wal_durable
make test-protocol
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
```

Not included: write-pressure threshold compaction and generational/backoff
policy.

## Series 4: Stop-The-World Write-Pressure Compaction

Depends on: Series 3

Design coverage:
`docs/plans/2026-05-06-export-head-ownership-compaction.md`, stop-the-world
write-pressure policy and internal 2 GiB WAL debt threshold.

Stable checkpoint: WAL durable writes append and apply normally while WAL debt
is below the threshold. When the threshold is reached, the writer that observes
it compacts through a stable applied target while holding the engine write
lock. New writes wait behind that lock; reads continue against the current
read view except for the brief root-advance update.

Review focus: target selection, debt accounting, write lock scope, admission
interaction, read concurrency, threshold test hooks, and avoiding public config
surface before it is needed.

Done means: production uses the internal 2 GiB threshold, tests can construct a
smaller threshold, write-pressure compaction publishes and advances the read
view, and a second write waits behind compaction rather than racing against the
base transition.

Approval: pending

Verification plan:

```text
cargo test -p nbd-server --test wal_durable
cargo test -p nbd-server --test compaction
cargo test -p nbd-server --test local_export_registry
make test-protocol
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
```

Not included: write backoff, generational compaction, manual operator
compaction, active read-view retention across processes, or orphan blob GC.
