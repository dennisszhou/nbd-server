Title: Simple Durable Engine Execution
Date: 2026-05-04
Status: in_progress
Approval:
- overall doc approved: yes
- current state: Series 1 finished; Series 2 approved
Completion:
- execution complete: no
- completed series: Series 1
- next series: Series 2, approved for implementation

## Goal

Implement the approved `SimpleDurableEngine` design in staged checkpoints that
keep metadata semantics, admission semantics, local blob I/O, and protocol
behavior independently reviewable.

The target end state is:

- a catalog current-head model that no longer treats mutable serving state as
  append-only historical generations;
- explicit simple mutable tree metadata in SQLite;
- admission policy mapping that lets simple durable serialize chunk-aligned
  writes while preserving current memory admission behavior;
- a local `LocalBlobStore` that owns one-blob file create, replace, and read
  operations;
- a `SimpleMutableTree` that owns tree interpretation and metadata commits;
- an opt-in `SimpleDurableEngine` behind the existing export runtime API;
- userspace protocol proof that simple durable data survives server restart;
- the existing memory default and Docker smoke path remain valid.

## Design Inputs

- `docs/plans/2026-05-04-simple-durable-engine.md`

## Why Split

This effort changes the catalog schema, the control-plane model, admission
conflict semantics, local filesystem durability behavior, engine construction,
and protocol-visible persistence. A single implementation series would make it
too easy to hide semantic drift between metadata truth, storage writes, and
admission guarantees.

The split keeps each risky boundary testable before layering the next one on
top:

1. catalog current-head and tree metadata;
2. admission policy boundary;
3. local blob store plus simple mutable tree owner;
4. opt-in simple durable engine and protocol restart proof.

## Series 1: Catalog Head And Tree Metadata

Depends on: none

Design coverage:
`docs/plans/2026-05-04-simple-durable-engine.md`

Stable checkpoint: catalog metadata has an explicit current-head model and
tree metadata tables, while existing memory exports still create, list,
inspect, delete, and load correctly.

Review focus: schema truth, migration safety, source-of-truth naming, moving
the catalog domain API to `ExportHead` terminology, keeping current head
separate from future immutable history, and not exposing simple durable server
behavior before the metadata foundation is stable.

Done means: a new migration creates `export_heads`, `tree_nodes`,
`tree_edges`, and `tree_leaf_refs`; existing export metadata is represented as
one current head per export; the catalog model and SQLite implementation load
current state from `export_heads`; `ExportMeta` exposes current head state
through head-oriented naming instead of generation-oriented serving APIs; tests
and server fixtures apply the current migration set consistently; memory
export behavior remains unchanged.

Approval: finished

Verification plan:

```text
cargo test -p nbd-control-plane
cargo test -p nbdcli
cargo test --workspace
cargo fmt --all --check
```

Not included: admission changes, `LocalBlobStore`, `SimpleMutableTree`,
`SimpleDurableEngine`, public `simple_durable` engine kind parsing, server
registry rollout, protocol durable tests, WAL, clone, compaction, S3, or GC.

## Series 2: Admission Policy Boundary

Depends on: Series 1

Design coverage:
`docs/plans/2026-05-04-simple-durable-engine.md`

Stable checkpoint: admission can express and test the simple durable conflict
shape without changing any storage engine behavior.

Review focus: read/write conflict semantics, FIFO fairness, flush as a global
barrier, preserving current memory behavior, using `ExportAdmissionPolicy`
naming for the engine-to-admission mapping boundary, and keeping admission
independent from tree nodes and blob keys.

Done means: the current `ExportAdmissionProfile` naming is replaced by
`ExportAdmissionPolicy`; `AdmissionOp::Write` keeps carrying one `ByteRange`;
future simple durable policy-owned write expansion is documented as
chunk-aligned `ByteRange` admission rather than a separate `ChunkRange`
primitive; flush remains a global barrier; existing memory engine admission
continues to allow non-overlapping writes; admission and runtime tests keep
covering the existing read/write/flush contract after the rename.

Approval: approved

Verification plan:

```text
cargo test -p nbd-server --test admission
cargo test -p nbd-server --test export_runtime
make test-protocol
cargo fmt --all --check
```

Not included: catalog schema changes beyond Series 1, blob file I/O, durable
engine construction, protocol durable persistence, WAL, clone, compaction, S3,
or GC.

## Series 3: Local Blob Store And Simple Mutable Tree

Depends on: Series 2

Design coverage:
`docs/plans/2026-05-04-simple-durable-engine.md`

Stable checkpoint: local blob files and simple mutable tree metadata can be
tested below the export engine boundary, with no protocol-visible durable
engine rollout yet.

Review focus: file path containment, full-blob replacement semantics,
fsync/rename ordering, avoiding synchronous filesystem blocking on Tokio core
worker threads, sparse tree lookup, v1 root-to-leaf tree shape, root creation,
metadata commit ordering, in-memory cache updates after DB commit, and avoiding
DB references to unwritten blobs.

Done means: `runtime.blob_dir` can be configured and defaults safely for
generated config; `LocalBlobStore` can create, replace, and range-read one
blob; `SimpleMutableTree` can load current head metadata, resolve sparse
zeroes using a v1 root node with direct leaf edges keyed by chunk index, commit
newly materialized chunks after blob work succeeds, and update its in-memory
view only after DB commit; tests cover a 128 MiB example with sparse chunk
reads and later leaf insertion.

Approval: pending

Verification plan:

```text
cargo test -p nbd-config
cargo test -p nbd-control-plane
cargo test -p nbd-server --test simple_durable
cargo test --workspace
cargo fmt --all --check
```

Not included: NBD protocol durable engine exposure, `nbdcli create --engine
simple_durable`, WAL, clone, compaction, S3, GC, or request-level atomicity for
multi-chunk write failures.

## Series 4: Simple Durable Engine Rollout

Depends on: Series 3

Design coverage:
`docs/plans/2026-05-04-simple-durable-engine.md`

Stable checkpoint: simple durable exports can be created explicitly and served
through the existing runtime path, and successful writes persist across server
restart.

Review focus: engine construction boundaries, registry config plumbing,
admission permit lifetime, chunk pagination, DB-after-fsync ordering,
multi-chunk write limitation truthfulness, protocol persistence proof, and no
regression to memory default behavior.

Done means: the `simple_durable` engine kind is accepted by the catalog schema,
domain model, and CLI parser; `nbdcli create --engine simple_durable` creates a
simple durable export; `LocalExportRegistry` constructs `SimpleDurableEngine`
for that engine kind using configured `runtime.blob_dir`; reads zero-fill
sparse chunks; writes materialize or replace 32 MiB blobs; flush remains a
barrier no-op; protocol integration proves write/read behavior and data
survival across server restart; memory remains the default create engine and
existing Docker smoke still passes.

Approval: pending

Verification plan:

```text
cargo test -p nbd-config
cargo test -p nbd-control-plane
cargo test -p nbdcli
cargo test -p nbd-server --test local_export_registry
make test-protocol
cargo test --workspace
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
make docker-smoke
```

Not included: WAL durability, replay, read view, compaction, immutable COW
roots, clone, S3, GC, online resize, dynamic blob sizing, or atomic
all-or-nothing semantics across multi-chunk write failures.

## Completion

Execution is complete when Series 4 is finished, the execution doc has a
truthful closeout, and the repository has protocol evidence that opt-in simple
durable exports persist written data across server restart while default
memory behavior and Docker smoke remain intact.

Deferred follow-up:

- `WALDurableEngine`;
- immutable COW tree publication;
- `ExportReadView`;
- compaction and checkpoint publication;
- clone;
- local blob GC;
- S3-compatible blob store;
- online resize;
- stronger multi-chunk request atomicity.
