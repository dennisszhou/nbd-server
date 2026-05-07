Title: Read-View Background Compaction Execution
Date: 2026-05-07
Status: approved
Approval:
- overall doc approved: yes
- current state: Series 1 approved
Completion:
- execution complete: no

## Goal

Implement the approved read-view background compaction design for
`wal_durable` exports.

The final checkpoint is an active WAL durable export that:

- uses retained WAL payload bytes as the compaction trigger metric;
- captures a read-view metadata snapshot for compaction instead of replaying
  WAL records from disk;
- keeps close-time and write-pressure compaction correct on the snapshot path;
- starts one per-export background Tokio compaction task;
- stops that task before close-time compaction;
- keeps behavior-specific proof with the commits that introduce each behavior.

## Design Inputs

- `docs/plans/2026-05-07-read-view-background-compaction.md`

## Why Split

One execution series is enough. The work changes several boundaries, but the
natural order is a single dependency chain: design doc, execution doc, read-view
snapshot primitive, snapshot compactor, close adoption, hard-threshold adoption,
background tick policy, and background task lifecycle.

This remains a durable execution artifact because the work is concurrency
sensitive, crosses runtime and compaction ownership, and is likely to span more
than one implementation session.

## Series 1: Read-View Background Compaction

Depends on: none

Design coverage: implements the approved design end to end for active
`wal_durable` exports. The series keeps WAL debt as retained payload bytes,
uses read-view snapshots for compaction output, preserves hard-threshold write
backpressure, adds the background tick policy, and wires one task per active
export.

Stable checkpoint: close, write-pressure, and background compaction all use the
snapshot compaction path. Background compaction runs for active COW-backed
`wal_durable` exports, skips below the soft threshold and at the hard
threshold, and shuts down before close compaction. Existing local and S3 smoke
paths remain valid.

Review focus: snapshot correctness, partial checkpoint debt accounting,
compaction publication invariants, write-lock and compaction-lock interaction,
background task shutdown, and proof placement.

Done means: every planned commit lands, per-commit verification passes, final
workspace verification passes, and end-of-series review finds no blocking
semantic or lifecycle issue.

Approval: approved

Verification plan:

```text
cargo fmt --all --check
cargo test -p nbd-server --lib
cargo test -p nbd-server --test compaction
cargo test -p nbd-server --test wal_durable
cargo test --workspace
make docker-smoke
make docker-smoke-s3
```

Not included: public compaction config, global compaction workers, full
read-version trees, WAL format changes, catalog schema changes, blob garbage
collection, or changing the S3 storage contract.

### Current-Series Commit Plan

```text
Commit 1/8: docs/plans: add read-view compaction design

  Type:             docs
  Required:         yes
  Summary:          Add the approved design for snapshot-based WAL durable
                    background compaction.
  Invariant focus:  Implementation proceeds from a committed source of truth
                    instead of chat-only agreement.
  Test level:       none
  Review gate:      structures
  Files:            docs/plans/2026-05-07-read-view-background-compaction.md
  Preconditions:    The design has passed review-plan with result ready for
                    series planning.
  Postconditions:   The approved design is present in the repository and can
                    constrain implementation commits.
  Verify:           awk 'length($0) > 80 { print FNR ":" length($0) ":" $0 }'
                    docs/plans/2026-05-07-read-view-background-compaction.md
  Risks:            low
  Not included:     No implementation files or execution approval state change.
  Depends on:       none
```

```text
Commit 2/8: docs/execution: add compaction execution plan

  Type:             docs
  Required:         yes
  Summary:          Add this durable single-series execution contract for the
                    approved background compaction design.
  Invariant focus:  Execution has explicit commit boundaries, approval state,
                    and verification placement before implementation starts.
  Test level:       none
  Review gate:      structures
  Files:            docs/execution/2026-05-07-read-view-background-compaction.md
  Preconditions:    Commit 1 has committed the approved design input.
  Postconditions:   The execution contract exists with implementation approval
                    recorded.
  Verify:           awk 'length($0) > 80 { print FNR ":" length($0) ":" $0 }'
                    docs/execution/2026-05-07-read-view-background-compaction.md
  Risks:            low
  Not included:     No code changes or implementation approval.
  Depends on:       Commit 1
```

```text
Commit 3/8: read-view: capture compaction snapshots

  Type:             semantic
  Required:         yes
  Summary:          Introduce the read-view compaction snapshot primitive with
                    retained WAL debt and overlay metadata capture.
  Invariant focus:  A snapshot represents the live view at one target WAL
                    sequence without copying WAL payload bytes or including
                    later writes.
  Test level:       unit
  Review gate:      structures
  Files:            crates/nbd-server/src/engines/wal_durable/read_view.rs
                    crates/nbd-server/src/engines/wal_durable/overlay.rs
  Preconditions:    Commit 2 has committed the execution source of truth.
  Postconditions:   `ExportReadView` can return a compaction snapshot only when
                    retained WAL exists, hot rewrites keep WAL debt physical,
                    and a captured snapshot remains stable after later writes.
  Verify:           cargo test -p nbd-server --lib
  Risks:            Snapshot debt and overlay clone semantics become a new
                    correctness boundary for later compaction commits.
  Not included:     No compactor or engine caller uses the snapshot yet.
  Depends on:       Commit 2
```

```text
Commit 4/8: compaction: write checkpoints from snapshots

  Type:             semantic
  Required:         yes
  Summary:          Add snapshot-based COW compaction that materializes dirty
                    chunk images from the captured root plus overlay.
  Invariant focus:  A checkpoint at target `S` writes latest visible data for
                    all dirty chunks represented by the snapshot.
  Test level:       integration
  Review gate:      code
  Files:            crates/nbd-server/src/engines/wal_durable/compaction.rs
                    crates/nbd-server/src/engines/wal_durable/read_view.rs
                    crates/nbd-server/tests/compaction.rs
  Preconditions:    Commit 3 exposes stable read-view compaction snapshots.
  Postconditions:   The compactor can publish from a snapshot, hot rewritten
                    chunks are written once with latest visible data, and stale
                    publication remains safe.
  Verify:           cargo test -p nbd-server --test compaction
                    cargo test -p nbd-server --lib
  Risks:            Snapshot grouping must not miss split overlay extents or
                    include writes beyond the target sequence.
  Not included:     Close, hard-threshold, and background paths still use the
                    old caller shape until adopted in later commits.
  Depends on:       Commit 3
```

```text
Commit 5/8: wal: use snapshots for close compaction

  Type:             semantic
  Required:         yes
  Summary:          Route close-time WAL durable compaction through the
                    snapshot path and update live read-view advancement.
  Invariant focus:  Close compaction publishes a snapshot target and subtracts
                    only the debt represented by that snapshot.
  Test level:       integration
  Review gate:      code
  Files:            crates/nbd-server/src/engines/wal_durable/mod.rs
                    crates/nbd-server/src/engines/wal_durable/read_view.rs
                    crates/nbd-server/tests/wal_durable.rs
  Preconditions:    Commit 4 has proven snapshot compaction mechanics.
  Postconditions:   Close compaction no longer replays WAL from disk in the
                    active engine path, advances the read view through the
                    published checkpoint, and preserves newer debt correctly.
  Verify:           cargo test -p nbd-server --test wal_durable
                    cargo test -p nbd-server --lib
  Risks:            Partial snapshot debt accounting must stay correct if
                    writes arrive while close races only with drained work.
  Not included:     Hard-threshold and background compaction adoption.
  Depends on:       Commit 4
```

```text
Commit 6/8: wal: use snapshots for hard compaction

  Type:             semantic
  Required:         yes
  Summary:          Move write-pressure compaction to the snapshot path and
                    recheck WAL debt after acquiring the compaction lock.
  Invariant focus:  At or above the hard threshold, compaction is write
                    backpressure and cannot let new writes get further ahead.
  Test level:       integration
  Review gate:      code
  Files:            crates/nbd-server/src/engines/wal_durable/mod.rs
                    crates/nbd-server/src/engines/wal_durable/read_view.rs
                    crates/nbd-server/tests/wal_durable.rs
  Preconditions:    Commit 5 has established snapshot read-view advancement for
                    an active engine caller.
  Postconditions:   The hard path waits for active compaction while holding the
                    write lock, rechecks debt, compacts only when still needed,
                    and leaves later writes queued behind pressure.
  Verify:           cargo test -p nbd-server --test wal_durable
                    cargo test -p nbd-server --lib
  Risks:            Lock ordering must avoid deadlock and preserve the intended
                    write backpressure behavior.
  Not included:     Periodic background scheduling.
  Depends on:       Commit 5
```

```text
Commit 7/8: wal: add background compaction ticks

  Type:             semantic
  Required:         yes
  Summary:          Add the soft-threshold background tick policy and
                    opportunistic try-lock compaction method.
  Invariant focus:  Background compaction is maintenance below the hard
                    threshold and never waits behind another compaction.
  Test level:       integration
  Review gate:      code
  Files:            crates/nbd-server/src/engines/wal_durable/mod.rs
                    crates/nbd-server/tests/wal_durable.rs
  Preconditions:    Commit 6 has a shared snapshot compaction path for close
                    and hard-threshold callers.
  Postconditions:   A background tick skips below the soft threshold, skips at
                    or above the hard threshold, skips when the compaction lock
                    is busy, and compacts otherwise.
  Verify:           cargo test -p nbd-server --test wal_durable
                    cargo test -p nbd-server --lib
  Risks:            Test hooks for thresholds and busy-lock behavior must not
                    become public policy.
  Not included:     No periodic task is spawned yet.
  Depends on:       Commit 6
```

```text
Commit 8/8: wal: run background compaction task

  Type:             semantic
  Required:         yes
  Summary:          Spawn one per-export background compaction task for
                    COW-backed WAL durable engines and stop it on close.
  Invariant focus:  The engine owns the task, the task is joined at most once,
                    and close stops background work before close compaction.
  Test level:       integration
  Review gate:      code
  Files:            crates/nbd-server/src/engines/wal_durable/mod.rs
                    crates/nbd-server/tests/wal_durable.rs
  Preconditions:    Commit 7 exposes a tested background tick operation.
  Postconditions:   Active COW-backed WAL durable engines run periodic
                    background compaction, zero-backed legacy opens do not, and
                    close shuts down the task before final close compaction.
  Verify:           cargo test -p nbd-server --test wal_durable
                    cargo test -p nbd-server --lib
                    cargo test --workspace
                    make docker-smoke
                    make docker-smoke-s3
  Risks:            Background task shutdown and join-once ownership are the
                    main async lifecycle risks.
  Not included:     Public config, global workers, and blob garbage collection.
  Depends on:       Commit 7
```
