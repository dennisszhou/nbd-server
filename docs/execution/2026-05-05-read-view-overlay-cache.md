Title: Read View Overlay Cache Execution
Date: 2026-05-05
Status: approved
Approval:
- overall doc approved: yes
- current state: Series 1 approved
Completion:
- execution complete: no

## Goal

Implement the approved read-view overlay/cache design as one self-contained
series. The series must move incrementally through correct checkpoints, keep
tests with the behavior they prove, avoid trailing test-only commits, and keep
the existing owned `Vec<u8>` read reply contract.

## Design Inputs

- `docs/plans/2026-05-05-read-view-overlay-cache.md`

## Why Split

Not needed. The effort is self-contained inside the WAL durable read-view path,
and the user has approved a single managed series. Commit boundaries inside the
series still separate primitives, semantic adoption, cache behavior, and root
advancement so each commit remains correct and reviewable.

## Series 1: Read View Overlay And Cache

Depends on: none
Design coverage: the full approved read-view overlay/cache design, including
the extent-map primitive, authoritative overlay, read cache, exact byte-budget
LRU, source-limited same-window cache merging, and root advancement through
locally applied WAL.
Stable checkpoint: `ExportReadView` serves reads from overlay, cache, then
`TreeReader`; repeated writes do not keep shadowed WAL records live in the
serving overlay; warm committed reads can avoid `TreeReader`; cache memory is
bounded by charged bytes; and `advance_root` retires visible overlay bytes into
cache without caching shadowed WAL records.
Review focus: range splitting, payload offsets, cache object backrefs, exact
LRU state, checkpoint validation, source-limited merge behavior, and lock
scope.
Done means: the sequence-keyed retained WAL map is gone, tree-fill caching is
live for WAL durable reads, writes trim stale cache ranges, root advancement is
implemented at the read-view boundary, and tests cover repeated writes, middle
splits, cache hits, write trimming, merge/no-merge cases, byte-budget eviction,
and retired WAL demotion.
Approval: approved
Verification plan:
- `cargo test -p nbd-server extent_map`
- `cargo test -p nbd-server wal_durable`
- `cargo test -p nbd-server --test wal_durable`
- `cargo test -p nbd-server --test compaction`
- `cargo test -p nbd-server --test local_export_registry`
Not included: inline compaction scheduling, operator cache configuration,
process-wide cache coordination, WAL payload reload after eviction, benchmark
harnesses, and scatter/gather replies.

### Commit 1: docs/plans: add read-view overlay cache design

Commit the approved design and this single-series execution artifact.

### Commit 2: readview: add extent map primitive

Introduce a local `BTreeMap`-backed extent map with tests for overlap lookup,
overwrite insertion, removal, and offset-preserving splits.

### Commit 3: wal_durable: switch overlay to extents

Replace the sequence-keyed retained WAL map with an overlay extent map. Keep
read behavior unchanged while proving repeated writes and middle splits.

### Commit 4: readview: add cache object store

Introduce the read-cache object table, exact byte-budget LRU, object backrefs,
cache-block splitting, and source-limited same-window merge behavior with tests.

### Commit 5: wal_durable: cache committed tree reads

Read from overlay, cache, then `TreeReader`; insert returned `BlockPart::Data`
into cache holes; promote cache hits; evict to budget; and trim cache ranges
under writes.

### Commit 6: wal_durable: advance roots through overlay

Add `advance_root` with checkpoint validation, visible overlay retirement,
retired WAL cache insertion, and tests proving shadowed WAL records are not
cached.
