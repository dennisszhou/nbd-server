Title: Control Plane Storage Adapter Boundary Execution
Date: 2026-05-12
Status: completed
Approval:
- overall doc approved: yes
- current state: Series 1 finished
Completion:
- execution complete: yes
- completed series: Series 1
- review complete: yes, `$review-series` accepted the series
- closeout approved: yes, `$finish-series`

## Goal

Implement the approved control-plane storage adapter boundary in one
reviewable series.

The target end state is:

- `nbd-control-plane` is the public facade and provider factory;
- `nbd-control-plane-core` owns storage-neutral catalog types, traits, and
  errors;
- `nbd-control-plane-sqlite` owns SQLite, `sqlx`, row mapping, transactions,
  and SQLite integration tests;
- `nbd-server` source imports no concrete database adapter, `sqlx`, table
  names, or adapter row types;
- tree format ids are stored catalog state, while tree geometry, lazy tree
  readers/editors, simple mutable tree updates, and COW publication planning
  are server-owned;
- tree metadata reads are lazy and bounded by explicit node ids or parent/slot
  lookup lists;
- tree record insertion and export-head publication are atomic with respect to
  the expected prior head.

## Design Inputs

- `docs/plans/2026-05-12-control-plane-storage-adapter-boundary.md`

## Why One Series

The work cuts across crates, catalog traits, SQLite persistence, server tree
logic, and tests. A multi-series split would create stable checkpoints that are
architecturally incomplete: either the adapter is hidden while tree algorithms
still live in the database layer, or the server tree API exists before its
callers have moved.

This execution keeps one stable checkpoint and uses small commits inside that
series for reviewability. Every commit must still build, keep docs truthful for
that checkout, and carry proof for the behavior it introduces.

## Series 1: Control Plane Storage Adapter Boundary

Depends on: none

Design coverage: implements the approved facade/core/adapter split, source/API
leakage boundary, lazy tree row API, server-owned tree geometry and mutation
planning, and final import hygiene.

Stable checkpoint: `nbd-server` and `nbdcli` call the `nbd-control-plane`
facade; storage-neutral catalog contracts live in `nbd-control-plane-core`;
SQLite implementation lives in `nbd-control-plane-sqlite`; server source has no
database-adapter imports; simple durable and WAL durable tree updates are
planned in server code and persisted through bounded lazy row reads plus
atomic tree publication.

Review focus: crate dependency direction, source/API leakage, facade/core
public exports, SQLite transaction boundaries, lazy metadata load bounds,
tree-format invariants, simple mutable update ownership, COW publish
correctness, and removal of legacy snapshot-style tree APIs.

Source topology checkpoint: confirm that no large file becomes the next
catalog dumping ground. In particular, `nbd-control-plane-core` must not become
a new monolithic `model.rs`, `nbd-control-plane-sqlite` must keep SQL row
mapping separate from transaction orchestration, and server tree behavior must
live under `crates/nbd-server/src/engines/tree/` or the specific engine owner.

Done means: all commits in this one series are landed, source leakage checks
pass, targeted crate tests pass, the workspace builds and tests, and the final
series review has no in-scope findings left unfixed.

Approval: finished

Verification plan:

```text
cargo fmt --all --check
cargo test -p nbd-control-plane-core
cargo test -p nbd-control-plane
cargo test -p nbd-control-plane-sqlite
cargo test -p nbd-server
cargo test -p nbdcli
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
rg "SQLite|Sqlite|sqlx" crates/nbd-server/src
rg "tree_nodes|tree_edges" crates/nbd-server/src
rg "tree_leaf_refs|export_heads" crates/nbd-server/src
```

The final `rg` commands must produce no matches.

Not included: PostgreSQL implementation, catalog compatibility migration for
old development databases, garbage collection, historical checkpoint APIs,
cross-format clone, auth, multi-connection semantics, blob-store behavior
changes, or NBD protocol behavior changes.

### Current-Series Commit Plan

Commit 1/10: docs: add control-plane storage adapter planning

  Summary:       Add the approved design and this single-series execution
                 contract as the durable source of truth before code changes
                 begin.
  Invariant focus: implementation starts from explicit facade/core/adapter
                   ownership, lazy row-read, and source-leakage rules rather
                   than chat-only agreement.
  Files:         docs/plans/2026-05-12-control-plane-storage-adapter-boundary.md
                 docs/execution/2026-05-12-control-plane-storage-adapter-boundary.md
  Source topology: not material: docs-only planning anchor records the
                   approved owners but does not change source layout.
  Preconditions: `$review-plan` returned ready for series planning and the
                 user requested `$plan-series`.
  Postconditions: the approved design and execution contract are present in
                  the repository with approval state ready for implementation
                  approval to be recorded before `$impl-series`.
  Evidence:      none, because this is a docs-only planning anchor.
  Review:        structures, because the commit establishes source ownership
                 and execution boundaries for the whole series.
  Verify:        git diff --check -- docs/plans docs/execution
  Not included:  no code, schema, tests, or runtime behavior changes.

Commit 2/10: control-plane: introduce core API crate

  Summary:       Add `nbd-control-plane-core` and move storage-neutral catalog
                 domain types, errors, and service traits behind it while
                 keeping `nbd-control-plane` as the public facade.
  Invariant focus: adapter crates have a neutral trait crate to implement, and
                   consumers can still import the public facade without seeing
                   concrete adapter names.
  Files:         Cargo.toml
                 crates/nbd-control-plane/Cargo.toml
                 crates/nbd-control-plane/src/lib.rs
                 crates/nbd-control-plane/src/model.rs
                 crates/nbd-control-plane/src/error.rs
                 crates/nbd-control-plane-core/Cargo.toml (new)
                 crates/nbd-control-plane-core/src/lib.rs (new)
                 crates/nbd-control-plane-core/src/model.rs (new)
                 crates/nbd-control-plane-core/src/error.rs (new)
                 crates/nbd-control-plane-core/tests/model.rs (new)
  Source topology: split: crates/nbd-control-plane-core because storage-neutral
                   traits and domain values need a crate that concrete
                   adapters can implement without depending on the facade.
  Preconditions: Commit 1 has anchored the approved planning artifacts.
  Postconditions: `nbd-control-plane-core` builds without `sqlx`;
                  `nbd-control-plane` re-exports the core API; existing facade
                  imports continue to compile.
  Evidence:      unit, because model validation and domain constructors are
                 small, stable, logic-dense primitives.
  Review:        structures, because this creates the core/facade dependency
                 boundary.
  Verify:        cargo test -p nbd-control-plane-core
                 cargo test -p nbd-control-plane
  Not included:  no SQLite crate split, no tree API redesign, no server caller
                 changes beyond mechanically following re-exports.

Commit 3/10: control-plane: move SQLite behind adapter crate

  Summary:       Add `nbd-control-plane-sqlite`, move the existing SQLite
                 catalog implementation and SQLite integration tests into it,
                 and make the facade delegate `open_catalog` to the adapter.
  Invariant focus: SQL, `sqlx`, row mapping, and SQLite transactions live in
                   the adapter crate, while callers continue using the
                   `nbd-control-plane` facade.
  Files:         Cargo.toml
                 crates/nbd-control-plane/Cargo.toml
                 crates/nbd-control-plane/src/lib.rs
                 crates/nbd-control-plane/src/catalog_url.rs
                 crates/nbd-control-plane-sqlite/Cargo.toml (new)
                 crates/nbd-control-plane-sqlite/src/lib.rs (new)
                 crates/nbd-control-plane-sqlite/src/adapter.rs (new)
                 crates/nbd-control-plane-sqlite/tests/sqlite_catalog.rs (new)
                 crates/nbd-control-plane/src/sqlite.rs
                 crates/nbd-control-plane/tests/sqlite_catalog.rs
  Source topology: split: crates/nbd-control-plane-sqlite because concrete
                   SQLite connection, SQL, rows, and transactions are an
                   adapter implementation behind the facade.
  Preconditions: Commit 2 has created the core API crate and kept facade
                 imports compiling.
  Postconditions: SQLite catalog behavior is implemented in
                  `nbd-control-plane-sqlite`; `nbd-control-plane::open_catalog`
                  returns storage-neutral handles; SQLite tests live with the
                  adapter.
  Evidence:      integration, because the real contract is SQLite persistence
                 through the catalog boundary.
  Review:        structures, because this is the concrete adapter split.
  Verify:        cargo test -p nbd-control-plane
                 cargo test -p nbd-control-plane-sqlite
  Not included:  no server tree behavior moves, no lazy row API, and no removal
                 of temporary facade compatibility needed by existing callers.

Commit 4/10: control-plane: split core domain modules

  Summary:       Split the core API away from a broad model module into focused
                 export, tree, tree-format, service, and error owners, with
                 facade re-exports preserved for external callers.
  Invariant focus: implementation modules import from the owner that defines
                   the concept instead of from one giant model namespace.
  Files:         crates/nbd-control-plane-core/src/lib.rs
                 crates/nbd-control-plane-core/src/model.rs
                 crates/nbd-control-plane-core/src/export.rs (new)
                 crates/nbd-control-plane-core/src/tree.rs (new)
                 crates/nbd-control-plane-core/src/tree_format.rs (new)
                 crates/nbd-control-plane-core/src/service.rs (new)
                 crates/nbd-control-plane/src/lib.rs
                 crates/nbd-control-plane-sqlite/src/adapter.rs
  Source topology: split: core export/tree/tree_format/service modules
                   because identity/head lifecycle, tree records, format ids,
                   and service traits are separate owners.
  Preconditions: Commit 3 has moved concrete SQLite code behind the adapter
                 crate.
  Postconditions: core modules expose focused owner paths; root re-exports
                  remain for compatibility; large implementation imports are
                  reduced where touched.
  Evidence:      none, because this is source ownership and import movement
                 proven by compile/test commands.
  Review:        structures, because this commit exists to correct ownership
                 topology and import hygiene.
  Verify:        cargo test -p nbd-control-plane-core
                 cargo test -p nbd-control-plane
                 cargo test -p nbd-control-plane-sqlite
  Not included:  no semantic tree behavior change and no server algorithm
                 movement.

Commit 5/10: catalog: store tree format on export heads

  Summary:       Add `TreeFormat` to tree-backed export heads, persist it in
                 `export_heads`, and default new tree-backed exports to the
                 first bounded format.
  Invariant focus: tree geometry is selected by durable head state rather than
                   inferred from adapter defaults or server constants.
  Files:         prisma/schema.prisma
                 prisma/migrations/<timestamp>_tree_format/migration.sql (new)
                 crates/nbd-control-plane-core/src/tree_format.rs
                 crates/nbd-control-plane-core/src/export.rs
                 crates/nbd-control-plane-core/tests/model.rs
                 crates/nbd-control-plane-sqlite/src/adapter.rs
                 crates/nbd-control-plane-sqlite/tests/sqlite_catalog.rs
                 crates/nbdcli/src/main.rs
                 crates/nbdcli/src/output.rs
  Source topology: owner: nbd-control-plane-core tree_format/export modules
                   because format id is catalog state; SQLite only persists
                   it, and CLI only displays or requests it.
  Preconditions: Commit 4 has focused core domain owners.
  Postconditions: tree-backed `ExportHead` values carry a `TreeFormat`; SQLite
                  rows persist it; clone preserves the source format; create,
                  inspect, and list paths stay truthful for memory and
                  tree-backed exports.
  Evidence:      integration, because the durable contract is schema,
                 catalog, and CLI-visible head state.
  Review:        migration, because this changes catalog schema and stored
                 head semantics while intentionally allowing dev-data
                 compatibility breakage.
  Verify:        make -C prisma db-migrate-check
                 cargo test -p nbd-control-plane-core
                 cargo test -p nbd-control-plane-sqlite
                 cargo test -p nbdcli
  Not included:  no bounded tree traversal or lazy metadata reads yet.

Commit 6/10: catalog: add lazy tree record store

  Summary:       Replace snapshot-style tree persistence as the target
                 boundary by adding lazy row-read records and atomic
                 `PublishTreeUpdate` support to the core API and SQLite
                 adapter.
  Invariant focus: adapters read bounded row sets and atomically publish tree
                   records with head CAS; they do not choose traversal or
                   mutation plans.
  Files:         crates/nbd-control-plane-core/src/tree.rs
                 crates/nbd-control-plane-core/src/service.rs
                 crates/nbd-control-plane-core/tests/model.rs
                 crates/nbd-control-plane-sqlite/src/adapter.rs
                 crates/nbd-control-plane-sqlite/src/tree_rows.rs (new)
                 crates/nbd-control-plane-sqlite/src/transaction.rs (new)
                 crates/nbd-control-plane-sqlite/tests/sqlite_catalog.rs
  Source topology: owner: core tree/service modules define storage-neutral
                   records and traits; sqlite tree_rows/transaction modules
                   own row mapping and atomic persistence.
  Preconditions: Commit 5 has durable `TreeFormat` on tree-backed heads.
  Postconditions: callers can load nodes, child edges, leaf refs, and publish
                  new tree records atomically through storage-neutral traits;
                  SQLite tests prove stale-head publish does not expose new
                  rows through a head.
  Evidence:      integration, because the boundary is SQLite persistence plus
                 transactional publication through the trait.
  Review:        code, because transaction atomicity and stale-plan behavior
                 are correctness-critical.
  Verify:        cargo test -p nbd-control-plane-core
                 cargo test -p nbd-control-plane-sqlite
  Not included:  no server engine adoption and no removal of legacy
                 `SimpleTreeMetadataStore` or `CowTreeMetadataStore` yet.

Commit 7/10: tree: add bounded geometry and lazy readers

  Summary:       Add server-owned bounded tree geometry and lazy reader/editor
                 helpers that traverse only caller-requested paths through the
                 new `TreeRecordStore` boundary.
  Invariant focus: server tree code derives geometry from stored `TreeFormat`
                   and loads metadata on demand rather than materializing whole
                   trees.
  Files:         crates/nbd-server/src/engines/tree/mod.rs
                 crates/nbd-server/src/engines/tree/read.rs
                 crates/nbd-server/src/engines/tree/geometry.rs (new)
                 crates/nbd-server/src/engines/tree/edit.rs (new)
  Source topology: owner: crates/nbd-server/src/engines/tree because geometry,
                   path traversal, lazy read caching, and edit planning are
                   storage-engine behavior, not catalog persistence.
  Preconditions: Commit 6 has lazy row-read and publish traits available.
  Postconditions: server tree helpers can resolve sparse paths and build
                  bounded update plans through an instrumented fake store;
                  tests prove a large sparse export reads only touched paths
                  and bounded sibling batches.
  Evidence:      unit, because geometry/path traversal is a stable,
                 logic-dense primitive and the fake store proves load bounds.
  Review:        structures, because this creates the server tree owner for
                 behavior previously embedded in catalog persistence.
  Verify:        cargo test -p nbd-server --lib engines::tree
                 cargo test -p nbd-server --test simple_durable
  Not included:  no simple durable or WAL durable caller cutover yet.

Commit 8/10: simple-durable: own mutable tree updates

  Summary:       Switch `SimpleMutableTree` from snapshot-style catalog calls
                 to server-owned lazy tree editing plus atomic tree record
                 publication.
  Invariant focus: simple durable chooses mutable tree materialization and
                   zero-fill behavior in server code; the adapter only persists
                   row batches and head updates.
  Files:         crates/nbd-server/src/engines/simple_durable/mutable_tree.rs
                 crates/nbd-server/src/engines/simple_durable/reader.rs
                 crates/nbd-server/src/engines/simple_durable/mod.rs
                 crates/nbd-server/src/registry/factory.rs
                 crates/nbd-server/tests/simple_durable.rs
                 crates/nbd-control-plane-sqlite/tests/sqlite_catalog.rs
  Source topology: owner: simple_durable/mutable_tree.rs because the simple
                   mutable layout's write semantics and cache refresh belong
                   to the engine that serves the data.
  Preconditions: Commit 7 has server tree geometry and lazy edit primitives.
  Postconditions: simple durable opens without loading the whole tree, commits
                  new chunks through `TreeRecordStore::publish_tree_update`,
                  and keeps existing restart/read behavior.
  Evidence:      integration, because the contract is durable restart and
                 engine/catalog interaction.
  Review:        code, because this changes the durable write path for
                 simple_durable.
  Verify:        cargo test -p nbd-server --test simple_durable
                 cargo test -p nbd-control-plane-sqlite
  Not included:  no WAL durable COW cutover and no legacy tree trait removal.

Commit 9/10: wal-durable: own COW tree publication

  Summary:       Move COW path-copy and changed-chunk publication planning into
                 WAL durable compaction code and persist the result through the
                 lazy tree record store.
  Invariant focus: WAL durable owns COW tree mutation semantics while the
                   adapter provides bounded row reads and atomic publish only.
  Files:         crates/nbd-server/src/engines/wal_durable/compaction.rs
                 crates/nbd-server/src/engines/wal_durable/read_view.rs
                 crates/nbd-server/src/engines/wal_durable/mod.rs
                 crates/nbd-server/src/registry/factory.rs
                 crates/nbd-server/tests/wal_durable.rs (?)
                 crates/nbd-control-plane-sqlite/tests/sqlite_catalog.rs
  Source topology: owner: wal_durable/compaction.rs because COW path-copy,
                   unchanged-subtree reuse, WAL checkpoint publication, and
                   changed-chunk decisions are WAL engine semantics.
  Preconditions: Commit 8 has proven server-owned tree publication on the
                 simple mutable layout.
  Postconditions: WAL close, hard, and background compaction publish COW roots
                  through server-planned tree updates; clone and read-view
                  behavior remain correct.
  Evidence:      integration, because the contract spans WAL replay,
                 compaction, catalog publication, clone, and read views.
  Review:        code, because COW publication and stale-plan behavior are
                 correctness-critical.
  Verify:        cargo test -p nbd-server --test wal_durable
                 cargo test -p nbd-server --lib engines::wal_durable
                 cargo test -p nbd-control-plane-sqlite
  Not included:  no GC, historical checkpoint browsing, or cross-format clone.

Commit 10/10: catalog: remove legacy tree metadata APIs

  Summary:       Remove snapshot-style tree metadata store traits and any
                 remaining adapter leaks from facade/server imports, leaving
                 only the lazy row-read and atomic publish boundary.
  Invariant focus: final source topology matches the design: server code sees
                   only the control-plane facade and neutral handles, and
                   database implementation details stay in adapter crates.
  Files:         crates/nbd-control-plane-core/src/service.rs
                 crates/nbd-control-plane-core/src/tree.rs
                 crates/nbd-control-plane-core/src/lib.rs
                 crates/nbd-control-plane/src/lib.rs
                 crates/nbd-control-plane-sqlite/src/adapter.rs
                 crates/nbd-control-plane-sqlite/src/tree_rows.rs
                 crates/nbd-server/src/registry/factory.rs
                 crates/nbd-server/src/registry/mod.rs
                 crates/nbd-server/src/engines/simple_durable/mod.rs
                 crates/nbd-server/src/engines/wal_durable/mod.rs
                 crates/nbd-server/src/engines/wal_durable/compaction.rs
                 crates/nbdcli/src/main.rs
  Source topology: owner: core service/tree modules expose only the final
                   storage-neutral boundary; facade re-exports remain public
                   compatibility paths but no longer expose concrete adapters.
  Preconditions: Commits 8 and 9 have moved all live simple and COW tree
                 callers to `TreeRecordStore`.
  Postconditions: `SimpleTreeMetadataStore`, `CowTreeMetadataStore`, full-tree
                  snapshot loaders, and `SQLiteExportCatalog` facade exports
                  are gone; source leakage checks pass.
  Evidence:      integration, because final proof requires all crates and
                 cross-crate imports to agree on the new boundary.
  Review:        structures, because this is the final topology checkpoint and
                 removes compatibility paths.
  Verify:        cargo fmt --all --check
                 cargo test -p nbd-control-plane-core
                 cargo test -p nbd-control-plane
                 cargo test -p nbd-control-plane-sqlite
                 cargo test -p nbd-server
                 cargo test -p nbdcli
                 cargo test --workspace
                 cargo clippy --workspace --all-targets -- -D warnings
                 rg "SQLite|Sqlite|sqlx" crates/nbd-server/src
                 rg "tree_nodes|tree_edges" crates/nbd-server/src
                 rg "tree_leaf_refs|export_heads" crates/nbd-server/src
  Not included:  no PostgreSQL adapter, no database compatibility migration,
                 no tree GC, and no NBD protocol behavior changes.
