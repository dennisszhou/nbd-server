Title: Rust Workspace And Test Harness
Date: 2026-05-01
Status: approved

# Problem

The first implementation needs a Rust project shape that can support the
control plane, NBD server, userspace validation-client integration tests, and
later Docker smoke tests without mixing developer-local state into tests.

The important first checkpoint is not NBD behavior yet. It is a clean runtime
and test foundation:

- predictable crate boundaries;
- explicit config loading;
- isolated temporary test databases;
- no accidental dependence on `~/.nbd`;
- simple `make` commands for the normal local loop;
- test helpers that later integration tests can reuse.

# Goal

Create the initial Rust workspace, config model, and test harness so later
control-plane and NBD protocol work can land behind clean boundaries.

This design covers M0 from the initial integration roadmap:

```text
workspace + config + temp DB harness
```

# Constraints

- Runtime code should be Rust.
- Prisma is schema/migration tooling, not the Rust runtime ORM.
- Tests must not read or write the developer's default `~/.nbd` state.
- Tests must create their own temporary database and remove it with the temp
  directory.
- Config must support an explicit path for tests and automation.
- The default operator config path is `~/.nbd/config.toml`.
- If the default operator config does not exist, loading it should create one.
- The first workspace should not force WAL, storage, compaction, or protocol
  implementation decisions.

# Non-Goals

- Implementing the export catalog schema.
- Running Prisma migrations.
- Implementing `nbdcli`.
- Implementing the NBD protocol.
- Implementing Docker or kernel-NBD smoke tests.
- Designing WAL, `ExportReadView`, S3, or compaction.

# End State

After this slice:

- `cargo test` works from the repository root.
- The workspace has a small set of crates for shared runtime/test
  infrastructure.
- A config loader can load either an explicit config path or the default
  operator path.
- Default config loading creates `~/.nbd/config.toml` when it is missing.
- A test harness can create an isolated runtime directory and SQLite database
  path.
- A root `Makefile` exposes `test`, `fmt`, and `clippy` targets.
- Tests can prove that explicit test config does not touch `~/.nbd`.
- Later catalog and protocol tests can reuse the harness instead of inventing
  their own temp-state setup.

# Proposed Approach

Use a Rust workspace with crates introduced only when they carry a clear
boundary.

Initial crate targets:

```text
crates/nbd-config
  config structs, default ~/.nbd bootstrap, and path handling

crates/nbd-test-support
  integration-test helpers for temp runtime state and config overrides
```

Later roadmap slices can add:

```text
crates/nbd-control-plane
  ExportCatalog / ExportLifecycleManager SDK

crates/nbdcli
  CLI wrapper over nbd-control-plane

crates/nbd-protocol
  NBD wire parsing and encoding

crates/nbd-server
  server binary and toy in-memory export

crates/nbd-client
  small userspace validation client for TCP protocol tests

crates/nbd-storage-engine
  future StorageEngine trait and local/S3 implementations
```

Do not create all future crates with empty public APIs in M0. In particular,
do not create `nbd-storage-engine` until a storage design needs it. The
workspace should make room for future crates, but each crate should land when
it has a real contract and tests.

# Config Model

Config should be a structured runtime object, not global process state.
Use TOML for the operator-facing config file.

```rust
struct NbdConfig {
    catalog: CatalogConfig,
    runtime: RuntimeConfig,
}

struct CatalogConfig {
    url: String,
}

struct RuntimeConfig {
    state_dir: PathBuf,
}
```

The first catalog URL can be a local SQLite file URL:

```text
file:/absolute/path/to/catalog.db
```

Postgres can later use the same field shape:

```text
postgres://...
```

Config loading should distinguish:

```rust
enum ConfigSource {
    ExplicitPath(PathBuf),
    DefaultUserPath,
}
```

The operator default path is:

```text
~/.nbd/config.toml
```

If `ConfigSource::DefaultUserPath` is requested and the config file does not
exist, the loader should create `~/.nbd`, write a default config, and then load
it. The default catalog URL should point at:

```text
file:/Users/example/.nbd/catalog.db
```

The default config contents should be:

```toml
[catalog]
url = "file:/Users/example/.nbd/catalog.db"

[runtime]
state_dir = "/Users/example/.nbd"
```

The written config should contain expanded absolute paths, not a literal `~`.
That keeps the config unambiguous for database clients and for future
Postgres/SQLite URL handling.

Tests should use `ConfigSource::ExplicitPath` or direct config construction.
They should not rely on changing the real process home directory unless a
specific test is proving default-path behavior.

Initial config shape:

```toml
[catalog]
url = "file:/absolute/path/to/catalog.db"

[runtime]
state_dir = "/absolute/path/to/state"
```

# Test Harness

`nbd-test-support` should expose a test runtime fixture:

```rust
struct TestRuntime {
    root: TempDir,
    config_path: PathBuf,
    state_dir: PathBuf,
    catalog_path: PathBuf,
    catalog_url: String,
}
```

The fixture should:

- create a temp directory;
- create a config file under that temp directory;
- choose a SQLite database path under that temp directory;
- expose the explicit config path and catalog URL;
- delete all test artifacts when dropped.

`nbd-test-support` should contain only test infrastructure:

- temp directory/runtime builders;
- temp config file creation;
- temp SQLite database path and URL helpers;
- helpers that assert paths are inside the temp runtime root;
- later, process/server harness helpers for integration tests;
- later, validation-client test helpers if they do not belong in `nbd-client`.

It should not contain production config parsing, catalog behavior, protocol
logic, or storage behavior.

Tests should prefer:

```rust
let runtime = TestRuntime::new()?;
let config = NbdConfig::load(
    ConfigSource::ExplicitPath(runtime.config_path()),
)?;
```

The fixture should not create schema tables yet. M1 owns migrations and schema.
For M0, it is enough to prove that the runtime can produce isolated paths and
load config.

`nbd-test-support` must be a test-only dependency. Production crates should not
depend on it.

# Source Of Truth

For M0:

- `NbdConfig` is the runtime source of truth after loading.
- `~/.nbd/config.toml` is only the operator default source.
- Missing default operator config is bootstrapped from built-in defaults.
- Test fixtures own test config/database paths.
- No database schema is authoritative yet.

Later slices will make Prisma migrations the schema source of truth and
`ExportCatalog` the runtime metadata boundary.

# Invariants

- Tests never silently use `~/.nbd`.
- Default config bootstrap only happens through `ConfigSource::DefaultUserPath`.
- Test databases live under a temp directory owned by the test fixture.
- Dropping the test fixture removes its database and config artifacts.
- Runtime code receives config explicitly after startup.
- Config loading has no hidden global mutable state.
- `nbdcli`, server, and tests will share the same config structures.
- Production crates do not depend on `nbd-test-support`.
- The first workspace does not add empty crates for future behavior.

# Alternatives Considered

## Flat Single Crate

A single crate would be simpler at the first commit, but it would blur test
support, CLI, protocol, and runtime SDK boundaries as soon as M1/M2 land.

## Create Every Future Crate Immediately

Creating all future crates upfront makes the layout look complete but creates
dormant APIs before their contracts exist. The better compromise is to create
only `nbd-config` and `nbd-test-support` first, then add catalog/protocol/server
crates with their design docs.

## Shared `nbd-core`

A broad `nbd-core` crate is intentionally avoided for M0. Shared code should
start with a narrower name such as `nbd-config`. If later repeated domain
types need a home, introduce a focused crate then rather than starting with a
general-purpose bucket.

# Makefile Direction

The repository should provide easy `make` entry points for common developer
workflows. M0 should include the non-Docker commands:

```text
make test
make fmt
make clippy
```

Expected future commands:

```text
make docker-build
make docker-run
make docker-smoke
```

The Docker/kernel-NBD design doc should define the exact commands, required
privileges, and cleanup behavior. Normal unit and integration tests should
remain runnable through `cargo test --workspace` and `make test` without
Docker.

## Tests Override `HOME`

Overriding `HOME` can test default-path behavior, but it is too implicit for
most integration tests. Explicit config paths are clearer and safer.

# Migration / Rollout

Not needed. This is the initial workspace and test harness.

# Validation Strategy

M0 should prove the harness, not the NBD protocol.

Expected checks:

- `cargo test --workspace`
- `make test` as a thin wrapper around the test command
- `make fmt` as a thin wrapper around `cargo fmt`
- `make clippy` as a thin wrapper around `cargo clippy --workspace`
- unit tests for config parsing and default path resolution
- integration test proving explicit config loads from a temp directory
- integration test proving test runtime paths are under the temp directory
- integration test proving fixture cleanup removes the temp database path
- test proving default config bootstrap writes expected config under an
  isolated home directory

The cleanup test should avoid depending on OS-specific timing. It can capture
the temp root path, drop the fixture, and assert the root no longer exists.

# Risks

- Overbuilding the workspace before component contracts exist.
- Letting tests accidentally use the real `~/.nbd` config.
- Treating config path lookup as global state instead of startup input.
- Choosing a SQLite URL format that later fights the Rust DB client.
- Adding Prisma workflow assumptions before the catalog schema design.
- Letting `nbd-test-support` become a production dependency or junk drawer.
- Making Docker required for the normal test loop.

# Open Questions

None.

# Design Exit Criteria

This design is ready for `$review-plan` when:

- the initial crate boundaries are accepted;
- explicit test config is accepted as the default testing pattern;
- `~/.nbd/config.toml` is accepted as operator default only;
- default config bootstrap behavior is accepted;
- the temporary database cleanup contract is accepted; and
- `nbd-test-support` is accepted as test-only infrastructure;
- `make test`, `make fmt`, and `make clippy` are accepted as M0 commands.

# Recommended Next Step

Run `$review-plan` on this design before turning it into an execution series.
