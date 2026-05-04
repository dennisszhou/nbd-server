Title: Concurrent Runtime Default And Fixed Tokio Workers
Date: 2026-05-03
Status: approved

# Problem

Series 5 introduced `ConcurrentExportRuntime` behind process config, but the
operator default still selects `SerialExportRuntime`. That made the risky
runtime rollout conservative, but it now leaves the base server path on the
less representative scheduling policy.

The `nbd-server` binary also uses Tokio's default multi-thread worker count.
With the current `#[tokio::main]` macro and no explicit worker count, Tokio
uses `TOKIO_WORKER_THREADS` when present and otherwise uses the process
available parallelism. That makes the worker count host-dependent. For the
next default-concurrent slice, we want a fixed and easy-to-reason-about worker
count.

# Goal

Make the base server runtime policy concurrent by default and pin the
`nbd-server` binary to four Tokio worker threads.

The intended behavior is:

- missing `[server].export_runtime` selects `ConcurrentExportRuntime`;
- explicit `export_runtime = "serial"` still selects `SerialExportRuntime`;
- explicit `export_runtime = "concurrent"` remains valid;
- `nbd-server serve` starts the binary on Tokio's multi-thread runtime with
  exactly four worker threads;
- Docker kernel smoke exercises the default concurrent runtime path instead of
  the old default serial path;
- userspace protocol tests retain explicit serial coverage.

# Constraints

- This is a process runtime policy change, not catalog metadata.
- Existing exports and catalog rows must not change shape.
- `exports.engine_kind` still selects the storage engine. It must not be
  confused with the export runtime kind.
- The server still must not advertise `NBD_FLAG_CAN_MULTI_CONN`.
- The current protocol path still assigns one synthetic `ExportOwner` per
  connection, so production multi-connection remains disabled.
- `export_queue_depth` remains the accepted outstanding export request budget.
  It is not an OS-thread count.
- Fixed Tokio worker count applies to the `nbd-server` binary startup path.
  Library tests that call `NbdServer::start` run inside the test runtime that
  the test harness creates.

# Non-Goals

- Add a dynamic `tokio_worker_threads` config setting.
- Change `export_queue_depth` or `reply_queue_capacity` defaults.
- Remove `SerialExportRuntime`.
- Collapse `SerialExportRuntime` into `ConcurrentExportRuntime` with queue
  depth one.
- Add production client identity or advertise `NBD_FLAG_CAN_MULTI_CONN`.
- Implement durable storage, WAL, read views, storage work queues, resize, or
  concurrent kernel multi-connection coverage.

# End State

After this change, default config loads as:

```toml
[server]
export_runtime = "concurrent"
export_queue_depth = 128

[server.connection]
reply_queue_capacity = 128
```

The literal `export_runtime = "concurrent"` line may be omitted from operator
config because it is the default, but generated/default config should make the
default visible if the current serializer writes it.

The server binary starts as if configured with:

```rust
#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
```

The local registry continues to choose the runtime from `ServerConfig` when an
export is opened. Already active exports are unaffected by later config edits.

# Proposed Approach

Flip the default runtime kind at the config boundary. The narrowest code shape
is to move the derived `Default` marker on `ExportRuntimeKind` from `Serial` to
`Concurrent`:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum ExportRuntimeKind {
    Serial,
    #[default]
    Concurrent,
}
```

`ServerConfig::default` can continue to use `ExportRuntimeKind::default()`.
Serde config loading already uses `#[serde(default)]` on
`ServerConfig.export_runtime`, so missing runtime config inherits the new
default automatically. Explicit `serial` and `concurrent` values keep their
existing serialized spellings.

Pin the binary worker count in `crates/nbd-server/src/main.rs` by changing the
Tokio macro to:

```rust
#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
```

This deliberately does not introduce a config value. Tokio constructs the async
runtime before `run()` loads `NbdConfig`, so a runtime worker count in
`NbdConfig` would be misleading unless startup were rewritten around
`tokio::runtime::Builder`.

Update tests so the default path and explicit paths are distinct:

- config tests should assert missing server runtime config defaults to
  `Concurrent`;
- explicit `export_runtime = "serial"` should continue to load as `Serial`;
- explicit `export_runtime = "concurrent"` should continue to load as
  `Concurrent`;
- local registry tests that use `ServerConfig::default()` should expect a
  concurrent runtime close error where they currently expect serial;
- local registry tests should add or preserve explicit serial coverage;
- protocol integration should treat the existing fixture default as concurrent;
- protocol integration should add an explicit serial smoke so serial remains
  covered after the default flips.

Docker smoke should remain config-minimal. Because the smoke script writes no
`[server].export_runtime`, it will validate the new default concurrent kernel
path after this change. The script should not grow an override just to preserve
the old serial smoke as the default evidence.

# Data Model / API Shape

Authoritative state:

- `ServerConfig.export_runtime` is the process-local source of truth for new
  active export runtime selection.
- `ExportRuntimeKind::default()` defines the behavior for missing
  `export_runtime` config.
- `LocalExportRegistry.active` remains the process-local active export truth.
- The Tokio worker count is binary startup policy, not runtime config.

Derived state:

- A loaded `NbdConfig` derives its server runtime kind from explicit config or
  `ExportRuntimeKind::default()`.
- A newly opened active export derives its `Arc<dyn ExportRuntime>` from the
  loaded `ServerConfig` at open time.

Cached state:

- None. Active runtime handles are live process state, not cached config.

No catalog schema or migration is needed.

# Invariants

- Missing `export_runtime` means `Concurrent`, not `Serial`.
- Explicit `export_runtime = "serial"` remains valid and selects
  `SerialExportRuntime`.
- Explicit `export_runtime = "concurrent"` remains valid and selects
  `ConcurrentExportRuntime`.
- Runtime kind changes apply only to newly opened exports.
- Queue depth and reply queue capacity remain nonzero config values.
- Queue depth bounds accepted outstanding export jobs; four Tokio workers bound
  scheduler worker threads for the server binary.
- Docker smoke uses the same default runtime policy an operator gets from a
  missing `export_runtime` field.
- Serial runtime remains covered by an explicit config path.
- `NBD_FLAG_CAN_MULTI_CONN` remains unadvertised.

# Alternatives Considered

Keep serial as default:

- This avoids behavior change, but it leaves the base path on the less
  representative runtime after the concurrent path has protocol, runtime, and
  Docker evidence.

Make Tokio worker count configurable now:

- This is premature. `NbdConfig` is loaded after `#[tokio::main]` has already
  built the runtime. Doing this truthfully would require replacing the macro
  with an explicit synchronous `main` and `tokio::runtime::Builder`.

Set worker count through `TOKIO_WORKER_THREADS`:

- This keeps code unchanged, but it makes the base behavior depend on operator
  environment. The requested policy is fixed at four workers for now.

Keep Docker smoke on explicit serial:

- This preserves old evidence but fails to prove the new default kernel path.
  Serial should remain covered through explicit userspace protocol tests and
  runtime tests instead.

# Migration / Rollout

This is a behavior change for config files that omit `server.export_runtime`.
Those configs will select `ConcurrentExportRuntime` after the change.

Configs with `export_runtime = "serial"` keep serial behavior. Operators that
need the conservative runtime can pin that setting explicitly.

No data migration is required. Existing active exports inside a running process
are not reconfigured; the server process must restart and reopen exports to
pick up the new default.

# Validation Strategy

Targeted validation:

```text
cargo test -p nbd-config
cargo test -p nbd-server --test local_export_registry
cargo test -p nbd-server --bin nbd-server
make test-protocol
```

Full handoff validation:

```text
cargo test --workspace
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
make docker-smoke
```

Expected evidence:

- config tests prove missing runtime config defaults to concurrent;
- config tests prove explicit serial and concurrent values still parse;
- registry tests prove default config opens a concurrent runtime;
- registry tests prove explicit serial config still opens a serial runtime;
- userspace protocol tests prove default concurrent transmission behavior and
  explicit serial transmission behavior;
- binary tests and clippy prove the fixed-worker Tokio macro compiles;
- Docker smoke proves the default concurrent kernel path can mount, write,
  drop caches, and read back data.

# Risks

- Default config behavior changes for operators who were implicitly relying on
  serial execution. The mitigation is explicit `export_runtime = "serial"`.
- Docker smoke will now exercise concurrent runtime by default. Failures there
  should be treated as default-path regressions, not as optional concurrent
  smoke failures.
- The fixed four-worker policy is intentionally blunt. It may be too low or too
  high for some hosts, but it is predictable and can be revisited with an
  explicit runtime-builder startup design.
- Protocol integration tests run inside the test harness runtime, so they do
  not prove the binary has exactly four Tokio workers. They prove the default
  `ServerConfig` runtime choice. Binary compile/clippy validates the macro
  policy.

# Open Questions

None.

# Design Exit Criteria

- The user agrees that concurrent should be the default for missing
  `export_runtime`.
- The user agrees that four Tokio workers should be hard-coded in the server
  binary for now.
- The user agrees that Docker smoke should move to the new default concurrent
  path rather than preserving default serial smoke.
- The user agrees that serial coverage can remain explicit rather than default.

# Recommended Next Step

Run `$review-plan` on this design. If it is accepted, use `$plan-series` to
turn it into a short commit stack.
