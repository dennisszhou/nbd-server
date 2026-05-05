Title: Logging Instrumentation
Date: 2026-05-04
Status: approved

Problem
- `nbd-server` has almost no durable runtime visibility today. Startup uses
  `println!`, top-level failures use `eprintln!`, and the request/runtime path
  does not record enough context to reconstruct what happened after a server
  issue.
- The server is now async and request-pipelined. A useful log model must
  correlate events across connection, export runtime, admission, engine, and
  reply-writing tasks without assuming one OS thread maps to one unit of work.
- Upcoming durable storage, WAL, compaction, and recovery work will be hard to
  debug if the server does not have a stable operational log and request
  correlation vocabulary.

Goal
- Add one process-wide Rust `tracing` instrumentation model for the server.
- Write durable local JSON-lines logs by default to:

```text
/tmp/nbd/current.log
```

- Keep file logging on by default for `nbd-server serve`.
- Add a config-file logging path with `/tmp/nbd/current.log` as the default.
- Add `nbd-server serve --log-stdout` to mirror the same filtered logs to
  stdout for containers and interactive debugging.
- Distinguish operational events from request/admission/storage events through
  structured targets and fields, not by creating separate logging APIs.
- Make request-level detail available when needed without making normal
  operation noisy.
- Preserve enabled log records during normal operation. If logging is enabled
  and the writer queue fills, the server should apply logging backpressure
  rather than silently dropping records.
- Keep library crates responsible for emitting events and spans, while the
  `nbd-server` binary owns subscriber setup.
- Keep log destination, format, filtering, and writer behavior behind one
  logging policy boundary so future changes do not touch request/runtime call
  sites.

Constraints
- The default path is `/tmp/nbd/current.log` for the first implementation so
  local macOS, Docker, and Linux development all have a writable default.
- Logging setup must create the configured parent directory when it is missing.
- Logging setup failure is a startup error. A silent fallback to no durable file
  would violate the operator contract.
- The active log file is appended by default so clean restarts preserve earlier
  context in the same local diagnostic trail.
- Enabled log records are part of the local audit/debug trail. The v1 writer
  policy is lossless under normal operation, even though that means excessive
  enabled logging can slow request handling.
- The request path must not log payload bytes.
- Storage logs must avoid absolute local file paths by default. Blob keys,
  chunk indexes, offsets, and lengths are enough for normal diagnostics.
- The server should not initialize `tracing` from library APIs such as
  `NbdServer::start`; only the binary entrypoint should install the global
  subscriber.
- Request, runtime, admission, and engine code must not mention log file paths,
  stdout policy, JSON formatting, append/truncate behavior, or writer
  implementation details.
- Instrumentation call sites may choose target, level, event name, spans, and
  structured fields. Subscriber setup owns every destination and formatting
  decision.
- Tokio task instrumentation must not hold synchronous span guards across
  `.await`. Spawned futures should be instrumented with spans instead.
- `nbdcli` remains an interactive CLI for this slice. Durable daemon-style
  logging for `nbdcli` is out of scope.

Non-goals
- Metrics, histograms, dashboards, Prometheus, or OpenTelemetry export.
- A separate request log file in v1.
- Application-managed log rotation in v1.
- Runtime log-level reload.
- Audit logging.
- Per-export log files.
- Logging full write payloads, read payloads, catalog URLs, or full local blob
  paths.
- Changing the NBD protocol or storage correctness behavior.

End state
- Running `nbd-server serve` writes JSON-lines records to
  `/tmp/nbd/current.log`.
- Running `nbd-server serve --log-stdout` writes the same JSON-lines records to
  the file and also mirrors them to stdout.
- The generated default config includes:

```toml
[logging]
file_path = "/tmp/nbd/current.log"
```

- Existing config files without `[logging]` continue to load and use
  `/tmp/nbd/current.log`.
- `RUST_LOG` controls filtering. If it is unset, the server uses a conservative
  default filter that keeps operational logs on and request internals quiet.
- Normal server lifecycle events are visible at `INFO`.
- Request summaries are available at `DEBUG`.
- Admission, queue handoff, and storage-inner events are available at `TRACE`
  or targeted `DEBUG`, depending on the event.
- A single request can be followed from protocol decode through queue
  reservation, admission, engine execution, completion handoff, and socket
  reply using structured fields rather than message parsing.

Proposed approach
- Use the Rust `tracing` ecosystem:
  - `tracing` for spans and events;
  - `tracing-subscriber` for JSON formatting and `RUST_LOG` filtering;
  - `tracing-appender` for non-blocking file output.
- Keep v1 observability inside `crates/nbd-server`. Do not create a separate
  workspace crate until multiple long-running binaries need the same taxonomy.
- Split the implementation into two internal modules:
  - `logging.rs` owns subscriber setup, destinations, filters, writer policy,
    and `LoggingGuard`;
  - `observability.rs` owns target constants, event-name constants,
    correlation context types, and helper functions for repeated event shapes.
- Add one server-local logging module that owns policy and bootstrap,
  conceptually:

```rust
const DEFAULT_LOG_FILTER: &str =
    "info,nbd_server::request=warn,\
     nbd_server::admission=warn,\
     nbd_server::storage=warn";

enum LogFormat {
    JsonLines,
}

enum LogDestination {
    File { path: PathBuf },
    Stdout,
}

enum LogWriterQueuePolicy {
    Lossless,
}

struct LoggingPolicy {
    destinations: Vec<LogDestination>,
    format: LogFormat,
    filter: LogFilterSource,
    append: bool,
    writer_queue_policy: LogWriterQueuePolicy,
}

struct LoggingOptions {
    file_path: PathBuf,
    log_stdout: bool,
    env_filter: Option<String>,
}

struct LoggingGuard {
    // Keeps non-blocking tracing workers alive until process shutdown.
}

impl LoggingPolicy {
    fn from_options(options: LoggingOptions) -> Self;
}

fn init_logging(policy: LoggingPolicy) -> Result<LoggingGuard>;
```

- `LoggingOptions` is the outer CLI/config shape. `LoggingPolicy` is the
  normalized internal source of truth for where logs go and how they are
  encoded.
- The rest of the server should not receive `LoggingOptions` or
  `LoggingPolicy`. Once the subscriber is installed, modules only emit
  `tracing` spans and events.
- The binary parses `--log-stdout`, loads config, builds `LoggingOptions` from
  `config.logging.file_path`, the CLI stdout flag, and `RUST_LOG`, initializes
  logging, and keeps the `LoggingGuard` alive for the full `serve` lifetime.
- The binary should stop using normal `println!` for routine serve status.
  Startup status should be emitted as structured logs. Fatal startup errors may
  still go to stderr because logging initialization itself can fail.
- Use one file sink in v1. Targets and fields define the logical log streams:

```text
nbd_server::ops          process, listener, registry, lifecycle events
nbd_server::connection   socket accept, handshake, negotiation, close
nbd_server::export       export open, close, engine/runtime selection
nbd_server::request      per-request summaries and protocol correlation
nbd_server::runtime      queue-depth, runtime close, task lifecycle
nbd_server::admission    ticket registration, grant, cancel, release
nbd_server::engine       engine load and request execution summaries
nbd_server::storage      blob/tree/storage inner operations
nbd_server::catalog      server-side catalog calls and tree commits
```

- If request volume makes the main file hard to use later, add a second
  subscriber layer that routes selected targets to a separate file. The call
  sites should not change for that split.

Policy boundary
- Logging policy is centralized in the logging module. It owns:
  - creation of parent directories;
  - append versus truncate behavior;
  - JSON-lines formatting;
  - stdout mirroring;
  - `RUST_LOG` parsing and fallback filter selection;
  - non-blocking writer setup, lossless queue policy, and guard lifetime.
- `nbd-config` owns the default log path and config-file shape. The logging
  module owns runtime normalization and subscriber construction.
- Server subsystems own only instrumentation semantics. They decide:
  - whether an event is worth logging;
  - the event name;
  - the target;
  - the level;
  - the stable structured fields.
- No subsystem should construct a file appender, open a log path, inspect
  `RUST_LOG`, or branch on `--log-stdout`.
- Future changes such as moving the default path to `~/.nbd/logs/current.log`,
  adding rotation, splitting request logs, or exporting OTEL logs should be
  logging-module changes plus tests, not request-path rewrites.
- The event taxonomy is a design contract mirrored in code by constants and
  small helper functions. Repeated or field-heavy events should use helpers.
  Simple lifecycle events may use direct `tracing!` calls with taxonomy
  constants.

Data model / API shape
- Add a top-level config section:

```rust
const DEFAULT_LOG_FILE_PATH: &str = "/tmp/nbd/current.log";

struct NbdConfig {
    catalog: CatalogConfig,
    runtime: RuntimeConfig,
    server: ServerConfig,
    logging: LoggingConfig,
}

struct LoggingConfig {
    file_path: PathBuf,
}
```

- `LoggingConfig::default()` uses `/tmp/nbd/current.log`.
- `NbdConfig` defaults the whole `logging` section so older config files remain
  valid.
- Generated default config writes the explicit `[logging]` section so operators
  can see and edit the log file path.
- Add process-local correlation identifiers:

```rust
struct ServerInstanceId(String); // random per process start
struct ConnectionId(u64);        // monotonic per process
struct RequestSequence(u64);     // monotonic per connection
```

- A request is uniquely identified in logs by:

```text
server_instance_id
connection_id
request_sequence
cookie
```

- `cookie` remains the NBD client-provided correlation value. It is not enough
  by itself because it is scoped to a connection and may be reused later.
- Add a request context carried by accepted export work, conceptually:

```rust
struct ExportJobContext {
    connection_id: ConnectionId,
    request_sequence: RequestSequence,
    cookie: NbdCookie,
    command: &'static str,
    offset: Option<u64>,
    length: Option<u64>,
    reply_kind: ReplyKind,
}

struct ExportJob {
    context: ExportJobContext,
    request: ExportRequest,
    completion: ExportCompletion,
    queue_slot: ExportQueueSlot,
}
```

- Do not carry a `tracing::Span` inside `ExportJob`. Carry plain context data
  and let `ExportRuntime` create the request span when it executes the job.
  This keeps the work item meaningful outside one tracing implementation.
- `ExportRuntime` combines `ExportJobContext` with `ExportMeta` to create a
  request span:

```text
request
  server_instance_id
  connection_id
  request_sequence
  cookie
  command
  offset
  length
  export_id
  export_name
  engine_kind
  runtime_kind
```

- `ConcurrentExportRuntime` must instrument spawned request tasks with that
  span. `SerialExportRuntime` must instrument the awaited job future with that
  span. Engine and storage events emitted inside those futures inherit the
  request context.
- Events that are useful as standalone log lines should still repeat their key
  fields directly. Logs should not require reconstructing state exclusively
  from nested span lists.

Event taxonomy
- Use stable event names in an `event` field. The human-readable message may
  change, but the event name and important structured fields should remain
  stable.
- Core fields appear on every record:

```text
event
level
target
service = "nbd-server"
server_instance_id
pid
```

- Server events use target `nbd_server::ops`.

```text
events:
  logging.initialized
  server.starting
  server.listening
  server.shutdown.started
  server.shutdown.completed
  server.error

fields:
  listen_addr
  config_source
  log_file_path
  duration_ms
  error
```

- Connection events use target `nbd_server::connection`.

```text
events:
  connection.accepted
  connection.handshake.completed
  connection.negotiation.started
  connection.negotiation.completed
  connection.disconnect.received
  connection.closed
  connection.error

fields:
  connection_id
  peer_addr
  export_name      -- only after negotiation identifies an export
  owner_id         -- only after export open succeeds
  duration_ms
  error
```

- Export events use target `nbd_server::export`.

```text
events:
  export.open.started
  export.open.completed
  export.open.rejected
  export.close.started
  export.close.completed
  export.runtime.selected
  export.engine.loaded

fields:
  export_id        -- after catalog load
  export_name
  owner_id
  engine_kind
  runtime_kind
  size_bytes
  queue_depth
  connections
  error
```

- Request events use target `nbd_server::request`.

```text
events:
  request.decoded
  request.submitted
  request.completed
  request.failed
  request.reply_written

fields:
  connection_id
  request_sequence
  cookie
  command
  offset           -- read/write only
  length           -- read/write only
  reply_kind
  status
  duration_ms
  error
```

- Runtime events use target `nbd_server::runtime`.

```text
events:
  queue.reserve.wait
  queue.reserve.acquired
  runtime.submit
  runtime.task.started
  runtime.task.completed
  runtime.closed

fields:
  export_id
  export_name
  runtime_kind
  queue_depth
  active_jobs
  connection_id    -- request-scoped runtime events only
  request_sequence -- request-scoped runtime events only
  cookie           -- request-scoped runtime events only
  duration_ms
  error
```

- Admission events use target `nbd_server::admission`.

```text
events:
  admission.registered
  admission.rejected
  admission.granted
  admission.cancelled
  admission.released

fields:
  export_id
  export_name
  admission_ticket
  admission_op
  range_start
  range_len
  connection_id
  request_sequence
  cookie
  duration_ms
  error
```

- Engine events use target `nbd_server::engine`.

```text
events:
  engine.execute.started
  engine.execute.completed
  engine.execute.failed
  engine.flush.completed

fields:
  export_id
  export_name
  engine_kind
  command
  offset
  length
  duration_ms
  error
```

- Storage events use target `nbd_server::storage`.

```text
events:
  blob.read
  blob.create
  blob.replace
  blob.directory_synced
  blob.error

fields:
  engine_kind
  blob_op
  blob_key
  chunk_index
  storage_offset
  storage_len
  duration_ms
  error
```

- Catalog events use target `nbd_server::catalog` for server-side catalog calls
  and simple tree commits. Do not force reusable catalog crates to depend on
  `nbd-server` taxonomy; emit these at the server boundary or mirror constants
  only when a crate intentionally participates in the daemon log contract.

```text
events:
  catalog.connect.started
  catalog.connect.completed
  catalog.export.loaded
  catalog.tree.loaded
  catalog.tree.commit.started
  catalog.tree.commit.completed
  catalog.error

fields:
  catalog_provider
  export_id
  export_name
  layout_kind
  root_node_id
  chunk_count
  duration_ms
  error
```

Request lifecycle logging
- Normal `INFO` logs should not emit one event per block request.
- At `DEBUG`, emit one request completion summary after the reply has been
  written or dropped during connection shutdown:

```text
event = "request.completed"
command
offset
length
status
duration_ms
```

- At `TRACE`, request internals may record:

```text
request.decoded
queue.reserve.wait
queue.reserve.acquired
runtime.submit
admission.registered
admission.granted
admission.released
engine.execute.started
engine.execute.completed
request.reply_written
```

- Error paths should record enough context to diagnose the failed phase:

```text
event = "request.failed"
phase = "admission" | "engine" | "reply_write" | "decode"
error
```

- Disconnect should be a connection lifecycle event, not a failed request.

Log filtering
- Default filter:

```text
info,nbd_server::request=warn,\
nbd_server::admission=warn,\
nbd_server::storage=warn
```

- Debugging request flow:

```text
RUST_LOG=info,nbd_server::request=debug
```

- Debugging admission ordering:

```text
RUST_LOG=info,nbd_server::request=debug,nbd_server::admission=trace
```

- Debugging simple durable storage:

```text
RUST_LOG=info,nbd_server::request=debug,nbd_server::storage=trace
```

Lifecycle contracts
- Argument parsing happens first, then config load, then logging
  initialization.
- Config-load failures are reported to stderr because the configured log file
  path is not trustworthy until config load succeeds.
- After config load succeeds, catalog startup failures, bind failures, export
  errors, and server runtime events should be logged to the configured file.
- If logging initialization fails after config load, `main` reports the error to
  stderr and exits non-zero.
- The non-blocking writer guard must live until `serve` exits. Dropping it
  early is an implementation bug because later logs may be lost.
- The writer should use a background worker so ordinary logging does not
  synchronously fsync or write from request tasks.
- The writer queue policy is lossless in v1. If the queue fills, logging may
  apply backpressure to the caller rather than dropping enabled records.
- Default filters must remain sparse enough that lossless logging can stay on
  during normal operation.
- Shutdown should record:

```text
server.shutdown.started
connection.close.started
export.close.started
export.close.completed
server.shutdown.completed
```

Invariants
- There is exactly one process-wide tracing subscriber installed by the
  `nbd-server` binary.
- Library crates never install or replace the global subscriber.
- Log policy has one normalized source of truth: `LoggingPolicy`.
- The durable file path has one config source of truth:
  `NbdConfig.logging.file_path`.
- The event taxonomy has one design source of truth and is mirrored in code by
  `observability` target/event constants plus helper functions for repeated
  shapes.
- Runtime, connection, admission, engine, and storage code never depend on log
  destination or formatting policy.
- Every durable daemon log record is JSON lines.
- The default active file is `/tmp/nbd/current.log`.
- `--log-stdout` mirrors logs; it does not replace file logging.
- Enabled log records are not intentionally dropped by the v1 writer policy.
- Request payload bytes are never logged.
- Request correlation does not depend on thread IDs.
- Request correlation does not depend only on NBD cookies.
- Request and storage detail is off by default but available through
  `RUST_LOG`.
- Call sites emit one tracing model. File splitting or stdout mirroring belongs
  to subscriber configuration.
- Operational logs stay sparse enough to leave enabled continuously.

Alternatives considered
- Plain `log` facade with `env_logger`:
  - Rejected because the server needs span context across async tasks and
    structured request fields. `tracing` is a better fit for Tokio request
    lifecycles.
- Separate operations and request log APIs:
  - Rejected because it would duplicate call-site policy. Targets and
    subscriber layers can split output later without changing instrumentation.
- Separate `/tmp/nbd/requests.log` in v1:
  - Deferred. One file is easier to operate initially, and request detail is
    disabled by default. Separate files can be added once volume justifies it.
- Stdout-only default:
  - Rejected because the user-local debugging contract needs a durable trail
    even when the server is run outside a service manager.
- Config-file logging settings beyond the file path:
  - Deferred. The first slice should establish the configurable file path,
    `RUST_LOG` filtering, and `--log-stdout`. Rotation, format, and stdout
    defaults should stay out of config until there is a concrete need.
- Separate workspace observability crate:
  - Deferred. V1 has one daemon binary with this taxonomy. Internal server
    modules keep the boundary contained without making the log schema a
    premature public workspace API.

Migration / rollout
- No catalog migration is needed.
- Add `LoggingConfig` to `nbd-config` with a defaulted top-level `logging`
  field so existing config files remain compatible.
- Update default config generation to include `[logging]`.
- Add new dependencies only to crates that emit or initialize tracing.
- Keep existing CLI output behavior for `nbdcli`.
- Replace `nbd-server serve` routine `println!` output with structured
  `server.listening` logs.
- Update Docker smoke only if it needs `--log-stdout` for debugging. The smoke
  should not depend on stdout logs for correctness.

Validation strategy
- Unit-test serve argument parsing for `--log-stdout`.
- Unit-test logging option defaults:
  - config default path is `/tmp/nbd/current.log`;
  - default stdout mirror is disabled;
  - `RUST_LOG` overrides the default filter when present.
- Unit-test config parsing:
  - missing `[logging]` uses `/tmp/nbd/current.log`;
  - explicit `[logging].file_path` is preserved;
  - generated default config includes `[logging]`.
- Unit-test normalization from `LoggingOptions` to `LoggingPolicy`, including
  destination selection, append behavior, and lossless queue policy.
- Unit-test or compile-test representative `observability` helpers so repeated
  event shapes retain their required fields and do not use ad hoc target names.
- Unit-test or integration-test that logging initialization creates the parent
  directory and appends JSON-lines records to the configured file path. The
  lower-level bootstrap should accept an injected path so tests do not have to
  write to the real `/tmp/nbd/current.log`.
- Add request-path tests only for context construction and non-payload fields.
  Do not make protocol tests assert an exhaustive list of log lines.
- Run the existing protocol suite and Docker smoke after instrumentation to
  ensure logging did not change request ordering, completion, or persistence.
- Manually validate a request-ordering debug run with:

```text
RUST_LOG=info,nbd_server::request=debug,nbd_server::admission=trace
```

  and confirm one write/read sequence can be followed through request,
  admission, engine, and reply events.

Risks
- Request logging can become too noisy if request summaries are enabled by
  default. Keep request summaries at `DEBUG`.
- Lossless logging means a slow or blocked log destination can slow server
  request handling when enabled log volume exceeds writer throughput. Keep
  default `INFO` sparse and request internals off by default.
- JSON generated by `tracing-subscriber` may not put every span field at the
  top level. Important standalone events should duplicate their key fields.
- Adding context to `ExportJob` changes a core runtime data structure. Keep the
  context plain and diagnostic; it must not affect scheduling correctness.
- If destination or filter policy leaks into request/runtime modules, later
  path, rotation, or sink changes will become broad and risky. Keep policy
  ownership in the logging module and review against that boundary.
- If event names are open-coded throughout the repo, the taxonomy will drift.
  Keep target and event names in constants, and add helpers for field-heavy
  repeated events.
- Appending to `/tmp/nbd/current.log` without rotation can grow without bound.
  Rotation is a follow-up once the basic file contract exists.
- Config parse/load failures are not recorded in the configured file because
  the configured path is not available yet. Keep stderr diagnostics clear for
  that pre-logging phase.

Open questions
- none

Design exit criteria
- The design is approved. The default file contract, config path behavior,
  stdout flag behavior, request correlation fields, target taxonomy, and
  lossless writer policy are agreed.

Implementation closeout
- Series state: finished.
- Completed commit stack:
  - `bbb8f3b docs/plans: add logging instrumentation design`
  - `9d8adcc config: add logging file path`
  - `a750adf server: add logging bootstrap`
  - `6b8b2bd server: add observability context`
  - `3585e09 server: instrument runtime logging`
- The config layer now owns `[logging].file_path`, keeps older config files
  compatible, and emits `/tmp/nbd/current.log` in generated default config.
- `nbd-server serve` initializes one process-wide JSON-lines tracing subscriber,
  writes to the configured file by default, and mirrors the same records to
  stdout when `--log-stdout` is passed.
- The runtime now carries `ExportJobContext` through request submission,
  admission, engine execution, completion, and reply writing so request-scoped
  logs can be correlated by `server_instance_id`, `connection_id`,
  `request_sequence`, and NBD cookie.
- Operational lifecycle events are visible at default `INFO` level. Per-request
  happy-path `ExportJob` detail remains off by default and is available through
  targeted `RUST_LOG` settings.
- The logging policy remains contained in `logging.rs`; subsystem call sites
  emit structured tracing events and do not know about file paths, stdout
  mirroring, JSON formatting, append policy, or writer implementation.

Validation
- `cargo fmt --check -p nbd-server`
- `cargo clippy -p nbd-server --all-targets -- -D warnings`
- `cargo test -p nbd-server`
- `cargo test --workspace`
- `make test-protocol`
- `make docker-smoke`
- Docker smoke bind-mounts `./.tmp/docker-smoke` as an artifact directory and
  copies `current.log`, server stdout/stderr, and export inspect snapshots out
  of the disposable container before exit.

Deferred follow-up
- Application-managed log rotation remains deferred.
- Separate request log files remain deferred until request volume justifies
  splitting targets into multiple sinks.
- Runtime log-level reload, metrics, OpenTelemetry export, and audit logging
  remain out of scope for this series.
