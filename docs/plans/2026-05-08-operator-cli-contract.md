Title: Operator CLI Contract
Date: 2026-05-08
Status: approved

Problem
- The workspace now has two operator-facing binaries with uneven CLI contracts.
  `nbdcli` uses `clap`, but `nbd-server` still hand-rolls argv parsing.
- `nbd-server` owns the canonical operator config at `~/.nbd/config.toml`, but
  `nbdcli` currently loads the default config through a path that can bootstrap
  that file. That makes config ownership ambiguous.
- Script and future-agent use is only partly supported. `nbdcli list` and
  `inspect` have per-command JSON output, while write commands, readiness
  checks, server config commands, and failures do not have one clear output
  contract.
- Setup failures are discovered indirectly when a real serve or catalog command
  fails. There is no `doctor` command for checking config and catalog readiness.

Goal
- Put both binaries on the standard parser foundation with `clap`.
- Make `nbd-server` the only binary that owns default config bootstrap,
  rendering, and inspection.
- Keep `nbdcli` as a thin catalog operator adapter that consumes an existing
  config only to locate the catalog.
- Add read-only `doctor` commands with binary-specific scope:
  - `nbd-server doctor` checks server readiness from the owned config.
  - `nbdcli doctor` checks catalog command readiness from an existing config.
- Add an explicit `nbd-server config init` setup command so first-run config
  creation is not hidden behind `serve`.
- Define stable JSON output for finite commands that scripts, tests, and future
  agents need to parse.
- Keep diagnostics on stderr and command results on stdout.

Constraints
- Existing operator commands must remain compatible:
  - `nbd-server serve [--config <path>] [--listen <addr:port>] [--log-stdout]`
  - `nbd-server config [--config <path>] [get <key>]`
  - `nbdcli --config <path> create|list|inspect|clone|delete ...`
- The server's default config behavior remains server-owned. `nbd-server serve`
  may continue to bootstrap the default user config when it is missing.
- `nbdcli` must not create, rewrite, or bootstrap `~/.nbd/config.toml`.
- Doctor commands must be read-only. They must not apply migrations, create
  exports, delete exports, create buckets, write probe files, or silently repair
  state.
- `config init` is the only new write command in this design. It writes a
  missing config file and must not overwrite an existing file.
- JSON output must not print secret-bearing config values. `config get` remains
  allowlisted through `nbd-config::ConfigKey`.
- This design is for command contracts and adapter structure, not for changing
  catalog schema, server runtime behavior, storage semantics, or logging
  taxonomy.

Non-goals
- Adding config editing such as `config set`.
- Adding migration execution to either binary.
- Adding S3 network reachability probes.
- Adding overwrite or force semantics to `config init`.
- Reworking server logging, tracing targets, or `serve --log-stdout`.
- Adding JSON mode to `nbd-server serve`.
- Adding shell completions or package-manager install metadata.
- Introducing auth, leases, or client identity.

End state
- `nbd-server` uses `clap` for all command parsing.
- `nbdcli` keeps using `clap`, but its output mode is global instead of
  command-specific.
- `nbd-server` exposes:

```text
nbd-server serve [--config <path>] [--listen <addr:port>] [--log-stdout]
nbd-server config [--config <path>]
nbd-server config [--config <path>] get <dotted.key>
nbd-server config --path
nbd-server config init [--config <path>]
nbd-server doctor [--config <path>] [--json]
```

- `nbdcli` exposes:

```text
nbdcli [--config <path>] [--json] create <name> --size <bytes> ...
nbdcli [--config <path>] [--json] list [--include-deleted]
nbdcli [--config <path>] [--json] inspect <name>
nbdcli [--config <path>] [--json] clone <source> <destination>
nbdcli [--config <path>] [--json] delete <name>
nbdcli [--config <path>] [--json] doctor
```

- `nbdcli --json` prints one machine-readable command result to stdout for
  successful finite commands. Runtime diagnostics and failures go to stderr.
- `nbd-server doctor --json` and `nbdcli --json doctor` print a shared-shaped
  doctor report to stdout.
- `nbd-server serve` does not accept JSON output mode. Runtime structured
  output remains controlled by `--log-stdout` and the configured JSON-lines log
  file.
- Application errors in JSON mode are emitted as one JSON diagnostic object on
  stderr after parsing succeeds. `clap` parse errors remain `clap` diagnostics
  and use `clap` exit behavior.
- README documents the ownership split:
  - use `nbd-server config` to inspect the server-owned config;
  - use `nbd-server doctor` to check serving readiness;
  - use `nbdcli doctor` to check catalog CLI readiness.

Proposed approach
- Replace `nbd-server`'s manual parser with `clap` derive structs. Keep the
  same command names, defaults, and flags.
- Keep `nbd-server main.rs` as the binary coordinator:
  1. parse args;
  2. dispatch to a command runner;
  3. initialize logging only for `serve`;
  4. render finite command results;
  5. map failures to stderr and exit codes.
- Move server binary concerns out of `main.rs` into small modules:
  - `cli` owns `clap` parser structs and typed command args;
  - `doctor` owns server readiness checks;
  - `output` owns human and JSON rendering for finite commands;
  - existing `logging` continues to own `serve` logging setup.
- Add a small `nbd-config` helper for explicit config initialization. The
  helper should use the same embedded default/template path logic as existing
  config loading, create parent directories, write only when the target file is
  absent, and report an explicit already-exists error otherwise.
- Keep `nbdcli` as a thin adapter over `nbd-control-plane`, but give it the
  same internal adapter shape:
  - `cli` owns `clap` parser structs and typed command args;
  - `doctor` owns catalog CLI readiness checks;
  - `output` owns human and JSON rendering.
- Do not introduce a new workspace crate for output or doctor reports in this
  slice. The shared contract is small and can be mirrored by binary-local
  serde structs. If a third binary needs the same report schema, that is the
  point where a small shared operator-surface crate becomes justified.

Data model / API shape
- Config source ownership:

```rust
enum OperatorConfigUse {
    ServerOwned,
    CliReadOnly,
}
```

- The exact enum does not need to exist in code, but the behavior must:
  - server-owned paths may use `ConfigFile::load_or_default()` for inspection
    and `NbdConfig::load(DefaultUserPath)` for `serve` bootstrap;
  - CLI read-only paths use `ConfigFile::load()` and fail if the selected
    config file is missing.

- Config initialization:

```rust
struct InitializedConfig {
    path: PathBuf,
    config: NbdConfig,
}

enum ConfigError {
    ConfigAlreadyExists { path: PathBuf },
    // existing variants...
}
```

- The exact returned type can change during implementation, but the contract is
  fixed: `ConfigFile::init` writes the generated config only when the target is
  missing and returns enough information for `nbd-server config init` to print
  what happened.

- Output mode:

```rust
enum OutputMode {
    Human,
    Json,
}
```

- Doctor report:

```rust
struct DoctorReport {
    status: DoctorStatus,
    checks: Vec<DoctorCheck>,
}

enum DoctorStatus {
    Ok,
    Warning,
    Failed,
}

struct DoctorCheck {
    name: &'static str,
    status: DoctorStatus,
    detail: Option<String>,
    remediation: Option<String>,
}
```

- Command error JSON:

```rust
struct ErrorReport {
    status: &'static str,      // "error"
    code: &'static str,        // stable, coarse error code
    message: String,
    operation: Option<String>,
    resource: Option<String>,
}
```

- `nbdcli` success JSON should be CLI-owned envelopes for write commands rather
  than incidental prose:

```rust
created  -> { "status": "created", "export": ExportRecord }
cloned   -> { "status": "cloned", "source": ExportRecord,
              "destination": ExportRecord, "source_wal_cloned": false }
deleted  -> { "status": "deleted", "name": "disk-a" }
list     -> [ExportRecord, ...]
inspect  -> ExportRecord
doctor   -> DoctorReport
```

- Human output remains operator-oriented prose and tables. Human wording is not
  the automation contract.

Source topology / project structure
- `crates/nbd-server/src/main.rs`
  - Small binary entrypoint and dispatcher only.
- `crates/nbd-server/src/cli.rs`
  - `clap` parser structs for `serve`, `config`, and `doctor`.
- `crates/nbd-server/src/doctor.rs`
  - Server readiness checks over config, catalog, runtime paths, log path, and
    blob-store config.
- `crates/nbd-server/src/output.rs`
  - Human/JSON rendering and parsed-command error reporting for finite server
    commands.
- `crates/nbd-server/src/logging.rs`
  - Existing logging policy and initialization; no parser ownership.
- `crates/nbdcli/src/main.rs`
  - Small binary entrypoint and dispatcher only.
- `crates/nbdcli/src/cli.rs`
  - `clap` parser structs for catalog commands and global output mode.
- `crates/nbdcli/src/doctor.rs`
  - Catalog CLI readiness checks.
- `crates/nbdcli/src/output.rs`
  - Human/JSON rendering and parsed-command error reporting.
- This change should leave no large binary `main.rs` as the obvious dumping
  ground for the next unrelated operator feature.

Invariants
- `nbd-server` is the owner of `~/.nbd/config.toml`.
- `nbdcli` must never bootstrap, create, or rewrite the default config.
- All CLI commands parse through `clap`; no new hand-rolled argv parsing is
  added.
- Successful JSON command output goes to stdout only.
- Diagnostics, human errors, JSON errors, progress, and doctor failure details
  go to stderr when they are not the command result.
- Doctor commands are read-only and do not repair state.
- `config init` is a narrow write command and never overwrites an existing
  config file.
- `config get` never prints secret-bearing values outside the existing
  allowlist.
- `serve` remains the only long-running command in this design.
- Logging setup remains specific to `serve`; finite inspection commands do not
  initialize the durable server log.

Operational and lifecycle contracts
- `nbd-server serve` keeps its current lifecycle: load or bootstrap config,
  initialize logging, start the server, wait for Ctrl-C, then shut down
  cooperatively.
- `nbd-server doctor` checks what `serve` needs without starting a listener and
  without initializing the durable server log.
- `nbd-server doctor` may report a missing default config as a warning because
  `serve` can bootstrap it. A missing explicit config is a failure.
- `nbdcli doctor` treats a missing default or explicit config as a failure
  because `nbdcli` is not the config owner.
- `nbd-server config init` creates the selected server-owned config file and
  parent directories, then exits. It does not migrate the catalog, create
  runtime directories, initialize blob storage, or start the server.
- `nbd-server config init` fails if the selected config file already exists.
  A future `--force` or `--if-missing` flag can define overwrite or idempotent
  behavior if there is concrete demand.
- SQLite schema readiness is checked by a harmless catalog read after verifying
  that the SQLite catalog file exists. Doctor should not create a new empty
  SQLite file just to discover that migrations are missing.
- Doctor exits `0` for `Ok` and `Warning`, and exits nonzero for `Failed`.

Alternatives considered
- Keep `nbd-server` hand-rolled parsing:
  - rejected because the engineering guide makes standard parser libraries the
    default, and the current parser already duplicates behavior `clap` owns.
- Add `nbdcli config`:
  - rejected because it blurs ownership of the server-owned config file.
    `nbdcli` can accept `--config` to locate the catalog, but `nbd-server`
    should remain the config inspection and bootstrap surface.
- Put shared output and doctor structs in a new workspace crate:
  - deferred because two binaries are not enough pressure to justify another
    crate. The design names the schema so it can be extracted later without
    changing the operator contract.
- Let doctor repair missing directories, migrations, or buckets:
  - rejected because diagnosis and repair should remain separate operator
    actions.
- Add global JSON mode to `nbd-server` including `serve`:
  - rejected because `serve` is long-running and already has a structured
    runtime stream through logging. JSON result mode is kept to finite commands.

Migration / rollout
- Existing command names, default values, and flags stay compatible.
- `nbd-server config init` is additive and gives scripts a direct setup path
  that does not require starting the server.
- `nbdcli list --json` and `nbdcli inspect --json` should continue to work
  during rollout as compatibility aliases for global `nbdcli --json list` and
  `nbdcli --json inspect`.
- `nbdcli` default-config behavior changes from bootstrap to read-only load.
  This is intentional ownership cleanup and should be called out in README.
- Any scripts relying on `nbdcli` to create `~/.nbd/config.toml` should switch
  to `nbd-server serve` first-run bootstrap or an explicit server-owned config
  setup command if one is added later.

Validation strategy
- Parser compatibility:
  - unit tests use `clap` `try_parse_from` for old and new command shapes.
- Config ownership:
  - tests prove `nbd-server` can inspect default/missing config without writing;
  - tests prove `nbd-server config --path` prints the selected config path;
  - tests prove `nbd-server config init` writes a missing config and refuses to
    overwrite an existing config;
  - tests prove `nbdcli` fails on missing default config without writing it;
  - tests prove explicit config paths remain isolated.
- Doctor behavior:
  - migrated temp SQLite catalog passes `nbd-server doctor` and
    `nbdcli doctor`;
  - missing explicit config fails;
  - missing SQLite catalog file fails without creating the file;
  - unmigrated SQLite catalog fails with a schema-readiness diagnostic.
- JSON contract:
  - success JSON parses from stdout for `nbdcli` write/read commands and both
    doctor commands;
  - runtime error JSON appears on stderr in JSON mode after parsing succeeds;
  - no diagnostics are mixed into JSON stdout.
- Installed-binary smoke:
  - run the compiled or installed command by binary name from outside the repo
    tree;
  - check `command -v`, top-level help, and a safe read-only command such as
    `doctor --json` against an explicit temp config.
- Standard validation:
  - `cargo fmt --all --check`
  - `cargo test -p nbdcli`
  - `cargo test -p nbd-server --test config_command`
  - broader workspace tests before handoff when implementation touches shared
    config or catalog behavior.

Risks
- JSON envelopes become an API. Field names should be conservative and stable.
- Changing `nbdcli` default-config load from bootstrap to read-only can break
  an accidental workflow, but that workflow conflicts with config ownership.
- `clap` may slightly change help or parse-error wording. Tests should assert
  behavior and important flags, not full help snapshots.
- Doctor path checks can become misleading if they try to infer too much
  without writing. Keep v1 checks simple and explicit about what was tested.

Open questions
- none

Design exit criteria
- The config ownership rule is accepted:
  - `nbd-server` owns default config bootstrap and inspection;
  - `nbdcli` only reads existing config to locate the catalog.
- The command surface above is accepted or revised.
- The doctor scope for each binary is accepted.
- The JSON success and error boundaries are accepted.
- The source topology is accepted as the owner map for implementation.

Recommended next step
- `$review-plan` after this draft design is accepted.
- Treat `ready for series planning` as permission to ask whether to start
  `$plan-series`, not as permission to start `$plan-series` automatically.
