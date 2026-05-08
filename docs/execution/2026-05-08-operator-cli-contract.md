Title: Operator CLI Contract Execution
Date: 2026-05-08
Status: approved
Approval:
- overall doc approved: yes
- current state: Series 1 approved
Completion:
- execution complete: no

## Goal

Implement the approved operator CLI contract for `nbd-server` and `nbdcli`:

- move `nbd-server` command parsing to `clap`;
- keep `nbd-server` as the owner of `~/.nbd/config.toml`;
- add `nbd-server config --path` and `nbd-server config init`;
- add read-only doctor commands with binary-specific scope;
- add global JSON output and structured JSON errors for `nbdcli`;
- keep `nbd-server serve` out of JSON result mode;
- add outside-source-tree installed-command smoke coverage.

## Roadmap Context

No separate roadmap slice governs this work. The source of truth is the
approved operator CLI design.

## Design Inputs

- `docs/plans/2026-05-08-operator-cli-contract.md`
- `~/workplace/llm-wiki/wiki/engineering-guides/systems-primitives/cli.md`
- `~/workplace/llm-wiki/wiki/engineering-guides/systems-primitives/config.md`
- `~/workplace/llm-wiki/wiki/engineering-guides/systems-primitives/testing-and-proof.md`

## Why Split

One execution series is enough. The work has one approved design, one coherent
operator-contract checkpoint, and no need for staged approvals between
subfeatures. The commits still separate planning artifacts, config primitive,
server parser/config behavior, server doctor, `nbdcli` JSON/config ownership,
and final doctor/smoke proof.

## Series 1: Operator CLI Contract

Depends on: none
Design coverage: full approved design
Stable checkpoint: both operator binaries expose the approved command surface,
JSON/readiness contracts are tested, and docs describe the config ownership
split.
Review focus: config ownership, parser migration, read-only doctor behavior,
JSON stdout/stderr separation, and source topology.
Source topology checkpoint: `main.rs` in both binaries should be dispatch-only;
parser, doctor, and output behavior should have named owning modules.
Done means: `nbd-server` uses `clap`; `nbd-server config --path`,
`nbd-server config init`, and `nbd-server doctor` work; `nbdcli --json`
works for finite catalog commands; `nbdcli doctor` works without writing; and
outside-source-tree smoke tests run each binary by command name.
Approval: approved
Approval handoff: before `$impl-series`, set overall doc approval to `yes`,
current state to `Series 1 approved`, document status to `approved`, and this
series approval to `approved`.
Verification plan:

```sh
cargo fmt --all --check
cargo test -p nbd-config
cargo test -p nbd-server --bin nbd-server
cargo test -p nbd-server --test config_command
cargo test -p nbd-server --test installed_smoke
cargo test -p nbdcli
```

Not included: `config set`, config init overwrite/force behavior, migration
execution, S3 network probes, shell completions, `nbd-server serve --json`, or
Docker/kernel smoke expansion.

## Current Series Commit Plan

```text
Commit 1/6: docs: approve operator CLI contract

  Summary:          Mark the accepted operator CLI design approved and add the
                    execution contract that will govern the implementation
                    series.
  Invariant focus:  Planning artifacts are truthful before implementation
                    starts, and the config ownership rule is explicit.
  Files:            docs/plans/2026-05-08-operator-cli-contract.md
                    docs/execution/2026-05-08-operator-cli-contract.md (new)
  Source topology:  not material: documentation-only planning state; source
                    ownership is named for later commits but no Rust module
                    changes land here.
  Preconditions:    The design in docs/plans/2026-05-08-operator-cli-contract.md
                    has been accepted by the user.
  Postconditions:   The approved design and current execution plan are the
                    durable source of truth for the implementation series.
  Evidence:         none, because this is a planning-artifact commit;
                    line-length hygiene is the useful proof.
  Review:           structures, because this commit names the module ownership
                    and commit boundaries that implementation must follow.
  Verify:           awk 'length($0) > 88 { print FNR ":" length($0) ":" $0 }'
                    docs/plans/2026-05-08-operator-cli-contract.md
                    docs/execution/2026-05-08-operator-cli-contract.md
  Not included:     No Rust implementation, parser migration, config primitive,
                    doctor command, or JSON output behavior.

Commit 2/6: config: add explicit config initialization

  Summary:          Add the config-layer primitive for writing a generated
                    config only when the target file is missing, with an
                    explicit already-exists error.
  Invariant focus:  Server-owned setup writes are narrow, explicit, and cannot
                    overwrite an existing operator config.
  Files:            crates/nbd-config/src/lib.rs
                    crates/nbd-config/tests/config_loading.rs
  Source topology:  owner: crates/nbd-config/src/lib.rs because config bootstrap
                    and generated-default path policy already live in
                    nbd-config.
  Preconditions:    Commit 1 has recorded the approved config ownership rule and
                    execution contract.
  Postconditions:   ConfigFile exposes an init-style API that creates parent
                    directories, writes the generated config when absent,
                    returns the written path/config, and fails without rewriting
                    when the target already exists.
  Evidence:         functional, because the contract is filesystem behavior
                    exercised through the public ConfigFile API.
  Review:           code, because this commit introduces the write primitive
                    that protects existing operator config files.
  Verify:           cargo test -p nbd-config
  Not included:     No server command uses the primitive yet, no force/overwrite
                    mode, and no migration or runtime directory creation.

Commit 3/6: server: move CLI parsing to clap

  Summary:          Replace nbd-server's hand-rolled parser with clap and add
                    the server-owned config --path and config init surfaces.
  Invariant focus:  All nbd-server commands parse through the standard parser
                    while existing serve/config behavior remains compatible.
  Files:            README.md
                    crates/nbd-server/Cargo.toml
                    crates/nbd-server/src/cli.rs (new)
                    crates/nbd-server/src/main.rs
                    crates/nbd-server/src/output.rs (new)
                    crates/nbd-server/tests/config_command.rs
  Source topology:  split: crates/nbd-server/src/cli.rs owns parser structs and
                    crates/nbd-server/src/output.rs owns finite command
                    rendering so main.rs stays a dispatcher instead of the next
                    operator-feature dumping ground.
  Preconditions:    Commit 2 has added the config init primitive and all
                    existing nbd-server parser tests are still represented by
                    behavior expectations.
  Postconditions:   nbd-server serve and config accept their existing flags
                    through clap, config --path prints the selected path, config
                    init writes only a missing config, and serve has no JSON
                    result mode.
  Evidence:         functional, because binary-level tests exercise the public
                    command surface and parser unit tests cover compatibility
                    shapes.
  Review:           structures, because this commit creates the server CLI
                    module ownership boundary and removes manual argv parsing.
  Verify:           cargo test -p nbd-server --bin nbd-server
                    cargo test -p nbd-server --test config_command
  Not included:     No doctor command, no nbdcli changes, no JSON error
                    contract, and no changes to serve logging behavior.

Commit 4/6: server: add readiness doctor

  Summary:          Add nbd-server doctor with human and JSON output for
                    read-only server readiness checks.
  Invariant focus:  Server readiness can be diagnosed without starting a
                    listener, initializing durable logging, or repairing state.
  Files:            README.md
                    crates/nbd-server/Cargo.toml
                    crates/nbd-server/src/cli.rs
                    crates/nbd-server/src/doctor.rs (new)
                    crates/nbd-server/src/main.rs
                    crates/nbd-server/src/output.rs
                    crates/nbd-server/tests/config_command.rs
  Source topology:  split: crates/nbd-server/src/doctor.rs owns server readiness
                    checks so startup, parser, and presentation code do not
                    absorb catalog/path probing policy.
  Preconditions:    Commit 3 has established clap parsing and finite-command
                    output ownership for nbd-server.
  Postconditions:   nbd-server doctor checks config loading, catalog
                    URL/provider, SQLite catalog file/schema readiness,
                    runtime/log path usability, and blob-store config without
                    writing or starting the server; --json emits one report on
                    stdout.
  Evidence:         functional, because command tests exercise successful,
                    missing-config, missing-catalog, and unmigrated-catalog
                    behavior through the binary.
  Review:           code, because doctor must be truthful about what it checked
                    and must not accidentally create or repair operator state.
  Verify:           cargo test -p nbd-server --bin nbd-server
                    cargo test -p nbd-server --test config_command
  Not included:     No S3 network probe, no migration execution, no directory
                    creation, no nbdcli doctor, and no JSON mode for serve.

Commit 5/6: nbdcli: define JSON and config ownership

  Summary:          Refactor nbdcli into parser and output modules, add global
                    --json envelopes and structured JSON errors, and make config
                    loading read-only.
  Invariant focus:  nbdcli remains a thin catalog adapter and never bootstraps
                    or rewrites the server-owned config.
  Files:            README.md
                    crates/nbdcli/src/cli.rs (new)
                    crates/nbdcli/src/main.rs
                    crates/nbdcli/src/output.rs (new)
                    crates/nbdcli/tests/cli.rs
  Source topology:  split: crates/nbdcli/src/cli.rs owns parser shape and
                    crates/nbdcli/src/output.rs owns presentation/JSON
                    contracts, leaving main.rs as catalog dispatch.
  Preconditions:    Commit 1 has recorded the ownership rule, and existing
                    catalog command tests pass before the nbdcli adapter is
                    refactored.
  Postconditions:   nbdcli --json works for create, list, inspect, clone, and
                    delete; list --json and inspect --json remain compatibility
                    aliases; JSON success goes to stdout, JSON runtime errors go
                    to stderr, and missing default config does not create
                    ~/.nbd/config.toml.
  Evidence:         functional, because existing and new nbdcli binary tests
                    exercise the public operator command surface and parse
                    stdout/stderr JSON.
  Review:           migration, because this intentionally changes accidental
                    default-config bootstrap behavior while preserving
                    explicit-config workflows and per-command JSON aliases.
  Verify:           cargo test -p nbdcli
  Not included:     No nbdcli doctor yet, no server changes, no shell
                    completions, and no raw request escape hatch.

Commit 6/6: cli: add catalog doctor and installed smoke

  Summary:          Add nbdcli doctor for read-only catalog command readiness
                    and add outside-source-tree smoke coverage for the
                    installed-command shape of both binaries.
  Invariant focus:  Both operator binaries can be invoked by command name from
                    outside the repo for safe read-only workflows.
  Files:            README.md
                    crates/nbd-server/tests/installed_smoke.rs (new)
                    crates/nbdcli/src/cli.rs
                    crates/nbdcli/src/doctor.rs (new)
                    crates/nbdcli/src/main.rs
                    crates/nbdcli/src/output.rs
                    crates/nbdcli/tests/cli.rs
                    crates/nbdcli/tests/installed_smoke.rs (new)
  Source topology:  split: crates/nbdcli/src/doctor.rs owns catalog CLI
                    readiness checks, while installed smoke lives in binary
                    integration tests because it proves process invocation
                    rather than library behavior.
  Preconditions:    Commit 4 has server doctor output and Commit 5 has nbdcli
                    global JSON/output ownership.
  Postconditions:   nbdcli doctor checks existing config load, catalog
                    URL/provider, SQLite catalog file/schema readiness, and JSON
                    report output without writing; installed smoke runs
                    nbd-server and nbdcli by command name from outside the repo
                    tree.
  Evidence:         integration, because the smoke tests exercise binary
                    discovery, working-directory independence, and safe
                    read-only command execution across process boundaries.
  Review:           code, because the final boundary proof must stay read-only
                    and avoid relying on repo-relative paths.
  Verify:           cargo test -p nbdcli
                    cargo test -p nbd-server --test installed_smoke
  Not included:     No config init force mode, no migration execution, no S3
                    reachability check, and no Docker/kernel smoke expansion.
```
