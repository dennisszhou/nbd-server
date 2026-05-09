Title: Docker Smoke Output Framework
Date: 2026-05-09
Status: approved

Problem
- The `make docker-smoke*` targets interleave operator state, Makefile command
  echo, Docker commands, Docker build output, ignored cleanup errors, kernel
  smoke logs, RustFS logs, and Cargo test output.
- The important state is not visually distinct from commands. It is hard to
  tell what scenario is running, which step failed, and where to look next.
- The Makefile currently owns both orchestration and presentation. That makes
  the targets hard to read and encourages more shell control flow to accumulate
  in the Makefile.
- Full echoed Docker commands include secret-shaped S3 environment variables.
  These are test credentials today, but the console contract should not train
  us to print credential values.
- Fixing only `docker-smoke-s3` would leave the same output model split across
  sibling smoke targets.

Goal
- Make every `make docker-smoke*` target concise, structured, and safe by
  moving host-side smoke orchestration and output presentation into a small Bash
  framework.
- Keep these Make targets as the stable operator commands:
  - `make docker-smoke`
  - `make docker-smoke-s3`
  - `make docker-smoke-s3-probe`
  - `make docker-smoke-s3-down`
- Preserve existing scenario behavior, Docker resource names, environment
  overrides, artifact locations, and `KEEP_RUSTFS=1` behavior.
- Make failures point to a named step and a named artifact log.
- Create a reusable output boundary for future Docker smoke targets without
  making the Makefile the lifecycle owner.

Constraints
- The framework must work from a macOS host and from Linux-oriented Docker
  workflows.
- Use tools already required by the smoke path: Bash, Make, Docker, Cargo, and
  standard Unix utilities.
- Existing Make variables remain the compatibility surface for callers and CI
  scripts.
- The first migration must not change kernel smoke scenario semantics, S3 blob
  store behavior, Prisma migration behavior, or the inner NBD/kernel lifecycle.
- Console output must not print raw secret values or full env-rich Docker
  command lines by default.
- Each smoke target must keep returning nonzero when a required smoke step
  fails.
- Cleanup of missing Docker resources must be quiet and idempotent.

Non-goals
- Rewriting inner kernel smoke scenario semantics. The harness may emit
  presentation-only progress events, but NBD device lifecycle, server
  lifecycle, catalog setup, and scenario behavior remain unchanged.
- Replacing Bash with Python, Rust, or an `xtask` runner in this slice.
- Adding a CI report format such as JUnit or JSON summaries.
- Changing Docker images, Dockerfile contents, Cargo features, or S3 test
  credentials.
- Changing `docker-rustfs-up`, manual RustFS developer workflows, or the
  storage engine contract.
- Redacting already existing artifact files such as generated config copies.
  This design controls the console contract first.

End state
- Every `make docker-smoke*` target delegates to a host-side script instead of
  embedding Docker smoke lifecycle logic in the Makefile recipe.
- The Makefile remains the stable entrypoint and the place where existing
  default variables are declared.
- A shared Bash output helper owns step rendering, state rendering, command
  capture, failure summaries, verbose mode, and command redaction.
- Shared Docker-smoke helpers own common Docker build and workspace argument
  construction so each runner does not hand-roll the same command shape.
- Default console output is a short sequence of state and step lines. It does
  not print long Docker commands or credential values.
- Child process output is captured under the relevant artifact directory, with
  clear names such as `docker-build.log`, `kernel-smoke.log`,
  `rustfs-probe.log`, `s3-prefix-test.log`, and `rustfs.log`.
- Long-running kernel smoke steps expose a small progress side channel while
  keeping full child output captured in `kernel-smoke.log`.
- On failure, the console reports the failed step, prints a bounded tail of the
  failed step log, and names the artifact directory.
- `VERBOSE=1 make docker-smoke*` provides command-oriented detail for local
  debugging without changing the default operator path.

Proposed approach
- Add a small Bash logging/capture library for Docker smoke scripts.
- Add a small Docker-smoke helper library for common image build, workspace
  mount, environment, and artifact argument construction.
- Add a plain Docker smoke runner for `make docker-smoke`:
  1. create the artifact directory;
  2. build or refresh the Docker image while capturing build output;
  3. run the privileged kernel smoke scenario in the container;
  4. report the artifact directory.
- Add one S3 smoke runner with modes for the S3 target family:
  - `run` for `make docker-smoke-s3`;
  - `probe` for `make docker-smoke-s3-probe`;
  - `down` for `make docker-smoke-s3-down`.
- The S3 `run` mode owns this lifecycle:
  1. create the artifact directory;
  2. build or refresh the Docker image while capturing build output;
  3. quietly clean any stale S3 smoke resources;
  4. create the smoke network and RustFS data volume;
  5. start the RustFS sidecar;
  6. wait for RustFS from inside the smoke network;
  7. run the privileged kernel smoke scenario with S3 settings;
  8. run the S3 prefix assertion test;
  9. collect RustFS logs;
  10. clean up sidecar resources unless `KEEP_RUSTFS=1`.
- The S3 `probe` mode shares the same setup, readiness, log collection, and
  cleanup lifecycle, but runs `scripts/docker/rustfs-s3-probe.sh` instead of
  the kernel smoke and prefix assertion pair.
- The S3 `down` mode performs quiet, idempotent cleanup of the RustFS container,
  network, and volume. It does not build the image.
- Keep `scripts/docker/kernel-smoke.sh` and
  `scripts/docker/kernel-smoke/harness.sh` as the inner scenario owners. They
  still own NBD device lifecycle, server lifecycle, smoke-home setup, config
  initialization, catalog migration, scenario actions, kernel artifacts, and
  milestone progress events for those actions.

Data model / API shape
- Make variables remain the external configuration API:
  - `DOCKER_IMAGE`
  - `DOCKER_PLATFORM`
  - `DOCKER_WORKDIR`
  - `DOCKER_CARGO_TARGET_DIR`
  - `DOCKER_PATH`
  - `KERNEL_SMOKE_OUTPUT`
  - `DOCKER_KERNEL_SMOKE_ARTIFACT_DIR`
  - `DOCKER_KERNEL_SMOKE_ARTIFACT_MOUNT`
  - `KERNEL_SMOKE_EXPORT`
  - `KERNEL_SMOKE_SCENARIO`
  - `KERNEL_SMOKE_SIZE_BYTES`
  - `KERNEL_SMOKE_ENGINE`
  - `KERNEL_SMOKE_CARGO_FEATURES`
  - `KERNEL_SMOKE_PORT`
  - `KERNEL_SMOKE_DEVICE`
  - `KERNEL_SMOKE_RUST_LOG`
  - `KERNEL_SMOKE_COMPACTION_SETTLE_SECONDS`
  - `DOCKER_SMOKE_S3_NETWORK`
  - `DOCKER_SMOKE_S3_RUSTFS_CONTAINER`
  - `DOCKER_SMOKE_S3_RUSTFS_ALIAS`
  - `DOCKER_SMOKE_S3_RUSTFS_VOLUME`
  - `DOCKER_SMOKE_S3_ARTIFACT_DIR`
  - `DOCKER_SMOKE_S3_ACCESS_KEY`
  - `DOCKER_SMOKE_S3_SECRET_KEY`
  - `DOCKER_SMOKE_S3_BUCKET`
  - `DOCKER_SMOKE_S3_KEY_PREFIX`
  - `RUSTFS_IMAGE`
  - `KEEP_RUSTFS`
  - `VERBOSE`
- The Makefile passes these variables to the host-side scripts as environment.
  The scripts derive Docker argument arrays from those environment values.
- Source-of-truth state:
  - Make/default environment values are the source of truth for scenario
    configuration.
  - Docker owns actual network, volume, container, and image state.
  - The artifact directory is the source of truth for child process logs after
    a run.
  - Console output is derived presentation only.
- Shared output helper API:

```text
smoke_step <message>
smoke_state <key> <value>
smoke_ok <message>
smoke_warn <message>
smoke_fail <message>
smoke_run <label> <log-path> <command> [args...]
smoke_run_with_progress <label> <log-path> <progress-path> <command> [args...]
smoke_run_quiet <label> <command> [args...]
smoke_tail_log <log-path>
smoke_redacted_command <command> [args...]
```

- `smoke_run` captures stdout and stderr to the named log. On success it prints
  a concise success line. On failure it prints the failed label, tails the log,
  and returns the command exit status.
- `smoke_run_with_progress` captures stdout and stderr to the named log while
  polling a progress file and rendering only those progress events to the
  console. The progress file is a side channel, not a replacement for the full
  command log.
- `smoke_run_quiet` is for expected-idempotent cleanup and inspect commands.
  Missing Docker resources are not treated as operator-visible errors.
- `smoke_redacted_command` is used only for verbose command display. It redacts
  values associated with secret-shaped environment names such as
  `*_SECRET*`, `*_PASSWORD*`, `*_TOKEN*`, `*_ACCESS_KEY*`, and `*_KEY`.
- Shared Docker helper API:

```text
docker_smoke_build_image <log-path>
docker_smoke_workspace_args <rw|ro>
docker_smoke_kernel_env_args
docker_smoke_artifact_args <host-dir> <container-dir>
docker_smoke_run_in_workspace <log-path> <extra-args...> -- <command...>
```

- The exact Bash function names may change during implementation, but the
  ownership is fixed: common Docker command construction lives in one helper,
  not in each runner and not in the Makefile.
- Runner step models:

```text
docker-smoke:
  build-image
  kernel-smoke

docker-smoke-s3 run:
  build-image
  cleanup-stale-resources
  create-network
  create-volume
  start-rustfs
  wait-rustfs
  kernel-smoke
  s3-prefix-test
  collect-rustfs-log
  cleanup-resources

docker-smoke-s3 probe:
  build-image
  cleanup-stale-resources
  create-network
  create-volume
  start-rustfs
  wait-rustfs
  rustfs-probe
  collect-rustfs-log
  cleanup-resources

docker-smoke-s3 down:
  cleanup-resources
```

- Each required step either succeeds, or it fails with a step name, an exit
  status, and an artifact path.

Source topology / project structure
- `Makefile`
  - Keeps default variables and stable phony targets.
  - `docker-smoke`, `docker-smoke-s3`, `docker-smoke-s3-probe`, and
    `docker-smoke-s3-down` become thin wrappers around host-side scripts.
- `scripts/docker/lib/smoke-log.sh`
  - Owns generic console rendering, command capture, verbose display, redaction,
    log tailing, and failure summaries.
  - Does not know S3, RustFS, NBD, Prisma, or Cargo semantics.
- `scripts/docker/lib/smoke-docker.sh`
  - Owns common Docker image build and workspace argument construction.
  - Knows Docker workspace mounts, Cargo cache volumes, target dir, platform,
    image name, and kernel smoke environment argument construction.
  - Does not know the S3 sidecar lifecycle.
- `scripts/docker/docker-smoke.sh`
  - Owns the host-side lifecycle for the plain `docker-smoke` target.
  - Calls `make kernel-smoke-inner` inside the privileged smoke container.
- `scripts/docker/docker-smoke-s3.sh`
  - Owns the outer S3 smoke lifecycle and Docker resource policy for the S3
    target family.
  - Implements `run`, `probe`, and `down` modes.
  - Calls `make kernel-smoke-inner` inside the privileged smoke container for
    `run` mode.
  - Calls `scripts/docker/rustfs-s3-probe.sh` inside the smoke network for
    `probe` mode.
- `scripts/docker/kernel-smoke.sh`
  - Remains the entrypoint for the inner kernel smoke scenario.
- `scripts/docker/kernel-smoke/harness.sh`
  - Remains the owner of NBD device, mount, server, config, catalog, and
    scenario artifacts.
- This change leaves the Makefile less likely to become the dumping ground for
  the next Docker smoke feature.

Invariants
- Default console output never prints raw S3 secret values.
- Default console output does not print full env-rich Docker command lines.
- Every required child process step has a named log file under the artifact
  directory for that target.
- A failed required step reports the step name, returns nonzero, and points to
  the artifact directory.
- Missing Docker resources during pre-run, post-run, or explicit down cleanup
  are quiet.
- `KEEP_RUSTFS=1` leaves the RustFS sidecar resources in place and prints the
  network, container, and cleanup command.
- Without `KEEP_RUSTFS=1`, S3 `run` and `probe` perform best-effort cleanup
  after both success and failure.
- The scripts pass the same smoke scenario variables, S3 endpoint, bucket,
  region, key prefix, and credentials that the Makefile targets pass today.
- The inner kernel smoke harness remains the only owner of `/dev/nbd0`, mount
  cleanup, server shutdown, and scenario-level artifacts.
- The Make targets remain the operator API and exit with the runner status.

Operational and lifecycle contracts
- The plain Docker smoke runner owns image build, artifact setup, and the outer
  container invocation for kernel smoke.
- The S3 runner owns sidecar startup, readiness, log collection, and cleanup.
- The S3 runner installs an exit trap after it creates Docker resources. The
  trap collects RustFS logs when possible and cleans resources unless
  `KEEP_RUSTFS=1`.
- The artifact directory is created before expensive or stateful steps begin.
- A Ctrl-C or command failure follows the same cleanup contract as a normal
  failed step.
- `VERBOSE=1` may show redacted command lines and stream or tee child output.
  Default mode captures child output and prints bounded failure tails.
- Artifact logs are diagnostic files, not the public console contract. Existing
  smoke artifacts may still contain test-only credentials because generated
  config files already do.

Alternatives considered
- Keep the Makefile targets and add more `@` prefixes:
  - rejected because it only hides some command echo. The Makefile would still
    own complex lifecycle logic, and future edits could reintroduce noisy or
    secret-bearing output.
- Keep orchestration in Make but wrap Docker commands with helper variables:
  - rejected because Make is a poor place for failure summaries, traps,
    redaction, and command arrays.
- Migrate only `docker-smoke-s3` first:
  - rejected because it leaves sibling `docker-smoke*` targets with the old
    output model and splits the operator experience.
- Rewrite the runner in Python:
  - deferred. Python would be more pleasant for structured output, but this
    path is mostly Docker command orchestration and Bash is already required by
    the smoke harness.
- Rewrite the runner in Rust or add an `xtask` binary:
  - deferred. That may make sense once smoke orchestration grows a matrix,
    machine-readable reports, or richer artifact indexing, but it is too much
    toolchain surface for this output cleanup.
- Update only the inner kernel smoke harness:
  - rejected because the worst noise and the secret-shaped command echo come
    from the outer Make/Docker orchestration.
- Stream the whole kernel smoke log live:
  - rejected because it would reintroduce noisy Prisma, Cargo, mkfs,
    nbd-client, and server output into the default console. A progress side
    channel gives useful milestones without losing the clean log boundary.

Migration / rollout
- First slice:
  - add the shared output helper;
  - add the shared Docker-smoke helper;
  - add the plain Docker smoke runner;
  - add the S3 runner with `run`, `probe`, and `down` modes;
  - convert `docker-smoke`, `docker-smoke-s3`, `docker-smoke-s3-probe`, and
    `docker-smoke-s3-down` to thin Make wrappers;
  - preserve target names, default variables, artifact paths, and exit status.
- No repository user should need to change the command they run for any
  `make docker-smoke*` target.
- Future Docker smoke targets should be implemented as scripts using the same
  helper libraries instead of adding lifecycle logic to the Makefile.

Validation strategy
- Syntax and helper proof:
  - `bash -n scripts/docker/lib/smoke-log.sh`
  - `bash -n scripts/docker/lib/smoke-docker.sh`
  - `bash -n scripts/docker/docker-smoke.sh`
  - `bash -n scripts/docker/docker-smoke-s3.sh`
  - a small helper self-check or shell test that proves `smoke_run` reports a
    failing step, writes a log, and redacts secret-shaped environment values in
    verbose command output;
  - a helper self-check that proves `smoke_run_with_progress` prints progress
    events while keeping raw command output in the captured log.
- End-to-end behavior:
  - `make docker-smoke`
  - `make docker-smoke-s3-probe`
  - `make docker-smoke-s3`
  - verify each console has concise state and step output;
  - verify each command exits zero on a passing smoke;
  - verify artifacts include the expected logs for that target;
  - verify S3 console output does not include `DOCKER_SMOKE_S3_SECRET_KEY`,
    `KERNEL_SMOKE_S3_SECRET_ACCESS_KEY`, `NBD_TEST_S3_SECRET_ACCESS_KEY`, or
    the configured secret value.
- Failure-path behavior:
  - run a focused helper failure check, or run the S3 runner with an invalid
    RustFS image in a disposable artifact directory;
  - verify the console names the failed step, tails the relevant log, exits
    nonzero, and cleans Docker resources unless `KEEP_RUSTFS=1`.
- Compatibility:
  - `KEEP_RUSTFS=1 make docker-smoke-s3-probe`
  - `KEEP_RUSTFS=1 make docker-smoke-s3`
  - `make docker-smoke-s3-down`
  - optional `VERBOSE=1 make docker-smoke` and
    `VERBOSE=1 make docker-smoke-s3` for local command visibility.

Risks
- Capturing child output can hide progress during a long smoke. The kernel
  smoke path mitigates this with a progress side channel whose events are
  intentionally small and secret-free; full command output remains in artifact
  logs.
- A Bash helper library can become too clever. Keep the first helpers small and
  generic: rendering, capture, redaction, log tails, and common Docker argument
  construction only.
- Moving Docker build execution into the runners changes where build output is
  displayed. Each target should still build the same image with the same args,
  and the build log should be easy to find on failure.
- Console redaction does not make artifact logs secret-safe. This is acceptable
  for the first slice because the existing smoke already writes test config
  artifacts, but the distinction must remain explicit.
- Docker cleanup traps need careful ordering so a failed setup does not mask
  the original failure.
- Migrating all `docker-smoke*` targets at once increases first-slice scope, but
  the shared framework is small and the targets have overlapping Docker command
  shapes. The operator-facing consistency is worth taking now.

Open questions
- none

Design exit criteria
- The Makefile/script ownership split is accepted.
- Bash is accepted as the right runner language for this slice.
- The default output contract is accepted: concise console, captured logs, and
  bounded tails on failure.
- The redaction invariant is accepted for console output.
- The first slice scope is accepted as all current `make docker-smoke*` targets:
  `docker-smoke`, `docker-smoke-s3`, `docker-smoke-s3-probe`, and
  `docker-smoke-s3-down`.

Recommended next step
- `$review-plan` after this draft design is accepted.
- Treat `ready for series planning` as permission to ask whether to start
  `$plan-series`, not as permission to start `$plan-series` automatically.
