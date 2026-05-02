Title: Docker Kernel NBD Smoke
Date: 2026-05-02
Status: approved

Problem
- The userspace TCP tests prove our protocol and toy export path, but they do
  not prove that a real Linux NBD client can attach the server to the kernel
  block layer.
- We need a cheap, repeatable Linux environment from macOS on Apple Silicon
  without making privileged kernel tests part of the normal inner loop.
- We also need an interactive workflow where a named container can stay running
  while additional shells attach for commands like `make test`.

Goal
- Add a Docker-based smoke environment that can:
  - build and test the Rust workspace using the container toolchain;
  - run the toy server inside Linux;
  - provision an isolated SQLite catalog and export;
  - connect a real kernel NBD device with `nbd-client`;
  - create, mount, write, read, sync, unmount, and disconnect the export;
  - support both one-command automated smoke and manual interactive use.

Constraints
- Runtime code remains Rust.
- The kernel smoke requires Linux kernel NBD support and privileged container
  access.
- The default local platform is `linux/arm64` because the primary development
  machine is Apple Silicon. The platform must be overridable for Linux/x86
  hosts.
- The smoke test must use temp config and temp SQLite state. It must not touch
  the operator's default `~/.nbd` catalog.
- Docker/kernel smoke must stay outside the normal `make test` loop.
- The server is still the toy non-durable in-memory implementation.
- The smoke path should use the real `nbd-client` userspace tool and the Linux
  kernel NBD device, not our `nbd-us-client` validation crate.
- In the Debian smoke image, `nbd-client` resolves to `/usr/sbin/nbd-client`
  from the prebuilt `nbd-client` package. The Rust validation client is named
  `nbd-us-client` so it cannot be confused with the kernel attach tool.
- The container should be useful for manual Linux testing without requiring a
  full image rebuild after every source edit.

Non-goals
- CI-ready privileged kernel testing.
- Supporting Docker Desktop installations whose Linux VM cannot expose kernel
  NBD support.
- Production image hardening.
- A minimal runtime-only image.
- WAL, `ExportReadView`, storage engines, compaction, leases, or durability.
- Multi-connection kernel NBD testing.
- Testing persistence across server restart.
- Replacing the userspace TCP integration tests as the normal correctness proof.

End state
- `make docker-build` builds a local Linux development/smoke image.
- `make docker-test` runs the normal Rust test/fmt/clippy loop inside the
  container without kernel privileges.
- `make docker-shell` opens an interactive Linux shell with the repository
  mounted and Cargo caches preserved.
- `make docker-kernel-shell` opens the same interactive shell with kernel
  privileges for manual NBD attachment.
- `make docker-attach` opens another shell inside the named interactive
  container that is already running.
- `make build-tools` builds `nbd-server` and `nbdcli` into the shared Cargo
  target directory.
- `make docker-stop` force-removes the named interactive container.
- `make docker-smoke` runs an automated privileged kernel NBD smoke script in
  an anonymous one-shot container.
- Inside the interactive shell, an operator can run:

```text
make test
make fmt
make clippy
make build-tools
make kernel-smoke-inner
```

- The smoke script fails early with a clear message if kernel NBD support,
  device access, or required tools are unavailable.

Proposed approach
- Add a single development/smoke Docker image first.
- Use a Debian-based Rust image rather than Alpine. Debian keeps `nbd-client`,
  `kmod`, `mount`, `mkfs.ext4`, Node/npm for Prisma, and glibc-based Rust builds
  on the most boring path.
- Build and run the image with Docker Buildx and an explicit platform:

```text
DOCKER_PLATFORM ?= linux/arm64
DOCKER_IMAGE ?= nbd-server-dev:local
```

- The initial image should be based on `rust:1-slim-bookworm` and install only
  the smoke/development tools we need:

```text
make
nodejs
npm
nbd-client
kmod
util-linux
e2fsprogs
sqlite3
netcat-openbsd
bash
ca-certificates
```

- The repository should be bind-mounted into `/work`. Cargo registry/git caches
  and the Linux target directory should use Docker named volumes so host macOS
  build artifacts are not mixed with Linux container artifacts:

```text
/work                         bind mount of repository
/usr/local/cargo/registry     named volume
/usr/local/cargo/git          named volume
/cargo-target                 named volume, used via CARGO_TARGET_DIR
```

- Interactive shell targets mount the repository read/write for development.
  The automated smoke target should mount the repository read-only and write all
  build artifacts to `/cargo-target`.
- The interactive shell targets should reserve `DOCKER_CONTAINER` so
  `make docker-attach` and `make docker-stop` can target the running shell.
  Automated one-shot targets such as `docker-test` and `docker-smoke` should not
  reserve that stable name, which keeps CI-style and repeated local runs from
  conflicting with an open development shell.
- Privileged mode is used only for the kernel smoke and the kernel-capable
  manual shell. The unprivileged container test path does not receive
  `--privileged`.

Server binary
- Series 4 introduced a server library, but kernel smoke needs an operator
  process with a stable listen address.
- Add a minimal `nbd-server` binary for the toy server:

```text
nbd-server serve [--config /path/to/config.toml] --listen 127.0.0.1:10809
```

- The binary should:
  - load the explicit config path when provided;
  - otherwise use `ConfigSource::DefaultUserPath`, which may bootstrap
    `$HOME/.nbd/config.toml`;
  - bind the provided listen address;
  - serve the existing toy `MemoryExport` path;
  - print the listen address after binding;
  - exit non-zero on startup or serving errors;
  - remain honest that this is the toy non-durable server.

- The binary does not create exports, run migrations, or own lifecycle policy.
  Provisioning remains explicit through Prisma migration commands and `nbdcli`.

Automated smoke flow
- `make docker-smoke` should build the image if needed, then run a privileged
  one-shot container that executes the inner smoke script.
- The inner script should perform this flow:

```text
create /tmp/nbd-smoke
write /tmp/nbd-smoke/config.toml with catalog.url=file:/tmp/nbd-smoke/catalog.db
DATABASE_URL=file:/tmp/nbd-smoke/catalog.db make -C prisma db-migrate
make build-tools
$CARGO_TARGET_DIR/debug/nbdcli --config /tmp/nbd-smoke/config.toml \
  create smoke --size 67108864 --block-size 4096
$CARGO_TARGET_DIR/debug/nbd-server serve \
  --config /tmp/nbd-smoke/config.toml \
  --listen 127.0.0.1:10809 &
wait until 127.0.0.1:10809 accepts TCP
verify kernel NBD support is enabled and /dev/nbd0 exists
ensure /dev/nbd0 is disconnected
/usr/sbin/nbd-client 127.0.0.1 10809 /dev/nbd0 -name smoke -block-size 4096
mkfs.ext4 -F -E nodiscard /dev/nbd0
mount /dev/nbd0 /mnt/nbd-smoke
write a small file
sync
read the file back and compare contents
umount /mnt/nbd-smoke
/usr/sbin/nbd-client -d /dev/nbd0
stop the server
remove /tmp/nbd-smoke
```

- The script must use `trap` cleanup so failed checks still unmount,
  disconnect, and stop the toy server where possible.
- The script should avoid discard-dependent behavior because the toy server does
  not advertise or implement trim/write-zeroes.
- The script should build once with `make build-tools`, then invoke
  `$CARGO_TARGET_DIR/debug/nbdcli`,
  `$CARGO_TARGET_DIR/debug/nbd-server`, and `/usr/sbin/nbd-client` directly
  rather than relying on `cargo run` or `PATH` resolution.
- Docker Desktop 4.63.0 on the primary Apple Silicon machine reports LinuxKit
  6.12.72 with `CONFIG_BLK_DEV_NBD=y`. In that environment, NBD is built into
  the kernel, so `modprobe nbd` fails even though `/proc/devices` contains the
  NBD major and `/dev/nbd0` exists. The smoke preflight should accept built-in
  NBD and should not require a loadable `nbd` module or readable kernel config.

Manual workflow
- `make docker-shell` should open an unprivileged development shell by default:

```text
make docker-build
make docker-shell
```

- `make docker-kernel-shell` should open the same image with `--privileged` for
  manual kernel testing:

```text
make docker-kernel-shell
```

- While that named container is running, another host terminal can attach a new
  shell to it:

```text
make docker-attach
```

- `docker-attach` does not add privileges to an existing container. If the
  original shell was started with `docker-shell`, attached shells are
  unprivileged. If it was started with `docker-kernel-shell`, attached shells
  inherit that privileged container environment.
- Inside either shell, the repository is mounted at `/work` and uses the Linux
  Cargo target volume. This makes these commands natural:

```text
make test
make fmt
make clippy
make build-tools
```

- Inside the privileged shell, the operator can run the same inner smoke target
  as the automated path:

```text
make kernel-smoke-inner
```

Data model / API shape
- Makefile variables:

```make
DOCKER_IMAGE ?= nbd-server-dev:local
DOCKER_CONTAINER ?= nbd-server-dev
DOCKER_PLATFORM ?= linux/arm64
DOCKER_WORKDIR ?= /work
DOCKER_CARGO_TARGET_DIR ?= /cargo-target
DOCKER_INTERACTIVE_FLAGS ?= -it
KERNEL_SMOKE_EXPORT ?= smoke
KERNEL_SMOKE_SIZE_BYTES ?= 67108864
KERNEL_SMOKE_PORT ?= 10809
KERNEL_SMOKE_DEVICE ?= /dev/nbd0
```

- Root Makefile targets:

```text
docker-build
docker-test
docker-shell
docker-kernel-shell
docker-smoke
docker-stop
build-tools
kernel-smoke-inner
```

- Files:

```text
docker/Dockerfile
scripts/docker/kernel-smoke.sh
crates/nbd-server/src/main.rs
```

- Source-of-truth state:
  - repository files are the source for code and Docker behavior;
  - the temp config and temp SQLite catalog are the smoke export metadata truth;
  - `MemoryExport` is the byte-content source while the toy server process runs;
  - `/dev/nbd0` is derived kernel state and must be cleaned up.

Invariants
- `make test` remains unprivileged and does not depend on Docker.
- `make docker-test` does not use `--privileged`.
- `make docker-smoke` mounts the repository read-only.
- Kernel smoke uses an explicit temp config path and explicit `DATABASE_URL`.
- Kernel smoke never uses the developer's default `~/.nbd` state.
- Kernel smoke connects with one NBD connection.
- Kernel smoke uses a fixed export size at or below the toy memory export cap.
- Kernel smoke cleans up mounts, NBD connections, and server process state on
  success and best-effort on failure.
- Kernel smoke does not claim durability across server restart.
- The Docker image is a development/smoke image, not a production runtime image.
- Missing kernel NBD support is reported as environment unsupported, not as a
  server correctness failure.

Alternatives considered
- Alpine base image:
  - Smaller, but musl plus NBD/kernel tooling adds avoidable friction. Debian is
    the lower-risk smoke base.
- Multi-stage runtime-only image:
  - Useful later, but less ergonomic for manual `make test` and iterative
    debugging. The first image should be a development/smoke image.
- Build on macOS and copy binaries into Linux:
  - This adds cross-compilation complexity. Building inside the target Linux
    container proves the container toolchain directly.
- Userspace-only smoke inside Docker:
  - Already covered by Rust integration tests. Series 5 exists specifically to
    prove kernel NBD.
- `qemu-nbd`:
  - Useful NBD tooling, but this smoke should use the standard Linux
    `nbd-client` path that attaches a kernel NBD block device.

Migration / rollout
- No data migration is needed.
- The Docker smoke uses an isolated temp catalog and creates a throwaway export.
- The first rollout is local/manual only:
  - default developer loop remains `make test`, `make fmt`, `make clippy`;
  - Docker kernel smoke is opt-in through `make docker-smoke`;
  - CI integration is deferred until we know the runner can provide privileged
    Linux NBD support.

Validation strategy
- Normal local validation:

```text
make test
make fmt
make clippy
```

- Container toolchain validation:

```text
make docker-test
```

- Kernel NBD validation:

```text
make docker-smoke
```

- Manual validation:

```text
make docker-kernel-shell
make kernel-smoke-inner
```

- The kernel smoke proves:
  - the container can build and run the server;
  - Prisma migrations can initialize a temp SQLite catalog in the container;
  - `nbdcli` can create the export in that catalog;
  - the toy server can serve the export to the Linux kernel client;
  - a mounted filesystem can perform basic write/read/sync over `/dev/nbd0`;
  - cleanup can detach the NBD device and stop the server.

Risks
- Docker Desktop on macOS may not expose usable kernel NBD support in its Linux
  VM. The smoke script must make this failure explicit.
- A mounted filesystem may issue block operations beyond the toy server's
  advertised feature set if command options are not constrained. Use
  `mkfs.ext4 -E nodiscard` and do not advertise unsupported NBD flags.
- Privileged containers are intentionally broad. Keep them out of the default
  test path and document the risk in the Makefile target comments or README.
- `nbd-client` command-line behavior can vary slightly by distribution version.
  Keep the invocation simple and avoid multi-connection options.
- Server shutdown and NBD disconnect cleanup must be reliable enough that failed
  smoke runs do not leave `/dev/nbd0` busy.

Open questions
- None for the design. The current primary Docker Desktop Linux VM exposes
  built-in NBD support, and environments without it should fail during smoke
  preflight with a clear unsupported-environment message.

Design exit criteria
- The base image, platform strategy, and privileged/non-privileged command split
  are accepted.
- The minimal toy `nbd-server serve` binary is accepted as part of Series 5.
- The automated kernel smoke flow and cleanup contract are accepted.
- The manual shell workflow is accepted.
- The Docker Desktop/kernel-NBD limitation is accepted as an environment
  preflight rather than a code correctness failure.

Recommended next step
- Run `$review-plan` after the draft is accepted, before Series 5 execution
  planning.

References
- Docker multi-platform build documentation:
  https://docs.docker.com/build/building/multi-platform/
- Docker run privileged/device documentation:
  https://docs.docker.com/engine/containers/run/
- Linux kernel NBD documentation:
  https://kernel.org/doc/html/latest/admin-guide/blockdev/nbd.html
- NetworkBlockDevice userland README:
  https://github.com/NetworkBlockDevice/nbd
- Rust Docker official image:
  https://hub.docker.com/_/rust
