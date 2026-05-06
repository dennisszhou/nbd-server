DOCKER_IMAGE ?= nbd-server-dev:local
DOCKER_CONTAINER ?= nbd-server-dev
DOCKER_PLATFORM ?= linux/arm64
DOCKER_WORKDIR ?= /work
DOCKER_CARGO_TARGET_DIR ?= /cargo-target
DOCKER_INTERACTIVE_FLAGS ?= -it
DOCKER_PATH := $(DOCKER_CARGO_TARGET_DIR)/debug:/usr/local/cargo/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
KERNEL_SMOKE_OUTPUT ?= docker-smoke
DOCKER_KERNEL_SMOKE_ARTIFACT_DIR ?= $(CURDIR)/.tmp/$(KERNEL_SMOKE_OUTPUT)
DOCKER_KERNEL_SMOKE_ARTIFACT_MOUNT ?= /tmp/nbd-smoke-artifacts
KERNEL_SMOKE_EFFECTIVE_ENGINE = $(if $(KERNEL_SMOKE_ENGINE),$(KERNEL_SMOKE_ENGINE),wal_durable)
KERNEL_SMOKE_EFFECTIVE_SCENARIO = $(if $(KERNEL_SMOKE_SCENARIO),$(KERNEL_SMOKE_SCENARIO),$(if $(filter memory,$(KERNEL_SMOKE_EFFECTIVE_ENGINE)),memory-basic,$(if $(filter simple_durable simple-durable,$(KERNEL_SMOKE_EFFECTIVE_ENGINE)),simple-durable-basic,$(if $(filter wal_durable wal-durable,$(KERNEL_SMOKE_EFFECTIVE_ENGINE)),wal-durable-basic,unknown))))

DOCKER_RUN_BASE = docker run --rm \
	--platform $(DOCKER_PLATFORM)

DOCKER_RUN_NAMED = $(DOCKER_RUN_BASE) \
	--name $(DOCKER_CONTAINER)

DOCKER_WORKSPACE_ARGS = \
	-v "$(CURDIR):$(DOCKER_WORKDIR)" \
	-v nbd-cargo-registry:/usr/local/cargo/registry \
	-v nbd-cargo-git:/usr/local/cargo/git \
	-v nbd-cargo-target:$(DOCKER_CARGO_TARGET_DIR) \
	-e CARGO_TARGET_DIR=$(DOCKER_CARGO_TARGET_DIR) \
	-e PATH=$(DOCKER_PATH) \
	-w $(DOCKER_WORKDIR)

DOCKER_WORKSPACE_READONLY_ARGS = \
	-v "$(CURDIR):$(DOCKER_WORKDIR):ro" \
	-v nbd-cargo-registry:/usr/local/cargo/registry \
	-v nbd-cargo-git:/usr/local/cargo/git \
	-v nbd-cargo-target:$(DOCKER_CARGO_TARGET_DIR) \
	-e CARGO_TARGET_DIR=$(DOCKER_CARGO_TARGET_DIR) \
	-e PATH=$(DOCKER_PATH) \
	-w $(DOCKER_WORKDIR)

DOCKER_RUN_WORKSPACE = $(DOCKER_RUN_BASE) $(DOCKER_WORKSPACE_ARGS)
DOCKER_RUN_WORKSPACE_NAMED = $(DOCKER_RUN_NAMED) $(DOCKER_WORKSPACE_ARGS)
DOCKER_RUN_WORKSPACE_READONLY = $(DOCKER_RUN_BASE) $(DOCKER_WORKSPACE_READONLY_ARGS)
DOCKER_RUN = $(DOCKER_RUN_WORKSPACE) $(DOCKER_IMAGE)
DOCKER_KERNEL_SMOKE_ENV = \
	$(if $(KERNEL_SMOKE_EXPORT),-e KERNEL_SMOKE_EXPORT=$(KERNEL_SMOKE_EXPORT)) \
	$(if $(KERNEL_SMOKE_SCENARIO),-e KERNEL_SMOKE_SCENARIO=$(KERNEL_SMOKE_SCENARIO)) \
	$(if $(KERNEL_SMOKE_SIZE_BYTES),-e KERNEL_SMOKE_SIZE_BYTES=$(KERNEL_SMOKE_SIZE_BYTES)) \
	$(if $(KERNEL_SMOKE_ENGINE),-e KERNEL_SMOKE_ENGINE=$(KERNEL_SMOKE_ENGINE)) \
	$(if $(KERNEL_SMOKE_PORT),-e KERNEL_SMOKE_PORT=$(KERNEL_SMOKE_PORT)) \
	$(if $(KERNEL_SMOKE_DEVICE),-e KERNEL_SMOKE_DEVICE=$(KERNEL_SMOKE_DEVICE)) \
	$(if $(KERNEL_SMOKE_RUST_LOG),-e KERNEL_SMOKE_RUST_LOG=$(KERNEL_SMOKE_RUST_LOG)) \
	$(if $(KERNEL_SMOKE_COMPACTION_SETTLE_SECONDS),-e KERNEL_SMOKE_COMPACTION_SETTLE_SECONDS=$(KERNEL_SMOKE_COMPACTION_SETTLE_SECONDS)) \
	-e KERNEL_SMOKE_ARTIFACT_DIR=$(DOCKER_KERNEL_SMOKE_ARTIFACT_MOUNT)
DOCKER_KERNEL_SMOKE_ARTIFACT_ARGS = \
	-v "$(DOCKER_KERNEL_SMOKE_ARTIFACT_DIR):$(DOCKER_KERNEL_SMOKE_ARTIFACT_MOUNT)"

.PHONY: test test-protocol fmt clippy build docker-build docker-test \
	docker-shell docker-kernel-shell docker-attach docker-smoke docker-stop \
	kernel-smoke-inner

test:
	cargo test --workspace

test-protocol:
	cargo test -p nbd-server --test tcp_integration

fmt:
	cargo fmt --all

clippy:
	cargo clippy --workspace --all-targets -- -D warnings

build:
	cargo build -p nbd-server -p nbdcli

docker-build:
	docker buildx build \
		--platform $(DOCKER_PLATFORM) \
		-t $(DOCKER_IMAGE) \
		-f docker/Dockerfile \
		--load \
		.

docker-test: docker-build
	$(DOCKER_RUN) sh -lc 'make test && make fmt && make clippy'

docker-shell: docker-build
	$(DOCKER_RUN_WORKSPACE_NAMED) $(DOCKER_INTERACTIVE_FLAGS) $(DOCKER_IMAGE) bash

docker-kernel-shell: docker-build
	$(DOCKER_RUN_WORKSPACE_NAMED) $(DOCKER_INTERACTIVE_FLAGS) --privileged $(DOCKER_IMAGE) bash

docker-attach:
	docker exec $(DOCKER_INTERACTIVE_FLAGS) -w $(DOCKER_WORKDIR) \
		$(DOCKER_CONTAINER) bash

docker-smoke: docker-build
	@echo "docker smoke:"
	@echo "  scenario: $(KERNEL_SMOKE_EFFECTIVE_SCENARIO)"
	@echo "  artifacts: $(DOCKER_KERNEL_SMOKE_ARTIFACT_DIR)"
	mkdir -p "$(DOCKER_KERNEL_SMOKE_ARTIFACT_DIR)"
	$(DOCKER_RUN_WORKSPACE_READONLY) $(DOCKER_KERNEL_SMOKE_ENV) $(DOCKER_KERNEL_SMOKE_ARTIFACT_ARGS) --privileged $(DOCKER_IMAGE) make kernel-smoke-inner
	@echo "docker smoke artifacts: $(DOCKER_KERNEL_SMOKE_ARTIFACT_DIR)"

docker-stop:
	-docker rm -f $(DOCKER_CONTAINER)

kernel-smoke-inner:
	bash scripts/docker/kernel-smoke.sh
