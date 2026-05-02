DOCKER_IMAGE ?= nbd-server-dev:local
DOCKER_CONTAINER ?= nbd-server-dev
DOCKER_PLATFORM ?= linux/arm64
DOCKER_WORKDIR ?= /work
DOCKER_CARGO_TARGET_DIR ?= /cargo-target
DOCKER_INTERACTIVE_FLAGS ?= -it
DOCKER_PATH := $(DOCKER_CARGO_TARGET_DIR)/debug:/usr/local/cargo/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

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

DOCKER_RUN_WORKSPACE = $(DOCKER_RUN_BASE) $(DOCKER_WORKSPACE_ARGS)
DOCKER_RUN_WORKSPACE_NAMED = $(DOCKER_RUN_NAMED) $(DOCKER_WORKSPACE_ARGS)
DOCKER_RUN = $(DOCKER_RUN_WORKSPACE) $(DOCKER_IMAGE)

.PHONY: test fmt clippy build-tools docker-build docker-test docker-shell docker-kernel-shell docker-attach docker-stop

test:
	cargo test --workspace

fmt:
	cargo fmt --all

clippy:
	cargo clippy --workspace --all-targets -- -D warnings

build-tools:
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

docker-stop:
	-docker rm -f $(DOCKER_CONTAINER)
