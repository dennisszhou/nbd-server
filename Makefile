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
RUSTFS_IMAGE ?= rustfs/rustfs:1.0.0-alpha.94-glibc
DOCKER_SMOKE_S3_NETWORK ?= nbd-smoke-s3-e2e
DOCKER_SMOKE_S3_RUSTFS_CONTAINER ?= nbd-smoke-s3-e2e-rustfs
DOCKER_SMOKE_S3_RUSTFS_ALIAS ?= rustfs
DOCKER_SMOKE_S3_RUSTFS_VOLUME ?= nbd-smoke-s3-e2e-rustfs-data
DOCKER_SMOKE_S3_ARTIFACT_DIR ?= $(CURDIR)/.tmp/docker-smoke-s3
DOCKER_SMOKE_S3_ACCESS_KEY ?= rustfsadmin
DOCKER_SMOKE_S3_SECRET_KEY ?= rustfsadmin
DOCKER_SMOKE_S3_BUCKET ?= everstore
DOCKER_SMOKE_S3_KEY_PREFIX ?= v0.1/blobs/
DOCKER_RUSTFS_NETWORK ?= nbd-rustfs-dev
DOCKER_RUSTFS_CONTAINER ?= nbd-rustfs-dev
DOCKER_RUSTFS_ALIAS ?= rustfs
DOCKER_RUSTFS_VOLUME ?= nbd-rustfs-dev-data
DOCKER_RUSTFS_PORT ?= 9000
DOCKER_RUSTFS_CONSOLE_PORT ?= 9001

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
	$(if $(KERNEL_SMOKE_CARGO_FEATURES),-e KERNEL_SMOKE_CARGO_FEATURES=$(KERNEL_SMOKE_CARGO_FEATURES)) \
	$(if $(KERNEL_SMOKE_PORT),-e KERNEL_SMOKE_PORT=$(KERNEL_SMOKE_PORT)) \
	$(if $(KERNEL_SMOKE_DEVICE),-e KERNEL_SMOKE_DEVICE=$(KERNEL_SMOKE_DEVICE)) \
	$(if $(KERNEL_SMOKE_RUST_LOG),-e KERNEL_SMOKE_RUST_LOG=$(KERNEL_SMOKE_RUST_LOG)) \
	$(if $(KERNEL_SMOKE_S3_ENDPOINT_URL),-e KERNEL_SMOKE_S3_ENDPOINT_URL=$(KERNEL_SMOKE_S3_ENDPOINT_URL)) \
	$(if $(KERNEL_SMOKE_S3_BUCKET),-e KERNEL_SMOKE_S3_BUCKET=$(KERNEL_SMOKE_S3_BUCKET)) \
	$(if $(KERNEL_SMOKE_S3_ACCESS_KEY_ID),-e KERNEL_SMOKE_S3_ACCESS_KEY_ID=$(KERNEL_SMOKE_S3_ACCESS_KEY_ID)) \
	$(if $(KERNEL_SMOKE_S3_SECRET_ACCESS_KEY),-e KERNEL_SMOKE_S3_SECRET_ACCESS_KEY=$(KERNEL_SMOKE_S3_SECRET_ACCESS_KEY)) \
	$(if $(KERNEL_SMOKE_S3_REGION),-e KERNEL_SMOKE_S3_REGION=$(KERNEL_SMOKE_S3_REGION)) \
	$(if $(KERNEL_SMOKE_S3_KEY_PREFIX),-e KERNEL_SMOKE_S3_KEY_PREFIX=$(KERNEL_SMOKE_S3_KEY_PREFIX)) \
	$(if $(KERNEL_SMOKE_COMPACTION_SETTLE_SECONDS),-e KERNEL_SMOKE_COMPACTION_SETTLE_SECONDS=$(KERNEL_SMOKE_COMPACTION_SETTLE_SECONDS)) \
	-e KERNEL_SMOKE_ARTIFACT_DIR=$(DOCKER_KERNEL_SMOKE_ARTIFACT_MOUNT)
DOCKER_KERNEL_SMOKE_ARTIFACT_ARGS = \
	-v "$(DOCKER_KERNEL_SMOKE_ARTIFACT_DIR):$(DOCKER_KERNEL_SMOKE_ARTIFACT_MOUNT)"
DOCKER_SMOKE_S3_ENV = \
	-e NBD_TEST_S3_ENDPOINT_URL=http://$(DOCKER_SMOKE_S3_RUSTFS_ALIAS):9000 \
	-e NBD_TEST_S3_BUCKET=$(DOCKER_SMOKE_S3_BUCKET) \
	-e NBD_TEST_S3_ACCESS_KEY_ID=$(DOCKER_SMOKE_S3_ACCESS_KEY) \
	-e NBD_TEST_S3_SECRET_ACCESS_KEY=$(DOCKER_SMOKE_S3_SECRET_KEY) \
	-e NBD_TEST_S3_REGION=us-east-1 \
	-e NBD_TEST_S3_KEY_PREFIX=$(DOCKER_SMOKE_S3_KEY_PREFIX)
DOCKER_RUSTFS_SHELL_ENV = \
	-e NBD_TEST_S3_ENDPOINT_URL=http://$(DOCKER_RUSTFS_ALIAS):9000 \
	-e NBD_TEST_S3_BUCKET=$(DOCKER_SMOKE_S3_BUCKET) \
	-e NBD_TEST_S3_ACCESS_KEY_ID=$(DOCKER_SMOKE_S3_ACCESS_KEY) \
	-e NBD_TEST_S3_SECRET_ACCESS_KEY=$(DOCKER_SMOKE_S3_SECRET_KEY) \
	-e NBD_TEST_S3_REGION=us-east-1 \
	-e NBD_TEST_S3_KEY_PREFIX=$(DOCKER_SMOKE_S3_KEY_PREFIX)

.PHONY: test test-protocol fmt clippy build docker-build docker-test \
	docker-shell docker-kernel-shell docker-attach docker-smoke docker-stop \
	docker-rustfs-up docker-rustfs-down docker-smoke-s3-down \
	docker-smoke-s3-probe docker-smoke-s3 kernel-smoke-inner

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

docker-kernel-shell: docker-build docker-rustfs-up
	$(DOCKER_RUN_WORKSPACE_NAMED) $(DOCKER_INTERACTIVE_FLAGS) \
		--network $(DOCKER_RUSTFS_NETWORK) \
		$(DOCKER_RUSTFS_SHELL_ENV) \
		--privileged $(DOCKER_IMAGE) bash

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

docker-rustfs-up:
	@echo "starting RustFS dev sidecar:"
	@echo "  image: $(RUSTFS_IMAGE)"
	@echo "  network: $(DOCKER_RUSTFS_NETWORK)"
	@echo "  container: $(DOCKER_RUSTFS_CONTAINER)"
	@docker network inspect $(DOCKER_RUSTFS_NETWORK) >/dev/null 2>&1 || \
		docker network create $(DOCKER_RUSTFS_NETWORK) >/dev/null
	@docker volume inspect $(DOCKER_RUSTFS_VOLUME) >/dev/null 2>&1 || \
		docker volume create $(DOCKER_RUSTFS_VOLUME) >/dev/null
	@if docker inspect $(DOCKER_RUSTFS_CONTAINER) >/dev/null 2>&1; then \
		docker start $(DOCKER_RUSTFS_CONTAINER) >/dev/null; \
	else \
		docker run -d \
			--name $(DOCKER_RUSTFS_CONTAINER) \
			--network $(DOCKER_RUSTFS_NETWORK) \
			--network-alias $(DOCKER_RUSTFS_ALIAS) \
			-p $(DOCKER_RUSTFS_PORT):9000 \
			-p $(DOCKER_RUSTFS_CONSOLE_PORT):9001 \
			-v $(DOCKER_RUSTFS_VOLUME):/data \
			-e RUSTFS_ACCESS_KEY=$(DOCKER_SMOKE_S3_ACCESS_KEY) \
			-e RUSTFS_SECRET_KEY=$(DOCKER_SMOKE_S3_SECRET_KEY) \
			-e RUSTFS_ADDRESS=:9000 \
			$(RUSTFS_IMAGE) /data >/dev/null; \
	fi
	@echo "RustFS endpoint: http://localhost:$(DOCKER_RUSTFS_PORT)"
	@echo "cleanup: make docker-rustfs-down"

docker-rustfs-down:
	-docker rm -f $(DOCKER_RUSTFS_CONTAINER)
	-docker network rm $(DOCKER_RUSTFS_NETWORK)
	-docker volume rm $(DOCKER_RUSTFS_VOLUME)

docker-smoke-s3-down:
	-docker rm -f $(DOCKER_SMOKE_S3_RUSTFS_CONTAINER)
	-docker network rm $(DOCKER_SMOKE_S3_NETWORK)
	-docker volume rm $(DOCKER_SMOKE_S3_RUSTFS_VOLUME)

docker-smoke-s3-probe: docker-build
	@echo "docker S3 smoke probe:"
	@echo "  rustfs image: $(RUSTFS_IMAGE)"
	@echo "  network: $(DOCKER_SMOKE_S3_NETWORK)"
	@echo "  artifacts: $(DOCKER_SMOKE_S3_ARTIFACT_DIR)"
	@$(MAKE) docker-smoke-s3-down >/dev/null
	mkdir -p "$(DOCKER_SMOKE_S3_ARTIFACT_DIR)"
	docker network create $(DOCKER_SMOKE_S3_NETWORK) >/dev/null
	docker volume create $(DOCKER_SMOKE_S3_RUSTFS_VOLUME) >/dev/null
	docker run -d \
		--name $(DOCKER_SMOKE_S3_RUSTFS_CONTAINER) \
		--network $(DOCKER_SMOKE_S3_NETWORK) \
		--network-alias $(DOCKER_SMOKE_S3_RUSTFS_ALIAS) \
		-v $(DOCKER_SMOKE_S3_RUSTFS_VOLUME):/data \
		-e RUSTFS_ACCESS_KEY=$(DOCKER_SMOKE_S3_ACCESS_KEY) \
		-e RUSTFS_SECRET_KEY=$(DOCKER_SMOKE_S3_SECRET_KEY) \
		-e RUSTFS_ADDRESS=:9000 \
		$(RUSTFS_IMAGE) /data >/dev/null
	$(DOCKER_RUN_WORKSPACE_READONLY) --network $(DOCKER_SMOKE_S3_NETWORK) \
		$(DOCKER_IMAGE) sh -lc 'for i in $$(seq 1 100); do nc -z $(DOCKER_SMOKE_S3_RUSTFS_ALIAS) 9000 && exit 0; sleep 0.2; done; echo "timed out waiting for RustFS" >&2; exit 1'
	$(DOCKER_RUN_WORKSPACE_READONLY) --network $(DOCKER_SMOKE_S3_NETWORK) \
		$(DOCKER_SMOKE_S3_ENV) \
		$(DOCKER_IMAGE) bash scripts/docker/rustfs-s3-probe.sh
	docker logs $(DOCKER_SMOKE_S3_RUSTFS_CONTAINER) >"$(DOCKER_SMOKE_S3_ARTIFACT_DIR)/rustfs.log" 2>&1
	@if [ "$${KEEP_RUSTFS:-0}" = "1" ]; then \
		echo "kept RustFS sidecar running"; \
		echo "cleanup: make docker-smoke-s3-down"; \
	else \
		$(MAKE) docker-smoke-s3-down >/dev/null; \
	fi
	@echo "docker S3 smoke probe artifacts: $(DOCKER_SMOKE_S3_ARTIFACT_DIR)"

docker-smoke-s3: docker-build
	@echo "docker S3 smoke:"
	@echo "  scenario: wal-durable-s3-basic"
	@echo "  rustfs image: $(RUSTFS_IMAGE)"
	@echo "  network: $(DOCKER_SMOKE_S3_NETWORK)"
	@echo "  artifacts: $(DOCKER_SMOKE_S3_ARTIFACT_DIR)"
	@$(MAKE) docker-smoke-s3-down >/dev/null
	mkdir -p "$(DOCKER_SMOKE_S3_ARTIFACT_DIR)"
	docker network create $(DOCKER_SMOKE_S3_NETWORK) >/dev/null
	docker volume create $(DOCKER_SMOKE_S3_RUSTFS_VOLUME) >/dev/null
	docker run -d \
		--name $(DOCKER_SMOKE_S3_RUSTFS_CONTAINER) \
		--network $(DOCKER_SMOKE_S3_NETWORK) \
		--network-alias $(DOCKER_SMOKE_S3_RUSTFS_ALIAS) \
		-v $(DOCKER_SMOKE_S3_RUSTFS_VOLUME):/data \
		-e RUSTFS_ACCESS_KEY=$(DOCKER_SMOKE_S3_ACCESS_KEY) \
		-e RUSTFS_SECRET_KEY=$(DOCKER_SMOKE_S3_SECRET_KEY) \
		-e RUSTFS_ADDRESS=:9000 \
		$(RUSTFS_IMAGE) /data >/dev/null
	$(DOCKER_RUN_WORKSPACE_READONLY) --network $(DOCKER_SMOKE_S3_NETWORK) \
		$(DOCKER_IMAGE) sh -lc 'for i in $$(seq 1 100); do nc -z $(DOCKER_SMOKE_S3_RUSTFS_ALIAS) 9000 && exit 0; sleep 0.2; done; echo "timed out waiting for RustFS" >&2; exit 1'
	$(DOCKER_RUN_WORKSPACE_READONLY) --network $(DOCKER_SMOKE_S3_NETWORK) \
		-e KERNEL_SMOKE_SCENARIO=wal-durable-s3-basic \
		-e KERNEL_SMOKE_CARGO_FEATURES=s3 \
		-e KERNEL_SMOKE_S3_ENDPOINT_URL=http://$(DOCKER_SMOKE_S3_RUSTFS_ALIAS):9000 \
		-e KERNEL_SMOKE_S3_BUCKET=$(DOCKER_SMOKE_S3_BUCKET) \
		-e KERNEL_SMOKE_S3_ACCESS_KEY_ID=$(DOCKER_SMOKE_S3_ACCESS_KEY) \
		-e KERNEL_SMOKE_S3_SECRET_ACCESS_KEY=$(DOCKER_SMOKE_S3_SECRET_KEY) \
		-e KERNEL_SMOKE_S3_REGION=us-east-1 \
		-e KERNEL_SMOKE_S3_KEY_PREFIX=$(DOCKER_SMOKE_S3_KEY_PREFIX) \
		-e KERNEL_SMOKE_ARTIFACT_DIR=$(DOCKER_KERNEL_SMOKE_ARTIFACT_MOUNT) \
		-v "$(DOCKER_SMOKE_S3_ARTIFACT_DIR):$(DOCKER_KERNEL_SMOKE_ARTIFACT_MOUNT)" \
		--privileged $(DOCKER_IMAGE) make kernel-smoke-inner || { \
			docker logs $(DOCKER_SMOKE_S3_RUSTFS_CONTAINER) >"$(DOCKER_SMOKE_S3_ARTIFACT_DIR)/rustfs.log" 2>&1 || true; \
			if [ "$${KEEP_RUSTFS:-0}" != "1" ]; then \
				$(MAKE) docker-smoke-s3-down >/dev/null; \
			fi; \
			exit 1; \
		}
	docker logs $(DOCKER_SMOKE_S3_RUSTFS_CONTAINER) >"$(DOCKER_SMOKE_S3_ARTIFACT_DIR)/rustfs.log" 2>&1
	@if [ "$${KEEP_RUSTFS:-0}" = "1" ]; then \
		echo "kept RustFS sidecar running"; \
		echo "network: $(DOCKER_SMOKE_S3_NETWORK)"; \
		echo "container: $(DOCKER_SMOKE_S3_RUSTFS_CONTAINER)"; \
		echo "cleanup: make docker-smoke-s3-down"; \
	else \
		$(MAKE) docker-smoke-s3-down >/dev/null; \
	fi
	@echo "docker S3 smoke artifacts: $(DOCKER_SMOKE_S3_ARTIFACT_DIR)"

kernel-smoke-inner:
	bash scripts/docker/kernel-smoke.sh
