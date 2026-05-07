Title: S3 Blob Store Demo
Date: 2026-05-07
Status: approved

# Problem

The server now has a `BlobStore` / `MutableBlobStore` boundary and one shared
local blob-store handle owned by `ExportFactory`. The next demo milestone is to
prove that committed WAL/COW blob data can live behind an S3-compatible object
store instead of local files.

MinIO was the first local S3 substitute considered, but its upstream situation
makes it less attractive as the demo dependency. RustFS is a plausible local
S3-compatible target because it has an official Docker image, supports a
single-node local deployment, documents S3 SDK usage, and can run beside the
privileged NBD smoke container on a Docker bridge network.

The storage design should not become RustFS-specific. The durable storage
contract is the S3-compatible blob-store contract: create-only object writes,
ranged object reads, and one shared backend client per server process.

# Goal

Add enough S3 blob-store support to run a Docker/kernel smoke scenario where a
`wal_durable` export compacts committed COW blobs into S3-compatible storage
and later reads those committed blobs back through the normal NBD path.

The demo should:

- keep local blob storage as the default;
- add explicit config for choosing `local` versus `s3` blob storage;
- implement `S3BlobStore` behind the existing `BlobStore` trait;
- use RustFS as the local S3-compatible service in Docker smoke;
- run RustFS as a sidecar container on the same Docker network as smoke;
- exercise the S3 path through `wal_durable`, close compaction, clone, reopen,
  and readback;
- keep WAL files and SQLite catalog local for this demo;
- avoid making `simple_durable` pretend to support S3 mutable blob replacement.

# Constraints

- This is a demo slice, not the full production S3 storage design.
- The base `BlobStore::put_blob` contract remains create-only.
- `S3BlobStore` should implement `BlobStore`, not `MutableBlobStore`, in this
  slice.
- `simple_durable` requires `MutableBlobStore` and remains local-only unless a
  later design accepts mutable S3 object replacement.
- WAL storage remains local behind `LocalWalProvider`.
- Catalog storage remains SQLite.
- The server should construct one shared S3 client/store during startup and
  clone the same handle into all durable exports.
- S3 credentials must not be logged.
- RustFS is a demo dependency. The smoke tooling should pin a RustFS image tag
  or digest instead of relying on `latest`.
- RustFS compatibility with the exact create-only operation we need must be
  verified before execution treats it as contract-compatible.
- The design should remain compatible with real AWS S3. RustFS startup details
  stay in Docker scripts, not in the storage API.

# Non-goals

- Production S3 hardening.
- Multipart upload.
- Object delete or garbage collection.
- S3-backed WAL.
- S3-backed catalog.
- Cross-server fencing or leases.
- Storage worker queues, backend-wide fairness, or request coalescing.
- Server-side encryption, KMS, object locking, retention, or bucket policies.
- Making `simple_durable` use S3.
- Migrating existing local blobs into S3.
- Running Docker from inside the privileged smoke container.

# End state

The server supports this config shape:

```toml
[blob_store]
kind = "local"
```

When omitted, `[blob_store]` defaults to local and continues to use
`runtime.blob_dir`.

For the RustFS smoke scenario, the config can select S3:

```toml
[blob_store]
kind = "s3"
endpoint_url = "http://rustfs:9000"
region = "us-east-1"
bucket = "everstore"
access_key_id = "rustfsadmin"
secret_access_key = "rustfsadmin"
force_path_style = true
key_prefix = "v0.1/blobs/"
auto_create_bucket = true
```

That maps committed blob objects into this namespace:

```text
s3://everstore/v0.1/blobs/<BlobKey>
```

`NbdServer::start_on` builds one blob-store backend from config. Local config
creates one `Arc<LocalBlobStore>`. S3 config creates one shared
`Arc<S3BlobStore>`.

`wal_durable` and COW compaction receive `BlobStoreHandle` and work with either
backend. `simple_durable` receives `MutableBlobStoreHandle` only when the
configured backend supports it. With S3 selected, opening a `simple_durable`
export returns a clear configuration/runtime error.

Docker smoke has a new S3 scenario, likely `wal-durable-s3-basic`, that starts
RustFS as a sidecar container, writes a config selecting the S3 blob store,
creates a `wal_durable` export, runs the existing kernel write/read workflow,
closes to compact into S3, reopens, and verifies readback.

# Proposed approach

## Configuration

Add a top-level `BlobStoreConfig` to `nbd-config`:

```rust
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum BlobStoreConfig {
    Local,
    S3(S3BlobStoreConfig),
}
```

The exact Rust representation can use struct variants if that is cleaner for
Serde:

```rust
pub struct S3BlobStoreConfig {
    pub endpoint_url: String,
    pub region: String,
    pub bucket: String,
    pub access_key_id: String,
    pub secret_access_key: String,
    pub force_path_style: bool,
    pub key_prefix: Option<String>,
    pub auto_create_bucket: bool,
}
```

Defaults:

- missing `[blob_store]` means local;
- generated config should include `[blob_store] kind = "local"` for
  discoverability;
- local still uses `runtime.blob_dir`;
- S3 ignores `runtime.blob_dir`, but the field remains in config for backward
  compatibility and test fixture stability.

Add config key support for non-secret values such as:

- `blob_store.kind`;
- `blob_store.endpoint_url`;
- `blob_store.region`;
- `blob_store.bucket`;
- `blob_store.force_path_style`;
- `blob_store.key_prefix`;
- `blob_store.auto_create_bucket`.

Do not expose `blob_store.secret_access_key` through `config get` unless there
is an explicit later operator decision to support secret printing.

`bucket` is the S3 bucket name. `key_prefix` is the allocation namespace within
that bucket. The display/debug form of the namespace is:

```text
s3://<bucket>/<normalized key_prefix>
```

The implementation should keep `bucket` and `key_prefix` as separate config
fields instead of accepting one `s3://...` URI as the source of truth. The
endpoint, region, path-style mode, and credentials are independent S3 client
settings, and keeping the bucket separate avoids ambiguous URI parsing.

For the first S3 demo, the bucket is owned by the backend server and the prefix
can stay simple:

```text
bucket = "everstore"
key_prefix = "v0.1/blobs/"
```

`v0.1` is a namespace/version label, not a catalog schema version. It gives us
a clean place to allocate new object keys without mixing future object keys
with this demo layout.

Long term, the prefix should become export-scoped, likely:

```text
<org>/<owner>/<export_name>/v0.1/blobs/
```

That requires catalog identity fields the server does not have yet. For this
demo, process config is the source of truth for the active blob-store location.

## Backend construction

Move blob-store construction one level above `ExportFactory`. `NbdServer`
should open the configured blob store from process config and pass that
configured store into `ExportFactory`.

```rust
pub enum ConfiguredBlobStore {
    Local(Arc<LocalBlobStore>),
    S3(Arc<S3BlobStore>),
}

impl ConfiguredBlobStore {
    pub async fn open(config: &NbdConfig) -> Result<Self>;
    pub fn blob_store(&self) -> BlobStoreHandle;
    pub fn mutable_blob_store(&self) -> Option<MutableBlobStoreHandle>;
}
```

`NbdServer::start_on` should call `ConfiguredBlobStore::open(&config).await?`
before constructing `ExportFactory`. `ExportFactory` should take
`ConfiguredBlobStore` instead of constructing local storage from
`runtime.blob_dir`.

Opening an export then selects the capability it needs:

```text
NbdConfig
  -> ConfiguredBlobStore::open(...)
  -> ExportFactory
     -> wal_durable gets ConfiguredBlobStore::blob_store()
     -> simple_durable gets ConfiguredBlobStore::mutable_blob_store()
```

This keeps the existing "one shared backend object per server process" shape.
It also gives S3 one long-lived SDK client inside `S3BlobStore`, which is
enough for the demo and for connection reuse. A producer/consumer storage
runtime can still wrap this configured store later if scheduling or retry
shaping becomes necessary.

## Blob-store hierarchy

The trait hierarchy stays intentionally small:

```text
BlobStore
  get_blob(...)
  put_blob(...)

MutableBlobStore: BlobStore
  overwrite_blob(...)
```

The concrete stores then line up by capability:

```text
LocalBlobStore: BlobStore + MutableBlobStore
S3BlobStore:    BlobStore
```

`wal_durable` only needs `BlobStoreHandle`, so it can use either local or S3.
`simple_durable` needs `MutableBlobStoreHandle`, so it can only use a backend
that supports overwrite semantics. This keeps the backend distinction at
factory/open time instead of leaking it into the request path.

The factory selection should behave like:

```text
Local backend
  blob_store()         -> Arc<dyn BlobStore>
  mutable_blob_store() -> Some(Arc<dyn MutableBlobStore>)

S3 backend
  blob_store()         -> Arc<dyn BlobStore>
  mutable_blob_store() -> None
```

This is the main reason `S3BlobStore` can have the same caller shape as
`LocalBlobStore` for `wal_durable` without pretending to satisfy the stronger
mutable contract required by `simple_durable`.

## S3 blob store

Add `crates/nbd-server/src/storage/s3.rs` behind an `s3` Cargo feature.

`S3BlobStore` owns:

```rust
pub struct S3BlobStore {
    client: Arc<aws_sdk_s3::Client>,
    bucket: String,
    key_prefix: String,
}
```

`ConfiguredBlobStore::open(config).await` builds one AWS SDK S3 client with:

- static demo credentials from config;
- configured region;
- optional `endpoint_url` for RustFS or other S3-compatible endpoints;
- `force_path_style = true` for RustFS/local endpoints;
- optional bucket bootstrap when `auto_create_bucket` is true.

The resulting `S3BlobStore` owns the shared client plus the configured
bucket/prefix.

The S3 object key is derived from the namespace and random blob id:

```text
<normalized key_prefix><random blob id>
```

`key_prefix` may be empty. If present, normalize it to have no leading slash
and exactly one trailing slash. Reject prefixes that attempt path traversal or
are otherwise ambiguous. The catalog stores the random blob id in leaf
metadata. `S3BlobStore` supplies the configured prefix.

For example:

```text
bucket = "everstore"
key_prefix = "v0.1/blobs/"
random blob id = "abc123"

object key = "v0.1/blobs/abc123"
display namespace = "s3://everstore/v0.1/blobs/"
```

Implement `BlobStore`:

```rust
async fn put_blob(&self, key: &BlobKey, data: &[u8]) -> Result<()>;
async fn get_blob(&self, key: &BlobKey, offset: u64, len: u64)
    -> Result<Vec<u8>>;
```

`put_blob` must use S3 conditional create semantics. For S3 this means
`PutObject` with `If-None-Match: *`. Existing-key responses such as
`PreconditionFailed` should be classified as "blob already exists" so
`put_random_blob` keeps its existing retry behavior.

This conditional write is the main compatibility gate for RustFS. AWS S3
documents `If-None-Match: *` for create-only `PutObject`, but the implementation
must prove that the pinned RustFS image honors the same behavior. If RustFS
does not honor it, the demo should switch local S3 providers or explicitly
remain blocked rather than weakening `BlobStore::put_blob`.

`get_blob` should use a single S3 `GetObject` range request:

```text
Range: bytes=<offset>-<offset + len - 1>
```

If `len == 0`, return an empty `Vec` without issuing a request. Check for range
end overflow before building the header. A missing object referenced by catalog
metadata remains a storage error, not a sparse zero.

For the demo, collect the response body into memory. Current committed chunks
are 32 MiB, and existing local paths already read full chunk images for
compaction. Streaming can be designed later.

## Error classification

The storage boundary needs one backend-independent way to classify create
collisions.

The narrowest acceptable approach is to add a helper used by both backends and
`put_random_blob`, for example:

```rust
pub(crate) fn blob_already_exists(context: &'static str, key: &BlobKey)
    -> ServerError;
pub(crate) fn is_blob_already_exists(error: &ServerError) -> bool;
```

Local `put_blob` can keep mapping `create_new` `AlreadyExists` errors into
that classification. S3 `put_blob` maps `If-None-Match` failures into the same
classification.

If implementation shows that fitting S3 service errors into `ServerError::Io`
is misleading or brittle, introduce a small storage-specific `ServerError`
variant instead. The invariant is more important than the exact enum shape:
random-key allocation must retry key collisions and must not retry unrelated
S3 failures such as auth, bucket missing, endpoint unavailable, or invalid
range.

## Docker and RustFS

Add RustFS only to the demo/smoke tooling.

The automated smoke path should use an isolated e2e RustFS sidecar plus the
existing privileged NBD smoke container on one user-defined Docker bridge
network:

```text
host Makefile
  -> docker network create nbd-smoke-s3-e2e
  -> docker run -d --name nbd-smoke-s3-e2e-rustfs \
       --network nbd-smoke-s3-e2e ...
  -> docker run --rm --privileged --network nbd-smoke-s3-e2e \
       nbd-server-dev ...
```

The RustFS container gets a stable network alias:

```text
rustfs
```

The NBD smoke container uses:

```text
endpoint_url = "http://rustfs:9000"
```

This avoids Docker-in-Docker and avoids running a second long-lived service
inside the privileged smoke container. It also matches the shape we actually
care about: the NBD server talks to an external S3 endpoint over TCP.

Makefile additions:

```make
RUSTFS_IMAGE ?= rustfs/rustfs:<pinned-tag-or-digest>
DOCKER_SMOKE_S3_NETWORK ?= nbd-smoke-s3-e2e
DOCKER_SMOKE_S3_RUSTFS_CONTAINER ?= nbd-smoke-s3-e2e-rustfs
DOCKER_SMOKE_S3_RUSTFS_ALIAS ?= rustfs
DOCKER_SMOKE_S3_RUSTFS_VOLUME ?= nbd-smoke-s3-e2e-rustfs-data
DOCKER_SMOKE_S3_ACCESS_KEY ?= rustfsadmin
DOCKER_SMOKE_S3_SECRET_KEY ?= rustfsadmin

DOCKER_RUSTFS_NETWORK ?= nbd-rustfs-dev
DOCKER_RUSTFS_CONTAINER ?= nbd-rustfs-dev
DOCKER_RUSTFS_ALIAS ?= rustfs
DOCKER_RUSTFS_VOLUME ?= nbd-rustfs-dev-data
```

The S3 smoke target should:

- create the Docker network if needed;
- start RustFS with `--network-alias rustfs`;
- pass the configured demo access key and secret into RustFS;
- mount RustFS data either as a Docker volume or an artifact directory;
- wait until the S3 endpoint is usable;
- run the privileged smoke container on the same network;
- pass endpoint, bucket, prefix, and credentials into the smoke container;
- collect RustFS logs with the other smoke artifacts;
- remove the RustFS container and network on exit.

The Makefile owns both Docker networks, but the lifecycles are different:

- `docker-smoke-s3` creates an ephemeral e2e network and sidecar for that run;
- `docker-smoke-s3` deletes the e2e network, sidecar, and volume on normal
  exit;
- `docker-rustfs-up` creates the manual dev network and sidecar;
- `docker-rustfs-down` deletes the manual dev network, sidecar, and volume;
- the smoke container only receives a network attachment and S3 environment
  variables; it never creates or removes Docker resources.

If e2e cleanup fails, the recovery path is explicit:

```text
make docker-smoke-s3-down
```

That target removes the RustFS sidecar container, the S3 smoke network, and the
temporary RustFS data volume. It should be safe to run before or after a failed
smoke attempt.

If RustFS data is bind-mounted from the host, account for RustFS running as
UID `10001`. For the first demo, a named Docker volume is simpler because it
avoids host directory ownership churn. If host-visible object files are useful
for debugging, use a bind mount and add a small permission-prep step.

Harness changes:

- add a `wal-durable-s3-basic` scenario;
- generate a server config with `[blob_store] kind = "s3"`;
- set `endpoint_url = "http://rustfs:9000"` by default in that scenario;
- use the same bucket, prefix, access key, and secret as the RustFS sidecar;
- create a fresh catalog and bucket/prefix per smoke run;
- run the existing WAL durable write, close, clone, reopen, and readback flow.

Add a new smoke scenario rather than changing the default:

```text
KERNEL_SMOKE_SCENARIO=wal-durable-s3-basic make docker-smoke-s3
```

The existing `wal-durable-basic` scenario remains local. Keeping the S3 smoke
target separate avoids pulling RustFS for every default local smoke run.

## Developer UX

The default developer command should be one-shot:

```text
make docker-smoke-s3
```

That target should own the full lifecycle:

1. build the NBD smoke image;
2. create a user-defined Docker bridge network;
3. start RustFS on that network with alias `rustfs`;
4. wait until RustFS accepts S3 requests;
5. run the privileged NBD smoke container on the same network;
6. export NBD server logs, config, inspect snapshots, and RustFS logs;
7. remove the RustFS container and network unless debugging is requested.

The smoke container should not need to know Docker exists. From its point of
view, RustFS is just this endpoint:

```text
http://rustfs:9000
```

This keeps the common flow simple while still letting the user learn the pieces.

Add separate interactive targets for manual debugging:

```text
make docker-rustfs-up
make docker-rustfs-down
make docker-kernel-shell
```

`docker-rustfs-up` starts the pinned RustFS sidecar on the manual dev network
and leaves it running. `docker-kernel-shell` should ensure that manual network
exists, join it by default, and receive the same S3 environment variables. The
user can then run:

```text
make kernel-smoke-inner
```

inside the container, or inspect connectivity manually with:

```text
nc -z rustfs 9000
```

The target names should make cleanup obvious. `docker-smoke-s3-down` removes
only e2e smoke resources. `docker-rustfs-down` removes only manual RustFS
resources.

For failed one-shot runs, the default should clean up containers but keep logs
under `.tmp/docker-smoke-s3`. A debug flag can keep RustFS running:

```text
KEEP_RUSTFS=1 make docker-smoke-s3
```

When `KEEP_RUSTFS=1` is set, the final message should print the active network
name, RustFS container name, and the exact `make docker-smoke-s3-down` cleanup
command.

## RustFS compatibility probe

Before implementing the full server path, add a tiny script or Rust test that
targets the pinned RustFS sidecar and proves the S3 operations we depend on:

1. create bucket;
2. `PutObject` new key with `If-None-Match: *` succeeds;
3. `PutObject` same key with `If-None-Match: *` fails as a collision;
4. `GetObject` with a byte range returns exact bytes;
5. missing object returns a missing-object error;
6. auth or bucket errors are not mistaken for create collisions.

This can be a smoke helper first. It does not need to be a permanent production
test if it would make ordinary `cargo test` depend on Docker.

# Data model / API shape

No catalog schema changes are needed for this demo.

Authoritative state stays split:

- SQLite catalog owns export heads and COW metadata that reference `BlobKey`;
- server config owns the active blob-store location;
- local WAL owns uncheckpointed acknowledged writes;
- S3/RustFS owns committed COW blob bytes for S3-configured servers;
- local filesystem owns committed blob bytes for local-configured servers.

Because this is a shared backend server, the process owns the S3 bucket and the
operator config owns the active prefix. That is enough for the demo. The
catalog does not need to store bucket or prefix until the system has stable
`org`, `owner`, and export identity semantics.

The process-level ownership model is:

```text
NbdConfig
  -> ConfiguredBlobStore::open(...)
  -> ExportFactory
     -> BlobStoreHandle clones for wal_durable/COW paths
     -> MutableBlobStoreHandle only for local simple_durable paths
```

The request path does not learn whether the backend is local or S3.

## Catalog blob references

Tree leaf metadata continues to store only the blob id:

```text
tree_leaf_refs.storage_key = <BlobKey>
```

The configured blob store maps that id to concrete storage. For S3:

```text
bucket = "everstore"
key_prefix = "v0.1/blobs/"
tree_leaf_refs.storage_key = "abc123"

resolved object = "s3://everstore/v0.1/blobs/abc123"
```

For local:

```text
root = "/var/lib/nbd/blobs"
key_prefix = ""
tree_leaf_refs.storage_key = "abc123"

resolved file = "/var/lib/nbd/blobs/abc123"
```

The same location model can also support a local prefix later:

```text
root = "/var/lib/nbd/blobs"
key_prefix = "v0.1/blobs/"
storage_key = "abc123"

resolved file = "/var/lib/nbd/blobs/v0.1/blobs/abc123"
```

For backward compatibility, local config should default to an empty prefix and
continue resolving blobs as `<runtime.blob_dir>/<BlobKey>`. `BlobKey` remains a
safe one-component id.

# Invariants

- `BlobStore::put_blob` is create-only for every backend.
- `ConfiguredBlobStore` owns the root/bucket plus prefix used to resolve blob
  ids for this process.
- Leaf metadata stores one-component blob ids.
- Leaf metadata does not store S3 buckets, prefixes, endpoints, credentials,
  or full `s3://...` URIs in this demo.
- `S3BlobStore` must never overwrite an existing object for `put_blob`.
- S3 create collisions are retryable only for random-key allocation.
- Missing sparse tree metadata reads as zeroes.
- Metadata pointing at a missing S3 object is a storage error.
- `S3BlobStore` does not implement `MutableBlobStore` in this demo.
- `simple_durable` cannot open against an S3-only backend.
- The server owns one shared S3 client/store per process.
- WAL durability and S3 blob durability are separate. A write success still
  depends on local WAL durability, not immediate S3 compaction.
- Bucket auto-create is opt-in and intended for demo/test only.
- Credentials are never emitted in logs or config-get output.
- RustFS is replaceable by another S3-compatible endpoint without touching
  catalog metadata or request-path code.

# Operational / lifecycle contracts

Startup:

- parse config;
- construct the selected blob backend;
- for S3, build the SDK client and optionally verify/create the bucket;
- fail startup on invalid S3 config, missing credentials, unreachable endpoint,
  or missing bucket when `auto_create_bucket` is false.

Runtime:

- regular writes append to local WAL and update the read view;
- compaction writes committed chunk blobs through the configured `BlobStore`;
- reads of committed chunks use ranged `get_blob`;
- compaction failures remain best-effort where they already are today.

Shutdown:

- no new S3-specific shutdown operation is required;
- the SDK client is dropped with the server process;
- smoke cleanup must stop RustFS after stopping NBD activity.

# Alternatives considered

## Producer/consumer storage runtime

A storage runtime with workers and a shared queue would centralize backend
fairness, retries, and observability. It is not necessary for the demo. The
shared SDK client already gives connection reuse, and current request ordering
belongs to export admission and WAL/read-view code.

## S3 implements `MutableBlobStore`

S3 can overwrite objects, but the current base contract intentionally prevents
that. Supporting mutable S3 blobs would make `simple_durable` appear portable
while preserving none of its local rename/fsync assumptions. Keep S3 immutable
for the demo and use `wal_durable`.

## Config under `[runtime]`

Adding `runtime.blob_store_kind` would be smaller, but S3 needs endpoint,
bucket, region, credentials, path-style mode, prefix, and bucket bootstrap.
A top-level `[blob_store]` section keeps storage backend configuration
cohesive and avoids crowding local runtime paths.

## RustFS inside the smoke container

Installing RustFS inside the privileged smoke image would keep one container,
but it would add process supervision, readiness, log capture, and shutdown
logic inside a script that already owns kernel NBD setup. The sidecar topology
is cleaner because Docker owns the RustFS process and Docker DNS gives the NBD
container a stable endpoint.

## Docker Compose

Docker Compose is good for humans, but the existing smoke harness is Makefile
and plain Docker based. A direct Makefile-managed network and sidecar avoids
adding a second orchestration tool for one service.

## SeaweedFS, Garage, LocalStack, or Ceph

These remain viable S3-compatible local endpoints if RustFS fails the
conditional-write probe. The storage code should not care which one is used.
The demo dependency should be selected by contract compatibility, Docker
ergonomics, and maintenance posture.

# Migration / rollout

No catalog data migration is needed for the demo.

Existing configs without `[blob_store]` continue to use local blob storage,
`runtime.blob_dir`, and an empty local prefix. Existing local blob keys remain
valid one-component ids.

Switching an existing catalog from local to S3 is not a transparent migration.
The leaf metadata will still point at the same `BlobKey` ids, but the selected
blob-store location will resolve those ids in a different backend. Operators
should use a fresh catalog/bucket for the demo. Copying existing local blobs
into S3 is out of scope.

Rollout should be staged:

1. prove RustFS sidecar compatibility with conditional put and range get;
2. add config model and default/local compatibility;
3. add backend construction while preserving local behavior;
4. add `S3BlobStore` and focused storage contract tests;
5. add Docker network/sidecar smoke support;
6. add the `wal-durable-s3-basic` scenario.

# Validation strategy

Config validation:

- old configs without `[blob_store]` load as local;
- generated config includes local blob-store selection;
- S3 config loads all fields;
- unknown S3 fields are rejected;
- secret values are not returned by `config get`.

Storage contract validation:

- `BlobKey` remains a safe one-component id;
- blob-store prefixes reject absolute paths, empty path components, `.`, `..`,
  NUL bytes, and backslashes;
- local location with empty prefix resolves as `<runtime.blob_dir>/<BlobKey>`;
- S3 location resolves as `s3://<bucket>/<key_prefix><BlobKey>`;
- local and S3 `put_blob` reject existing keys;
- `put_random_blob` retries create collisions for both backends;
- S3 `put_random_blob` writes objects under the configured `key_prefix`;
- S3 `get_blob` returns exact ranged bytes;
- zero-length `get_blob` returns empty bytes;
- missing referenced object returns a storage error;
- auth, endpoint, and bucket errors are not classified as create collisions.

Server integration validation:

- local registry tests still pass with default local config;
- opening a `simple_durable` export with S3 config fails clearly;
- opening and using `wal_durable` with S3 config succeeds.

Docker smoke validation:

```text
cargo fmt --all --check
cargo test -p nbd-config
cargo test -p nbd-server --lib
cargo test -p nbd-server --test local_export_registry
cargo test -p nbd-server --test wal_durable
KERNEL_SMOKE_SCENARIO=wal-durable-s3-basic make docker-smoke-s3
```

If the S3 feature is feature-gated, the relevant cargo and Docker commands
must build `nbd-server` with that feature enabled.

# Risks

- The AWS SDK dependency may materially increase compile time and image size.
  Feature-gating S3 keeps the default local workflow lighter.
- RustFS is newer and should be treated as a demo endpoint until the contract
  probe and smoke path prove the operations we need.
- S3 conditional-write behavior must be verified against the exact RustFS image
  used by the demo.
- Storing demo credentials in config is convenient but not a production secret
  handling model.
- Bucket auto-create is useful for demos but should not be assumed in
  production.
- S3 object reads collect up to a full chunk into memory. That matches current
  local compaction behavior but is not the final streaming design.
- S3 is eventually an external service. The existing close compaction path is
  best-effort, so transient S3 failures can delay committed checkpoints while
  WAL replay still preserves acknowledged writes.
- The Makefile must clean up the RustFS container and network reliably, or
  repeated smoke runs will leave stale resources behind.

# External references

- RustFS GitHub: `https://github.com/rustfs/rustfs`
- RustFS Docker installation:
  `https://docs.rustfs.com/installation/docker/index.html`
- RustFS SDK overview: `https://docs.rustfs.com/developer/sdk/`
- RustFS Rust SDK guide:
  `https://docs.rustfs.com/developer/sdk/rust.html`
- AWS S3 conditional writes:
  `https://docs.aws.amazon.com/AmazonS3/latest/userguide/`
  `conditional-writes.html`
- AWS SDK for Rust endpoint documentation:
  `https://docs.aws.amazon.com/sdk-for-rust/latest/dg/endpoints.html`

# Open questions

- Does the pinned RustFS image honor `PutObject` with `If-None-Match: *` exactly
  enough for `BlobStore::put_blob`? This is the main gate.
- Should S3 be behind a Cargo feature for the first implementation? The design
  recommends yes, but execution planning should decide the exact build targets
  and Makefile shape.
- Should demo credentials live directly in config, or should config name
  environment variables to read? The design chooses direct config values for
  the first Docker demo and explicitly treats that as non-production.
- Should bucket creation live in `S3BlobStore::open` or only in smoke scripts?
  The design recommends opt-in `auto_create_bucket` in `S3BlobStore::open`
  because it keeps the smoke harness smaller and makes startup validation
  explicit.

# Design exit criteria

This design is ready for `$review-plan` when:

- `[blob_store]` is accepted as the config section name;
- local default/backward compatibility is accepted;
- S3 support is accepted as `wal_durable`/COW-only for the demo;
- `S3BlobStore` not implementing `MutableBlobStore` is accepted;
- config-owned `ConfiguredBlobStore` is accepted for this demo;
- create-only S3 writes using conditional put are accepted;
- the RustFS conditional-write probe is accepted as a pre-implementation gate;
- opt-in bucket auto-create is accepted for demo/test;
- RustFS sidecar topology is accepted as the first demo topology;
- S3 feature gating is either accepted or explicitly rejected.

# Recommended next step

Run `$review-plan` after the draft design is accepted. A `ready for series
planning` result should lead to asking whether to start `$plan-series`, not to
implementation automatically.
