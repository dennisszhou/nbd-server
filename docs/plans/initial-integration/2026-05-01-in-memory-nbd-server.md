Title: In-Memory NBD Server
Date: 2026-05-01
Status: approved

# Problem

The first data-path checkpoint should prove the NBD wire protocol and request
loop without WAL, `ExportReadView`, storage engines, compaction, Docker, or a
kernel NBD client. The server needs to be real enough to speak TCP NBD to a
userspace validation client, while byte contents stay intentionally
non-durable behind the `Export` boundary.

# Goal

Implement the M2/M3 in-memory server slice:

- `nbd-protocol` crate for fixed-newstyle wire parsing/encoding;
- `nbd-us-client` crate for a small serial userspace validation client;
- `nbd-server` crate for listener, connection, and export serving;
- in-memory export backing store;
- integration tests that create an export via `nbd-control-plane`, start the
  server, and prove read/write/flush/disconnect over TCP.

# Constraints

- Runtime code must be Rust.
- The userspace validation client must exercise real TCP protocol framing.
- The server must use the catalog-created export metadata for size/name.
- The backing store is in-memory and intentionally not durable.
- No WAL, `ExportReadView`, `StorageEngine`, compaction, S3, Docker, or kernel
  NBD is implemented in this slice.
- The server should not advertise protocol flags it does not implement.
- The server and client should bound wire-advertised lengths before allocating
  buffers: option payloads are limited to 64 KiB and read/write I/O is limited
  to 64 MiB in this in-memory slice.
- Integration tests must use temp config and temp SQLite databases.
- Tests for behavior should land with the behavior they prove; Series 4 should
  not end with a generic test-only coverage commit.
- `MemoryExport` should reject sizes above an explicit in-memory limit
  instead of attempting an unbounded allocation.

# Non-Goals

- Oldstyle handshake.
- `NBD_OPT_EXPORT_NAME`.
- `NBD_OPT_LIST`.
- Standalone `NBD_OPT_INFO`.
- Structured replies.
- TLS.
- FUA, trim, write-zeroes, block status, cache, or
  `NBD_FLAG_CAN_MULTI_CONN`.
- Pipelined or out-of-order client requests.
- A fully general reusable NBD client with rich option coverage.
- Scripted protocol peers.
- Real persistence.
- Concurrent request execution beyond one task per accepted connection.
- Docker or privileged kernel-NBD testing.
- Operator-ready `nbd-server` binary packaging.
- Visibility between multiple simultaneous connections to the same export.

# End State

After this slice:

- a userspace validation client can connect to `nbd-server` over TCP;
- the client can negotiate an export with `NBD_OPT_GO`;
- reads from a new export return zeroes;
- writes update the in-memory export;
- later reads observe earlier writes;
- flush succeeds as a no-op barrier for `MemoryExport`;
- disconnect closes the connection cleanly;
- missing/deleted exports fail during option negotiation;
- two catalog exports served by the same process have independent in-memory
  contents;
- the integration test creates export metadata through `nbd-control-plane`.

# Proposed Approach

Add two crates and use the existing protocol crate:

```text
crates/nbd-protocol
  protocol constants, wire parsing/encoding, request/reply structs

crates/nbd-us-client
  small serial userspace validation client over TCP

crates/nbd-server
  server library, connection loop, export opener, MemoryExport backend
```

`nbd-protocol` must not depend on `nbd-control-plane` or `nbd-server`.
`nbd-us-client` must not depend on `nbd-server`, `nbd-control-plane`, or
`nbd-test-support`. `nbd-server` may depend on `nbd-protocol`,
`nbd-control-plane`, and `nbd-config`; it may use `nbd-test-support` only in
tests.

# Protocol Scope

Support fixed newstyle only:

```text
server: INIT_PASSWD, IHAVEOPT, NBD_FLAG_FIXED_NEWSTYLE | NBD_FLAG_NO_ZEROES
client: NBD_FLAG_C_FIXED_NEWSTYLE, optionally NBD_FLAG_C_NO_ZEROES
options: NBD_OPT_GO, NBD_OPT_ABORT
commands: READ, WRITE, FLUSH, DISC
replies: simple replies
supported option payload: <= 64 KiB
supported read/write length: <= 64 MiB
```

Advertise transmission flags:

```text
NBD_FLAG_HAS_FLAGS
NBD_FLAG_SEND_FLUSH
```

Do not advertise:

```text
NBD_FLAG_SEND_FUA
NBD_FLAG_CAN_MULTI_CONN
```

The first implementation should preserve reply order for simplicity. It can
process one request at a time per connection while still keeping the socket
read/write behavior clear enough to evolve later.

# Userspace Validation Client

`nbd-us-client` is a small validation client, not a production NBD client.

It should provide a serial API:

```rust
impl NbdClient {
    async fn connect(addr: SocketAddr, export_name: &str) -> Result<Self>;
    fn export_size_bytes(&self) -> u64;
    fn transmission_flags(&self) -> u16;
    async fn read(&mut self, offset: u64, len: u32) -> Result<Vec<u8>>;
    async fn write(&mut self, offset: u64, data: &[u8]) -> Result<()>;
    async fn flush(&mut self) -> Result<()>;
    async fn disconnect(self) -> Result<()>;
}
```

Scope:

- one negotiated export per connection;
- parse and retain the negotiated export size and transmission flags;
- one in-flight command at a time;
- monotonically generated cookies for request/reply correlation;
- validate reply magic, cookie, and NBD error values;
- fail tests clearly on protocol errors or unexpected disconnects.

Non-goals:

- pipelining;
- reconnects;
- option discovery/listing;
- standalone `NBD_OPT_INFO`;
- structured replies;
- kernel-NBD parity.

The API should be informed by existing NBD clients such as libnbd's
read/write/flush shape, but the implementation should stay minimal and should
not copy external library code.

# In-Memory Export Model

Define a server-side `Export` boundary and use an in-memory implementation for
this slice.

Conceptual boundary:

```rust
#[async_trait::async_trait]
trait Export: Send + Sync {
    async fn read(&self, offset: u64, len: u32) -> Result<Vec<u8>>;
    async fn write(&self, offset: u64, data: &[u8]) -> Result<()>;
    async fn flush(&self) -> Result<()>;
}
```

The exact Rust API can follow the existing `nbd-control-plane` async trait
style. The important boundary is that socket handling depends on an export
interface, not on `MemoryExport` directly.

Use an in-memory implementation:

```rust
struct MemoryExport {
    name: ExportName,
    size_bytes: u64,
    block_size: u64,
    data: Vec<u8>,
}
```

Semantics:

- initial bytes are zero;
- construction rejects export sizes above the in-memory limit;
- reads validate bounds and return exactly the requested bytes;
- writes validate bounds and copy bytes into `data`;
- flush is a no-op that returns success after earlier in-memory writes
  complete;
- disconnect does not persist anything.

This export intentionally does not implement WAL durability. Tests should not
claim durability beyond the lifetime of the in-memory server.

`MemoryExport` is the first concrete implementation behind the `Export`
boundary. Later durable work can replace the factory with a WAL/read-view backed
implementation without changing protocol handling.

# Export Opening

For this in-memory slice, the server should open exports on demand through the
catalog when the client sends `NBD_OPT_GO`.

Preferred M3 path:

```text
test creates export through nbd-control-plane
server starts with explicit config
client sends NBD_OPT_GO(name)
server loads export metadata from ExportCatalog
server creates MemoryExport for that active connection
```

Deleted or missing exports should fail during `NBD_OPT_GO`.

The NBD server should support multiple export names in one server process by
loading each export from the catalog by name. Tests do not need to start one
server per export. Since Series 4 creates a fresh `MemoryExport` per successful
connection, independent contents for two exports should fall out naturally and
should be covered by an integration test. Same-export visibility across
multiple simultaneous connections is out of scope until the server has a real
export registry and durable backing store.

Open/delete race prevention is out of scope. The catalog doc already accepts
that M1/M3 ignore this race for the in-memory example.

# Server Lifecycle

The test server should support:

```rust
struct TestServer {
    addr: SocketAddr,
    shutdown: ShutdownHandle,
}
```

Starting the server should:

- bind to `127.0.0.1:0`;
- expose the selected address to tests;
- accept connections and spawn one sequential task per accepted connection;
- serve until shutdown or connection close;
- release the port when dropped or shut down.

M2/M3 can use a single-threaded or simple async server. It does not need a
full production workqueue yet. It should still keep protocol parsing separate
from export read/write logic.

The long-term architecture separates inbound socket handling, export work, and
per-connection reply serialization. Series 4 may collapse those boundaries into
one sequential connection task as long as the code does not make export logic
depend on socket internals.

# Request Handling

For the in-memory server, request handling can be sequential:

```text
decode request
validate bounds and flags
execute MemoryExport operation
write simple reply
read next request
```

Sequential handling is enough for userspace validation and avoids introducing
admission-control complexity before WAL/read-view exist. The protocol-facing
request structs should still match the long-term command shape so later
workqueue/admission changes do not rewrite wire parsing.

# Source Of Truth

- `ExportCatalog` is the source of export metadata: name, size, block size,
  and deleted/active state.
- `MemoryExport` is the in-process source of byte contents for the NBD server.
- The validation client observes only the TCP protocol, not server internals.
- No durable byte-content source exists in this slice.

# Invariants

- `nbd-protocol` does not depend on catalog or server crates.
- `nbd-us-client` does not depend on server, catalog, or test-support crates.
- Server/catalog test harness helpers live in `nbd-test-support`.
- Server connection handling calls through the `Export` boundary, not directly
  through `MemoryExport` internals.
- The server advertises only features it implements.
- Successful reads return exactly the requested number of bytes.
- Wire lengths are capped before allocation.
- Out-of-bounds reads/writes fail with an NBD error.
- Successful in-memory writes are visible to later reads on the same connection.
- Flush returns only after earlier sequential in-memory writes have completed.
- `NBD_CMD_DISC` closes without a command reply.
- Missing/deleted exports do not enter transmission mode.
- One NBD server can serve multiple catalog exports by name.
- Each successful connection gets its own `MemoryExport`.
- Integration tests use real TCP framing through `nbd-us-client`.
- Tests use temp config and temp SQLite catalogs.

# Alternatives Considered

## Start With Kernel NBD

Kernel NBD would prove more real behavior but makes basic protocol debugging
slower and requires privileged Linux setup. Userspace TCP validation should be
the inner loop first.

## Build A Scripted Peer First

A scripted peer would isolate `nbd-us-client` from `nbd-server`, but it creates
a second protocol implementation just for tests. Series 4 should validate
against the real NBD server instead.

## Call Server Internals From Tests

Internal tests are useful for small parsing or export units, but the integration
proof must exercise real TCP framing. Otherwise the riskiest boundary remains
untested.

## Implement Workqueues Immediately

The architecture needs workqueues later, but the in-memory server does not need
them to prove handshake/read/write/flush. Sequential request handling keeps the
first data-path slice smaller.

# Migration / Rollout

No migration is needed. This extends the initial workspace with protocol and
client/server crates.

# Validation Strategy

Expected checks:

- `make test`
- `make fmt`
- `make clippy`
- `MemoryExport` tests for zero reads, write/readback, bounds errors, and
  flush no-op;
- integration test for active export connect and read zeroes;
- integration test for write/readback/flush/disconnect;
- integration test for independent export contents;
- integration test for missing or deleted export failure.

The integration tests should create their own temp catalog and export metadata
through `nbd-control-plane`. Behavior tests should appear with the commits that
introduce the behavior.

# Risks

- Accidentally implementing a private protocol shape instead of NBD framing.
- Letting the validation client depend on server internals.
- Making write success sound durable when it is only in-memory.
- Introducing concurrency before the first protocol path is proven.
- Failing to preserve enough protocol structure for later workqueue/admission
  evolution.
- Choosing an in-memory limit that is too high for the normal local test loop or
  too low for useful protocol validation.

# Open Questions

None.

# Design Approval

This design is approved for Series 4 execution planning with these accepted
boundaries:

- the `nbd-protocol` / `nbd-server` split is accepted;
- the intentionally small `nbd-us-client` validation scope is accepted;
- sequential request handling is accepted for the in-memory slice;
- the in-memory export semantics are accepted as non-durable;
- the `MemoryExport` size limit is accepted;
- TCP integration through `nbd-us-client` is accepted as the primary proof; and
- the `Export` / `MemoryExport` boundary is accepted.

# Recommended Next Step

Plan and approve Series 4 before implementation.
