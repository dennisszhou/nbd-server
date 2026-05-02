Title: Toy NBD Server
Date: 2026-05-01
Status: approved

# Problem

The first data-path checkpoint should prove the NBD wire protocol and request
loop without WAL, `ExportReadView`, storage engines, compaction, Docker, or a
kernel NBD client. The server needs to be real enough to speak TCP NBD to a
mock client, but intentionally toy-like behind the `Export` boundary.

# Goal

Implement the M2/M3 toy server slice:

- `nbd-protocol` crate for fixed-newstyle wire parsing/encoding;
- `nbd-server` crate for listener, connection, and toy export serving;
- mock NBD client test helper that uses real TCP framing;
- in-memory export backing store;
- integration tests that create an export via `nbd-control-plane`, start the
  server, and prove read/write/flush/disconnect over TCP.

# Constraints

- Runtime code must be Rust.
- The mock client must exercise real TCP protocol framing.
- The server must use the catalog-created export metadata for size/name.
- The backing store is in-memory and intentionally not durable.
- No WAL, `ExportReadView`, `StorageEngine`, compaction, S3, Docker, or kernel
  NBD is implemented in this slice.
- The server should not advertise protocol flags it does not implement.
- Integration tests must use temp config and temp SQLite databases.

# Non-Goals

- Oldstyle handshake.
- `NBD_OPT_EXPORT_NAME`.
- `NBD_OPT_LIST`.
- Standalone `NBD_OPT_INFO`.
- Structured replies.
- TLS.
- FUA, trim, write-zeroes, block status, cache, or multi-connection support.
- Real persistence.
- Concurrent request execution beyond what is needed for the mock tests.
- Docker or privileged kernel-NBD testing.

# End State

After this slice:

- a mock client can connect to `nbd-server` over TCP;
- the client can negotiate an export with `NBD_OPT_GO`;
- reads from a new export return zeroes;
- writes update the in-memory export;
- later reads observe earlier writes;
- flush succeeds as a no-op barrier for the toy export;
- disconnect closes the connection cleanly;
- missing/deleted exports fail during option negotiation;
- the integration test creates export metadata through `nbd-control-plane`.

# Proposed Approach

Add two crates:

```text
crates/nbd-protocol
  protocol constants, wire parsing/encoding, request/reply structs,
  mock-client framing helpers for tests

crates/nbd-server
  server library/binary, connection loop, export opener, toy in-memory export
```

`nbd-protocol` must not depend on `nbd-control-plane` or `nbd-server`.
`nbd-server` may depend on `nbd-protocol`, `nbd-control-plane`, `nbd-config`,
and `nbd-test-support` only in tests.

# Protocol Scope

Support fixed newstyle only:

```text
server: INIT_PASSWD, IHAVEOPT, NBD_FLAG_FIXED_NEWSTYLE
client: NBD_FLAG_C_FIXED_NEWSTYLE
options: NBD_OPT_GO, NBD_OPT_ABORT
commands: READ, WRITE, FLUSH, DISC
replies: simple replies
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

# Toy Export Model

Use an in-memory export implementation:

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
- reads validate bounds and return exactly the requested bytes;
- writes validate bounds and copy bytes into `data`;
- flush is a no-op that returns success after earlier toy writes complete;
- disconnect does not persist anything.

This export intentionally does not implement WAL durability. Tests should not
claim durability beyond the lifetime of the in-memory server.

# Export Opening

For this toy slice, the server should open exports on demand through the
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

The toy server should support multiple export names in one server process by
loading each export from the catalog by name. Tests do not need to start one
server per export.

Open/delete race prevention is out of scope. The catalog doc already accepts
that M1/M3 ignore this race for the toy example.

# Mock Client

The mock client should be split by responsibility:

- low-level protocol framing and mock client connection logic live in
  `nbd-protocol`;
- higher-level test harness helpers that start servers or create temp catalogs
  live in `nbd-test-support`.

Mock client requirements:

- connect to `127.0.0.1:0` server address chosen by the OS;
- perform fixed-newstyle handshake;
- send `NBD_OPT_GO` with an export name;
- read export info and final option acknowledgement;
- issue read/write/flush/disconnect commands;
- validate cookies in replies;
- fail tests on protocol errors or unexpected disconnects.

The mock client must not call server internals.

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
- serve until shutdown or connection close;
- release the port when dropped or shut down.

M2/M3 can use a single-threaded or simple async server. It does not need a
full production workqueue yet. It should still keep protocol parsing separate
from export read/write logic.

# Request Handling

For the toy server, request handling can be sequential:

```text
decode request
validate bounds and flags
execute MemoryExport operation
write simple reply
read next request
```

Sequential handling is enough for mock-client proof and avoids introducing
admission-control complexity before WAL/read-view exist. The protocol-facing
request structs should still match the long-term command shape so later
workqueue/admission changes do not rewrite wire parsing.

# Source Of Truth

- `ExportCatalog` is the source of export metadata: name, size, block size,
  and deleted/active state.
- `MemoryExport` is the in-process source of byte contents for the toy server.
- The mock client observes only the TCP protocol, not server internals.
- No durable byte-content source exists in this slice.

# Invariants

- `nbd-protocol` does not depend on catalog or server crates.
- Low-level mock client protocol framing lives in `nbd-protocol`.
- Server/catalog test harness helpers live in `nbd-test-support`.
- The server advertises only features it implements.
- Successful reads return exactly the requested number of bytes.
- Out-of-bounds reads/writes fail with an NBD error.
- Successful toy writes are visible to later reads on the same connection.
- Flush returns only after earlier sequential toy writes have completed.
- `NBD_CMD_DISC` closes without a command reply.
- Missing/deleted exports do not enter transmission mode.
- One toy server can serve multiple catalog exports by name.
- Mock integration tests use real TCP framing.
- Tests use temp config and temp SQLite catalogs.

# Alternatives Considered

## Start With Kernel NBD

Kernel NBD would prove more real behavior but makes basic protocol debugging
slower and requires privileged Linux setup. Mock TCP tests should be the inner
loop first.

## Call Server Internals From Tests

Internal tests are useful for small parsing units, but the integration proof
must exercise real TCP framing. Otherwise the riskiest boundary remains
untested.

## Implement Workqueues Immediately

The architecture needs workqueues later, but the toy server does not need them
to prove handshake/read/write/flush. Sequential request handling keeps the
first data-path slice smaller.

# Migration / Rollout

No migration is needed. This extends the initial workspace with protocol and
server crates.

# Validation Strategy

Expected checks:

- `make test`
- `make fmt`
- `make clippy`
- protocol unit tests for endian encoding, constants, request parsing, and
  reply encoding;
- mock-client integration test for read zeroes from a new export;
- mock-client integration test for write/readback/flush/disconnect;
- mock-client integration test for independent export contents;
- mock-client integration test for missing or deleted export failure.

The integration tests should create their own temp catalog and export metadata
through `nbd-control-plane`.

# Risks

- Accidentally implementing a private protocol shape instead of NBD framing.
- Letting the mock client depend on server internals.
- Making toy write success sound durable when it is only in-memory.
- Introducing concurrency before the first protocol path is proven.
- Failing to preserve enough protocol structure for later workqueue/admission
  evolution.

# Open Questions

None.

# Design Exit Criteria

This design is ready for `$review-plan` when:

- the `nbd-protocol` / `nbd-server` split is accepted;
- sequential request handling is accepted for the toy slice;
- the in-memory export semantics are accepted as non-durable;
- mock-client TCP integration is accepted as the primary proof; and
- the mock-client/helper split is accepted.

# Recommended Next Step

Run `$review-plan` before execution planning.
