Title: NBD Protocol Architecture
Date: 2026-05-01
Status: draft

# Problem

The NBD protocol is a public wire protocol. The server should follow the
defined protocol rather than inventing a private request shape around the
assignment. The NBD layer should also remain thin so storage, WAL, cache, and
catalog behavior can evolve behind `Export`.

# Protocol Source

The implementation should follow the upstream NBD protocol document:

```text
https://github.com/NetworkBlockDevice/nbd/blob/master/doc/proto.md
```

Protocol constants, wire structs, and parsing rules should be implemented from
that document and isolated in protocol-specific modules.

# Scope

Required:

- fixed newstyle handshake;
- `NBD_OPT_GO`;
- `NBD_OPT_ABORT`;
- transmission phase request parsing;
- `NBD_CMD_READ`;
- `NBD_CMD_WRITE`;
- `NBD_CMD_FLUSH`;
- `NBD_CMD_DISC`;
- simple replies.

Deferred:

- oldstyle handshake;
- `NBD_OPT_EXPORT_NAME`;
- `NBD_OPT_LIST`;
- `NBD_OPT_INFO` as a standalone management option;
- structured replies;
- TLS negotiation;
- extended headers;
- block status;
- trim;
- write zeroes;
- FUA advertisement.

# Responsibilities

## NBDServer

Owns listener setup, connection acceptance, and global server shutdown.
It should also own a process-local connection registry or task set for active
connections. That registry is used to signal shutdown, stop accepting new
requests, drain or cancel per-connection work according to policy, and join
connection tasks before server shutdown returns.

This connection registry is not durable metadata and is not the
`LocalExportRegistry`. `LocalExportRegistry` tracks active exports and serving
leases; the connection registry tracks socket/task lifecycle.

## NBDConnection

Owns one client connection. It is the transport owner for that socket, not the
export execution owner.

Long term, each connection should have distinct inbound and outbound ownership:
one reader side for protocol input and one writer side for serialized replies.
The exact task names and implementation details are flexible, but the boundary
is not: export work should not write sockets, and socket readers should not
execute storage/WAL work.

- handshake state;
- negotiated export;
- request decode loop;
- request enqueueing;
- reply writing;
- disconnect handling.

## NbdProtocol

Owns wire-level parsing and encoding:

- magic values;
- big-endian integer encoding;
- option request parsing;
- option reply encoding;
- transmission request parsing;
- simple reply encoding;
- protocol error mapping.

It must not call storage, WAL, cache, or catalog code directly.

# Runtime Model

The server should use one process runtime and bounded work queues, not one OS
thread per connection or per export. In Rust, Tokio is the expected async
runtime, but the architectural requirement is the ownership split, not an exact
task layout.

Conceptual long-term shape:

```text
accept/listen
  -> per-connection protocol input
  -> per-export admission/order boundary
  -> export/storage work
  -> per-connection reply serialization
```

Multiple connections to one export share the same export ordering domain but
keep independent reply paths:

```text
connection A ─┐
connection B ─┼─> export X admission/work -> reply queue A/B/C
connection C ─┘
```

The Series 4 toy server may collapse this to one sequential connection task
that reads a request, executes `MemoryExport`, and writes a reply. That is an
implementation shortcut for the first vertical slice, not the long-term socket
architecture.

# Runtime Boundaries

## Socket Runtime

Socket handling owns byte transport and protocol state. It may validate wire
shape, size write payloads, and route requests. It must not own export
correctness, WAL durability, storage object I/O, cache fill, or compaction.

Long-term socket handling should include an explicit active-connection registry
or task set. The accept loop registers each connection before spawning its
runtime work and unregisters it only after inbound handling, reply writing, and
connection cleanup have finished. Global shutdown uses that registry to:

- stop accepting new connections;
- signal active connections to stop accepting new requests;
- drain or cancel outstanding per-connection work according to shutdown policy;
- join connection tasks so shutdown completion is truthful.

The toy server may detach connection tasks for the first vertical slice, but
future socket-runtime design should not preserve that as the production model.

## Export Runtime

The export runtime owns the ordering domain for one active export. Requests
from one or more connections enter through the same export request queue.
`ExportAdmissionCtl` decides which reads, writes, and flushes may run. Work
queues execute admitted work and return results to the original connection's
reply queue.

This is a logical ownership boundary. It does not require one OS thread per
export.

## StorageEngine Runtime

Storage object I/O should be isolated behind a storage runtime or queue with
bounded concurrency. S3-compatible backends should reuse client/config objects
so HTTP connection pools, credentials, retries, and timeouts are shared rather
than recreated per request.

## Reply Queues

Each connection has its own reply serialization path. Export work completes by
returning a reply to the original connection's reply queue. The reply writer
does not know whether a reply was served from memory, WAL, S3, or a future
cache.

NBD cookies let replies be correlated even when requests complete out of order.
Reply order is therefore a scheduling policy, not the correctness mechanism.
Flush correctness is owned by export admission and WAL durability, not by the
connection writer.

# Handshake

The server supports fixed newstyle only.

Initial server message:

```text
INIT_PASSWD
IHAVEOPT
handshake_flags = NBD_FLAG_FIXED_NEWSTYLE | NBD_FLAG_NO_ZEROES
```

The client replies with client flags. The server should reject unknown client
flags except `NBD_FLAG_C_NO_ZEROES` and should require
`NBD_FLAG_C_FIXED_NEWSTYLE` for the supported path. Advertising
`NBD_FLAG_NO_ZEROES` keeps the server handshake consistent with not writing the
old trailing zero block after client flags.

After that, the server accepts option requests.

# Option Handling

## NBD_OPT_GO

`NBD_OPT_GO` is the only path into transmission mode.

Handling:

```text
parse export name and info requests
  -> ExportOpener.open(export_name)
  -> on failure, send fixed-newstyle option error reply
  -> on success, send NBD_REP_INFO for NBD_INFO_EXPORT
  -> send final NBD_REP_ACK
  -> enter transmission phase
```

The export info reply must include:

- export size in bytes;
- transmission flags.

The export name must follow the NBD string constraints:

- UTF-8;
- not NUL-terminated;
- no NUL characters;
- no longer than the protocol maximum.

## NBD_OPT_ABORT

`NBD_OPT_ABORT` requests a soft disconnect during option haggling.

Handling:

```text
send NBD_REP_ACK
close connection cleanly
```

## Unsupported Options

Unsupported options should receive an option error reply such as
`NBD_REP_ERR_UNSUP` rather than causing a hard disconnect, unless the client
violated a mandatory protocol rule.

# Transmission Flags

The first implementation should advertise only flags whose contracts it
actually satisfies.

Advertise:

- `NBD_FLAG_HAS_FLAGS`;
- `NBD_FLAG_SEND_FLUSH`.

Do not advertise initially:

- `NBD_FLAG_SEND_FUA`;
- `NBD_FLAG_CAN_MULTI_CONN`;
- trim/write-zeroes/cache/block-status flags.

`NBD_FLAG_SEND_FUA` should be advertised only after the write path explicitly
handles `NBD_CMD_FLAG_FUA`. `NBD_FLAG_CAN_MULTI_CONN` should be advertised only
after multi-connection cache visibility, write ordering, and flush semantics
are designed and tested.

# Transmission Request Handling

The request decode loop reads:

```text
request_magic
command_flags
command_type
cookie
offset
length
payload, only for NBD_CMD_WRITE
```

The socket read path must only decode, validate enough to size/read the
payload, build an internal request, enqueue it, and return to reading.

Decoded request shape:

```rust
enum NbdCommand {
    Read { range: ByteRange },
    Write { range: ByteRange, data: Bytes },
    Flush,
    Disc,
}

struct NbdRequest {
    cookie: NbdCookie,
    command: NbdCommand,
    flags: NbdCommandFlags,
}
```

# Command Semantics

## NBD_CMD_READ

Handling:

```text
validate offset and length against export size
  -> enqueue request job
  -> Export.read(range)
  -> send simple reply with data on success
  -> send simple error reply on failure before payload starts
```

Without structured replies, a successful read simple reply must include exactly
the requested number of bytes. The implementation should therefore complete the
whole `Export.read` before writing a success reply header.

## NBD_CMD_WRITE

Handling:

```text
validate offset and length against export size
  -> read request payload from socket
  -> enqueue request job
  -> Export.write(range, data)
  -> send simple reply with no payload
```

A successful write reply means the write is durable in WAL and visible to later
reads.

## NBD_CMD_FLUSH

Handling:

```text
enqueue request job
  -> Export.flush()
  -> send simple reply with no payload
```

The flush reply must not be sent until all writes covered by the flush contract
are durable in WAL and visible through the serving view.

## NBD_CMD_DISC

Handling:

```text
stop accepting new requests on this connection
  -> do not send a reply for DISC
  -> close the connection cleanly after shutdown handling
```

The server should allow already-started request work to finish or fail
according to the connection shutdown policy.

# Request Lifecycle

The protocol layer should use explicit request states:

```text
decoded
queued
started
admitted
export_complete
replying
replied
failed
canceled
```

For write requests, `export_complete` means WAL-durable and read-view-visible.

Flush coverage should be defined in terms of the export's internal admission
order:

```text
all writes ordered before the flush by ExportAdmissionCtl must be WAL-durable
and read-view-visible before the flush reply is sent
```

This is stricter than waiting only for writes that already received successful
replies. Since successful write replies already imply WAL durability, this
also satisfies the protocol-visible completed-write contract.

The first implementation can be more conservative by making flush an
export-wide admission barrier. Later implementations may process requests more
concurrently as long as the protocol-visible contract remains true.

# Replies

The first implementation uses simple replies only.

Simple reply contents:

```text
simple_reply_magic
error
cookie
read payload, only for successful reads
```

The reply writer must serialize writes to the socket for one connection. Reply
order may differ from request order once export work can complete concurrently.
Cookies are the correlation key. The first implementation may preserve request
order for simplicity.

# Error Mapping

The protocol layer should map internal errors to NBD errors in one place.

Initial mapping:

```text
out of bounds / invalid flags / malformed request -> NBD_EINVAL
missing or deleted export during NBD_OPT_GO       -> NBD_REP_ERR_UNKNOWN
permission or policy failure                     -> NBD_EPERM / option error
storage or WAL I/O failure                       -> NBD_EIO
server shutdown                                  -> NBD_ESHUTDOWN
unsupported option                               -> NBD_REP_ERR_UNSUP
```

Malformed mandatory protocol input can hard-disconnect when the protocol does
not permit an error reply.

# Bounds And Limits

Every request must be checked for:

- request magic;
- supported command type;
- unsupported command flags;
- offset plus length overflow;
- range past export size;
- write payload length matching the request length;
- configured maximum payload size;
- zero-length behavior.

The first implementation should reject unsupported nonzero command flags unless
the negotiated transmission flags make those command flags meaningful.

# Multi-Connection Policy

The first implementation should not advertise `NBD_FLAG_CAN_MULTI_CONN`.

Long term, multiple connections for one export are allowed only when they
belong to the same authenticated client/host and share the same active export
state. The auth and host differentiation needed for that are out of scope for
the first implementation.

The default is conservative: one active writable NBD connection per export.

Multiple transport connections still use separate per-connection inbound and
outbound ownership. If future authenticated multi-connection support is
enabled, all connections serving the same export must route through the same
export admission/order boundary.

# Invariants

- Protocol parsing and encoding live in protocol-specific modules.
- The NBD layer dispatches to `Export`; it does not know storage internals.
- One connection has one inbound protocol owner and one outbound reply owner.
- A slow connection's reply path must not block writes to other connections.
- All requests for the same export enter the same export ordering domain.
- `NBD_OPT_GO` is the only supported path into transmission mode.
- The server advertises only transmission flags it satisfies.
- A successful read reply contains exactly the requested bytes.
- A successful write reply means WAL-durable and read-view-visible.
- A successful flush reply satisfies the NBD flush ordering contract.
- `NBD_CMD_DISC` closes without sending a command reply.
- Unsupported protocol features fail explicitly when the protocol allows it.

# Open Questions

- Exact maximum payload size for the first implementation.
- Whether to include standalone `NBD_OPT_INFO` before it is required.
- Whether `NBD_OPT_GO` should ignore all info requests except
  `NBD_INFO_EXPORT`, or reject malformed/duplicated requests more strictly.
- Whether the long-term export ordering owner should be called `ExportRuntime`,
  `ExportAdmissionCtl`, or another name in code.
