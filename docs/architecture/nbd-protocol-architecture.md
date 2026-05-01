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

## NBDConnection

Owns one client connection:

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

# Handshake

The server supports fixed newstyle only.

Initial server message:

```text
INIT_PASSWD
IHAVEOPT
handshake_flags = NBD_FLAG_FIXED_NEWSTYLE
```

The client replies with client flags. The server should reject unknown client
flags and should require `NBD_FLAG_C_FIXED_NEWSTYLE` for the supported path.

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
order may differ from request order only if the connection tracks cookies and
the implementation is intentionally allowing concurrent completion. The first
implementation may preserve request order for simplicity.

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

# Invariants

- Protocol parsing and encoding live in protocol-specific modules.
- The NBD layer dispatches to `Export`; it does not know storage internals.
- `NBD_OPT_GO` is the only supported path into transmission mode.
- The server advertises only transmission flags it satisfies.
- A successful read reply contains exactly the requested bytes.
- A successful write reply means WAL-durable and read-view-visible.
- A successful flush reply satisfies the NBD flush ordering contract.
- `NBD_CMD_DISC` closes without sending a command reply.
- Unsupported protocol features fail explicitly when the protocol allows it.

# Open Questions

- Exact maximum payload size for the first implementation.
- Whether first implementation preserves reply order or allows out-of-order
  replies by cookie.
- Whether to support `NBD_FLAG_C_NO_ZEROES` in the initial handshake.
- Whether to include standalone `NBD_OPT_INFO` before it is required.
- Whether `NBD_OPT_GO` should ignore all info requests except
  `NBD_INFO_EXPORT`, or reject malformed/duplicated requests more strictly.
