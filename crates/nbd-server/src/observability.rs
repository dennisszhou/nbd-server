use nbd_control_plane::ExportRecord;
use nbd_protocol::wire::NbdCookie;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;
use std::time::{Duration, Instant};
use uuid::Uuid;

pub const SERVICE_NAME: &str = "nbd-server";

pub mod target {
    pub const OPS: &str = "nbd_server::ops";
    pub const CONNECTION: &str = "nbd_server::connection";
    pub const EXPORT: &str = "nbd_server::export";
    pub const REQUEST: &str = "nbd_server::request";
    pub const RUNTIME: &str = "nbd_server::runtime";
    pub const ADMISSION: &str = "nbd_server::admission";
    pub const ENGINE: &str = "nbd_server::engine";
    pub const STORAGE: &str = "nbd_server::storage";
    pub const WAL: &str = "nbd_server::wal";
    pub const CATALOG: &str = "nbd_server::catalog";
}

pub mod event {
    pub const LOGGING_INITIALIZED: &str = "logging.initialized";
    pub const SERVER_STARTING: &str = "server.starting";
    pub const SERVER_LISTENING: &str = "server.listening";
    pub const SERVER_SHUTDOWN_STARTED: &str = "server.shutdown.started";
    pub const SERVER_SHUTDOWN_COMPLETED: &str = "server.shutdown.completed";
    pub const SERVER_ERROR: &str = "server.error";

    pub const CONNECTION_ACCEPTED: &str = "connection.accepted";
    pub const CONNECTION_HANDSHAKE_COMPLETED: &str = "connection.handshake.completed";
    pub const CONNECTION_NEGOTIATION_STARTED: &str = "connection.negotiation.started";
    pub const CONNECTION_NEGOTIATION_COMPLETED: &str = "connection.negotiation.completed";
    pub const CONNECTION_DISCONNECT_RECEIVED: &str = "connection.disconnect.received";
    pub const CONNECTION_CLOSED: &str = "connection.closed";
    pub const CONNECTION_ERROR: &str = "connection.error";

    pub const EXPORT_OPEN_STARTED: &str = "export.open.started";
    pub const EXPORT_OPEN_COMPLETED: &str = "export.open.completed";
    pub const EXPORT_OPEN_REJECTED: &str = "export.open.rejected";
    pub const EXPORT_CLOSE_STARTED: &str = "export.close.started";
    pub const EXPORT_CLOSE_COMPLETED: &str = "export.close.completed";
    pub const EXPORT_RUNTIME_SELECTED: &str = "export.runtime.selected";
    pub const EXPORT_ENGINE_LOADED: &str = "export.engine.loaded";

    pub const REQUEST_DECODED: &str = "request.decoded";
    pub const REQUEST_SUBMITTED: &str = "request.submitted";
    pub const REQUEST_COMPLETED: &str = "request.completed";
    pub const REQUEST_FAILED: &str = "request.failed";
    pub const REQUEST_REPLY_WRITTEN: &str = "request.reply_written";

    pub const QUEUE_RESERVE_WAIT: &str = "queue.reserve.wait";
    pub const QUEUE_RESERVE_ACQUIRED: &str = "queue.reserve.acquired";
    pub const RUNTIME_SUBMIT: &str = "runtime.submit";
    pub const RUNTIME_TASK_STARTED: &str = "runtime.task.started";
    pub const RUNTIME_TASK_COMPLETED: &str = "runtime.task.completed";
    pub const RUNTIME_CLOSED: &str = "runtime.closed";

    pub const ADMISSION_REGISTERED: &str = "admission.registered";
    pub const ADMISSION_REJECTED: &str = "admission.rejected";
    pub const ADMISSION_GRANTED: &str = "admission.granted";
    pub const ADMISSION_CANCELLED: &str = "admission.cancelled";
    pub const ADMISSION_RELEASED: &str = "admission.released";

    pub const ENGINE_EXECUTE_STARTED: &str = "engine.execute.started";
    pub const ENGINE_EXECUTE_COMPLETED: &str = "engine.execute.completed";
    pub const ENGINE_EXECUTE_FAILED: &str = "engine.execute.failed";
    pub const ENGINE_FLUSH_COMPLETED: &str = "engine.flush.completed";

    pub const BLOB_READ: &str = "blob.read";
    pub const BLOB_CREATE: &str = "blob.create";
    pub const BLOB_REPLACE: &str = "blob.replace";
    pub const BLOB_DIRECTORY_SYNCED: &str = "blob.directory_synced";
    pub const BLOB_ERROR: &str = "blob.error";

    pub const CATALOG_CONNECT_STARTED: &str = "catalog.connect.started";
    pub const CATALOG_CONNECT_COMPLETED: &str = "catalog.connect.completed";
    pub const CATALOG_EXPORT_LOADED: &str = "catalog.export.loaded";
    pub const CATALOG_TREE_LOADED: &str = "catalog.tree.loaded";
    pub const CATALOG_TREE_COMMIT_STARTED: &str = "catalog.tree.commit.started";
    pub const CATALOG_TREE_COMMIT_COMPLETED: &str = "catalog.tree.commit.completed";
    pub const CATALOG_ERROR: &str = "catalog.error";

    pub const WAL_ROOT_LOADED: &str = "wal.root.loaded";
    pub const WAL_REPLAY_COMPLETED: &str = "wal.replay.completed";
    pub const WAL_COMPACTION_COMPLETED: &str = "wal.compaction.completed";
    pub const WAL_COMPACTION_ENQUEUED: &str = "wal.compaction.enqueued";
    pub const WAL_COMPACTION_ENQUEUE_FAILED: &str = "wal.compaction.enqueue_failed";
    pub const WAL_COMPACTION_FAILED: &str = "wal.compaction.failed";
}

// `tracing` event levels are callsite-static, so request failure severity has
// to branch around the event macro instead of passing a runtime level.
macro_rules! request_failure_event {
    (target: $target:expr, error: $error:expr, $($fields:tt)*) => {
        match $error.request_failure_log_level() {
            $crate::error::RequestFailureLogLevel::Debug => tracing::debug!(
                target: $target,
                $($fields)*
                error = %$error,
            ),
            $crate::error::RequestFailureLogLevel::Warn => tracing::warn!(
                target: $target,
                $($fields)*
                error = %$error,
            ),
        }
    };
}

pub(crate) use request_failure_event;

static SERVER_INSTANCE_ID: OnceLock<String> = OnceLock::new();
static NEXT_CONNECTION_ID: AtomicU64 = AtomicU64::new(1);

pub fn server_instance_id() -> &'static str {
    SERVER_INSTANCE_ID
        .get_or_init(|| Uuid::new_v4().as_simple().to_string())
        .as_str()
}

pub fn pid() -> u32 {
    std::process::id()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ConnectionId(u64);

impl ConnectionId {
    pub(crate) fn next() -> Self {
        Self(NEXT_CONNECTION_ID.fetch_add(1, Ordering::Relaxed))
    }

    pub(crate) fn internal() -> Self {
        Self(0)
    }

    pub fn raw(self) -> u64 {
        self.0
    }
}

impl fmt::Display for ConnectionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RequestSequence(u64);

impl RequestSequence {
    pub(crate) fn internal() -> Self {
        Self(0)
    }

    pub fn raw(self) -> u64 {
        self.0
    }
}

impl fmt::Display for RequestSequence {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

#[derive(Debug)]
pub(crate) struct RequestSequenceGenerator {
    next: u64,
}

impl RequestSequenceGenerator {
    pub(crate) fn new() -> Self {
        Self { next: 1 }
    }

    pub(crate) fn next(&mut self) -> RequestSequence {
        let sequence = RequestSequence(self.next);
        self.next += 1;
        sequence
    }
}

#[derive(Debug, Clone)]
pub struct ExportJobContext {
    connection_id: ConnectionId,
    request_sequence: RequestSequence,
    cookie: NbdCookie,
    command: &'static str,
    offset: Option<u64>,
    length: Option<u64>,
    reply_kind: &'static str,
    started_at: Instant,
}

impl ExportJobContext {
    pub(crate) fn new(
        connection_id: ConnectionId,
        request_sequence: RequestSequence,
        cookie: NbdCookie,
        command: &'static str,
        offset: Option<u64>,
        length: Option<u64>,
        reply_kind: &'static str,
    ) -> Self {
        Self {
            connection_id,
            request_sequence,
            cookie,
            command,
            offset,
            length,
            reply_kind,
            started_at: Instant::now(),
        }
    }

    pub(crate) fn internal(cookie: NbdCookie, command: &'static str) -> Self {
        Self::new(
            ConnectionId::internal(),
            RequestSequence::internal(),
            cookie,
            command,
            None,
            None,
            "internal",
        )
    }

    pub fn connection_id(&self) -> ConnectionId {
        self.connection_id
    }

    pub fn request_sequence(&self) -> RequestSequence {
        self.request_sequence
    }

    pub fn cookie(&self) -> NbdCookie {
        self.cookie
    }

    pub fn command(&self) -> &'static str {
        self.command
    }

    pub fn offset(&self) -> Option<u64> {
        self.offset
    }

    pub fn length(&self) -> Option<u64> {
        self.length
    }

    pub fn reply_kind(&self) -> &'static str {
        self.reply_kind
    }

    pub fn elapsed(&self) -> Duration {
        self.started_at.elapsed()
    }
}

pub fn duration_ms(duration: Duration) -> u128 {
    duration.as_millis()
}

pub(crate) fn request_span(
    meta: &ExportRecord,
    runtime_kind: &'static str,
    context: &ExportJobContext,
) -> tracing::Span {
    tracing::debug_span!(
        target: target::REQUEST,
        "request",
        service = SERVICE_NAME,
        server_instance_id = server_instance_id(),
        pid = pid(),
        connection_id = context.connection_id().raw(),
        request_sequence = context.request_sequence().raw(),
        cookie = context.cookie().raw(),
        command = context.command(),
        offset = ?context.offset(),
        length = ?context.length(),
        reply_kind = context.reply_kind(),
        export_id = %meta.id(),
        export_name = %meta.name(),
        engine_kind = %meta.engine_kind(),
        runtime_kind = runtime_kind,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_sequence_generator_starts_at_one() {
        let mut generator = RequestSequenceGenerator::new();

        assert_eq!(generator.next().raw(), 1);
        assert_eq!(generator.next().raw(), 2);
    }

    #[test]
    fn server_instance_id_is_process_stable() {
        let first = server_instance_id();
        let second = server_instance_id();

        assert_eq!(first, second);
        assert_eq!(first.len(), 32);
    }
}
