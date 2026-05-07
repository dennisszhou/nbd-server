use super::shutdown::ConnectionShutdown;
use crate::export::ExportCompletionSink;
use crate::observability::{self, event, target};
use crate::{
    CompletedExport, ExportCompletion, ExportJobContext, ExportQueueSlot, ExportReply,
    ExportResult, RequestCookie, Result, ServerError,
};
use nbd_protocol::constants::NBD_EINVAL;
use nbd_protocol::transmission::{encode_read_reply, encode_simple_reply};
use nbd_protocol::wire::NbdCookie;
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio::sync::mpsc;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ReplyKind {
    Read,
    Simple,
}

#[derive(Debug)]
pub(super) struct ConnectionReply {
    cookie: NbdCookie,
    payload: ConnectionReplyPayload,
}

#[derive(Debug)]
enum ConnectionReplyPayload {
    Export {
        context: ExportJobContext,
        kind: ReplyKind,
        result: ExportResult,
        _queue_slot: ExportQueueSlot,
    },
    SimpleError {
        error: u32,
    },
}

#[derive(Debug)]
struct ConnectionExportCompletion {
    context: ExportJobContext,
    kind: ReplyKind,
    replies: mpsc::Sender<ConnectionReply>,
}

impl ConnectionReply {
    pub(super) fn export_result(
        context: ExportJobContext,
        kind: ReplyKind,
        result: ExportResult,
        queue_slot: ExportQueueSlot,
    ) -> Self {
        let cookie = nbd_cookie(context.cookie());
        Self {
            cookie,
            payload: ConnectionReplyPayload::Export {
                context,
                kind,
                result,
                _queue_slot: queue_slot,
            },
        }
    }

    fn simple_error(cookie: NbdCookie, error: u32) -> Self {
        Self {
            cookie,
            payload: ConnectionReplyPayload::SimpleError { error },
        }
    }
}

impl ConnectionExportCompletion {
    fn new(
        context: ExportJobContext,
        kind: ReplyKind,
        replies: mpsc::Sender<ConnectionReply>,
    ) -> Self {
        Self {
            context,
            kind,
            replies,
        }
    }
}

#[async_trait::async_trait]
impl ExportCompletionSink for ConnectionExportCompletion {
    async fn complete(self: Box<Self>, completed: CompletedExport) {
        let (result, queue_slot) = completed.into_parts();
        let reply = ConnectionReply::export_result(self.context, self.kind, result, queue_slot);
        let _ = self.replies.send(reply).await;
    }
}

impl ReplyKind {
    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::Read => "read",
            Self::Simple => "simple",
        }
    }
}

pub(super) fn export_completion(
    context: ExportJobContext,
    kind: ReplyKind,
    replies: mpsc::Sender<ConnectionReply>,
) -> ExportCompletion {
    ExportCompletion::sink(ConnectionExportCompletion::new(context, kind, replies))
}

pub(super) async fn write_replies<W>(
    mut writer: W,
    mut replies: mpsc::Receiver<ConnectionReply>,
    mut shutdown: ConnectionShutdown,
) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    loop {
        let reply = tokio::select! {
            reply = replies.recv() => reply,
            () = shutdown.cancelled() => return Ok(()),
        };
        let Some(reply) = reply else {
            return Ok(());
        };
        if !write_connection_reply_or_shutdown(&mut writer, reply, &mut shutdown).await? {
            return Ok(());
        }
    }
}

pub(super) async fn send_error_then_return(
    replies: &mpsc::Sender<ConnectionReply>,
    cookie: NbdCookie,
    error: ServerError,
) -> Result<()> {
    send_simple_error(replies, cookie).await?;
    Err(error)
}

pub(super) async fn send_simple_error(
    replies: &mpsc::Sender<ConnectionReply>,
    cookie: NbdCookie,
) -> Result<()> {
    replies
        .send(ConnectionReply::simple_error(cookie, NBD_EINVAL))
        .await
        .map_err(|_| ServerError::RuntimeClosed {
            resource: "connection reply queue",
        })
}

async fn write_connection_reply_or_shutdown<W>(
    writer: &mut W,
    reply: ConnectionReply,
    shutdown: &mut ConnectionShutdown,
) -> Result<bool>
where
    W: AsyncWrite + Unpin,
{
    if shutdown.is_cancelled() {
        return Ok(false);
    }

    tokio::select! {
        result = write_connection_reply(writer, reply) => result.map(|()| true),
        () = shutdown.cancelled() => Ok(false),
    }
}

pub(super) async fn write_connection_reply<W>(writer: &mut W, reply: ConnectionReply) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    match reply.payload {
        ConnectionReplyPayload::Export {
            context,
            kind,
            result,
            _queue_slot: queue_slot,
        } => {
            let write_result = match (kind, result) {
                (ReplyKind::Read, Ok(ExportReply::Read { data })) => writer
                    .write_all(&encode_read_reply(reply.cookie, &data))
                    .await
                    .map_err(|source| ServerError::io("write NBD read reply", source))
                    .map(|()| "ok"),
                (ReplyKind::Simple, Ok(ExportReply::Done)) => writer
                    .write_all(&encode_simple_reply(reply.cookie, 0))
                    .await
                    .map_err(|source| ServerError::io("write NBD simple reply", source))
                    .map(|()| "ok"),
                _ => writer
                    .write_all(&encode_simple_reply(reply.cookie, NBD_EINVAL))
                    .await
                    .map_err(|source| ServerError::io("write NBD error reply", source))
                    .map(|()| "error"),
            };
            match &write_result {
                Ok(status) => {
                    tracing::trace!(
                        target: target::REQUEST,
                        event = event::REQUEST_REPLY_WRITTEN,
                        service = observability::SERVICE_NAME,
                        server_instance_id = observability::server_instance_id(),
                        pid = observability::pid(),
                        connection_id = context.connection_id().raw(),
                        request_sequence = context.request_sequence().raw(),
                        cookie = context.cookie().raw(),
                        command = context.command(),
                        offset = ?context.offset(),
                        length = ?context.length(),
                        reply_kind = context.reply_kind(),
                        status = *status,
                        duration_ms = observability::duration_ms(context.elapsed()),
                    );
                    tracing::debug!(
                        target: target::REQUEST,
                        event = event::REQUEST_COMPLETED,
                        service = observability::SERVICE_NAME,
                        server_instance_id = observability::server_instance_id(),
                        pid = observability::pid(),
                        connection_id = context.connection_id().raw(),
                        request_sequence = context.request_sequence().raw(),
                        cookie = context.cookie().raw(),
                        command = context.command(),
                        offset = ?context.offset(),
                        length = ?context.length(),
                        reply_kind = context.reply_kind(),
                        status = *status,
                        duration_ms = observability::duration_ms(context.elapsed()),
                    );
                }
                Err(error) => {
                    tracing::warn!(
                        target: target::REQUEST,
                        event = event::REQUEST_FAILED,
                        service = observability::SERVICE_NAME,
                        server_instance_id = observability::server_instance_id(),
                        pid = observability::pid(),
                        connection_id = context.connection_id().raw(),
                        request_sequence = context.request_sequence().raw(),
                        cookie = context.cookie().raw(),
                        command = context.command(),
                        offset = ?context.offset(),
                        length = ?context.length(),
                        reply_kind = context.reply_kind(),
                        phase = "reply_write",
                        duration_ms = observability::duration_ms(context.elapsed()),
                        error = %error,
                    );
                }
            }
            drop(queue_slot);
            write_result.map(|_| ())
        }
        ConnectionReplyPayload::SimpleError { error } => writer
            .write_all(&encode_simple_reply(reply.cookie, error))
            .await
            .map_err(|source| ServerError::io("write NBD error reply", source)),
    }
}

fn nbd_cookie(cookie: RequestCookie) -> NbdCookie {
    NbdCookie::new(cookie.raw())
}
