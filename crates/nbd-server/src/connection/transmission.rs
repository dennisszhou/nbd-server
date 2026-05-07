use super::{
    replies::{
        ConnectionReply, ReplyKind, export_completion, send_error_then_return, send_simple_error,
    },
    shutdown::ConnectionShutdown,
};
use crate::export::RequestSequenceGenerator;
use crate::observability::{self, event, target};
use crate::{
    ConnectionId, ExportJob, ExportJobContext, ExportRequest, ExportRuntimeHandle, RequestCookie,
    Result, ServerError,
};
use nbd_protocol::constants::NBD_CMD_DISC;
use nbd_protocol::transmission::{
    MAX_IO_BYTES, REQUEST_HEADER_BYTES, RequestHeader, TransmissionRequest, parse_request,
    parse_request_header,
};
use nbd_protocol::wire::NbdCookie;
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::sync::mpsc;

pub(super) struct RequestReaderExit {
    pub(super) result: Result<()>,
    pub(super) reply_drain: ConnectionReplyDrain,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ConnectionReplyDrain {
    DropPending,
    DrainQueued,
}

pub(super) async fn read_requests<R>(
    mut reader: R,
    connection_id: ConnectionId,
    runtime: ExportRuntimeHandle,
    replies: mpsc::Sender<ConnectionReply>,
    mut shutdown: ConnectionShutdown,
) -> RequestReaderExit
where
    R: AsyncRead + Unpin,
{
    let mut request_sequences = RequestSequenceGenerator::new();
    loop {
        let mut bytes = vec![0; REQUEST_HEADER_BYTES];
        let read_header = tokio::select! {
            result = reader.read_exact(&mut bytes) => result,
            () = shutdown.cancelled() => return RequestReaderExit::close(Ok(())),
        };
        match read_header {
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => {
                return RequestReaderExit::close(Ok(()));
            }
            Err(source) => {
                return RequestReaderExit::close(Err(ServerError::io(
                    "read NBD transmission header",
                    source,
                )));
            }
        }

        let header = match parse_request_header(&bytes) {
            Ok(header) => header,
            Err(error) => {
                let error = ServerError::from(error);
                trace_decode_request_failed(connection_id, &error);
                return RequestReaderExit::close(Err(error));
            }
        };
        let payload_len = match header.payload_len(MAX_IO_BYTES) {
            Ok(payload_len) => payload_len,
            Err(error) => {
                let error = ServerError::from(error);
                trace_header_request_failed(connection_id, &header, "decode", &error);
                return RequestReaderExit::drain(
                    send_error_then_return(&replies, header.cookie, error).await,
                );
            }
        };

        if header.command.raw() == NBD_CMD_DISC {
            tracing::info!(
                target: target::CONNECTION,
                event = event::CONNECTION_DISCONNECT_RECEIVED,
                service = observability::SERVICE_NAME,
                server_instance_id = observability::server_instance_id(),
                pid = observability::pid(),
                connection_id = connection_id.raw(),
                cookie = header.cookie.raw(),
            );
            return RequestReaderExit::close(Ok(()));
        }

        tracing::trace!(
            target: target::RUNTIME,
            event = event::QUEUE_RESERVE_WAIT,
            service = observability::SERVICE_NAME,
            server_instance_id = observability::server_instance_id(),
            pid = observability::pid(),
            connection_id = connection_id.raw(),
            cookie = header.cookie.raw(),
            command_raw = header.command.raw(),
            offset = header.offset,
            length = header.length,
        );
        let reserve_result = tokio::select! {
            result = runtime.reserve() => result,
            () = shutdown.cancelled() => return RequestReaderExit::close(Ok(())),
        };
        let queue_slot = match reserve_result {
            Ok(queue_slot) => {
                tracing::trace!(
                    target: target::RUNTIME,
                    event = event::QUEUE_RESERVE_ACQUIRED,
                    service = observability::SERVICE_NAME,
                    server_instance_id = observability::server_instance_id(),
                    pid = observability::pid(),
                    connection_id = connection_id.raw(),
                    cookie = header.cookie.raw(),
                    command_raw = header.command.raw(),
                    offset = header.offset,
                    length = header.length,
                );
                queue_slot
            }
            Err(error) => {
                trace_header_request_failed(connection_id, &header, "queue_reserve", &error);
                return RequestReaderExit::drain(
                    send_simple_error(&replies, header.cookie)
                        .await
                        .and(Err(error)),
                );
            }
        };

        let mut payload = vec![0; payload_len];
        let read_payload = tokio::select! {
            result = reader.read_exact(&mut payload) => result,
            () = shutdown.cancelled() => {
                drop(queue_slot);
                return RequestReaderExit::close(Ok(()));
            }
        };
        if let Err(source) = read_payload {
            drop(queue_slot);
            return RequestReaderExit::close(Err(ServerError::io(
                "read NBD transmission payload",
                source,
            )));
        }
        bytes.extend_from_slice(&payload);

        let request = match parse_request(&bytes, MAX_IO_BYTES) {
            Ok(request) => request,
            Err(error) => {
                let error = ServerError::from(error);
                trace_header_request_failed(connection_id, &header, "decode", &error);
                drop(queue_slot);
                return RequestReaderExit::drain(
                    send_error_then_return(&replies, header.cookie, error).await,
                );
            }
        };

        let (cookie, kind, request, offset, length) = match request {
            TransmissionRequest::Read {
                cookie,
                offset,
                length,
            } => (
                cookie,
                ReplyKind::Read,
                ExportRequest::Read {
                    offset,
                    len: length,
                },
                Some(offset),
                Some(u64::from(length)),
            ),
            TransmissionRequest::Write {
                cookie,
                offset,
                data,
            } => (
                cookie,
                ReplyKind::Simple,
                ExportRequest::Write { offset, data },
                Some(offset),
                Some(payload_len as u64),
            ),
            TransmissionRequest::Flush { cookie } => {
                (cookie, ReplyKind::Simple, ExportRequest::Flush, None, None)
            }
            TransmissionRequest::Disconnect { .. } => {
                drop(queue_slot);
                return RequestReaderExit::close(Ok(()));
            }
        };

        let context = ExportJobContext::new(
            connection_id,
            request_sequences.next(),
            request_cookie(cookie),
            request.command_name(),
            offset,
            length,
            kind.as_str(),
        );
        tracing::trace!(
            target: target::REQUEST,
            event = event::REQUEST_DECODED,
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
        );
        let completion = export_completion(context.clone(), kind, replies.clone());
        let job = ExportJob::with_context(context, request, completion, queue_slot);
        tracing::trace!(
            target: target::REQUEST,
            event = event::REQUEST_SUBMITTED,
            service = observability::SERVICE_NAME,
            server_instance_id = observability::server_instance_id(),
            pid = observability::pid(),
            connection_id = job.context().connection_id().raw(),
            request_sequence = job.context().request_sequence().raw(),
            cookie = job.context().cookie().raw(),
            command = job.context().command(),
            offset = ?job.context().offset(),
            length = ?job.context().length(),
        );
        let submit_context = job.context().clone();
        if let Err(error) = runtime.submit(job).await {
            trace_context_request_failed(&submit_context, "runtime_submit", &error);
            return RequestReaderExit::drain(
                send_simple_error(&replies, cookie).await.and(Err(error)),
            );
        }
    }
}

impl RequestReaderExit {
    fn close(result: Result<()>) -> Self {
        Self {
            result,
            reply_drain: ConnectionReplyDrain::DropPending,
        }
    }

    fn drain(result: Result<()>) -> Self {
        Self {
            result,
            reply_drain: ConnectionReplyDrain::DrainQueued,
        }
    }
}

fn request_cookie(cookie: NbdCookie) -> RequestCookie {
    RequestCookie::new(cookie.raw())
}

fn trace_header_request_failed(
    connection_id: ConnectionId,
    header: &RequestHeader,
    phase: &'static str,
    error: &ServerError,
) {
    observability::request_failure_event!(
        target: target::REQUEST,
        error: error,
        event = event::REQUEST_FAILED,
        service = observability::SERVICE_NAME,
        server_instance_id = observability::server_instance_id(),
        pid = observability::pid(),
        connection_id = connection_id.raw(),
        cookie = header.cookie.raw(),
        command_raw = header.command.raw(),
        offset = header.offset,
        length = header.length,
        phase = phase,
    );
}

fn trace_decode_request_failed(connection_id: ConnectionId, error: &ServerError) {
    observability::request_failure_event!(
        target: target::REQUEST,
        error: error,
        event = event::REQUEST_FAILED,
        service = observability::SERVICE_NAME,
        server_instance_id = observability::server_instance_id(),
        pid = observability::pid(),
        connection_id = connection_id.raw(),
        phase = "decode",
    );
}

fn trace_context_request_failed(
    context: &ExportJobContext,
    phase: &'static str,
    error: &ServerError,
) {
    observability::request_failure_event!(
        target: target::REQUEST,
        error: error,
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
        phase = phase,
        duration_ms = observability::duration_ms(context.elapsed()),
    );
}
