use crate::export::{ExportCompletionSink, RequestSequenceGenerator};
use crate::observability::{self, event, target};
use crate::{
    CompletedExport, ConnectionId, ExportCompletion, ExportJob, ExportJobContext, ExportQueueSlot,
    ExportReply, ExportRequest, ExportResult, ExportRuntimeHandle, LocalExportRegistry,
    RequestCookie, Result, ServerError,
};
use nbd_protocol::constants::{NBD_CMD_DISC, NBD_EINVAL};
use nbd_protocol::transmission::{
    MAX_IO_BYTES, REQUEST_HEADER_BYTES, RequestHeader, TransmissionRequest, encode_read_reply,
    encode_simple_reply, parse_request, parse_request_header,
};
use nbd_protocol::wire::NbdCookie;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

mod handshake;
mod io;
mod options;
mod shutdown;

use handshake::write_handshake;
use io::write_connection_reply_or_shutdown;
use options::negotiate_options;
use shutdown::ConnectionShutdown;
pub(crate) use shutdown::ServerConnectionShutdown;

struct ConnectionRuntime {
    connection_id: ConnectionId,
    runtime: ExportRuntimeHandle,
    reply_capacity: usize,
}

struct RequestReaderExit {
    result: Result<()>,
    reply_drain: ConnectionReplyDrain,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ConnectionReplyDrain {
    DropPending,
    DrainQueued,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReplyKind {
    Read,
    Simple,
}

#[derive(Debug)]
pub(crate) struct ConnectionReply {
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
    pub(crate) fn export_result(
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

impl ConnectionRuntime {
    fn new(
        connection_id: ConnectionId,
        runtime: ExportRuntimeHandle,
        reply_capacity: usize,
    ) -> Self {
        Self {
            connection_id,
            runtime,
            reply_capacity,
        }
    }

    async fn run_with_shutdown(
        self,
        stream: TcpStream,
        shutdown: ConnectionShutdown,
    ) -> Result<()> {
        let (reader, writer) = stream.into_split();
        self.run_io(reader, writer, shutdown).await
    }

    async fn run_io<R, W>(self, reader: R, writer: W, shutdown: ConnectionShutdown) -> Result<()>
    where
        R: AsyncRead + Unpin + Send + 'static,
        W: AsyncWrite + Unpin + Send + 'static,
    {
        let (reply_sender, reply_receiver) = mpsc::channel(self.reply_capacity);
        let writer_shutdown = shutdown.clone();
        let reader_task = tokio::spawn(read_requests(
            reader,
            self.connection_id,
            self.runtime,
            reply_sender,
            shutdown,
        ));
        let writer_task = tokio::spawn(write_replies(writer, reply_receiver, writer_shutdown));

        run_connection_tasks(reader_task, writer_task).await
    }
}

pub(crate) async fn serve_with_shutdown(
    mut stream: TcpStream,
    registry: Arc<LocalExportRegistry>,
    reply_capacity: usize,
    connection_id: ConnectionId,
    peer_addr: SocketAddr,
    mut shutdown: ConnectionShutdown,
) -> Result<()> {
    if !write_handshake(&mut stream, &mut shutdown).await? {
        return Ok(());
    }
    tracing::debug!(
        target: target::CONNECTION,
        event = event::CONNECTION_HANDSHAKE_COMPLETED,
        service = observability::SERVICE_NAME,
        server_instance_id = observability::server_instance_id(),
        pid = observability::pid(),
        connection_id = connection_id.raw(),
        peer_addr = %peer_addr,
    );
    let Some(export) = negotiate_options(
        &mut stream,
        registry.clone(),
        connection_id,
        peer_addr,
        &mut shutdown,
    )
    .await?
    else {
        tracing::info!(
            target: target::CONNECTION,
            event = event::CONNECTION_CLOSED,
            service = observability::SERVICE_NAME,
            server_instance_id = observability::server_instance_id(),
            pid = observability::pid(),
            connection_id = connection_id.raw(),
            peer_addr = %peer_addr,
            status = "no_export",
        );
        return Ok(());
    };
    let result = ConnectionRuntime::new(connection_id, export.runtime.clone(), reply_capacity)
        .run_with_shutdown(stream, shutdown)
        .await;
    let close_result = registry.close(&export.name, &export.owner).await;

    match (result, close_result) {
        (Err(error), _) => Err(error),
        (Ok(()), Err(error)) => Err(error),
        (Ok(()), Ok(())) => {
            tracing::info!(
                target: target::CONNECTION,
                event = event::CONNECTION_CLOSED,
                service = observability::SERVICE_NAME,
                server_instance_id = observability::server_instance_id(),
                pid = observability::pid(),
                connection_id = connection_id.raw(),
                peer_addr = %peer_addr,
                export_name = %export.name,
                owner_id = export.owner.id().raw(),
                status = "ok",
            );
            Ok(())
        }
    }
}

async fn read_requests<R>(
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
        let completion = ExportCompletion::sink(ConnectionExportCompletion::new(
            context.clone(),
            kind,
            replies.clone(),
        ));
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

async fn write_replies<W>(
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

async fn run_connection_tasks(
    mut reader_task: JoinHandle<RequestReaderExit>,
    mut writer_task: JoinHandle<Result<()>>,
) -> Result<()> {
    tokio::select! {
        biased;

        reader_result = &mut reader_task => {
            let reader_exit = match reader_result {
                Ok(exit) => exit,
                Err(_) => {
                    writer_task.abort();
                    let _ = writer_task.await;
                    return Err(ServerError::RuntimeClosed {
                        resource: "connection request reader",
                    });
                }
            };

            if reader_exit.reply_drain == ConnectionReplyDrain::DrainQueued {
                match writer_task.await {
                    Ok(Ok(())) => reader_exit.result,
                    Ok(Err(error)) => Err(error),
                    Err(_) => Err(ServerError::RuntimeClosed {
                        resource: "connection reply writer",
                    }),
                }
            } else {
                writer_task.abort();
                let _ = writer_task.await;
                reader_exit.result
            }
        }
        writer_result = &mut writer_task => {
            reader_task.abort();
            let _ = reader_task.await;
            match writer_result {
                Ok(result) => result,
                Err(_) => Err(ServerError::RuntimeClosed {
                    resource: "connection reply writer",
                }),
            }
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

fn nbd_cookie(cookie: RequestCookie) -> NbdCookie {
    NbdCookie::new(cookie.raw())
}

async fn send_error_then_return(
    replies: &mpsc::Sender<ConnectionReply>,
    cookie: NbdCookie,
    error: ServerError,
) -> Result<()> {
    send_simple_error(replies, cookie).await?;
    Err(error)
}

async fn send_simple_error(
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

pub(crate) async fn write_connection_reply<W>(writer: &mut W, reply: ConnectionReply) -> Result<()>
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

impl ReplyKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Read => "read",
            Self::Simple => "simple",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        AdmittedExportRequest, ExportAdmissionPolicyHandle, ExportEngine, ExportRuntime,
        MemoryAdmissionPolicy, SerialExportRuntime,
    };
    use nbd_control_plane::{
        ExportEngineKind, ExportHead, ExportId, ExportName, ExportRecord, ExportState, Timestamp,
    };
    use nbd_protocol::constants::NBD_CMD_WRITE;
    use nbd_protocol::transmission::{
        RequestHeader, SIMPLE_REPLY_BYTES, encode_disconnect_request, encode_read_request,
        encode_request_header, parse_simple_reply,
    };
    use nbd_protocol::wire::{NbdCommandFlags, NbdCommandType};
    use std::sync::Arc;
    use tokio::io::{DuplexStream, duplex, split};
    use tokio::sync::{
        mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel},
        watch,
    };

    #[tokio::test]
    async fn connection_runtime_writes_out_of_order_completions_by_cookie() {
        let (runtime, mut submitted, _reserve_started, _reserve_acquired) = controllable_runtime(2);
        let (mut client, server_task) = spawn_connection(runtime, 2);
        let first_cookie = NbdCookie::new(101);
        let second_cookie = NbdCookie::new(102);

        client
            .write_all(&encode_read_request(first_cookie, 0, 4).expect("first read"))
            .await
            .expect("send first read");
        client
            .write_all(&encode_read_request(second_cookie, 4, 4).expect("second read"))
            .await
            .expect("send second read");

        let first_job = submitted.recv().await.expect("first job");
        let second_job = submitted.recv().await.expect("second job");
        assert_eq!(first_job.context().cookie(), request_cookie(first_cookie));
        assert_eq!(first_job.context().request_sequence().raw(), 1);
        assert_eq!(first_job.context().offset(), Some(0));
        assert_eq!(first_job.context().length(), Some(4));
        assert_eq!(second_job.context().cookie(), request_cookie(second_cookie));
        assert_eq!(second_job.context().request_sequence().raw(), 2);
        assert_eq!(second_job.context().offset(), Some(4));
        assert_eq!(second_job.context().length(), Some(4));

        complete_job(
            second_job,
            ExportRequest::Read { offset: 4, len: 4 },
            Ok(ExportReply::Read {
                data: b"bbbb".to_vec(),
            }),
        )
        .await;
        assert_eq!(
            read_successful_read(&mut client, 4).await,
            (second_cookie, b"bbbb".to_vec()),
        );

        complete_job(
            first_job,
            ExportRequest::Read { offset: 0, len: 4 },
            Ok(ExportReply::Read {
                data: b"aaaa".to_vec(),
            }),
        )
        .await;
        assert_eq!(
            read_successful_read(&mut client, 4).await,
            (first_cookie, b"aaaa".to_vec()),
        );

        disconnect_and_join(client, server_task).await;
    }

    #[tokio::test]
    async fn connection_runtime_backpressures_before_write_payload() {
        let (runtime, mut submitted, mut reserve_started, mut reserve_acquired) =
            controllable_runtime(1);
        let (mut client, server_task) = spawn_connection(runtime, 1);
        let first_cookie = NbdCookie::new(201);
        let write_cookie = NbdCookie::new(202);

        client
            .write_all(&encode_read_request(first_cookie, 0, 4).expect("first read"))
            .await
            .expect("send first read");
        expect_event(&mut reserve_started).await;
        expect_event(&mut reserve_acquired).await;
        let first_job = submitted.recv().await.expect("first job");

        client
            .write_all(&encode_request_header(RequestHeader {
                flags: NbdCommandFlags::new(0),
                command: NbdCommandType::new(NBD_CMD_WRITE),
                cookie: write_cookie,
                offset: 8,
                length: 4,
            }))
            .await
            .expect("send write header");
        expect_event(&mut reserve_started).await;
        assert_no_event(&mut reserve_acquired, "second reserve should wait").await;
        assert!(
            submitted.try_recv().is_err(),
            "write should not submit before queue depth is available",
        );

        complete_job(
            first_job,
            ExportRequest::Read { offset: 0, len: 4 },
            Ok(ExportReply::Read {
                data: b"aaaa".to_vec(),
            }),
        )
        .await;
        assert_eq!(
            read_successful_read(&mut client, 4).await,
            (first_cookie, b"aaaa".to_vec()),
        );

        expect_event(&mut reserve_acquired).await;
        assert!(
            submitted.try_recv().is_err(),
            "write should wait for payload after reserving queue depth",
        );

        client.write_all(b"zzzz").await.expect("send write payload");
        let write_job = submitted.recv().await.expect("write job");
        complete_job(
            write_job,
            ExportRequest::Write {
                offset: 8,
                data: b"zzzz".to_vec(),
            },
            Ok(ExportReply::Done),
        )
        .await;
        assert_success_reply(&mut client, write_cookie).await;

        disconnect_and_join(client, server_task).await;
    }

    #[tokio::test]
    async fn reply_write_holds_queue_slot_until_socket_write_finishes() {
        let meta = export_record("disk-a", 4096);
        let engine = Arc::new(NoopEngine);
        let runtime = SerialExportRuntime::with_capacity(meta, engine, 1);
        let queue_slot = runtime.reserve().await.expect("reserve queue slot");
        let reply = ConnectionReply::export_result(
            ExportJobContext::internal(RequestCookie::new(301), "read"),
            ReplyKind::Read,
            Ok(ExportReply::Read {
                data: vec![7; 1024],
            }),
            queue_slot,
        );
        let (mut writer, mut reader) = duplex(16);

        let write_task =
            tokio::spawn(async move { write_connection_reply(&mut writer, reply).await });
        tokio::task::yield_now().await;
        assert!(
            !write_task.is_finished(),
            "small duplex buffer should block the reply write",
        );

        let waiter_runtime = runtime.clone();
        let waiter =
            tokio::spawn(async move { waiter_runtime.reserve().await.expect("reserve again") });
        tokio::task::yield_now().await;
        assert!(
            !waiter.is_finished(),
            "reply write should hold queue depth until write_all finishes",
        );

        let mut bytes = vec![0; SIMPLE_REPLY_BYTES + 1024];
        reader
            .read_exact(&mut bytes)
            .await
            .expect("drain blocked reply");
        write_task
            .await
            .expect("reply write task")
            .expect("reply write");
        let next_slot = waiter.await.expect("reservation task");
        drop(next_slot);
    }

    #[tokio::test]
    async fn connection_shutdown_stops_blocked_request_reader() {
        let (runtime, _submitted, _reserve_started, _reserve_acquired) = controllable_runtime(1);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let (client, server) = duplex(64 * 1024);
        let (reader, _server_writer) = split(server);
        let (reply_sender, _reply_receiver) = mpsc::channel(1);
        let task = tokio::spawn(read_requests(
            reader,
            ConnectionId::next(),
            runtime,
            reply_sender,
            ConnectionShutdown::from_receiver(shutdown_rx),
        ));

        shutdown_tx.send(true).expect("signal shutdown");

        let exit = task.await.expect("request reader task");
        assert_eq!(exit.reply_drain, ConnectionReplyDrain::DropPending);
        exit.result.expect("reader shutdown");
        drop(client);
    }

    async fn complete_job(job: ExportJob, expected: ExportRequest, result: ExportResult) {
        let (_context, request, completion, queue_slot) = job.into_parts();
        assert_eq!(request, expected);
        completion.complete(result, queue_slot).await;
    }

    async fn read_successful_read(client: &mut DuplexStream, len: usize) -> (NbdCookie, Vec<u8>) {
        let reply = read_simple_reply(client).await;
        assert_eq!(reply.error, 0);

        let mut data = vec![0; len];
        client
            .read_exact(&mut data)
            .await
            .expect("read reply payload");
        (reply.cookie, data)
    }

    async fn assert_success_reply(client: &mut DuplexStream, expected_cookie: NbdCookie) {
        let reply = read_simple_reply(client).await;
        assert_eq!(reply.cookie, expected_cookie);
        assert_eq!(reply.error, 0);
    }

    async fn read_simple_reply(client: &mut DuplexStream) -> nbd_protocol::SimpleReply {
        let mut bytes = [0; SIMPLE_REPLY_BYTES];
        client.read_exact(&mut bytes).await.expect("read reply");
        parse_simple_reply(&bytes).expect("simple reply")
    }

    async fn disconnect_and_join(mut client: DuplexStream, server_task: JoinHandle<Result<()>>) {
        client
            .write_all(&encode_disconnect_request(NbdCookie::new(999)).expect("disconnect"))
            .await
            .expect("send disconnect");
        client.shutdown().await.expect("shutdown client");
        server_task
            .await
            .expect("connection task")
            .expect("connection runtime");
    }

    async fn expect_event(receiver: &mut UnboundedReceiver<()>) {
        receiver.recv().await.expect("runtime event");
    }

    async fn assert_no_event(receiver: &mut UnboundedReceiver<()>, message: &str) {
        for _ in 0..4 {
            assert!(receiver.try_recv().is_err(), "{message}");
            tokio::task::yield_now().await;
        }
    }

    fn spawn_connection(
        runtime: ExportRuntimeHandle,
        reply_capacity: usize,
    ) -> (DuplexStream, JoinHandle<Result<()>>) {
        let (client, server) = duplex(64 * 1024);
        let (reader, writer) = split(server);
        let task = tokio::spawn(
            ConnectionRuntime::new(ConnectionId::next(), runtime, reply_capacity).run_io(
                reader,
                writer,
                ConnectionShutdown::not_cancelled(),
            ),
        );
        (client, task)
    }

    fn controllable_runtime(
        capacity: usize,
    ) -> (
        ExportRuntimeHandle,
        mpsc::Receiver<ExportJob>,
        UnboundedReceiver<()>,
        UnboundedReceiver<()>,
    ) {
        let meta = export_record("disk-a", 4096);
        let engine = Arc::new(NoopEngine);
        let reservations = SerialExportRuntime::with_capacity(meta.clone(), engine, capacity);
        let (submitted_sender, submitted_receiver) = mpsc::channel(8);
        let (reserve_started_sender, reserve_started_receiver) = unbounded_channel();
        let (reserve_acquired_sender, reserve_acquired_receiver) = unbounded_channel();

        (
            Arc::new(ControllableRuntime {
                meta,
                reservations,
                submitted: submitted_sender,
                reserve_started: reserve_started_sender,
                reserve_acquired: reserve_acquired_sender,
            }),
            submitted_receiver,
            reserve_started_receiver,
            reserve_acquired_receiver,
        )
    }

    #[derive(Clone)]
    struct ControllableRuntime {
        meta: ExportRecord,
        reservations: SerialExportRuntime,
        submitted: mpsc::Sender<ExportJob>,
        reserve_started: UnboundedSender<()>,
        reserve_acquired: UnboundedSender<()>,
    }

    #[async_trait::async_trait]
    impl crate::runtime::ExportRuntime for ControllableRuntime {
        fn export_record(&self) -> ExportRecord {
            self.meta.clone()
        }

        async fn reserve(&self) -> Result<ExportQueueSlot> {
            let _ = self.reserve_started.send(());
            let queue_slot = self.reservations.reserve().await?;
            let _ = self.reserve_acquired.send(());
            Ok(queue_slot)
        }

        async fn submit(&self, job: ExportJob) -> Result<()> {
            self.submitted
                .send(job)
                .await
                .map_err(|_| ServerError::RuntimeClosed {
                    resource: "controllable runtime",
                })
        }

        async fn close(&self) -> Result<()> {
            self.reservations.close().await
        }
    }

    struct NoopEngine;

    #[async_trait::async_trait]
    impl ExportEngine for NoopEngine {
        fn admission_policy(&self) -> ExportAdmissionPolicyHandle {
            Arc::new(MemoryAdmissionPolicy::new(4096))
        }

        async fn execute_admitted(&self, _request: AdmittedExportRequest) -> ExportResult {
            Ok(ExportReply::Done)
        }
    }

    fn export_record(name: &str, size_bytes: u64) -> ExportRecord {
        ExportRecord::new(
            ExportId::new(format!("export-{name}")).expect("export id"),
            ExportName::new(name).expect("export name"),
            4096,
            ExportEngineKind::Memory,
            ExportState::Active,
            ExportHead::memory_empty(size_bytes).expect("memory head"),
            Timestamp::new("created").expect("created timestamp"),
            Timestamp::new("updated").expect("updated timestamp"),
            None,
        )
        .expect("export meta")
    }
}
