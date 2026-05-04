use crate::export::ExportCompletionSink;
use crate::{
    CompletedExport, ExportCompletion, ExportJob, ExportOwner, ExportQueueSlot, ExportReply,
    ExportRequest, ExportResult, ExportRuntimeHandle, LocalExportRegistry, Result, ServerError,
};
use nbd_control_plane::ExportName;
use nbd_protocol::constants::{NBD_CMD_DISC, NBD_EINVAL, NBD_FLAG_HAS_FLAGS, NBD_FLAG_SEND_FLUSH};
use nbd_protocol::handshake::{decode_client_flags, encode_server_handshake};
use nbd_protocol::option::{
    encode_ack_reply, encode_export_info_reply, encode_policy_option_reply,
    encode_unknown_export_reply, encode_unsupported_option_reply, parse_option_request,
    parse_option_request_header, OptionRequest, OPTION_REQUEST_HEADER_BYTES,
};
use nbd_protocol::transmission::{
    encode_read_reply, encode_simple_reply, parse_request, parse_request_header,
    TransmissionRequest, MAX_IO_BYTES, REQUEST_HEADER_BYTES,
};
use nbd_protocol::wire::{NbdCookie, NbdOptionCode};
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

const SUPPORTED_TRANSMISSION_FLAGS: u16 = NBD_FLAG_HAS_FLAGS | NBD_FLAG_SEND_FLUSH;

struct ConnectionExport {
    name: ExportName,
    owner: ExportOwner,
    runtime: ExportRuntimeHandle,
}

struct ConnectionRuntime {
    runtime: ExportRuntimeHandle,
    reply_capacity: usize,
}

struct RequestReaderExit {
    result: Result<()>,
    drain_replies: bool,
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
    cookie: NbdCookie,
    kind: ReplyKind,
    replies: mpsc::Sender<ConnectionReply>,
}

impl ConnectionReply {
    pub(crate) fn export_result(
        cookie: NbdCookie,
        kind: ReplyKind,
        result: ExportResult,
        queue_slot: ExportQueueSlot,
    ) -> Self {
        Self {
            cookie,
            payload: ConnectionReplyPayload::Export {
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
    fn new(cookie: NbdCookie, kind: ReplyKind, replies: mpsc::Sender<ConnectionReply>) -> Self {
        Self {
            cookie,
            kind,
            replies,
        }
    }
}

#[async_trait::async_trait]
impl ExportCompletionSink for ConnectionExportCompletion {
    async fn complete(self: Box<Self>, completed: CompletedExport) {
        let (result, queue_slot) = completed.into_parts();
        let reply = ConnectionReply::export_result(self.cookie, self.kind, result, queue_slot);
        let _ = self.replies.send(reply).await;
    }
}

impl ConnectionRuntime {
    fn new(runtime: ExportRuntimeHandle, reply_capacity: usize) -> Self {
        Self {
            runtime,
            reply_capacity,
        }
    }

    async fn run(self, stream: TcpStream) -> Result<()> {
        let (reader, writer) = stream.into_split();
        self.run_io(reader, writer).await
    }

    async fn run_io<R, W>(self, reader: R, writer: W) -> Result<()>
    where
        R: AsyncRead + Unpin + Send + 'static,
        W: AsyncWrite + Unpin + Send + 'static,
    {
        let (reply_sender, reply_receiver) = mpsc::channel(self.reply_capacity);
        let reader_task = tokio::spawn(read_requests(reader, self.runtime, reply_sender));
        let writer_task = tokio::spawn(write_replies(writer, reply_receiver));

        run_connection_tasks(reader_task, writer_task).await
    }
}

pub async fn serve(
    mut stream: TcpStream,
    registry: Arc<LocalExportRegistry>,
    reply_capacity: usize,
) -> Result<()> {
    write_handshake(&mut stream).await?;
    let Some(export) = negotiate_options(&mut stream, registry.clone()).await? else {
        return Ok(());
    };
    let result = ConnectionRuntime::new(export.runtime.clone(), reply_capacity)
        .run(stream)
        .await;
    let close_result = registry.close(&export.name, &export.owner).await;

    match (result, close_result) {
        (Err(error), _) => Err(error),
        (Ok(()), Err(error)) => Err(error),
        (Ok(()), Ok(())) => Ok(()),
    }
}

async fn write_handshake(stream: &mut TcpStream) -> Result<()> {
    stream
        .write_all(&encode_server_handshake())
        .await
        .map_err(|source| ServerError::io("write NBD server handshake", source))?;

    let mut client_flags = [0; 4];
    stream
        .read_exact(&mut client_flags)
        .await
        .map_err(|source| ServerError::io("read NBD client flags", source))?;
    decode_client_flags(&client_flags)?;
    Ok(())
}

async fn negotiate_options(
    stream: &mut TcpStream,
    registry: Arc<LocalExportRegistry>,
) -> Result<Option<ConnectionExport>> {
    loop {
        let request = read_option_request(stream).await?;
        match request {
            OptionRequest::Go(go) => {
                let option = NbdOptionCode::new(nbd_protocol::constants::NBD_OPT_GO);
                let export_name =
                    ExportName::new(go.export_name().to_owned()).map_err(ServerError::catalog)?;
                let owner = ExportOwner::unique_connection();
                let runtime = match registry.open(export_name.clone(), owner).await {
                    Ok(runtime) => runtime,
                    Err(error) => {
                        write_go_error(stream, option, &error).await?;
                        return Ok(None);
                    }
                };

                let meta = runtime.export_meta();
                let result = stream
                    .write_all(&encode_export_info_reply(
                        option,
                        meta.size_bytes(),
                        SUPPORTED_TRANSMISSION_FLAGS,
                    )?)
                    .await
                    .map_err(|source| ServerError::io("write NBD export info", source));
                if let Err(error) = result {
                    let _ = registry.close(&export_name, &owner).await;
                    return Err(error);
                }

                let result = stream
                    .write_all(&encode_ack_reply(option)?)
                    .await
                    .map_err(|source| ServerError::io("write NBD_OPT_GO ack", source));
                if let Err(error) = result {
                    let _ = registry.close(&export_name, &owner).await;
                    return Err(error);
                }

                return Ok(Some(ConnectionExport {
                    name: export_name,
                    owner,
                    runtime,
                }));
            }
            OptionRequest::Abort { .. } => {
                let option = NbdOptionCode::new(nbd_protocol::constants::NBD_OPT_ABORT);
                stream
                    .write_all(&encode_ack_reply(option)?)
                    .await
                    .map_err(|source| ServerError::io("write NBD_OPT_ABORT ack", source))?;
                return Ok(None);
            }
            OptionRequest::Unknown { code, .. } => {
                stream
                    .write_all(&encode_unsupported_option_reply(
                        code,
                        b"unsupported option",
                    )?)
                    .await
                    .map_err(|source| ServerError::io("write unsupported option", source))?;
            }
        }
    }
}

async fn write_go_error(
    stream: &mut TcpStream,
    option: NbdOptionCode,
    error: &ServerError,
) -> Result<()> {
    let message = error.to_string();
    let reply = match error {
        ServerError::ExportBusy { .. } | ServerError::ExportTooLarge { .. } => {
            encode_policy_option_reply(option, message.as_bytes())?
        }
        _ => encode_unknown_export_reply(option, message.as_bytes())?,
    };
    stream
        .write_all(&reply)
        .await
        .map_err(|source| ServerError::io("write NBD_OPT_GO error", source))
}

async fn read_option_request(stream: &mut TcpStream) -> Result<OptionRequest> {
    let mut bytes = vec![0; OPTION_REQUEST_HEADER_BYTES];
    stream
        .read_exact(&mut bytes)
        .await
        .map_err(|source| ServerError::io("read NBD option request header", source))?;

    let header = parse_option_request_header(&bytes)?;
    let mut payload = vec![0; header.bounded_payload_len()?];
    stream
        .read_exact(&mut payload)
        .await
        .map_err(|source| ServerError::io("read NBD option request payload", source))?;
    bytes.extend_from_slice(&payload);

    Ok(parse_option_request(&bytes)?)
}

async fn read_requests<R>(
    mut reader: R,
    runtime: ExportRuntimeHandle,
    replies: mpsc::Sender<ConnectionReply>,
) -> RequestReaderExit
where
    R: AsyncRead + Unpin,
{
    loop {
        let mut bytes = vec![0; REQUEST_HEADER_BYTES];
        match reader.read_exact(&mut bytes).await {
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
            Err(error) => return RequestReaderExit::close(Err(error.into())),
        };
        let payload_len = match header.payload_len(MAX_IO_BYTES) {
            Ok(payload_len) => payload_len,
            Err(error) => {
                return RequestReaderExit::drain(
                    send_error_then_return(&replies, header.cookie, error.into()).await,
                );
            }
        };

        if header.command.raw() == NBD_CMD_DISC {
            return RequestReaderExit::close(Ok(()));
        }

        let queue_slot = match runtime.reserve().await {
            Ok(queue_slot) => queue_slot,
            Err(error) => {
                return RequestReaderExit::drain(
                    send_simple_error(&replies, header.cookie)
                        .await
                        .and(Err(error)),
                );
            }
        };

        let mut payload = vec![0; payload_len];
        if let Err(source) = reader.read_exact(&mut payload).await {
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
                drop(queue_slot);
                return RequestReaderExit::drain(
                    send_error_then_return(&replies, header.cookie, error.into()).await,
                );
            }
        };

        let (cookie, kind, request) = match request {
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
            ),
            TransmissionRequest::Write {
                cookie,
                offset,
                data,
            } => (
                cookie,
                ReplyKind::Simple,
                ExportRequest::Write { offset, data },
            ),
            TransmissionRequest::Flush { cookie } => {
                (cookie, ReplyKind::Simple, ExportRequest::Flush)
            }
            TransmissionRequest::Disconnect { .. } => {
                drop(queue_slot);
                return RequestReaderExit::close(Ok(()));
            }
        };

        let completion = ExportCompletion::sink(ConnectionExportCompletion::new(
            cookie,
            kind,
            replies.clone(),
        ));
        let job = ExportJob::new(request, completion, queue_slot);
        if let Err(error) = runtime.submit(job).await {
            return RequestReaderExit::drain(
                send_simple_error(&replies, cookie).await.and(Err(error)),
            );
        }
    }
}

async fn write_replies<W>(mut writer: W, mut replies: mpsc::Receiver<ConnectionReply>) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    while let Some(reply) = replies.recv().await {
        write_connection_reply(&mut writer, reply).await?;
    }
    Ok(())
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

            if reader_exit.drain_replies {
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
            drain_replies: false,
        }
    }

    fn drain(result: Result<()>) -> Self {
        Self {
            result,
            drain_replies: true,
        }
    }
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

pub(crate) async fn write_connection_reply<W>(writer: &mut W, reply: ConnectionReply) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    match reply.payload {
        ConnectionReplyPayload::Export {
            kind,
            result,
            _queue_slot: queue_slot,
        } => {
            let write_result = match (kind, result) {
                (ReplyKind::Read, Ok(ExportReply::Read { data })) => writer
                    .write_all(&encode_read_reply(reply.cookie, &data))
                    .await
                    .map_err(|source| ServerError::io("write NBD read reply", source)),
                (ReplyKind::Simple, Ok(ExportReply::Done)) => writer
                    .write_all(&encode_simple_reply(reply.cookie, 0))
                    .await
                    .map_err(|source| ServerError::io("write NBD simple reply", source)),
                _ => writer
                    .write_all(&encode_simple_reply(reply.cookie, NBD_EINVAL))
                    .await
                    .map_err(|source| ServerError::io("write NBD error reply", source)),
            };
            drop(queue_slot);
            write_result
        }
        ConnectionReplyPayload::SimpleError { error } => writer
            .write_all(&encode_simple_reply(reply.cookie, error))
            .await
            .map_err(|source| ServerError::io("write NBD error reply", source)),
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
        ExportEngineKind, ExportHead, ExportId, ExportMeta, ExportName, ExportState, Timestamp,
    };
    use nbd_protocol::constants::NBD_CMD_WRITE;
    use nbd_protocol::transmission::{
        encode_disconnect_request, encode_read_request, encode_request_header, parse_simple_reply,
        RequestHeader, SIMPLE_REPLY_BYTES,
    };
    use nbd_protocol::wire::{NbdCommandFlags, NbdCommandType};
    use std::sync::Arc;
    use tokio::io::{duplex, split, DuplexStream};
    use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

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
        let meta = export_meta("disk-a", 4096);
        let engine = Arc::new(NoopEngine);
        let runtime = SerialExportRuntime::with_capacity(meta, engine, 1);
        let queue_slot = runtime.reserve().await.expect("reserve queue slot");
        let reply = ConnectionReply::export_result(
            NbdCookie::new(301),
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

    async fn complete_job(job: ExportJob, expected: ExportRequest, result: ExportResult) {
        let (request, completion, queue_slot) = job.into_parts();
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
            ConnectionRuntime {
                runtime,
                reply_capacity,
            }
            .run_io(reader, writer),
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
        let meta = export_meta("disk-a", 4096);
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
        meta: ExportMeta,
        reservations: SerialExportRuntime,
        submitted: mpsc::Sender<ExportJob>,
        reserve_started: UnboundedSender<()>,
        reserve_acquired: UnboundedSender<()>,
    }

    #[async_trait::async_trait]
    impl crate::runtime::ExportRuntime for ControllableRuntime {
        fn export_meta(&self) -> ExportMeta {
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

    fn export_meta(name: &str, size_bytes: u64) -> ExportMeta {
        ExportMeta::new(
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
