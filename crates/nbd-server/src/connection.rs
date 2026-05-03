use crate::{
    ExportCompletion, ExportJob, ExportOwner, ExportQueueSlot, ExportReply, ExportRequest,
    ExportResult, ExportRuntimeHandle, LocalExportRegistry, Result, ServerError,
    DEFAULT_EXPORT_QUEUE_CAPACITY,
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

impl ConnectionRuntime {
    fn new(runtime: ExportRuntimeHandle) -> Self {
        Self {
            runtime,
            reply_capacity: DEFAULT_EXPORT_QUEUE_CAPACITY,
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

pub async fn serve(mut stream: TcpStream, registry: Arc<LocalExportRegistry>) -> Result<()> {
    write_handshake(&mut stream).await?;
    let Some(export) = negotiate_options(&mut stream, registry.clone()).await? else {
        return Ok(());
    };
    let result = ConnectionRuntime::new(export.runtime.clone())
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

        let completion = ExportCompletion::connection(cookie, kind, replies.clone());
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
            _queue_slot,
        } => match (kind, result) {
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
        },
        ConnectionReplyPayload::SimpleError { error } => writer
            .write_all(&encode_simple_reply(reply.cookie, error))
            .await
            .map_err(|source| ServerError::io("write NBD error reply", source)),
    }
}
