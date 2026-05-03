use crate::{
    CompletedExport, ExportJob, ExportOwner, ExportQueueSlot, ExportReply, ExportRequest,
    ExportResult, ExportRuntimeHandle, LocalExportRegistry, Result, ServerError,
};
use nbd_control_plane::ExportName;
use nbd_protocol::constants::{NBD_EINVAL, NBD_FLAG_HAS_FLAGS, NBD_FLAG_SEND_FLUSH};
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
use tokio::io::{AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;

const SUPPORTED_TRANSMISSION_FLAGS: u16 = NBD_FLAG_HAS_FLAGS | NBD_FLAG_SEND_FLUSH;

struct ConnectionExport {
    name: ExportName,
    owner: ExportOwner,
    runtime: ExportRuntimeHandle,
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
    fn completed(cookie: NbdCookie, kind: ReplyKind, completed: CompletedExport) -> Self {
        let (result, queue_slot) = completed.into_parts();
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

pub async fn serve(mut stream: TcpStream, registry: Arc<LocalExportRegistry>) -> Result<()> {
    write_handshake(&mut stream).await?;
    let Some(export) = negotiate_options(&mut stream, registry.clone()).await? else {
        return Ok(());
    };
    let result = handle_transmission(&mut stream, export.runtime.clone()).await;
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

async fn handle_transmission(stream: &mut TcpStream, runtime: ExportRuntimeHandle) -> Result<()> {
    loop {
        let mut bytes = vec![0; REQUEST_HEADER_BYTES];
        match stream.read_exact(&mut bytes).await {
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(()),
            Err(source) => {
                return Err(ServerError::io("read NBD transmission header", source));
            }
        }

        let header = parse_request_header(&bytes)?;
        let payload_len = match header.payload_len(MAX_IO_BYTES) {
            Ok(payload_len) => payload_len,
            Err(error) => {
                write_connection_reply(
                    stream,
                    ConnectionReply::simple_error(header.cookie, NBD_EINVAL),
                )
                .await?;
                return Err(error.into());
            }
        };
        let mut payload = vec![0; payload_len];
        stream
            .read_exact(&mut payload)
            .await
            .map_err(|source| ServerError::io("read NBD transmission payload", source))?;
        bytes.extend_from_slice(&payload);

        match parse_request(&bytes, MAX_IO_BYTES)? {
            TransmissionRequest::Read {
                cookie,
                offset,
                length,
            } => {
                match execute_export(
                    &runtime,
                    ExportRequest::Read {
                        offset,
                        len: length,
                    },
                )
                .await
                {
                    Ok(completed) => {
                        write_connection_reply(
                            stream,
                            ConnectionReply::completed(cookie, ReplyKind::Read, completed),
                        )
                        .await?;
                    }
                    Err(_) => {
                        write_connection_reply(
                            stream,
                            ConnectionReply::simple_error(cookie, NBD_EINVAL),
                        )
                        .await?;
                    }
                }
            }
            TransmissionRequest::Write {
                cookie,
                offset,
                data,
            } => {
                let reply =
                    match execute_export(&runtime, ExportRequest::Write { offset, data }).await {
                        Ok(completed) => {
                            ConnectionReply::completed(cookie, ReplyKind::Simple, completed)
                        }
                        Err(_) => ConnectionReply::simple_error(cookie, NBD_EINVAL),
                    };
                write_connection_reply(stream, reply).await?;
            }
            TransmissionRequest::Flush { cookie } => {
                let reply = match execute_export(&runtime, ExportRequest::Flush).await {
                    Ok(completed) => {
                        ConnectionReply::completed(cookie, ReplyKind::Simple, completed)
                    }
                    Err(_) => ConnectionReply::simple_error(cookie, NBD_EINVAL),
                };
                write_connection_reply(stream, reply).await?;
            }
            TransmissionRequest::Disconnect { .. } => return Ok(()),
        }
    }
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

async fn execute_export(
    runtime: &ExportRuntimeHandle,
    request: ExportRequest,
) -> Result<CompletedExport> {
    let queue_slot = runtime.reserve().await?;
    let (job, receiver) = ExportJob::oneshot(request, queue_slot);
    runtime.submit(job).await?;
    receiver.await.map_err(|_| ServerError::RuntimeClosed {
        resource: "export runtime reply",
    })
}
