use crate::{ExportHandle, MemoryExport, Result, ServerError};
use nbd_control_plane::{ExportCatalog, ExportName, SQLiteExportCatalog};
use nbd_protocol::constants::{NBD_EINVAL, NBD_FLAG_HAS_FLAGS, NBD_FLAG_SEND_FLUSH};
use nbd_protocol::handshake::{decode_client_flags, encode_server_handshake};
use nbd_protocol::option::{
    encode_ack_reply, encode_export_info_reply, encode_unknown_export_reply,
    encode_unsupported_option_reply, parse_option_request, parse_option_request_header,
    OptionRequest, OPTION_REQUEST_HEADER_BYTES,
};
use nbd_protocol::transmission::{
    encode_read_reply, encode_simple_reply, encode_success_reply, parse_request,
    parse_request_header, TransmissionRequest, MAX_IO_BYTES, REQUEST_HEADER_BYTES,
};
use nbd_protocol::wire::NbdOptionCode;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

const SUPPORTED_TRANSMISSION_FLAGS: u16 = NBD_FLAG_HAS_FLAGS | NBD_FLAG_SEND_FLUSH;

pub async fn serve(mut stream: TcpStream, catalog: SQLiteExportCatalog) -> Result<()> {
    write_handshake(&mut stream).await?;
    let Some(export) = negotiate_options(&mut stream, catalog).await? else {
        return Ok(());
    };
    handle_transmission(&mut stream, export).await
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
    catalog: SQLiteExportCatalog,
) -> Result<Option<ExportHandle>> {
    loop {
        let request = read_option_request(stream).await?;
        match request {
            OptionRequest::Go(go) => {
                let option = NbdOptionCode::new(nbd_protocol::constants::NBD_OPT_GO);
                let export_name =
                    ExportName::new(go.export_name().to_owned()).map_err(ServerError::catalog)?;
                let meta = match catalog.load_export(export_name).await {
                    Ok(meta) => meta,
                    Err(error) => {
                        stream
                            .write_all(&encode_unknown_export_reply(
                                option,
                                error.to_string().as_bytes(),
                            )?)
                            .await
                            .map_err(|source| ServerError::io("write NBD_OPT_GO error", source))?;
                        return Ok(None);
                    }
                };
                let export: ExportHandle = match MemoryExport::new(&meta) {
                    Ok(export) => Arc::new(export),
                    Err(error) => {
                        stream
                            .write_all(&encode_unknown_export_reply(
                                option,
                                error.to_string().as_bytes(),
                            )?)
                            .await
                            .map_err(|source| ServerError::io("write NBD_OPT_GO error", source))?;
                        return Ok(None);
                    }
                };
                stream
                    .write_all(&encode_export_info_reply(
                        option,
                        meta.size_bytes(),
                        SUPPORTED_TRANSMISSION_FLAGS,
                    )?)
                    .await
                    .map_err(|source| ServerError::io("write NBD export info", source))?;
                stream
                    .write_all(&encode_ack_reply(option)?)
                    .await
                    .map_err(|source| ServerError::io("write NBD_OPT_GO ack", source))?;
                return Ok(Some(export));
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

async fn handle_transmission(stream: &mut TcpStream, export: ExportHandle) -> Result<()> {
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
                stream
                    .write_all(&encode_simple_reply(header.cookie, NBD_EINVAL))
                    .await
                    .map_err(|source| ServerError::io("write NBD request error", source))?;
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
            } => match export.read(offset, length).await {
                Ok(data) => {
                    stream
                        .write_all(&encode_read_reply(cookie, &data))
                        .await
                        .map_err(|source| ServerError::io("write NBD read reply", source))?;
                }
                Err(_) => {
                    stream
                        .write_all(&encode_simple_reply(cookie, NBD_EINVAL))
                        .await
                        .map_err(|source| ServerError::io("write NBD read error", source))?;
                }
            },
            TransmissionRequest::Write {
                cookie,
                offset,
                data,
            } => {
                let error = match export.write(offset, &data).await {
                    Ok(()) => 0,
                    Err(_) => NBD_EINVAL,
                };
                stream
                    .write_all(&encode_simple_reply(cookie, error))
                    .await
                    .map_err(|source| ServerError::io("write NBD write reply", source))?;
            }
            TransmissionRequest::Flush { cookie } => {
                export.flush().await?;
                stream
                    .write_all(&encode_success_reply(cookie))
                    .await
                    .map_err(|source| ServerError::io("write NBD flush reply", source))?;
            }
            TransmissionRequest::Disconnect { .. } => return Ok(()),
        }
    }
}
