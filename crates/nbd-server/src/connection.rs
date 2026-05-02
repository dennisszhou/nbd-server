use crate::{MemoryExport, Result, ServerError};
use nbd_control_plane::{ExportCatalog, ExportName, SQLiteExportCatalog};
use nbd_protocol::constants::{NBD_FLAG_HAS_FLAGS, NBD_FLAG_SEND_FLUSH};
use nbd_protocol::handshake::{decode_client_flags, encode_server_handshake};
use nbd_protocol::option::{
    encode_ack_reply, encode_export_info_reply, encode_unknown_export_reply,
    encode_unsupported_option_reply, parse_option_request, OptionRequest,
    OPTION_REQUEST_HEADER_BYTES,
};
use nbd_protocol::wire::NbdOptionCode;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

const TOY_TRANSMISSION_FLAGS: u16 = NBD_FLAG_HAS_FLAGS | NBD_FLAG_SEND_FLUSH;

pub async fn serve(mut stream: TcpStream, catalog: SQLiteExportCatalog) -> Result<()> {
    write_handshake(&mut stream).await?;
    let Some(_export) = negotiate_options(&mut stream, catalog).await? else {
        return Ok(());
    };
    drain_until_eof(&mut stream).await
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
) -> Result<Option<MemoryExport>> {
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
                let export = match MemoryExport::new(&meta) {
                    Ok(export) => export,
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
                        TOY_TRANSMISSION_FLAGS,
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

    let payload_len = u32::from_be_bytes(bytes[12..16].try_into().expect("header length"));
    let mut payload = vec![0; payload_len as usize];
    stream
        .read_exact(&mut payload)
        .await
        .map_err(|source| ServerError::io("read NBD option request payload", source))?;
    bytes.extend_from_slice(&payload);

    Ok(parse_option_request(&bytes)?)
}

async fn drain_until_eof(stream: &mut TcpStream) -> Result<()> {
    let mut buf = [0; 1024];
    loop {
        let n = stream
            .read(&mut buf)
            .await
            .map_err(|source| ServerError::io("read toy transmission placeholder", source))?;
        if n == 0 {
            return Ok(());
        }
    }
}
