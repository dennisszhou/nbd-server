use super::{
    io::{read_exact_or_shutdown, write_all_or_shutdown},
    shutdown::ConnectionShutdown,
};
use crate::error::{Result, ServerError};
use crate::export::{ConnectionId, ExportRuntimeHandle};
use crate::observability::{self, event, target};
use crate::registry::{ExportOwner, LocalExportRegistry};
use nbd_control_plane::ExportName;
use nbd_protocol::constants::{NBD_FLAG_HAS_FLAGS, NBD_FLAG_SEND_FLUSH};
use nbd_protocol::option::{
    OPTION_REQUEST_HEADER_BYTES, OptionRequest, encode_ack_reply, encode_export_info_reply,
    encode_policy_option_reply, encode_unknown_export_reply, encode_unsupported_option_reply,
    parse_option_request, parse_option_request_header,
};
use nbd_protocol::wire::NbdOptionCode;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpStream;

const SUPPORTED_TRANSMISSION_FLAGS: u16 = NBD_FLAG_HAS_FLAGS | NBD_FLAG_SEND_FLUSH;

pub(super) struct ConnectionExport {
    pub(super) name: ExportName,
    pub(super) owner: ExportOwner,
    pub(super) runtime: ExportRuntimeHandle,
}

pub(super) async fn negotiate_options(
    stream: &mut TcpStream,
    registry: Arc<LocalExportRegistry>,
    connection_id: ConnectionId,
    peer_addr: SocketAddr,
    shutdown: &mut ConnectionShutdown,
) -> Result<Option<ConnectionExport>> {
    tracing::debug!(
        target: target::CONNECTION,
        event = event::CONNECTION_NEGOTIATION_STARTED,
        service = observability::SERVICE_NAME,
        server_instance_id = observability::server_instance_id(),
        pid = observability::pid(),
        connection_id = connection_id.raw(),
        peer_addr = %peer_addr,
    );
    loop {
        let Some(request) = read_option_request(stream, shutdown).await? else {
            return Ok(None);
        };
        match request {
            OptionRequest::Go(go) => {
                let option = NbdOptionCode::new(nbd_protocol::constants::NBD_OPT_GO);
                let export_name =
                    ExportName::new(go.export_name().to_owned()).map_err(ServerError::catalog)?;
                let owner = ExportOwner::unique_connection();
                tracing::info!(
                    target: target::EXPORT,
                    event = event::EXPORT_OPEN_STARTED,
                    service = observability::SERVICE_NAME,
                    server_instance_id = observability::server_instance_id(),
                    pid = observability::pid(),
                    connection_id = connection_id.raw(),
                    peer_addr = %peer_addr,
                    export_name = %export_name,
                    owner_id = owner.id().raw(),
                );
                let runtime = match registry.open(export_name.clone(), owner).await {
                    Ok(runtime) => runtime,
                    Err(error) => {
                        tracing::warn!(
                            target: target::EXPORT,
                            event = event::EXPORT_OPEN_REJECTED,
                            service = observability::SERVICE_NAME,
                            server_instance_id = observability::server_instance_id(),
                            pid = observability::pid(),
                            connection_id = connection_id.raw(),
                            peer_addr = %peer_addr,
                            export_name = %export_name,
                            owner_id = owner.id().raw(),
                            error = %error,
                        );
                        if !write_go_error(stream, option, &error, shutdown).await? {
                            return Ok(None);
                        }
                        return Ok(None);
                    }
                };

                let meta = runtime.export_record();
                tracing::info!(
                    target: target::EXPORT,
                    event = event::EXPORT_OPEN_COMPLETED,
                    service = observability::SERVICE_NAME,
                    server_instance_id = observability::server_instance_id(),
                    pid = observability::pid(),
                    connection_id = connection_id.raw(),
                    peer_addr = %peer_addr,
                    export_id = %meta.id(),
                    export_name = %meta.name(),
                    owner_id = owner.id().raw(),
                    engine_kind = %meta.engine_kind(),
                    size_bytes = meta.size_bytes(),
                );
                let export_info = encode_export_info_reply(
                    option,
                    meta.size_bytes(),
                    SUPPORTED_TRANSMISSION_FLAGS,
                )?;
                let result =
                    write_all_or_shutdown(stream, &export_info, shutdown, "write NBD export info")
                        .await;
                match result {
                    Ok(true) => {}
                    Ok(false) => {
                        let _ = registry.close(&export_name, &owner).await;
                        return Ok(None);
                    }
                    Err(error) => {
                        let _ = registry.close(&export_name, &owner).await;
                        return Err(error);
                    }
                }

                let ack = encode_ack_reply(option)?;
                let result =
                    write_all_or_shutdown(stream, &ack, shutdown, "write NBD_OPT_GO ack").await;
                match result {
                    Ok(true) => {}
                    Ok(false) => {
                        let _ = registry.close(&export_name, &owner).await;
                        return Ok(None);
                    }
                    Err(error) => {
                        let _ = registry.close(&export_name, &owner).await;
                        return Err(error);
                    }
                }

                tracing::debug!(
                    target: target::CONNECTION,
                    event = event::CONNECTION_NEGOTIATION_COMPLETED,
                    service = observability::SERVICE_NAME,
                    server_instance_id = observability::server_instance_id(),
                    pid = observability::pid(),
                    connection_id = connection_id.raw(),
                    peer_addr = %peer_addr,
                    export_name = %export_name,
                    owner_id = owner.id().raw(),
                );
                return Ok(Some(ConnectionExport {
                    name: export_name,
                    owner,
                    runtime,
                }));
            }
            OptionRequest::Abort { .. } => {
                let option = NbdOptionCode::new(nbd_protocol::constants::NBD_OPT_ABORT);
                let ack = encode_ack_reply(option)?;
                if !write_all_or_shutdown(stream, &ack, shutdown, "write NBD_OPT_ABORT ack").await?
                {
                    return Ok(None);
                }
                return Ok(None);
            }
            OptionRequest::Unknown { code, .. } => {
                let reply = encode_unsupported_option_reply(code, b"unsupported option")?;
                if !write_all_or_shutdown(stream, &reply, shutdown, "write unsupported option")
                    .await?
                {
                    return Ok(None);
                }
            }
        }
    }
}

async fn write_go_error(
    stream: &mut TcpStream,
    option: NbdOptionCode,
    error: &ServerError,
    shutdown: &mut ConnectionShutdown,
) -> Result<bool> {
    let message = error.to_string();
    let reply = match error {
        ServerError::ExportBusy { .. } | ServerError::ExportTooLarge { .. } => {
            encode_policy_option_reply(option, message.as_bytes())?
        }
        _ => encode_unknown_export_reply(option, message.as_bytes())?,
    };
    write_all_or_shutdown(stream, &reply, shutdown, "write NBD_OPT_GO error").await
}

async fn read_option_request(
    stream: &mut TcpStream,
    shutdown: &mut ConnectionShutdown,
) -> Result<Option<OptionRequest>> {
    let mut bytes = vec![0; OPTION_REQUEST_HEADER_BYTES];
    if !read_exact_or_shutdown(
        stream,
        &mut bytes,
        shutdown,
        "read NBD option request header",
    )
    .await?
    {
        return Ok(None);
    }

    let header = parse_option_request_header(&bytes)?;
    let mut payload = vec![0; header.bounded_payload_len()?];
    if !read_exact_or_shutdown(
        stream,
        &mut payload,
        shutdown,
        "read NBD option request payload",
    )
    .await?
    {
        return Ok(None);
    }
    bytes.extend_from_slice(&payload);

    Ok(Some(parse_option_request(&bytes)?))
}
