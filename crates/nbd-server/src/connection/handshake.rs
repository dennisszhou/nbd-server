use super::{
    io::{read_exact_or_shutdown, write_all_or_shutdown},
    shutdown::ConnectionShutdown,
};
use crate::error::Result;
use nbd_protocol::handshake::{decode_client_flags, encode_server_handshake};
use tokio::net::TcpStream;

pub(super) async fn write_handshake(
    stream: &mut TcpStream,
    shutdown: &mut ConnectionShutdown,
) -> Result<bool> {
    if !write_all_or_shutdown(
        stream,
        &encode_server_handshake(),
        shutdown,
        "write NBD server handshake",
    )
    .await?
    {
        return Ok(false);
    }

    let mut client_flags = [0; 4];
    if !read_exact_or_shutdown(stream, &mut client_flags, shutdown, "read NBD client flags").await?
    {
        return Ok(false);
    }
    decode_client_flags(&client_flags)?;
    Ok(true)
}
