use super::shutdown::ConnectionShutdown;
use crate::{Result, ServerError};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub(super) async fn read_exact_or_shutdown<R>(
    reader: &mut R,
    bytes: &mut [u8],
    shutdown: &mut ConnectionShutdown,
    context: &'static str,
) -> Result<bool>
where
    R: AsyncRead + Unpin,
{
    if shutdown.is_cancelled() {
        return Ok(false);
    }

    tokio::select! {
        result = reader.read_exact(bytes) => {
            result
                .map_err(|source| ServerError::io(context, source))
                .map(|_| true)
        }
        () = shutdown.cancelled() => Ok(false),
    }
}

pub(super) async fn write_all_or_shutdown<W>(
    writer: &mut W,
    bytes: &[u8],
    shutdown: &mut ConnectionShutdown,
    context: &'static str,
) -> Result<bool>
where
    W: AsyncWrite + Unpin,
{
    if shutdown.is_cancelled() {
        return Ok(false);
    }

    tokio::select! {
        result = writer.write_all(bytes) => {
            result
                .map_err(|source| ServerError::io(context, source))
                .map(|()| true)
        }
        () = shutdown.cancelled() => Ok(false),
    }
}
