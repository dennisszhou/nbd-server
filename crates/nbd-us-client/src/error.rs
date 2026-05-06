use nbd_protocol::ProtocolError;
use nbd_protocol::wire::NbdCookie;
use std::io;
use thiserror::Error;

pub type Result<T> = std::result::Result<T, ClientError>;

#[derive(Debug, Error)]
pub enum ClientError {
    #[error("{context}: {source}")]
    Io {
        context: &'static str,
        source: io::Error,
    },
    #[error("{source}")]
    Protocol {
        #[from]
        source: ProtocolError,
    },
    #[error("NBD server advertised unsupported flags: 0x{flags:04x}")]
    UnsupportedServerFlags { flags: u16 },
    #[error(
        "NBD option negotiation failed: reply_type=0x{reply_type:08x}, message={}",
        String::from_utf8_lossy(message)
    )]
    OptionError { reply_type: u32, message: Vec<u8> },
    #[error("NBD {command} failed with error {error}")]
    CommandError { command: &'static str, error: u32 },
    #[error(
        "NBD reply cookie mismatch: expected 0x{:016x}, got 0x{:016x}",
        expected.raw(),
        actual.raw()
    )]
    CookieMismatch {
        expected: NbdCookie,
        actual: NbdCookie,
    },
    #[error("unexpected NBD option reply: {reply}")]
    UnexpectedOptionReply { reply: &'static str },
}

impl ClientError {
    pub(crate) fn io(context: &'static str, source: io::Error) -> Self {
        Self::Io { context, source }
    }
}
