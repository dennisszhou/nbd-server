use nbd_protocol::ProtocolError;
use std::error::Error;
use std::fmt;
use std::io;

pub type Result<T> = std::result::Result<T, ClientError>;

#[derive(Debug)]
pub enum ClientError {
    Io {
        context: &'static str,
        source: io::Error,
    },
    Protocol {
        source: ProtocolError,
    },
    UnsupportedServerFlags {
        flags: u16,
    },
    OptionError {
        reply_type: u32,
        message: Vec<u8>,
    },
    UnexpectedOptionReply {
        reply: &'static str,
    },
}

impl ClientError {
    pub(crate) fn io(context: &'static str, source: io::Error) -> Self {
        Self::Io { context, source }
    }
}

impl From<ProtocolError> for ClientError {
    fn from(source: ProtocolError) -> Self {
        Self::Protocol { source }
    }
}

impl fmt::Display for ClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io { context, source } => write!(f, "{context}: {source}"),
            Self::Protocol { source } => write!(f, "{source}"),
            Self::UnsupportedServerFlags { flags } => {
                write!(f, "NBD server advertised unsupported flags: 0x{flags:04x}")
            }
            Self::OptionError {
                reply_type,
                message,
            } => write!(
                f,
                "NBD option negotiation failed: reply_type=0x{reply_type:08x}, message={}",
                String::from_utf8_lossy(message),
            ),
            Self::UnexpectedOptionReply { reply } => {
                write!(f, "unexpected NBD option reply: {reply}")
            }
        }
    }
}

impl Error for ClientError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io { source, .. } => Some(source),
            Self::Protocol { source } => Some(source),
            Self::UnsupportedServerFlags { .. }
            | Self::OptionError { .. }
            | Self::UnexpectedOptionReply { .. } => None,
        }
    }
}
