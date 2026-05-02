use std::error::Error;
use std::fmt;

pub type Result<T> = std::result::Result<T, ProtocolError>;

/// Error returned when byte input does not match the supported NBD wire shape.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProtocolError {
    UnexpectedEof { needed: usize, remaining: usize },
    TrailingBytes { remaining: usize },
    MissingClientFlag { flag: &'static str },
    UnsupportedClientFlags { raw: u32, unsupported: u32 },
}

impl fmt::Display for ProtocolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnexpectedEof { needed, remaining } => write!(
                f,
                "not enough bytes to decode NBD field: needed {needed}, remaining {remaining}",
            ),
            Self::TrailingBytes { remaining } => {
                write!(f, "NBD message has {remaining} trailing byte(s)")
            }
            Self::MissingClientFlag { flag } => {
                write!(f, "NBD client did not set required flag {flag}")
            }
            Self::UnsupportedClientFlags { raw, unsupported } => write!(
                f,
                "NBD client flags include unsupported bits: raw=0x{raw:08x}, unsupported=0x{unsupported:08x}",
            ),
        }
    }
}

impl Error for ProtocolError {}
