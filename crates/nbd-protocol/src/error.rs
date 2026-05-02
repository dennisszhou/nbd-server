use std::error::Error;
use std::fmt;

pub type Result<T> = std::result::Result<T, ProtocolError>;

/// Error returned when byte input does not match the supported NBD wire shape.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProtocolError {
    UnexpectedEof {
        needed: usize,
        remaining: usize,
    },
    TrailingBytes {
        remaining: usize,
    },
    MissingClientFlag {
        flag: &'static str,
    },
    UnsupportedClientFlags {
        raw: u32,
        unsupported: u32,
    },
    InvalidMagic {
        context: &'static str,
        expected: u64,
        actual: u64,
    },
    InvalidUtf8 {
        field: &'static str,
    },
    InvalidString {
        field: &'static str,
        reason: &'static str,
    },
    LengthTooLarge {
        field: &'static str,
        len: usize,
        max: usize,
    },
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
            Self::InvalidMagic {
                context,
                expected,
                actual,
            } => write!(
                f,
                "invalid NBD {context} magic: expected 0x{expected:016x}, got 0x{actual:016x}",
            ),
            Self::InvalidUtf8 { field } => write!(f, "NBD {field} is not valid UTF-8"),
            Self::InvalidString { field, reason } => {
                write!(f, "invalid NBD {field}: {reason}")
            }
            Self::LengthTooLarge { field, len, max } => write!(
                f,
                "NBD {field} length {len} exceeds maximum supported length {max}",
            ),
        }
    }
}

impl Error for ProtocolError {}
