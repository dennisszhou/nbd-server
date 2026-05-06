use thiserror::Error;

pub type Result<T> = std::result::Result<T, ProtocolError>;

/// Error returned when byte input does not match the supported NBD wire shape.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ProtocolError {
    #[error("not enough bytes to decode NBD field: needed {needed}, remaining {remaining}")]
    UnexpectedEof { needed: usize, remaining: usize },
    #[error("NBD message has {remaining} trailing byte(s)")]
    TrailingBytes { remaining: usize },
    #[error("NBD client did not set required flag {flag}")]
    MissingClientFlag { flag: &'static str },
    #[error(
        "NBD client flags include unsupported bits: raw=0x{raw:08x}, unsupported=0x{unsupported:08x}"
    )]
    UnsupportedClientFlags { raw: u32, unsupported: u32 },
    #[error("invalid NBD {context} magic: expected 0x{expected:016x}, got 0x{actual:016x}")]
    InvalidMagic {
        context: &'static str,
        expected: u64,
        actual: u64,
    },
    #[error("NBD {field} is not valid UTF-8")]
    InvalidUtf8 { field: &'static str },
    #[error("invalid NBD {field}: {reason}")]
    InvalidString {
        field: &'static str,
        reason: &'static str,
    },
    #[error("NBD {field} length {len} exceeds maximum supported length {max}")]
    LengthTooLarge {
        field: &'static str,
        len: usize,
        max: usize,
    },
    #[error("NBD command flags are unsupported: raw=0x{raw:04x}")]
    UnsupportedCommandFlags { raw: u16 },
    #[error("NBD command type {command} is not supported")]
    UnsupportedCommand { command: u16 },
    #[error("invalid NBD {command} request: {reason}")]
    InvalidRequest {
        command: &'static str,
        reason: &'static str,
    },
    #[error("invalid NBD {reply} reply: {reason}")]
    InvalidReply {
        reply: &'static str,
        reason: &'static str,
    },
    #[error("NBD request range overflows u64: offset={offset}, length={length}")]
    LengthOverflow { offset: u64, length: u32 },
}
