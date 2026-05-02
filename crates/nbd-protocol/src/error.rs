use std::error::Error;
use std::fmt;

pub type Result<T> = std::result::Result<T, ProtocolError>;

/// Error returned when byte input does not match the supported NBD wire shape.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProtocolError {
    UnexpectedEof { needed: usize, remaining: usize },
}

impl fmt::Display for ProtocolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnexpectedEof { needed, remaining } => write!(
                f,
                "not enough bytes to decode NBD field: needed {needed}, remaining {remaining}",
            ),
        }
    }
}

impl Error for ProtocolError {}
