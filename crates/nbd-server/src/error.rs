use nbd_control_plane::ExportName;
use std::error::Error;
use std::fmt;

pub type Result<T> = std::result::Result<T, ServerError>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ServerError {
    ExportTooLarge {
        name: ExportName,
        size_bytes: u64,
        max_size_bytes: u64,
    },
    OutOfBounds {
        operation: &'static str,
        offset: u64,
        length: u64,
        size_bytes: u64,
    },
    LockPoisoned {
        resource: &'static str,
    },
}

impl fmt::Display for ServerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ExportTooLarge {
                name,
                size_bytes,
                max_size_bytes,
            } => write!(
                f,
                "export `{name}` size {size_bytes} exceeds toy in-memory limit {max_size_bytes}",
            ),
            Self::OutOfBounds {
                operation,
                offset,
                length,
                size_bytes,
            } => write!(
                f,
                "export {operation} range is out of bounds: offset={offset}, length={length}, size={size_bytes}",
            ),
            Self::LockPoisoned { resource } => write!(f, "lock poisoned for {resource}"),
        }
    }
}

impl Error for ServerError {}
