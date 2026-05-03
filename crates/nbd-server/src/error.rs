use nbd_control_plane::ExportName;
use nbd_protocol::ProtocolError;
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
    ExportBusy {
        name: ExportName,
    },
    ExportOwnerMismatch {
        name: ExportName,
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
    RuntimeClosed {
        resource: &'static str,
    },
    Io {
        context: &'static str,
        message: String,
    },
    Protocol {
        source: ProtocolError,
    },
    Catalog {
        message: String,
    },
}

impl ServerError {
    pub(crate) fn io(context: &'static str, source: std::io::Error) -> Self {
        Self::Io {
            context,
            message: source.to_string(),
        }
    }

    pub(crate) fn catalog(source: nbd_control_plane::CatalogError) -> Self {
        Self::Catalog {
            message: source.to_string(),
        }
    }
}

impl From<ProtocolError> for ServerError {
    fn from(source: ProtocolError) -> Self {
        Self::Protocol { source }
    }
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
                "export `{name}` size {size_bytes} exceeds in-memory limit {max_size_bytes}",
            ),
            Self::ExportBusy { name } => write!(f, "export `{name}` is already active"),
            Self::ExportOwnerMismatch { name } => {
                write!(f, "export `{name}` is owned by a different active owner")
            }
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
            Self::RuntimeClosed { resource } => write!(f, "{resource} is closed"),
            Self::Io { context, message } => write!(f, "{context}: {message}"),
            Self::Protocol { source } => write!(f, "{source}"),
            Self::Catalog { message } => write!(f, "{message}"),
        }
    }
}

impl Error for ServerError {}
