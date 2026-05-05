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
    Wal {
        context: &'static str,
        message: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RequestFailureLogLevel {
    Debug,
    Warn,
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

    pub(crate) fn wal(context: &'static str, message: impl Into<String>) -> Self {
        Self::Wal {
            context,
            message: message.into(),
        }
    }

    pub(crate) fn request_failure_log_level(&self) -> RequestFailureLogLevel {
        match self {
            Self::OutOfBounds { .. } | Self::Protocol { .. } => RequestFailureLogLevel::Debug,
            Self::ExportTooLarge { .. }
            | Self::ExportBusy { .. }
            | Self::ExportOwnerMismatch { .. }
            | Self::LockPoisoned { .. }
            | Self::RuntimeClosed { .. }
            | Self::Io { .. }
            | Self::Catalog { .. }
            | Self::Wal { .. } => RequestFailureLogLevel::Warn,
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
            Self::Wal { context, message } => write!(f, "{context}: {message}"),
        }
    }
}

impl Error for ServerError {}

#[cfg(test)]
mod tests {
    use super::*;
    use nbd_protocol::ProtocolError;

    #[test]
    fn expected_client_request_errors_are_debug() {
        assert_eq!(
            ServerError::OutOfBounds {
                operation: "read",
                offset: 4096,
                length: 4096,
                size_bytes: 4096,
            }
            .request_failure_log_level(),
            RequestFailureLogLevel::Debug,
        );
        assert_eq!(
            ServerError::Protocol {
                source: ProtocolError::InvalidMagic {
                    context: "test",
                    expected: 1,
                    actual: 2,
                },
            }
            .request_failure_log_level(),
            RequestFailureLogLevel::Debug,
        );
    }

    #[test]
    fn server_side_request_errors_are_warn() {
        assert_eq!(
            ServerError::RuntimeClosed {
                resource: "connection reply queue",
            }
            .request_failure_log_level(),
            RequestFailureLogLevel::Warn,
        );
        assert_eq!(
            ServerError::Io {
                context: "write reply",
                message: "broken pipe".to_owned(),
            }
            .request_failure_log_level(),
            RequestFailureLogLevel::Warn,
        );
    }
}
