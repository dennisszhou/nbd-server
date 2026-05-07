use nbd_control_plane::{BlobKey, ExportName};
use nbd_protocol::ProtocolError;
use std::sync::Arc;
use thiserror::Error;

pub type Result<T> = std::result::Result<T, ServerError>;

#[derive(Debug, Clone, Error)]
pub enum ServerError {
    #[error("export `{name}` size {size_bytes} exceeds in-memory limit {max_size_bytes}")]
    ExportTooLarge {
        name: ExportName,
        size_bytes: u64,
        max_size_bytes: u64,
    },
    #[error("export `{name}` is already active")]
    ExportBusy { name: ExportName },
    #[error("export `{name}` is owned by a different active owner")]
    ExportOwnerMismatch { name: ExportName },
    #[error(
        "export {operation} range is out of bounds: offset={offset}, length={length}, size={size_bytes}"
    )]
    OutOfBounds {
        operation: &'static str,
        offset: u64,
        length: u64,
        size_bytes: u64,
    },
    #[error("lock poisoned for {resource}")]
    LockPoisoned { resource: &'static str },
    #[error("{resource} is closed")]
    RuntimeClosed { resource: &'static str },
    #[error("{context}: blob `{key}` already exists")]
    BlobAlreadyExists { context: &'static str, key: BlobKey },
    #[error("{context}: {message}")]
    Io {
        context: &'static str,
        message: String,
        #[source]
        source: Option<Arc<std::io::Error>>,
    },
    #[error("{source}")]
    Protocol {
        #[from]
        source: ProtocolError,
    },
    #[error("{message}")]
    Catalog {
        message: String,
        #[source]
        source: Option<nbd_control_plane::CatalogError>,
    },
    #[error("{context}: {message}")]
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
            source: Some(Arc::new(source)),
        }
    }

    pub(crate) fn catalog(source: nbd_control_plane::CatalogError) -> Self {
        Self::Catalog {
            message: source.to_string(),
            source: Some(source),
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
            | Self::BlobAlreadyExists { .. }
            | Self::Io { .. }
            | Self::Catalog { .. }
            | Self::Wal { .. } => RequestFailureLogLevel::Warn,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nbd_protocol::ProtocolError;
    use std::error::Error as _;

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
                source: None,
            }
            .request_failure_log_level(),
            RequestFailureLogLevel::Warn,
        );
    }

    #[test]
    fn wrapped_errors_expose_sources() {
        let io = ServerError::io(
            "read test file",
            std::io::Error::new(std::io::ErrorKind::NotFound, "missing"),
        );
        assert!(io.source().is_some());
        assert!(io.clone().source().is_some());

        let catalog = ServerError::catalog(nbd_control_plane::CatalogError::invalid_field(
            "test",
            "bad value",
        ));
        assert!(catalog.source().is_some());
        assert!(catalog.clone().source().is_some());
    }
}
