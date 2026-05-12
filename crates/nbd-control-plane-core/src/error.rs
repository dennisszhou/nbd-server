//! Error types for catalog operations.

use crate::model::ExportName;
use std::error::Error as StdError;
use std::sync::Arc;
use thiserror::Error;

pub type Result<T> = std::result::Result<T, CatalogError>;

#[derive(Debug, Clone, Error)]
pub enum CatalogError {
    #[error("invalid catalog URL `{url}`: {reason}")]
    InvalidCatalogUrl { url: String, reason: String },
    #[error("unsupported catalog provider for `{url}`: {reason}")]
    UnsupportedCatalogProvider { url: String, reason: String },
    #[error("invalid export name `{name}`: {reason}")]
    InvalidExportName { name: String, reason: String },
    #[error("invalid {field}: {reason}")]
    InvalidField { field: &'static str, reason: String },
    #[error("export `{name}` already exists")]
    ExportAlreadyExists { name: ExportName },
    #[error("export `{name}` not found")]
    ExportNotFound { name: ExportName },
    #[error("export `{name}` is deleted")]
    ExportDeleted { name: ExportName },
    #[error("invalid export state `{state}`")]
    InvalidExportState { state: String },
    #[error("invalid export engine kind `{engine_kind}`")]
    InvalidExportEngineKind { engine_kind: String },
    #[error("{message}")]
    Database {
        message: String,
        #[source]
        source: Option<Arc<dyn StdError + Send + Sync>>,
    },
}

impl CatalogError {
    pub fn invalid_catalog_url(url: impl Into<String>, reason: impl Into<String>) -> Self {
        Self::InvalidCatalogUrl {
            url: url.into(),
            reason: reason.into(),
        }
    }

    pub fn unsupported_catalog_provider(url: impl Into<String>, reason: impl Into<String>) -> Self {
        Self::UnsupportedCatalogProvider {
            url: url.into(),
            reason: reason.into(),
        }
    }

    pub fn invalid_export_name(name: impl Into<String>, reason: impl Into<String>) -> Self {
        Self::InvalidExportName {
            name: name.into(),
            reason: reason.into(),
        }
    }

    pub fn invalid_field(field: &'static str, reason: impl Into<String>) -> Self {
        Self::InvalidField {
            field,
            reason: reason.into(),
        }
    }

    pub fn database(message: impl Into<String>) -> Self {
        Self::Database {
            message: message.into(),
            source: None,
        }
    }

    pub fn database_source(
        message: impl Into<String>,
        source: impl StdError + Send + Sync + 'static,
    ) -> Self {
        Self::Database {
            message: message.into(),
            source: Some(Arc::new(source)),
        }
    }
}
