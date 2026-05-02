//! Error types for catalog operations.

use crate::model::ExportName;
use std::error::Error;
use std::fmt;

pub type Result<T> = std::result::Result<T, CatalogError>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CatalogError {
    InvalidCatalogUrl { url: String, reason: String },
    UnsupportedCatalogProvider { url: String, reason: String },
    InvalidExportName { name: String, reason: String },
    InvalidField { field: &'static str, reason: String },
    ExportAlreadyExists { name: ExportName },
    ExportNotFound { name: ExportName },
    ExportDeleted { name: ExportName },
    InvalidExportState { state: String },
    Database { message: String },
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
        }
    }
}

impl fmt::Display for CatalogError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidCatalogUrl { url, reason } => {
                write!(f, "invalid catalog URL `{url}`: {reason}")
            }
            Self::UnsupportedCatalogProvider { url, reason } => {
                write!(f, "unsupported catalog provider for `{url}`: {reason}")
            }
            Self::InvalidExportName { name, reason } => {
                write!(f, "invalid export name `{name}`: {reason}")
            }
            Self::InvalidField { field, reason } => {
                write!(f, "invalid {field}: {reason}")
            }
            Self::ExportAlreadyExists { name } => {
                write!(f, "export `{name}` already exists")
            }
            Self::ExportNotFound { name } => {
                write!(f, "export `{name}` not found")
            }
            Self::ExportDeleted { name } => {
                write!(f, "export `{name}` is deleted")
            }
            Self::InvalidExportState { state } => {
                write!(f, "invalid export state `{state}`")
            }
            Self::Database { message } => f.write_str(message),
        }
    }
}

impl Error for CatalogError {}
