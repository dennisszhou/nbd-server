//! Control-plane API for export catalog operations.

#![forbid(unsafe_code)]

pub mod catalog_url;
pub mod error;
pub mod model;
pub mod sqlite;

pub use catalog_url::{CatalogProvider, CatalogUrl};
pub use error::{CatalogError, Result};
pub use model::{
    CommittedRoot, CreateExport, DeleteExport, ExportEngineKind, ExportGeneration, ExportId,
    ExportMeta, ExportName, ExportState, InspectExport, ListExports, NodeId, Timestamp, WalSeq,
};
pub use sqlite::SQLiteExportCatalog;

/// Runtime metadata boundary for export catalog operations.
#[async_trait::async_trait]
pub trait ExportCatalog: Send + Sync {
    async fn create_export(&self, request: CreateExport) -> Result<ExportMeta>;

    async fn delete_export(&self, request: DeleteExport) -> Result<()>;

    /// Load an export for serving/open paths.
    ///
    /// Implementations must reject deleted exports.
    async fn load_export(&self, name: ExportName) -> Result<ExportMeta>;

    /// Inspect an export for operator visibility.
    ///
    /// Unlike `load_export`, this may return deleted exports.
    async fn inspect_export(&self, request: InspectExport) -> Result<ExportMeta>;

    async fn list_exports(&self, request: ListExports) -> Result<Vec<ExportMeta>>;
}
