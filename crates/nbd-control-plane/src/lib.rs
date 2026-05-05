//! Control-plane API for export catalog operations.

#![forbid(unsafe_code)]

pub mod catalog_url;
pub mod error;
pub mod model;
pub mod sqlite;

pub use catalog_url::{CatalogProvider, CatalogUrl};
pub use error::{CatalogError, Result};
pub use model::{
    BlobKey, ChunkIndex, CowChunkRef, CowTreeSnapshot, CreateExport, DeleteExport,
    ExportDescriptor, ExportEngineKind, ExportHead, ExportId, ExportLayoutKind, ExportMeta,
    ExportName, ExportState, InspectExport, ListExports, NodeId, PublishCompaction,
    PublishCompactionOutcome, SimpleChunkRef, SimpleTreeSnapshot, Timestamp, WalSeq,
    SIMPLE_CHUNK_BYTES, TREE_CHUNK_BYTES,
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

    /// Load exports-only metadata for serving/open paths.
    ///
    /// Implementations must reject deleted exports. Storage engines must load
    /// the latest serving head or tree snapshot separately.
    async fn load_export_descriptor(&self, name: ExportName) -> Result<ExportDescriptor>;

    /// Load the latest serving head for an export.
    async fn load_export_head(&self, export_id: &ExportId) -> Result<ExportHead>;

    /// Inspect an export for operator visibility.
    ///
    /// Unlike `load_export`, this may return deleted exports.
    async fn inspect_export(&self, request: InspectExport) -> Result<ExportMeta>;

    async fn list_exports(&self, request: ListExports) -> Result<Vec<ExportMeta>>;
}

#[async_trait::async_trait]
pub trait SimpleTreeMetadataStore: Send + Sync {
    async fn load_simple_tree(&self, export_id: &ExportId) -> Result<SimpleTreeSnapshot>;

    async fn commit_simple_chunks(
        &self,
        export_id: &ExportId,
        chunks: Vec<SimpleChunkRef>,
    ) -> Result<SimpleTreeSnapshot>;
}

#[async_trait::async_trait]
pub trait CowTreeMetadataStore: Send + Sync {
    async fn load_cow_tree(&self, export_id: &ExportId) -> Result<CowTreeSnapshot>;

    async fn publish_compaction(
        &self,
        request: PublishCompaction,
    ) -> Result<PublishCompactionOutcome>;
}
