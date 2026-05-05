//! Control-plane API for export catalog operations.

#![forbid(unsafe_code)]

pub mod catalog_url;
pub mod error;
pub mod model;
pub mod sqlite;

pub use catalog_url::{CatalogProvider, CatalogUrl};
pub use error::{CatalogError, Result};
pub use model::{
    BlobKey, ChunkIndex, CloneExport, CloneExportResult, CowChunkRef, CowTreeSnapshot,
    CreateExport, DeleteExport, ExportDescriptor, ExportEngineKind, ExportHead, ExportId,
    ExportLayoutKind, ExportMeta, ExportName, ExportState, InspectExport, ListExports, NodeId,
    PublishCompaction, PublishCompactionOutcome, SimpleChunkRef, SimpleTreeSnapshot, Timestamp,
    WalSeq, SIMPLE_CHUNK_BYTES, TREE_CHUNK_BYTES,
};
pub use sqlite::SQLiteExportCatalog;
use std::sync::Arc;

/// Runtime metadata boundary for export catalog operations.
#[async_trait::async_trait]
pub trait ExportCatalog: Send + Sync {
    async fn create_export(&self, request: CreateExport) -> Result<ExportMeta>;

    async fn clone_export(&self, request: CloneExport) -> Result<CloneExportResult>;

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

/// Opened catalog service handles for runtime consumers.
#[derive(Clone)]
pub struct CatalogHandle {
    export_catalog: Arc<dyn ExportCatalog>,
    simple_tree_store: Arc<dyn SimpleTreeMetadataStore>,
    cow_tree_store: Arc<dyn CowTreeMetadataStore>,
}

impl CatalogHandle {
    pub fn export_catalog(&self) -> Arc<dyn ExportCatalog> {
        Arc::clone(&self.export_catalog)
    }

    pub fn simple_tree_store(&self) -> Arc<dyn SimpleTreeMetadataStore> {
        Arc::clone(&self.simple_tree_store)
    }

    pub fn cow_tree_store(&self) -> Arc<dyn CowTreeMetadataStore> {
        Arc::clone(&self.cow_tree_store)
    }
}

/// Open catalog services from a runtime catalog URL.
pub async fn open_catalog(url: &CatalogUrl) -> Result<CatalogHandle> {
    match url.provider() {
        CatalogProvider::Sqlite => {
            let catalog = Arc::new(SQLiteExportCatalog::connect(url).await?);
            let export_catalog: Arc<dyn ExportCatalog> = catalog.clone();
            let simple_tree_store: Arc<dyn SimpleTreeMetadataStore> = catalog.clone();
            let cow_tree_store: Arc<dyn CowTreeMetadataStore> = catalog;

            Ok(CatalogHandle {
                export_catalog,
                simple_tree_store,
                cow_tree_store,
            })
        }
        CatalogProvider::Postgres => Err(CatalogError::unsupported_catalog_provider(
            url.as_str(),
            "Postgres catalog is not implemented",
        )),
    }
}
