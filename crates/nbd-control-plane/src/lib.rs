//! Control-plane API for export catalog operations.

#![forbid(unsafe_code)]

pub mod catalog_url;
pub mod error;
pub mod model;

pub use catalog_url::{CatalogProvider, CatalogUrl};
pub use error::{CatalogError, Result};
pub use model::{
    ActiveExportDescriptor, BlobKey, ChunkIndex, CloneExport, CloneExportResult, CowChunkRef,
    CowTreeSnapshot, CreateExport, DeleteExport, ExportDescriptor, ExportEngineKind, ExportHead,
    ExportId, ExportLayoutKind, ExportName, ExportRecord, ExportState, InspectExport, ListExports,
    NodeId, PublishCompaction, PublishCompactionOutcome, PublishTreeUpdate,
    PublishTreeUpdateOutcome, SIMPLE_CHUNK_BYTES, SimpleChunkRef, SimpleTreeSnapshot,
    TREE_CHUNK_BYTES, Timestamp, TreeEdgeLookup, TreeEdgeRecord, TreeFormat, TreeLeafRefRecord,
    TreeNodeKind, TreeNodeRecord, TreeRecordBatch, TreeStorageKind, WalSeq,
};
pub use nbd_control_plane_core::{
    CatalogHandle, CowTreeMetadataStore, ExportCatalog, SimpleTreeMetadataStore, TreeRecordStore,
};
pub use nbd_control_plane_sqlite::SQLiteExportCatalog;
use std::sync::Arc;

/// Open catalog services from a runtime catalog URL.
pub async fn open_catalog(url: &CatalogUrl) -> Result<CatalogHandle> {
    match url.provider() {
        CatalogProvider::Sqlite => {
            let catalog = Arc::new(SQLiteExportCatalog::connect_path(url.sqlite_path()?).await?);
            let export_catalog: Arc<dyn ExportCatalog> = catalog.clone();
            let simple_tree_store: Arc<dyn SimpleTreeMetadataStore> = catalog.clone();
            let cow_tree_store: Arc<dyn CowTreeMetadataStore> = catalog.clone();
            let tree_record_store: Arc<dyn TreeRecordStore> = catalog;

            Ok(CatalogHandle::new(
                export_catalog,
                simple_tree_store,
                cow_tree_store,
                tree_record_store,
            ))
        }
        CatalogProvider::Postgres => Err(CatalogError::unsupported_catalog_provider(
            url.as_str(),
            "Postgres catalog is not implemented",
        )),
    }
}
