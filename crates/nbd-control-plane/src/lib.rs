//! Control-plane API for export catalog operations.

#![forbid(unsafe_code)]

pub mod catalog_url;
pub mod error;
pub mod model;

pub use catalog_url::{CatalogProvider, CatalogUrl};
pub use error::{CatalogError, Result};
pub use model::{
    ActiveExportDescriptor, BlobKey, ChunkIndex, CloneExport, CloneExportResult, CowChunkRef,
    CreateExport, DeleteExport, ExportDescriptor, ExportEngineKind, ExportHead, ExportId,
    ExportLayoutKind, ExportName, ExportRecord, ExportState, InspectExport, ListExports, NodeId,
    PublishTreeUpdate, PublishTreeUpdateOutcome, SIMPLE_CHUNK_BYTES, SimpleChunkRef,
    TREE_CHUNK_BYTES, Timestamp, TreeEdgeLookup, TreeEdgeRecord, TreeFormat, TreeLeafRefRecord,
    TreeNodeKind, TreeNodeRecord, TreeRecordBatch, TreeStorageKind, WalSeq,
};
pub use nbd_control_plane_core::{
    CatalogDoctorCheck, CatalogDoctorStatus, CatalogHandle, ExportCatalog, TreeRecordStore,
};
use nbd_control_plane_sqlite::SQLiteExportCatalog;
use std::sync::Arc;

/// Open catalog services from a runtime catalog URL.
pub async fn open_catalog(url: &CatalogUrl) -> Result<CatalogHandle> {
    match url.provider() {
        CatalogProvider::Sqlite => {
            let catalog = Arc::new(SQLiteExportCatalog::connect_path(url.sqlite_path()?).await?);
            let export_catalog: Arc<dyn ExportCatalog> = catalog.clone();
            let tree_record_store: Arc<dyn TreeRecordStore> = catalog;

            Ok(CatalogHandle::new(export_catalog, tree_record_store))
        }
        CatalogProvider::Postgres => Err(CatalogError::unsupported_catalog_provider(
            url.as_str(),
            "Postgres catalog is not implemented",
        )),
    }
}

/// Run provider-specific catalog diagnostics without creating catalog state.
pub async fn doctor_catalog(url: &CatalogUrl) -> Vec<CatalogDoctorCheck> {
    match url.provider() {
        CatalogProvider::Sqlite => {
            let mut checks = vec![CatalogDoctorCheck::ok("catalog_provider", "sqlite")];
            match url.sqlite_path() {
                Ok(path) => checks.extend(SQLiteExportCatalog::doctor_path(path).await),
                Err(error) => checks.push(CatalogDoctorCheck::failed(
                    "catalog_open",
                    error.to_string(),
                    "set catalog.url to a file-backed SQLite catalog",
                )),
            }
            checks
        }
        CatalogProvider::Postgres => vec![CatalogDoctorCheck::failed(
            "catalog_provider",
            url.provider().as_str(),
            "Postgres catalog is not implemented",
        )],
    }
}
