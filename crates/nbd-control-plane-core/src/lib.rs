//! Storage-neutral control-plane API for export catalog operations.

#![forbid(unsafe_code)]

pub mod error;
pub mod export;
pub mod model;
pub mod service;
pub mod tree;
pub mod tree_format;

pub use error::{CatalogError, Result};
pub use export::{
    ActiveExportDescriptor, CloneExport, CloneExportResult, CreateExport, DeleteExport,
    ExportDescriptor, ExportEngineKind, ExportHead, ExportId, ExportLayoutKind, ExportName,
    ExportRecord, ExportState, InspectExport, ListExports,
};
pub use service::{CatalogHandle, CowTreeMetadataStore, ExportCatalog, SimpleTreeMetadataStore};
pub use tree::{
    BlobKey, ChunkIndex, CowChunkRef, CowTreeSnapshot, NodeId, PublishCompaction,
    PublishCompactionOutcome, SIMPLE_CHUNK_BYTES, SimpleChunkRef, SimpleTreeSnapshot,
    TREE_CHUNK_BYTES, Timestamp, WalSeq,
};
