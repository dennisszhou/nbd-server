//! Storage-neutral control-plane API for export catalog operations.

#![forbid(unsafe_code)]

pub mod diagnostics;
pub mod error;
pub mod export;
pub mod model;
pub mod service;
pub mod tree;
pub mod tree_format;

pub use diagnostics::{CatalogDoctorCheck, CatalogDoctorStatus};
pub use error::{CatalogError, Result};
pub use export::{
    ActiveExportDescriptor, CloneExport, CloneExportResult, CreateExport, DeleteExport,
    ExportDescriptor, ExportEngineKind, ExportHead, ExportId, ExportLayoutKind, ExportName,
    ExportRecord, ExportState, InspectExport, ListExports,
};
pub use service::{CatalogHandle, ExportCatalog, TreeRecordStore};
pub use tree::{
    BlobKey, ChunkIndex, CowChunkRef, NodeId, PublishTreeUpdate, PublishTreeUpdateOutcome,
    SIMPLE_CHUNK_BYTES, SimpleChunkRef, TREE_CHUNK_BYTES, Timestamp, TreeEdgeLookup,
    TreeEdgeRecord, TreeLeafRefRecord, TreeNodeKind, TreeNodeRecord, TreeRecordBatch,
    TreeStorageKind, WalSeq,
};
pub use tree_format::TreeFormat;
