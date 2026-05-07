//! NBD server implementation.

#![deny(unsafe_code)]

mod compaction;
mod connection;

mod engines;
mod error;
mod export;
mod extent_map;
pub mod observability;
mod range;
mod read_cache;
mod registry;
mod server;
mod storage;
mod wal;
mod wal_durable;

pub use compaction::{CompactionOutcome, CompactionResult, CowCompactor};
pub use engines::{
    MAX_MEMORY_EXPORT_BYTES, MemoryAdmissionPolicy, MemoryExportEngine,
    SimpleDurableAdmissionPolicy, SimpleDurableEngine, SimpleMutableTree,
};
pub use error::{Result, ServerError};
pub use export::{
    AdmissionOp, AdmissionPermit, AdmissionTicket, AdmissionWaiter, AdmittedExportRequest,
    CompletedExport, ConcurrentExportRuntime, ConnectionId, ExportAdmissionCtl,
    ExportAdmissionPolicy, ExportAdmissionPolicyHandle, ExportCompletion, ExportEngine,
    ExportEngineHandle, ExportJob, ExportJobContext, ExportQueueSlot, ExportReply, ExportRequest,
    ExportResult, ExportRuntime, ExportRuntimeHandle, OwnedAdmittedExportRequest, RequestCookie,
    RequestSequence, SerialExportRuntime,
};
pub use range::ByteRange;
pub use registry::{ExportFactory, ExportOwner, LocalExportRegistry};
pub use server::NbdServer;
pub use storage::{
    BlobStore, BlobStoreHandle, LocalBlobStore, MutableBlobStore, MutableBlobStoreHandle,
    put_random_blob,
};
pub use wal::{
    ExportWal, ExportWalHandle, LocalExportWal, LocalWalProvider, OpenWal, WalBounds, WalDomain,
    WalProvider, WalPruneResult, WalRecord, WalReplay, WalRequest,
};
pub use wal_durable::{ExportReadView, RootSnapshot, WalDurableAdmissionPolicy, WalDurableEngine};
