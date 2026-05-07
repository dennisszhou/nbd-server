//! NBD server implementation.

#![deny(unsafe_code)]

mod connection;

mod engines;
mod error;
mod export;
pub mod observability;
mod range;
mod registry;
mod server;
mod storage;
mod wal;

pub use engines::{
    CompactionOutcome, CompactionResult, CowCompactor, ExportReadView, MAX_MEMORY_EXPORT_BYTES,
    MemoryAdmissionPolicy, MemoryExportEngine, RootSnapshot, SimpleDurableAdmissionPolicy,
    SimpleDurableEngine, SimpleMutableTree, WalDurableAdmissionPolicy, WalDurableEngine,
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
    BlobStore, BlobStoreHandle, ConfiguredBlobStore, LocalBlobStore, MutableBlobStore,
    MutableBlobStoreHandle, put_random_blob,
};
pub use wal::{
    ExportWal, ExportWalHandle, LocalExportWal, LocalWalProvider, OpenWal, WalBounds, WalDomain,
    WalProvider, WalPruneResult, WalRecord, WalReplay, WalRequest,
};
