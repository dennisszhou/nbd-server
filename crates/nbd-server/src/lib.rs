//! NBD server implementation with an in-memory export backend.

#![deny(unsafe_code)]

pub mod admission;
pub mod compaction;
mod connection;

pub mod error;
pub mod export;
mod extent_map;
// The memory module owns the admitted unsafe byte-storage boundary.
#[allow(unsafe_code)]
pub mod memory;
pub mod observability;
pub mod registry;
pub mod runtime;
pub mod server;
pub mod simple_durable;
mod tree_reader;
pub mod wal;
pub mod wal_durable;

pub use admission::{
    AdmissionOp, AdmissionPermit, AdmissionTicket, AdmissionWaiter, ByteRange, ExportAdmissionCtl,
};
pub use compaction::{
    CompactionEnqueueOutcome, CompactionJob, CompactionManager, CompactionOutcome,
    CompactionResult, CompactionShutdown,
};
pub use error::{Result, ServerError};
pub use export::{
    AdmittedExportRequest, CompletedExport, ExportAdmissionPolicy, ExportAdmissionPolicyHandle,
    ExportCompletion, ExportEngine, ExportEngineHandle, ExportJob, ExportReply, ExportRequest,
    ExportResult, OwnedAdmittedExportRequest,
};
pub use memory::{MemoryAdmissionPolicy, MemoryExportEngine, MAX_MEMORY_EXPORT_BYTES};
pub use observability::{ConnectionId, ExportJobContext, RequestSequence};
pub use registry::{ExportFactory, ExportOwner, LocalExportRegistry};
pub use runtime::{
    ConcurrentExportRuntime, ExportQueueSlot, ExportRuntime, ExportRuntimeHandle,
    SerialExportRuntime, DEFAULT_EXPORT_QUEUE_CAPACITY,
};
pub use server::NbdServer;
pub use simple_durable::{
    LocalBlobStore, SimpleDurableAdmissionPolicy, SimpleDurableEngine, SimpleMutableTree,
};
pub use wal::{
    ExportWal, ExportWalHandle, LocalExportWal, LocalWalProvider, OpenWal, WalBounds, WalDomain,
    WalProvider, WalPruneResult, WalRecord, WalReplay, WalRequest,
};
pub use wal_durable::{ExportReadView, RootSnapshot, WalDurableAdmissionPolicy, WalDurableEngine};
