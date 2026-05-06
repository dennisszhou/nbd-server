//! NBD server implementation with an in-memory export backend.

#![deny(unsafe_code)]

mod admission;
mod compaction;
mod connection;

mod error;
mod export;
mod extent_map;
// The memory module owns the admitted unsafe byte-storage boundary.
#[allow(unsafe_code)]
mod memory;
pub mod observability;
mod read_cache;
mod registry;
mod runtime;
mod server;
mod simple_durable;
mod tree_reader;
mod wal;
mod wal_durable;

pub use admission::{
    AdmissionOp, AdmissionPermit, AdmissionTicket, AdmissionWaiter, ByteRange, ExportAdmissionCtl,
};
pub use compaction::{CompactionOutcome, CompactionResult, CowCompactor};
pub use error::{Result, ServerError};
pub use export::{
    AdmittedExportRequest, CompletedExport, ExportAdmissionPolicy, ExportAdmissionPolicyHandle,
    ExportCompletion, ExportEngine, ExportEngineHandle, ExportJob, ExportReply, ExportRequest,
    ExportResult, OwnedAdmittedExportRequest,
};
pub use memory::{MAX_MEMORY_EXPORT_BYTES, MemoryAdmissionPolicy, MemoryExportEngine};
pub use observability::{ConnectionId, ExportJobContext, RequestSequence};
pub use registry::{ExportFactory, ExportOwner, LocalExportRegistry};
pub use runtime::{
    ConcurrentExportRuntime, DEFAULT_EXPORT_QUEUE_CAPACITY, ExportQueueSlot, ExportRuntime,
    ExportRuntimeHandle, SerialExportRuntime,
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
