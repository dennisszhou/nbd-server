//! NBD server implementation.

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
mod range;
mod read_cache;
mod registry;
mod runtime;
mod server;
mod simple_durable;
mod storage;
mod tree_reader;
mod wal;
mod wal_durable;

pub use admission::{
    AdmissionOp, AdmissionPermit, AdmissionTicket, AdmissionWaiter, ExportAdmissionCtl,
};
pub use compaction::{CompactionOutcome, CompactionResult, CowCompactor};
pub use error::{Result, ServerError};
pub use export::{
    AdmittedExportRequest, CompletedExport, ConnectionId, ExportAdmissionPolicy,
    ExportAdmissionPolicyHandle, ExportCompletion, ExportEngine, ExportEngineHandle, ExportJob,
    ExportJobContext, ExportReply, ExportRequest, ExportResult, OwnedAdmittedExportRequest,
    RequestCookie, RequestSequence,
};
pub use memory::{MAX_MEMORY_EXPORT_BYTES, MemoryAdmissionPolicy, MemoryExportEngine};
pub use range::ByteRange;
pub use registry::{ExportFactory, ExportOwner, LocalExportRegistry};
pub use runtime::{
    ConcurrentExportRuntime, ExportQueueSlot, ExportRuntime, ExportRuntimeHandle,
    SerialExportRuntime,
};
pub use server::NbdServer;
pub use simple_durable::{SimpleDurableAdmissionPolicy, SimpleDurableEngine, SimpleMutableTree};
pub use storage::{
    BlobStore, BlobStoreHandle, LocalBlobStore, MutableBlobStore, MutableBlobStoreHandle,
    put_random_blob,
};
pub use wal::{
    ExportWal, ExportWalHandle, LocalExportWal, LocalWalProvider, OpenWal, WalBounds, WalDomain,
    WalProvider, WalPruneResult, WalRecord, WalReplay, WalRequest,
};
pub use wal_durable::{ExportReadView, RootSnapshot, WalDurableAdmissionPolicy, WalDurableEngine};
