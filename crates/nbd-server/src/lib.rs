//! NBD server implementation with an in-memory export backend.

#![deny(unsafe_code)]

pub mod admission;
mod connection;

pub mod error;
pub mod export;
// The memory module owns the admitted unsafe byte-storage boundary.
#[allow(unsafe_code)]
pub mod memory;
pub mod registry;
pub mod runtime;
pub mod server;

pub use admission::{
    AdmissionOp, AdmissionPermit, AdmissionTicket, AdmissionWaiter, ByteRange, ExportAdmissionCtl,
};
pub use error::{Result, ServerError};
pub use export::{
    AdmittedExportRequest, CompletedExport, ExportAdmissionPolicy, ExportAdmissionPolicyHandle,
    ExportCompletion, ExportEngine, ExportEngineHandle, ExportJob, ExportReply, ExportRequest,
    ExportResult,
};
pub use memory::{MemoryAdmissionPolicy, MemoryExportEngine, MAX_MEMORY_EXPORT_BYTES};
pub use registry::{ExportOwner, LocalExportRegistry};
pub use runtime::{
    ConcurrentExportRuntime, ExportQueueSlot, ExportRuntime, ExportRuntimeHandle,
    SerialExportRuntime, DEFAULT_EXPORT_QUEUE_CAPACITY,
};
pub use server::NbdServer;
