//! NBD server implementation with an in-memory export backend.

#![forbid(unsafe_code)]

pub mod admission;
mod connection;

pub mod error;
pub mod export;
pub mod memory;
pub mod registry;
pub mod runtime;
pub mod server;

pub use admission::{
    AdmissionOp, AdmissionPermit, AdmissionTicket, AdmissionWaiter, ByteRange, ExportAdmissionCtl,
};
pub use error::{Result, ServerError};
pub use export::{
    CompletedExport, Export, ExportCompletion, ExportEngine, ExportEngineHandle, ExportHandle,
    ExportJob, ExportReply, ExportRequest, ExportResult,
};
pub use memory::{MemoryExport, MemoryExportEngine, MAX_MEMORY_EXPORT_BYTES};
pub use registry::{ExportOwner, LocalExportRegistry};
pub use runtime::{
    ExportQueueSlot, ExportRuntime, ExportRuntimeHandle, SerialExportRuntime,
    DEFAULT_EXPORT_QUEUE_CAPACITY,
};
pub use server::NbdServer;
