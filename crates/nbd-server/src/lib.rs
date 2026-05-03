//! NBD server implementation with an in-memory export backend.

#![forbid(unsafe_code)]

mod connection;

pub mod error;
pub mod export;
pub mod memory;
pub mod runtime;
pub mod server;

pub use error::{Result, ServerError};
pub use export::{
    Export, ExportEngine, ExportEngineHandle, ExportHandle, ExportJob, ExportReply, ExportRequest,
    ExportResult, ReplySink,
};
pub use memory::{MemoryExport, MemoryExportEngine, MAX_MEMORY_EXPORT_BYTES};
pub use runtime::{
    ExportRuntime, ExportRuntimeHandle, SerialExportRuntime, DEFAULT_EXPORT_QUEUE_CAPACITY,
};
pub use server::NbdServer;
