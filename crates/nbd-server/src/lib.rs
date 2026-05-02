//! NBD server implementation with an in-memory export backend.

#![forbid(unsafe_code)]

mod connection;

pub mod error;
pub mod export;
pub mod memory;
pub mod server;

pub use error::{Result, ServerError};
pub use export::{Export, ExportHandle};
pub use memory::{MemoryExport, MAX_MEMORY_EXPORT_BYTES};
pub use server::NbdServer;
