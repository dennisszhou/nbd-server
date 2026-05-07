// The memory module owns the admitted unsafe byte-storage boundary.
#[allow(unsafe_code)]
mod memory;
mod simple_durable;
pub(crate) mod tree;

pub use memory::{MAX_MEMORY_EXPORT_BYTES, MemoryAdmissionPolicy, MemoryExportEngine};
pub use simple_durable::{SimpleDurableAdmissionPolicy, SimpleDurableEngine, SimpleMutableTree};
