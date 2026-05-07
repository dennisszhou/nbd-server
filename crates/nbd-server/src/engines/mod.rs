// The memory module owns the admitted unsafe byte-storage boundary.
#[allow(unsafe_code)]
mod memory;

pub use memory::{MAX_MEMORY_EXPORT_BYTES, MemoryAdmissionPolicy, MemoryExportEngine};
