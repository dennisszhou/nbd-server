use crate::Result;
use std::sync::Arc;

/// Byte-oriented export boundary used by protocol handling.
#[async_trait::async_trait]
pub trait Export: Send + Sync {
    async fn read(&self, offset: u64, len: u32) -> Result<Vec<u8>>;

    async fn write(&self, offset: u64, data: &[u8]) -> Result<()>;

    async fn flush(&self) -> Result<()>;
}

pub type ExportHandle = Arc<dyn Export>;
