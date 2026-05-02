use crate::{Export, Result, ServerError};
use nbd_control_plane::{ExportMeta, ExportName};
use std::sync::Mutex;

pub const MAX_MEMORY_EXPORT_BYTES: u64 = 64 * 1024 * 1024;

#[derive(Debug)]
pub struct MemoryExport {
    name: ExportName,
    size_bytes: u64,
    block_size: u64,
    data: Mutex<Vec<u8>>,
}

impl MemoryExport {
    pub fn new(meta: &ExportMeta) -> Result<Self> {
        let size_bytes = meta.size_bytes();
        if size_bytes > MAX_MEMORY_EXPORT_BYTES {
            return Err(ServerError::ExportTooLarge {
                name: meta.name().clone(),
                size_bytes,
                max_size_bytes: MAX_MEMORY_EXPORT_BYTES,
            });
        }

        Ok(Self {
            name: meta.name().clone(),
            size_bytes,
            block_size: meta.block_size(),
            data: Mutex::new(vec![0; size_bytes as usize]),
        })
    }

    pub fn name(&self) -> &ExportName {
        &self.name
    }

    pub fn size_bytes(&self) -> u64 {
        self.size_bytes
    }

    pub fn block_size(&self) -> u64 {
        self.block_size
    }

    fn validate_range(&self, operation: &'static str, offset: u64, length: u64) -> Result<usize> {
        let end = offset.checked_add(length).ok_or(ServerError::OutOfBounds {
            operation,
            offset,
            length,
            size_bytes: self.size_bytes,
        })?;
        if end > self.size_bytes {
            return Err(ServerError::OutOfBounds {
                operation,
                offset,
                length,
                size_bytes: self.size_bytes,
            });
        }

        Ok(offset as usize)
    }

    fn data(&self) -> Result<std::sync::MutexGuard<'_, Vec<u8>>> {
        self.data.lock().map_err(|_| ServerError::LockPoisoned {
            resource: "memory export data",
        })
    }
}

#[async_trait::async_trait]
impl Export for MemoryExport {
    async fn read(&self, offset: u64, len: u32) -> Result<Vec<u8>> {
        let start = self.validate_range("read", offset, u64::from(len))?;
        let end = start + len as usize;
        Ok(self.data()?[start..end].to_vec())
    }

    async fn write(&self, offset: u64, data: &[u8]) -> Result<()> {
        let start = self.validate_range("write", offset, data.len() as u64)?;
        let end = start + data.len();
        self.data()?[start..end].copy_from_slice(data);
        Ok(())
    }

    async fn flush(&self) -> Result<()> {
        Ok(())
    }
}
