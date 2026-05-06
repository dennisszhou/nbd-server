use crate::{
    AdmissionOp, AdmittedExportRequest, ByteRange, ExportAdmissionPolicy,
    ExportAdmissionPolicyHandle, ExportEngine, ExportReply, ExportRequest, ExportResult, Result,
    ServerError,
};
use nbd_control_plane::{ExportDescriptor, ExportHead, ExportLayoutKind, ExportName, ExportRecord};
use std::cell::UnsafeCell;
use std::fmt;
use std::ptr;
use std::sync::Arc;

pub const MAX_MEMORY_EXPORT_BYTES: u64 = 64 * 1024 * 1024;

#[derive(Debug)]
pub struct MemoryExportEngine {
    name: ExportName,
    size_bytes: u64,
    block_size: u64,
    data: MemoryStorage,
}

struct MemoryStorage {
    bytes: Box<[UnsafeCell<u8>]>,
}

// SAFETY: MemoryStorage is only accessed through admitted export requests.
// ExportAdmissionCtl guarantees that concurrently active reads and writes do
// not conflict by byte range, and MemoryExportEngine never resizes storage.
unsafe impl Sync for MemoryStorage {}

#[derive(Debug)]
pub struct MemoryAdmissionPolicy {
    size_bytes: u64,
}

impl MemoryAdmissionPolicy {
    pub fn new(size_bytes: u64) -> Self {
        Self { size_bytes }
    }
}

impl ExportAdmissionPolicy for MemoryAdmissionPolicy {
    fn operation_for(&self, request: &ExportRequest) -> Result<AdmissionOp> {
        match request {
            ExportRequest::Read { offset, len } => {
                Ok(AdmissionOp::Read(ByteRange::new(*offset, *len)))
            }
            ExportRequest::Write { offset, data } => {
                let len = u32::try_from(data.len()).map_err(|_| ServerError::OutOfBounds {
                    operation: "write",
                    offset: *offset,
                    length: u64::try_from(data.len()).unwrap_or(u64::MAX),
                    size_bytes: self.size_bytes,
                })?;
                Ok(AdmissionOp::Write(ByteRange::new(*offset, len)))
            }
            ExportRequest::Flush => Ok(AdmissionOp::Flush),
        }
    }
}

impl MemoryExportEngine {
    pub fn new(meta: &ExportRecord) -> Result<Self> {
        Self::from_export_head(meta.name().clone(), meta.block_size(), meta.head())
    }

    pub fn from_descriptor(descriptor: &ExportDescriptor, head: &ExportHead) -> Result<Self> {
        Self::from_export_head(descriptor.name().clone(), descriptor.block_size(), head)
    }

    fn from_export_head(name: ExportName, block_size: u64, head: &ExportHead) -> Result<Self> {
        if head.layout_kind() != ExportLayoutKind::MemoryEmpty {
            return Err(ServerError::Catalog {
                message: format!("export `{name}` does not have a memory_empty head"),
            });
        }
        let size_bytes = head.size_bytes();
        if size_bytes > MAX_MEMORY_EXPORT_BYTES {
            return Err(ServerError::ExportTooLarge {
                name,
                size_bytes,
                max_size_bytes: MAX_MEMORY_EXPORT_BYTES,
            });
        }

        Ok(Self {
            name,
            size_bytes,
            block_size,
            data: MemoryStorage::new(size_bytes as usize),
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

    fn read(&self, offset: u64, len: u32) -> Result<Vec<u8>> {
        let start = self.validate_range("read", offset, u64::from(len))?;
        Ok(self.data.read(start, len as usize))
    }

    fn write(&self, offset: u64, data: &[u8]) -> Result<()> {
        let start = self.validate_range("write", offset, data.len() as u64)?;
        self.data.write(start, data);
        Ok(())
    }

    fn flush(&self) -> Result<()> {
        Ok(())
    }
}

impl MemoryStorage {
    fn new(size_bytes: usize) -> Self {
        let mut bytes = Vec::with_capacity(size_bytes);
        bytes.resize_with(size_bytes, || UnsafeCell::new(0));
        Self {
            bytes: bytes.into_boxed_slice(),
        }
    }

    fn read(&self, start: usize, len: usize) -> Vec<u8> {
        let mut data = vec![0; len];
        if len == 0 {
            return data;
        }

        // SAFETY: validate_range checked that start..start+len is in bounds.
        // Admission ensures no active writer overlaps this read range. Each
        // UnsafeCell<u8> has u8 layout, and the boxed slice is contiguous.
        unsafe {
            let source = self.bytes.as_ptr().cast::<u8>().add(start);
            ptr::copy_nonoverlapping(source, data.as_mut_ptr(), len);
        }
        data
    }

    fn write(&self, start: usize, data: &[u8]) {
        if data.is_empty() {
            return;
        }

        // SAFETY: validate_range checked that start..start+data.len() is in
        // bounds. Admission ensures no active reader or writer overlaps this
        // write range. UnsafeCell<u8> has u8 layout, and the boxed slice is
        // contiguous.
        unsafe {
            let target = self.bytes.as_ptr().cast::<u8>().add(start).cast_mut();
            ptr::copy_nonoverlapping(data.as_ptr(), target, data.len());
        }
    }
}

impl fmt::Debug for MemoryStorage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MemoryStorage")
            .field("len", &self.bytes.len())
            .finish()
    }
}

#[async_trait::async_trait]
impl ExportEngine for MemoryExportEngine {
    fn admission_policy(&self) -> ExportAdmissionPolicyHandle {
        Arc::new(MemoryAdmissionPolicy::new(self.size_bytes))
    }

    async fn execute_admitted(&self, request: AdmittedExportRequest) -> ExportResult {
        match request.request() {
            ExportRequest::Read { offset, len } => Ok(ExportReply::Read {
                data: self.read(*offset, *len)?,
            }),
            ExportRequest::Write { offset, data } => {
                self.write(*offset, data)?;
                Ok(ExportReply::Done)
            }
            ExportRequest::Flush => {
                self.flush()?;
                Ok(ExportReply::Done)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{AdmissionPermit, ExportAdmissionCtl, ExportEngine, ExportJobContext};
    use nbd_control_plane::{ExportEngineKind, ExportHead, ExportId, ExportState, Timestamp};
    use nbd_protocol::wire::NbdCookie;
    use std::sync::Arc;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn admitted_non_overlapping_writes_can_share_storage() {
        let meta = export_record("disk-a", 4096);
        let engine = Arc::new(MemoryExportEngine::new(&meta).expect("memory export"));
        let admission = ExportAdmissionCtl::new(meta.size_bytes());
        let policy = engine.admission_policy();

        let left_request = ExportRequest::Write {
            offset: 0,
            data: b"aaaa".to_vec(),
        };
        let right_request = ExportRequest::Write {
            offset: 4,
            data: b"bbbb".to_vec(),
        };
        let left_permit = admission
            .register(
                policy
                    .operation_for(&left_request)
                    .expect("left admission op"),
            )
            .expect("left admission")
            .wait()
            .await
            .expect("left permit");
        let right_permit = admission
            .register(
                policy
                    .operation_for(&right_request)
                    .expect("right admission op"),
            )
            .expect("right admission")
            .wait()
            .await
            .expect("right permit");

        let left_engine = engine.clone();
        let left = tokio::spawn(async move {
            left_engine
                .execute_admitted(admitted(left_request, left_permit))
                .await
        });
        let right_engine = engine.clone();
        let right = tokio::spawn(async move {
            right_engine
                .execute_admitted(admitted(right_request, right_permit))
                .await
        });

        assert_eq!(
            left.await.expect("left task").expect("left write"),
            ExportReply::Done
        );
        assert_eq!(
            right.await.expect("right task").expect("right write"),
            ExportReply::Done,
        );

        let read_request = ExportRequest::Read { offset: 0, len: 8 };
        let read_permit = admission
            .register(
                policy
                    .operation_for(&read_request)
                    .expect("read admission op"),
            )
            .expect("read admission")
            .wait()
            .await
            .expect("read permit");
        assert_eq!(
            engine
                .execute_admitted(admitted(read_request, read_permit))
                .await
                .expect("read"),
            ExportReply::Read {
                data: b"aaaabbbb".to_vec(),
            },
        );
    }

    fn admitted(request: ExportRequest, permit: AdmissionPermit) -> AdmittedExportRequest {
        let context = ExportJobContext::internal(NbdCookie::new(0), request.command_name());
        AdmittedExportRequest::new(request, permit, context)
    }

    #[tokio::test]
    async fn memory_policy_blocks_overlapping_requests_in_admission() {
        let meta = export_record("disk-a", 4096);
        let engine = MemoryExportEngine::new(&meta).expect("memory export");
        let admission = ExportAdmissionCtl::new(meta.size_bytes());
        let policy = engine.admission_policy();

        let write_request = ExportRequest::Write {
            offset: 0,
            data: b"aaaa".to_vec(),
        };
        let write_permit = admission
            .register(
                policy
                    .operation_for(&write_request)
                    .expect("write admission op"),
            )
            .expect("write admission")
            .wait()
            .await
            .expect("write permit");
        let read_request = ExportRequest::Read { offset: 1, len: 2 };
        let read_waiter = admission
            .register(
                policy
                    .operation_for(&read_request)
                    .expect("read admission op"),
            )
            .expect("read admission");
        let pending_read = tokio::spawn(async move { read_waiter.wait().await });

        tokio::task::yield_now().await;
        assert!(
            !pending_read.is_finished(),
            "overlapping read should wait for the active write permit",
        );

        drop(write_permit);
        pending_read
            .await
            .expect("read waiter task")
            .expect("read permit");
    }

    fn export_record(name: &str, size_bytes: u64) -> ExportRecord {
        ExportRecord::new(
            ExportId::new(format!("export-{name}")).expect("export id"),
            ExportName::new(name).expect("export name"),
            4096,
            ExportEngineKind::Memory,
            ExportState::Active,
            ExportHead::memory_empty(size_bytes).expect("memory head"),
            Timestamp::new("created").expect("created timestamp"),
            Timestamp::new("updated").expect("updated timestamp"),
            None,
        )
        .expect("export meta")
    }
}
