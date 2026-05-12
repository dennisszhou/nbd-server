mod mutable_tree;
mod reader;

pub use mutable_tree::SimpleMutableTree;

use reader::SimpleTreeReader;

use crate::engines::tree::TreeReader;
use crate::error::{Result, ServerError};
use crate::export::{
    AdmissionOp, AdmittedExportRequest, ExportAdmissionPolicy, ExportAdmissionPolicyHandle,
    ExportEngine, ExportReply, ExportRequest, ExportResult,
};
use crate::range::ByteRange;
use crate::storage::{MutableBlobStoreHandle, put_random_blob};
use nbd_control_plane::{
    ActiveExportDescriptor, ChunkIndex, ExportHead, ExportName, SIMPLE_CHUNK_BYTES, SimpleChunkRef,
    TreeRecordStore,
};
use std::sync::Arc;

const SIMPLE_CHUNK_BYTES_USIZE: usize = SIMPLE_CHUNK_BYTES as usize;

#[derive(Debug)]
pub struct SimpleDurableEngine {
    name: ExportName,
    size_bytes: u64,
    block_size: u64,
    blob_store: MutableBlobStoreHandle,
    tree_reader: Arc<dyn TreeReader<SimpleMutableTree>>,
    tree: SimpleMutableTree,
}

#[derive(Debug)]
pub struct SimpleDurableAdmissionPolicy {
    size_bytes: u64,
}

impl SimpleDurableEngine {
    pub async fn load(
        descriptor: &ActiveExportDescriptor,
        blob_store: MutableBlobStoreHandle,
        catalog: Arc<dyn TreeRecordStore>,
        head: ExportHead,
    ) -> Result<Self> {
        let tree = SimpleMutableTree::load(catalog, descriptor, head).await?;
        Self::from_loaded_tree(descriptor, blob_store, tree).await
    }

    async fn from_loaded_tree(
        descriptor: &ActiveExportDescriptor,
        blob_store: MutableBlobStoreHandle,
        tree: SimpleMutableTree,
    ) -> Result<Self> {
        Ok(Self {
            name: descriptor.name().clone(),
            size_bytes: tree.size_bytes().await,
            block_size: descriptor.block_size(),
            blob_store: blob_store.clone(),
            tree_reader: Arc::new(SimpleTreeReader::new(blob_store)),
            tree,
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

    pub async fn export_head(&self) -> Result<ExportHead> {
        self.tree.export_head().await
    }

    fn validate_range(&self, operation: &'static str, offset: u64, length: u64) -> Result<()> {
        validate_range(operation, offset, length, self.size_bytes)
    }

    async fn read(&self, offset: u64, len: u32) -> Result<Vec<u8>> {
        let range = ByteRange::new(offset, len);
        self.validate_range("read", range.start(), range.len())?;
        self.tree_reader
            .read_committed(&self.tree, range)
            .await?
            .materialize()
    }

    async fn write(&self, offset: u64, data: &[u8]) -> Result<()> {
        self.validate_range("write", offset, data.len() as u64)?;
        if data.is_empty() {
            return Ok(());
        }

        let mut new_chunks = Vec::new();
        let mut copied = 0;

        while copied < data.len() {
            let current_offset = offset + copied as u64;
            let chunk_index = ChunkIndex::new(current_offset / SIMPLE_CHUNK_BYTES);
            let chunk_offset = (current_offset % SIMPLE_CHUNK_BYTES) as usize;
            let chunk_available = SIMPLE_CHUNK_BYTES_USIZE - chunk_offset;
            let copy_len = chunk_available.min(data.len() - copied);

            match self.tree.lookup_chunk(chunk_index).await? {
                Some(key) => {
                    let mut chunk = self
                        .blob_store
                        .get_blob(&key, 0, SIMPLE_CHUNK_BYTES)
                        .await?;
                    chunk[chunk_offset..chunk_offset + copy_len]
                        .copy_from_slice(&data[copied..copied + copy_len]);
                    self.blob_store.overwrite_blob(&key, &chunk).await?;
                }
                None => {
                    let mut chunk = vec![0; SIMPLE_CHUNK_BYTES_USIZE];
                    chunk[chunk_offset..chunk_offset + copy_len]
                        .copy_from_slice(&data[copied..copied + copy_len]);
                    let key = put_random_blob(self.blob_store.as_ref(), &chunk).await?;
                    new_chunks.push(
                        SimpleChunkRef::new(chunk_index, key, SIMPLE_CHUNK_BYTES)
                            .map_err(ServerError::catalog)?,
                    );
                }
            }

            copied += copy_len;
        }

        self.tree.commit_new_chunks(new_chunks).await
    }

    fn flush(&self) -> Result<()> {
        Ok(())
    }
}

fn validate_range(
    operation: &'static str,
    offset: u64,
    length: u64,
    size_bytes: u64,
) -> Result<()> {
    let end = offset.checked_add(length).ok_or(ServerError::OutOfBounds {
        operation,
        offset,
        length,
        size_bytes,
    })?;
    if end > size_bytes {
        return Err(ServerError::OutOfBounds {
            operation,
            offset,
            length,
            size_bytes,
        });
    }

    Ok(())
}

impl SimpleDurableAdmissionPolicy {
    pub fn new(size_bytes: u64) -> Self {
        Self { size_bytes }
    }

    fn validate_request_range(
        &self,
        operation: &'static str,
        offset: u64,
        length: u64,
    ) -> Result<()> {
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
        Ok(())
    }

    fn chunk_aligned_write(&self, offset: u64, len: u64) -> Result<ByteRange> {
        self.validate_request_range("write", offset, len)?;
        if len == 0 {
            return Ok(ByteRange::new(offset, 0));
        }

        let request_end = offset + len;
        let chunk_bytes = SIMPLE_CHUNK_BYTES;
        let start_chunk = offset / chunk_bytes;
        let end_chunk = (request_end - 1) / chunk_bytes;
        let start = start_chunk
            .checked_mul(chunk_bytes)
            .ok_or(ServerError::OutOfBounds {
                operation: "write",
                offset,
                length: len,
                size_bytes: self.size_bytes,
            })?;
        let next_chunk = end_chunk.checked_add(1).ok_or(ServerError::OutOfBounds {
            operation: "write",
            offset,
            length: len,
            size_bytes: self.size_bytes,
        })?;
        let unclamped_end =
            next_chunk
                .checked_mul(chunk_bytes)
                .ok_or(ServerError::OutOfBounds {
                    operation: "write",
                    offset,
                    length: len,
                    size_bytes: self.size_bytes,
                })?;
        let end = unclamped_end.min(self.size_bytes);
        let aligned_len = end.checked_sub(start).ok_or(ServerError::OutOfBounds {
            operation: "write",
            offset,
            length: len,
            size_bytes: self.size_bytes,
        })?;
        let aligned_len = u32::try_from(aligned_len).map_err(|_| ServerError::OutOfBounds {
            operation: "write",
            offset: start,
            length: aligned_len,
            size_bytes: self.size_bytes,
        })?;

        Ok(ByteRange::new(start, aligned_len))
    }
}

impl ExportAdmissionPolicy for SimpleDurableAdmissionPolicy {
    fn operation_for(&self, request: &ExportRequest) -> Result<AdmissionOp> {
        match request {
            ExportRequest::Read { offset, len } => {
                Ok(AdmissionOp::Read(ByteRange::new(*offset, *len)))
            }
            ExportRequest::Write { offset, data } => {
                let len = u64::try_from(data.len()).map_err(|_| ServerError::OutOfBounds {
                    operation: "write",
                    offset: *offset,
                    length: u64::MAX,
                    size_bytes: self.size_bytes,
                })?;
                Ok(AdmissionOp::Write(self.chunk_aligned_write(*offset, len)?))
            }
            ExportRequest::Flush => Ok(AdmissionOp::Flush),
        }
    }
}

#[async_trait::async_trait]
impl ExportEngine for SimpleDurableEngine {
    fn admission_policy(&self) -> ExportAdmissionPolicyHandle {
        Arc::new(SimpleDurableAdmissionPolicy::new(self.size_bytes))
    }

    async fn execute_admitted(&self, request: AdmittedExportRequest) -> ExportResult {
        match request.request() {
            ExportRequest::Read { offset, len } => Ok(ExportReply::Read {
                data: self.read(*offset, *len).await?,
            }),
            ExportRequest::Write { offset, data } => {
                self.write(*offset, data).await?;
                Ok(ExportReply::Done)
            }
            ExportRequest::Flush => {
                self.flush()?;
                Ok(ExportReply::Done)
            }
        }
    }
}
