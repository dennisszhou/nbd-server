use crate::{
    AdmissionOp, AdmittedExportRequest, ByteRange, ExportAdmissionPolicy,
    ExportAdmissionPolicyHandle, ExportEngine, ExportReply, ExportRequest, ExportResult,
    MutableBlobStoreHandle, Result, ServerError,
    observability::{self, event, target},
    put_random_blob,
    tree_reader::{Block, BlockPart, TreeReader},
};
use bytes::Bytes;
use nbd_control_plane::{
    ActiveExportDescriptor, BlobKey, ChunkIndex, ExportHead, ExportLayoutKind, ExportName, NodeId,
    SIMPLE_CHUNK_BYTES, SimpleChunkRef, SimpleTreeMetadataStore, SimpleTreeSnapshot, WalSeq,
};
use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};

const SIMPLE_CHUNK_BYTES_USIZE: usize = SIMPLE_CHUNK_BYTES as usize;

#[derive(Debug)]
pub struct SimpleDurableEngine {
    name: ExportName,
    size_bytes: u64,
    block_size: u64,
    blob_store: MutableBlobStoreHandle,
    tree_reader: Arc<dyn TreeReader<SimpleTreeSnapshot>>,
    tree: SimpleMutableTree,
}

#[derive(Debug)]
pub struct SimpleDurableAdmissionPolicy {
    size_bytes: u64,
}

pub struct SimpleMutableTree {
    catalog: Arc<dyn SimpleTreeMetadataStore>,
    commit_lock: Mutex<()>,
    state: RwLock<SimpleTreeState>,
}

#[derive(Debug, Clone)]
struct SimpleTreeState {
    export_id: nbd_control_plane::ExportId,
    size_bytes: u64,
    root_node_id: Option<NodeId>,
    chunks: BTreeMap<ChunkIndex, SimpleChunkRef>,
}

#[derive(Debug)]
struct SimpleTreeReader {
    blob_store: MutableBlobStoreHandle,
}

impl SimpleDurableEngine {
    pub async fn load(
        descriptor: &ActiveExportDescriptor,
        blob_store: MutableBlobStoreHandle,
        catalog: Arc<dyn SimpleTreeMetadataStore>,
    ) -> Result<Self> {
        let tree = SimpleMutableTree::load(catalog, descriptor).await?;
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
            tree_reader: Arc::new(SimpleTreeReader { blob_store }),
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
        let snapshot = self.tree.snapshot().await?;
        ExportHead::new(
            ExportLayoutKind::SimpleMutableTree,
            snapshot.root_node_id().cloned(),
            snapshot.size_bytes(),
            WalSeq::zero(),
        )
        .map_err(ServerError::catalog)
    }

    fn validate_range(&self, operation: &'static str, offset: u64, length: u64) -> Result<()> {
        validate_range(operation, offset, length, self.size_bytes)
    }

    async fn read(&self, offset: u64, len: u32) -> Result<Vec<u8>> {
        let range = ByteRange::new(offset, len);
        self.validate_range("read", range.start(), range.len())?;
        let snapshot = self.tree.snapshot().await?;
        self.tree_reader
            .read_committed(&snapshot, range)
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

#[async_trait::async_trait]
impl TreeReader<SimpleTreeSnapshot> for SimpleTreeReader {
    async fn read_committed(&self, root: &SimpleTreeSnapshot, range: ByteRange) -> Result<Block> {
        validate_range("read", range.start(), range.len(), root.size_bytes())?;
        let mut parts = Vec::new();
        let mut copied = 0;

        while copied < range.len() as usize {
            let current_offset = range.start() + copied as u64;
            let chunk_index = ChunkIndex::new(current_offset / SIMPLE_CHUNK_BYTES);
            let chunk_offset = current_offset % SIMPLE_CHUNK_BYTES;
            let chunk_available = SIMPLE_CHUNK_BYTES - chunk_offset;
            let copy_len = chunk_available.min(range.len() - copied as u64) as u32;
            let part_range = ByteRange::new(current_offset, copy_len);

            if let Some(chunk) = root.chunk(chunk_index) {
                let chunk_data = self
                    .blob_store
                    .get_blob(chunk.blob_key(), chunk_offset, u64::from(copy_len))
                    .await?;
                parts.push(BlockPart::Data {
                    range: part_range,
                    bytes: Bytes::from(chunk_data),
                });
            } else {
                parts.push(BlockPart::Zero { range: part_range });
            }

            copied += copy_len as usize;
        }

        Block::new(range, parts)
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

impl SimpleMutableTree {
    pub async fn load(
        catalog: Arc<dyn SimpleTreeMetadataStore>,
        descriptor: &ActiveExportDescriptor,
    ) -> Result<Self> {
        tracing::debug!(
            target: target::CATALOG,
            event = event::CATALOG_TREE_LOADED,
            service = observability::SERVICE_NAME,
            server_instance_id = observability::server_instance_id(),
            pid = observability::pid(),
            export_id = %descriptor.id(),
            export_name = %descriptor.name(),
            layout_kind = "simple_mutable_tree",
            phase = "start",
        );
        let snapshot = catalog
            .load_simple_tree(descriptor.id())
            .await
            .map_err(ServerError::catalog)?;
        if snapshot.export_id() != descriptor.id() {
            return Err(ServerError::Catalog {
                message: format!(
                    "simple tree export id {} does not match export {}",
                    snapshot.export_id(),
                    descriptor.id()
                ),
                source: None,
            });
        }

        tracing::debug!(
            target: target::CATALOG,
            event = event::CATALOG_TREE_LOADED,
            service = observability::SERVICE_NAME,
            server_instance_id = observability::server_instance_id(),
            pid = observability::pid(),
            export_id = %snapshot.export_id(),
            export_name = %descriptor.name(),
            layout_kind = "simple_mutable_tree",
            root_node_id = ?snapshot.root_node_id(),
            chunk_count = snapshot.chunks().len(),
            phase = "complete",
        );

        Ok(Self {
            catalog,
            commit_lock: Mutex::new(()),
            state: RwLock::new(SimpleTreeState::from_snapshot(&snapshot)),
        })
    }

    pub async fn size_bytes(&self) -> u64 {
        self.state.read().await.size_bytes
    }

    pub async fn snapshot(&self) -> Result<SimpleTreeSnapshot> {
        self.state
            .read()
            .await
            .to_snapshot()
            .map_err(ServerError::catalog)
    }

    pub async fn lookup_chunk(&self, chunk_index: ChunkIndex) -> Result<Option<BlobKey>> {
        Ok(self
            .state
            .read()
            .await
            .chunks
            .get(&chunk_index)
            .map(|chunk| chunk.blob_key().clone()))
    }

    pub async fn commit_new_chunks(&self, chunks: Vec<SimpleChunkRef>) -> Result<()> {
        if chunks.is_empty() {
            return Ok(());
        }

        let _commit = self.commit_lock.lock().await;
        let export_id = self.state.read().await.export_id.clone();
        tracing::debug!(
            target: target::CATALOG,
            event = event::CATALOG_TREE_COMMIT_STARTED,
            service = observability::SERVICE_NAME,
            server_instance_id = observability::server_instance_id(),
            pid = observability::pid(),
            export_id = %export_id,
            layout_kind = "simple_mutable_tree",
            chunk_count = chunks.len(),
        );
        let snapshot = self
            .catalog
            .commit_simple_chunks(&export_id, chunks)
            .await
            .map_err(ServerError::catalog)?;
        tracing::debug!(
            target: target::CATALOG,
            event = event::CATALOG_TREE_COMMIT_COMPLETED,
            service = observability::SERVICE_NAME,
            server_instance_id = observability::server_instance_id(),
            pid = observability::pid(),
            export_id = %snapshot.export_id(),
            layout_kind = "simple_mutable_tree",
            root_node_id = ?snapshot.root_node_id(),
            chunk_count = snapshot.chunks().len(),
        );
        *self.state.write().await = SimpleTreeState::from_snapshot(&snapshot);
        Ok(())
    }
}

impl fmt::Debug for SimpleMutableTree {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SimpleMutableTree").finish_non_exhaustive()
    }
}

impl SimpleTreeState {
    fn from_snapshot(snapshot: &SimpleTreeSnapshot) -> Self {
        Self {
            export_id: snapshot.export_id().clone(),
            size_bytes: snapshot.size_bytes(),
            root_node_id: snapshot.root_node_id().cloned(),
            chunks: snapshot.chunks().clone(),
        }
    }

    fn to_snapshot(&self) -> nbd_control_plane::Result<SimpleTreeSnapshot> {
        SimpleTreeSnapshot::new(
            self.export_id.clone(),
            self.size_bytes,
            self.root_node_id.clone(),
            self.chunks.clone(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::LocalBlobStore;
    use nbd_control_plane::ExportId;
    use nbd_test_support::TestRuntime;

    #[tokio::test]
    async fn simple_tree_reader_reads_from_snapshot() {
        let runtime = TestRuntime::new().expect("test runtime");
        let blob_store: MutableBlobStoreHandle =
            Arc::new(LocalBlobStore::new(runtime.root_path().join("blobs")));
        let mut chunk_data = vec![0; 4096];
        chunk_data[8..12].copy_from_slice(b"tree");
        let key = put_random_blob(blob_store.as_ref(), &chunk_data)
            .await
            .expect("create blob");
        let chunk =
            SimpleChunkRef::new(ChunkIndex::new(0), key, SIMPLE_CHUNK_BYTES).expect("chunk ref");
        let snapshot = SimpleTreeSnapshot::new(
            ExportId::new("simple-tree-reader").expect("export id"),
            4096,
            Some(NodeId::new("root-node").expect("node id")),
            BTreeMap::from([(ChunkIndex::new(0), chunk)]),
        )
        .expect("tree snapshot");
        let reader = SimpleTreeReader { blob_store };

        let block = reader
            .read_committed(&snapshot, ByteRange::new(8, 4))
            .await
            .expect("read committed tree");

        assert_eq!(
            block.parts(),
            &[BlockPart::Data {
                range: ByteRange::new(8, 4),
                bytes: Bytes::from_static(b"tree"),
            }],
        );
        assert_eq!(block.materialize().expect("materialize"), b"tree");
    }

    #[tokio::test]
    async fn simple_tree_reader_splits_large_sparse_reads_on_chunk_boundaries() {
        let runtime = TestRuntime::new().expect("test runtime");
        let blob_store: MutableBlobStoreHandle =
            Arc::new(LocalBlobStore::new(runtime.root_path().join("blobs")));
        let snapshot = SimpleTreeSnapshot::new(
            ExportId::new("simple-tree-reader-sparse").expect("export id"),
            SIMPLE_CHUNK_BYTES * 2,
            None,
            BTreeMap::new(),
        )
        .expect("tree snapshot");
        let reader = SimpleTreeReader { blob_store };
        let range = ByteRange::new(0, (SIMPLE_CHUNK_BYTES + 16 * 1024 * 1024) as u32);

        let block = reader
            .read_committed(&snapshot, range)
            .await
            .expect("read committed tree");

        assert_eq!(
            block.parts(),
            &[
                BlockPart::Zero {
                    range: ByteRange::new(0, SIMPLE_CHUNK_BYTES as u32),
                },
                BlockPart::Zero {
                    range: ByteRange::new(SIMPLE_CHUNK_BYTES, 16 * 1024 * 1024),
                },
            ],
        );
    }
}
