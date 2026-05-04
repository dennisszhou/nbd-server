use crate::{
    observability::{self, event, target},
    AdmissionOp, AdmittedExportRequest, ByteRange, ExportAdmissionPolicy,
    ExportAdmissionPolicyHandle, ExportEngine, ExportReply, ExportRequest, ExportResult, Result,
    ServerError,
};
use nbd_control_plane::{
    BlobKey, ChunkIndex, ExportLayoutKind, ExportMeta, ExportName, NodeId, SimpleChunkRef,
    SimpleTreeMetadataStore, SimpleTreeSnapshot, SIMPLE_CHUNK_BYTES,
};
use std::collections::BTreeMap;
use std::fmt;
use std::io::{self, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::fs::{self, File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::{Mutex, RwLock};

const SIMPLE_CHUNK_BYTES_USIZE: usize = SIMPLE_CHUNK_BYTES as usize;

#[derive(Debug, Clone)]
pub struct LocalBlobStore {
    root: PathBuf,
}

#[derive(Debug)]
pub struct SimpleDurableEngine {
    name: ExportName,
    size_bytes: u64,
    block_size: u64,
    blob_store: LocalBlobStore,
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

impl SimpleDurableEngine {
    pub async fn load(
        meta: &ExportMeta,
        blob_store: LocalBlobStore,
        catalog: Arc<dyn SimpleTreeMetadataStore>,
    ) -> Result<Self> {
        let tree = SimpleMutableTree::load(catalog, meta).await?;
        Self::from_loaded_tree(meta, blob_store, tree)
    }

    fn from_loaded_tree(
        meta: &ExportMeta,
        blob_store: LocalBlobStore,
        tree: SimpleMutableTree,
    ) -> Result<Self> {
        if meta.head().layout_kind() != ExportLayoutKind::SimpleMutableTree {
            return Err(ServerError::Catalog {
                message: format!(
                    "export `{}` does not have a simple mutable tree head",
                    meta.name()
                ),
            });
        }

        Ok(Self {
            name: meta.name().clone(),
            size_bytes: meta.size_bytes(),
            block_size: meta.block_size(),
            blob_store,
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

    fn validate_range(&self, operation: &'static str, offset: u64, length: u64) -> Result<()> {
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

    async fn read(&self, offset: u64, len: u32) -> Result<Vec<u8>> {
        self.validate_range("read", offset, u64::from(len))?;
        let mut data = vec![0; len as usize];
        let mut copied = 0;

        while copied < data.len() {
            let current_offset = offset + copied as u64;
            let chunk_index = ChunkIndex::new(current_offset / SIMPLE_CHUNK_BYTES);
            let chunk_offset = current_offset % SIMPLE_CHUNK_BYTES;
            let chunk_available = SIMPLE_CHUNK_BYTES - chunk_offset;
            let copy_len = chunk_available.min((data.len() - copied) as u64) as usize;

            if let Some(key) = self.tree.lookup_chunk(chunk_index).await? {
                let chunk_data = self
                    .blob_store
                    .read_blob(&key, chunk_offset, copy_len as u64)
                    .await?;
                data[copied..copied + copy_len].copy_from_slice(&chunk_data);
            }

            copied += copy_len;
        }

        Ok(data)
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
                        .read_blob(&key, 0, SIMPLE_CHUNK_BYTES)
                        .await?;
                    chunk[chunk_offset..chunk_offset + copy_len]
                        .copy_from_slice(&data[copied..copied + copy_len]);
                    self.blob_store.replace_blob(&key, &chunk).await?;
                }
                None => {
                    let mut chunk = vec![0; SIMPLE_CHUNK_BYTES_USIZE];
                    chunk[chunk_offset..chunk_offset + copy_len]
                        .copy_from_slice(&data[copied..copied + copy_len]);
                    let key = self.blob_store.create_blob(&chunk).await?;
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

impl LocalBlobStore {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub async fn create_blob(&self, data: &[u8]) -> Result<BlobKey> {
        self.ensure_root_dir().await?;

        for _ in 0..16 {
            let key = BlobKey::random();
            let path = self.blob_path(&key)?;
            match write_new_file(&path, data).await {
                Ok(()) => {
                    sync_directory(self.root.clone()).await?;
                    tracing::trace!(
                        target: target::STORAGE,
                        event = event::BLOB_CREATE,
                        service = observability::SERVICE_NAME,
                        server_instance_id = observability::server_instance_id(),
                        pid = observability::pid(),
                        engine_kind = "simple_durable",
                        blob_op = "create",
                        blob_key = %key,
                        storage_len = data.len(),
                    );
                    return Ok(key);
                }
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                    continue;
                }
                Err(error) => {
                    let _ = fs::remove_file(&path).await;
                    return Err(ServerError::io("create blob", error));
                }
            }
        }

        Err(ServerError::Io {
            context: "create blob",
            message: "failed to allocate unique blob key".to_owned(),
        })
    }

    pub async fn replace_blob(&self, key: &BlobKey, data: &[u8]) -> Result<()> {
        self.ensure_root_dir().await?;
        let path = self.blob_path(key)?;
        fs::metadata(&path)
            .await
            .map_err(|source| ServerError::io("stat blob before replace", source))?;

        let temp_key =
            BlobKey::new(format!(".tmp-{}", BlobKey::random())).map_err(ServerError::catalog)?;
        let temp_path = self.blob_path(&temp_key)?;
        write_new_file(&temp_path, data)
            .await
            .map_err(|source| ServerError::io("write replacement blob", source))?;
        if let Err(error) = fs::rename(&temp_path, &path).await {
            let _ = fs::remove_file(&temp_path).await;
            return Err(ServerError::io("rename replacement blob", error));
        }
        sync_directory(self.root.clone()).await?;
        tracing::trace!(
            target: target::STORAGE,
            event = event::BLOB_REPLACE,
            service = observability::SERVICE_NAME,
            server_instance_id = observability::server_instance_id(),
            pid = observability::pid(),
            engine_kind = "simple_durable",
            blob_op = "replace",
            blob_key = %key,
            storage_len = data.len(),
        );
        Ok(())
    }

    pub async fn read_blob(&self, key: &BlobKey, offset: u64, len: u64) -> Result<Vec<u8>> {
        let len = usize::try_from(len).map_err(|_| ServerError::Io {
            context: "read blob",
            message: format!("length {len} does not fit in memory"),
        })?;
        let path = self.blob_path(key)?;
        let mut file = File::open(&path)
            .await
            .map_err(|source| ServerError::io("open blob for read", source))?;
        file.seek(SeekFrom::Start(offset))
            .await
            .map_err(|source| ServerError::io("seek blob", source))?;

        let mut data = vec![0; len];
        file.read_exact(&mut data)
            .await
            .map_err(|source| ServerError::io("read blob", source))?;
        tracing::trace!(
            target: target::STORAGE,
            event = event::BLOB_READ,
            service = observability::SERVICE_NAME,
            server_instance_id = observability::server_instance_id(),
            pid = observability::pid(),
            engine_kind = "simple_durable",
            blob_op = "read",
            blob_key = %key,
            storage_offset = offset,
            storage_len = len,
        );
        Ok(data)
    }

    fn blob_path(&self, key: &BlobKey) -> Result<PathBuf> {
        let path = self.root.join(key.as_str());
        if path.parent() != Some(self.root.as_path()) || !path.starts_with(&self.root) {
            return Err(ServerError::Io {
                context: "resolve blob path",
                message: format!("blob key `{key}` escaped blob root"),
            });
        }
        Ok(path)
    }

    async fn ensure_root_dir(&self) -> Result<()> {
        fs::create_dir_all(&self.root)
            .await
            .map_err(|source| ServerError::io("create blob directory", source))
    }
}

impl SimpleMutableTree {
    pub async fn load(
        catalog: Arc<dyn SimpleTreeMetadataStore>,
        meta: &ExportMeta,
    ) -> Result<Self> {
        if meta.head().layout_kind() != ExportLayoutKind::SimpleMutableTree {
            return Err(ServerError::Catalog {
                message: format!(
                    "export `{}` does not have a simple mutable tree head",
                    meta.name()
                ),
            });
        }

        tracing::debug!(
            target: target::CATALOG,
            event = event::CATALOG_TREE_LOADED,
            service = observability::SERVICE_NAME,
            server_instance_id = observability::server_instance_id(),
            pid = observability::pid(),
            export_id = %meta.id(),
            export_name = %meta.name(),
            layout_kind = %meta.head().layout_kind(),
            phase = "start",
        );
        let snapshot = catalog
            .load_simple_tree(meta.id())
            .await
            .map_err(ServerError::catalog)?;
        if snapshot.export_id() != meta.id() {
            return Err(ServerError::Catalog {
                message: format!(
                    "simple tree export id {} does not match export {}",
                    snapshot.export_id(),
                    meta.id()
                ),
            });
        }
        if snapshot.size_bytes() != meta.size_bytes() {
            return Err(ServerError::Catalog {
                message: format!(
                    "simple tree size {} does not match export size {}",
                    snapshot.size_bytes(),
                    meta.size_bytes()
                ),
            });
        }

        tracing::debug!(
            target: target::CATALOG,
            event = event::CATALOG_TREE_LOADED,
            service = observability::SERVICE_NAME,
            server_instance_id = observability::server_instance_id(),
            pid = observability::pid(),
            export_id = %snapshot.export_id(),
            export_name = %meta.name(),
            layout_kind = %meta.head().layout_kind(),
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

async fn write_new_file(path: &Path, data: &[u8]) -> io::Result<()> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .await?;
    file.write_all(data).await?;
    file.sync_all().await
}

async fn sync_directory(path: PathBuf) -> Result<()> {
    tokio::task::spawn_blocking(move || {
        let dir = std::fs::File::open(path)?;
        match dir.sync_all() {
            Ok(()) => Ok(()),
            // macOS and some filesystems reject directory fsync. The blob
            // file itself was already fsynced; keep directory fsync best
            // effort where the platform does not support it.
            Err(error)
                if error.kind() == io::ErrorKind::InvalidInput
                    || error.raw_os_error() == Some(libc_einval()) =>
            {
                Ok(())
            }
            Err(error) => Err(error),
        }
    })
    .await
    .map_err(|error| ServerError::Io {
        context: "sync blob directory",
        message: error.to_string(),
    })?
    .map_err(|source| ServerError::io("sync blob directory", source))?;
    tracing::trace!(
        target: target::STORAGE,
        event = event::BLOB_DIRECTORY_SYNCED,
        service = observability::SERVICE_NAME,
        server_instance_id = observability::server_instance_id(),
        pid = observability::pid(),
        engine_kind = "simple_durable",
        blob_op = "sync_directory",
    );
    Ok(())
}

#[cfg(unix)]
fn libc_einval() -> i32 {
    22
}

#[cfg(not(unix))]
fn libc_einval() -> i32 {
    i32::MIN
}
