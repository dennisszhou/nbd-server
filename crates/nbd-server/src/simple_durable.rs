use crate::{Result, ServerError};
use nbd_control_plane::{
    BlobKey, ChunkIndex, ExportLayoutKind, ExportMeta, NodeId, SimpleChunkRef,
    SimpleTreeMetadataStore, SimpleTreeSnapshot,
};
use std::collections::BTreeMap;
use std::fmt;
use std::io::{self, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::fs::{self, File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::{Mutex, RwLock};

#[derive(Debug, Clone)]
pub struct LocalBlobStore {
    root: PathBuf,
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
        sync_directory(self.root.clone()).await
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
        let snapshot = self
            .catalog
            .commit_simple_chunks(&export_id, chunks)
            .await
            .map_err(ServerError::catalog)?;
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
    .map_err(|source| ServerError::io("sync blob directory", source))
}

#[cfg(unix)]
fn libc_einval() -> i32 {
    22
}

#[cfg(not(unix))]
fn libc_einval() -> i32 {
    i32::MIN
}
