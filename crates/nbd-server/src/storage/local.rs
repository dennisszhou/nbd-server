use crate::{
    Result, ServerError,
    observability::{self, event, target},
};
use nbd_control_plane::BlobKey;
use std::io::{self, SeekFrom};
use std::path::{Path, PathBuf};
use tokio::fs::{self, File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

#[derive(Debug, Clone)]
pub struct LocalBlobStore {
    root: PathBuf,
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
            source: None,
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
            source: None,
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
                source: None,
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
        source: None,
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
