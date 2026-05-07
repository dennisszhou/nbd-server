mod local;

use crate::{Result, ServerError};
use nbd_control_plane::BlobKey;
use std::fmt;
use std::io;
use std::sync::Arc;

pub use local::LocalBlobStore;

pub type BlobStoreHandle = Arc<dyn BlobStore>;
pub type MutableBlobStoreHandle = Arc<dyn MutableBlobStore>;

#[async_trait::async_trait]
pub trait BlobStore: fmt::Debug + Send + Sync {
    async fn get_blob(&self, key: &BlobKey, offset: u64, len: u64) -> Result<Vec<u8>>;

    async fn put_blob(&self, key: &BlobKey, data: &[u8]) -> Result<()>;
}

#[async_trait::async_trait]
pub trait MutableBlobStore: BlobStore {
    async fn overwrite_blob(&self, key: &BlobKey, data: &[u8]) -> Result<()>;
}

pub async fn put_random_blob<S: BlobStore + ?Sized>(store: &S, data: &[u8]) -> Result<BlobKey> {
    for _ in 0..16 {
        let key = BlobKey::random();
        match store.put_blob(&key, data).await {
            Ok(()) => return Ok(key),
            Err(error) if is_already_exists(&error) => continue,
            Err(error) => return Err(error),
        }
    }

    Err(ServerError::Io {
        context: "put random blob",
        message: "failed to allocate unique blob key".to_owned(),
        source: None,
    })
}

fn is_already_exists(error: &ServerError) -> bool {
    matches!(
        error,
        ServerError::Io {
            source: Some(source),
            ..
        } if source.kind() == io::ErrorKind::AlreadyExists
    )
}
