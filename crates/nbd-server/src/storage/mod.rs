mod local;
#[cfg(feature = "s3")]
mod s3;

use crate::error::{Result, ServerError};
use nbd_config::{BlobStoreConfig, NbdConfig};
use nbd_control_plane::BlobKey;
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub use local::LocalBlobStore;
#[cfg(feature = "s3")]
pub use s3::S3BlobStore;

pub type BlobStoreHandle = Arc<dyn BlobStore>;
pub type MutableBlobStoreHandle = Arc<dyn MutableBlobStore>;

#[derive(Debug, Clone)]
pub enum ConfiguredBlobStore {
    Local(Arc<LocalBlobStore>),
    #[cfg(feature = "s3")]
    S3(Arc<S3BlobStore>),
}

impl ConfiguredBlobStore {
    pub async fn open(config: &NbdConfig) -> Result<Self> {
        match &config.blob_store {
            BlobStoreConfig::Local => Ok(Self::local(config.runtime.blob_dir.clone())),
            #[cfg(feature = "s3")]
            BlobStoreConfig::S3 { .. } => Ok(Self::S3(Arc::new(
                S3BlobStore::open(&config.blob_store).await?,
            ))),
            #[cfg(not(feature = "s3"))]
            BlobStoreConfig::S3 { .. } => Err(ServerError::Io {
                context: "open blob store",
                message: "S3 blob store backend is not available in this build".to_owned(),
                source: None,
            }),
        }
    }

    pub fn local(root: impl Into<PathBuf>) -> Self {
        Self::Local(Arc::new(LocalBlobStore::new(root)))
    }

    pub fn blob_store(&self) -> BlobStoreHandle {
        match self {
            Self::Local(store) => store.clone(),
            #[cfg(feature = "s3")]
            Self::S3(store) => store.clone(),
        }
    }

    pub fn mutable_blob_store(&self) -> Option<MutableBlobStoreHandle> {
        match self {
            Self::Local(store) => Some(store.clone()),
            #[cfg(feature = "s3")]
            Self::S3(_) => None,
        }
    }

    pub fn local_root(&self) -> Option<&Path> {
        match self {
            Self::Local(store) => Some(store.root()),
            #[cfg(feature = "s3")]
            Self::S3(_) => None,
        }
    }
}

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
            Err(error) if is_blob_already_exists(&error) => continue,
            Err(error) => return Err(error),
        }
    }

    Err(ServerError::Io {
        context: "put random blob",
        message: "failed to allocate unique blob key".to_owned(),
        source: None,
    })
}

pub(crate) fn blob_already_exists(context: &'static str, key: &BlobKey) -> ServerError {
    ServerError::BlobAlreadyExists {
        context,
        key: key.clone(),
    }
}

pub(crate) fn is_blob_already_exists(error: &ServerError) -> bool {
    matches!(error, ServerError::BlobAlreadyExists { .. })
}

#[cfg(test)]
mod tests {
    use super::*;
    use nbd_config::{CatalogConfig, LoggingConfig, RuntimeConfig, ServerConfig};

    #[tokio::test]
    async fn configured_local_store_uses_runtime_blob_dir() {
        let config = test_config(BlobStoreConfig::Local);

        let store = ConfiguredBlobStore::open(&config)
            .await
            .expect("open local store");

        assert_eq!(
            store.local_root().expect("local root"),
            Path::new("/tmp/nbd/blobs")
        );
        assert!(store.mutable_blob_store().is_some());
    }

    #[cfg(not(feature = "s3"))]
    #[tokio::test]
    async fn configured_s3_store_fails_until_backend_exists() {
        let config = test_config(BlobStoreConfig::S3 {
            endpoint_url: Some("http://rustfs:9000".to_owned()),
            region: "us-east-1".to_owned(),
            bucket: "everstore".to_owned(),
            access_key_id: "rustfsadmin".to_owned(),
            secret_access_key: "rustfsadmin".to_owned(),
            force_path_style: true,
            key_prefix: Some("v0.1/blobs/".to_owned()),
            auto_create_bucket: true,
        });

        let error = ConfiguredBlobStore::open(&config)
            .await
            .expect_err("S3 backend is not implemented yet");

        assert!(matches!(
            error,
            ServerError::Io {
                context: "open blob store",
                ..
            }
        ));
        assert!(error.to_string().contains("S3 blob store backend"));
    }

    fn test_config(blob_store: BlobStoreConfig) -> NbdConfig {
        NbdConfig {
            catalog: CatalogConfig {
                url: "file:/tmp/nbd/catalog.db".to_owned(),
            },
            runtime: RuntimeConfig {
                state_dir: PathBuf::from("/tmp/nbd"),
                blob_dir: PathBuf::from("/tmp/nbd/blobs"),
                wal_dir: PathBuf::from("/tmp/nbd/wal"),
            },
            blob_store,
            server: ServerConfig::default(),
            logging: LoggingConfig::default(),
        }
    }
}
