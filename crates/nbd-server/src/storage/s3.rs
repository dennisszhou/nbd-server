use super::{BlobStore, blob_already_exists};
use crate::error::{Result, ServerError};
use aws_sdk_s3::Client;
use aws_sdk_s3::config::{Credentials, Region};
use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::primitives::ByteStream;
use nbd_config::BlobStoreConfig;
use nbd_control_plane::BlobKey;

#[derive(Debug, Clone)]
pub struct S3BlobStore {
    client: Client,
    bucket: String,
    key_prefix: String,
}

impl S3BlobStore {
    pub async fn open(config: &BlobStoreConfig) -> Result<Self> {
        let BlobStoreConfig::S3 {
            endpoint_url,
            region,
            bucket,
            access_key_id,
            secret_access_key,
            force_path_style,
            key_prefix,
            auto_create_bucket,
        } = config
        else {
            return Err(ServerError::Io {
                context: "open S3 blob store",
                message: "blob store config is not S3".to_owned(),
                source: None,
            });
        };

        let key_prefix = normalize_key_prefix(key_prefix.as_deref())?;
        let mut builder = aws_sdk_s3::config::Builder::new()
            .region(Region::new(region.clone()))
            .credentials_provider(Credentials::new(
                access_key_id.clone(),
                secret_access_key.clone(),
                None,
                None,
                "nbd-config",
            ))
            .force_path_style(*force_path_style);

        if let Some(endpoint_url) = endpoint_url {
            builder = builder.endpoint_url(endpoint_url.clone());
        }

        let store = Self {
            client: Client::from_conf(builder.build()),
            bucket: bucket.clone(),
            key_prefix,
        };

        if *auto_create_bucket {
            store.create_bucket_if_needed().await?;
        } else {
            store.verify_bucket_exists().await?;
        }

        Ok(store)
    }

    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    pub fn key_prefix(&self) -> &str {
        &self.key_prefix
    }

    fn object_key(&self, key: &BlobKey) -> String {
        object_key_for_prefix(&self.key_prefix, key)
    }

    async fn create_bucket_if_needed(&self) -> Result<()> {
        match self
            .client
            .create_bucket()
            .bucket(self.bucket.clone())
            .send()
            .await
        {
            Ok(_) => Ok(()),
            Err(error) if is_bucket_already_exists(error.as_service_error()) => Ok(()),
            Err(error) => Err(s3_error("create S3 bucket", error)),
        }
    }

    async fn verify_bucket_exists(&self) -> Result<()> {
        self.client
            .head_bucket()
            .bucket(self.bucket.clone())
            .send()
            .await
            .map(|_| ())
            .map_err(|error| s3_error("verify S3 bucket", error))
    }
}

#[async_trait::async_trait]
impl BlobStore for S3BlobStore {
    async fn put_blob(&self, key: &BlobKey, data: &[u8]) -> Result<()> {
        let object_key = self.object_key(key);
        match self
            .client
            .put_object()
            .bucket(self.bucket.clone())
            .key(object_key)
            .if_none_match("*")
            .body(ByteStream::from(data.to_vec()))
            .send()
            .await
        {
            Ok(_) => Ok(()),
            Err(error) if is_create_collision(error.as_service_error()) => {
                Err(blob_already_exists("put blob", key))
            }
            Err(error) => Err(s3_error("put S3 blob", error)),
        }
    }

    async fn get_blob(&self, key: &BlobKey, offset: u64, len: u64) -> Result<Vec<u8>> {
        if len == 0 {
            return Ok(Vec::new());
        }
        let end = offset.checked_add(len - 1).ok_or_else(|| ServerError::Io {
            context: "get S3 blob",
            message: format!("range offset={offset} length={len} overflows"),
            source: None,
        })?;
        let range = format!("bytes={offset}-{end}");
        let object_key = self.object_key(key);
        let output = self
            .client
            .get_object()
            .bucket(self.bucket.clone())
            .key(object_key)
            .range(range)
            .send()
            .await
            .map_err(|error| s3_error("get S3 blob", error))?;

        let body = output
            .body
            .collect()
            .await
            .map_err(|error| ServerError::Io {
                context: "read S3 blob body",
                message: error.to_string(),
                source: None,
            })?;
        Ok(body.into_bytes().to_vec())
    }
}

fn normalize_key_prefix(prefix: Option<&str>) -> Result<String> {
    let Some(prefix) = prefix else {
        return Ok(String::new());
    };
    let trimmed = prefix.trim();
    if trimmed.is_empty() {
        return Ok(String::new());
    }
    if trimmed.starts_with('/') || trimmed.contains('\\') || trimmed.contains('\0') {
        return Err(invalid_prefix(prefix));
    }
    if trimmed.ends_with("//") {
        return Err(invalid_prefix(prefix));
    }

    let trimmed = trimmed.strip_suffix('/').unwrap_or(trimmed);
    let mut normalized = String::new();
    for segment in trimmed.split('/') {
        if segment.is_empty() || segment == "." || segment == ".." {
            return Err(invalid_prefix(prefix));
        }
        if !normalized.is_empty() {
            normalized.push('/');
        }
        normalized.push_str(segment);
    }
    normalized.push('/');
    Ok(normalized)
}

fn object_key_for_prefix(prefix: &str, key: &BlobKey) -> String {
    format!("{prefix}{key}")
}

fn invalid_prefix(prefix: &str) -> ServerError {
    ServerError::Io {
        context: "normalize S3 blob key prefix",
        message: format!("invalid S3 blob key prefix `{prefix}`"),
        source: None,
    }
}

fn is_create_collision(error: Option<&impl ProvideErrorMetadata>) -> bool {
    matches!(
        error.and_then(ProvideErrorMetadata::code),
        Some("PreconditionFailed" | "ConditionalRequestConflict")
    )
}

fn is_bucket_already_exists(error: Option<&impl ProvideErrorMetadata>) -> bool {
    matches!(
        error.and_then(ProvideErrorMetadata::code),
        Some("BucketAlreadyOwnedByYou" | "BucketAlreadyExists")
    )
}

fn s3_error(context: &'static str, error: impl std::fmt::Display) -> ServerError {
    ServerError::Io {
        context,
        message: error.to_string(),
        source: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_key_prefix_accepts_empty_and_relative_prefixes() {
        assert_eq!(normalize_key_prefix(None).expect("missing prefix"), "");
        assert_eq!(normalize_key_prefix(Some("")).expect("empty prefix"), "");
        assert_eq!(
            normalize_key_prefix(Some("v0.1/blobs")).expect("relative prefix"),
            "v0.1/blobs/"
        );
        assert_eq!(
            normalize_key_prefix(Some("v0.1/blobs/")).expect("trailing slash"),
            "v0.1/blobs/"
        );
    }

    #[test]
    fn normalize_key_prefix_rejects_ambiguous_paths() {
        for prefix in [
            "/v0.1/blobs",
            "v0.1//blobs",
            "v0.1/blobs//",
            "v0.1/./blobs",
            "v0.1/../blobs",
            "v0.1\\blobs",
            "v0.1\0blobs",
        ] {
            assert!(
                normalize_key_prefix(Some(prefix)).is_err(),
                "prefix {prefix:?} should be rejected",
            );
        }
    }

    #[test]
    fn object_key_appends_blob_key_to_normalized_prefix() {
        let key = BlobKey::new("abc123").expect("valid blob key");

        assert_eq!(object_key_for_prefix("", &key), "abc123");
        assert_eq!(
            object_key_for_prefix("v0.1/blobs/", &key),
            "v0.1/blobs/abc123"
        );
    }
}
