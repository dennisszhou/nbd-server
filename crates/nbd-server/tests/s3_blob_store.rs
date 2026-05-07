#![cfg(feature = "s3")]

use nbd_config::BlobStoreConfig;
use nbd_control_plane::BlobKey;
use nbd_server::{BlobStore, S3BlobStore, ServerError};
use std::env;

#[tokio::test]
async fn s3_blob_store_exercises_configured_endpoint_when_available() {
    let Some(config) = configured_s3_blob_store() else {
        return;
    };
    let store = S3BlobStore::open(&config)
        .await
        .expect("open S3 blob store");
    let key = BlobKey::random();

    store
        .put_blob(&key, b"abcdefghijklmnop")
        .await
        .expect("put new blob");
    assert!(matches!(
        store.put_blob(&key, b"replacement").await,
        Err(ServerError::BlobAlreadyExists {
            context: "put blob",
            ..
        }),
    ));
    assert_eq!(
        store.get_blob(&key, 4, 6).await.expect("get range"),
        b"efghij"
    );
    assert!(matches!(
        store
            .get_blob(&BlobKey::random(), 0, 1)
            .await
            .expect_err("missing object should fail"),
        ServerError::Io {
            context: "get S3 blob",
            ..
        },
    ));
}

fn configured_s3_blob_store() -> Option<BlobStoreConfig> {
    let endpoint_url = optional_env("NBD_TEST_S3_ENDPOINT_URL")?;
    let bucket = optional_env("NBD_TEST_S3_BUCKET").unwrap_or_else(|| "everstore".to_owned());
    let access_key_id =
        optional_env("NBD_TEST_S3_ACCESS_KEY_ID").unwrap_or_else(|| "rustfsadmin".to_owned());
    let secret_access_key =
        optional_env("NBD_TEST_S3_SECRET_ACCESS_KEY").unwrap_or_else(|| "rustfsadmin".to_owned());
    let region = optional_env("NBD_TEST_S3_REGION").unwrap_or_else(|| "us-east-1".to_owned());
    let key_prefix =
        optional_env("NBD_TEST_S3_KEY_PREFIX").unwrap_or_else(|| "v0.1/test-blobs/".to_owned());

    Some(BlobStoreConfig::S3 {
        endpoint_url: Some(endpoint_url),
        region,
        bucket,
        access_key_id,
        secret_access_key,
        force_path_style: true,
        key_prefix: Some(key_prefix),
        auto_create_bucket: true,
    })
}

fn optional_env(key: &str) -> Option<String> {
    env::var(key).ok().filter(|value| !value.is_empty())
}
