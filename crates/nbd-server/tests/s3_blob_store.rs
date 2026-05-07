#![cfg(feature = "s3")]

use aws_sdk_s3::Client;
use aws_sdk_s3::config::{Credentials, Region};
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

#[tokio::test]
async fn s3_configured_prefix_contains_objects_when_required() {
    if !required_env_flag("NBD_TEST_S3_REQUIRE_NONEMPTY_PREFIX") {
        return;
    }

    let config = configured_s3_blob_store().expect("required S3 blob store config");
    let (client, bucket, key_prefix) = s3_client_and_location(&config);
    let output = client
        .list_objects_v2()
        .bucket(bucket.clone())
        .prefix(key_prefix.clone())
        .max_keys(10)
        .send()
        .await
        .expect("list configured S3 prefix");

    let object = output
        .contents()
        .iter()
        .find(|object| object.size().unwrap_or_default() > 0)
        .expect("expected at least one non-empty object under configured S3 prefix");
    let key = object
        .key()
        .expect("listed S3 object should include a key")
        .to_owned();

    let body = client
        .get_object()
        .bucket(bucket)
        .key(key)
        .range("bytes=0-0")
        .send()
        .await
        .expect("get one byte from configured S3 prefix object")
        .body
        .collect()
        .await
        .expect("read configured S3 prefix object body")
        .into_bytes();
    assert_eq!(body.len(), 1);
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

fn s3_client_and_location(config: &BlobStoreConfig) -> (Client, String, String) {
    let BlobStoreConfig::S3 {
        endpoint_url,
        region,
        bucket,
        access_key_id,
        secret_access_key,
        force_path_style,
        key_prefix,
        auto_create_bucket: _,
    } = config
    else {
        panic!("expected S3 blob store config");
    };

    let mut builder = aws_sdk_s3::config::Builder::new()
        .region(Region::new(region.clone()))
        .credentials_provider(Credentials::new(
            access_key_id.clone(),
            secret_access_key.clone(),
            None,
            None,
            "nbd-test",
        ))
        .force_path_style(*force_path_style);

    if let Some(endpoint_url) = endpoint_url {
        builder = builder.endpoint_url(endpoint_url.clone());
    }

    (
        Client::from_conf(builder.build()),
        bucket.clone(),
        key_prefix.clone().unwrap_or_default(),
    )
}

fn optional_env(key: &str) -> Option<String> {
    env::var(key).ok().filter(|value| !value.is_empty())
}

fn required_env_flag(key: &str) -> bool {
    optional_env(key).is_some_and(|value| value != "0")
}
