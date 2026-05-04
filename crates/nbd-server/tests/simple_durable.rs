use nbd_control_plane::BlobKey;
use nbd_server::{LocalBlobStore, ServerError};
use nbd_test_support::TestRuntime;
use tokio::fs;

#[tokio::test]
async fn local_blob_store_creates_and_reads_blob_ranges() {
    let runtime = TestRuntime::new().expect("test runtime");
    let blob_dir = runtime.root_path().join("blobs");
    let store = LocalBlobStore::new(&blob_dir);

    let key = store
        .create_blob(b"abcdefghijklmnop")
        .await
        .expect("create blob");

    runtime.assert_path_inside(blob_dir.join(key.as_str()));
    assert_eq!(
        store.read_blob(&key, 4, 6).await.expect("read blob range"),
        b"efghij",
    );
    assert_eq!(
        fs::read(blob_dir.join(key.as_str()))
            .await
            .expect("read blob file"),
        b"abcdefghijklmnop",
    );
}

#[tokio::test]
async fn local_blob_store_replaces_full_blob_contents() {
    let runtime = TestRuntime::new().expect("test runtime");
    let blob_dir = runtime.root_path().join("blobs");
    let store = LocalBlobStore::new(&blob_dir);
    let key = store
        .create_blob(b"old-contents")
        .await
        .expect("create blob");

    store
        .replace_blob(&key, b"new")
        .await
        .expect("replace blob");

    assert_eq!(
        store
            .read_blob(&key, 0, 3)
            .await
            .expect("read replaced blob"),
        b"new",
    );
    assert!(matches!(
        store.read_blob(&key, 0, 4).await,
        Err(ServerError::Io {
            context: "read blob",
            ..
        }),
    ));
}

#[tokio::test]
async fn local_blob_store_requires_existing_blob_for_replace() {
    let runtime = TestRuntime::new().expect("test runtime");
    let blob_dir = runtime.root_path().join("blobs");
    let store = LocalBlobStore::new(&blob_dir);
    let key = BlobKey::new("missing").expect("valid blob key");

    assert!(matches!(
        store.replace_blob(&key, b"data").await,
        Err(ServerError::Io {
            context: "stat blob before replace",
            ..
        }),
    ));
}

#[tokio::test]
async fn local_blob_store_uses_validated_blob_keys() {
    assert!(BlobKey::new("../outside").is_err());
    assert!(BlobKey::new("dir/blob").is_err());
    assert!(BlobKey::new("dir\\blob").is_err());
}
