use nbd_control_plane::{
    BlobKey, CatalogUrl, ChunkIndex, CreateExport, ExportCatalog, ExportEngineKind, ExportMeta,
    ExportName, InspectExport, SQLiteExportCatalog, SimpleChunkRef, SIMPLE_CHUNK_BYTES,
};
use nbd_server::{LocalBlobStore, ServerError, SimpleMutableTree};
use nbd_test_support::TestRuntime;
use std::sync::Arc;
use tokio::fs;

const MIGRATIONS: &[&str] = &[
    include_str!("../../../prisma/migrations/20260501000000_init/migration.sql"),
    include_str!(
        "../../../prisma/migrations/20260504000000_export_heads_tree_metadata/migration.sql"
    ),
];

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

#[tokio::test]
async fn simple_mutable_tree_loads_sparse_128mib_head() {
    let (_runtime, catalog, meta) = simple_tree_fixture("disk-a").await;
    let tree = load_tree(&catalog, &meta).await;

    let snapshot = tree.snapshot().await.expect("snapshot");
    assert_eq!(snapshot.size_bytes(), 128 * 1024 * 1024);
    assert!(snapshot.root_node_id().is_none());
    assert!(snapshot.chunks().is_empty());
    assert_eq!(
        tree.lookup_chunk(ChunkIndex::new(0))
            .await
            .expect("lookup sparse chunk"),
        None,
    );
}

#[tokio::test]
async fn simple_mutable_tree_commits_later_leaf_insertion() {
    let (_runtime, catalog, meta) = simple_tree_fixture("disk-a").await;
    let tree = load_tree(&catalog, &meta).await;
    let chunk = SimpleChunkRef::new(
        ChunkIndex::new(2),
        BlobKey::new("blob-two").expect("valid blob key"),
        SIMPLE_CHUNK_BYTES,
    )
    .expect("valid chunk");

    tree.commit_new_chunks(vec![chunk.clone()])
        .await
        .expect("commit new chunk");

    assert_eq!(
        tree.lookup_chunk(ChunkIndex::new(2))
            .await
            .expect("lookup committed chunk"),
        Some(chunk.blob_key().clone()),
    );
    assert_eq!(
        tree.lookup_chunk(ChunkIndex::new(1))
            .await
            .expect("lookup sparse chunk"),
        None,
    );

    let reloaded = load_tree(&catalog, &meta).await;
    assert_eq!(
        reloaded
            .lookup_chunk(ChunkIndex::new(2))
            .await
            .expect("lookup reloaded chunk"),
        Some(chunk.blob_key().clone()),
    );
}

#[tokio::test]
async fn simple_mutable_tree_keeps_cache_after_failed_commit() {
    let (_runtime, catalog, meta) = simple_tree_fixture("disk-a").await;
    let tree = load_tree(&catalog, &meta).await;
    let first = SimpleChunkRef::new(
        ChunkIndex::new(1),
        BlobKey::new("blob-one").expect("valid blob key"),
        SIMPLE_CHUNK_BYTES,
    )
    .expect("valid chunk");
    tree.commit_new_chunks(vec![first.clone()])
        .await
        .expect("commit first chunk");

    let replacement = SimpleChunkRef::new(
        ChunkIndex::new(1),
        BlobKey::new("blob-one-replacement").expect("valid blob key"),
        SIMPLE_CHUNK_BYTES,
    )
    .expect("valid chunk");
    tree.commit_new_chunks(vec![replacement])
        .await
        .expect_err("duplicate chunk should fail");

    assert_eq!(
        tree.lookup_chunk(ChunkIndex::new(1))
            .await
            .expect("lookup original chunk"),
        Some(first.blob_key().clone()),
    );
}

async fn simple_tree_fixture(name: &str) -> (TestRuntime, SQLiteExportCatalog, ExportMeta) {
    let runtime = TestRuntime::new().expect("test runtime");
    let url = CatalogUrl::parse(runtime.catalog_url()).expect("catalog URL");
    let catalog = SQLiteExportCatalog::connect(&url)
        .await
        .expect("connect catalog");

    for migration in MIGRATIONS {
        sqlx::raw_sql(migration)
            .execute(catalog.pool())
            .await
            .expect("apply migration");
    }

    let created = catalog
        .create_export(
            CreateExport::new(
                ExportName::new(name).expect("export name"),
                128 * 1024 * 1024,
                4096,
                ExportEngineKind::Memory,
            )
            .expect("create request"),
        )
        .await
        .expect("create export");
    sqlx::query(
        r#"
        UPDATE export_heads
        SET layout_kind = 'simple_mutable_tree'
        WHERE export_id = ?
        "#,
    )
    .bind(created.id().as_str())
    .execute(catalog.pool())
    .await
    .expect("mark simple tree head");

    let meta = catalog
        .inspect_export(InspectExport::new(
            ExportName::new(name).expect("export name"),
        ))
        .await
        .expect("inspect export");
    (runtime, catalog, meta)
}

async fn load_tree(catalog: &SQLiteExportCatalog, meta: &ExportMeta) -> SimpleMutableTree {
    SimpleMutableTree::load(Arc::new(catalog.clone()), meta)
        .await
        .expect("load simple mutable tree")
}
