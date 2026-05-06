use nbd_control_plane::{
    BlobKey, CatalogUrl, ChunkIndex, CreateExport, ExportCatalog, ExportEngineKind, ExportName,
    ExportRecord, InspectExport, SQLiteExportCatalog, SimpleChunkRef, SimpleTreeMetadataStore,
    SIMPLE_CHUNK_BYTES,
};
use nbd_server::{
    AdmissionOp, ByteRange, ConcurrentExportRuntime, ExportAdmissionPolicy, ExportJob, ExportReply,
    ExportRequest, ExportRuntime, LocalBlobStore, ServerError, SimpleDurableAdmissionPolicy,
    SimpleDurableEngine, SimpleMutableTree,
};
use nbd_test_support::TestRuntime;
use std::sync::Arc;
use tokio::fs;

const MIGRATIONS: &[&str] = &[include_str!(
    "../../../prisma/migrations/20260506000000_baseline/migration.sql"
)];

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

#[test]
fn simple_durable_policy_maps_writes_to_chunk_aligned_ranges() {
    let policy = SimpleDurableAdmissionPolicy::new(SIMPLE_CHUNK_BYTES * 2);
    let request = ExportRequest::Write {
        offset: SIMPLE_CHUNK_BYTES - 2,
        data: b"abcd".to_vec(),
    };

    assert_eq!(
        policy.operation_for(&request).expect("admission op"),
        AdmissionOp::Write(ByteRange::new(0, (SIMPLE_CHUNK_BYTES * 2) as u32)),
    );
}

#[test]
fn simple_durable_policy_clamps_final_chunk_to_export_size() {
    let policy = SimpleDurableAdmissionPolicy::new(SIMPLE_CHUNK_BYTES + 4096);
    let request = ExportRequest::Write {
        offset: SIMPLE_CHUNK_BYTES + 1,
        data: b"ab".to_vec(),
    };

    assert_eq!(
        policy.operation_for(&request).expect("admission op"),
        AdmissionOp::Write(ByteRange::new(SIMPLE_CHUNK_BYTES, 4096)),
    );
}

#[test]
fn simple_durable_policy_rejects_original_out_of_bounds_write() {
    let policy = SimpleDurableAdmissionPolicy::new(SIMPLE_CHUNK_BYTES + 4096);
    let request = ExportRequest::Write {
        offset: SIMPLE_CHUNK_BYTES + 4095,
        data: b"ab".to_vec(),
    };

    assert!(matches!(
        policy.operation_for(&request),
        Err(ServerError::OutOfBounds {
            operation: "write",
            offset,
            length: 2,
            size_bytes,
        }) if offset == SIMPLE_CHUNK_BYTES + 4095
            && size_bytes == SIMPLE_CHUNK_BYTES + 4096,
    ));
}

#[tokio::test]
async fn simple_durable_engine_reads_sparse_zeroes_through_runtime() {
    let (_runtime, _catalog, _meta, export_runtime) =
        simple_durable_runtime("disk-sparse", SIMPLE_CHUNK_BYTES + 4096).await;

    assert_eq!(
        execute_request(
            &export_runtime,
            ExportRequest::Read {
                offset: SIMPLE_CHUNK_BYTES - 2,
                len: 4,
            },
        )
        .await,
        ExportReply::Read { data: vec![0; 4] },
    );

    export_runtime.close().await.expect("close runtime");
}

#[tokio::test]
async fn simple_durable_engine_writes_and_reads_across_chunks() {
    let (_runtime, _catalog, _meta, export_runtime) =
        simple_durable_runtime("disk-cross", SIMPLE_CHUNK_BYTES + 4096).await;

    assert_eq!(
        execute_request(
            &export_runtime,
            ExportRequest::Write {
                offset: SIMPLE_CHUNK_BYTES - 2,
                data: b"abcd".to_vec(),
            },
        )
        .await,
        ExportReply::Done,
    );
    assert_eq!(
        execute_request(
            &export_runtime,
            ExportRequest::Read {
                offset: SIMPLE_CHUNK_BYTES - 4,
                len: 8,
            },
        )
        .await,
        ExportReply::Read {
            data: b"\0\0abcd\0\0".to_vec(),
        },
    );

    export_runtime.close().await.expect("close runtime");
}

#[tokio::test]
async fn simple_durable_engine_overwrites_existing_chunk_without_new_metadata() {
    let (_runtime, catalog, meta, export_runtime) =
        simple_durable_runtime("disk-overwrite", SIMPLE_CHUNK_BYTES).await;

    assert_eq!(
        execute_request(
            &export_runtime,
            ExportRequest::Write {
                offset: 10,
                data: b"abcdef".to_vec(),
            },
        )
        .await,
        ExportReply::Done,
    );
    let first_key = simple_chunk_key(&catalog, &meta, 0).await;

    assert_eq!(
        execute_request(
            &export_runtime,
            ExportRequest::Write {
                offset: 12,
                data: b"ZZ".to_vec(),
            },
        )
        .await,
        ExportReply::Done,
    );
    assert_eq!(simple_chunk_key(&catalog, &meta, 0).await, first_key);
    assert_eq!(
        execute_request(&export_runtime, ExportRequest::Read { offset: 10, len: 6 },).await,
        ExportReply::Read {
            data: b"abZZef".to_vec(),
        },
    );

    export_runtime.close().await.expect("close runtime");
}

#[tokio::test]
async fn simple_durable_engine_flush_is_done() {
    let (_runtime, _catalog, _meta, export_runtime) =
        simple_durable_runtime("disk-flush", SIMPLE_CHUNK_BYTES).await;

    assert_eq!(
        execute_request(&export_runtime, ExportRequest::Flush).await,
        ExportReply::Done,
    );

    export_runtime.close().await.expect("close runtime");
}

async fn simple_tree_fixture(name: &str) -> (TestRuntime, SQLiteExportCatalog, ExportRecord) {
    simple_tree_fixture_with_size(name, 128 * 1024 * 1024).await
}

async fn simple_tree_fixture_with_size(
    name: &str,
    size_bytes: u64,
) -> (TestRuntime, SQLiteExportCatalog, ExportRecord) {
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

    catalog
        .create_export(
            CreateExport::new(
                ExportName::new(name).expect("export name"),
                size_bytes,
                4096,
                ExportEngineKind::SimpleDurable,
            )
            .expect("create request"),
        )
        .await
        .expect("create export");

    let meta = catalog
        .inspect_export(InspectExport::new(
            ExportName::new(name).expect("export name"),
        ))
        .await
        .expect("inspect export");
    (runtime, catalog, meta)
}

async fn load_tree(catalog: &SQLiteExportCatalog, meta: &ExportRecord) -> SimpleMutableTree {
    let descriptor = catalog
        .load_export_descriptor(meta.name().clone())
        .await
        .expect("load descriptor");
    SimpleMutableTree::load(Arc::new(catalog.clone()), &descriptor)
        .await
        .expect("load simple mutable tree")
}

async fn simple_durable_runtime(
    name: &str,
    size_bytes: u64,
) -> (
    TestRuntime,
    SQLiteExportCatalog,
    ExportRecord,
    ConcurrentExportRuntime,
) {
    let (runtime, catalog, meta) = simple_tree_fixture_with_size(name, size_bytes).await;
    let descriptor = catalog
        .load_export_descriptor(meta.name().clone())
        .await
        .expect("load descriptor");
    let engine = SimpleDurableEngine::load(
        &descriptor,
        LocalBlobStore::new(runtime.state_dir().join("blobs")),
        Arc::new(catalog.clone()),
    )
    .await
    .expect("simple durable engine");
    let export_runtime = ConcurrentExportRuntime::with_capacity(meta.clone(), Arc::new(engine), 4);

    (runtime, catalog, meta, export_runtime)
}

async fn execute_request(
    export_runtime: &ConcurrentExportRuntime,
    request: ExportRequest,
) -> ExportReply {
    let queue_slot = export_runtime.reserve().await.expect("reserve queue slot");
    let (job, receiver) = ExportJob::oneshot(request, queue_slot);
    export_runtime.submit(job).await.expect("submit request");
    let completed = receiver.await.expect("receive completion");
    let (result, _queue_slot) = completed.into_parts();
    result.expect("export reply")
}

async fn simple_chunk_key(
    catalog: &SQLiteExportCatalog,
    meta: &ExportRecord,
    index: u64,
) -> BlobKey {
    catalog
        .load_simple_tree(meta.id())
        .await
        .expect("load simple tree")
        .chunk(ChunkIndex::new(index))
        .expect("materialized chunk")
        .blob_key()
        .clone()
}
