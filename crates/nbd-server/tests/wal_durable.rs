use nbd_control_plane::{
    CatalogUrl, ChunkIndex, CowChunkRef, CowTreeMetadataStore, CreateExport, ExportCatalog,
    ExportEngineKind, ExportHead, ExportId, ExportLayoutKind, ExportName, ExportRecord,
    ExportState, NodeId, PublishCompaction, SQLiteExportCatalog, Timestamp, WalSeq,
    TREE_CHUNK_BYTES,
};
use nbd_server::{
    ConcurrentExportRuntime, ExportJob, ExportReply, ExportRequest, ExportRuntime, ExportWalHandle,
    LocalBlobStore, LocalWalProvider, OpenWal, Result, ServerError, WalDomain, WalDurableEngine,
    WalProvider, WalRequest,
};
use nbd_test_support::TestRuntime;
use std::sync::Arc;

const MIGRATIONS: &[&str] = &[include_str!(
    "../../../prisma/migrations/20260506000000_baseline/migration.sql"
)];

#[tokio::test]
async fn wal_durable_engine_reads_zeroes_then_written_overlay() {
    let (_runtime, wal, meta, export_runtime) =
        wal_durable_runtime("disk-a", "export-a", 4096).await;

    assert_eq!(
        execute_request(&export_runtime, ExportRequest::Read { offset: 0, len: 8 })
            .await
            .expect("zero read"),
        ExportReply::Read { data: vec![0; 8] },
    );
    assert_eq!(
        execute_request(
            &export_runtime,
            ExportRequest::Write {
                offset: 2,
                data: b"abcd".to_vec(),
            },
        )
        .await
        .expect("write"),
        ExportReply::Done,
    );
    assert_eq!(
        execute_request(&export_runtime, ExportRequest::Read { offset: 0, len: 8 })
            .await
            .expect("read back"),
        ExportReply::Read {
            data: b"\0\0abcd\0\0".to_vec(),
        },
    );
    assert_eq!(
        execute_request(&export_runtime, ExportRequest::Flush)
            .await
            .expect("flush"),
        ExportReply::Done,
    );
    assert_eq!(
        wal.bounds().await.expect("bounds").last_durable,
        WalSeq::new(1),
    );
    assert_eq!(export_runtime.export_record(), meta);

    export_runtime.close().await.expect("close runtime");
}

#[tokio::test]
async fn wal_durable_engine_replays_retained_records_on_open() {
    let runtime = TestRuntime::new().expect("test runtime");
    let meta = export_record("disk-replay", "export-replay", 4096);
    let wal = open_wal(&runtime, "export-replay").await;
    wal.append(
        WalRequest::new(nbd_server::ByteRange::new(1, 3), b"abc".to_vec())
            .expect("first WAL request"),
    )
    .await
    .expect("append first");
    wal.append(
        WalRequest::new(nbd_server::ByteRange::new(2, 2), b"ZZ".to_vec())
            .expect("second WAL request"),
    )
    .await
    .expect("append second");
    let engine = Arc::new(
        WalDurableEngine::open(&meta, wal)
            .await
            .expect("wal durable engine"),
    );
    let export_runtime = ConcurrentExportRuntime::with_capacity(meta, engine, 4);

    assert_eq!(
        execute_request(&export_runtime, ExportRequest::Read { offset: 0, len: 6 })
            .await
            .expect("read replayed view"),
        ExportReply::Read {
            data: b"\0aZZ\0\0".to_vec(),
        },
    );

    export_runtime.close().await.expect("close runtime");
}

#[tokio::test]
async fn wal_durable_engine_reopen_recovers_runtime_write() {
    let (runtime, _wal, meta, export_runtime) =
        wal_durable_runtime("disk-recover", "export-recover", 4096).await;

    assert_eq!(
        execute_request(
            &export_runtime,
            ExportRequest::Write {
                offset: 8,
                data: b"persist".to_vec(),
            },
        )
        .await
        .expect("write"),
        ExportReply::Done,
    );
    export_runtime.close().await.expect("close first runtime");

    let reopened_wal = open_wal(&runtime, "export-recover").await;
    let reopened_engine = Arc::new(
        WalDurableEngine::open(&meta, reopened_wal)
            .await
            .expect("reopen wal durable engine"),
    );
    let reopened_runtime = ConcurrentExportRuntime::with_capacity(meta, reopened_engine, 4);

    assert_eq!(
        execute_request(
            &reopened_runtime,
            ExportRequest::Read { offset: 4, len: 16 },
        )
        .await
        .expect("read recovered write"),
        ExportReply::Read {
            data: b"\0\0\0\0persist\0\0\0\0\0".to_vec(),
        },
    );

    reopened_runtime
        .close()
        .await
        .expect("close reopened runtime");
}

#[tokio::test]
async fn wal_durable_engine_uses_current_cow_root_from_descriptor() {
    let runtime = TestRuntime::new().expect("test runtime");
    let catalog = migrated_catalog(&runtime).await;
    let created = catalog
        .create_export(
            CreateExport::new(
                ExportName::new("disk-cow").expect("export name"),
                TREE_CHUNK_BYTES,
                4096,
                ExportEngineKind::WalDurable,
            )
            .expect("create export"),
        )
        .await
        .expect("create wal export");
    let blob_store = LocalBlobStore::new(runtime.root_path().join("blobs"));
    let descriptor = catalog
        .load_export_descriptor(created.name().clone())
        .await
        .expect("load descriptor");
    let mut chunk = vec![0; TREE_CHUNK_BYTES as usize];
    chunk[4..8].copy_from_slice(b"base");
    let key = blob_store.create_blob(&chunk).await.expect("create blob");
    let wal = open_wal(&runtime, created.id().as_str()).await;
    wal.append(
        WalRequest::new(nbd_server::ByteRange::new(4, 2), b"ba".to_vec())
            .expect("first WAL request"),
    )
    .await
    .expect("append first");
    wal.append(
        WalRequest::new(nbd_server::ByteRange::new(6, 2), b"se".to_vec())
            .expect("second WAL request"),
    )
    .await
    .expect("append second");
    catalog
        .publish_compaction(
            PublishCompaction::new(
                created.id().clone(),
                created.head().clone(),
                WalSeq::new(2),
                vec![
                    CowChunkRef::new(ChunkIndex::new(0), key, TREE_CHUNK_BYTES).expect("cow chunk")
                ],
            )
            .expect("publish compaction"),
        )
        .await
        .expect("publish cow checkpoint");
    wal.append(
        WalRequest::new(nbd_server::ByteRange::new(5, 2), b"ZZ".to_vec())
            .expect("overlay WAL request"),
    )
    .await
    .expect("append overlay");
    let engine = Arc::new(
        WalDurableEngine::open_with_cow_tree(
            &descriptor,
            wal,
            blob_store,
            Arc::new(catalog.clone()) as Arc<dyn CowTreeMetadataStore>,
        )
        .await
        .expect("wal durable engine"),
    );
    let head = engine.export_head().await.expect("engine head");
    let meta = descriptor.into_record(head).expect("runtime meta");
    let export_runtime = ConcurrentExportRuntime::with_capacity(meta, engine, 4);

    assert_eq!(
        execute_request(&export_runtime, ExportRequest::Read { offset: 4, len: 4 })
            .await
            .expect("read committed plus overlay"),
        ExportReply::Read {
            data: b"bZZe".to_vec(),
        },
    );

    export_runtime.close().await.expect("close runtime");
}

#[tokio::test]
async fn wal_durable_engine_rejects_out_of_bounds_ranges() {
    let (_runtime, _wal, _meta, export_runtime) =
        wal_durable_runtime("disk-bounds", "export-bounds", 8).await;

    assert!(matches!(
        execute_request(&export_runtime, ExportRequest::Read { offset: 7, len: 2 }).await,
        Err(ServerError::OutOfBounds {
            operation: "read",
            offset: 7,
            length: 2,
            size_bytes: 8,
        }),
    ));
    assert!(matches!(
        execute_request(
            &export_runtime,
            ExportRequest::Write {
                offset: 8,
                data: b"x".to_vec(),
            },
        )
        .await,
        Err(ServerError::OutOfBounds {
            operation: "write",
            offset: 8,
            length: 1,
            size_bytes: 8,
        }),
    ));

    export_runtime.close().await.expect("close runtime");
}

#[tokio::test]
async fn wal_durable_zero_backing_open_rejects_committed_root() {
    let runtime = TestRuntime::new().expect("test runtime");
    let head = ExportHead::new(
        ExportLayoutKind::CowImmutableTree,
        Some(NodeId::new("root-node").expect("node id")),
        4096,
        WalSeq::zero(),
    )
    .expect("head");
    let meta = ExportRecord::new(
        ExportId::new("export-root").expect("export id"),
        ExportName::new("disk-root").expect("export name"),
        4096,
        ExportEngineKind::WalDurable,
        ExportState::Active,
        head,
        Timestamp::new("created").expect("created timestamp"),
        Timestamp::new("updated").expect("updated timestamp"),
        None,
    )
    .expect("export meta");

    assert!(matches!(
        WalDurableEngine::open(&meta, open_wal(&runtime, "export-root").await).await,
        Err(ServerError::Catalog { .. }),
    ));
}

async fn wal_durable_runtime(
    name: &str,
    export_id: &str,
    size_bytes: u64,
) -> (
    TestRuntime,
    ExportWalHandle,
    ExportRecord,
    ConcurrentExportRuntime,
) {
    let runtime = TestRuntime::new().expect("test runtime");
    let meta = export_record(name, export_id, size_bytes);
    let wal = open_wal(&runtime, export_id).await;
    let engine = Arc::new(
        WalDurableEngine::open(&meta, wal.clone())
            .await
            .expect("wal durable engine"),
    );
    let export_runtime = ConcurrentExportRuntime::with_capacity(meta.clone(), engine, 4);

    (runtime, wal, meta, export_runtime)
}

async fn execute_request(
    export_runtime: &ConcurrentExportRuntime,
    request: ExportRequest,
) -> Result<ExportReply> {
    let queue_slot = export_runtime.reserve().await?;
    let (job, receiver) = ExportJob::oneshot(request, queue_slot);
    export_runtime.submit(job).await?;
    let completed = receiver.await.expect("receive completion");
    let (result, _queue_slot) = completed.into_parts();
    result
}

async fn open_wal(runtime: &TestRuntime, export_id: &str) -> ExportWalHandle {
    let provider = LocalWalProvider::new(runtime.wal_dir());
    provider
        .open_export(OpenWal::new(WalDomain::for_export_id(
            ExportId::new(export_id).expect("export id"),
        )))
        .await
        .expect("open WAL")
}

async fn migrated_catalog(runtime: &TestRuntime) -> SQLiteExportCatalog {
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
}

fn export_record(name: &str, export_id: &str, size_bytes: u64) -> ExportRecord {
    ExportRecord::new(
        ExportId::new(export_id).expect("export id"),
        ExportName::new(name).expect("export name"),
        4096,
        ExportEngineKind::WalDurable,
        ExportState::Active,
        ExportHead::cow_immutable_tree(size_bytes).expect("cow head"),
        Timestamp::new("created").expect("created timestamp"),
        Timestamp::new("updated").expect("updated timestamp"),
        None,
    )
    .expect("export meta")
}
