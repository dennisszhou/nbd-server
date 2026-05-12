use nbd_control_plane::{
    CatalogError, CatalogUrl, ChunkIndex, CowChunkRef, CreateExport, ExportCatalog,
    ExportEngineKind, ExportHead, ExportId, ExportLayoutKind, ExportName, ExportRecord,
    ExportState, NodeId, PublishTreeUpdate, PublishTreeUpdateOutcome, TREE_CHUNK_BYTES, Timestamp,
    TreeEdgeLookup, TreeEdgeRecord, TreeLeafRefRecord, TreeNodeKind, TreeNodeRecord,
    TreeRecordBatch, TreeRecordStore, TreeStorageKind, WalSeq,
};
use nbd_control_plane_sqlite::SQLiteExportCatalog;
use nbd_server::{
    BlobStoreHandle, ConcurrentExportRuntime, ExportJob, ExportReply, ExportRequest, ExportRuntime,
    ExportWalHandle, LocalBlobStore, LocalWalProvider, OpenWal, Result, ServerError, WalDomain,
    WalDurableEngine, WalProvider, WalRequest, put_random_blob,
};
use nbd_test_support::TestRuntime;
use std::fs;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::Notify;
use tokio::time::timeout;

const MIGRATIONS: &[&str] = &[
    include_str!("../../../prisma/migrations/20260506000000_baseline/migration.sql"),
    include_str!("../../../prisma/migrations/20260512000000_tree_format/migration.sql"),
];

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
async fn wal_durable_engine_tracks_wal_debt_for_replay_and_writes() {
    let runtime = TestRuntime::new().expect("test runtime");
    let meta = export_record("disk-debt", "export-debt", 4096);
    let wal = open_wal(&runtime, "export-debt").await;
    wal.append(
        WalRequest::new(nbd_server::ByteRange::new(1, 3), b"abc".to_vec())
            .expect("first WAL request"),
    )
    .await
    .expect("append first");
    wal.append(
        WalRequest::new(nbd_server::ByteRange::new(8, 2), b"de".to_vec())
            .expect("second WAL request"),
    )
    .await
    .expect("append second");
    let engine = Arc::new(
        WalDurableEngine::open(&meta, wal)
            .await
            .expect("wal durable engine"),
    );
    assert_eq!(engine.wal_debt_bytes().await, 5);

    let export_runtime = ConcurrentExportRuntime::with_capacity(meta, engine.clone(), 4);
    assert_eq!(
        execute_request(
            &export_runtime,
            ExportRequest::Write {
                offset: 16,
                data: b"more".to_vec(),
            },
        )
        .await
        .expect("write"),
        ExportReply::Done,
    );
    assert_eq!(engine.wal_debt_bytes().await, 9);

    export_runtime.close().await.expect("close runtime");
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
    let blob_store: BlobStoreHandle =
        Arc::new(LocalBlobStore::new(runtime.root_path().join("blobs")));
    let descriptor = catalog
        .load_export_descriptor(created.name().clone())
        .await
        .expect("load descriptor");
    let mut chunk = vec![0; TREE_CHUNK_BYTES as usize];
    chunk[4..8].copy_from_slice(b"base");
    let key = put_random_blob(blob_store.as_ref(), &chunk)
        .await
        .expect("create blob");
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
    publish_cow_root(
        &catalog,
        &created,
        2,
        vec![CowChunkRef::new(ChunkIndex::new(0), key, TREE_CHUNK_BYTES).expect("cow chunk")],
    )
    .await;
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
            Arc::new(catalog.clone()) as Arc<dyn TreeRecordStore>,
            catalog
                .load_export_head(created.id())
                .await
                .expect("load export head"),
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
async fn wal_durable_engine_close_compacts_applied_writes_and_advances_read_view() {
    let runtime = TestRuntime::new().expect("test runtime");
    let catalog = migrated_catalog(&runtime).await;
    let created = catalog
        .create_export(
            CreateExport::new(
                ExportName::new("disk-close").expect("export name"),
                TREE_CHUNK_BYTES,
                4096,
                ExportEngineKind::WalDurable,
            )
            .expect("create export"),
        )
        .await
        .expect("create wal export");
    let descriptor = catalog
        .load_export_descriptor(created.name().clone())
        .await
        .expect("load descriptor");
    let wal = open_wal(&runtime, created.id().as_str()).await;
    let engine = Arc::new(
        WalDurableEngine::open_with_cow_tree(
            &descriptor,
            wal.clone(),
            Arc::new(LocalBlobStore::new(runtime.root_path().join("blobs"))),
            Arc::new(catalog.clone()) as Arc<dyn TreeRecordStore>,
            catalog
                .load_export_head(created.id())
                .await
                .expect("load export head"),
        )
        .await
        .expect("wal durable engine"),
    );
    let head = engine.export_head().await.expect("engine head");
    let meta = descriptor.clone().into_record(head).expect("runtime meta");
    let export_runtime = ConcurrentExportRuntime::with_capacity(meta, engine.clone(), 4);

    assert_eq!(
        execute_request(
            &export_runtime,
            ExportRequest::Write {
                offset: 0,
                data: b"close".to_vec(),
            },
        )
        .await
        .expect("first write"),
        ExportReply::Done,
    );
    assert_eq!(
        execute_request(
            &export_runtime,
            ExportRequest::Write {
                offset: 8,
                data: b"done".to_vec(),
            },
        )
        .await
        .expect("second write"),
        ExportReply::Done,
    );

    export_runtime.close().await.expect("close runtime");

    let compacted_head = catalog
        .load_export_head(created.id())
        .await
        .expect("load compacted head");
    assert!(compacted_head.root_node_id().is_some());
    assert_eq!(compacted_head.base_wal_seq(), WalSeq::new(2));
    assert_eq!(
        engine
            .export_head()
            .await
            .expect("engine read view head")
            .base_wal_seq(),
        WalSeq::new(2),
    );
    assert_eq!(engine.wal_debt_bytes().await, 0);
    assert_eq!(
        wal.bounds().await.expect("WAL bounds").pruned_through,
        WalSeq::new(2),
    );

    let reopened_descriptor = catalog
        .load_export_descriptor(created.name().clone())
        .await
        .expect("load reopened descriptor");
    let reopened_engine = Arc::new(
        WalDurableEngine::open_with_cow_tree(
            &reopened_descriptor,
            open_wal(&runtime, created.id().as_str()).await,
            Arc::new(LocalBlobStore::new(runtime.root_path().join("blobs"))),
            Arc::new(catalog.clone()) as Arc<dyn TreeRecordStore>,
            catalog
                .load_export_head(created.id())
                .await
                .expect("load reopened export head"),
        )
        .await
        .expect("reopen wal durable engine"),
    );
    let reopened_head = reopened_engine
        .export_head()
        .await
        .expect("reopened engine head");
    let reopened_meta = reopened_descriptor
        .into_record(reopened_head)
        .expect("reopened runtime meta");
    let reopened_runtime =
        ConcurrentExportRuntime::with_capacity(reopened_meta, reopened_engine, 4);

    assert_eq!(
        execute_request(
            &reopened_runtime,
            ExportRequest::Read { offset: 0, len: 12 },
        )
        .await
        .expect("read compacted data"),
        ExportReply::Read {
            data: b"close\0\0\0done".to_vec(),
        },
    );

    reopened_runtime
        .close()
        .await
        .expect("close reopened runtime");
}

#[tokio::test]
async fn wal_durable_compaction_preserves_unchanged_committed_chunks() {
    let runtime = TestRuntime::new().expect("test runtime");
    let catalog = migrated_catalog(&runtime).await;
    let created = catalog
        .create_export(
            CreateExport::new(
                ExportName::new("disk-cow-preserve").expect("export name"),
                TREE_CHUNK_BYTES * 2,
                4096,
                ExportEngineKind::WalDurable,
            )
            .expect("create export"),
        )
        .await
        .expect("create wal export");
    let descriptor = catalog
        .load_export_descriptor(created.name().clone())
        .await
        .expect("load descriptor");
    let blob_store: BlobStoreHandle =
        Arc::new(LocalBlobStore::new(runtime.root_path().join("blobs")));
    let wal = open_wal(&runtime, created.id().as_str()).await;
    wal.append(
        WalRequest::new(nbd_server::ByteRange::new(0, 1), b"x".to_vec()).expect("seed WAL request"),
    )
    .await
    .expect("append seed");
    let mut first_chunk = vec![0; TREE_CHUNK_BYTES as usize];
    first_chunk[..4].copy_from_slice(b"keep");
    let first_key = put_random_blob(blob_store.as_ref(), &first_chunk)
        .await
        .expect("write first chunk");
    let mut second_chunk = vec![0; TREE_CHUNK_BYTES as usize];
    second_chunk[4..8].copy_from_slice(b"base");
    let second_key = put_random_blob(blob_store.as_ref(), &second_chunk)
        .await
        .expect("write second chunk");
    publish_cow_root(
        &catalog,
        &created,
        1,
        vec![
            CowChunkRef::new(ChunkIndex::new(0), first_key, TREE_CHUNK_BYTES)
                .expect("first cow chunk"),
            CowChunkRef::new(ChunkIndex::new(1), second_key, TREE_CHUNK_BYTES)
                .expect("second cow chunk"),
        ],
    )
    .await;

    let engine = Arc::new(
        WalDurableEngine::open_with_cow_tree(
            &descriptor,
            wal,
            blob_store.clone(),
            Arc::new(catalog.clone()) as Arc<dyn TreeRecordStore>,
            catalog
                .load_export_head(created.id())
                .await
                .expect("load export head"),
        )
        .await
        .expect("wal durable engine"),
    );
    let head = engine.export_head().await.expect("engine head");
    let meta = descriptor.clone().into_record(head).expect("runtime meta");
    let export_runtime = ConcurrentExportRuntime::with_capacity(meta, engine, 4);

    assert_eq!(
        execute_request(
            &export_runtime,
            ExportRequest::Write {
                offset: TREE_CHUNK_BYTES + 6,
                data: b"ZZ".to_vec(),
            },
        )
        .await
        .expect("overlay second chunk"),
        ExportReply::Done,
    );
    export_runtime.close().await.expect("close runtime");

    let compacted_head = catalog
        .load_export_head(created.id())
        .await
        .expect("load compacted head");
    assert_eq!(compacted_head.base_wal_seq(), WalSeq::new(2));
    let root = compacted_head.root_node_id().expect("compacted root");
    let preserved = load_cow_chunk(&catalog, root, 0).await.expect("chunk zero");
    assert_eq!(
        blob_store
            .get_blob(preserved.blob_key(), 0, 4)
            .await
            .expect("read preserved chunk"),
        b"keep",
    );
    let rewritten = load_cow_chunk(&catalog, root, 1).await.expect("chunk one");
    assert_eq!(
        blob_store
            .get_blob(rewritten.blob_key(), 4, 4)
            .await
            .expect("read rewritten chunk"),
        b"baZZ",
    );
}

#[tokio::test]
async fn wal_durable_compaction_supports_multi_level_sparse_roots() {
    const ONE_TIB: u64 = 1024 * 1024 * 1024 * 1024;

    let runtime = TestRuntime::new().expect("test runtime");
    let catalog = migrated_catalog(&runtime).await;
    let created = catalog
        .create_export(
            CreateExport::new(
                ExportName::new("disk-cow-large").expect("export name"),
                ONE_TIB,
                4096,
                ExportEngineKind::WalDurable,
            )
            .expect("create export"),
        )
        .await
        .expect("create wal export");
    let descriptor = catalog
        .load_export_descriptor(created.name().clone())
        .await
        .expect("load descriptor");
    let blob_store: BlobStoreHandle =
        Arc::new(LocalBlobStore::new(runtime.root_path().join("blobs")));
    let engine = Arc::new(
        WalDurableEngine::open_with_cow_tree(
            &descriptor,
            open_wal(&runtime, created.id().as_str()).await,
            blob_store.clone(),
            Arc::new(catalog.clone()) as Arc<dyn TreeRecordStore>,
            catalog
                .load_export_head(created.id())
                .await
                .expect("load export head"),
        )
        .await
        .expect("wal durable engine"),
    );
    let head = engine.export_head().await.expect("engine head");
    let meta = descriptor.clone().into_record(head).expect("runtime meta");
    let export_runtime = ConcurrentExportRuntime::with_capacity(meta, engine, 4);

    assert_eq!(
        execute_request(
            &export_runtime,
            ExportRequest::Write {
                offset: ONE_TIB - 4,
                data: b"tail".to_vec(),
            },
        )
        .await
        .expect("write tail chunk"),
        ExportReply::Done,
    );
    export_runtime.close().await.expect("close runtime");

    let published = catalog
        .load_export_head(created.id())
        .await
        .expect("load published head");
    assert!(published.root_node_id().is_some());
    assert_eq!(published.base_wal_seq(), WalSeq::new(1));
    let reopened_descriptor = catalog
        .load_export_descriptor(created.name().clone())
        .await
        .expect("load reopened descriptor");
    let reopened_engine = Arc::new(
        WalDurableEngine::open_with_cow_tree(
            &reopened_descriptor,
            open_wal(&runtime, created.id().as_str()).await,
            blob_store,
            Arc::new(catalog.clone()) as Arc<dyn TreeRecordStore>,
            published,
        )
        .await
        .expect("reopen wal durable engine"),
    );
    let reopened_head = reopened_engine
        .export_head()
        .await
        .expect("reopened engine head");
    let reopened_meta = reopened_descriptor
        .into_record(reopened_head)
        .expect("reopened runtime meta");
    let reopened_runtime =
        ConcurrentExportRuntime::with_capacity(reopened_meta, reopened_engine, 4);

    assert_eq!(
        execute_request(
            &reopened_runtime,
            ExportRequest::Read {
                offset: ONE_TIB - 4,
                len: 4,
            },
        )
        .await
        .expect("read tail chunk"),
        ExportReply::Read {
            data: b"tail".to_vec(),
        },
    );
    reopened_runtime
        .close()
        .await
        .expect("close reopened runtime");
}

#[tokio::test]
async fn wal_durable_write_pressure_compacts_when_debt_reaches_threshold() {
    let runtime = TestRuntime::new().expect("test runtime");
    let catalog = migrated_catalog(&runtime).await;
    let tree_store = Arc::new(catalog.clone()) as Arc<dyn TreeRecordStore>;
    let (created, wal, engine, export_runtime) =
        wal_durable_cow_runtime(&runtime, &catalog, "disk-pressure", tree_store, 5).await;

    assert_eq!(
        execute_request(
            &export_runtime,
            ExportRequest::Write {
                offset: 0,
                data: b"abcd".to_vec(),
            },
        )
        .await
        .expect("first write"),
        ExportReply::Done,
    );
    assert_eq!(engine.wal_debt_bytes().await, 4);
    assert_eq!(
        catalog
            .load_export_head(created.id())
            .await
            .expect("load pre-threshold head")
            .base_wal_seq(),
        WalSeq::zero(),
    );

    assert_eq!(
        execute_request(
            &export_runtime,
            ExportRequest::Write {
                offset: 4,
                data: b"e".to_vec(),
            },
        )
        .await
        .expect("threshold write"),
        ExportReply::Done,
    );

    let compacted_head = catalog
        .load_export_head(created.id())
        .await
        .expect("load compacted head");
    assert!(compacted_head.root_node_id().is_some());
    assert_eq!(compacted_head.base_wal_seq(), WalSeq::new(2));
    assert_eq!(engine.wal_debt_bytes().await, 0);
    assert_eq!(
        wal.bounds().await.expect("WAL bounds").pruned_through,
        WalSeq::new(2),
    );
    assert_eq!(
        execute_request(&export_runtime, ExportRequest::Read { offset: 0, len: 5 })
            .await
            .expect("read compacted write"),
        ExportReply::Read {
            data: b"abcde".to_vec(),
        },
    );

    export_runtime.close().await.expect("close runtime");
}

#[tokio::test]
async fn wal_durable_write_pressure_failure_preserves_successful_write() {
    let runtime = TestRuntime::new().expect("test runtime");
    let catalog = migrated_catalog(&runtime).await;
    let failing_store = Arc::new(FailingCowTreeStore::new(catalog.clone()));
    let (created, _wal, engine, export_runtime) = wal_durable_cow_runtime(
        &runtime,
        &catalog,
        "disk-pressure-fail",
        failing_store.clone() as Arc<dyn TreeRecordStore>,
        5,
    )
    .await;

    failing_store.fail_future_calls();
    assert_eq!(
        execute_request(
            &export_runtime,
            ExportRequest::Write {
                offset: 0,
                data: b"fails".to_vec(),
            },
        )
        .await
        .expect("write remains successful"),
        ExportReply::Done,
    );
    failing_store.wait_for_attempt().await;

    let head = catalog
        .load_export_head(created.id())
        .await
        .expect("load head after failed compaction");
    assert_eq!(head.base_wal_seq(), WalSeq::zero());
    assert!(head.root_node_id().is_none());
    assert_eq!(engine.wal_debt_bytes().await, 5);
    assert_eq!(
        execute_request(&export_runtime, ExportRequest::Read { offset: 0, len: 5 })
            .await
            .expect("read retained overlay"),
        ExportReply::Read {
            data: b"fails".to_vec(),
        },
    );

    failing_store.allow_future_calls();
    export_runtime.close().await.expect("close runtime");
}

#[tokio::test]
async fn wal_durable_write_pressure_blocks_later_writes_until_compaction_finishes() {
    let runtime = TestRuntime::new().expect("test runtime");
    let catalog = migrated_catalog(&runtime).await;
    let blocking_store = Arc::new(BlockingTreeRecordStore::new(catalog.clone()));
    let (_created, _wal, engine, export_runtime) = wal_durable_cow_runtime(
        &runtime,
        &catalog,
        "disk-pressure-block",
        blocking_store.clone() as Arc<dyn TreeRecordStore>,
        5,
    )
    .await;

    let first_slot = export_runtime.reserve().await.expect("reserve first");
    let (first_job, first_receiver) = ExportJob::oneshot(
        ExportRequest::Write {
            offset: 0,
            data: b"first".to_vec(),
        },
        first_slot,
    );
    export_runtime
        .submit(first_job)
        .await
        .expect("submit first write");
    blocking_store.wait_for_publish_count(1).await;
    assert_eq!(
        timeout(
            Duration::from_secs(5),
            execute_request(
                &export_runtime,
                ExportRequest::Read {
                    offset: 1024,
                    len: 1
                }
            ),
        )
        .await
        .expect("independent read should not wait for compaction")
        .expect("independent read"),
        ExportReply::Read { data: vec![0] },
    );

    let second_slot = export_runtime.reserve().await.expect("reserve second");
    let (second_job, mut second_receiver) = ExportJob::oneshot(
        ExportRequest::Write {
            offset: 16,
            data: b"x".to_vec(),
        },
        second_slot,
    );
    export_runtime
        .submit(second_job)
        .await
        .expect("submit second write");
    assert!(
        timeout(Duration::from_millis(50), &mut second_receiver)
            .await
            .is_err(),
        "second write completed while first write-pressure compaction was blocked",
    );

    blocking_store.release_first_publish();
    assert_done(first_receiver.await.expect("first completion"));
    assert_done(second_receiver.await.expect("second completion"));
    assert_eq!(engine.wal_debt_bytes().await, 1);
    assert_eq!(
        execute_request(&export_runtime, ExportRequest::Read { offset: 0, len: 17 })
            .await
            .expect("read both writes"),
        ExportReply::Read {
            data: b"first\0\0\0\0\0\0\0\0\0\0\0x".to_vec(),
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

async fn wal_durable_cow_runtime(
    runtime: &TestRuntime,
    catalog: &SQLiteExportCatalog,
    name: &str,
    tree_store: Arc<dyn TreeRecordStore>,
    wal_debt_threshold_bytes: u64,
) -> (
    ExportRecord,
    ExportWalHandle,
    Arc<WalDurableEngine>,
    ConcurrentExportRuntime,
) {
    let created = catalog
        .create_export(
            CreateExport::new(
                ExportName::new(name).expect("export name"),
                TREE_CHUNK_BYTES,
                4096,
                ExportEngineKind::WalDurable,
            )
            .expect("create export"),
        )
        .await
        .expect("create wal export");
    let descriptor = catalog
        .load_export_descriptor(created.name().clone())
        .await
        .expect("load descriptor");
    let wal = open_wal(runtime, created.id().as_str()).await;
    let head = catalog
        .load_export_head(created.id())
        .await
        .expect("load export head");
    let engine = Arc::new(
        WalDurableEngine::open_with_cow_tree_and_wal_debt_threshold(
            &descriptor,
            wal.clone(),
            Arc::new(LocalBlobStore::new(runtime.root_path().join("blobs"))),
            tree_store,
            head,
            wal_debt_threshold_bytes,
        )
        .await
        .expect("wal durable engine"),
    );
    let head = engine.export_head().await.expect("engine head");
    let meta = descriptor.into_record(head).expect("runtime meta");
    let export_runtime = ConcurrentExportRuntime::with_capacity(meta, engine.clone(), 4);

    (created, wal, engine, export_runtime)
}

async fn publish_cow_root(
    catalog: &SQLiteExportCatalog,
    export: &ExportRecord,
    checkpoint: u64,
    chunks: Vec<CowChunkRef>,
) -> ExportRecord {
    let root = NodeId::new(format!("{}-root-{checkpoint}", export.id())).expect("root node");
    let mut nodes = vec![TreeNodeRecord {
        id: root.clone(),
        layout_kind: ExportLayoutKind::CowImmutableTree,
        owner_export_id: None,
        kind: TreeNodeKind::Internal,
        level: 1,
        span_start_bytes: 0,
        span_len_bytes: export.size_bytes(),
    }];
    let mut edges = Vec::new();
    let mut leaf_refs = Vec::new();
    for chunk in chunks {
        let chunk_start = chunk.chunk_index().get() * TREE_CHUNK_BYTES;
        let slot = u16::try_from(chunk.chunk_index().get()).expect("test chunk slot");
        let leaf = NodeId::new(format!(
            "{}-leaf-{checkpoint}-{}",
            export.id(),
            chunk.chunk_index()
        ))
        .expect("leaf node");
        nodes.push(TreeNodeRecord {
            id: leaf.clone(),
            layout_kind: ExportLayoutKind::CowImmutableTree,
            owner_export_id: None,
            kind: TreeNodeKind::Leaf,
            level: 0,
            span_start_bytes: chunk_start,
            span_len_bytes: TREE_CHUNK_BYTES.min(export.size_bytes() - chunk_start),
        });
        edges.push(TreeEdgeRecord {
            parent_node_id: root.clone(),
            slot,
            child_node_id: leaf.clone(),
        });
        leaf_refs.push(TreeLeafRefRecord {
            node_id: leaf,
            storage_kind: TreeStorageKind::ImmutableBlob,
            storage_key: chunk.blob_key().clone(),
            len_bytes: chunk.len_bytes(),
        });
    }
    let next_head = ExportHead::new_with_tree_format(
        ExportLayoutKind::CowImmutableTree,
        Some(root),
        export.size_bytes(),
        WalSeq::new(checkpoint),
        export.head().tree_format(),
    )
    .expect("next head");
    let outcome = catalog
        .publish_tree_update(PublishTreeUpdate {
            export_id: export.id().clone(),
            expected_head: export.head().clone(),
            next_head,
            records: TreeRecordBatch {
                nodes,
                edges,
                leaf_refs,
            },
        })
        .await
        .expect("publish COW root");
    match outcome {
        PublishTreeUpdateOutcome::Published(record) => record,
        outcome => panic!("expected COW root publish, got {outcome:?}"),
    }
}

async fn load_cow_chunk(
    catalog: &SQLiteExportCatalog,
    root: &NodeId,
    slot: u64,
) -> Option<CowChunkRef> {
    let slot = u16::try_from(slot).expect("test chunk slot");
    let edge = catalog
        .load_child_edges(&[TreeEdgeLookup {
            parent_node_id: root.clone(),
            slots: vec![slot],
        }])
        .await
        .expect("load child edge")
        .into_iter()
        .next()?;
    let leaf_ref = catalog
        .load_leaf_refs(&[edge.child_node_id])
        .await
        .expect("load leaf ref")
        .into_iter()
        .next()?;
    assert_eq!(leaf_ref.storage_kind, TreeStorageKind::ImmutableBlob);
    Some(
        CowChunkRef::new(
            ChunkIndex::new(u64::from(slot)),
            leaf_ref.storage_key,
            leaf_ref.len_bytes,
        )
        .expect("cow chunk"),
    )
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

fn assert_done(completed: nbd_server::CompletedExport) {
    let (result, _queue_slot) = completed.into_parts();
    assert_eq!(result.expect("export reply"), ExportReply::Done);
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

struct FailingCowTreeStore {
    inner: SQLiteExportCatalog,
    fail_calls: AtomicBool,
    attempted: AtomicBool,
    notify: Notify,
}

impl FailingCowTreeStore {
    fn new(inner: SQLiteExportCatalog) -> Self {
        Self {
            inner,
            fail_calls: AtomicBool::new(false),
            attempted: AtomicBool::new(false),
            notify: Notify::new(),
        }
    }

    fn fail_future_calls(&self) {
        self.fail_calls.store(true, Ordering::SeqCst);
    }

    fn allow_future_calls(&self) {
        self.fail_calls.store(false, Ordering::SeqCst);
    }

    async fn wait_for_attempt(&self) {
        timeout(Duration::from_secs(5), async {
            loop {
                if self.attempted.load(Ordering::SeqCst) {
                    return;
                }
                self.notify.notified().await;
            }
        })
        .await
        .expect("wait for failed compaction attempt");
    }

    fn mark_attempted(&self) {
        self.attempted.store(true, Ordering::SeqCst);
        self.notify.notify_waiters();
    }
}

#[async_trait::async_trait]
impl TreeRecordStore for FailingCowTreeStore {
    async fn load_node(
        &self,
        node_id: &NodeId,
    ) -> nbd_control_plane::Result<Option<TreeNodeRecord>> {
        if self.fail_calls.load(Ordering::SeqCst) {
            self.mark_attempted();
            return Err(CatalogError::database(
                "injected compaction node load failure",
            ));
        }
        self.inner.load_node(node_id).await
    }

    async fn load_nodes(
        &self,
        node_ids: &[NodeId],
    ) -> nbd_control_plane::Result<Vec<TreeNodeRecord>> {
        if self.fail_calls.load(Ordering::SeqCst) {
            self.mark_attempted();
            return Err(CatalogError::database(
                "injected compaction node load failure",
            ));
        }
        self.inner.load_nodes(node_ids).await
    }

    async fn load_child_edges(
        &self,
        lookups: &[TreeEdgeLookup],
    ) -> nbd_control_plane::Result<Vec<TreeEdgeRecord>> {
        if self.fail_calls.load(Ordering::SeqCst) {
            self.mark_attempted();
            return Err(CatalogError::database(
                "injected compaction edge load failure",
            ));
        }
        self.inner.load_child_edges(lookups).await
    }

    async fn load_leaf_refs(
        &self,
        node_ids: &[NodeId],
    ) -> nbd_control_plane::Result<Vec<TreeLeafRefRecord>> {
        if self.fail_calls.load(Ordering::SeqCst) {
            self.mark_attempted();
            return Err(CatalogError::database(
                "injected compaction leaf load failure",
            ));
        }
        self.inner.load_leaf_refs(node_ids).await
    }

    async fn publish_tree_update(
        &self,
        request: PublishTreeUpdate,
    ) -> nbd_control_plane::Result<PublishTreeUpdateOutcome> {
        if self.fail_calls.load(Ordering::SeqCst) {
            self.mark_attempted();
            return Err(CatalogError::database(
                "injected compaction tree publish failure",
            ));
        }
        self.inner.publish_tree_update(request).await
    }
}

struct BlockingTreeRecordStore {
    inner: SQLiteExportCatalog,
    publish_count: AtomicUsize,
    publish_started: Notify,
    release_publish: Notify,
}

impl BlockingTreeRecordStore {
    fn new(inner: SQLiteExportCatalog) -> Self {
        Self {
            inner,
            publish_count: AtomicUsize::new(0),
            publish_started: Notify::new(),
            release_publish: Notify::new(),
        }
    }

    async fn wait_for_publish_count(&self, expected: usize) {
        timeout(Duration::from_secs(5), async {
            loop {
                if self.publish_count.load(Ordering::SeqCst) >= expected {
                    return;
                }
                self.publish_started.notified().await;
            }
        })
        .await
        .expect("wait for blocked compaction publish");
    }

    fn release_first_publish(&self) {
        self.release_publish.notify_waiters();
    }
}

#[async_trait::async_trait]
impl TreeRecordStore for BlockingTreeRecordStore {
    async fn load_node(
        &self,
        node_id: &NodeId,
    ) -> nbd_control_plane::Result<Option<TreeNodeRecord>> {
        self.inner.load_node(node_id).await
    }

    async fn load_nodes(
        &self,
        node_ids: &[NodeId],
    ) -> nbd_control_plane::Result<Vec<TreeNodeRecord>> {
        self.inner.load_nodes(node_ids).await
    }

    async fn load_child_edges(
        &self,
        lookups: &[TreeEdgeLookup],
    ) -> nbd_control_plane::Result<Vec<TreeEdgeRecord>> {
        self.inner.load_child_edges(lookups).await
    }

    async fn load_leaf_refs(
        &self,
        node_ids: &[NodeId],
    ) -> nbd_control_plane::Result<Vec<TreeLeafRefRecord>> {
        self.inner.load_leaf_refs(node_ids).await
    }

    async fn publish_tree_update(
        &self,
        request: PublishTreeUpdate,
    ) -> nbd_control_plane::Result<PublishTreeUpdateOutcome> {
        let count = self.publish_count.fetch_add(1, Ordering::SeqCst) + 1;
        self.publish_started.notify_waiters();
        if count == 1 {
            self.release_publish.notified().await;
        }
        self.inner.publish_tree_update(request).await
    }
}

async fn migrated_catalog(runtime: &TestRuntime) -> SQLiteExportCatalog {
    let url = CatalogUrl::parse(runtime.catalog_url()).expect("catalog URL");
    fs::File::create(url.sqlite_path().expect("sqlite path")).expect("create catalog file");
    let catalog = SQLiteExportCatalog::connect_path(url.sqlite_path().expect("sqlite path"))
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
