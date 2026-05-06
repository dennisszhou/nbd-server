use nbd_control_plane::{
    CatalogError, CatalogUrl, ChunkIndex, CowChunkRef, CowTreeMetadataStore, CowTreeSnapshot,
    CreateExport, ExportCatalog, ExportEngineKind, ExportHead, ExportId, ExportLayoutKind,
    ExportName, ExportRecord, ExportState, NodeId, PublishCompaction, PublishCompactionOutcome,
    SQLiteExportCatalog, TREE_CHUNK_BYTES, Timestamp, WalSeq,
};
use nbd_server::{
    ConcurrentExportRuntime, ExportJob, ExportReply, ExportRequest, ExportRuntime, ExportWalHandle,
    LocalBlobStore, LocalWalProvider, OpenWal, Result, ServerError, WalDomain, WalDurableEngine,
    WalProvider, WalRequest,
};
use nbd_test_support::TestRuntime;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::Notify;
use tokio::time::timeout;

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
                    CowChunkRef::new(ChunkIndex::new(0), key, TREE_CHUNK_BYTES).expect("cow chunk"),
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
            LocalBlobStore::new(runtime.root_path().join("blobs")),
            Arc::new(catalog.clone()) as Arc<dyn CowTreeMetadataStore>,
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

    let snapshot = catalog
        .load_cow_tree(created.id())
        .await
        .expect("load compacted snapshot");
    assert!(snapshot.root_node_id().is_some());
    assert_eq!(snapshot.base_wal_seq(), WalSeq::new(2));
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
            LocalBlobStore::new(runtime.root_path().join("blobs")),
            Arc::new(catalog.clone()) as Arc<dyn CowTreeMetadataStore>,
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
async fn wal_durable_write_pressure_compacts_when_debt_reaches_threshold() {
    let runtime = TestRuntime::new().expect("test runtime");
    let catalog = migrated_catalog(&runtime).await;
    let cow_tree_store = Arc::new(catalog.clone()) as Arc<dyn CowTreeMetadataStore>;
    let (created, wal, engine, export_runtime) =
        wal_durable_cow_runtime(&runtime, &catalog, "disk-pressure", cow_tree_store, 5).await;

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
            .load_cow_tree(created.id())
            .await
            .expect("load pre-threshold tree")
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

    let snapshot = catalog
        .load_cow_tree(created.id())
        .await
        .expect("load compacted tree");
    assert!(snapshot.root_node_id().is_some());
    assert_eq!(snapshot.base_wal_seq(), WalSeq::new(2));
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
        failing_store.clone() as Arc<dyn CowTreeMetadataStore>,
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

    let snapshot = catalog
        .load_cow_tree(created.id())
        .await
        .expect("load tree after failed compaction");
    assert_eq!(snapshot.base_wal_seq(), WalSeq::zero());
    assert!(snapshot.root_node_id().is_none());
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
    let blocking_store = Arc::new(BlockingCowTreeStore::new(catalog.clone()));
    let (_created, _wal, engine, export_runtime) = wal_durable_cow_runtime(
        &runtime,
        &catalog,
        "disk-pressure-block",
        blocking_store.clone() as Arc<dyn CowTreeMetadataStore>,
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
    cow_tree_store: Arc<dyn CowTreeMetadataStore>,
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
    let engine = Arc::new(
        WalDurableEngine::open_with_cow_tree_and_wal_debt_threshold(
            &descriptor,
            wal.clone(),
            LocalBlobStore::new(runtime.root_path().join("blobs")),
            cow_tree_store,
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
impl CowTreeMetadataStore for FailingCowTreeStore {
    async fn load_cow_tree(
        &self,
        export_id: &ExportId,
    ) -> nbd_control_plane::Result<CowTreeSnapshot> {
        if self.fail_calls.load(Ordering::SeqCst) {
            self.mark_attempted();
            return Err(CatalogError::database("injected compaction load failure"));
        }
        self.inner.load_cow_tree(export_id).await
    }

    async fn publish_compaction(
        &self,
        request: PublishCompaction,
    ) -> nbd_control_plane::Result<PublishCompactionOutcome> {
        if self.fail_calls.load(Ordering::SeqCst) {
            self.mark_attempted();
            return Err(CatalogError::database(
                "injected compaction publish failure",
            ));
        }
        self.inner.publish_compaction(request).await
    }
}

struct BlockingCowTreeStore {
    inner: SQLiteExportCatalog,
    publish_count: AtomicUsize,
    publish_started: Notify,
    release_publish: Notify,
}

impl BlockingCowTreeStore {
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
impl CowTreeMetadataStore for BlockingCowTreeStore {
    async fn load_cow_tree(
        &self,
        export_id: &ExportId,
    ) -> nbd_control_plane::Result<CowTreeSnapshot> {
        self.inner.load_cow_tree(export_id).await
    }

    async fn publish_compaction(
        &self,
        request: PublishCompaction,
    ) -> nbd_control_plane::Result<PublishCompactionOutcome> {
        let count = self.publish_count.fetch_add(1, Ordering::SeqCst) + 1;
        self.publish_started.notify_waiters();
        if count == 1 {
            self.release_publish.notified().await;
        }
        self.inner.publish_compaction(request).await
    }
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
