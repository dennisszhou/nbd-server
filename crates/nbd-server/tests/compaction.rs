use nbd_control_plane::{
    CatalogUrl, ChunkIndex, CowTreeMetadataStore, CowTreeSnapshot, CreateExport, ExportCatalog,
    ExportEngineKind, ExportId, ExportName, PublishCompaction, PublishCompactionOutcome,
    SQLiteExportCatalog, WalSeq, TREE_CHUNK_BYTES,
};
use nbd_server::{
    ByteRange, CompactionEnqueueOutcome, CompactionJob, CompactionManager, CompactionOutcome,
    ExportWalHandle, LocalBlobStore, LocalWalProvider, OpenWal, WalDomain, WalProvider, WalRequest,
};
use nbd_test_support::TestRuntime;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;
use tokio::time::timeout;

const MIGRATIONS: &[&str] = &[
    include_str!("../../../prisma/migrations/20260501000000_init/migration.sql"),
    include_str!(
        "../../../prisma/migrations/20260504000000_export_heads_tree_metadata/migration.sql"
    ),
    include_str!(
        "../../../prisma/migrations/20260504010000_simple_durable_engine_kind/migration.sql"
    ),
    include_str!("../../../prisma/migrations/20260505000000_wal_durable_engine_kind/migration.sql"),
    include_str!("../../../prisma/migrations/20260505010000_cow_tree_metadata/migration.sql"),
];

#[tokio::test]
async fn compaction_publishes_checkpoint_from_wal_records() {
    let fixture = CompactionFixture::new().await;
    let created = fixture.create_wal_export("disk-a", TREE_CHUNK_BYTES).await;
    let wal = fixture.open_wal(created.id()).await;
    append(&wal, 0, b"abcd").await;
    append(&wal, 2, b"ZZ").await;

    let result = fixture
        .manager
        .compact_export(CompactionJob::new(created.id().clone(), WalSeq::new(2)))
        .await
        .expect("compact export");

    assert_eq!(result.outcome(), CompactionOutcome::Published);
    assert_eq!(result.target_wal_seq(), WalSeq::new(2));
    assert_eq!(result.compacted_records(), 2);
    assert_eq!(result.written_leaf_blobs(), 1);
    let snapshot = fixture
        .catalog
        .load_cow_tree(created.id())
        .await
        .expect("load cow tree");
    assert!(snapshot.root_node_id().is_some());
    assert_eq!(snapshot.checkpoint_wal_seq(), WalSeq::new(2));
    let chunk = snapshot.chunk(ChunkIndex::new(0)).expect("chunk zero");
    assert_eq!(
        fixture
            .blob_store
            .read_blob(chunk.blob_key(), 0, 4)
            .await
            .expect("read compacted blob"),
        b"abZZ",
    );
}

#[tokio::test]
async fn compaction_preserves_unaffected_committed_chunks() {
    let fixture = CompactionFixture::new().await;
    let created = fixture
        .create_wal_export("disk-a", 2 * TREE_CHUNK_BYTES)
        .await;
    let wal = fixture.open_wal(created.id()).await;
    append(&wal, 0, b"keep").await;
    fixture
        .manager
        .compact_export(CompactionJob::new(created.id().clone(), WalSeq::new(1)))
        .await
        .expect("compact first chunk");
    let base = fixture
        .catalog
        .load_cow_tree(created.id())
        .await
        .expect("load base cow tree");
    let base_chunk = base.chunk(ChunkIndex::new(0)).expect("base chunk").clone();

    append(&wal, TREE_CHUNK_BYTES + 4, b"next").await;
    fixture
        .manager
        .compact_export(CompactionJob::new(created.id().clone(), WalSeq::new(2)))
        .await
        .expect("compact second chunk");

    let snapshot = fixture
        .catalog
        .load_cow_tree(created.id())
        .await
        .expect("load cow tree");
    assert_eq!(snapshot.checkpoint_wal_seq(), WalSeq::new(2));
    assert_eq!(snapshot.chunk(ChunkIndex::new(0)), Some(&base_chunk));
    assert!(snapshot.chunk(ChunkIndex::new(1)).is_some());
}

#[tokio::test]
async fn compaction_is_idempotent_for_covered_jobs() {
    let fixture = CompactionFixture::new().await;
    let created = fixture.create_wal_export("disk-a", TREE_CHUNK_BYTES).await;
    let wal = fixture.open_wal(created.id()).await;
    append(&wal, 0, b"done").await;
    fixture
        .manager
        .compact_export(CompactionJob::new(created.id().clone(), WalSeq::new(1)))
        .await
        .expect("compact first job");

    let result = fixture
        .manager
        .compact_export(CompactionJob::new(created.id().clone(), WalSeq::new(1)))
        .await
        .expect("compact duplicate");

    assert_eq!(result.outcome(), CompactionOutcome::AlreadyCovered);
    assert_eq!(result.compacted_records(), 0);
    assert_eq!(result.written_leaf_blobs(), 0);
}

#[tokio::test]
async fn compaction_clamps_target_to_durable_wal_bounds() {
    let fixture = CompactionFixture::new().await;
    let created = fixture.create_wal_export("disk-a", TREE_CHUNK_BYTES).await;
    let wal = fixture.open_wal(created.id()).await;
    append(&wal, 0, b"only").await;

    let result = fixture
        .manager
        .compact_export(CompactionJob::new(created.id().clone(), WalSeq::new(99)))
        .await
        .expect("compact clamped job");

    assert_eq!(result.outcome(), CompactionOutcome::Published);
    assert_eq!(result.target_wal_seq(), WalSeq::new(1));
    let snapshot = fixture
        .catalog
        .load_cow_tree(created.id())
        .await
        .expect("load cow tree");
    assert_eq!(snapshot.checkpoint_wal_seq(), WalSeq::new(1));
}

#[tokio::test]
async fn shutdown_finishes_current_job_and_drops_pending_jobs() {
    let runtime = TestRuntime::new().expect("test runtime");
    let catalog = migrated_catalog(&runtime).await;
    let blob_store = LocalBlobStore::new(runtime.root_path().join("blobs"));
    let wal_provider = Arc::new(LocalWalProvider::new(runtime.wal_dir()));
    let blocking_store = Arc::new(BlockingCowTreeStore::new(catalog.clone()));
    let manager = CompactionManager::with_queue_capacity(
        blocking_store.clone() as Arc<dyn CowTreeMetadataStore>,
        wal_provider.clone(),
        blob_store,
        4,
    );
    let created = catalog
        .create_export(
            CreateExport::new(
                ExportName::new("disk-a").expect("export name"),
                TREE_CHUNK_BYTES,
                4096,
                ExportEngineKind::WalDurable,
            )
            .expect("create export"),
        )
        .await
        .expect("create wal export");
    let wal = wal_provider
        .open_export(OpenWal::new(WalDomain::for_export_id(created.id().clone())))
        .await
        .expect("open wal");
    append(&wal, 0, b"first").await;
    append(&wal, 8, b"second").await;

    assert_eq!(
        manager.enqueue(CompactionJob::new(created.id().clone(), WalSeq::new(1))),
        CompactionEnqueueOutcome::Queued,
    );
    blocking_store.wait_for_publish_count(1).await;
    assert_eq!(
        manager.enqueue(CompactionJob::new(created.id().clone(), WalSeq::new(2))),
        CompactionEnqueueOutcome::Queued,
    );

    manager.request_shutdown();
    assert_eq!(
        manager.enqueue(CompactionJob::new(created.id().clone(), WalSeq::new(2))),
        CompactionEnqueueOutcome::ShuttingDown,
    );
    blocking_store.release_first_publish();
    let shutdown = manager.shutdown().await.expect("shutdown compaction");

    assert_eq!(shutdown.dropped_pending_jobs(), 1);
    assert_eq!(blocking_store.publish_count(), 1);
    let snapshot = catalog
        .load_cow_tree(created.id())
        .await
        .expect("load cow tree");
    assert_eq!(snapshot.checkpoint_wal_seq(), WalSeq::new(1));
}

struct CompactionFixture {
    _runtime: TestRuntime,
    catalog: SQLiteExportCatalog,
    blob_store: LocalBlobStore,
    wal_provider: Arc<LocalWalProvider>,
    manager: CompactionManager,
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

    fn publish_count(&self) -> usize {
        self.publish_count.load(Ordering::SeqCst)
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

impl CompactionFixture {
    async fn new() -> Self {
        let runtime = TestRuntime::new().expect("test runtime");
        let catalog = migrated_catalog(&runtime).await;
        let blob_store = LocalBlobStore::new(runtime.root_path().join("blobs"));
        let wal_provider = Arc::new(LocalWalProvider::new(runtime.wal_dir()));
        let manager = CompactionManager::with_queue_capacity(
            Arc::new(catalog.clone()) as Arc<dyn CowTreeMetadataStore>,
            wal_provider.clone(),
            blob_store.clone(),
            4,
        );

        Self {
            _runtime: runtime,
            catalog,
            blob_store,
            wal_provider,
            manager,
        }
    }

    async fn create_wal_export(
        &self,
        name: &str,
        size_bytes: u64,
    ) -> nbd_control_plane::ExportMeta {
        self.catalog
            .create_export(
                CreateExport::new(
                    ExportName::new(name).expect("export name"),
                    size_bytes,
                    4096,
                    ExportEngineKind::WalDurable,
                )
                .expect("create export"),
            )
            .await
            .expect("create wal export")
    }

    async fn open_wal(&self, export_id: &ExportId) -> ExportWalHandle {
        self.wal_provider
            .open_export(OpenWal::new(WalDomain::for_export_id(export_id.clone())))
            .await
            .expect("open wal")
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

async fn append(wal: &ExportWalHandle, offset: u64, data: &[u8]) {
    let len = u32::try_from(data.len()).expect("payload length fits u32");
    wal.append(WalRequest::new(ByteRange::new(offset, len), data.to_vec()).expect("wal request"))
        .await
        .expect("append wal record");
}
