use nbd_control_plane::{
    CatalogUrl, ChunkIndex, CowTreeMetadataStore, CreateExport, ExportCatalog, ExportEngineKind,
    ExportId, ExportName, SQLiteExportCatalog, TREE_CHUNK_BYTES, WalSeq,
};
use nbd_server::{
    ByteRange, CompactionOutcome, CowCompactor, ExportWalHandle, LocalBlobStore, LocalWalProvider,
    OpenWal, WalDomain, WalProvider, WalRequest,
};
use nbd_test_support::TestRuntime;
use std::sync::Arc;

const MIGRATIONS: &[&str] = &[include_str!(
    "../../../prisma/migrations/20260506000000_baseline/migration.sql"
)];

#[tokio::test]
async fn compaction_publishes_checkpoint_from_wal_records() {
    let fixture = CompactionFixture::new().await;
    let created = fixture.create_wal_export("disk-a", TREE_CHUNK_BYTES).await;
    let wal = fixture.open_wal(created.id()).await;
    append(&wal, 0, b"abcd").await;
    append(&wal, 2, b"ZZ").await;

    let result = fixture
        .compactor
        .compact_export(created.id(), &wal, WalSeq::new(2))
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
    assert_eq!(snapshot.base_wal_seq(), WalSeq::new(2));
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
        .compactor
        .compact_export(created.id(), &wal, WalSeq::new(1))
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
        .compactor
        .compact_export(created.id(), &wal, WalSeq::new(2))
        .await
        .expect("compact second chunk");

    let snapshot = fixture
        .catalog
        .load_cow_tree(created.id())
        .await
        .expect("load cow tree");
    assert_eq!(snapshot.base_wal_seq(), WalSeq::new(2));
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
        .compactor
        .compact_export(created.id(), &wal, WalSeq::new(1))
        .await
        .expect("compact first job");

    let result = fixture
        .compactor
        .compact_export(created.id(), &wal, WalSeq::new(1))
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
        .compactor
        .compact_export(created.id(), &wal, WalSeq::new(99))
        .await
        .expect("compact clamped job");

    assert_eq!(result.outcome(), CompactionOutcome::Published);
    assert_eq!(result.target_wal_seq(), WalSeq::new(1));
    let snapshot = fixture
        .catalog
        .load_cow_tree(created.id())
        .await
        .expect("load cow tree");
    assert_eq!(snapshot.base_wal_seq(), WalSeq::new(1));
}

struct CompactionFixture {
    _runtime: TestRuntime,
    catalog: SQLiteExportCatalog,
    blob_store: LocalBlobStore,
    wal_provider: Arc<LocalWalProvider>,
    compactor: CowCompactor,
}

impl CompactionFixture {
    async fn new() -> Self {
        let runtime = TestRuntime::new().expect("test runtime");
        let catalog = migrated_catalog(&runtime).await;
        let blob_store = LocalBlobStore::new(runtime.root_path().join("blobs"));
        let wal_provider = Arc::new(LocalWalProvider::new(runtime.wal_dir()));
        let compactor = CowCompactor::new(
            Arc::new(catalog.clone()) as Arc<dyn CowTreeMetadataStore>,
            blob_store.clone(),
        );

        Self {
            _runtime: runtime,
            catalog,
            blob_store,
            wal_provider,
            compactor,
        }
    }

    async fn create_wal_export(
        &self,
        name: &str,
        size_bytes: u64,
    ) -> nbd_control_plane::ExportRecord {
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
