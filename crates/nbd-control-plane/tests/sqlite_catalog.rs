use nbd_control_plane::{
    BlobKey, CatalogError, CatalogUrl, ChunkIndex, CowChunkRef, CowTreeMetadataStore, CreateExport,
    DeleteExport, ExportCatalog, ExportEngineKind, ExportLayoutKind, ExportName, ExportState,
    InspectExport, ListExports, PublishCompaction, PublishCompactionOutcome, SQLiteExportCatalog,
    SimpleChunkRef, SimpleTreeMetadataStore, WalSeq, SIMPLE_CHUNK_BYTES, TREE_CHUNK_BYTES,
};
use nbd_test_support::TestRuntime;

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
async fn create_export_initializes_memory_head() {
    let (_runtime, catalog) = migrated_catalog().await;

    let created = catalog
        .create_export(create_export("disk-a", 1024 * 1024, 4096))
        .await
        .expect("create export");

    assert_eq!(created.name().as_str(), "disk-a");
    assert_eq!(created.size_bytes(), 1024 * 1024);
    assert_eq!(created.block_size(), 4096);
    assert_eq!(created.engine_kind(), ExportEngineKind::Memory);
    assert_eq!(created.state(), ExportState::Active);
    assert_eq!(created.head().layout_kind(), ExportLayoutKind::MemoryEmpty);
    assert!(created.head().root_node_id().is_none());
    assert_eq!(created.head().size_bytes(), 1024 * 1024);
    assert_eq!(created.head().checkpoint_wal_seq(), WalSeq::zero());

    let inspected = catalog
        .inspect_export(InspectExport::new(export_name("disk-a")))
        .await
        .expect("inspect export");
    assert_eq!(inspected, created);
}

#[tokio::test]
async fn create_export_initializes_simple_durable_head() {
    let (_runtime, catalog) = migrated_catalog().await;

    let created = catalog
        .create_export(create_export_with_engine(
            "disk-durable",
            128 * 1024 * 1024,
            4096,
            ExportEngineKind::SimpleDurable,
        ))
        .await
        .expect("create export");

    assert_eq!(created.engine_kind(), ExportEngineKind::SimpleDurable);
    assert_eq!(
        created.head().layout_kind(),
        ExportLayoutKind::SimpleMutableTree,
    );
    assert!(created.head().root_node_id().is_none());
    assert_eq!(created.head().checkpoint_wal_seq(), WalSeq::zero());

    let snapshot = catalog
        .load_simple_tree(created.id())
        .await
        .expect("load simple tree");
    assert_eq!(snapshot.export_id(), created.id());
    assert!(snapshot.root_node_id().is_none());
    assert!(snapshot.chunks().is_empty());
}

#[tokio::test]
async fn create_export_initializes_wal_durable_head() {
    let (_runtime, catalog) = migrated_catalog().await;

    let created = catalog
        .create_export(create_export_with_engine(
            "disk-wal",
            128 * 1024 * 1024,
            4096,
            ExportEngineKind::WalDurable,
        ))
        .await
        .expect("create export");

    assert_eq!(created.engine_kind(), ExportEngineKind::WalDurable);
    assert_eq!(
        created.head().layout_kind(),
        ExportLayoutKind::CowImmutableTree
    );
    assert!(created.head().root_node_id().is_none());
    assert_eq!(created.head().checkpoint_wal_seq(), WalSeq::zero());

    let snapshot = catalog
        .load_cow_tree(created.id())
        .await
        .expect("load cow tree");
    assert_eq!(snapshot.export_id(), created.id());
    assert!(snapshot.root_node_id().is_none());
    assert_eq!(snapshot.checkpoint_wal_seq(), WalSeq::zero());
    assert!(snapshot.chunks().is_empty());
}

#[tokio::test]
async fn descriptor_and_head_load_separate_export_identity_from_serving_head() {
    let (_runtime, catalog) = migrated_catalog().await;
    let created = catalog
        .create_export(create_export_with_engine(
            "disk-wal",
            128 * 1024 * 1024,
            4096,
            ExportEngineKind::WalDurable,
        ))
        .await
        .expect("create export");

    let descriptor = catalog
        .load_export_descriptor(export_name("disk-wal"))
        .await
        .expect("load descriptor");
    assert_eq!(descriptor.id(), created.id());
    assert_eq!(descriptor.name().as_str(), "disk-wal");
    assert_eq!(descriptor.engine_kind(), ExportEngineKind::WalDurable);
    assert_eq!(descriptor.block_size(), 4096);

    let initial_head = catalog
        .load_export_head(created.id())
        .await
        .expect("load initial head");
    assert_eq!(initial_head, created.head().clone());

    assert_eq!(descriptor.state(), ExportState::Active);
    assert!(descriptor.deleted_at().is_none());
}

#[tokio::test]
async fn duplicate_create_fails_clearly() {
    let (_runtime, catalog) = migrated_catalog().await;

    catalog
        .create_export(create_export("disk-a", 1024, 4096))
        .await
        .expect("create export");

    let error = catalog
        .create_export(create_export("disk-a", 1024, 4096))
        .await
        .unwrap_err();

    assert!(matches!(error, CatalogError::ExportAlreadyExists { .. }));
}

#[tokio::test]
async fn export_head_owns_serving_size() {
    let (_runtime, catalog) = migrated_catalog().await;

    let created = catalog
        .create_export(create_export("disk-a", 1024, 4096))
        .await
        .expect("create export");

    sqlx::query(
        r#"
        UPDATE export_heads
        SET size_bytes = 3072
        WHERE export_id = ?
        "#,
    )
    .bind(created.id().as_str())
    .execute(catalog.pool())
    .await
    .expect("update export head");

    let inspected = catalog
        .inspect_export(InspectExport::new(export_name("disk-a")))
        .await
        .expect("inspect export");

    assert_eq!(inspected.size_bytes(), 3072);
    assert_eq!(inspected.head().size_bytes(), 3072);
    assert_eq!(
        inspected.head().layout_kind(),
        ExportLayoutKind::MemoryEmpty
    );
}

#[tokio::test]
async fn migration_does_not_create_export_generations() {
    let (_runtime, catalog) = migrated_catalog().await;

    let table_count: i64 = sqlx::query_scalar(
        r#"
        SELECT COUNT(*)
        FROM sqlite_master
        WHERE type = 'table' AND name = 'export_generations'
        "#,
    )
    .fetch_one(catalog.pool())
    .await
    .expect("inspect sqlite schema");

    assert_eq!(table_count, 0);
}

#[tokio::test]
async fn durable_engine_kind_migrations_preserve_existing_exports() {
    let runtime = TestRuntime::new().expect("test runtime");
    let url = CatalogUrl::parse(runtime.catalog_url()).expect("catalog URL");
    let catalog = SQLiteExportCatalog::connect(&url)
        .await
        .expect("connect catalog");

    for migration in &MIGRATIONS[..2] {
        sqlx::raw_sql(migration)
            .execute(catalog.pool())
            .await
            .expect("apply base migration");
    }

    catalog
        .create_export(create_export("disk-memory", 1024, 4096))
        .await
        .expect("create memory export before migration");

    sqlx::raw_sql(MIGRATIONS[2])
        .execute(catalog.pool())
        .await
        .expect("apply simple durable engine migration");

    let memory = catalog
        .inspect_export(InspectExport::new(export_name("disk-memory")))
        .await
        .expect("inspect migrated memory export");
    assert_eq!(memory.engine_kind(), ExportEngineKind::Memory);
    assert_eq!(memory.head().layout_kind(), ExportLayoutKind::MemoryEmpty);

    let durable = catalog
        .create_export(create_export_with_engine(
            "disk-durable",
            1024,
            4096,
            ExportEngineKind::SimpleDurable,
        ))
        .await
        .expect("create simple durable after migration");
    assert_eq!(durable.engine_kind(), ExportEngineKind::SimpleDurable);
    assert_eq!(
        durable.head().layout_kind(),
        ExportLayoutKind::SimpleMutableTree,
    );

    sqlx::raw_sql(MIGRATIONS[3])
        .execute(catalog.pool())
        .await
        .expect("apply wal durable engine migration");

    let memory = catalog
        .inspect_export(InspectExport::new(export_name("disk-memory")))
        .await
        .expect("inspect migrated memory export after wal migration");
    assert_eq!(memory.engine_kind(), ExportEngineKind::Memory);
    assert_eq!(memory.head().layout_kind(), ExportLayoutKind::MemoryEmpty);

    let durable = catalog
        .inspect_export(InspectExport::new(export_name("disk-durable")))
        .await
        .expect("inspect migrated simple durable export");
    assert_eq!(durable.engine_kind(), ExportEngineKind::SimpleDurable);
    assert_eq!(
        durable.head().layout_kind(),
        ExportLayoutKind::SimpleMutableTree,
    );

    let wal = catalog
        .create_export(create_export_with_engine(
            "disk-wal",
            1024,
            4096,
            ExportEngineKind::WalDurable,
        ))
        .await
        .expect("create wal durable after migration");
    assert_eq!(wal.engine_kind(), ExportEngineKind::WalDurable);
    assert_eq!(wal.head().layout_kind(), ExportLayoutKind::CowImmutableTree);

    sqlx::raw_sql(MIGRATIONS[4])
        .execute(catalog.pool())
        .await
        .expect("apply cow tree metadata migration");

    let wal = catalog
        .inspect_export(InspectExport::new(export_name("disk-wal")))
        .await
        .expect("inspect migrated wal export");
    assert_eq!(wal.engine_kind(), ExportEngineKind::WalDurable);
    assert_eq!(wal.head().layout_kind(), ExportLayoutKind::CowImmutableTree);
}

#[tokio::test]
async fn delete_hides_export_from_load_and_default_list() {
    let (_runtime, catalog) = migrated_catalog().await;

    catalog
        .create_export(create_export("disk-a", 1024, 4096))
        .await
        .expect("create export");
    catalog
        .delete_export(DeleteExport::new(export_name("disk-a")))
        .await
        .expect("delete export");

    let load_error = catalog
        .load_export(export_name("disk-a"))
        .await
        .unwrap_err();
    assert!(matches!(load_error, CatalogError::ExportDeleted { .. }));
    let descriptor_error = catalog
        .load_export_descriptor(export_name("disk-a"))
        .await
        .unwrap_err();
    assert!(matches!(
        descriptor_error,
        CatalogError::ExportDeleted { .. }
    ));

    let inspected = catalog
        .inspect_export(InspectExport::new(export_name("disk-a")))
        .await
        .expect("inspect deleted export");
    assert_eq!(inspected.state(), ExportState::Deleted);
    assert!(inspected.deleted_at().is_some());
    assert_eq!(
        inspected.head().layout_kind(),
        ExportLayoutKind::MemoryEmpty
    );

    assert!(catalog
        .list_exports(ListExports::active_only())
        .await
        .expect("list active")
        .is_empty());

    let all = catalog
        .list_exports(ListExports::include_deleted())
        .await
        .expect("list all");
    assert_eq!(all.len(), 1);
    assert_eq!(all[0].name().as_str(), "disk-a");
}

#[tokio::test]
async fn list_exports_orders_active_exports_by_name() {
    let (_runtime, catalog) = migrated_catalog().await;

    catalog
        .create_export(create_export("disk-b", 1024, 4096))
        .await
        .expect("create disk-b");
    catalog
        .create_export(create_export("disk-a", 1024, 4096))
        .await
        .expect("create disk-a");

    let names: Vec<String> = catalog
        .list_exports(ListExports::active_only())
        .await
        .expect("list active")
        .iter()
        .map(|meta| meta.name().as_str().to_owned())
        .collect();

    assert_eq!(names, ["disk-a", "disk-b"]);
}

#[tokio::test]
async fn migration_rejects_zero_sized_heads() {
    let (_runtime, catalog) = migrated_catalog().await;

    sqlx::query(
        r#"
        INSERT INTO exports (
          id, name, engine_kind, block_size, state, created_at, updated_at
        )
        VALUES ('export-zero', 'zero', 'memory', 4096, 'active', 'now', 'now')
        "#,
    )
    .execute(catalog.pool())
    .await
    .expect("insert export row");

    let error = sqlx::query(
        r#"
        INSERT INTO export_heads (
          export_id, layout_kind, root_node_id, size_bytes,
          checkpoint_wal_seq, updated_at
        )
        VALUES (
          'export-zero', 'memory_empty', NULL, 0, 0, 'now'
        )
        "#,
    )
    .execute(catalog.pool())
    .await
    .expect_err("zero-sized head should violate migration constraints");

    assert!(error.to_string().contains("CHECK constraint failed"));
}

#[tokio::test]
async fn simple_tree_loads_empty_sparse_head() {
    let (_runtime, catalog) = migrated_catalog().await;
    let created = catalog
        .create_export(create_export_with_engine(
            "disk-a",
            128 * 1024 * 1024,
            4096,
            ExportEngineKind::SimpleDurable,
        ))
        .await
        .expect("create export");

    let snapshot = catalog
        .load_simple_tree(created.id())
        .await
        .expect("load simple tree");

    assert_eq!(snapshot.export_id(), created.id());
    assert_eq!(snapshot.size_bytes(), 128 * 1024 * 1024);
    assert!(snapshot.root_node_id().is_none());
    assert!(snapshot.chunks().is_empty());
}

#[tokio::test]
async fn simple_tree_commits_new_leaf_metadata() {
    let (_runtime, catalog) = migrated_catalog().await;
    let created = catalog
        .create_export(create_export_with_engine(
            "disk-a",
            128 * 1024 * 1024,
            4096,
            ExportEngineKind::SimpleDurable,
        ))
        .await
        .expect("create export");
    let chunk = SimpleChunkRef::new(
        ChunkIndex::new(2),
        BlobKey::new("blob-two").expect("valid blob key"),
        SIMPLE_CHUNK_BYTES,
    )
    .expect("valid chunk");

    let snapshot = catalog
        .commit_simple_chunks(created.id(), vec![chunk.clone()])
        .await
        .expect("commit simple chunk");

    let root_node_id = snapshot.root_node_id().expect("root node should exist");
    assert_eq!(
        snapshot
            .chunk(ChunkIndex::new(2))
            .expect("chunk should be materialized"),
        &chunk
    );

    let reloaded = catalog
        .load_simple_tree(created.id())
        .await
        .expect("reload simple tree");
    assert_eq!(reloaded.root_node_id(), Some(root_node_id));
    assert_eq!(reloaded.chunk(ChunkIndex::new(2)), Some(&chunk));
    assert!(reloaded.chunk(ChunkIndex::new(1)).is_none());
}

#[tokio::test]
async fn simple_tree_rejects_existing_leaf_metadata() {
    let (_runtime, catalog) = migrated_catalog().await;
    let created = catalog
        .create_export(create_export_with_engine(
            "disk-a",
            128 * 1024 * 1024,
            4096,
            ExportEngineKind::SimpleDurable,
        ))
        .await
        .expect("create export");
    let first = SimpleChunkRef::new(
        ChunkIndex::new(1),
        BlobKey::new("blob-one").expect("valid blob key"),
        SIMPLE_CHUNK_BYTES,
    )
    .expect("valid chunk");
    catalog
        .commit_simple_chunks(created.id(), vec![first])
        .await
        .expect("commit first chunk");

    let second = SimpleChunkRef::new(
        ChunkIndex::new(1),
        BlobKey::new("blob-one-replacement").expect("valid blob key"),
        SIMPLE_CHUNK_BYTES,
    )
    .expect("valid chunk");
    let error = catalog
        .commit_simple_chunks(created.id(), vec![second])
        .await
        .unwrap_err();

    assert!(error
        .to_string()
        .contains("chunk 1 is already materialized"));
}

#[tokio::test]
async fn cow_tree_publish_creates_checkpoint_root() {
    let (_runtime, catalog) = migrated_catalog().await;
    let created = catalog
        .create_export(create_export_with_engine(
            "disk-wal",
            128 * 1024 * 1024,
            4096,
            ExportEngineKind::WalDurable,
        ))
        .await
        .expect("create export");
    let chunk = cow_chunk(2, "blob-two");

    let outcome = catalog
        .publish_compaction(
            PublishCompaction::new(
                created.id().clone(),
                created.head().clone(),
                WalSeq::new(4),
                vec![chunk.clone()],
            )
            .expect("publish request"),
        )
        .await
        .expect("publish compaction");
    let published = match outcome {
        PublishCompactionOutcome::Published(meta) => meta,
        outcome => panic!("expected Published, got {outcome:?}"),
    };
    assert_eq!(published.head().checkpoint_wal_seq(), WalSeq::new(4));
    assert!(published.head().root_node_id().is_some());

    let snapshot = catalog
        .load_cow_tree(created.id())
        .await
        .expect("load cow tree");
    assert_eq!(snapshot.root_node_id(), published.head().root_node_id());
    assert_eq!(snapshot.checkpoint_wal_seq(), WalSeq::new(4));
    assert_eq!(snapshot.chunk(ChunkIndex::new(2)), Some(&chunk));
    assert!(snapshot.chunk(ChunkIndex::new(1)).is_none());
}

#[tokio::test]
async fn cow_tree_publish_is_idempotent_for_covered_checkpoint() {
    let (_runtime, catalog) = migrated_catalog().await;
    let created = catalog
        .create_export(create_export_with_engine(
            "disk-wal",
            128 * 1024 * 1024,
            4096,
            ExportEngineKind::WalDurable,
        ))
        .await
        .expect("create export");
    let base = created.head().clone();
    catalog
        .publish_compaction(
            PublishCompaction::new(
                created.id().clone(),
                base.clone(),
                WalSeq::new(4),
                vec![cow_chunk(0, "blob-zero")],
            )
            .expect("first publish"),
        )
        .await
        .expect("publish first checkpoint");

    let outcome = catalog
        .publish_compaction(
            PublishCompaction::new(
                created.id().clone(),
                base,
                WalSeq::new(2),
                vec![cow_chunk(0, "blob-duplicate")],
            )
            .expect("duplicate publish"),
        )
        .await
        .expect("publish covered checkpoint");

    let covered = match outcome {
        PublishCompactionOutcome::AlreadyCovered(meta) => meta,
        outcome => panic!("expected AlreadyCovered, got {outcome:?}"),
    };
    assert_eq!(covered.head().checkpoint_wal_seq(), WalSeq::new(4));
    let snapshot = catalog
        .load_cow_tree(created.id())
        .await
        .expect("load cow tree");
    assert_eq!(
        snapshot
            .chunk(ChunkIndex::new(0))
            .unwrap()
            .blob_key()
            .as_str(),
        "blob-zero"
    );
}

#[tokio::test]
async fn cow_tree_publish_rejects_stale_base() {
    let (_runtime, catalog) = migrated_catalog().await;
    let created = catalog
        .create_export(create_export_with_engine(
            "disk-wal",
            128 * 1024 * 1024,
            4096,
            ExportEngineKind::WalDurable,
        ))
        .await
        .expect("create export");
    let base = created.head().clone();
    catalog
        .publish_compaction(
            PublishCompaction::new(
                created.id().clone(),
                base.clone(),
                WalSeq::new(4),
                vec![cow_chunk(0, "blob-zero")],
            )
            .expect("first publish"),
        )
        .await
        .expect("publish first checkpoint");

    let outcome = catalog
        .publish_compaction(
            PublishCompaction::new(
                created.id().clone(),
                base,
                WalSeq::new(6),
                vec![cow_chunk(1, "blob-one")],
            )
            .expect("stale publish"),
        )
        .await
        .expect("publish stale checkpoint");

    let stale = match outcome {
        PublishCompactionOutcome::StalePlan(meta) => meta,
        outcome => panic!("expected StalePlan, got {outcome:?}"),
    };
    assert_eq!(stale.head().checkpoint_wal_seq(), WalSeq::new(4));
    let snapshot = catalog
        .load_cow_tree(created.id())
        .await
        .expect("load cow tree");
    assert!(snapshot.chunk(ChunkIndex::new(1)).is_none());
}

#[tokio::test]
async fn cow_tree_publish_rejects_deleted_and_wrong_layout_exports() {
    let (_runtime, catalog) = migrated_catalog().await;
    let memory = catalog
        .create_export(create_export("disk-memory", TREE_CHUNK_BYTES, 4096))
        .await
        .expect("create memory export");
    let wal = catalog
        .create_export(create_export_with_engine(
            "disk-wal",
            TREE_CHUNK_BYTES,
            4096,
            ExportEngineKind::WalDurable,
        ))
        .await
        .expect("create wal export");
    catalog
        .delete_export(DeleteExport::new(export_name("disk-wal")))
        .await
        .expect("delete wal export");

    let wrong_layout = catalog
        .publish_compaction(
            PublishCompaction::new(
                memory.id().clone(),
                wal.head().clone(),
                WalSeq::new(1),
                vec![cow_chunk(0, "blob-memory")],
            )
            .expect("wrong layout publish"),
        )
        .await
        .unwrap_err();
    assert!(wrong_layout.to_string().contains("engine_kind"));

    let deleted = catalog
        .publish_compaction(
            PublishCompaction::new(
                wal.id().clone(),
                wal.head().clone(),
                WalSeq::new(1),
                vec![cow_chunk(0, "blob-deleted")],
            )
            .expect("deleted publish"),
        )
        .await
        .unwrap_err();
    assert!(matches!(deleted, CatalogError::ExportDeleted { .. }));
}

async fn migrated_catalog() -> (TestRuntime, SQLiteExportCatalog) {
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

    (runtime, catalog)
}

fn export_name(name: &str) -> ExportName {
    ExportName::new(name).expect("valid export name")
}

fn create_export(name: &str, size_bytes: u64, block_size: u64) -> CreateExport {
    create_export_with_engine(name, size_bytes, block_size, ExportEngineKind::Memory)
}

fn create_export_with_engine(
    name: &str,
    size_bytes: u64,
    block_size: u64,
    engine_kind: ExportEngineKind,
) -> CreateExport {
    CreateExport::new(export_name(name), size_bytes, block_size, engine_kind)
        .expect("valid create export request")
}

fn cow_chunk(index: u64, blob_key: &str) -> CowChunkRef {
    CowChunkRef::new(
        ChunkIndex::new(index),
        BlobKey::new(blob_key).expect("valid blob key"),
        TREE_CHUNK_BYTES,
    )
    .expect("valid cow chunk")
}
