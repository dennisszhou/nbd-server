use nbd_control_plane::{
    BlobKey, CatalogError, CatalogUrl, ChunkIndex, CreateExport, DeleteExport, ExportCatalog,
    ExportEngineKind, ExportLayoutKind, ExportName, ExportState, InspectExport, ListExports,
    SQLiteExportCatalog, SimpleChunkRef, SimpleTreeMetadataStore, WalSeq, SIMPLE_CHUNK_BYTES,
};
use nbd_test_support::TestRuntime;

const MIGRATIONS: &[&str] = &[
    include_str!("../../../prisma/migrations/20260501000000_init/migration.sql"),
    include_str!(
        "../../../prisma/migrations/20260504000000_export_heads_tree_metadata/migration.sql"
    ),
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
        .create_export(create_export("disk-a", 128 * 1024 * 1024, 4096))
        .await
        .expect("create export");
    mark_simple_tree_head(&catalog, created.id().as_str()).await;

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
        .create_export(create_export("disk-a", 128 * 1024 * 1024, 4096))
        .await
        .expect("create export");
    mark_simple_tree_head(&catalog, created.id().as_str()).await;
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
        .create_export(create_export("disk-a", 128 * 1024 * 1024, 4096))
        .await
        .expect("create export");
    mark_simple_tree_head(&catalog, created.id().as_str()).await;
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

async fn mark_simple_tree_head(catalog: &SQLiteExportCatalog, export_id: &str) {
    sqlx::query(
        r#"
        UPDATE export_heads
        SET layout_kind = 'simple_mutable_tree'
        WHERE export_id = ?
        "#,
    )
    .bind(export_id)
    .execute(catalog.pool())
    .await
    .expect("mark simple tree head");
}

fn export_name(name: &str) -> ExportName {
    ExportName::new(name).expect("valid export name")
}

fn create_export(name: &str, size_bytes: u64, block_size: u64) -> CreateExport {
    CreateExport::new(
        export_name(name),
        size_bytes,
        block_size,
        ExportEngineKind::Memory,
    )
    .expect("valid create export request")
}
