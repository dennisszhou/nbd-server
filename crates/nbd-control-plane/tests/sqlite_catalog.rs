use nbd_control_plane::{
    CatalogError, CatalogUrl, CreateExport, DeleteExport, ExportCatalog, ExportEngineKind,
    ExportLayoutKind, ExportName, ExportState, InspectExport, ListExports, SQLiteExportCatalog,
    WalSeq,
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
        INSERT INTO export_generations (
          id, export_id, generation, size_bytes, root_node_id,
          checkpoint_wal_seq, created_at
        )
        VALUES ('generation-1', ?, 1, 2048, NULL, 0, 'later')
        "#,
    )
    .bind(created.id().as_str())
    .execute(catalog.pool())
    .await
    .expect("insert newer generation");

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
    CreateExport::new(
        export_name(name),
        size_bytes,
        block_size,
        ExportEngineKind::Memory,
    )
    .expect("valid create export request")
}
