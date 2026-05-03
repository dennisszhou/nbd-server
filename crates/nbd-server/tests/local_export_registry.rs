use nbd_config::ServerConfig;
use nbd_control_plane::{CatalogUrl, CreateExport, ExportCatalog, ExportName, SQLiteExportCatalog};
use nbd_server::{ExportOwner, LocalExportRegistry, ServerError, MAX_MEMORY_EXPORT_BYTES};
use nbd_test_support::TestRuntime;
use std::sync::Arc;

const MIGRATION: &str =
    include_str!("../../../prisma/migrations/20260501000000_init/migration.sql");

#[tokio::test]
async fn registry_rejects_second_unique_owner_until_close() {
    let runtime = TestRuntime::new().expect("test runtime");
    let catalog = migrated_catalog(&runtime).await;
    catalog
        .create_export(CreateExport::new(export_name("disk-a"), 4096, 4096).unwrap())
        .await
        .expect("create export");
    let registry = LocalExportRegistry::new(Arc::new(catalog), ServerConfig::default());
    let owner_a = ExportOwner::unique_connection();
    let owner_b = ExportOwner::unique_connection();

    let first_runtime = registry
        .open(export_name("disk-a"), owner_a)
        .await
        .expect("open first owner");
    assert!(matches!(
        registry.open(export_name("disk-a"), owner_b).await,
        Err(ServerError::ExportBusy { name }) if name.as_str() == "disk-a",
    ));

    registry
        .close(&export_name("disk-a"), &owner_a)
        .await
        .expect("close first owner");
    drop(first_runtime);

    let second_runtime = registry
        .open(export_name("disk-a"), owner_b)
        .await
        .expect("open after close");
    registry
        .close(&export_name("disk-a"), &owner_b)
        .await
        .expect("close second owner");
    drop(second_runtime);
}

#[tokio::test]
async fn failed_open_removes_opening_reservation() {
    let runtime = TestRuntime::new().expect("test runtime");
    let catalog = migrated_catalog(&runtime).await;
    catalog
        .create_export(
            CreateExport::new(export_name("huge"), MAX_MEMORY_EXPORT_BYTES + 1, 4096).unwrap(),
        )
        .await
        .expect("create export");
    let registry = LocalExportRegistry::new(Arc::new(catalog), ServerConfig::default());

    for _ in 0..2 {
        assert!(matches!(
            registry
                .open(export_name("huge"), ExportOwner::unique_connection())
                .await,
            Err(ServerError::ExportTooLarge {
                name,
                size_bytes,
                max_size_bytes,
            }) if name.as_str() == "huge"
                && size_bytes == MAX_MEMORY_EXPORT_BYTES + 1
                && max_size_bytes == MAX_MEMORY_EXPORT_BYTES,
        ));
    }
}

async fn migrated_catalog(runtime: &TestRuntime) -> SQLiteExportCatalog {
    let url = CatalogUrl::parse(runtime.catalog_url()).expect("catalog URL");
    let catalog = SQLiteExportCatalog::connect(&url)
        .await
        .expect("connect catalog");

    sqlx::raw_sql(MIGRATION)
        .execute(catalog.pool())
        .await
        .expect("apply migration");

    catalog
}

fn export_name(name: &str) -> ExportName {
    ExportName::new(name).expect("valid export name")
}
