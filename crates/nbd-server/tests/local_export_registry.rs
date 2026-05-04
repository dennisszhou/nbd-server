use nbd_config::ServerConfig;
use nbd_control_plane::{
    CatalogUrl, CreateExport, ExportCatalog, ExportEngineKind, ExportName, SQLiteExportCatalog,
};
use nbd_server::{ExportOwner, LocalExportRegistry, ServerError, MAX_MEMORY_EXPORT_BYTES};
use nbd_test_support::TestRuntime;
use std::num::NonZeroUsize;
use std::sync::Arc;

const MIGRATION: &str =
    include_str!("../../../prisma/migrations/20260501000000_init/migration.sql");

#[tokio::test]
async fn registry_rejects_second_unique_owner_until_close() {
    let runtime = TestRuntime::new().expect("test runtime");
    let catalog = migrated_catalog(&runtime).await;
    catalog
        .create_export(create_export("disk-a", 4096, 4096))
        .await
        .expect("create export");
    let registry = LocalExportRegistry::new(Arc::new(catalog), ServerConfig::default());
    let owner_a = ExportOwner::unique_connection();
    let owner_b = ExportOwner::unique_connection();

    let first_runtime = registry
        .open(export_name("disk-a"), owner_a)
        .await
        .expect("open first owner");
    assert_eq!(first_runtime.export_meta().size_bytes(), 4096);
    assert!(matches!(
        registry.open(export_name("disk-a"), owner_b).await,
        Err(ServerError::ExportBusy { name }) if name.as_str() == "disk-a",
    ));

    registry
        .close(&export_name("disk-a"), &owner_a)
        .await
        .expect("close first owner");
    assert!(matches!(
        first_runtime.reserve().await,
        Err(ServerError::RuntimeClosed { resource }) if resource == "serial export runtime",
    ));
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
        .create_export(create_export("huge", MAX_MEMORY_EXPORT_BYTES + 1, 4096))
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

#[tokio::test]
async fn registry_applies_configured_export_queue_depth() {
    let runtime = TestRuntime::new().expect("test runtime");
    let catalog = migrated_catalog(&runtime).await;
    catalog
        .create_export(create_export("disk-a", 4096, 4096))
        .await
        .expect("create export");
    let config = ServerConfig {
        export_queue_depth: NonZeroUsize::new(1).expect("nonzero queue depth"),
        ..ServerConfig::default()
    };
    let registry = LocalExportRegistry::new(Arc::new(catalog), config);
    let owner = ExportOwner::unique_connection();

    let export_runtime = registry
        .open(export_name("disk-a"), owner)
        .await
        .expect("open export");
    let first_slot = export_runtime.reserve().await.expect("reserve first slot");
    let waiter_runtime = export_runtime.clone();
    let waiter =
        tokio::spawn(async move { waiter_runtime.reserve().await.expect("reserve second slot") });

    tokio::task::yield_now().await;
    assert!(
        !waiter.is_finished(),
        "configured queue depth should limit export reservations",
    );

    drop(first_slot);
    let second_slot = waiter.await.expect("reservation task");
    drop(second_slot);
    registry
        .close(&export_name("disk-a"), &owner)
        .await
        .expect("close export");
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

fn create_export(name: &str, size_bytes: u64, block_size: u64) -> CreateExport {
    CreateExport::new(
        export_name(name),
        size_bytes,
        block_size,
        ExportEngineKind::Memory,
    )
    .expect("valid create export request")
}
