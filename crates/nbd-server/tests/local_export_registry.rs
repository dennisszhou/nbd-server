use nbd_config::{ExportRuntimeKind, ServerConfig};
use nbd_control_plane::{
    CatalogUrl, CreateExport, ExportCatalog, ExportEngineKind, ExportName, SQLiteExportCatalog,
};
use nbd_server::{
    ExportOwner, ExportReply, LocalExportRegistry, ServerError, MAX_MEMORY_EXPORT_BYTES,
};
use nbd_test_support::TestRuntime;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::Arc;

const MIGRATIONS: &[&str] = &[
    include_str!("../../../prisma/migrations/20260501000000_init/migration.sql"),
    include_str!(
        "../../../prisma/migrations/20260504000000_export_heads_tree_metadata/migration.sql"
    ),
    include_str!(
        "../../../prisma/migrations/20260504010000_simple_durable_engine_kind/migration.sql"
    ),
    include_str!(
        "../../../prisma/migrations/20260505000000_wal_durable_engine_kind/migration.sql"
    ),
];

#[tokio::test]
async fn registry_rejects_second_unique_owner_until_close() {
    let runtime = TestRuntime::new().expect("test runtime");
    let catalog = migrated_catalog(&runtime).await;
    catalog
        .create_export(create_export("disk-a", 4096, 4096))
        .await
        .expect("create export");
    let registry = LocalExportRegistry::new(
        Arc::new(catalog),
        ServerConfig::default(),
        blob_dir(&runtime),
    );
    runtime.assert_path_inside(registry.blob_dir());
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
        Err(ServerError::RuntimeClosed { resource }) if resource == "concurrent export runtime",
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
    let registry = LocalExportRegistry::new(
        Arc::new(catalog),
        ServerConfig::default(),
        blob_dir(&runtime),
    );

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
    let registry = LocalExportRegistry::new(Arc::new(catalog), config, blob_dir(&runtime));
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

#[tokio::test]
async fn registry_can_open_concurrent_runtime_from_config() {
    let runtime = TestRuntime::new().expect("test runtime");
    let catalog = migrated_catalog(&runtime).await;
    catalog
        .create_export(create_export("disk-a", 4096, 4096))
        .await
        .expect("create export");
    let config = ServerConfig {
        export_runtime: ExportRuntimeKind::Concurrent,
        ..ServerConfig::default()
    };
    let registry = LocalExportRegistry::new(Arc::new(catalog), config, blob_dir(&runtime));
    let owner = ExportOwner::unique_connection();

    let export_runtime = registry
        .open(export_name("disk-a"), owner)
        .await
        .expect("open concurrent runtime");
    let queue_slot = export_runtime
        .reserve()
        .await
        .expect("reserve from concurrent runtime");
    let (job, receiver) =
        nbd_server::ExportJob::oneshot(nbd_server::ExportRequest::Flush, queue_slot);
    export_runtime.submit(job).await.expect("submit flush");
    let completed = receiver.await.expect("runtime completion");
    let (result, _queue_slot) = completed.into_parts();
    assert_eq!(result.expect("flush reply"), nbd_server::ExportReply::Done);

    registry
        .close(&export_name("disk-a"), &owner)
        .await
        .expect("close concurrent runtime");
    assert!(matches!(
        export_runtime.reserve().await,
        Err(ServerError::RuntimeClosed { resource }) if resource == "concurrent export runtime",
    ));
}

#[tokio::test]
async fn registry_can_open_serial_runtime_from_config() {
    let runtime = TestRuntime::new().expect("test runtime");
    let catalog = migrated_catalog(&runtime).await;
    catalog
        .create_export(create_export("disk-a", 4096, 4096))
        .await
        .expect("create export");
    let config = ServerConfig {
        export_runtime: ExportRuntimeKind::Serial,
        ..ServerConfig::default()
    };
    let registry = LocalExportRegistry::new(Arc::new(catalog), config, blob_dir(&runtime));
    let owner = ExportOwner::unique_connection();

    let export_runtime = registry
        .open(export_name("disk-a"), owner)
        .await
        .expect("open serial runtime");
    let queue_slot = export_runtime
        .reserve()
        .await
        .expect("reserve from serial runtime");
    drop(queue_slot);

    registry
        .close(&export_name("disk-a"), &owner)
        .await
        .expect("close serial runtime");
    assert!(matches!(
        export_runtime.reserve().await,
        Err(ServerError::RuntimeClosed { resource }) if resource == "serial export runtime",
    ));
}

#[tokio::test]
async fn registry_shares_active_runtime_for_same_owner() {
    let runtime = TestRuntime::new().expect("test runtime");
    let catalog = migrated_catalog(&runtime).await;
    catalog
        .create_export(create_export("disk-a", 4096, 4096))
        .await
        .expect("create export");
    let config = ServerConfig {
        export_runtime: ExportRuntimeKind::Concurrent,
        ..ServerConfig::default()
    };
    let registry = LocalExportRegistry::new(Arc::new(catalog), config, blob_dir(&runtime));
    let owner = ExportOwner::unique_connection();

    let first_runtime = registry
        .open(export_name("disk-a"), owner)
        .await
        .expect("open first connection");
    let second_runtime = registry
        .open(export_name("disk-a"), owner)
        .await
        .expect("open same owner connection");
    assert!(Arc::ptr_eq(&first_runtime, &second_runtime));

    registry
        .close(&export_name("disk-a"), &owner)
        .await
        .expect("close first connection");
    let queue_slot = second_runtime
        .reserve()
        .await
        .expect("runtime remains open after one same-owner close");
    drop(queue_slot);

    registry
        .close(&export_name("disk-a"), &owner)
        .await
        .expect("close second connection");
    assert!(matches!(
        second_runtime.reserve().await,
        Err(ServerError::RuntimeClosed { resource }) if resource == "concurrent export runtime",
    ));
}

#[tokio::test]
async fn registry_opens_simple_durable_runtime_from_catalog() {
    let runtime = TestRuntime::new().expect("test runtime");
    let catalog = migrated_catalog(&runtime).await;
    catalog
        .create_export(create_export_with_engine(
            "disk-durable",
            4096,
            4096,
            ExportEngineKind::SimpleDurable,
        ))
        .await
        .expect("create export");
    let registry = LocalExportRegistry::new(
        Arc::new(catalog),
        ServerConfig::default(),
        blob_dir(&runtime),
    );
    let owner = ExportOwner::unique_connection();

    let export_runtime = registry
        .open(export_name("disk-durable"), owner)
        .await
        .expect("open simple durable runtime");
    let write_slot = export_runtime.reserve().await.expect("reserve write slot");
    let (write_job, write_receiver) = nbd_server::ExportJob::oneshot(
        nbd_server::ExportRequest::Write {
            offset: 0,
            data: b"durable".to_vec(),
        },
        write_slot,
    );
    export_runtime
        .submit(write_job)
        .await
        .expect("submit write");
    let completed = write_receiver.await.expect("write completion");
    let (result, _slot) = completed.into_parts();
    assert_eq!(result.expect("write reply"), ExportReply::Done);

    let read_slot = export_runtime.reserve().await.expect("reserve read slot");
    let (read_job, read_receiver) = nbd_server::ExportJob::oneshot(
        nbd_server::ExportRequest::Read { offset: 0, len: 7 },
        read_slot,
    );
    export_runtime.submit(read_job).await.expect("submit read");
    let completed = read_receiver.await.expect("read completion");
    let (result, _slot) = completed.into_parts();
    assert_eq!(
        result.expect("read reply"),
        ExportReply::Read {
            data: b"durable".to_vec(),
        },
    );

    registry
        .close(&export_name("disk-durable"), &owner)
        .await
        .expect("close simple durable runtime");
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

fn export_name(name: &str) -> ExportName {
    ExportName::new(name).expect("valid export name")
}

fn blob_dir(runtime: &TestRuntime) -> PathBuf {
    runtime.state_dir().join("blobs")
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
