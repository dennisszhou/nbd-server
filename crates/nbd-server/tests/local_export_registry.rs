use nbd_config::{ExportRuntimeKind, ServerConfig};
use nbd_control_plane::{
    CatalogError, CatalogUrl, CreateExport, ExportCatalog, ExportEngineKind, ExportName, NodeId,
    PublishTreeUpdate, PublishTreeUpdateOutcome, TreeEdgeLookup, TreeEdgeRecord, TreeLeafRefRecord,
    TreeNodeRecord, TreeRecordStore, WalSeq,
};
use nbd_control_plane_sqlite::SQLiteExportCatalog;
use nbd_server::{
    ConfiguredBlobStore, ExportFactory, ExportOwner, ExportReply, LocalExportRegistry,
    LocalWalProvider, MAX_MEMORY_EXPORT_BYTES, ServerError,
};
use nbd_test_support::TestRuntime;
use std::fs;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::sync::Notify;
use tokio::time::{sleep, timeout};

const MIGRATIONS: &[&str] = &[
    include_str!("../../../prisma/migrations/20260506000000_baseline/migration.sql"),
    include_str!("../../../prisma/migrations/20260512000000_tree_format/migration.sql"),
];

#[tokio::test]
async fn registry_rejects_second_unique_owner_until_close() {
    let runtime = TestRuntime::new().expect("test runtime");
    let catalog = migrated_catalog(&runtime).await;
    catalog
        .create_export(create_export("disk-a", 4096, 4096))
        .await
        .expect("create export");
    let registry = local_registry(catalog, &runtime, ServerConfig::default());
    runtime.assert_path_inside(registry.blob_dir().expect("local blob directory"));
    let owner_a = ExportOwner::unique_connection();
    let owner_b = ExportOwner::unique_connection();

    let first_runtime = registry
        .open(export_name("disk-a"), owner_a)
        .await
        .expect("open first owner");
    assert_eq!(first_runtime.export_record().size_bytes(), 4096);
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
    let registry = local_registry(catalog, &runtime, ServerConfig::default());

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
    let registry = local_registry(catalog, &runtime, config);
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
    let registry = local_registry(catalog, &runtime, config);
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
    let registry = local_registry(catalog, &runtime, config);
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
    let registry = local_registry(catalog, &runtime, config);
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
    let registry = local_registry(catalog, &runtime, ServerConfig::default());
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

#[tokio::test]
async fn registry_opens_wal_durable_runtime_from_catalog() {
    let runtime = TestRuntime::new().expect("test runtime");
    let catalog = migrated_catalog(&runtime).await;
    let created = catalog
        .create_export(create_export_with_engine(
            "disk-wal",
            4096,
            4096,
            ExportEngineKind::WalDurable,
        ))
        .await
        .expect("create export");
    let catalog_for_assert = catalog.clone();
    let registry = local_registry(catalog, &runtime, ServerConfig::default());
    let owner = ExportOwner::unique_connection();

    let export_runtime = registry
        .open(export_name("disk-wal"), owner)
        .await
        .expect("open wal durable runtime");
    execute_request(
        &export_runtime,
        nbd_server::ExportRequest::Write {
            offset: 4,
            data: b"wal".to_vec(),
        },
    )
    .await
    .expect("write");
    assert_eq!(
        execute_request(
            &export_runtime,
            nbd_server::ExportRequest::Read { offset: 0, len: 8 },
        )
        .await
        .expect("read"),
        ExportReply::Read {
            data: b"\0\0\0\0wal\0".to_vec(),
        },
    );

    registry
        .close(&export_name("disk-wal"), &owner)
        .await
        .expect("close wal durable runtime");
    wait_for_checkpoint(&catalog_for_assert, created.id(), WalSeq::new(1)).await;
}

#[tokio::test]
async fn registry_reopen_replays_wal_after_close_compaction_fails() {
    let runtime = TestRuntime::new().expect("test runtime");
    let catalog = migrated_catalog(&runtime).await;
    let created = catalog
        .create_export(create_export_with_engine(
            "disk-wal",
            4096,
            4096,
            ExportEngineKind::WalDurable,
        ))
        .await
        .expect("create export");
    let catalog_for_assert = catalog.clone();
    let failing_tree_store = Arc::new(FailingTreeRecordStore::new(catalog.clone()));
    let registry = local_registry_with_tree_record_store(
        catalog,
        &runtime,
        ServerConfig::default(),
        failing_tree_store.clone(),
    );
    let owner = ExportOwner::unique_connection();

    let export_runtime = registry
        .open(export_name("disk-wal"), owner)
        .await
        .expect("open wal durable runtime");
    execute_request(
        &export_runtime,
        nbd_server::ExportRequest::Write {
            offset: 4,
            data: b"replay".to_vec(),
        },
    )
    .await
    .expect("write");
    failing_tree_store.fail_future_calls();
    registry
        .close(&export_name("disk-wal"), &owner)
        .await
        .expect("close wal durable runtime");
    failing_tree_store.wait_for_attempt().await;
    let head = catalog_for_assert
        .load_export_head(created.id())
        .await
        .expect("load head after failed compaction");
    assert_eq!(head.base_wal_seq(), WalSeq::zero());
    assert!(head.root_node_id().is_none());

    failing_tree_store.allow_future_calls();
    let reopened_owner = ExportOwner::unique_connection();
    let reopened = registry
        .open(export_name("disk-wal"), reopened_owner)
        .await
        .expect("reopen wal durable runtime");
    assert_eq!(
        execute_request(
            &reopened,
            nbd_server::ExportRequest::Read { offset: 0, len: 12 },
        )
        .await
        .expect("read replayed data"),
        ExportReply::Read {
            data: b"\0\0\0\0replay\0\0".to_vec(),
        },
    );
    registry
        .close(&export_name("disk-wal"), &reopened_owner)
        .await
        .expect("close reopened runtime");
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

fn export_name(name: &str) -> ExportName {
    ExportName::new(name).expect("valid export name")
}

fn blob_dir(runtime: &TestRuntime) -> PathBuf {
    runtime.state_dir().join("blobs")
}

fn local_registry(
    catalog: SQLiteExportCatalog,
    runtime: &TestRuntime,
    config: ServerConfig,
) -> LocalExportRegistry {
    let tree_record_store = Arc::new(catalog.clone()) as Arc<dyn TreeRecordStore>;
    local_registry_with_tree_record_store(catalog, runtime, config, tree_record_store)
}

fn local_registry_with_tree_record_store(
    catalog: SQLiteExportCatalog,
    runtime: &TestRuntime,
    config: ServerConfig,
    tree_record_store: Arc<dyn TreeRecordStore>,
) -> LocalExportRegistry {
    let catalog = Arc::new(catalog);
    let export_catalog: Arc<dyn ExportCatalog> = catalog.clone();
    let wal_provider = Arc::new(LocalWalProvider::new(runtime.wal_dir()));
    let factory = Arc::new(ExportFactory::new(
        config,
        ConfiguredBlobStore::local(blob_dir(runtime)),
        export_catalog,
        tree_record_store,
        wal_provider,
    ));
    LocalExportRegistry::new(catalog, factory)
}

struct FailingTreeRecordStore {
    inner: SQLiteExportCatalog,
    fail_calls: AtomicBool,
    attempted: AtomicBool,
    notify: Notify,
}

impl FailingTreeRecordStore {
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
impl TreeRecordStore for FailingTreeRecordStore {
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

async fn execute_request(
    export_runtime: &nbd_server::ExportRuntimeHandle,
    request: nbd_server::ExportRequest,
) -> nbd_server::Result<ExportReply> {
    let queue_slot = export_runtime.reserve().await?;
    let (job, receiver) = nbd_server::ExportJob::oneshot(request, queue_slot);
    export_runtime.submit(job).await?;
    let completed = receiver.await.expect("runtime completion");
    let (result, _queue_slot) = completed.into_parts();
    result
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

async fn wait_for_checkpoint(
    catalog: &SQLiteExportCatalog,
    export_id: &nbd_control_plane::ExportId,
    checkpoint: WalSeq,
) {
    timeout(Duration::from_secs(5), async {
        loop {
            let head = catalog
                .load_export_head(export_id)
                .await
                .expect("load export head");
            if head.base_wal_seq() >= checkpoint && head.root_node_id().is_some() {
                return;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("compaction checkpoint");
}
