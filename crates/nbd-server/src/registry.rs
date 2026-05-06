use crate::{
    ConcurrentExportRuntime, ExportEngineHandle, ExportRuntimeHandle, ExportWalHandle,
    LocalBlobStore, MemoryExportEngine, OpenWal, Result, SerialExportRuntime, ServerError,
    SimpleDurableEngine, WalDomain, WalDurableEngine, WalProvider,
    observability::{self, event, target},
};
use nbd_config::{ExportRuntimeKind, ServerConfig};
use nbd_control_plane::{
    ActiveExportDescriptor, CowTreeMetadataStore, ExportCatalog, ExportEngineKind, ExportId,
    ExportName, ExportRecord, SimpleTreeMetadataStore,
};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

static NEXT_EXPORT_OWNER_ID: AtomicU64 = AtomicU64::new(1);

/// Active serving owner for one export runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ExportOwner {
    id: ExportOwnerId,
}

impl ExportOwner {
    pub fn unique_connection() -> Self {
        Self {
            id: ExportOwnerId(NEXT_EXPORT_OWNER_ID.fetch_add(1, Ordering::Relaxed)),
        }
    }

    pub fn id(self) -> ExportOwnerId {
        self.id
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ExportOwnerId(u64);

impl ExportOwnerId {
    pub fn raw(self) -> u64 {
        self.0
    }
}

pub struct LocalExportRegistry {
    catalog: Arc<dyn ExportCatalog>,
    factory: Arc<ExportFactory>,
    active: Mutex<HashMap<ExportName, ActiveExportState>>,
}

/// Concrete factory for active export runtimes and their backing engines.
pub struct ExportFactory {
    config: ServerConfig,
    blob_dir: PathBuf,
    catalog: Arc<dyn ExportCatalog>,
    simple_tree_store: Arc<dyn SimpleTreeMetadataStore>,
    cow_tree_store: Arc<dyn CowTreeMetadataStore>,
    wal_provider: Arc<dyn WalProvider>,
}

struct OpenedEngine {
    meta: ExportRecord,
    engine: ExportEngineHandle,
}

enum ActiveExportState {
    Opening { owner: ExportOwner },
    Open(ActiveExport),
    Closing { owner: ExportOwner },
}

struct ActiveExport {
    owner: ExportOwner,
    runtime: ExportRuntimeHandle,
    connections: usize,
}

impl LocalExportRegistry {
    pub fn new(catalog: Arc<dyn ExportCatalog>, factory: Arc<ExportFactory>) -> Self {
        Self {
            catalog,
            factory,
            active: Mutex::new(HashMap::new()),
        }
    }

    pub fn blob_dir(&self) -> &Path {
        self.factory.blob_dir()
    }

    pub async fn open(&self, name: ExportName, owner: ExportOwner) -> Result<ExportRuntimeHandle> {
        {
            let mut active = self.active()?;
            match active.get_mut(&name) {
                Some(ActiveExportState::Open(active_export)) if active_export.owner == owner => {
                    active_export.connections += 1;
                    return Ok(active_export.runtime.clone());
                }
                Some(_) => return Err(ServerError::ExportBusy { name }),
                None => {
                    active.insert(name.clone(), ActiveExportState::Opening { owner });
                }
            }
        }

        match self.create_runtime(name.clone()).await {
            Ok(runtime) => {
                let mut active = self.active()?;
                let meta = runtime.export_record();
                active.insert(
                    name.clone(),
                    ActiveExportState::Open(ActiveExport {
                        owner,
                        runtime: runtime.clone(),
                        connections: 1,
                    }),
                );
                tracing::info!(
                    target: target::EXPORT,
                    event = event::EXPORT_RUNTIME_SELECTED,
                    service = observability::SERVICE_NAME,
                    server_instance_id = observability::server_instance_id(),
                    pid = observability::pid(),
                    export_id = %meta.id(),
                    export_name = %meta.name(),
                    owner_id = owner.id().raw(),
                    engine_kind = %meta.engine_kind(),
                    runtime_kind = runtime_kind_name(self.factory.export_runtime_kind()),
                    queue_depth = self.factory.export_queue_depth(),
                    connections = 1usize,
                );
                Ok(runtime)
            }
            Err(error) => {
                self.remove_opening(&name, owner)?;
                Err(error)
            }
        }
    }

    pub async fn close(&self, name: &ExportName, owner: &ExportOwner) -> Result<()> {
        let runtime = {
            let mut active = self.active()?;
            let Some(state) = active.get_mut(name) else {
                return Ok(());
            };
            let active_export = match state {
                ActiveExportState::Open(active_export) if &active_export.owner == owner => {
                    active_export
                }
                ActiveExportState::Open(_) => {
                    return Err(ServerError::ExportOwnerMismatch { name: name.clone() });
                }
                ActiveExportState::Opening {
                    owner: active_owner,
                }
                | ActiveExportState::Closing {
                    owner: active_owner,
                } if active_owner == owner => {
                    return Ok(());
                }
                ActiveExportState::Opening { .. } | ActiveExportState::Closing { .. } => {
                    return Err(ServerError::ExportOwnerMismatch { name: name.clone() });
                }
            };

            active_export.connections -= 1;
            if active_export.connections > 0 {
                return Ok(());
            }

            let runtime = active_export.runtime.clone();
            tracing::info!(
                target: target::EXPORT,
                event = event::EXPORT_CLOSE_STARTED,
                service = observability::SERVICE_NAME,
                server_instance_id = observability::server_instance_id(),
                pid = observability::pid(),
                export_name = %name,
                owner_id = owner.id().raw(),
            );
            active.insert(name.clone(), ActiveExportState::Closing { owner: *owner });
            runtime
        };

        let close_result = runtime.close().await;
        {
            let mut active = self.active()?;
            if matches!(
                active.get(name),
                Some(ActiveExportState::Closing { owner: active_owner })
                    if active_owner == owner
            ) {
                active.remove(name);
            }
        }

        match &close_result {
            Ok(()) => {
                tracing::info!(
                    target: target::EXPORT,
                    event = event::EXPORT_CLOSE_COMPLETED,
                    service = observability::SERVICE_NAME,
                    server_instance_id = observability::server_instance_id(),
                    pid = observability::pid(),
                    export_name = %name,
                    owner_id = owner.id().raw(),
                    status = "ok",
                );
            }
            Err(error) => {
                tracing::warn!(
                    target: target::EXPORT,
                    event = event::EXPORT_CLOSE_COMPLETED,
                    service = observability::SERVICE_NAME,
                    server_instance_id = observability::server_instance_id(),
                    pid = observability::pid(),
                    export_name = %name,
                    owner_id = owner.id().raw(),
                    status = "error",
                    error = %error,
                );
            }
        }
        close_result
    }

    async fn create_runtime(&self, name: ExportName) -> Result<ExportRuntimeHandle> {
        tracing::debug!(
            target: target::CATALOG,
            event = event::CATALOG_EXPORT_LOADED,
            service = observability::SERVICE_NAME,
            server_instance_id = observability::server_instance_id(),
            pid = observability::pid(),
            export_name = %name,
            phase = "start",
        );
        let descriptor = self
            .catalog
            .load_export_descriptor(name)
            .await
            .map_err(ServerError::catalog)?;
        tracing::debug!(
            target: target::CATALOG,
            event = event::CATALOG_EXPORT_LOADED,
            service = observability::SERVICE_NAME,
            server_instance_id = observability::server_instance_id(),
            pid = observability::pid(),
            export_id = %descriptor.id(),
            export_name = %descriptor.name(),
            engine_kind = %descriptor.engine_kind(),
            phase = "complete",
        );
        self.factory.open_export(descriptor).await
    }

    fn remove_opening(&self, name: &ExportName, owner: ExportOwner) -> Result<()> {
        let mut active = self.active()?;
        match active.get(name) {
            Some(ActiveExportState::Opening {
                owner: active_owner,
            }) if *active_owner == owner => {
                active.remove(name);
            }
            _ => {}
        }
        Ok(())
    }

    fn active(&self) -> Result<std::sync::MutexGuard<'_, HashMap<ExportName, ActiveExportState>>> {
        self.active.lock().map_err(|_| ServerError::LockPoisoned {
            resource: "local export registry",
        })
    }
}

impl ExportFactory {
    pub fn new(
        config: ServerConfig,
        blob_dir: impl Into<PathBuf>,
        catalog: Arc<dyn ExportCatalog>,
        simple_tree_store: Arc<dyn SimpleTreeMetadataStore>,
        cow_tree_store: Arc<dyn CowTreeMetadataStore>,
        wal_provider: Arc<dyn WalProvider>,
    ) -> Self {
        Self {
            config,
            blob_dir: blob_dir.into(),
            catalog,
            simple_tree_store,
            cow_tree_store,
            wal_provider,
        }
    }

    pub fn blob_dir(&self) -> &Path {
        &self.blob_dir
    }

    fn export_runtime_kind(&self) -> ExportRuntimeKind {
        self.config.export_runtime
    }

    fn export_queue_depth(&self) -> usize {
        self.config.export_queue_depth.get()
    }

    pub async fn open_export(
        &self,
        descriptor: ActiveExportDescriptor,
    ) -> Result<ExportRuntimeHandle> {
        let opened = self.open_engine(&descriptor).await?;
        let meta = opened.meta;
        let engine = opened.engine;
        tracing::info!(
            target: target::EXPORT,
            event = event::EXPORT_ENGINE_LOADED,
            service = observability::SERVICE_NAME,
            server_instance_id = observability::server_instance_id(),
            pid = observability::pid(),
            export_id = %meta.id(),
            export_name = %meta.name(),
            engine_kind = %meta.engine_kind(),
            size_bytes = meta.size_bytes(),
        );

        let runtime: ExportRuntimeHandle = match self.config.export_runtime {
            ExportRuntimeKind::Serial => Arc::new(SerialExportRuntime::with_capacity(
                meta,
                engine,
                self.config.export_queue_depth.get(),
            )),
            ExportRuntimeKind::Concurrent => Arc::new(ConcurrentExportRuntime::with_capacity(
                meta,
                engine,
                self.config.export_queue_depth.get(),
            )),
        };
        Ok(runtime)
    }

    async fn open_engine(&self, descriptor: &ActiveExportDescriptor) -> Result<OpenedEngine> {
        let opened = match descriptor.engine_kind() {
            ExportEngineKind::Memory => {
                let head = self
                    .catalog
                    .load_export_head(descriptor.id())
                    .await
                    .map_err(ServerError::catalog)?;
                let engine: ExportEngineHandle =
                    Arc::new(MemoryExportEngine::from_descriptor(descriptor, &head)?);
                OpenedEngine {
                    meta: descriptor
                        .clone()
                        .into_record(head)
                        .map_err(ServerError::catalog)?,
                    engine,
                }
            }
            ExportEngineKind::SimpleDurable => {
                let engine = SimpleDurableEngine::load(
                    descriptor,
                    LocalBlobStore::new(self.blob_dir.clone()),
                    self.simple_tree_store.clone(),
                )
                .await?;
                let head = engine.export_head().await?;
                let engine: ExportEngineHandle = Arc::new(engine);
                OpenedEngine {
                    meta: descriptor
                        .clone()
                        .into_record(head)
                        .map_err(ServerError::catalog)?,
                    engine,
                }
            }
            ExportEngineKind::WalDurable => {
                let wal = self.open_wal(descriptor.id()).await?;
                let engine = WalDurableEngine::open_with_cow_tree(
                    descriptor,
                    wal,
                    LocalBlobStore::new(self.blob_dir.clone()),
                    self.cow_tree_store.clone(),
                )
                .await?;
                let head = engine.export_head().await?;
                let engine: ExportEngineHandle = Arc::new(engine);
                OpenedEngine {
                    meta: descriptor
                        .clone()
                        .into_record(head)
                        .map_err(ServerError::catalog)?,
                    engine,
                }
            }
        };
        Ok(opened)
    }

    async fn open_wal(&self, export_id: &ExportId) -> Result<ExportWalHandle> {
        let domain = WalDomain::for_export_id(export_id.clone());
        self.wal_provider.open_export(OpenWal::new(domain)).await
    }
}

fn runtime_kind_name(kind: ExportRuntimeKind) -> &'static str {
    match kind {
        ExportRuntimeKind::Serial => "serial",
        ExportRuntimeKind::Concurrent => "concurrent",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ExportJob, ExportQueueSlot, ExportRuntime};
    use nbd_control_plane::{
        CatalogError, CloneExport, CloneExportResult, CowTreeSnapshot, CreateExport, DeleteExport,
        ExportHead, ExportState, InspectExport, ListExports, PublishCompaction,
        PublishCompactionOutcome, SimpleChunkRef, SimpleTreeSnapshot, Timestamp,
    };

    #[tokio::test]
    async fn final_owner_close_error_removes_closing_entry() {
        let name = export_name("disk-a");
        let owner = ExportOwner::unique_connection();
        let runtime = Arc::new(FailingCloseRuntime::new(export_record("disk-a", 4096)));
        let registry = registry_with_active_runtime(name.clone(), owner, runtime);

        assert!(matches!(
            registry.close(&name, &owner).await,
            Err(ServerError::RuntimeClosed { resource }) if resource == "failing test runtime",
        ));
        assert!(
            !registry.active().expect("active map").contains_key(&name),
            "failed final close should not leave a stale Closing entry",
        );
        registry
            .close(&name, &owner)
            .await
            .expect("close is idempotent after cleanup");
    }

    fn registry_with_active_runtime(
        name: ExportName,
        owner: ExportOwner,
        runtime: ExportRuntimeHandle,
    ) -> LocalExportRegistry {
        let catalog = Arc::new(UnusedCatalog);
        let export_catalog: Arc<dyn ExportCatalog> = catalog.clone();
        let simple_tree_store: Arc<dyn SimpleTreeMetadataStore> = catalog.clone();
        let cow_tree_store: Arc<dyn CowTreeMetadataStore> = catalog.clone();
        let factory = Arc::new(ExportFactory::new(
            ServerConfig::default(),
            PathBuf::from("."),
            export_catalog.clone(),
            simple_tree_store,
            cow_tree_store,
            Arc::new(UnusedWalProvider),
        ));
        let mut active = HashMap::new();
        active.insert(
            name,
            ActiveExportState::Open(ActiveExport {
                owner,
                runtime,
                connections: 1,
            }),
        );

        LocalExportRegistry {
            catalog: export_catalog,
            factory,
            active: Mutex::new(active),
        }
    }

    struct FailingCloseRuntime {
        meta: ExportRecord,
    }

    impl FailingCloseRuntime {
        fn new(meta: ExportRecord) -> Self {
            Self { meta }
        }
    }

    #[async_trait::async_trait]
    impl ExportRuntime for FailingCloseRuntime {
        fn export_record(&self) -> ExportRecord {
            self.meta.clone()
        }

        async fn reserve(&self) -> Result<ExportQueueSlot> {
            panic!("failing close runtime should not reserve");
        }

        async fn submit(&self, _job: ExportJob) -> Result<()> {
            panic!("failing close runtime should not submit");
        }

        async fn close(&self) -> Result<()> {
            Err(ServerError::RuntimeClosed {
                resource: "failing test runtime",
            })
        }
    }

    struct UnusedCatalog;

    #[async_trait::async_trait]
    impl ExportCatalog for UnusedCatalog {
        async fn create_export(
            &self,
            _request: CreateExport,
        ) -> nbd_control_plane::Result<ExportRecord> {
            Err(unused_catalog_error())
        }

        async fn clone_export(
            &self,
            _request: CloneExport,
        ) -> nbd_control_plane::Result<CloneExportResult> {
            Err(unused_catalog_error())
        }

        async fn delete_export(&self, _request: DeleteExport) -> nbd_control_plane::Result<()> {
            Err(unused_catalog_error())
        }

        async fn load_export(&self, _name: ExportName) -> nbd_control_plane::Result<ExportRecord> {
            Err(unused_catalog_error())
        }

        async fn load_export_descriptor(
            &self,
            _name: ExportName,
        ) -> nbd_control_plane::Result<ActiveExportDescriptor> {
            Err(unused_catalog_error())
        }

        async fn load_export_head(
            &self,
            _export_id: &ExportId,
        ) -> nbd_control_plane::Result<ExportHead> {
            Err(unused_catalog_error())
        }

        async fn inspect_export(
            &self,
            _request: InspectExport,
        ) -> nbd_control_plane::Result<ExportRecord> {
            Err(unused_catalog_error())
        }

        async fn list_exports(
            &self,
            _request: ListExports,
        ) -> nbd_control_plane::Result<Vec<ExportRecord>> {
            Err(unused_catalog_error())
        }
    }

    #[async_trait::async_trait]
    impl SimpleTreeMetadataStore for UnusedCatalog {
        async fn load_simple_tree(
            &self,
            _export_id: &ExportId,
        ) -> nbd_control_plane::Result<SimpleTreeSnapshot> {
            Err(unused_catalog_error())
        }

        async fn commit_simple_chunks(
            &self,
            _export_id: &ExportId,
            _chunks: Vec<SimpleChunkRef>,
        ) -> nbd_control_plane::Result<SimpleTreeSnapshot> {
            Err(unused_catalog_error())
        }
    }

    #[async_trait::async_trait]
    impl CowTreeMetadataStore for UnusedCatalog {
        async fn load_cow_tree(
            &self,
            _export_id: &ExportId,
        ) -> nbd_control_plane::Result<CowTreeSnapshot> {
            Err(unused_catalog_error())
        }

        async fn publish_compaction(
            &self,
            _request: PublishCompaction,
        ) -> nbd_control_plane::Result<PublishCompactionOutcome> {
            Err(unused_catalog_error())
        }
    }

    struct UnusedWalProvider;

    #[async_trait::async_trait]
    impl WalProvider for UnusedWalProvider {
        async fn open_export(&self, _request: OpenWal) -> Result<ExportWalHandle> {
            Err(ServerError::wal("unused test WAL provider", "not used"))
        }
    }

    fn unused_catalog_error() -> CatalogError {
        CatalogError::database("unused test catalog")
    }

    fn export_name(name: &str) -> ExportName {
        ExportName::new(name).expect("valid export name")
    }

    fn export_record(name: &str, size_bytes: u64) -> ExportRecord {
        ExportRecord::new(
            ExportId::new(format!("export-{name}")).expect("export id"),
            export_name(name),
            4096,
            ExportEngineKind::Memory,
            ExportState::Active,
            ExportHead::memory_empty(size_bytes).expect("memory head"),
            Timestamp::new("created").expect("created timestamp"),
            Timestamp::new("updated").expect("updated timestamp"),
            None,
        )
        .expect("export record")
    }
}
