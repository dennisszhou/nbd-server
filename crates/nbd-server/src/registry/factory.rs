use crate::engines::{MemoryExportEngine, SimpleDurableEngine, WalDurableEngine};
use crate::error::{Result, ServerError};
use crate::export::{
    ConcurrentExportRuntime, ExportEngineHandle, ExportRuntimeHandle, SerialExportRuntime,
};
use crate::observability::{self, event, target};
use crate::storage::{BlobStoreHandle, ConfiguredBlobStore};
use crate::wal::{ExportWalHandle, OpenWal, WalDomain, WalProvider};
use nbd_config::{ExportRuntimeKind, ServerConfig};
use nbd_control_plane::{
    ActiveExportDescriptor, CowTreeMetadataStore, ExportCatalog, ExportEngineKind, ExportId,
    ExportRecord, SimpleTreeMetadataStore,
};
use std::path::Path;
use std::sync::Arc;

/// Concrete factory for active export runtimes and their backing engines.
pub struct ExportFactory {
    config: ServerConfig,
    blob_store: ConfiguredBlobStore,
    catalog: Arc<dyn ExportCatalog>,
    simple_tree_store: Arc<dyn SimpleTreeMetadataStore>,
    cow_tree_store: Arc<dyn CowTreeMetadataStore>,
    wal_provider: Arc<dyn WalProvider>,
}

struct OpenedEngine {
    meta: ExportRecord,
    engine: ExportEngineHandle,
}

impl ExportFactory {
    pub fn new(
        config: ServerConfig,
        blob_store: ConfiguredBlobStore,
        catalog: Arc<dyn ExportCatalog>,
        simple_tree_store: Arc<dyn SimpleTreeMetadataStore>,
        cow_tree_store: Arc<dyn CowTreeMetadataStore>,
        wal_provider: Arc<dyn WalProvider>,
    ) -> Self {
        Self {
            config,
            blob_store,
            catalog,
            simple_tree_store,
            cow_tree_store,
            wal_provider,
        }
    }

    pub fn blob_dir(&self) -> Option<&Path> {
        self.blob_store.local_root()
    }

    pub(super) fn export_runtime_kind(&self) -> ExportRuntimeKind {
        self.config.export_runtime
    }

    pub(super) fn export_queue_depth(&self) -> usize {
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
                let blob_store =
                    self.blob_store
                        .mutable_blob_store()
                        .ok_or_else(|| ServerError::Io {
                            context: "open simple_durable export",
                            message: "configured blob store does not support mutable blobs"
                                .to_owned(),
                            source: None,
                        })?;
                let engine = SimpleDurableEngine::load(
                    descriptor,
                    blob_store,
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
                let blob_store: BlobStoreHandle = self.blob_store.blob_store();
                let engine = WalDurableEngine::open_with_cow_tree(
                    descriptor,
                    wal,
                    blob_store,
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

pub(super) fn runtime_kind_name(kind: ExportRuntimeKind) -> &'static str {
    match kind {
        ExportRuntimeKind::Serial => "serial",
        ExportRuntimeKind::Concurrent => "concurrent",
    }
}
