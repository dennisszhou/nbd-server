use crate::{
    observability::{self, event, target},
    ConcurrentExportRuntime, ExportEngineHandle, ExportRuntimeHandle, LocalBlobStore,
    MemoryExportEngine, Result, SerialExportRuntime, ServerError, SimpleDurableEngine,
};
use nbd_config::{ExportRuntimeKind, ServerConfig};
use nbd_control_plane::{ExportCatalog, ExportEngineKind, ExportName, SimpleTreeMetadataStore};
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
    simple_tree_store: Arc<dyn SimpleTreeMetadataStore>,
    config: ServerConfig,
    blob_dir: PathBuf,
    active: Mutex<HashMap<ExportName, ActiveExportState>>,
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
    pub fn new<T>(catalog: Arc<T>, config: ServerConfig, blob_dir: impl Into<PathBuf>) -> Self
    where
        T: ExportCatalog + SimpleTreeMetadataStore + 'static,
    {
        let export_catalog: Arc<dyn ExportCatalog> = catalog.clone();
        let simple_tree_store: Arc<dyn SimpleTreeMetadataStore> = catalog;
        Self {
            catalog: export_catalog,
            simple_tree_store,
            config,
            blob_dir: blob_dir.into(),
            active: Mutex::new(HashMap::new()),
        }
    }

    pub fn blob_dir(&self) -> &Path {
        &self.blob_dir
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
                let meta = runtime.export_meta();
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
                    runtime_kind = runtime_kind_name(self.config.export_runtime),
                    queue_depth = self.config.export_queue_depth.get(),
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

        runtime.close().await?;
        self.active()?.remove(name);
        tracing::info!(
            target: target::EXPORT,
            event = event::EXPORT_CLOSE_COMPLETED,
            service = observability::SERVICE_NAME,
            server_instance_id = observability::server_instance_id(),
            pid = observability::pid(),
            export_name = %name,
            owner_id = owner.id().raw(),
        );
        Ok(())
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
        let meta = self
            .catalog
            .load_export(name)
            .await
            .map_err(ServerError::catalog)?;
        tracing::debug!(
            target: target::CATALOG,
            event = event::CATALOG_EXPORT_LOADED,
            service = observability::SERVICE_NAME,
            server_instance_id = observability::server_instance_id(),
            pid = observability::pid(),
            export_id = %meta.id(),
            export_name = %meta.name(),
            engine_kind = %meta.engine_kind(),
            layout_kind = %meta.head().layout_kind(),
            size_bytes = meta.size_bytes(),
            phase = "complete",
        );
        let engine: ExportEngineHandle = match meta.engine_kind() {
            ExportEngineKind::Memory => Arc::new(MemoryExportEngine::new(&meta)?),
            ExportEngineKind::SimpleDurable => Arc::new(
                SimpleDurableEngine::load(
                    &meta,
                    LocalBlobStore::new(self.blob_dir.clone()),
                    self.simple_tree_store.clone(),
                )
                .await?,
            ),
        };
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

fn runtime_kind_name(kind: ExportRuntimeKind) -> &'static str {
    match kind {
        ExportRuntimeKind::Serial => "serial",
        ExportRuntimeKind::Concurrent => "concurrent",
    }
}
