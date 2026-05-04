use crate::{
    ConcurrentExportRuntime, ExportEngineHandle, ExportRuntimeHandle, MemoryExportEngine, Result,
    SerialExportRuntime, ServerError,
};
use nbd_config::{ExportRuntimeKind, ServerConfig};
use nbd_control_plane::{ExportCatalog, ExportEngineKind, ExportName};
use std::collections::HashMap;
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
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ExportOwnerId(u64);

pub struct LocalExportRegistry {
    catalog: Arc<dyn ExportCatalog>,
    config: ServerConfig,
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
    pub fn new(catalog: Arc<dyn ExportCatalog>, config: ServerConfig) -> Self {
        Self {
            catalog,
            config,
            active: Mutex::new(HashMap::new()),
        }
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
                active.insert(
                    name,
                    ActiveExportState::Open(ActiveExport {
                        owner,
                        runtime: runtime.clone(),
                        connections: 1,
                    }),
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
            active.insert(name.clone(), ActiveExportState::Closing { owner: *owner });
            runtime
        };

        runtime.close().await?;
        self.active()?.remove(name);
        Ok(())
    }

    async fn create_runtime(&self, name: ExportName) -> Result<ExportRuntimeHandle> {
        let meta = self
            .catalog
            .load_export(name)
            .await
            .map_err(ServerError::catalog)?;
        let engine: ExportEngineHandle = match meta.engine_kind() {
            ExportEngineKind::Memory => Arc::new(MemoryExportEngine::new(&meta)?),
        };
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
