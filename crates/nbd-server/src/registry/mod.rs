mod active;
mod factory;

pub use active::ExportOwner;
use active::{ActiveExport, ActiveExportState};
pub use factory::ExportFactory;
use factory::runtime_kind_name;

use crate::error::{Result, ServerError};
use crate::export::ExportRuntimeHandle;
use crate::observability::{self, event, target};
use nbd_control_plane::{ExportCatalog, ExportName};
use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex};

pub struct LocalExportRegistry {
    catalog: Arc<dyn ExportCatalog>,
    factory: Arc<ExportFactory>,
    active: Mutex<HashMap<ExportName, ActiveExportState>>,
}

impl LocalExportRegistry {
    pub fn new(catalog: Arc<dyn ExportCatalog>, factory: Arc<ExportFactory>) -> Self {
        Self {
            catalog,
            factory,
            active: Mutex::new(HashMap::new()),
        }
    }

    pub fn blob_dir(&self) -> Option<&Path> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::export::{ExportJob, ExportQueueSlot, ExportRuntime};
    use crate::storage::ConfiguredBlobStore;
    use crate::wal::{ExportWalHandle, OpenWal, WalProvider};
    use nbd_config::ServerConfig;
    use nbd_control_plane::{
        ActiveExportDescriptor, CatalogError, CloneExport, CloneExportResult, CreateExport,
        DeleteExport, ExportEngineKind, ExportHead, ExportId, ExportRecord, ExportState,
        InspectExport, ListExports, NodeId, PublishTreeUpdate, PublishTreeUpdateOutcome, Timestamp,
        TreeEdgeLookup, TreeEdgeRecord, TreeLeafRefRecord, TreeNodeRecord, TreeRecordStore,
    };
    use std::path::PathBuf;

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
        let tree_record_store: Arc<dyn TreeRecordStore> = catalog.clone();
        let factory = Arc::new(ExportFactory::new(
            ServerConfig::default(),
            ConfiguredBlobStore::local(PathBuf::from(".")),
            export_catalog.clone(),
            tree_record_store,
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
    impl TreeRecordStore for UnusedCatalog {
        async fn load_node(
            &self,
            _node_id: &NodeId,
        ) -> nbd_control_plane::Result<Option<TreeNodeRecord>> {
            Err(unused_catalog_error())
        }

        async fn load_nodes(
            &self,
            _node_ids: &[NodeId],
        ) -> nbd_control_plane::Result<Vec<TreeNodeRecord>> {
            Err(unused_catalog_error())
        }

        async fn load_child_edges(
            &self,
            _lookups: &[TreeEdgeLookup],
        ) -> nbd_control_plane::Result<Vec<TreeEdgeRecord>> {
            Err(unused_catalog_error())
        }

        async fn load_leaf_refs(
            &self,
            _node_ids: &[NodeId],
        ) -> nbd_control_plane::Result<Vec<TreeLeafRefRecord>> {
            Err(unused_catalog_error())
        }

        async fn publish_tree_update(
            &self,
            _request: PublishTreeUpdate,
        ) -> nbd_control_plane::Result<PublishTreeUpdateOutcome> {
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
