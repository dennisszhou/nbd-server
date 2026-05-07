use crate::error::{Result, ServerError};
use crate::observability::{self, event, target};
use nbd_control_plane::{
    ActiveExportDescriptor, BlobKey, ChunkIndex, NodeId, SimpleChunkRef, SimpleTreeMetadataStore,
    SimpleTreeSnapshot,
};
use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};

pub struct SimpleMutableTree {
    catalog: Arc<dyn SimpleTreeMetadataStore>,
    commit_lock: Mutex<()>,
    state: RwLock<SimpleTreeState>,
}

#[derive(Debug, Clone)]
struct SimpleTreeState {
    export_id: nbd_control_plane::ExportId,
    size_bytes: u64,
    root_node_id: Option<NodeId>,
    chunks: BTreeMap<ChunkIndex, SimpleChunkRef>,
}

impl SimpleMutableTree {
    pub async fn load(
        catalog: Arc<dyn SimpleTreeMetadataStore>,
        descriptor: &ActiveExportDescriptor,
    ) -> Result<Self> {
        tracing::debug!(
            target: target::CATALOG,
            event = event::CATALOG_TREE_LOADED,
            service = observability::SERVICE_NAME,
            server_instance_id = observability::server_instance_id(),
            pid = observability::pid(),
            export_id = %descriptor.id(),
            export_name = %descriptor.name(),
            layout_kind = "simple_mutable_tree",
            phase = "start",
        );
        let snapshot = catalog
            .load_simple_tree(descriptor.id())
            .await
            .map_err(ServerError::catalog)?;
        if snapshot.export_id() != descriptor.id() {
            return Err(ServerError::Catalog {
                message: format!(
                    "simple tree export id {} does not match export {}",
                    snapshot.export_id(),
                    descriptor.id()
                ),
                source: None,
            });
        }

        tracing::debug!(
            target: target::CATALOG,
            event = event::CATALOG_TREE_LOADED,
            service = observability::SERVICE_NAME,
            server_instance_id = observability::server_instance_id(),
            pid = observability::pid(),
            export_id = %snapshot.export_id(),
            export_name = %descriptor.name(),
            layout_kind = "simple_mutable_tree",
            root_node_id = ?snapshot.root_node_id(),
            chunk_count = snapshot.chunks().len(),
            phase = "complete",
        );

        Ok(Self {
            catalog,
            commit_lock: Mutex::new(()),
            state: RwLock::new(SimpleTreeState::from_snapshot(&snapshot)),
        })
    }

    pub async fn size_bytes(&self) -> u64 {
        self.state.read().await.size_bytes
    }

    pub async fn snapshot(&self) -> Result<SimpleTreeSnapshot> {
        self.state
            .read()
            .await
            .to_snapshot()
            .map_err(ServerError::catalog)
    }

    pub async fn lookup_chunk(&self, chunk_index: ChunkIndex) -> Result<Option<BlobKey>> {
        Ok(self
            .state
            .read()
            .await
            .chunks
            .get(&chunk_index)
            .map(|chunk| chunk.blob_key().clone()))
    }

    pub async fn commit_new_chunks(&self, chunks: Vec<SimpleChunkRef>) -> Result<()> {
        if chunks.is_empty() {
            return Ok(());
        }

        let _commit = self.commit_lock.lock().await;
        let export_id = self.state.read().await.export_id.clone();
        tracing::debug!(
            target: target::CATALOG,
            event = event::CATALOG_TREE_COMMIT_STARTED,
            service = observability::SERVICE_NAME,
            server_instance_id = observability::server_instance_id(),
            pid = observability::pid(),
            export_id = %export_id,
            layout_kind = "simple_mutable_tree",
            chunk_count = chunks.len(),
        );
        let snapshot = self
            .catalog
            .commit_simple_chunks(&export_id, chunks)
            .await
            .map_err(ServerError::catalog)?;
        tracing::debug!(
            target: target::CATALOG,
            event = event::CATALOG_TREE_COMMIT_COMPLETED,
            service = observability::SERVICE_NAME,
            server_instance_id = observability::server_instance_id(),
            pid = observability::pid(),
            export_id = %snapshot.export_id(),
            layout_kind = "simple_mutable_tree",
            root_node_id = ?snapshot.root_node_id(),
            chunk_count = snapshot.chunks().len(),
        );
        *self.state.write().await = SimpleTreeState::from_snapshot(&snapshot);
        Ok(())
    }
}

impl fmt::Debug for SimpleMutableTree {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SimpleMutableTree").finish_non_exhaustive()
    }
}

impl SimpleTreeState {
    fn from_snapshot(snapshot: &SimpleTreeSnapshot) -> Self {
        Self {
            export_id: snapshot.export_id().clone(),
            size_bytes: snapshot.size_bytes(),
            root_node_id: snapshot.root_node_id().cloned(),
            chunks: snapshot.chunks().clone(),
        }
    }

    fn to_snapshot(&self) -> nbd_control_plane::Result<SimpleTreeSnapshot> {
        SimpleTreeSnapshot::new(
            self.export_id.clone(),
            self.size_bytes,
            self.root_node_id.clone(),
            self.chunks.clone(),
        )
    }
}
