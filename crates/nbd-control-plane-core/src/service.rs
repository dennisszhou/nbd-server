//! Storage-neutral catalog service traits and handle bundle.

use crate::error::Result;
use crate::export::{
    ActiveExportDescriptor, CloneExport, CloneExportResult, CreateExport, DeleteExport, ExportHead,
    ExportId, ExportName, ExportRecord, InspectExport, ListExports,
};
use crate::tree::{
    NodeId, PublishTreeUpdate, PublishTreeUpdateOutcome, TreeEdgeLookup, TreeEdgeRecord,
    TreeLeafRefRecord, TreeNodeRecord,
};
use std::sync::Arc;

/// Runtime metadata boundary for export catalog operations.
#[async_trait::async_trait]
pub trait ExportCatalog: Send + Sync {
    async fn create_export(&self, request: CreateExport) -> Result<ExportRecord>;

    async fn clone_export(&self, request: CloneExport) -> Result<CloneExportResult>;

    async fn delete_export(&self, request: DeleteExport) -> Result<()>;

    /// Load an export for serving/open paths.
    ///
    /// Implementations must reject deleted exports.
    async fn load_export(&self, name: ExportName) -> Result<ExportRecord>;

    /// Load exports-only metadata for serving/open paths.
    ///
    /// Implementations must reject deleted exports. Storage engines must load
    /// the latest serving head or tree snapshot separately.
    async fn load_export_descriptor(&self, name: ExportName) -> Result<ActiveExportDescriptor>;

    /// Load the latest serving head for an export.
    async fn load_export_head(&self, export_id: &ExportId) -> Result<ExportHead>;

    /// Inspect an export for operator visibility.
    ///
    /// Unlike `load_export`, this may return deleted exports.
    async fn inspect_export(&self, request: InspectExport) -> Result<ExportRecord>;

    async fn list_exports(&self, request: ListExports) -> Result<Vec<ExportRecord>>;
}

#[async_trait::async_trait]
pub trait TreeRecordStore: Send + Sync {
    async fn load_node(&self, node_id: &NodeId) -> Result<Option<TreeNodeRecord>>;

    async fn load_nodes(&self, node_ids: &[NodeId]) -> Result<Vec<TreeNodeRecord>>;

    async fn load_child_edges(&self, lookups: &[TreeEdgeLookup]) -> Result<Vec<TreeEdgeRecord>>;

    async fn load_leaf_refs(&self, node_ids: &[NodeId]) -> Result<Vec<TreeLeafRefRecord>>;

    async fn publish_tree_update(
        &self,
        request: PublishTreeUpdate,
    ) -> Result<PublishTreeUpdateOutcome>;
}

/// Opened catalog service handles for runtime consumers.
#[derive(Clone)]
pub struct CatalogHandle {
    export_catalog: Arc<dyn ExportCatalog>,
    tree_record_store: Arc<dyn TreeRecordStore>,
}

impl CatalogHandle {
    pub fn new(
        export_catalog: Arc<dyn ExportCatalog>,
        tree_record_store: Arc<dyn TreeRecordStore>,
    ) -> Self {
        Self {
            export_catalog,
            tree_record_store,
        }
    }

    pub fn export_catalog(&self) -> Arc<dyn ExportCatalog> {
        Arc::clone(&self.export_catalog)
    }

    pub fn tree_record_store(&self) -> Arc<dyn TreeRecordStore> {
        Arc::clone(&self.tree_record_store)
    }
}
