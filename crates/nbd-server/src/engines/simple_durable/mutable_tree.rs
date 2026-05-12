use crate::engines::tree::{LazyTreeMetadataReader, TreeGeometry, TreeNodeSpan, TreeRecordFactory};
use crate::error::{Result, ServerError};
use crate::observability::{self, event, target};
use nbd_control_plane::{
    ActiveExportDescriptor, BlobKey, ChunkIndex, ExportHead, ExportId, ExportLayoutKind, NodeId,
    PublishTreeUpdate, PublishTreeUpdateOutcome, SimpleChunkRef, SimpleTreeSnapshot,
    TreeEdgeLookup, TreeEdgeRecord, TreeFormat, TreeNodeKind, TreeNodeRecord, TreeRecordBatch,
    TreeRecordStore, TreeStorageKind, WalSeq,
};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fmt;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};
use uuid::Uuid;

pub struct SimpleMutableTree {
    catalog: Arc<dyn TreeRecordStore>,
    commit_lock: Mutex<()>,
    state: RwLock<SimpleTreeState>,
}

#[derive(Debug, Clone)]
struct SimpleTreeState {
    export_id: ExportId,
    size_bytes: u64,
    root_node_id: Option<NodeId>,
    tree_format: TreeFormat,
    chunks: BTreeMap<ChunkIndex, SimpleChunkRef>,
}

impl SimpleMutableTree {
    pub async fn load(
        catalog: Arc<dyn TreeRecordStore>,
        descriptor: &ActiveExportDescriptor,
        head: ExportHead,
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
        if head.layout_kind() != ExportLayoutKind::SimpleMutableTree {
            return Err(ServerError::Catalog {
                message: format!(
                    "simple durable export requires simple_mutable_tree head, got {}",
                    head.layout_kind()
                ),
                source: None,
            });
        }
        let tree_format = head.tree_format().ok_or_else(|| ServerError::Catalog {
            message: "simple durable head is missing tree format".to_owned(),
            source: None,
        })?;
        let _geometry = TreeGeometry::new(tree_format, head.size_bytes())?;

        tracing::debug!(
            target: target::CATALOG,
            event = event::CATALOG_TREE_LOADED,
            service = observability::SERVICE_NAME,
            server_instance_id = observability::server_instance_id(),
            pid = observability::pid(),
            export_id = %descriptor.id(),
            export_name = %descriptor.name(),
            layout_kind = "simple_mutable_tree",
            root_node_id = ?head.root_node_id(),
            tree_format = %tree_format,
            phase = "complete",
        );

        Ok(Self {
            catalog,
            commit_lock: Mutex::new(()),
            state: RwLock::new(SimpleTreeState {
                export_id: descriptor.id().clone(),
                size_bytes: head.size_bytes(),
                root_node_id: head.root_node_id().cloned(),
                tree_format,
                chunks: BTreeMap::new(),
            }),
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

    pub async fn export_head(&self) -> Result<ExportHead> {
        self.state
            .read()
            .await
            .export_head()
            .map_err(ServerError::catalog)
    }

    pub async fn lookup_chunk(&self, chunk_index: ChunkIndex) -> Result<Option<BlobKey>> {
        if let Some(key) = self
            .state
            .read()
            .await
            .chunks
            .get(&chunk_index)
            .map(|chunk| chunk.blob_key().clone())
        {
            return Ok(Some(key));
        }

        let state = self.state.read().await.clone();
        let geometry = TreeGeometry::new(state.tree_format, state.size_bytes)?;
        let reader = LazyTreeMetadataReader::new(
            self.catalog.clone(),
            geometry,
            ExportLayoutKind::SimpleMutableTree,
            state.root_node_id,
        );
        let Some(leaf) = reader.load_leaf(chunk_index).await? else {
            return Ok(None);
        };
        if leaf.leaf_ref().storage_kind != TreeStorageKind::MutableBlob {
            return Err(ServerError::Catalog {
                message: format!(
                    "simple tree chunk {chunk_index} has storage kind {}",
                    leaf.leaf_ref().storage_kind
                ),
                source: None,
            });
        }
        let chunk = SimpleChunkRef::new(
            chunk_index,
            leaf.leaf_ref().storage_key.clone(),
            leaf.leaf_ref().len_bytes,
        )
        .map_err(ServerError::catalog)?;
        let key = chunk.blob_key().clone();
        self.state
            .write()
            .await
            .chunks
            .entry(chunk_index)
            .or_insert(chunk);
        Ok(Some(key))
    }

    pub async fn commit_new_chunks(&self, chunks: Vec<SimpleChunkRef>) -> Result<()> {
        if chunks.is_empty() {
            return Ok(());
        }

        let _commit = self.commit_lock.lock().await;
        let export_id = self.state.read().await.export_id.clone();
        let mut seen = BTreeSet::new();
        for chunk in &chunks {
            if !seen.insert(chunk.chunk_index()) {
                return Err(ServerError::Catalog {
                    message: format!("duplicate chunk index {}", chunk.chunk_index()),
                    source: None,
                });
            }
            if self.lookup_chunk(chunk.chunk_index()).await?.is_some() {
                return Err(ServerError::Catalog {
                    message: format!("chunk {} is already materialized", chunk.chunk_index()),
                    source: None,
                });
            }
        }
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
        let state = self.state.read().await.clone();
        let geometry = TreeGeometry::new(state.tree_format, state.size_bytes)?;
        let mut planner = SimpleTreeCommitPlanner::new(geometry, &state);
        let records = planner.plan_chunks(&chunks, self.catalog.as_ref()).await?;
        let next_root = planner.root_node_id().cloned();
        let expected_head = state.export_head().map_err(ServerError::catalog)?;
        let next_head = ExportHead::new_with_tree_format(
            ExportLayoutKind::SimpleMutableTree,
            next_root.clone(),
            state.size_bytes,
            WalSeq::zero(),
            Some(state.tree_format),
        )
        .map_err(ServerError::catalog)?;
        let outcome = self
            .catalog
            .publish_tree_update(PublishTreeUpdate {
                export_id: export_id.clone(),
                expected_head,
                next_head: next_head.clone(),
                records,
            })
            .await
            .map_err(ServerError::catalog)?;
        let published = match outcome {
            PublishTreeUpdateOutcome::Published(record) => record,
            PublishTreeUpdateOutcome::StaleHead(record) => {
                return Err(ServerError::Catalog {
                    message: format!(
                        "simple tree publish lost stale head race; current root is {:?}",
                        record.head().root_node_id()
                    ),
                    source: None,
                });
            }
        };
        tracing::debug!(
            target: target::CATALOG,
            event = event::CATALOG_TREE_COMMIT_COMPLETED,
            service = observability::SERVICE_NAME,
            server_instance_id = observability::server_instance_id(),
            pid = observability::pid(),
            export_id = %published.id(),
            layout_kind = "simple_mutable_tree",
            root_node_id = ?published.head().root_node_id(),
            chunk_count = chunks.len(),
        );
        let mut state = self.state.write().await;
        state.root_node_id = next_head.root_node_id().cloned();
        for chunk in chunks {
            state.chunks.insert(chunk.chunk_index(), chunk);
        }
        Ok(())
    }
}

impl fmt::Debug for SimpleMutableTree {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SimpleMutableTree").finish_non_exhaustive()
    }
}

impl SimpleTreeState {
    fn to_snapshot(&self) -> nbd_control_plane::Result<SimpleTreeSnapshot> {
        SimpleTreeSnapshot::new(
            self.export_id.clone(),
            self.size_bytes,
            self.root_node_id.clone(),
            self.chunks.clone(),
        )
    }

    fn export_head(&self) -> nbd_control_plane::Result<ExportHead> {
        ExportHead::new_with_tree_format(
            ExportLayoutKind::SimpleMutableTree,
            self.root_node_id.clone(),
            self.size_bytes,
            WalSeq::zero(),
            Some(self.tree_format),
        )
    }
}

struct SimpleTreeCommitPlanner {
    geometry: TreeGeometry,
    factory: TreeRecordFactory,
    root_node_id: Option<NodeId>,
    pending_nodes: HashSet<String>,
    pending_edges: HashMap<(String, u16), NodeId>,
    records: TreeRecordBatch,
}

impl SimpleTreeCommitPlanner {
    fn new(geometry: TreeGeometry, state: &SimpleTreeState) -> Self {
        Self {
            geometry,
            factory: TreeRecordFactory::new(
                geometry,
                ExportLayoutKind::SimpleMutableTree,
                Some(state.export_id.clone()),
            ),
            root_node_id: state.root_node_id.clone(),
            pending_nodes: HashSet::new(),
            pending_edges: HashMap::new(),
            records: TreeRecordBatch::default(),
        }
    }

    fn root_node_id(&self) -> Option<&NodeId> {
        self.root_node_id.as_ref()
    }

    async fn plan_chunks(
        &mut self,
        chunks: &[SimpleChunkRef],
        catalog: &dyn TreeRecordStore,
    ) -> Result<TreeRecordBatch> {
        for chunk in chunks {
            self.plan_chunk(chunk, catalog).await?;
        }
        Ok(std::mem::take(&mut self.records))
    }

    async fn plan_chunk(
        &mut self,
        chunk: &SimpleChunkRef,
        catalog: &dyn TreeRecordStore,
    ) -> Result<()> {
        let path = self.geometry.path_for_chunk(chunk.chunk_index())?;
        let mut parent_id = self.ensure_root();
        let mut parent_span = self.geometry.root_span();

        for slot in path.slots() {
            let child_span = self.geometry.child_span(parent_span, *slot)?;
            let child_key = (parent_id.as_str().to_owned(), *slot);
            if let Some(child_id) = self.pending_edges.get(&child_key).cloned() {
                if child_span.level() == 0 {
                    return Err(ServerError::Catalog {
                        message: format!("chunk {} is already pending", chunk.chunk_index()),
                        source: None,
                    });
                }
                parent_id = child_id;
                parent_span = child_span;
                continue;
            }

            if !self.pending_nodes.contains(parent_id.as_str()) {
                if let Some(edge) = load_child_edge(catalog, &parent_id, *slot).await? {
                    if child_span.level() == 0 {
                        return Err(ServerError::Catalog {
                            message: format!(
                                "chunk {} is already materialized",
                                chunk.chunk_index()
                            ),
                            source: None,
                        });
                    }
                    let child = load_required_node(catalog, &edge.child_node_id).await?;
                    validate_existing_node(
                        &child,
                        ExportLayoutKind::SimpleMutableTree,
                        child_span,
                        TreeNodeKind::Internal,
                    )?;
                    parent_id = edge.child_node_id;
                    parent_span = child_span;
                    continue;
                }
            }

            let child_id = NodeId::new(Uuid::new_v4().to_string()).map_err(ServerError::catalog)?;
            if child_span.level() == 0 {
                self.records
                    .nodes
                    .push(self.factory.leaf_node(child_id.clone(), child_span));
                self.records.leaf_refs.push(self.factory.leaf_ref(
                    child_id.clone(),
                    TreeStorageKind::MutableBlob,
                    chunk.blob_key().clone(),
                ));
            } else {
                self.records
                    .nodes
                    .push(self.factory.internal_node(child_id.clone(), child_span));
            }
            self.records.edges.push(self.factory.child_edge(
                parent_id.clone(),
                *slot,
                child_id.clone(),
            ));
            self.pending_nodes.insert(child_id.as_str().to_owned());
            self.pending_edges.insert(child_key, child_id.clone());
            parent_id = child_id;
            parent_span = child_span;
        }

        Ok(())
    }

    fn ensure_root(&mut self) -> NodeId {
        if let Some(root) = &self.root_node_id {
            return root.clone();
        }

        let root = NodeId::new(Uuid::new_v4().to_string()).expect("generated node id");
        self.records
            .nodes
            .push(self.factory.root_node(root.clone()));
        self.pending_nodes.insert(root.as_str().to_owned());
        self.root_node_id = Some(root.clone());
        root
    }
}

async fn load_child_edge(
    catalog: &dyn TreeRecordStore,
    parent_node_id: &NodeId,
    slot: u16,
) -> Result<Option<TreeEdgeRecord>> {
    let edges = catalog
        .load_child_edges(&[TreeEdgeLookup {
            parent_node_id: parent_node_id.clone(),
            slots: vec![slot],
        }])
        .await
        .map_err(ServerError::catalog)?;
    if edges.len() > 1 {
        return Err(ServerError::Catalog {
            message: format!("node `{parent_node_id}` has duplicate edge for slot {slot}"),
            source: None,
        });
    }
    let edge = edges.into_iter().next();
    if let Some(edge) = &edge {
        if &edge.parent_node_id != parent_node_id || edge.slot != slot {
            return Err(ServerError::Catalog {
                message: format!(
                    "node `{parent_node_id}` slot {slot} returned mismatched edge row"
                ),
                source: None,
            });
        }
    }
    Ok(edge)
}

async fn load_required_node(
    catalog: &dyn TreeRecordStore,
    node_id: &NodeId,
) -> Result<TreeNodeRecord> {
    catalog
        .load_node(node_id)
        .await
        .map_err(ServerError::catalog)?
        .ok_or_else(|| ServerError::Catalog {
            message: format!("tree node `{node_id}` is missing"),
            source: None,
        })
}

fn validate_existing_node(
    node: &TreeNodeRecord,
    layout_kind: ExportLayoutKind,
    span: TreeNodeSpan,
    kind: TreeNodeKind,
) -> Result<()> {
    if node.layout_kind != layout_kind
        || node.kind != kind
        || node.level != span.level()
        || node.span_start_bytes != span.start_bytes()
        || node.span_len_bytes != span.len_bytes()
    {
        return Err(ServerError::Catalog {
            message: format!(
                "tree node `{}` does not match simple tree geometry",
                node.id
            ),
            source: None,
        });
    }
    Ok(())
}
