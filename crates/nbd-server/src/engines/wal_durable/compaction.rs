use super::overlay::OverlayReadSlice;
use super::read_view::{ReadViewCompactionSnapshot, RootSnapshot};
use crate::engines::tree::{LazyTreeMetadataReader, TreeGeometry, TreeNodeSpan, TreeRecordFactory};
use crate::error::{Result, ServerError};
use crate::storage::{BlobStore, BlobStoreHandle, put_random_blob};
use nbd_control_plane::{
    ChunkIndex, CowChunkRef, ExportHead, ExportId, ExportLayoutKind, NodeId, PublishTreeUpdate,
    PublishTreeUpdateOutcome, TREE_CHUNK_BYTES, TreeEdgeLookup, TreeEdgeRecord, TreeNodeKind,
    TreeNodeRecord, TreeRecordBatch, TreeRecordStore, TreeStorageKind, WalSeq,
};
use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::Arc;
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactionResult {
    export_id: ExportId,
    base_wal_seq: WalSeq,
    target_wal_seq: WalSeq,
    compacted_records: u64,
    written_leaf_blobs: u64,
    outcome: CompactionOutcome,
    published_root: Option<RootSnapshot>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompactionOutcome {
    Published,
    StalePlan,
    NoRecords,
}

#[derive(Clone)]
pub struct CowCompactor {
    tree_store: Arc<dyn TreeRecordStore>,
    blob_store: BlobStoreHandle,
}

impl CompactionResult {
    pub fn export_id(&self) -> &ExportId {
        &self.export_id
    }

    pub fn base_wal_seq(&self) -> WalSeq {
        self.base_wal_seq
    }

    pub fn target_wal_seq(&self) -> WalSeq {
        self.target_wal_seq
    }

    pub fn compacted_records(&self) -> u64 {
        self.compacted_records
    }

    pub fn written_leaf_blobs(&self) -> u64 {
        self.written_leaf_blobs
    }

    pub fn outcome(&self) -> CompactionOutcome {
        self.outcome
    }

    pub(crate) fn published_root(&self) -> Option<&RootSnapshot> {
        self.published_root.as_ref()
    }
}

impl CowCompactor {
    pub(crate) fn new(tree_store: Arc<dyn TreeRecordStore>, blob_store: BlobStoreHandle) -> Self {
        Self {
            tree_store,
            blob_store,
        }
    }

    pub(super) async fn compact_snapshot(
        &self,
        export_id: &ExportId,
        snapshot: &ReadViewCompactionSnapshot,
    ) -> Result<CompactionResult> {
        let base_wal_seq = snapshot.root.base_wal_seq();
        let target_wal_seq = snapshot.target_wal_seq;
        if target_wal_seq <= base_wal_seq {
            return Ok(CompactionResult {
                export_id: export_id.clone(),
                base_wal_seq,
                target_wal_seq,
                compacted_records: 0,
                written_leaf_blobs: 0,
                outcome: CompactionOutcome::NoRecords,
                published_root: None,
            });
        }

        let overlay_slices = snapshot.overlay.all_slices();
        if overlay_slices.is_empty() {
            return Ok(CompactionResult {
                export_id: export_id.clone(),
                base_wal_seq,
                target_wal_seq,
                compacted_records: 0,
                written_leaf_blobs: 0,
                outcome: CompactionOutcome::NoRecords,
                published_root: None,
            });
        }

        let mut visible_records = BTreeSet::new();
        let mut chunk_images = BTreeMap::new();
        for slice in &overlay_slices {
            validate_overlay_slice_range(&snapshot.root, target_wal_seq, slice)?;
            visible_records.insert(slice.record.seq());
            apply_overlay_slice_to_chunks(
                self.blob_store.as_ref(),
                self.tree_store.clone(),
                &snapshot.root,
                &mut chunk_images,
                slice,
            )
            .await?;
        }

        let mut chunks = BTreeMap::new();
        let mut written_leaf_blobs = 0u64;
        for (chunk_index, data) in chunk_images {
            let key = put_random_blob(self.blob_store.as_ref(), &data).await?;
            let chunk = CowChunkRef::new(chunk_index, key, TREE_CHUNK_BYTES)
                .map_err(ServerError::catalog)?;
            chunks.insert(chunk_index, chunk);
            written_leaf_blobs += 1;
        }

        let (outcome, published_root) = publish_lazy_compaction(
            self.tree_store.as_ref(),
            export_id,
            &snapshot.root,
            target_wal_seq,
            chunks.into_values().collect(),
        )
        .await?;

        Ok(CompactionResult {
            export_id: export_id.clone(),
            base_wal_seq,
            target_wal_seq,
            compacted_records: visible_records.len() as u64,
            written_leaf_blobs,
            outcome,
            published_root,
        })
    }
}

async fn apply_overlay_slice_to_chunks(
    blob_store: &dyn BlobStore,
    tree_store: Arc<dyn TreeRecordStore>,
    root: &RootSnapshot,
    chunk_images: &mut BTreeMap<ChunkIndex, Vec<u8>>,
    slice: &OverlayReadSlice,
) -> Result<()> {
    let total_len = usize::try_from(slice.end - slice.start).map_err(|_| {
        ServerError::wal("compact overlay slice", "slice length does not fit usize")
    })?;
    let mut copied = 0usize;
    while copied < total_len {
        let current_offset = slice.start + copied as u64;
        let chunk_index = ChunkIndex::new(current_offset / TREE_CHUNK_BYTES);
        let chunk_offset = (current_offset % TREE_CHUNK_BYTES) as usize;
        let chunk_available = TREE_CHUNK_BYTES as usize - chunk_offset;
        let copy_len = chunk_available.min(total_len - copied);
        let src_start = usize::try_from(slice.record_offset)
            .map_err(|_| {
                ServerError::wal("compact overlay slice", "record offset does not fit usize")
            })?
            .checked_add(copied)
            .ok_or_else(|| ServerError::wal("compact overlay slice", "record offset overflowed"))?;
        let src_end = src_start
            .checked_add(copy_len)
            .ok_or_else(|| ServerError::wal("compact overlay slice", "record slice overflowed"))?;
        let chunk = load_or_create_root_chunk(
            blob_store,
            tree_store.clone(),
            root,
            chunk_images,
            chunk_index,
        )
        .await?;
        chunk[chunk_offset..chunk_offset + copy_len]
            .copy_from_slice(&slice.record.data()[src_start..src_end]);
        copied += copy_len;
    }
    Ok(())
}

async fn load_or_create_root_chunk<'a>(
    blob_store: &dyn BlobStore,
    tree_store: Arc<dyn TreeRecordStore>,
    root: &RootSnapshot,
    chunk_images: &'a mut BTreeMap<ChunkIndex, Vec<u8>>,
    chunk_index: ChunkIndex,
) -> Result<&'a mut Vec<u8>> {
    match chunk_images.entry(chunk_index) {
        Entry::Occupied(entry) => Ok(entry.into_mut()),
        Entry::Vacant(entry) => {
            let data = load_committed_chunk(blob_store, tree_store, root, chunk_index).await?;
            Ok(entry.insert(data))
        }
    }
}

async fn load_committed_chunk(
    blob_store: &dyn BlobStore,
    tree_store: Arc<dyn TreeRecordStore>,
    root: &RootSnapshot,
    chunk_index: ChunkIndex,
) -> Result<Vec<u8>> {
    let Some(tree_format) = root.tree_format() else {
        return Ok(vec![0; TREE_CHUNK_BYTES as usize]);
    };
    let geometry = TreeGeometry::new(tree_format, root.size_bytes())?;
    let reader = LazyTreeMetadataReader::new(
        tree_store,
        geometry,
        ExportLayoutKind::CowImmutableTree,
        root.root_node_id().cloned(),
    );
    let Some(leaf) = reader.load_leaf(chunk_index).await? else {
        return Ok(vec![0; TREE_CHUNK_BYTES as usize]);
    };
    if leaf.leaf_ref().storage_kind != TreeStorageKind::ImmutableBlob {
        return Err(ServerError::Catalog {
            message: format!(
                "COW tree chunk {chunk_index} has storage kind {}",
                leaf.leaf_ref().storage_kind
            ),
            source: None,
        });
    }
    blob_store
        .get_blob(&leaf.leaf_ref().storage_key, 0, TREE_CHUNK_BYTES)
        .await
}

fn validate_overlay_slice_range(
    root: &RootSnapshot,
    target_wal_seq: WalSeq,
    slice: &OverlayReadSlice,
) -> Result<()> {
    if slice.record.seq() <= root.base_wal_seq() {
        return Err(ServerError::wal(
            "compact overlay slice",
            format!(
                "record sequence {} is at or before checkpoint {}",
                slice.record.seq(),
                root.base_wal_seq()
            ),
        ));
    }
    if slice.record.seq() > target_wal_seq {
        return Err(ServerError::wal(
            "compact overlay slice",
            format!(
                "record sequence {} is after compaction target {}",
                slice.record.seq(),
                target_wal_seq
            ),
        ));
    }
    let end = slice
        .end
        .checked_sub(slice.start)
        .and_then(|len| slice.start.checked_add(len))
        .ok_or(ServerError::OutOfBounds {
            operation: "compact overlay slice",
            offset: slice.start,
            length: slice.end.saturating_sub(slice.start),
            size_bytes: root.size_bytes(),
        })?;
    if end > root.size_bytes() {
        return Err(ServerError::OutOfBounds {
            operation: "compact overlay slice",
            offset: slice.start,
            length: slice.end.saturating_sub(slice.start),
            size_bytes: root.size_bytes(),
        });
    }
    let record_end = slice
        .record_offset
        .checked_add(slice.end.saturating_sub(slice.start))
        .ok_or_else(|| ServerError::wal("compact overlay slice", "record offset overflowed"))?;
    if record_end > slice.record.data().len() as u64 {
        return Err(ServerError::wal(
            "compact overlay slice",
            "overlay slice extends beyond WAL record payload",
        ));
    }
    Ok(())
}

async fn publish_lazy_compaction(
    tree_store: &dyn TreeRecordStore,
    export_id: &ExportId,
    root: &RootSnapshot,
    compacted_through: WalSeq,
    chunks: Vec<CowChunkRef>,
) -> Result<(CompactionOutcome, Option<RootSnapshot>)> {
    if chunks.is_empty() {
        return Ok((CompactionOutcome::NoRecords, None));
    }

    let tree_format = root.tree_format().ok_or_else(|| ServerError::Catalog {
        message: "COW compaction root is missing tree format".to_owned(),
        source: None,
    })?;
    let geometry = TreeGeometry::new(tree_format, root.size_bytes())?;
    let mut planner = CowTreePublishPlanner::new(geometry, export_id, root.root_node_id());
    let records = planner.plan_chunks(tree_store, &chunks).await?;
    let next_head = ExportHead::new_with_tree_format(
        ExportLayoutKind::CowImmutableTree,
        Some(planner.new_root_node_id().clone()),
        root.size_bytes(),
        compacted_through,
        Some(tree_format),
    )
    .map_err(ServerError::catalog)?;
    let outcome = tree_store
        .publish_tree_update(PublishTreeUpdate {
            export_id: export_id.clone(),
            expected_head: root.to_export_head()?,
            next_head,
            records,
        })
        .await
        .map_err(ServerError::catalog)?;

    match outcome {
        PublishTreeUpdateOutcome::Published(record) => {
            let published = RootSnapshot::from_head(record.head())?;
            Ok((CompactionOutcome::Published, Some(published)))
        }
        PublishTreeUpdateOutcome::StaleHead(_) => Ok((CompactionOutcome::StalePlan, None)),
    }
}

struct CowTreePublishPlanner {
    geometry: TreeGeometry,
    factory: TreeRecordFactory,
    old_root_node_id: Option<NodeId>,
    new_root_node_id: NodeId,
    copied_parents: HashSet<String>,
    new_internal_edges: HashMap<(String, u16), NodeId>,
    pending_edges: HashMap<(String, u16), NodeId>,
    records: TreeRecordBatch,
}

impl CowTreePublishPlanner {
    fn new(
        geometry: TreeGeometry,
        export_id: &ExportId,
        old_root_node_id: Option<&NodeId>,
    ) -> Self {
        let new_root_node_id = generated_node_id();
        let factory = TreeRecordFactory::new(
            geometry,
            ExportLayoutKind::CowImmutableTree,
            Some(export_id.clone()),
        );
        let mut records = TreeRecordBatch::default();
        records
            .nodes
            .push(factory.root_node(new_root_node_id.clone()));
        Self {
            geometry,
            factory,
            old_root_node_id: old_root_node_id.cloned(),
            new_root_node_id,
            copied_parents: HashSet::new(),
            new_internal_edges: HashMap::new(),
            pending_edges: HashMap::new(),
            records,
        }
    }

    fn new_root_node_id(&self) -> &NodeId {
        &self.new_root_node_id
    }

    async fn plan_chunks(
        &mut self,
        tree_store: &dyn TreeRecordStore,
        chunks: &[CowChunkRef],
    ) -> Result<TreeRecordBatch> {
        for chunk in chunks {
            self.plan_chunk(tree_store, chunk).await?;
        }

        for ((parent, slot), child) in std::mem::take(&mut self.pending_edges) {
            self.records.edges.push(self.factory.child_edge(
                NodeId::new(parent).map_err(ServerError::catalog)?,
                slot,
                child,
            ));
        }
        Ok(std::mem::take(&mut self.records))
    }

    async fn plan_chunk(
        &mut self,
        tree_store: &dyn TreeRecordStore,
        chunk: &CowChunkRef,
    ) -> Result<()> {
        let path = self.geometry.path_for_chunk(chunk.chunk_index())?;
        let mut old_parent_id = self.old_root_node_id.clone();
        let mut new_parent_id = self.new_root_node_id.clone();
        let mut parent_span = self.geometry.root_span();

        self.copy_existing_edges_once(
            tree_store,
            old_parent_id.as_ref(),
            &new_parent_id,
            parent_span,
        )
        .await?;

        for slot in path.slots() {
            let child_span = self.geometry.child_span(parent_span, *slot)?;
            let old_child_id = match old_parent_id.as_ref() {
                Some(parent) => load_child_edge(tree_store, parent, *slot)
                    .await?
                    .map(|edge| edge.child_node_id),
                None => None,
            };
            let edge_key = (new_parent_id.as_str().to_owned(), *slot);

            if child_span.level() == 0 {
                let child_id = generated_node_id();
                self.records
                    .nodes
                    .push(self.factory.leaf_node(child_id.clone(), child_span));
                self.records.leaf_refs.push(self.factory.leaf_ref(
                    child_id.clone(),
                    TreeStorageKind::ImmutableBlob,
                    chunk.blob_key().clone(),
                ));
                self.pending_edges.insert(edge_key, child_id);
                return Ok(());
            }

            let child_id = if let Some(child_id) = self.new_internal_edges.get(&edge_key).cloned() {
                child_id
            } else {
                if let Some(old_child_id) = &old_child_id {
                    let old_child = load_required_node(tree_store, old_child_id).await?;
                    validate_existing_node(
                        &old_child,
                        ExportLayoutKind::CowImmutableTree,
                        child_span,
                        TreeNodeKind::Internal,
                    )?;
                }
                let child_id = generated_node_id();
                self.records
                    .nodes
                    .push(self.factory.internal_node(child_id.clone(), child_span));
                self.pending_edges
                    .insert(edge_key.clone(), child_id.clone());
                self.new_internal_edges.insert(edge_key, child_id.clone());
                child_id
            };

            self.copy_existing_edges_once(tree_store, old_child_id.as_ref(), &child_id, child_span)
                .await?;
            old_parent_id = old_child_id;
            new_parent_id = child_id;
            parent_span = child_span;
        }

        Ok(())
    }

    async fn copy_existing_edges_once(
        &mut self,
        tree_store: &dyn TreeRecordStore,
        old_parent_id: Option<&NodeId>,
        new_parent_id: &NodeId,
        span: TreeNodeSpan,
    ) -> Result<()> {
        let Some(old_parent_id) = old_parent_id else {
            return Ok(());
        };
        if !self
            .copied_parents
            .insert(new_parent_id.as_str().to_owned())
        {
            return Ok(());
        }

        let old_parent = load_required_node(tree_store, old_parent_id).await?;
        validate_existing_node(
            &old_parent,
            ExportLayoutKind::CowImmutableTree,
            span,
            TreeNodeKind::Internal,
        )?;
        let slots = (0..self.geometry.fanout()).collect::<Vec<_>>();
        let edges = tree_store
            .load_child_edges(&[TreeEdgeLookup {
                parent_node_id: old_parent_id.clone(),
                slots,
            }])
            .await
            .map_err(ServerError::catalog)?;

        for edge in edges {
            if &edge.parent_node_id != old_parent_id {
                return Err(ServerError::Catalog {
                    message: format!(
                        "node `{old_parent_id}` returned edge for mismatched parent `{}`",
                        edge.parent_node_id
                    ),
                    source: None,
                });
            }
            if edge.slot >= self.geometry.fanout() {
                return Err(ServerError::Catalog {
                    message: format!(
                        "node `{old_parent_id}` returned edge for invalid slot {}",
                        edge.slot
                    ),
                    source: None,
                });
            }
            self.pending_edges.insert(
                (new_parent_id.as_str().to_owned(), edge.slot),
                edge.child_node_id,
            );
        }
        Ok(())
    }
}

async fn load_child_edge(
    tree_store: &dyn TreeRecordStore,
    parent_node_id: &NodeId,
    slot: u16,
) -> Result<Option<TreeEdgeRecord>> {
    let edges = tree_store
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
    tree_store: &dyn TreeRecordStore,
    node_id: &NodeId,
) -> Result<TreeNodeRecord> {
    tree_store
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
            message: format!("tree node `{}` does not match COW tree geometry", node.id),
            source: None,
        });
    }
    Ok(())
}

fn generated_node_id() -> NodeId {
    NodeId::new(Uuid::new_v4().to_string()).expect("generated node id")
}
