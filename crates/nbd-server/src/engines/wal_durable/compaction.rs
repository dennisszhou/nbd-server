use super::overlay::OverlayReadSlice;
use super::read_view::{ReadViewCompactionSnapshot, RootSnapshot};
use crate::engines::tree::{LazyTreeMetadataReader, TreeGeometry, TreeNodeSpan, TreeRecordFactory};
use crate::error::{Result, ServerError};
use crate::storage::{BlobStore, BlobStoreHandle, put_random_blob};
use crate::wal::ExportWalHandle;
use nbd_control_plane::{
    ChunkIndex, CowChunkRef, CowTreeMetadataStore, CowTreeSnapshot, ExportHead, ExportId,
    ExportLayoutKind, NodeId, PublishCompaction, PublishCompactionOutcome, PublishTreeUpdate,
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
    AlreadyCovered,
    StalePlan,
    NoRecords,
}

#[derive(Clone)]
pub struct CowCompactor {
    catalog: Option<Arc<dyn CowTreeMetadataStore>>,
    tree_store: Option<Arc<dyn TreeRecordStore>>,
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
    pub fn new(catalog: Arc<dyn CowTreeMetadataStore>, blob_store: BlobStoreHandle) -> Self {
        Self {
            catalog: Some(catalog),
            tree_store: None,
            blob_store,
        }
    }

    pub(crate) fn new_lazy(
        tree_store: Arc<dyn TreeRecordStore>,
        blob_store: BlobStoreHandle,
    ) -> Self {
        Self {
            catalog: None,
            tree_store: Some(tree_store),
            blob_store,
        }
    }

    pub async fn compact_export(
        &self,
        export_id: &ExportId,
        wal: &ExportWalHandle,
        through_wal_seq: WalSeq,
    ) -> Result<CompactionResult> {
        let catalog = self.catalog.as_ref().ok_or_else(|| ServerError::Catalog {
            message: "direct export compaction requires legacy COW catalog".to_owned(),
            source: None,
        })?;
        let snapshot = catalog
            .load_cow_tree(export_id)
            .await
            .map_err(ServerError::catalog)?;
        let bounds = wal.bounds().await?;
        let target_wal_seq = through_wal_seq.min(bounds.last_durable);
        let base_wal_seq = snapshot.base_wal_seq();

        if target_wal_seq <= base_wal_seq {
            return Ok(CompactionResult {
                export_id: export_id.clone(),
                base_wal_seq,
                target_wal_seq,
                compacted_records: 0,
                written_leaf_blobs: 0,
                outcome: CompactionOutcome::AlreadyCovered,
                published_root: None,
            });
        }

        let mut replay = wal.replay_range(base_wal_seq, target_wal_seq).await?;
        let mut chunk_images = BTreeMap::new();
        let mut compacted_records = 0u64;
        while let Some(record) = replay.next_record().await? {
            compacted_records += 1;
            apply_record_to_chunks(
                self.blob_store.as_ref(),
                &snapshot,
                &mut chunk_images,
                &record,
            )
            .await?;
        }

        if compacted_records == 0 {
            return Ok(CompactionResult {
                export_id: export_id.clone(),
                base_wal_seq,
                target_wal_seq,
                compacted_records,
                written_leaf_blobs: 0,
                outcome: CompactionOutcome::NoRecords,
                published_root: None,
            });
        }

        let mut chunks = snapshot.chunks().clone();
        let mut written_leaf_blobs = 0u64;
        for (chunk_index, data) in chunk_images {
            let key = put_random_blob(self.blob_store.as_ref(), &data).await?;
            let chunk = CowChunkRef::new(chunk_index, key, TREE_CHUNK_BYTES)
                .map_err(ServerError::catalog)?;
            chunks.insert(chunk_index, chunk);
            written_leaf_blobs += 1;
        }

        let expected_base = snapshot_to_export_head(&snapshot)?;
        let publication = catalog
            .publish_compaction(
                PublishCompaction::new(
                    export_id.clone(),
                    expected_base,
                    target_wal_seq,
                    chunks.into_values().collect(),
                )
                .map_err(ServerError::catalog)?,
            )
            .await
            .map_err(ServerError::catalog)?;
        let outcome = match publication {
            PublishCompactionOutcome::Published(_) => CompactionOutcome::Published,
            PublishCompactionOutcome::AlreadyCovered(_) => CompactionOutcome::AlreadyCovered,
            PublishCompactionOutcome::StalePlan(_) => CompactionOutcome::StalePlan,
        };

        Ok(CompactionResult {
            export_id: export_id.clone(),
            base_wal_seq,
            target_wal_seq,
            compacted_records,
            written_leaf_blobs,
            outcome,
            published_root: None,
        })
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
                outcome: CompactionOutcome::AlreadyCovered,
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

        let mut chunks = if self.tree_store.is_some() {
            BTreeMap::new()
        } else {
            snapshot
                .root
                .cow_tree()
                .map(CowTreeSnapshot::chunks)
                .cloned()
                .unwrap_or_default()
        };
        let mut written_leaf_blobs = 0u64;
        for (chunk_index, data) in chunk_images {
            let key = put_random_blob(self.blob_store.as_ref(), &data).await?;
            let chunk = CowChunkRef::new(chunk_index, key, TREE_CHUNK_BYTES)
                .map_err(ServerError::catalog)?;
            chunks.insert(chunk_index, chunk);
            written_leaf_blobs += 1;
        }

        let (outcome, published_root) = if let Some(tree_store) = &self.tree_store {
            publish_lazy_compaction(
                tree_store.as_ref(),
                export_id,
                &snapshot.root,
                target_wal_seq,
                chunks.into_values().collect(),
            )
            .await?
        } else {
            let catalog = self.catalog.as_ref().ok_or_else(|| ServerError::Catalog {
                message: "legacy COW compaction requires COW catalog".to_owned(),
                source: None,
            })?;
            let publication = catalog
                .publish_compaction(
                    PublishCompaction::new(
                        export_id.clone(),
                        snapshot.root.to_export_head()?,
                        target_wal_seq,
                        chunks.into_values().collect(),
                    )
                    .map_err(ServerError::catalog)?,
                )
                .await
                .map_err(ServerError::catalog)?;
            let outcome = match publication {
                PublishCompactionOutcome::Published(_) => CompactionOutcome::Published,
                PublishCompactionOutcome::AlreadyCovered(_) => CompactionOutcome::AlreadyCovered,
                PublishCompactionOutcome::StalePlan(_) => CompactionOutcome::StalePlan,
            };
            (outcome, None)
        };

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

async fn apply_record_to_chunks(
    blob_store: &dyn BlobStore,
    snapshot: &CowTreeSnapshot,
    chunk_images: &mut BTreeMap<ChunkIndex, Vec<u8>>,
    record: &crate::WalRecord,
) -> Result<()> {
    validate_record_range(snapshot, record)?;
    let payload = record.data();
    let mut copied = 0usize;
    while copied < payload.len() {
        let current_offset = record.range().start() + copied as u64;
        let chunk_index = ChunkIndex::new(current_offset / TREE_CHUNK_BYTES);
        let chunk_offset = (current_offset % TREE_CHUNK_BYTES) as usize;
        let chunk_available = TREE_CHUNK_BYTES as usize - chunk_offset;
        let copy_len = chunk_available.min(payload.len() - copied);
        let chunk = load_or_create_chunk(blob_store, snapshot, chunk_images, chunk_index).await?;
        chunk[chunk_offset..chunk_offset + copy_len]
            .copy_from_slice(&payload[copied..copied + copy_len]);
        copied += copy_len;
    }
    Ok(())
}

async fn load_or_create_chunk<'a>(
    blob_store: &dyn BlobStore,
    snapshot: &CowTreeSnapshot,
    chunk_images: &'a mut BTreeMap<ChunkIndex, Vec<u8>>,
    chunk_index: ChunkIndex,
) -> Result<&'a mut Vec<u8>> {
    match chunk_images.entry(chunk_index) {
        Entry::Occupied(entry) => Ok(entry.into_mut()),
        Entry::Vacant(entry) => {
            let data = match snapshot.chunk(chunk_index) {
                Some(chunk) => {
                    blob_store
                        .get_blob(chunk.blob_key(), 0, TREE_CHUNK_BYTES)
                        .await?
                }
                None => vec![0; TREE_CHUNK_BYTES as usize],
            };
            Ok(entry.insert(data))
        }
    }
}

fn validate_record_range(snapshot: &CowTreeSnapshot, record: &crate::WalRecord) -> Result<()> {
    let end = record
        .range()
        .start()
        .checked_add(record.range().len())
        .ok_or(ServerError::OutOfBounds {
            operation: "compact WAL record",
            offset: record.range().start(),
            length: record.range().len(),
            size_bytes: snapshot.size_bytes(),
        })?;
    if end > snapshot.size_bytes() {
        return Err(ServerError::OutOfBounds {
            operation: "compact WAL record",
            offset: record.range().start(),
            length: record.range().len(),
            size_bytes: snapshot.size_bytes(),
        });
    }
    Ok(())
}

async fn apply_overlay_slice_to_chunks(
    blob_store: &dyn BlobStore,
    tree_store: Option<Arc<dyn TreeRecordStore>>,
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
    tree_store: Option<Arc<dyn TreeRecordStore>>,
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
    tree_store: Option<Arc<dyn TreeRecordStore>>,
    root: &RootSnapshot,
    chunk_index: ChunkIndex,
) -> Result<Vec<u8>> {
    if let Some(chunk) = root
        .cow_tree()
        .and_then(|snapshot| snapshot.chunk(chunk_index))
    {
        return blob_store
            .get_blob(chunk.blob_key(), 0, TREE_CHUNK_BYTES)
            .await;
    }

    let Some(tree_store) = tree_store else {
        return Ok(vec![0; TREE_CHUNK_BYTES as usize]);
    };
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

fn snapshot_to_export_head(snapshot: &CowTreeSnapshot) -> Result<ExportHead> {
    ExportHead::new(
        ExportLayoutKind::CowImmutableTree,
        snapshot.root_node_id().cloned(),
        snapshot.size_bytes(),
        snapshot.base_wal_seq(),
    )
    .map_err(ServerError::catalog)
}

#[cfg(test)]
mod tests {
    use super::super::read_view::{CowTreeReader, ExportReadView};
    use super::*;
    use crate::range::ByteRange;
    use crate::storage::{LocalBlobStore, put_random_blob};
    use crate::wal::WalRecord;
    use nbd_control_plane::{
        CatalogUrl, CreateExport, ExportCatalog, ExportEngineKind, ExportName, SQLiteExportCatalog,
    };
    use nbd_test_support::TestRuntime;

    const MIGRATIONS: &[&str] = &[
        include_str!("../../../../../prisma/migrations/20260506000000_baseline/migration.sql"),
        include_str!("../../../../../prisma/migrations/20260512000000_tree_format/migration.sql"),
    ];

    #[tokio::test]
    async fn compact_snapshot_uses_latest_visible_hot_write() {
        let fixture = SnapshotCompactionFixture::new().await;
        let created = fixture
            .create_wal_export("snapshot-hot-write", TREE_CHUNK_BYTES)
            .await;
        let read_view = fixture.open_read_view(created.id()).await;
        read_view
            .apply_wal_record(wal_record(1, 0, b"aaaa"))
            .await
            .expect("apply first write");
        read_view
            .apply_wal_record(wal_record(2, 0, b"bbbb"))
            .await
            .expect("apply second write");
        let snapshot = read_view
            .capture_compaction_snapshot()
            .await
            .expect("capture snapshot")
            .expect("snapshot present");

        let result = fixture
            .compactor
            .compact_snapshot(created.id(), &snapshot)
            .await
            .expect("compact snapshot");

        assert_eq!(result.outcome(), CompactionOutcome::Published);
        assert_eq!(result.target_wal_seq(), WalSeq::new(2));
        assert_eq!(result.compacted_records(), 1);
        assert_eq!(result.written_leaf_blobs(), 1);
        let snapshot = fixture
            .catalog
            .load_cow_tree(created.id())
            .await
            .expect("load compacted tree");
        let chunk = snapshot.chunk(ChunkIndex::new(0)).expect("chunk zero");
        assert_eq!(
            fixture
                .blob_store
                .get_blob(chunk.blob_key(), 0, 4)
                .await
                .expect("read compacted blob"),
            b"bbbb",
        );
    }

    #[tokio::test]
    async fn compact_snapshot_groups_visible_extents_by_chunk() {
        let fixture = SnapshotCompactionFixture::new().await;
        let created = fixture
            .create_wal_export("snapshot-one-chunk", TREE_CHUNK_BYTES)
            .await;
        let read_view = fixture.open_read_view(created.id()).await;
        read_view
            .apply_wal_record(wal_record(1, 0, b"aa"))
            .await
            .expect("apply first write");
        read_view
            .apply_wal_record(wal_record(2, 4, b"bb"))
            .await
            .expect("apply second write");
        let snapshot = read_view
            .capture_compaction_snapshot()
            .await
            .expect("capture snapshot")
            .expect("snapshot present");

        let result = fixture
            .compactor
            .compact_snapshot(created.id(), &snapshot)
            .await
            .expect("compact snapshot");

        assert_eq!(result.outcome(), CompactionOutcome::Published);
        assert_eq!(result.compacted_records(), 2);
        assert_eq!(result.written_leaf_blobs(), 1);
        let snapshot = fixture
            .catalog
            .load_cow_tree(created.id())
            .await
            .expect("load compacted tree");
        let chunk = snapshot.chunk(ChunkIndex::new(0)).expect("chunk zero");
        assert_eq!(
            fixture
                .blob_store
                .get_blob(chunk.blob_key(), 0, 6)
                .await
                .expect("read compacted blob"),
            b"aa\0\0bb",
        );
    }

    #[tokio::test]
    async fn compact_snapshot_applies_overlay_to_committed_chunk() {
        let fixture = SnapshotCompactionFixture::new().await;
        let created = fixture
            .create_wal_export("snapshot-committed-base", TREE_CHUNK_BYTES)
            .await;
        fixture
            .publish_base_chunk(created.id(), b"base", WalSeq::new(1))
            .await;
        let read_view = fixture.open_read_view(created.id()).await;
        read_view
            .apply_wal_record(wal_record(2, 2, b"ZZ"))
            .await
            .expect("apply overlay write");
        let snapshot = read_view
            .capture_compaction_snapshot()
            .await
            .expect("capture snapshot")
            .expect("snapshot present");

        let result = fixture
            .compactor
            .compact_snapshot(created.id(), &snapshot)
            .await
            .expect("compact snapshot");

        assert_eq!(result.outcome(), CompactionOutcome::Published);
        assert_eq!(result.target_wal_seq(), WalSeq::new(2));
        assert_eq!(result.written_leaf_blobs(), 1);
        let snapshot = fixture
            .catalog
            .load_cow_tree(created.id())
            .await
            .expect("load compacted tree");
        let chunk = snapshot.chunk(ChunkIndex::new(0)).expect("chunk zero");
        assert_eq!(
            fixture
                .blob_store
                .get_blob(chunk.blob_key(), 0, 4)
                .await
                .expect("read compacted blob"),
            b"baZZ",
        );
    }

    struct SnapshotCompactionFixture {
        _runtime: TestRuntime,
        catalog: SQLiteExportCatalog,
        blob_store: Arc<LocalBlobStore>,
        compactor: CowCompactor,
    }

    impl SnapshotCompactionFixture {
        async fn new() -> Self {
            let runtime = TestRuntime::new().expect("test runtime");
            let catalog = migrated_catalog(&runtime).await;
            let blob_store = Arc::new(LocalBlobStore::new(runtime.root_path().join("blobs")));
            let compactor_blob_store: BlobStoreHandle = blob_store.clone();
            let compactor = CowCompactor::new(
                Arc::new(catalog.clone()) as Arc<dyn CowTreeMetadataStore>,
                compactor_blob_store,
            );

            Self {
                _runtime: runtime,
                catalog,
                blob_store,
                compactor,
            }
        }

        async fn create_wal_export(
            &self,
            name: &str,
            size_bytes: u64,
        ) -> nbd_control_plane::ExportRecord {
            self.catalog
                .create_export(
                    CreateExport::new(
                        ExportName::new(name).expect("export name"),
                        size_bytes,
                        4096,
                        ExportEngineKind::WalDurable,
                    )
                    .expect("create export"),
                )
                .await
                .expect("create wal export")
        }

        async fn open_read_view(&self, export_id: &ExportId) -> ExportReadView {
            let snapshot = self
                .catalog
                .load_cow_tree(export_id)
                .await
                .expect("load cow tree");
            ExportReadView::new(
                RootSnapshot::from_cow_snapshot(snapshot),
                Arc::new(CowTreeReader {
                    blob_store: self.blob_store.clone(),
                    store: Arc::new(self.catalog.clone()) as Arc<dyn TreeRecordStore>,
                }),
            )
        }

        async fn publish_base_chunk(
            &self,
            export_id: &ExportId,
            initial_bytes: &[u8],
            compacted_through: WalSeq,
        ) {
            let base = self
                .catalog
                .load_cow_tree(export_id)
                .await
                .expect("load base tree");
            let mut data = vec![0; TREE_CHUNK_BYTES as usize];
            data[..initial_bytes.len()].copy_from_slice(initial_bytes);
            let key = put_random_blob(self.blob_store.as_ref(), &data)
                .await
                .expect("write base chunk");
            let chunk =
                CowChunkRef::new(ChunkIndex::new(0), key, TREE_CHUNK_BYTES).expect("base chunk");
            self.catalog
                .publish_compaction(
                    PublishCompaction::new(
                        export_id.clone(),
                        snapshot_to_export_head(&base).expect("base head"),
                        compacted_through,
                        vec![chunk],
                    )
                    .expect("base publication"),
                )
                .await
                .expect("publish base chunk");
        }
    }

    async fn migrated_catalog(runtime: &TestRuntime) -> SQLiteExportCatalog {
        let url = CatalogUrl::parse(runtime.catalog_url()).expect("catalog URL");
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

    fn wal_record(seq: u64, offset: u64, data: &[u8]) -> WalRecord {
        WalRecord::new(
            WalSeq::new(seq),
            ByteRange::new(offset, data.len() as u32),
            data.to_vec(),
        )
        .expect("WAL record")
    }
}
