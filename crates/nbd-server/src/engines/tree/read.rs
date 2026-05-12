use crate::error::{Result, ServerError};
use crate::range::ByteRange;
use bytes::Bytes;
use nbd_control_plane::{
    ChunkIndex, ExportLayoutKind, NodeId, TreeEdgeLookup, TreeLeafRefRecord, TreeNodeKind,
    TreeNodeRecord, TreeRecordStore,
};
use std::fmt;
use std::sync::Arc;

use super::TreeGeometry;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Block {
    range: ByteRange,
    parts: Vec<BlockPart>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum BlockPart {
    Data { range: ByteRange, bytes: Bytes },
    Zero { range: ByteRange },
}

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LoadedTreeLeaf {
    chunk_index: ChunkIndex,
    node: TreeNodeRecord,
    leaf_ref: TreeLeafRefRecord,
}

#[allow(dead_code)]
#[derive(Clone)]
pub(crate) struct LazyTreeMetadataReader {
    store: Arc<dyn TreeRecordStore>,
    geometry: TreeGeometry,
    layout_kind: ExportLayoutKind,
    root_node_id: Option<NodeId>,
}

#[async_trait::async_trait]
pub(crate) trait TreeReader<R>: fmt::Debug + Send + Sync {
    async fn read_committed(&self, root: &R, range: ByteRange) -> Result<Block>;
}

impl Block {
    pub(crate) fn new(range: ByteRange, parts: Vec<BlockPart>) -> Result<Self> {
        validate_block_parts(range, &parts)?;
        Ok(Self { range, parts })
    }

    pub(crate) fn range(&self) -> ByteRange {
        self.range
    }

    pub(crate) fn parts(&self) -> &[BlockPart] {
        &self.parts
    }

    pub(crate) fn materialize(&self) -> Result<Vec<u8>> {
        let len = usize::try_from(self.range.len())
            .map_err(|_| invalid_block("read range length does not fit usize"))?;
        let mut data = Vec::with_capacity(len);
        for part in self.parts() {
            match part {
                BlockPart::Data { bytes, .. } => data.extend_from_slice(bytes),
                BlockPart::Zero { range } => {
                    let part_len = usize::try_from(range.len())
                        .map_err(|_| invalid_block("zero range length does not fit usize"))?;
                    data.resize(data.len() + part_len, 0);
                }
            }
        }
        Ok(data)
    }
}

impl BlockPart {
    pub(crate) fn range(&self) -> ByteRange {
        match self {
            Self::Data { range, .. } | Self::Zero { range } => *range,
        }
    }
}

#[allow(dead_code)]
impl LoadedTreeLeaf {
    pub(crate) fn chunk_index(&self) -> ChunkIndex {
        self.chunk_index
    }

    pub(crate) fn node(&self) -> &TreeNodeRecord {
        &self.node
    }

    pub(crate) fn leaf_ref(&self) -> &TreeLeafRefRecord {
        &self.leaf_ref
    }
}

#[allow(dead_code)]
impl LazyTreeMetadataReader {
    pub(crate) fn new(
        store: Arc<dyn TreeRecordStore>,
        geometry: TreeGeometry,
        layout_kind: ExportLayoutKind,
        root_node_id: Option<NodeId>,
    ) -> Self {
        Self {
            store,
            geometry,
            layout_kind,
            root_node_id,
        }
    }

    pub(crate) fn geometry(&self) -> TreeGeometry {
        self.geometry
    }

    pub(crate) fn root_node_id(&self) -> Option<&NodeId> {
        self.root_node_id.as_ref()
    }

    pub(crate) async fn load_leaf(
        &self,
        chunk_index: ChunkIndex,
    ) -> Result<Option<LoadedTreeLeaf>> {
        let Some(root_node_id) = &self.root_node_id else {
            return Ok(None);
        };
        let path = self.geometry.path_for_chunk(chunk_index)?;
        let mut span = self.geometry.root_span();
        let mut current = self.load_required_node(root_node_id).await?;
        self.validate_node(&current, span, TreeNodeKind::Internal)?;

        for slot in path.slots() {
            let edge = self.load_child_edge(&current.id, *slot).await?;
            let Some(edge) = edge else {
                return Ok(None);
            };
            span = self.geometry.child_span(span, *slot)?;
            current = self.load_required_node(&edge.child_node_id).await?;
            let expected_kind = if span.level() == 0 {
                TreeNodeKind::Leaf
            } else {
                TreeNodeKind::Internal
            };
            self.validate_node(&current, span, expected_kind)?;
        }

        let leaf_ref = self.load_required_leaf_ref(&current.id).await?;
        Ok(Some(LoadedTreeLeaf {
            chunk_index,
            node: current,
            leaf_ref,
        }))
    }

    pub(crate) async fn load_leaves_for_range(
        &self,
        range: ByteRange,
    ) -> Result<Vec<LoadedTreeLeaf>> {
        let mut leaves = Vec::new();
        for chunk in self.geometry.chunks_for_range(range)? {
            if let Some(leaf) = self.load_leaf(chunk.chunk_index()).await? {
                leaves.push(leaf);
            }
        }
        Ok(leaves)
    }

    async fn load_required_node(&self, node_id: &NodeId) -> Result<TreeNodeRecord> {
        self.store
            .load_node(node_id)
            .await
            .map_err(ServerError::catalog)?
            .ok_or_else(|| invalid_tree(format!("tree node `{node_id}` is missing")))
    }

    async fn load_child_edge(
        &self,
        parent_node_id: &NodeId,
        slot: u16,
    ) -> Result<Option<nbd_control_plane::TreeEdgeRecord>> {
        let edges = self
            .store
            .load_child_edges(&[TreeEdgeLookup {
                parent_node_id: parent_node_id.clone(),
                slots: vec![slot],
            }])
            .await
            .map_err(ServerError::catalog)?;
        if edges.len() > 1 {
            return Err(invalid_tree(format!(
                "tree node `{parent_node_id}` returned duplicate edge for slot {slot}"
            )));
        }
        let edge = edges.into_iter().next();
        if let Some(edge) = &edge {
            if &edge.parent_node_id != parent_node_id || edge.slot != slot {
                return Err(invalid_tree(format!(
                    "tree edge for `{parent_node_id}` slot {slot} returned mismatched row"
                )));
            }
        }
        Ok(edge)
    }

    async fn load_required_leaf_ref(&self, node_id: &NodeId) -> Result<TreeLeafRefRecord> {
        let leaf_refs = self
            .store
            .load_leaf_refs(std::slice::from_ref(node_id))
            .await
            .map_err(ServerError::catalog)?;
        if leaf_refs.len() != 1 {
            return Err(invalid_tree(format!(
                "leaf node `{node_id}` has {} leaf refs",
                leaf_refs.len()
            )));
        }
        let leaf_ref = leaf_refs.into_iter().next().expect("one leaf ref");
        if &leaf_ref.node_id != node_id {
            return Err(invalid_tree(format!(
                "leaf ref for `{node_id}` returned mismatched row"
            )));
        }
        Ok(leaf_ref)
    }

    fn validate_node(
        &self,
        node: &TreeNodeRecord,
        span: super::TreeNodeSpan,
        expected_kind: TreeNodeKind,
    ) -> Result<()> {
        if node.layout_kind != self.layout_kind {
            return Err(invalid_tree(format!(
                "tree node `{}` has layout {}, expected {}",
                node.id, node.layout_kind, self.layout_kind
            )));
        }
        if node.kind != expected_kind {
            return Err(invalid_tree(format!(
                "tree node `{}` has kind {}, expected {}",
                node.id, node.kind, expected_kind
            )));
        }
        if node.level != span.level()
            || node.span_start_bytes != span.start_bytes()
            || node.span_len_bytes != span.len_bytes()
        {
            return Err(invalid_tree(format!(
                "tree node `{}` span does not match bounded geometry",
                node.id
            )));
        }
        Ok(())
    }
}

impl fmt::Debug for LazyTreeMetadataReader {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LazyTreeMetadataReader")
            .field("geometry", &self.geometry)
            .field("layout_kind", &self.layout_kind)
            .field("root_node_id", &self.root_node_id)
            .finish_non_exhaustive()
    }
}

fn validate_block_parts(range: ByteRange, parts: &[BlockPart]) -> Result<()> {
    let mut expected_start = range.start();
    let read_end = checked_range_end(range)?;

    for part in parts {
        let part_range = part.range();
        if part_range.start() != expected_start {
            return Err(invalid_block(format!(
                "part starts at {}, expected {}",
                part_range.start(),
                expected_start
            )));
        }
        if let BlockPart::Data { bytes, range } = part {
            if bytes.len() as u64 != range.len() {
                return Err(invalid_block(format!(
                    "data part has {} bytes for {} byte range",
                    bytes.len(),
                    range.len()
                )));
            }
        }
        expected_start = checked_range_end(part_range)?;
        if expected_start > read_end {
            return Err(invalid_block("parts exceed read range"));
        }
    }

    if expected_start != read_end {
        return Err(invalid_block(format!(
            "parts end at {}, expected {}",
            expected_start, read_end
        )));
    }

    Ok(())
}

fn checked_range_end(range: ByteRange) -> Result<u64> {
    range
        .start()
        .checked_add(range.len())
        .ok_or_else(|| invalid_block("range end overflowed"))
}

fn invalid_block(message: impl Into<String>) -> ServerError {
    ServerError::Io {
        context: "block read",
        message: message.into(),
        source: None,
    }
}

#[allow(dead_code)]
fn invalid_tree(message: impl Into<String>) -> ServerError {
    ServerError::Catalog {
        message: message.into(),
        source: None,
    }
}

#[cfg(test)]
mod lazy_tests {
    use super::super::edit::TreeRecordFactory;
    use super::*;
    use nbd_control_plane::{
        BlobKey, CatalogError, ExportId, PublishTreeUpdate, PublishTreeUpdateOutcome,
        TREE_CHUNK_BYTES, TreeEdgeRecord, TreeFormat, TreeStorageKind,
    };
    use std::collections::HashMap;
    use std::sync::Mutex;

    #[derive(Debug, Clone, PartialEq, Eq)]
    enum StoreCall {
        LoadNode(String),
        LoadChildEdges { parent: String, slots: Vec<u16> },
        LoadLeafRefs(Vec<String>),
    }

    #[derive(Debug, Default)]
    struct FakeTreeRecordStore {
        nodes: Mutex<HashMap<String, TreeNodeRecord>>,
        edges: Mutex<HashMap<(String, u16), TreeEdgeRecord>>,
        leaf_refs: Mutex<HashMap<String, TreeLeafRefRecord>>,
        calls: Mutex<Vec<StoreCall>>,
    }

    impl FakeTreeRecordStore {
        fn insert_node(&self, node: TreeNodeRecord) {
            self.nodes
                .lock()
                .unwrap()
                .insert(node.id.as_str().to_owned(), node);
        }

        fn insert_edge(&self, edge: TreeEdgeRecord) {
            self.edges
                .lock()
                .unwrap()
                .insert((edge.parent_node_id.as_str().to_owned(), edge.slot), edge);
        }

        fn insert_leaf_ref(&self, leaf_ref: TreeLeafRefRecord) {
            self.leaf_refs
                .lock()
                .unwrap()
                .insert(leaf_ref.node_id.as_str().to_owned(), leaf_ref);
        }

        fn calls(&self) -> Vec<StoreCall> {
            self.calls.lock().unwrap().clone()
        }
    }

    #[async_trait::async_trait]
    impl TreeRecordStore for FakeTreeRecordStore {
        async fn load_node(
            &self,
            node_id: &NodeId,
        ) -> nbd_control_plane::Result<Option<TreeNodeRecord>> {
            self.calls
                .lock()
                .unwrap()
                .push(StoreCall::LoadNode(node_id.as_str().to_owned()));
            Ok(self.nodes.lock().unwrap().get(node_id.as_str()).cloned())
        }

        async fn load_nodes(
            &self,
            node_ids: &[NodeId],
        ) -> nbd_control_plane::Result<Vec<TreeNodeRecord>> {
            let mut nodes = Vec::new();
            for node_id in node_ids {
                if let Some(node) = self.load_node(node_id).await? {
                    nodes.push(node);
                }
            }
            Ok(nodes)
        }

        async fn load_child_edges(
            &self,
            lookups: &[TreeEdgeLookup],
        ) -> nbd_control_plane::Result<Vec<TreeEdgeRecord>> {
            let mut edges = Vec::new();
            for lookup in lookups {
                self.calls.lock().unwrap().push(StoreCall::LoadChildEdges {
                    parent: lookup.parent_node_id.as_str().to_owned(),
                    slots: lookup.slots.clone(),
                });
                for slot in &lookup.slots {
                    if let Some(edge) = self
                        .edges
                        .lock()
                        .unwrap()
                        .get(&(lookup.parent_node_id.as_str().to_owned(), *slot))
                        .cloned()
                    {
                        edges.push(edge);
                    }
                }
            }
            Ok(edges)
        }

        async fn load_leaf_refs(
            &self,
            node_ids: &[NodeId],
        ) -> nbd_control_plane::Result<Vec<TreeLeafRefRecord>> {
            self.calls.lock().unwrap().push(StoreCall::LoadLeafRefs(
                node_ids
                    .iter()
                    .map(|node_id| node_id.as_str().to_owned())
                    .collect(),
            ));
            Ok(node_ids
                .iter()
                .filter_map(|node_id| {
                    self.leaf_refs
                        .lock()
                        .unwrap()
                        .get(node_id.as_str())
                        .cloned()
                })
                .collect())
        }

        async fn publish_tree_update(
            &self,
            _request: PublishTreeUpdate,
        ) -> nbd_control_plane::Result<PublishTreeUpdateOutcome> {
            Err(CatalogError::database("fake store does not publish"))
        }
    }

    #[tokio::test]
    async fn lazy_reader_loads_only_path_edges_for_sparse_large_tree() {
        let geometry =
            TreeGeometry::new(TreeFormat::Bounded32V1, 1024 * 1024 * 1024 * 1024).unwrap();
        let export_id = ExportId::new("export-a").unwrap();
        let factory = TreeRecordFactory::new(
            geometry,
            ExportLayoutKind::CowImmutableTree,
            Some(export_id),
        );
        let root = NodeId::new("root").unwrap();
        let internal_l2 = NodeId::new("internal-l2").unwrap();
        let internal_l1 = NodeId::new("internal-l1").unwrap();
        let leaf = NodeId::new("leaf").unwrap();
        let path = geometry
            .path_for_chunk(ChunkIndex::new(32 * 32 * 32 - 1))
            .unwrap();
        let l2_span = geometry
            .child_span(geometry.root_span(), path.slots()[0])
            .unwrap();
        let l1_span = geometry.child_span(l2_span, path.slots()[1]).unwrap();
        let leaf_span = geometry.child_span(l1_span, path.slots()[2]).unwrap();

        let store = Arc::new(FakeTreeRecordStore::default());
        store.insert_node(factory.root_node(root.clone()));
        store.insert_node(factory.internal_node(internal_l2.clone(), l2_span));
        store.insert_node(factory.internal_node(internal_l1.clone(), l1_span));
        store.insert_node(factory.leaf_node(leaf.clone(), leaf_span));
        store.insert_edge(factory.child_edge(root.clone(), path.slots()[0], internal_l2.clone()));
        store.insert_edge(factory.child_edge(
            internal_l2.clone(),
            path.slots()[1],
            internal_l1.clone(),
        ));
        store.insert_edge(factory.child_edge(internal_l1.clone(), path.slots()[2], leaf.clone()));
        store.insert_leaf_ref(factory.leaf_ref(
            leaf.clone(),
            TreeStorageKind::ImmutableBlob,
            BlobKey::new("leaf-blob").unwrap(),
        ));
        let store_handle: Arc<dyn TreeRecordStore> = store.clone();
        let reader = LazyTreeMetadataReader::new(
            store_handle,
            geometry,
            ExportLayoutKind::CowImmutableTree,
            Some(root.clone()),
        );

        let leaves = reader
            .load_leaves_for_range(ByteRange::new((32 * 32 * 32 - 1) * TREE_CHUNK_BYTES, 4096))
            .await
            .unwrap();
        assert_eq!(leaves.len(), 1);
        let loaded = leaves.first().expect("leaf");

        assert_eq!(loaded.chunk_index(), ChunkIndex::new(32 * 32 * 32 - 1));
        assert_eq!(loaded.node().id, leaf);
        assert_eq!(loaded.leaf_ref().storage_key.as_str(), "leaf-blob");
        assert_eq!(
            store.calls(),
            vec![
                StoreCall::LoadNode("root".to_owned()),
                StoreCall::LoadChildEdges {
                    parent: "root".to_owned(),
                    slots: vec![31],
                },
                StoreCall::LoadNode("internal-l2".to_owned()),
                StoreCall::LoadChildEdges {
                    parent: "internal-l2".to_owned(),
                    slots: vec![31],
                },
                StoreCall::LoadNode("internal-l1".to_owned()),
                StoreCall::LoadChildEdges {
                    parent: "internal-l1".to_owned(),
                    slots: vec![31],
                },
                StoreCall::LoadNode("leaf".to_owned()),
                StoreCall::LoadLeafRefs(vec!["leaf".to_owned()]),
            ]
        );
    }

    #[tokio::test]
    async fn lazy_reader_treats_missing_edges_as_sparse_zero() {
        let geometry = TreeGeometry::new(TreeFormat::Bounded32V1, TREE_CHUNK_BYTES * 64).unwrap();
        let factory = TreeRecordFactory::new(geometry, ExportLayoutKind::SimpleMutableTree, None);
        let root = NodeId::new("root").unwrap();
        let store = Arc::new(FakeTreeRecordStore::default());
        store.insert_node(factory.root_node(root.clone()));
        let store_handle: Arc<dyn TreeRecordStore> = store.clone();
        let reader = LazyTreeMetadataReader::new(
            store_handle,
            geometry,
            ExportLayoutKind::SimpleMutableTree,
            Some(root),
        );

        assert!(
            reader
                .load_leaf(ChunkIndex::new(63))
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(
            store.calls(),
            vec![
                StoreCall::LoadNode("root".to_owned()),
                StoreCall::LoadChildEdges {
                    parent: "root".to_owned(),
                    slots: vec![1],
                },
            ]
        );
    }

    #[test]
    fn zero_root_metadata_keeps_empty_tree_sparse() {
        let geometry = TreeGeometry::new(TreeFormat::Bounded32V1, TREE_CHUNK_BYTES).unwrap();
        let store = Arc::new(FakeTreeRecordStore::default());
        let store_handle: Arc<dyn TreeRecordStore> = store;
        let reader = LazyTreeMetadataReader::new(
            store_handle,
            geometry,
            ExportLayoutKind::CowImmutableTree,
            None,
        );

        assert!(reader.root_node_id().is_none());
        assert_eq!(reader.geometry().format(), TreeFormat::Bounded32V1);
    }
}
