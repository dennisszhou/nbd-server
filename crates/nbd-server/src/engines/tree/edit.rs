use super::{TreeGeometry, TreeNodeSpan};
use nbd_control_plane::{
    BlobKey, ExportId, ExportLayoutKind, NodeId, TreeEdgeRecord, TreeLeafRefRecord, TreeNodeKind,
    TreeNodeRecord, TreeStorageKind,
};

#[derive(Debug, Clone)]
pub(crate) struct TreeRecordFactory {
    geometry: TreeGeometry,
    layout_kind: ExportLayoutKind,
    owner_export_id: Option<ExportId>,
}

impl TreeRecordFactory {
    pub(crate) fn new(
        geometry: TreeGeometry,
        layout_kind: ExportLayoutKind,
        owner_export_id: Option<ExportId>,
    ) -> Self {
        Self {
            geometry,
            layout_kind,
            owner_export_id,
        }
    }

    pub(crate) fn root_node(&self, id: NodeId) -> TreeNodeRecord {
        self.internal_node(id, self.geometry.root_span())
    }

    pub(crate) fn internal_node(&self, id: NodeId, span: TreeNodeSpan) -> TreeNodeRecord {
        TreeNodeRecord {
            id,
            layout_kind: self.layout_kind,
            owner_export_id: self.owner_export_id.clone(),
            kind: TreeNodeKind::Internal,
            level: span.level(),
            span_start_bytes: span.start_bytes(),
            span_len_bytes: span.len_bytes(),
        }
    }

    pub(crate) fn leaf_node(&self, id: NodeId, span: TreeNodeSpan) -> TreeNodeRecord {
        TreeNodeRecord {
            id,
            layout_kind: self.layout_kind,
            owner_export_id: self.owner_export_id.clone(),
            kind: TreeNodeKind::Leaf,
            level: span.level(),
            span_start_bytes: span.start_bytes(),
            span_len_bytes: span.len_bytes(),
        }
    }

    pub(crate) fn child_edge(
        &self,
        parent_node_id: NodeId,
        slot: u16,
        child_node_id: NodeId,
    ) -> TreeEdgeRecord {
        TreeEdgeRecord {
            parent_node_id,
            slot,
            child_node_id,
        }
    }

    pub(crate) fn leaf_ref(
        &self,
        node_id: NodeId,
        storage_kind: TreeStorageKind,
        storage_key: BlobKey,
    ) -> TreeLeafRefRecord {
        TreeLeafRefRecord {
            node_id,
            storage_kind,
            storage_key,
            len_bytes: self.geometry.chunk_bytes(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nbd_control_plane::{ChunkIndex, TREE_CHUNK_BYTES, TreeFormat};

    #[test]
    fn record_factory_uses_geometry_spans() {
        let geometry = TreeGeometry::new(TreeFormat::Bounded32V1, TREE_CHUNK_BYTES * 64).unwrap();
        let export_id = ExportId::new("export-a").unwrap();
        let factory = TreeRecordFactory::new(
            geometry,
            ExportLayoutKind::SimpleMutableTree,
            Some(export_id.clone()),
        );

        let root = factory.root_node(NodeId::new("root").unwrap());
        assert_eq!(root.kind, TreeNodeKind::Internal);
        assert_eq!(root.level, 2);
        assert_eq!(root.span_start_bytes, 0);
        assert_eq!(root.span_len_bytes, TREE_CHUNK_BYTES * 64);
        assert_eq!(root.owner_export_id, Some(export_id));

        let path = geometry.path_for_chunk(ChunkIndex::new(63)).unwrap();
        assert_eq!(path.slots(), &[1, 31]);
        let leaf_span = geometry
            .child_span(
                geometry
                    .child_span(geometry.root_span(), path.slots()[0])
                    .unwrap(),
                path.slots()[1],
            )
            .unwrap();
        let leaf = factory.leaf_node(NodeId::new("leaf").unwrap(), leaf_span);
        assert_eq!(leaf.kind, TreeNodeKind::Leaf);
        assert_eq!(leaf.level, 0);
        assert_eq!(leaf.span_start_bytes, TREE_CHUNK_BYTES * 63);
        assert_eq!(leaf.span_len_bytes, TREE_CHUNK_BYTES);
    }
}
