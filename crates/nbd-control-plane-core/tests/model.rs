use nbd_control_plane_core::{
    ActiveExportDescriptor, BlobKey, ChunkIndex, CloneExport, CowChunkRef, CowTreeSnapshot,
    CreateExport, ExportDescriptor, ExportEngineKind, ExportHead, ExportId, ExportLayoutKind,
    ExportName, ExportState, ListExports, NodeId, PublishCompaction, SIMPLE_CHUNK_BYTES,
    SimpleChunkRef, TREE_CHUNK_BYTES, Timestamp, WalSeq,
};
use std::collections::BTreeMap;
use std::str::FromStr;

#[test]
fn create_export_validates_basic_domain_values() {
    let name = ExportName::new("disk-a").expect("valid name");
    let request = CreateExport::new(name, 1024 * 1024, 4096, ExportEngineKind::Memory)
        .expect("valid request");

    assert_eq!(request.name().as_str(), "disk-a");
    assert_eq!(request.size_bytes(), 1024 * 1024);
    assert_eq!(request.block_size(), 4096);
    assert_eq!(request.engine_kind(), ExportEngineKind::Memory);
    assert!(
        CreateExport::new(
            ExportName::new("disk-b").unwrap(),
            0,
            4096,
            ExportEngineKind::Memory,
        )
        .is_err()
    );
    assert!(
        CreateExport::new(
            ExportName::new("disk-c").unwrap(),
            4096,
            0,
            ExportEngineKind::Memory,
        )
        .is_err()
    );
}

#[test]
fn clone_export_validates_distinct_names() {
    let request = CloneExport::new(
        ExportName::new("source").expect("source name"),
        ExportName::new("destination").expect("destination name"),
    )
    .expect("valid clone request");

    assert_eq!(request.source().as_str(), "source");
    assert_eq!(request.destination().as_str(), "destination");
    assert!(
        CloneExport::new(
            ExportName::new("same").expect("source name"),
            ExportName::new("same").expect("destination name"),
        )
        .is_err()
    );
}

#[test]
fn active_export_descriptors_reject_deleted_exports() {
    let active = export_descriptor("disk-active", ExportState::Active, None);
    let active = ActiveExportDescriptor::new(active).expect("active descriptor");

    assert_eq!(active.name().as_str(), "disk-active");
    assert_eq!(active.state(), ExportState::Active);

    let deleted_at = Timestamp::new("unix_us:2").expect("deleted timestamp");
    let deleted = export_descriptor("disk-deleted", ExportState::Deleted, Some(deleted_at));
    assert!(ActiveExportDescriptor::new(deleted).is_err());
}

#[test]
fn export_names_must_not_be_empty_or_contain_nul() {
    assert!(ExportName::new("").is_err());
    assert!(ExportName::new("bad\0name").is_err());
}

#[test]
fn export_head_can_represent_empty_memory() {
    let head = ExportHead::memory_empty(4096).expect("memory head");

    assert_eq!(head.layout_kind(), ExportLayoutKind::MemoryEmpty);
    assert!(head.root_node_id().is_none());
    assert_eq!(head.size_bytes(), 4096);
    assert_eq!(head.base_wal_seq(), WalSeq::zero());
    assert!(ExportHead::memory_empty(0).is_err());
    assert!(
        ExportHead::new(
            ExportLayoutKind::MemoryEmpty,
            Some(NodeId::new("root").expect("node id")),
            4096,
            WalSeq::zero(),
        )
        .is_err()
    );
    assert!(ExportHead::new(ExportLayoutKind::MemoryEmpty, None, 4096, WalSeq::new(1),).is_err());
}

#[test]
fn export_head_can_represent_simple_mutable_tree() {
    let head = ExportHead::simple_mutable_tree(4096).expect("simple tree head");

    assert_eq!(head.layout_kind(), ExportLayoutKind::SimpleMutableTree);
    assert!(head.root_node_id().is_none());
    assert_eq!(head.size_bytes(), 4096);
    assert_eq!(head.base_wal_seq(), WalSeq::zero());
    assert!(ExportHead::simple_mutable_tree(0).is_err());
    assert!(
        ExportHead::new(
            ExportLayoutKind::SimpleMutableTree,
            None,
            4096,
            WalSeq::new(1),
        )
        .is_err()
    );
}

#[test]
fn export_head_can_represent_empty_cow_tree() {
    let head = ExportHead::cow_immutable_tree(4096).expect("cow tree head");

    assert_eq!(head.layout_kind(), ExportLayoutKind::CowImmutableTree);
    assert!(head.root_node_id().is_none());
    assert_eq!(head.size_bytes(), 4096);
    assert_eq!(head.base_wal_seq(), WalSeq::zero());
    assert!(ExportHead::cow_immutable_tree(0).is_err());

    let root = NodeId::new("root").expect("node id");
    let head = ExportHead::new(
        ExportLayoutKind::CowImmutableTree,
        Some(root.clone()),
        4096,
        WalSeq::new(7),
    )
    .expect("cow tree head with base");
    assert_eq!(head.root_node_id(), Some(&root));
    assert_eq!(head.base_wal_seq(), WalSeq::new(7));
}

#[test]
fn blob_keys_are_safe_path_components() {
    let key = BlobKey::new("blob-123").expect("valid blob key");

    assert_eq!(key.as_str(), "blob-123");
    assert!(BlobKey::new("").is_err());
    assert!(BlobKey::new(".").is_err());
    assert!(BlobKey::new("..").is_err());
    assert!(BlobKey::new("dir/blob").is_err());
    assert!(BlobKey::new("dir\\blob").is_err());
    assert!(BlobKey::new("bad\0blob").is_err());
}

#[test]
fn simple_chunk_refs_are_full_sized_blob_refs() {
    let chunk = SimpleChunkRef::new(
        ChunkIndex::new(7),
        BlobKey::new("blob-7").expect("valid blob key"),
        SIMPLE_CHUNK_BYTES,
    )
    .expect("valid simple chunk");

    assert_eq!(chunk.chunk_index(), ChunkIndex::new(7));
    assert_eq!(chunk.blob_key().as_str(), "blob-7");
    assert_eq!(chunk.len_bytes(), SIMPLE_CHUNK_BYTES);
    assert!(
        SimpleChunkRef::new(
            ChunkIndex::new(7),
            BlobKey::new("blob-7").expect("valid blob key"),
            SIMPLE_CHUNK_BYTES - 1,
        )
        .is_err()
    );
}

#[test]
fn cow_chunk_refs_are_full_sized_blob_refs() {
    let chunk = CowChunkRef::new(
        ChunkIndex::new(3),
        BlobKey::new("blob-3").expect("valid blob key"),
        TREE_CHUNK_BYTES,
    )
    .expect("valid cow chunk");

    assert_eq!(chunk.chunk_index(), ChunkIndex::new(3));
    assert_eq!(chunk.blob_key().as_str(), "blob-3");
    assert_eq!(chunk.len_bytes(), TREE_CHUNK_BYTES);
    assert!(
        CowChunkRef::new(
            ChunkIndex::new(3),
            BlobKey::new("blob-3").expect("valid blob key"),
            TREE_CHUNK_BYTES - 1,
        )
        .is_err()
    );
}

#[test]
fn cow_tree_snapshots_validate_chunk_shape() {
    let export_id = ExportId::new("export-cow").expect("export id");
    let root = NodeId::new("root-cow").expect("root node");
    let chunk = CowChunkRef::new(
        ChunkIndex::new(0),
        BlobKey::new("blob-0").expect("valid blob key"),
        TREE_CHUNK_BYTES,
    )
    .expect("cow chunk");
    let mut chunks = BTreeMap::new();
    chunks.insert(chunk.chunk_index(), chunk);

    let snapshot = CowTreeSnapshot::new(
        export_id.clone(),
        TREE_CHUNK_BYTES,
        Some(root.clone()),
        WalSeq::new(4),
        chunks.clone(),
    )
    .expect("cow snapshot");
    assert_eq!(snapshot.export_id(), &export_id);
    assert_eq!(snapshot.root_node_id(), Some(&root));
    assert_eq!(snapshot.base_wal_seq(), WalSeq::new(4));
    assert_eq!(
        snapshot.chunk(ChunkIndex::new(0)).unwrap().len_bytes(),
        TREE_CHUNK_BYTES
    );

    assert!(
        CowTreeSnapshot::new(export_id, TREE_CHUNK_BYTES, None, WalSeq::new(4), chunks).is_err()
    );
}

#[test]
fn publish_compaction_validates_expected_base_and_chunks() {
    let export_id = ExportId::new("export-cow").expect("export id");
    let base = ExportHead::cow_immutable_tree(TREE_CHUNK_BYTES).expect("cow head");
    let chunk = CowChunkRef::new(
        ChunkIndex::new(0),
        BlobKey::new("blob-0").expect("valid blob key"),
        TREE_CHUNK_BYTES,
    )
    .expect("cow chunk");

    let request = PublishCompaction::new(
        export_id.clone(),
        base.clone(),
        WalSeq::new(1),
        vec![chunk.clone()],
    )
    .expect("publish request");
    assert_eq!(request.export_id(), &export_id);
    assert_eq!(request.expected_base(), &base);
    assert_eq!(request.compacted_through(), WalSeq::new(1));
    assert_eq!(request.chunks(), std::slice::from_ref(&chunk));

    assert!(
        PublishCompaction::new(export_id.clone(), base.clone(), WalSeq::zero(), vec![chunk])
            .is_err()
    );
    assert!(
        PublishCompaction::new(
            export_id,
            ExportHead::simple_mutable_tree(TREE_CHUNK_BYTES).expect("simple head"),
            WalSeq::new(1),
            vec![
                CowChunkRef::new(
                    ChunkIndex::new(0),
                    BlobKey::new("blob-1").expect("valid blob key"),
                    TREE_CHUNK_BYTES,
                )
                .expect("cow chunk")
            ],
        )
        .is_err()
    );
}

#[test]
fn export_state_round_trips_catalog_values() {
    assert_eq!(
        ExportState::from_str("active").unwrap(),
        ExportState::Active
    );
    assert_eq!(
        ExportState::from_str("deleted").unwrap(),
        ExportState::Deleted
    );
    assert_eq!(ExportState::Active.to_string(), "active");
    assert_eq!(ExportState::Deleted.to_string(), "deleted");
    assert!(ExportState::from_str("paused").is_err());
}

#[test]
fn export_engine_kind_round_trips_catalog_values() {
    assert_eq!(
        ExportEngineKind::from_str("memory").unwrap(),
        ExportEngineKind::Memory
    );
    assert_eq!(
        ExportEngineKind::from_str("simple_durable").unwrap(),
        ExportEngineKind::SimpleDurable,
    );
    assert_eq!(
        ExportEngineKind::from_str("wal_durable").unwrap(),
        ExportEngineKind::WalDurable,
    );
    assert_eq!(ExportEngineKind::Memory.to_string(), "memory");
    assert_eq!(
        ExportEngineKind::SimpleDurable.to_string(),
        "simple_durable",
    );
    assert_eq!(ExportEngineKind::WalDurable.to_string(), "wal_durable");
    assert!(ExportEngineKind::from_str("durable").is_err());
}

#[test]
fn export_layout_kind_round_trips_catalog_values() {
    assert_eq!(
        ExportLayoutKind::from_str("memory_empty").unwrap(),
        ExportLayoutKind::MemoryEmpty
    );
    assert_eq!(
        ExportLayoutKind::from_str("simple_mutable_tree").unwrap(),
        ExportLayoutKind::SimpleMutableTree
    );
    assert_eq!(
        ExportLayoutKind::from_str("cow_immutable_tree").unwrap(),
        ExportLayoutKind::CowImmutableTree
    );
    assert_eq!(ExportLayoutKind::MemoryEmpty.to_string(), "memory_empty");
    assert_eq!(
        ExportLayoutKind::SimpleMutableTree.to_string(),
        "simple_mutable_tree"
    );
    assert_eq!(
        ExportLayoutKind::CowImmutableTree.to_string(),
        "cow_immutable_tree"
    );
    assert!(ExportLayoutKind::from_str("generation").is_err());
}

#[test]
fn list_exports_defaults_to_active_only() {
    assert!(!ListExports::active_only().includes_deleted());
    assert!(ListExports::include_deleted().includes_deleted());
}

fn export_descriptor(
    name: &str,
    state: ExportState,
    deleted_at: Option<Timestamp>,
) -> ExportDescriptor {
    ExportDescriptor::new(
        ExportId::new(format!("{name}-id")).expect("export id"),
        ExportName::new(name).expect("export name"),
        4096,
        ExportEngineKind::Memory,
        state,
        Timestamp::new("unix_us:1").expect("created timestamp"),
        Timestamp::new("unix_us:1").expect("updated timestamp"),
        deleted_at,
    )
    .expect("descriptor")
}
