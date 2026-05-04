use nbd_control_plane::{
    BlobKey, CatalogProvider, CatalogUrl, ChunkIndex, CreateExport, ExportEngineKind, ExportHead,
    ExportLayoutKind, ExportName, ExportState, ListExports, SimpleChunkRef, WalSeq,
    SIMPLE_CHUNK_BYTES,
};
use std::str::FromStr;

#[test]
fn catalog_url_parses_file_urls_as_sqlite() {
    let url = CatalogUrl::parse("file:/tmp/catalog.db").expect("parse catalog URL");

    assert_eq!(url.provider(), CatalogProvider::Sqlite);
    assert_eq!(url.sqlite_path().unwrap().to_str(), Some("/tmp/catalog.db"));
    assert_eq!(url.as_str(), "file:/tmp/catalog.db");
}

#[test]
fn catalog_url_rejects_unknown_schemes() {
    let error = CatalogUrl::parse("mysql://localhost/catalog").unwrap_err();

    assert!(error
        .to_string()
        .contains("unsupported catalog URL scheme `mysql`"));
}

#[test]
fn create_export_validates_basic_domain_values() {
    let name = ExportName::new("disk-a").expect("valid name");
    let request = CreateExport::new(name, 1024 * 1024, 4096, ExportEngineKind::Memory)
        .expect("valid request");

    assert_eq!(request.name().as_str(), "disk-a");
    assert_eq!(request.size_bytes(), 1024 * 1024);
    assert_eq!(request.block_size(), 4096);
    assert_eq!(request.engine_kind(), ExportEngineKind::Memory);
    assert!(CreateExport::new(
        ExportName::new("disk-b").unwrap(),
        0,
        4096,
        ExportEngineKind::Memory,
    )
    .is_err());
    assert!(CreateExport::new(
        ExportName::new("disk-c").unwrap(),
        4096,
        0,
        ExportEngineKind::Memory,
    )
    .is_err());
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
    assert_eq!(head.checkpoint_wal_seq(), WalSeq::zero());
    assert!(ExportHead::memory_empty(0).is_err());
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
    assert!(SimpleChunkRef::new(
        ChunkIndex::new(7),
        BlobKey::new("blob-7").expect("valid blob key"),
        SIMPLE_CHUNK_BYTES - 1,
    )
    .is_err());
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
    assert_eq!(ExportEngineKind::Memory.to_string(), "memory");
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
    assert_eq!(ExportLayoutKind::MemoryEmpty.to_string(), "memory_empty");
    assert_eq!(
        ExportLayoutKind::SimpleMutableTree.to_string(),
        "simple_mutable_tree"
    );
    assert!(ExportLayoutKind::from_str("generation").is_err());
}

#[test]
fn list_exports_defaults_to_active_only() {
    assert!(!ListExports::active_only().includes_deleted());
    assert!(ListExports::include_deleted().includes_deleted());
}
