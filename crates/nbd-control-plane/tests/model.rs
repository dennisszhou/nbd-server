use nbd_control_plane::{
    CatalogProvider, CatalogUrl, CommittedRoot, CreateExport, ExportGeneration, ExportName,
    ExportState, ListExports, WalSeq,
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
    let request = CreateExport::new(name, 1024 * 1024, 4096).expect("valid request");

    assert_eq!(request.name().as_str(), "disk-a");
    assert_eq!(request.size_bytes(), 1024 * 1024);
    assert_eq!(request.block_size(), 4096);
    assert!(CreateExport::new(ExportName::new("disk-b").unwrap(), 0, 4096).is_err());
    assert!(CreateExport::new(ExportName::new("disk-c").unwrap(), 4096, 0).is_err());
}

#[test]
fn export_names_must_not_be_empty_or_contain_nul() {
    assert!(ExportName::new("").is_err());
    assert!(ExportName::new("bad\0name").is_err());
}

#[test]
fn committed_root_can_represent_empty_generation_zero() {
    let root = CommittedRoot::empty();

    assert!(root.root_node_id().is_none());
    assert_eq!(root.checkpoint_wal_seq(), WalSeq::zero());
    assert_eq!(root.generation(), ExportGeneration::zero());
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
fn list_exports_defaults_to_active_only() {
    assert!(!ListExports::active_only().includes_deleted());
    assert!(ListExports::include_deleted().includes_deleted());
}
