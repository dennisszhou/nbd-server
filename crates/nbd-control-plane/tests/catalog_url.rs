use nbd_control_plane::{CatalogProvider, CatalogUrl};

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

    assert!(
        error
            .to_string()
            .contains("unsupported catalog URL scheme `mysql`")
    );
}
