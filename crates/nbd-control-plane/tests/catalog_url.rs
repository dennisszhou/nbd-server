use nbd_control_plane::{CatalogDoctorCheck, CatalogDoctorStatus, CatalogProvider, CatalogUrl};
use nbd_test_support::TestRuntime;

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

#[tokio::test]
async fn doctor_catalog_missing_sqlite_catalog_does_not_create_file() {
    let runtime = TestRuntime::new().expect("test runtime");
    let url = CatalogUrl::parse(runtime.catalog_url()).expect("parse catalog URL");

    let checks = nbd_control_plane::doctor_catalog(&url).await;

    assert!(has_check(
        &checks,
        "catalog_provider",
        CatalogDoctorStatus::Ok
    ));
    assert!(has_check(
        &checks,
        "catalog_file",
        CatalogDoctorStatus::Failed
    ));
    assert!(!runtime.catalog_path().exists());
}

#[tokio::test]
async fn doctor_catalog_rejects_postgres_until_adapter_exists() {
    let url = CatalogUrl::parse("postgres://localhost/nbd").expect("parse catalog URL");

    let checks = nbd_control_plane::doctor_catalog(&url).await;

    assert!(has_check(
        &checks,
        "catalog_provider",
        CatalogDoctorStatus::Failed
    ));
}

fn has_check(checks: &[CatalogDoctorCheck], name: &str, status: CatalogDoctorStatus) -> bool {
    checks
        .iter()
        .any(|check| check.name() == name && check.status() == status)
}
