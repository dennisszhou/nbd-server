use nbd_client::{ClientError, NbdClient};
use nbd_config::{ConfigSource, NbdConfig};
use nbd_control_plane::{
    CatalogUrl, CreateExport, DeleteExport, ExportCatalog, ExportName, SQLiteExportCatalog,
};
use nbd_protocol::constants::{NBD_FLAG_HAS_FLAGS, NBD_FLAG_SEND_FLUSH, NBD_REP_ERR_UNKNOWN};
use nbd_server::ToyServer;
use nbd_test_support::TestRuntime;

const MIGRATION: &str =
    include_str!("../../../prisma/migrations/20260501000000_init/migration.sql");

#[tokio::test]
async fn active_export_negotiates_over_tcp() {
    let runtime = TestRuntime::new().expect("test runtime");
    let catalog = migrated_catalog(&runtime).await;
    catalog
        .create_export(CreateExport::new(export_name("disk-a"), 4096, 4096).unwrap())
        .await
        .expect("create export");

    let server = ToyServer::start(load_config(&runtime).expect("load config"))
        .await
        .expect("start server");
    let client = NbdClient::connect(server.addr(), "disk-a")
        .await
        .expect("connect client");

    assert_eq!(client.export_size_bytes(), 4096);
    assert_eq!(
        client.transmission_flags(),
        NBD_FLAG_HAS_FLAGS | NBD_FLAG_SEND_FLUSH,
    );
    assert!(client.has_transmission_flags());
    assert_eq!(client.peer_addr().expect("peer addr"), server.addr());

    server.shutdown().await.expect("shutdown server");
}

#[tokio::test]
async fn missing_or_deleted_exports_fail_during_go() {
    let runtime = TestRuntime::new().expect("test runtime");
    let catalog = migrated_catalog(&runtime).await;
    catalog
        .create_export(CreateExport::new(export_name("deleted"), 4096, 4096).unwrap())
        .await
        .expect("create export");
    catalog
        .delete_export(DeleteExport::new(export_name("deleted")))
        .await
        .expect("delete export");

    let server = ToyServer::start(load_config(&runtime).expect("load config"))
        .await
        .expect("start server");

    assert_unknown_export(NbdClient::connect(server.addr(), "missing").await);
    assert_unknown_export(NbdClient::connect(server.addr(), "deleted").await);

    server.shutdown().await.expect("shutdown server");
}

async fn migrated_catalog(runtime: &TestRuntime) -> SQLiteExportCatalog {
    let url = CatalogUrl::parse(runtime.catalog_url()).expect("catalog URL");
    let catalog = SQLiteExportCatalog::connect(&url)
        .await
        .expect("connect catalog");

    sqlx::raw_sql(MIGRATION)
        .execute(catalog.pool())
        .await
        .expect("apply migration");

    catalog
}

fn load_config(runtime: &TestRuntime) -> Result<NbdConfig, nbd_config::ConfigError> {
    NbdConfig::load(ConfigSource::ExplicitPath(
        runtime.config_path().to_path_buf(),
    ))
}

fn export_name(name: &str) -> ExportName {
    ExportName::new(name).expect("valid export name")
}

fn assert_unknown_export(result: nbd_client::Result<NbdClient>) {
    assert!(matches!(
        result,
        Err(ClientError::OptionError {
            reply_type: NBD_REP_ERR_UNKNOWN,
            ..
        }),
    ));
}
