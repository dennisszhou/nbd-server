use nbd_config::{ConfigSource, NbdConfig};
use nbd_control_plane::{
    CatalogUrl, CreateExport, DeleteExport, ExportCatalog, ExportName, SQLiteExportCatalog,
};
use nbd_protocol::constants::{
    NBD_EINVAL, NBD_FLAG_HAS_FLAGS, NBD_FLAG_SEND_FLUSH, NBD_REP_ERR_UNKNOWN,
};
use nbd_server::ToyServer;
use nbd_test_support::TestRuntime;
use nbd_us_client::{ClientError, NbdClient};

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
async fn client_reads_writes_flushes_and_disconnects() {
    let runtime = TestRuntime::new().expect("test runtime");
    let catalog = migrated_catalog(&runtime).await;
    catalog
        .create_export(CreateExport::new(export_name("disk-a"), 4096, 4096).unwrap())
        .await
        .expect("create export");

    let server = ToyServer::start(load_config(&runtime).expect("load config"))
        .await
        .expect("start server");
    let mut client = NbdClient::connect(server.addr(), "disk-a")
        .await
        .expect("connect client");

    assert_eq!(client.read(0, 8).await.expect("zero read"), vec![0; 8]);
    client.write(2, b"hello").await.expect("write");
    assert_eq!(
        client.read(0, 10).await.expect("readback"),
        vec![0, 0, b'h', b'e', b'l', b'l', b'o', 0, 0, 0],
    );
    client.flush().await.expect("flush");
    client.disconnect().await.expect("disconnect");

    server.shutdown().await.expect("shutdown server");
}

#[tokio::test]
async fn different_exports_have_independent_toy_contents() {
    let runtime = TestRuntime::new().expect("test runtime");
    let catalog = migrated_catalog(&runtime).await;
    catalog
        .create_export(CreateExport::new(export_name("disk-a"), 4096, 4096).unwrap())
        .await
        .expect("create disk-a");
    catalog
        .create_export(CreateExport::new(export_name("disk-b"), 4096, 4096).unwrap())
        .await
        .expect("create disk-b");

    let server = ToyServer::start(load_config(&runtime).expect("load config"))
        .await
        .expect("start server");
    let mut disk_a = NbdClient::connect(server.addr(), "disk-a")
        .await
        .expect("connect disk-a");
    let mut disk_b = NbdClient::connect(server.addr(), "disk-b")
        .await
        .expect("connect disk-b");

    disk_a.write(0, b"aaaa").await.expect("write disk-a");
    assert_eq!(
        disk_a.read(0, 4).await.expect("read disk-a"),
        b"aaaa".to_vec(),
    );
    assert_eq!(disk_b.read(0, 4).await.expect("read disk-b"), vec![0; 4]);

    disk_a.disconnect().await.expect("disconnect disk-a");
    disk_b.disconnect().await.expect("disconnect disk-b");
    server.shutdown().await.expect("shutdown server");
}

#[tokio::test]
async fn out_of_bounds_reads_return_nbd_error() {
    let runtime = TestRuntime::new().expect("test runtime");
    let catalog = migrated_catalog(&runtime).await;
    catalog
        .create_export(CreateExport::new(export_name("disk-a"), 8, 4096).unwrap())
        .await
        .expect("create export");

    let server = ToyServer::start(load_config(&runtime).expect("load config"))
        .await
        .expect("start server");
    let mut client = NbdClient::connect(server.addr(), "disk-a")
        .await
        .expect("connect client");

    assert!(matches!(
        client.read(7, 2).await,
        Err(ClientError::CommandError {
            command: "READ",
            error: NBD_EINVAL,
        }),
    ));

    client.disconnect().await.expect("disconnect");
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

fn assert_unknown_export(result: nbd_us_client::Result<NbdClient>) {
    assert!(matches!(
        result,
        Err(ClientError::OptionError {
            reply_type: NBD_REP_ERR_UNKNOWN,
            ..
        }),
    ));
}
