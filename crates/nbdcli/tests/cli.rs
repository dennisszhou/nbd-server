use nbd_control_plane::{CatalogUrl, SQLiteExportCatalog};
use nbd_test_support::TestRuntime;
use serde_json::Value;
use std::process::{Command, Output};

const MIGRATIONS: &[&str] = &[
    include_str!("../../../prisma/migrations/20260501000000_init/migration.sql"),
    include_str!(
        "../../../prisma/migrations/20260504000000_export_heads_tree_metadata/migration.sql"
    ),
    include_str!(
        "../../../prisma/migrations/20260504010000_simple_durable_engine_kind/migration.sql"
    ),
    include_str!("../../../prisma/migrations/20260505000000_wal_durable_engine_kind/migration.sql"),
    include_str!("../../../prisma/migrations/20260505010000_cow_tree_metadata/migration.sql"),
];

#[tokio::test]
async fn cli_creates_inspects_lists_and_deletes_exports() {
    let runtime = TestRuntime::new().expect("test runtime");
    migrate_catalog(&runtime).await;

    let create = nbdcli(
        &runtime,
        &[
            "create",
            "disk-a",
            "--size",
            "1048576",
            "--block-size",
            "4096",
        ],
    );
    assert_success(&create);
    assert!(stdout(&create).contains("created export disk-a"));
    assert!(stdout(&create).contains("engine=memory"));

    let inspect = nbdcli(&runtime, &["inspect", "disk-a", "--json"]);
    assert_success(&inspect);
    let inspected = json_stdout(&inspect);
    assert_eq!(inspected["name"], "disk-a");
    assert_eq!(inspected["state"], "active");
    assert_eq!(inspected["engine_kind"], "memory");
    assert_eq!(inspected["head"]["layout_kind"], "memory_empty");
    assert_eq!(inspected["head"]["size_bytes"], 1048576);
    assert_eq!(inspected["head"]["checkpoint_wal_seq"], 0);
    assert!(inspected["head"]["root_node_id"].is_null());

    let list = nbdcli(&runtime, &["list", "--json"]);
    assert_success(&list);
    let listed = json_stdout(&list);
    assert_eq!(listed.as_array().expect("list array").len(), 1);
    assert_eq!(listed[0]["name"], "disk-a");
    assert_eq!(listed[0]["engine_kind"], "memory");

    let delete = nbdcli(&runtime, &["delete", "disk-a"]);
    assert_success(&delete);
    assert!(stdout(&delete).contains("deleted export disk-a"));

    let active = nbdcli(&runtime, &["list", "--json"]);
    assert_success(&active);
    assert!(json_stdout(&active)
        .as_array()
        .expect("active list")
        .is_empty());

    let include_deleted = nbdcli(&runtime, &["list", "--include-deleted", "--json"]);
    assert_success(&include_deleted);
    let all = json_stdout(&include_deleted);
    assert_eq!(all.as_array().expect("all list").len(), 1);
    assert_eq!(all[0]["state"], "deleted");
}

#[tokio::test]
async fn cli_uses_explicit_config_and_reports_missing_exports() {
    let runtime = TestRuntime::new().expect("test runtime");
    migrate_catalog(&runtime).await;

    let inspect = nbdcli(&runtime, &["inspect", "missing"]);

    assert!(!inspect.status.success(), "inspect unexpectedly succeeded");
    assert!(stderr(&inspect).contains("export `missing` not found"));
}

#[tokio::test]
async fn cli_creates_simple_durable_exports() {
    let runtime = TestRuntime::new().expect("test runtime");
    migrate_catalog(&runtime).await;

    let create = nbdcli(
        &runtime,
        &[
            "create",
            "disk-durable",
            "--size",
            "1048576",
            "--engine",
            "simple_durable",
        ],
    );
    assert_success(&create);
    assert!(stdout(&create).contains("engine=simple_durable"));

    let inspect = nbdcli(&runtime, &["inspect", "disk-durable", "--json"]);
    assert_success(&inspect);
    let inspected = json_stdout(&inspect);
    assert_eq!(inspected["engine_kind"], "simple_durable");
    assert_eq!(inspected["head"]["layout_kind"], "simple_mutable_tree");
    assert!(inspected["head"]["root_node_id"].is_null());
}

#[tokio::test]
async fn cli_creates_wal_durable_exports() {
    let runtime = TestRuntime::new().expect("test runtime");
    migrate_catalog(&runtime).await;

    let create = nbdcli(
        &runtime,
        &[
            "create",
            "disk-wal",
            "--size",
            "1048576",
            "--engine",
            "wal_durable",
        ],
    );
    assert_success(&create);
    assert!(stdout(&create).contains("engine=wal_durable"));

    let inspect = nbdcli(&runtime, &["inspect", "disk-wal", "--json"]);
    assert_success(&inspect);
    let inspected = json_stdout(&inspect);
    assert_eq!(inspected["engine_kind"], "wal_durable");
    assert_eq!(inspected["head"]["layout_kind"], "cow_immutable_tree");
    assert_eq!(inspected["head"]["checkpoint_wal_seq"], 0);
    assert!(inspected["head"]["root_node_id"].is_null());
}

async fn migrate_catalog(runtime: &TestRuntime) {
    let url = CatalogUrl::parse(runtime.catalog_url()).expect("catalog URL");
    let catalog = SQLiteExportCatalog::connect(&url)
        .await
        .expect("connect catalog");

    for migration in MIGRATIONS {
        sqlx::raw_sql(migration)
            .execute(catalog.pool())
            .await
            .expect("apply migration");
    }
}

fn nbdcli(runtime: &TestRuntime, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_nbdcli"))
        .arg("--config")
        .arg(runtime.config_path())
        .args(args)
        .output()
        .expect("run nbdcli")
}

fn assert_success(output: &Output) {
    assert!(
        output.status.success(),
        "command failed\nstdout:\n{}\nstderr:\n{}",
        stdout(output),
        stderr(output)
    );
}

fn json_stdout(output: &Output) -> Value {
    serde_json::from_slice(&output.stdout).expect("JSON stdout")
}

fn stdout(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned()
}

fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}
