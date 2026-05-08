use nbd_config::default_config_path_for_home;
use nbd_control_plane::{
    BlobKey, CatalogUrl, ChunkIndex, CowChunkRef, CowTreeMetadataStore, ExportCatalog, ExportName,
    InspectExport, PublishCompaction, SQLiteExportCatalog, TREE_CHUNK_BYTES, WalSeq,
};
use nbd_test_support::TestRuntime;
use serde_json::Value;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

const MIGRATIONS: &[&str] = &[include_str!(
    "../../../prisma/migrations/20260506000000_baseline/migration.sql"
)];
static NEXT_TEMP_ID: AtomicU64 = AtomicU64::new(1);

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
    assert!(inspected["head"].get("base_wal_seq").is_none());
    assert!(inspected["head"].get("root_node_id").is_none());

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
    assert!(
        json_stdout(&active)
            .as_array()
            .expect("active list")
            .is_empty()
    );

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

    let inspect_json = nbdcli(&runtime, &["--json", "inspect", "missing"]);
    assert!(
        !inspect_json.status.success(),
        "inspect unexpectedly succeeded"
    );
    assert!(
        stdout(&inspect_json).is_empty(),
        "JSON error command wrote stdout"
    );
    let error = json_stderr(&inspect_json);
    assert_eq!(error["status"], "error");
    assert_eq!(error["code"], "catalog_error");
    assert_eq!(error["operation"], "inspect");
    assert_eq!(error["resource"], "missing");
    assert!(
        error["message"]
            .as_str()
            .expect("error message")
            .contains("export `missing` not found")
    );
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
    assert_eq!(inspected["head"]["base_wal_seq"], 0);
    assert!(inspected["head"]["root_node_id"].is_null());
}

#[tokio::test]
async fn cli_clones_committed_wal_durable_exports() {
    let runtime = TestRuntime::new().expect("test runtime");
    migrate_catalog(&runtime).await;
    let size = TREE_CHUNK_BYTES.to_string();

    let create = nbdcli(
        &runtime,
        &[
            "create",
            "source",
            "--size",
            &size,
            "--engine",
            "wal_durable",
        ],
    );
    assert_success(&create);
    let source_root = publish_cow_root(&runtime, "source", 7).await;

    let clone = nbdcli(&runtime, &["clone", "source", "destination"]);
    assert_success(&clone);
    assert!(stdout(&clone).contains("cloned export destination from source"));
    assert!(stdout(&clone).contains("source_base_wal_seq=7"));
    assert!(stdout(&clone).contains("destination_base_wal_seq=0"));
    assert!(stdout(&clone).contains("source WAL was not cloned"));

    let inspect = nbdcli(&runtime, &["inspect", "destination", "--json"]);
    assert_success(&inspect);
    let inspected = json_stdout(&inspect);
    assert_eq!(inspected["name"], "destination");
    assert_eq!(inspected["engine_kind"], "wal_durable");
    assert_eq!(inspected["head"]["layout_kind"], "cow_immutable_tree");
    assert_eq!(inspected["head"]["base_wal_seq"], 0);
    assert_eq!(inspected["head"]["root_node_id"], source_root);
    assert_eq!(inspected["head"]["size_bytes"], TREE_CHUNK_BYTES);
}

#[tokio::test]
async fn cli_global_json_reports_write_results() {
    let runtime = TestRuntime::new().expect("test runtime");
    migrate_catalog(&runtime).await;
    let size = TREE_CHUNK_BYTES.to_string();

    let create = nbdcli(
        &runtime,
        &[
            "--json",
            "create",
            "source",
            "--size",
            &size,
            "--engine",
            "wal_durable",
        ],
    );
    assert_success(&create);
    let created = json_stdout(&create);
    assert_eq!(created["status"], "created");
    assert_eq!(created["export"]["name"], "source");
    assert_eq!(created["export"]["engine_kind"], "wal_durable");

    let source_root = publish_cow_root(&runtime, "source", 9).await;

    let clone = nbdcli(&runtime, &["--json", "clone", "source", "destination"]);
    assert_success(&clone);
    let cloned = json_stdout(&clone);
    assert_eq!(cloned["status"], "cloned");
    assert_eq!(cloned["source"]["name"], "source");
    assert_eq!(cloned["source"]["head"]["root_node_id"], source_root);
    assert_eq!(cloned["destination"]["name"], "destination");
    assert_eq!(cloned["source_wal_cloned"], false);

    let delete = nbdcli(&runtime, &["--json", "delete", "destination"]);
    assert_success(&delete);
    let deleted = json_stdout(&delete);
    assert_eq!(deleted["status"], "deleted");
    assert_eq!(deleted["name"], "destination");
}

#[tokio::test]
async fn cli_clone_rejects_empty_wal_sources() {
    let runtime = TestRuntime::new().expect("test runtime");
    migrate_catalog(&runtime).await;

    let create = nbdcli(
        &runtime,
        &[
            "create",
            "empty",
            "--size",
            "1048576",
            "--engine",
            "wal_durable",
        ],
    );
    assert_success(&create);

    let clone = nbdcli(&runtime, &["clone", "empty", "destination"]);
    assert!(!clone.status.success(), "clone unexpectedly succeeded");
    assert!(stderr(&clone).contains("source committed snapshot is empty"));
}

#[test]
fn cli_default_config_load_is_read_only() {
    let temp = TempRoot::new();
    let config_path = default_config_path_for_home(temp.path());

    let output = Command::new(env!("CARGO_BIN_EXE_nbdcli"))
        .env("HOME", temp.path())
        .arg("--json")
        .arg("list")
        .output()
        .expect("run nbdcli");

    assert!(!output.status.success());
    assert!(
        stdout(&output).is_empty(),
        "JSON error command wrote stdout"
    );
    let error = json_stderr(&output);
    assert_eq!(error["status"], "error");
    assert_eq!(error["code"], "config_error");
    assert_eq!(error["operation"], "list");
    assert!(error["resource"].is_null());
    assert!(!config_path.exists());
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

async fn publish_cow_root(runtime: &TestRuntime, name: &str, checkpoint: u64) -> Value {
    let url = CatalogUrl::parse(runtime.catalog_url()).expect("catalog URL");
    let catalog = SQLiteExportCatalog::connect(&url)
        .await
        .expect("connect catalog");
    let export = catalog
        .inspect_export(InspectExport::new(export_name(name)))
        .await
        .expect("inspect source export");
    let published = catalog
        .publish_compaction(
            PublishCompaction::new(
                export.id().clone(),
                export.head().clone(),
                WalSeq::new(checkpoint),
                vec![cow_chunk(0, "source-root")],
            )
            .expect("publish request"),
        )
        .await
        .expect("publish source root")
        .into_record();

    Value::String(
        published
            .head()
            .root_node_id()
            .expect("published root")
            .as_str()
            .to_owned(),
    )
}

fn cow_chunk(index: u64, key: &str) -> CowChunkRef {
    CowChunkRef::new(
        ChunkIndex::new(index),
        BlobKey::new(key).expect("valid blob key"),
        TREE_CHUNK_BYTES,
    )
    .expect("valid cow chunk")
}

fn export_name(name: &str) -> ExportName {
    ExportName::new(name).expect("valid export name")
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

fn json_stderr(output: &Output) -> Value {
    serde_json::from_slice(&output.stderr).expect("JSON stderr")
}

fn stdout(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned()
}

fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

struct TempRoot {
    path: PathBuf,
}

impl TempRoot {
    fn new() -> Self {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let counter = NEXT_TEMP_ID.fetch_add(1, Ordering::Relaxed);
        let path = env::temp_dir().join(format!(
            "nbdcli-config-test-{}-{unique}-{counter}",
            std::process::id()
        ));

        fs::create_dir_all(&path).unwrap();
        Self { path }
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempRoot {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}
