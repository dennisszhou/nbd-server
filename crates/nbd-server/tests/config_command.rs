use nbd_config::ConfigFile;
use nbd_control_plane_sqlite::SQLiteExportCatalog;
use nbd_test_support::TestRuntime;
use serde_json::Value;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

static NEXT_TEMP_ID: AtomicU64 = AtomicU64::new(1);
const MIGRATIONS: &[&str] = &[
    include_str!("../../../prisma/migrations/20260506000000_baseline/migration.sql"),
    include_str!("../../../prisma/migrations/20260512000000_tree_format/migration.sql"),
];

#[test]
fn config_command_prints_missing_explicit_defaults_without_writing() {
    let temp = TempRoot::new();
    let config_path = temp.path().join("server").join("config.toml");

    let output = Command::new(env!("CARGO_BIN_EXE_nbd-server"))
        .arg("config")
        .arg("--config")
        .arg(&config_path)
        .output()
        .expect("run nbd-server config");

    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("config stdout is UTF-8");
    assert!(stdout.contains("[catalog]"));
    assert!(stdout.contains("[runtime]"));
    assert!(stdout.contains("[blob_store]"));
    assert!(stdout.contains("[server]"));
    assert!(stdout.contains("[logging]"));
    assert!(stdout.contains("kind = \"local\""));
    assert!(stdout.contains(&format!(
        "url = \"{}\"",
        nbd_config::catalog_file_url_for_path(temp.path().join("server").join("catalog.db"))
            .expect("catalog URL")
    )));
    assert!(!config_path.exists());
}

#[test]
fn config_command_get_prints_existing_explicit_value() {
    let temp = TempRoot::new();
    let config_path = temp.path().join("config.toml");
    let config = ConfigFile::explicit(&config_path)
        .default_config()
        .expect("default config");
    fs::write(
        &config_path,
        config.to_toml_string().expect("serialize config"),
    )
    .expect("write config");

    let output = Command::new(env!("CARGO_BIN_EXE_nbd-server"))
        .arg("config")
        .arg("--config")
        .arg(&config_path)
        .arg("get")
        .arg("server.export_queue_depth")
        .output()
        .expect("run nbd-server config get");

    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        String::from_utf8(output.stdout).expect("config stdout is UTF-8"),
        "64\n"
    );
}

#[test]
fn config_command_path_prints_selected_explicit_path_without_writing() {
    let temp = TempRoot::new();
    let config_path = temp.path().join("server").join("config.toml");

    let output = Command::new(env!("CARGO_BIN_EXE_nbd-server"))
        .arg("config")
        .arg("--config")
        .arg(&config_path)
        .arg("--path")
        .output()
        .expect("run nbd-server config --path");

    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        String::from_utf8(output.stdout).expect("config path stdout is UTF-8"),
        format!("{}\n", config_path.display())
    );
    assert!(!config_path.exists());
}

#[test]
fn config_command_init_writes_missing_explicit_config_once() {
    let temp = TempRoot::new();
    let config_path = temp.path().join("server").join("config.toml");

    let output = Command::new(env!("CARGO_BIN_EXE_nbd-server"))
        .arg("config")
        .arg("init")
        .arg("--config")
        .arg(&config_path)
        .output()
        .expect("run nbd-server config init");

    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(config_path.exists());
    assert!(
        String::from_utf8(output.stdout)
            .expect("config init stdout is UTF-8")
            .contains(&format!("initialized config {}", config_path.display()))
    );

    let existing = Command::new(env!("CARGO_BIN_EXE_nbd-server"))
        .arg("config")
        .arg("init")
        .arg("--config")
        .arg(&config_path)
        .output()
        .expect("rerun nbd-server config init");

    assert!(!existing.status.success());
    assert!(
        String::from_utf8(existing.stderr)
            .expect("config init stderr is UTF-8")
            .contains("config already exists")
    );
}

#[test]
fn config_command_get_prints_blob_store_kind() {
    let temp = TempRoot::new();
    let config_path = temp.path().join("config.toml");
    let config = ConfigFile::explicit(&config_path)
        .default_config()
        .expect("default config");
    fs::write(
        &config_path,
        config.to_toml_string().expect("serialize config"),
    )
    .expect("write config");

    let output = Command::new(env!("CARGO_BIN_EXE_nbd-server"))
        .arg("config")
        .arg("--config")
        .arg(&config_path)
        .arg("get")
        .arg("blob_store.kind")
        .output()
        .expect("run nbd-server config get");

    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        String::from_utf8(output.stdout).expect("config stdout is UTF-8"),
        "local\n"
    );
}

#[test]
fn config_command_get_rejects_secret_access_key() {
    let temp = TempRoot::new();
    let config_path = temp.path().join("config.toml");
    let config = ConfigFile::explicit(&config_path)
        .default_config()
        .expect("default config");
    fs::write(
        &config_path,
        config.to_toml_string().expect("serialize config"),
    )
    .expect("write config");

    let output = Command::new(env!("CARGO_BIN_EXE_nbd-server"))
        .arg("config")
        .arg("--config")
        .arg(&config_path)
        .arg("get")
        .arg("blob_store.secret_access_key")
        .output()
        .expect("run nbd-server config get");

    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).expect("config stderr is UTF-8");
    assert!(stderr.contains("unknown config key"));
    assert!(!nbd_config::ConfigKey::SUPPORTED_KEYS.contains("secret_access_key"));
}

#[tokio::test]
async fn doctor_command_reports_migrated_explicit_catalog_json() {
    let runtime = TestRuntime::new().expect("test runtime");
    migrate_catalog(&runtime).await;

    let output = Command::new(env!("CARGO_BIN_EXE_nbd-server"))
        .arg("doctor")
        .arg("--config")
        .arg(runtime.config_path())
        .arg("--json")
        .output()
        .expect("run nbd-server doctor");

    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let report = json_stdout(&output);
    assert_eq!(report["status"], "ok");
    assert!(has_check(&report, "config", "ok"));
    assert!(has_check(&report, "catalog_schema", "ok"));
}

#[test]
fn doctor_command_rejects_missing_explicit_config() {
    let temp = TempRoot::new();
    let config_path = temp.path().join("missing").join("config.toml");

    let output = Command::new(env!("CARGO_BIN_EXE_nbd-server"))
        .arg("doctor")
        .arg("--config")
        .arg(&config_path)
        .output()
        .expect("run nbd-server doctor");

    assert!(!output.status.success());
    let stdout = String::from_utf8(output.stdout).expect("doctor stdout is UTF-8");
    assert!(stdout.contains("config: failed"));
    assert!(String::from_utf8_lossy(&output.stderr).contains("doctor checks failed"));
    assert!(!config_path.exists());
}

#[test]
fn doctor_command_rejects_missing_catalog_without_creating_it() {
    let temp = TempRoot::new();
    let config_path = temp.path().join("config.toml");
    let catalog_path = temp.path().join("catalog.db");
    let config = ConfigFile::explicit(&config_path)
        .default_config()
        .expect("default config");
    fs::write(
        &config_path,
        config.to_toml_string().expect("serialize config"),
    )
    .expect("write config");

    let output = Command::new(env!("CARGO_BIN_EXE_nbd-server"))
        .arg("doctor")
        .arg("--config")
        .arg(&config_path)
        .arg("--json")
        .output()
        .expect("run nbd-server doctor");

    assert!(!output.status.success());
    let report = json_stdout(&output);
    assert_eq!(report["status"], "failed");
    assert!(has_check(&report, "catalog_file", "failed"));
    assert!(!catalog_path.exists());
}

#[test]
fn doctor_command_rejects_unmigrated_catalog() {
    let runtime = TestRuntime::new().expect("test runtime");
    fs::File::create(runtime.catalog_path()).expect("create catalog file");

    let output = Command::new(env!("CARGO_BIN_EXE_nbd-server"))
        .arg("doctor")
        .arg("--config")
        .arg(runtime.config_path())
        .arg("--json")
        .output()
        .expect("run nbd-server doctor");

    assert!(!output.status.success());
    let report = json_stdout(&output);
    assert_eq!(report["status"], "failed");
    assert!(has_check(&report, "catalog_schema", "failed"));
}

async fn migrate_catalog(runtime: &TestRuntime) {
    fs::File::create(runtime.catalog_path()).expect("create catalog file");
    let catalog = SQLiteExportCatalog::connect_path(runtime.catalog_path())
        .await
        .expect("connect catalog");

    for migration in MIGRATIONS {
        sqlx::raw_sql(migration)
            .execute(catalog.pool())
            .await
            .expect("apply migration");
    }
}

fn json_stdout(output: &std::process::Output) -> Value {
    serde_json::from_slice(&output.stdout).expect("JSON stdout")
}

fn has_check(report: &Value, name: &str, status: &str) -> bool {
    report["checks"]
        .as_array()
        .expect("checks array")
        .iter()
        .any(|check| check["name"] == name && check["status"] == status)
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
            "nbd-server-config-command-test-{}-{unique}-{counter}",
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
