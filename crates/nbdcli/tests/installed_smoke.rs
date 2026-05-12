use nbd_control_plane_sqlite::SQLiteExportCatalog;
use nbd_test_support::TestRuntime;
use serde_json::Value;
use std::env;
use std::ffi::OsString;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

const MIGRATIONS: &[&str] = &[
    include_str!("../../../prisma/migrations/20260506000000_baseline/migration.sql"),
    include_str!("../../../prisma/migrations/20260512000000_tree_format/migration.sql"),
];
static NEXT_TEMP_ID: AtomicU64 = AtomicU64::new(1);

#[tokio::test]
async fn installed_nbdcli_doctor_runs_by_name_outside_source_tree() {
    let runtime = TestRuntime::new().expect("test runtime");
    migrate_catalog(&runtime).await;
    let cwd = TempRoot::new();
    assert_outside_repo(cwd.path());

    let output = Command::new("nbdcli")
        .env("PATH", path_with_binary(env!("CARGO_BIN_EXE_nbdcli")))
        .current_dir(cwd.path())
        .arg("--config")
        .arg(runtime.config_path())
        .arg("--json")
        .arg("doctor")
        .output()
        .expect("run nbdcli by command name");

    assert_success(&output);
    let report = json_stdout(&output);
    assert_eq!(report["status"], "ok");
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

fn path_with_binary(binary: &str) -> OsString {
    let binary_dir = Path::new(binary).parent().expect("binary parent");
    let existing_path = env::var_os("PATH").unwrap_or_default();
    let paths = std::iter::once(binary_dir.to_path_buf()).chain(env::split_paths(&existing_path));
    env::join_paths(paths).expect("join PATH")
}

fn assert_outside_repo(path: &Path) {
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let repo_root = manifest_dir
        .parent()
        .and_then(Path::parent)
        .expect("workspace root");
    assert!(
        !path.starts_with(repo_root),
        "{} unexpectedly lives inside {}",
        path.display(),
        repo_root.display()
    );
}

fn assert_success(output: &Output) {
    assert!(
        output.status.success(),
        "command failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn json_stdout(output: &Output) -> Value {
    serde_json::from_slice(&output.stdout).expect("JSON stdout")
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
            "nbdcli-installed-smoke-{}-{unique}-{counter}",
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
