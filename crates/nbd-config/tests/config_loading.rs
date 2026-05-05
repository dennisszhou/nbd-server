use nbd_config::{
    catalog_file_url_for_path, default_blob_dir_for_home, default_config_path_for_home,
    default_log_file_path, default_state_dir_for_home, default_wal_dir_for_home, ConfigSource,
    ExportRuntimeKind, NbdConfig, DEFAULT_EXPORT_QUEUE_DEPTH, DEFAULT_LOG_FILE_PATH,
    DEFAULT_REPLY_QUEUE_CAPACITY,
};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

static HOME_ENV_LOCK: Mutex<()> = Mutex::new(());
static NEXT_TEMP_ID: AtomicU64 = AtomicU64::new(1);

#[test]
fn explicit_config_loads_from_requested_path() {
    let temp = TempRoot::new();
    let state_dir = temp.path().join("state");
    let catalog_path = temp.path().join("catalog.db");
    let config_path = temp.path().join("config.toml");

    write_config(&config_path, &state_dir, &catalog_path);

    let config = NbdConfig::load(ConfigSource::ExplicitPath(config_path)).unwrap();

    assert_eq!(config.runtime.state_dir, state_dir);
    assert_eq!(config.runtime.blob_dir, state_dir.join("blobs"));
    assert_eq!(config.runtime.wal_dir, state_dir.join("wal"));
    assert_eq!(
        config.catalog.url,
        catalog_file_url_for_path(catalog_path).unwrap()
    );
    assert_eq!(config.server.export_runtime, ExportRuntimeKind::Concurrent);
    assert_eq!(
        config.server.export_queue_depth.get(),
        DEFAULT_EXPORT_QUEUE_DEPTH
    );
    assert_eq!(
        config.server.connection.reply_queue_capacity.get(),
        DEFAULT_REPLY_QUEUE_CAPACITY,
    );
    assert_eq!(config.logging.file_path, default_log_file_path());
}

#[test]
fn explicit_config_does_not_bootstrap_user_default() {
    let _guard = HOME_ENV_LOCK.lock().unwrap();
    let temp = TempRoot::new();
    let fake_home = temp.path().join("home");
    let explicit_root = temp.path().join("explicit");
    let config_path = explicit_root.join("config.toml");
    let state_dir = explicit_root.join("state");
    let catalog_path = explicit_root.join("catalog.db");

    fs::create_dir_all(&fake_home).unwrap();
    fs::create_dir_all(&explicit_root).unwrap();
    write_config(&config_path, &state_dir, &catalog_path);

    let old_home = EnvVarGuard::set("HOME", &fake_home);
    let config = NbdConfig::load(ConfigSource::ExplicitPath(config_path)).unwrap();

    assert_eq!(config.runtime.state_dir, state_dir);
    assert_eq!(config.runtime.blob_dir, state_dir.join("blobs"));
    assert_eq!(config.runtime.wal_dir, state_dir.join("wal"));
    assert!(!default_state_dir_for_home(&fake_home).exists());
    drop(old_home);
}

#[test]
fn default_user_path_bootstraps_absolute_config() {
    let _guard = HOME_ENV_LOCK.lock().unwrap();
    let temp = TempRoot::new();
    let fake_home = temp.path().join("home");

    fs::create_dir_all(&fake_home).unwrap();

    let old_home = EnvVarGuard::set("HOME", &fake_home);
    let config = NbdConfig::load(ConfigSource::DefaultUserPath).unwrap();
    let config_path = default_config_path_for_home(&fake_home);

    assert!(config_path.exists());
    assert_eq!(config.runtime.state_dir, fake_home.join(".nbd"));
    assert_eq!(
        config.runtime.blob_dir,
        default_blob_dir_for_home(&fake_home)
    );
    assert_eq!(config.runtime.wal_dir, default_wal_dir_for_home(&fake_home));
    assert!(config.runtime.state_dir.is_absolute());
    assert!(config.runtime.blob_dir.is_absolute());
    assert!(config.runtime.wal_dir.is_absolute());
    assert_eq!(
        config.catalog.url,
        catalog_file_url_for_path(fake_home.join(".nbd").join("catalog.db")).unwrap()
    );
    assert!(config.catalog.url.starts_with("file:"));
    assert_eq!(config.server.export_runtime, ExportRuntimeKind::Concurrent);
    assert_eq!(
        config.server.export_queue_depth.get(),
        DEFAULT_EXPORT_QUEUE_DEPTH
    );
    assert_eq!(
        config.server.connection.reply_queue_capacity.get(),
        DEFAULT_REPLY_QUEUE_CAPACITY,
    );
    assert_eq!(config.logging.file_path, default_log_file_path());

    let loaded = NbdConfig::load(ConfigSource::ExplicitPath(config_path)).unwrap();
    assert_eq!(loaded, config);
    drop(old_home);
}

#[test]
fn generated_default_config_includes_logging_section() {
    let _guard = HOME_ENV_LOCK.lock().unwrap();
    let temp = TempRoot::new();
    let fake_home = temp.path().join("home");

    fs::create_dir_all(&fake_home).unwrap();

    let old_home = EnvVarGuard::set("HOME", &fake_home);
    let config_path = default_config_path_for_home(&fake_home);
    let config = NbdConfig::load(ConfigSource::DefaultUserPath).unwrap();
    let contents = fs::read_to_string(config_path).unwrap();

    assert_eq!(config.logging.file_path, default_log_file_path());
    assert!(contents.contains("[logging]"));
    assert!(contents.contains(&format!("file_path = \"{DEFAULT_LOG_FILE_PATH}\"")));
    drop(old_home);
}

#[test]
fn explicit_config_loads_logging_file_path() {
    let temp = TempRoot::new();
    let state_dir = temp.path().join("state");
    let catalog_path = temp.path().join("catalog.db");
    let wal_dir = state_dir.join("wal");
    let log_path = temp.path().join("logs").join("current.log");
    let config_path = temp.path().join("config.toml");
    let contents = format!(
        "[catalog]\nurl = {:?}\n\n[runtime]\nstate_dir = {:?}\nwal_dir = {:?}\n\n[logging]\nfile_path = {:?}\n",
        catalog_file_url_for_path(catalog_path).unwrap(),
        state_dir,
        wal_dir,
        log_path,
    );
    fs::write(&config_path, contents).unwrap();

    let config = NbdConfig::load(ConfigSource::ExplicitPath(config_path)).unwrap();

    assert_eq!(config.logging.file_path, log_path);
}

#[test]
fn explicit_config_loads_blob_directory() {
    let temp = TempRoot::new();
    let state_dir = temp.path().join("state");
    let blob_dir = temp.path().join("isolated-blobs");
    let wal_dir = state_dir.join("wal");
    let catalog_path = temp.path().join("catalog.db");
    let config_path = temp.path().join("config.toml");
    let contents = format!(
        "[catalog]\nurl = {:?}\n\n[runtime]\nstate_dir = {:?}\nblob_dir = {:?}\nwal_dir = {:?}\n",
        catalog_file_url_for_path(catalog_path).unwrap(),
        state_dir,
        blob_dir,
        wal_dir,
    );
    fs::write(&config_path, contents).unwrap();

    let config = NbdConfig::load(ConfigSource::ExplicitPath(config_path)).unwrap();

    assert_eq!(config.runtime.state_dir, state_dir);
    assert_eq!(config.runtime.blob_dir, blob_dir);
    assert_eq!(config.runtime.wal_dir, wal_dir);
}

#[test]
fn explicit_config_loads_wal_directory() {
    let temp = TempRoot::new();
    let state_dir = temp.path().join("state");
    let wal_dir = temp.path().join("isolated-wal");
    let catalog_path = temp.path().join("catalog.db");
    let config_path = temp.path().join("config.toml");
    let contents = format!(
        "[catalog]\nurl = {:?}\n\n[runtime]\nstate_dir = {:?}\nwal_dir = {:?}\n",
        catalog_file_url_for_path(catalog_path).unwrap(),
        state_dir,
        wal_dir,
    );
    fs::write(&config_path, contents).unwrap();

    let config = NbdConfig::load(ConfigSource::ExplicitPath(config_path)).unwrap();

    assert_eq!(config.runtime.state_dir, state_dir);
    assert_eq!(config.runtime.blob_dir, state_dir.join("blobs"));
    assert_eq!(config.runtime.wal_dir, wal_dir);
}

#[test]
fn malformed_config_reports_error() {
    let temp = TempRoot::new();
    let config_path = temp.path().join("bad.toml");
    fs::write(&config_path, "[catalog]\nurl = 1\n").unwrap();

    let error = NbdConfig::load(ConfigSource::ExplicitPath(config_path))
        .expect_err("malformed config should fail");

    assert!(error.to_string().contains("failed to parse config"));
}

#[test]
fn explicit_server_config_loads_runtime_choice() {
    let temp = TempRoot::new();
    let state_dir = temp.path().join("state");
    let catalog_path = temp.path().join("catalog.db");
    let wal_dir = state_dir.join("wal");
    let config_path = temp.path().join("config.toml");
    let contents = format!(
        "[catalog]\nurl = {:?}\n\n[runtime]\nstate_dir = {:?}\nwal_dir = {:?}\n\n[server]\nexport_runtime = \"serial\"\n",
        catalog_file_url_for_path(catalog_path).unwrap(),
        state_dir,
        wal_dir,
    );
    fs::write(&config_path, contents).unwrap();

    let config = NbdConfig::load(ConfigSource::ExplicitPath(config_path)).unwrap();

    assert_eq!(config.server.export_runtime, ExportRuntimeKind::Serial);
    assert_eq!(
        config.server.export_queue_depth.get(),
        DEFAULT_EXPORT_QUEUE_DEPTH
    );
    assert_eq!(
        config.server.connection.reply_queue_capacity.get(),
        DEFAULT_REPLY_QUEUE_CAPACITY,
    );
}

#[test]
fn explicit_server_config_loads_concurrent_runtime_choice() {
    let temp = TempRoot::new();
    let state_dir = temp.path().join("state");
    let catalog_path = temp.path().join("catalog.db");
    let wal_dir = state_dir.join("wal");
    let config_path = temp.path().join("config.toml");
    let contents = format!(
        "[catalog]\nurl = {:?}\n\n[runtime]\nstate_dir = {:?}\nwal_dir = {:?}\n\n[server]\nexport_runtime = \"concurrent\"\n",
        catalog_file_url_for_path(catalog_path).unwrap(),
        state_dir,
        wal_dir,
    );
    fs::write(&config_path, contents).unwrap();

    let config = NbdConfig::load(ConfigSource::ExplicitPath(config_path)).unwrap();

    assert_eq!(config.server.export_runtime, ExportRuntimeKind::Concurrent);
    assert_eq!(
        config.server.export_queue_depth.get(),
        DEFAULT_EXPORT_QUEUE_DEPTH
    );
    assert_eq!(
        config.server.connection.reply_queue_capacity.get(),
        DEFAULT_REPLY_QUEUE_CAPACITY,
    );
}

#[test]
fn explicit_server_config_rejects_unknown_runtime_choice() {
    let temp = TempRoot::new();
    let state_dir = temp.path().join("state");
    let catalog_path = temp.path().join("catalog.db");
    let wal_dir = state_dir.join("wal");
    let config_path = temp.path().join("config.toml");
    let contents = format!(
        "[catalog]\nurl = {:?}\n\n[runtime]\nstate_dir = {:?}\nwal_dir = {:?}\n\n[server]\nexport_runtime = \"parallel\"\n",
        catalog_file_url_for_path(catalog_path).unwrap(),
        state_dir,
        wal_dir,
    );
    fs::write(&config_path, contents).unwrap();

    let error = NbdConfig::load(ConfigSource::ExplicitPath(config_path))
        .expect_err("unknown runtime choice should fail");

    assert!(error.to_string().contains("failed to parse config"));
}

#[test]
fn explicit_server_config_loads_queue_sizing() {
    let temp = TempRoot::new();
    let state_dir = temp.path().join("state");
    let catalog_path = temp.path().join("catalog.db");
    let wal_dir = state_dir.join("wal");
    let config_path = temp.path().join("config.toml");
    let contents = format!(
        "[catalog]\nurl = {:?}\n\n[runtime]\nstate_dir = {:?}\nwal_dir = {:?}\n\n[server]\nexport_runtime = \"serial\"\nexport_queue_depth = 7\n\n[server.connection]\nreply_queue_capacity = 3\n",
        catalog_file_url_for_path(catalog_path).unwrap(),
        state_dir,
        wal_dir,
    );
    fs::write(&config_path, contents).unwrap();

    let config = NbdConfig::load(ConfigSource::ExplicitPath(config_path)).unwrap();

    assert_eq!(config.server.export_runtime, ExportRuntimeKind::Serial);
    assert_eq!(config.server.export_queue_depth.get(), 7);
    assert_eq!(config.server.connection.reply_queue_capacity.get(), 3);
}

#[test]
fn zero_queue_sizing_is_rejected() {
    let temp = TempRoot::new();
    let state_dir = temp.path().join("state");
    let catalog_path = temp.path().join("catalog.db");
    let wal_dir = state_dir.join("wal");
    let config_path = temp.path().join("config.toml");
    let contents = format!(
        "[catalog]\nurl = {:?}\n\n[runtime]\nstate_dir = {:?}\nwal_dir = {:?}\n\n[server]\nexport_queue_depth = 0\n\n[server.connection]\nreply_queue_capacity = 1\n",
        catalog_file_url_for_path(catalog_path).unwrap(),
        state_dir,
        wal_dir,
    );
    fs::write(&config_path, contents).unwrap();

    let error = NbdConfig::load(ConfigSource::ExplicitPath(config_path))
        .expect_err("zero export queue depth should fail");

    assert!(error.to_string().contains("failed to parse config"));
}

fn write_config(config_path: &Path, state_dir: &Path, catalog_path: &Path) {
    let wal_dir = state_dir.join("wal");
    let contents = format!(
        "[catalog]\nurl = {:?}\n\n[runtime]\nstate_dir = {:?}\nwal_dir = {:?}\n",
        catalog_file_url_for_path(catalog_path).unwrap(),
        state_dir,
        wal_dir,
    );

    fs::write(config_path, contents).unwrap();
}

struct EnvVarGuard {
    name: &'static str,
    old_value: Option<std::ffi::OsString>,
}

impl EnvVarGuard {
    fn set(name: &'static str, value: &Path) -> Self {
        let old_value = env::var_os(name);
        env::set_var(name, value);

        Self { name, old_value }
    }
}

impl Drop for EnvVarGuard {
    fn drop(&mut self) {
        match &self.old_value {
            Some(value) => env::set_var(self.name, value),
            None => env::remove_var(self.name),
        }
    }
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
            "nbd-config-test-{}-{unique}-{counter}",
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
