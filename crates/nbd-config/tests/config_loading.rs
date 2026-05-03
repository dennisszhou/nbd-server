use nbd_config::{
    catalog_file_url_for_path, default_config_path_for_home, default_state_dir_for_home,
    ConfigSource, ExportEngineKind, ExportRuntimeKind, NbdConfig,
};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

static HOME_ENV_LOCK: Mutex<()> = Mutex::new(());

#[test]
fn explicit_config_loads_from_requested_path() {
    let temp = TempRoot::new();
    let state_dir = temp.path().join("state");
    let catalog_path = temp.path().join("catalog.db");
    let config_path = temp.path().join("config.toml");

    write_config(&config_path, &state_dir, &catalog_path);

    let config = NbdConfig::load(ConfigSource::ExplicitPath(config_path)).unwrap();

    assert_eq!(config.runtime.state_dir, state_dir);
    assert_eq!(
        config.catalog.url,
        catalog_file_url_for_path(catalog_path).unwrap()
    );
    assert_eq!(config.server.export_runtime, ExportRuntimeKind::Serial);
    assert_eq!(config.server.export_engine, ExportEngineKind::Memory);
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
    assert!(config.runtime.state_dir.is_absolute());
    assert_eq!(
        config.catalog.url,
        catalog_file_url_for_path(fake_home.join(".nbd").join("catalog.db")).unwrap()
    );
    assert!(config.catalog.url.starts_with("file:"));
    assert_eq!(config.server.export_runtime, ExportRuntimeKind::Serial);
    assert_eq!(config.server.export_engine, ExportEngineKind::Memory);

    let loaded = NbdConfig::load(ConfigSource::ExplicitPath(config_path)).unwrap();
    assert_eq!(loaded, config);
    drop(old_home);
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
fn explicit_server_config_loads_backend_choices() {
    let temp = TempRoot::new();
    let state_dir = temp.path().join("state");
    let catalog_path = temp.path().join("catalog.db");
    let config_path = temp.path().join("config.toml");
    let contents = format!(
        "[catalog]\nurl = {:?}\n\n[runtime]\nstate_dir = {:?}\n\n[server]\nexport_runtime = \"serial\"\nexport_engine = \"memory\"\n",
        catalog_file_url_for_path(catalog_path).unwrap(),
        state_dir
    );
    fs::write(&config_path, contents).unwrap();

    let config = NbdConfig::load(ConfigSource::ExplicitPath(config_path)).unwrap();

    assert_eq!(config.server.export_runtime, ExportRuntimeKind::Serial);
    assert_eq!(config.server.export_engine, ExportEngineKind::Memory);
}

fn write_config(config_path: &Path, state_dir: &Path, catalog_path: &Path) {
    let contents = format!(
        "[catalog]\nurl = {:?}\n\n[runtime]\nstate_dir = {:?}\n",
        catalog_file_url_for_path(catalog_path).unwrap(),
        state_dir
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
        let path = env::temp_dir().join(format!("nbd-config-test-{}-{unique}", std::process::id()));

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
