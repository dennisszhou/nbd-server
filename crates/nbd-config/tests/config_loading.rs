use nbd_config::{
    BlobStoreConfig, ConfigError, ConfigFile, ConfigKey, ConfigSource, ExportRuntimeKind,
    NbdConfig, catalog_file_url_for_path, default_blob_dir_for_home, default_config_path_for_home,
    default_log_file_path, default_wal_dir_for_home,
};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

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
    assert_eq!(config.server.export_queue_depth.get(), 64);
    assert_eq!(config.blob_store, BlobStoreConfig::Local);
    assert_eq!(config.logging.file_path, default_log_file_path());
}

#[test]
fn explicit_config_load_does_not_bootstrap_missing_file() {
    let temp = TempRoot::new();
    let config_path = temp.path().join("missing").join("config.toml");

    let error = NbdConfig::load(ConfigSource::ExplicitPath(config_path.clone()))
        .expect_err("explicit missing config should fail");

    assert!(error.to_string().contains("failed to read config"));
    assert!(!config_path.exists());
}

#[test]
fn default_config_for_home_uses_absolute_home_paths() {
    let temp = TempRoot::new();
    let fake_home = temp.path().join("home");

    let config = NbdConfig::default_for_home(&fake_home).unwrap();
    let config_path = default_config_path_for_home(&fake_home);

    assert!(!config_path.exists());
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
    assert_eq!(config.server.export_queue_depth.get(), 64);
    assert_eq!(config.blob_store, BlobStoreConfig::Local);
    assert_eq!(config.logging.file_path, default_log_file_path());
}

#[test]
fn config_file_load_or_default_prints_missing_explicit_defaults_without_writing() {
    let temp = TempRoot::new();
    let config_path = temp.path().join("generated").join("config.toml");

    let loaded = ConfigFile::explicit(&config_path)
        .load_or_default()
        .expect("load defaults");

    assert!(!loaded.existed());
    assert_eq!(loaded.path(), config_path);
    assert_eq!(
        loaded.config().runtime.state_dir,
        temp.path().join("generated")
    );
    assert_eq!(
        loaded.config().runtime.blob_dir,
        temp.path().join("generated").join("blobs")
    );
    assert_eq!(
        loaded.config().runtime.wal_dir,
        temp.path().join("generated").join("wal")
    );
    assert_eq!(
        loaded.config().catalog.url,
        catalog_file_url_for_path(temp.path().join("generated").join("catalog.db")).unwrap()
    );
    assert_eq!(loaded.config().blob_store, BlobStoreConfig::Local);
    assert!(!config_path.exists());
}

#[test]
fn config_file_load_or_bootstrap_writes_generated_defaults() {
    let temp = TempRoot::new();
    let config_path = temp.path().join("bootstrapped").join("config.toml");

    let loaded = ConfigFile::explicit(&config_path)
        .load_or_bootstrap()
        .expect("bootstrap config");

    assert!(!loaded.existed());
    assert!(config_path.exists());

    let reloaded = ConfigFile::explicit(&config_path)
        .load()
        .expect("reload config");
    assert!(reloaded.existed());
    assert_eq!(reloaded.config(), loaded.config());
}

#[test]
fn config_file_init_writes_generated_defaults_once() {
    let temp = TempRoot::new();
    let config_path = temp.path().join("initialized").join("config.toml");

    let initialized = ConfigFile::explicit(&config_path)
        .init()
        .expect("initialize config");

    assert_eq!(initialized.path(), config_path);
    assert!(config_path.exists());
    assert_eq!(
        initialized.config().runtime.state_dir,
        temp.path().join("initialized")
    );
    assert_eq!(
        initialized.config().catalog.url,
        catalog_file_url_for_path(temp.path().join("initialized").join("catalog.db")).unwrap()
    );

    let reloaded = ConfigFile::explicit(&config_path)
        .load()
        .expect("reload initialized config");
    assert_eq!(reloaded.config(), initialized.config());
}

#[test]
fn config_file_init_refuses_to_overwrite_existing_config() {
    let temp = TempRoot::new();
    let config_path = temp.path().join("config.toml");
    let config_file = ConfigFile::explicit(&config_path);
    let original = config_file.default_config().expect("default config");
    fs::write(
        &config_path,
        original.to_toml_string().expect("serialize config"),
    )
    .expect("write existing config");

    let error = config_file
        .init()
        .expect_err("init should not overwrite an existing config");

    assert!(matches!(
        error,
        ConfigError::ConfigAlreadyExists { path } if path == config_path
    ));
    let reloaded = ConfigFile::explicit(&config_path)
        .load()
        .expect("reload existing config");
    assert_eq!(reloaded.config(), &original);
}

#[test]
fn generated_default_config_includes_logging_section() {
    let temp = TempRoot::new();
    let config_path = temp.path().join("config.toml");
    let config = ConfigFile::explicit(&config_path)
        .default_config()
        .expect("default config");
    let contents = config.to_toml_string().expect("serialize default config");

    assert_eq!(config.logging.file_path, default_log_file_path());
    assert_eq!(config.blob_store, BlobStoreConfig::Local);
    assert!(contents.contains("[blob_store]"));
    assert!(contents.contains("kind = \"local\""));
    assert!(contents.contains("[logging]"));
    assert!(contents.contains("file_path = \"/tmp/nbd/current.log\""));
}

#[test]
fn config_keys_read_typed_values() {
    let temp = TempRoot::new();
    let config_path = temp.path().join("config.toml");
    let config = ConfigFile::explicit(&config_path)
        .default_config()
        .expect("default config");

    assert_eq!(
        ConfigKey::from_str("catalog.url").unwrap().value(&config),
        catalog_file_url_for_path(temp.path().join("catalog.db")).unwrap()
    );
    assert_eq!(
        ConfigKey::from_str("runtime.blob_dir")
            .unwrap()
            .value(&config),
        temp.path().join("blobs").display().to_string()
    );
    assert_eq!(
        ConfigKey::from_str("blob_store.kind")
            .unwrap()
            .value(&config),
        "local"
    );
    assert_eq!(
        ConfigKey::from_str("server.export_runtime")
            .unwrap()
            .value(&config),
        "concurrent"
    );
    assert_eq!(
        ConfigKey::from_str("server.export_queue_depth")
            .unwrap()
            .value(&config),
        "64"
    );
}

#[test]
fn config_key_rejects_unknown_key_with_supported_keys() {
    let error = ConfigKey::from_str("server.queue_depth").expect_err("unknown key should fail");

    assert!(error.to_string().contains("unknown config key"));
    assert!(error.to_string().contains("server.export_queue_depth"));
}

#[test]
fn config_key_does_not_expose_secret_access_key() {
    let error = ConfigKey::from_str("blob_store.secret_access_key")
        .expect_err("secret key should not be supported");

    assert!(error.to_string().contains("unknown config key"));
    assert!(!ConfigKey::SUPPORTED_KEYS.contains("secret_access_key"));
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
fn explicit_config_loads_s3_blob_store() {
    let temp = TempRoot::new();
    let state_dir = temp.path().join("state");
    let wal_dir = state_dir.join("wal");
    let catalog_path = temp.path().join("catalog.db");
    let config_path = temp.path().join("config.toml");
    let contents = format!(
        concat!(
            "[catalog]\n",
            "url = {:?}\n\n",
            "[runtime]\n",
            "state_dir = {:?}\n",
            "wal_dir = {:?}\n\n",
            "[blob_store]\n",
            "kind = \"s3\"\n",
            "endpoint_url = \"http://rustfs:9000\"\n",
            "region = \"us-east-1\"\n",
            "bucket = \"everstore\"\n",
            "access_key_id = \"rustfsadmin\"\n",
            "secret_access_key = \"rustfsadmin\"\n",
            "force_path_style = true\n",
            "key_prefix = \"v0.1/blobs/\"\n",
            "auto_create_bucket = true\n",
        ),
        catalog_file_url_for_path(catalog_path).unwrap(),
        state_dir,
        wal_dir,
    );
    fs::write(&config_path, contents).unwrap();

    let config = NbdConfig::load(ConfigSource::ExplicitPath(config_path)).unwrap();

    assert_eq!(
        config.blob_store,
        BlobStoreConfig::S3 {
            endpoint_url: Some("http://rustfs:9000".to_owned()),
            region: "us-east-1".to_owned(),
            bucket: "everstore".to_owned(),
            access_key_id: "rustfsadmin".to_owned(),
            secret_access_key: "rustfsadmin".to_owned(),
            force_path_style: true,
            key_prefix: Some("v0.1/blobs/".to_owned()),
            auto_create_bucket: true,
        }
    );
    assert_eq!(
        ConfigKey::from_str("blob_store.bucket")
            .unwrap()
            .value(&config),
        "everstore"
    );
    assert_eq!(
        ConfigKey::from_str("blob_store.key_prefix")
            .unwrap()
            .value(&config),
        "v0.1/blobs/"
    );
    assert_eq!(
        ConfigKey::from_str("blob_store.force_path_style")
            .unwrap()
            .value(&config),
        "true"
    );
    assert_eq!(
        ConfigKey::from_str("blob_store.auto_create_bucket")
            .unwrap()
            .value(&config),
        "true"
    );
    let debug = format!("{:?}", config.blob_store);
    assert!(debug.contains("<redacted>"));
    assert!(!debug.contains("secret_access_key: \"rustfsadmin\""));
}

#[test]
fn explicit_config_rejects_unknown_s3_blob_store_fields() {
    let temp = TempRoot::new();
    let state_dir = temp.path().join("state");
    let wal_dir = state_dir.join("wal");
    let catalog_path = temp.path().join("catalog.db");
    let config_path = temp.path().join("config.toml");
    let contents = format!(
        concat!(
            "[catalog]\n",
            "url = {:?}\n\n",
            "[runtime]\n",
            "state_dir = {:?}\n",
            "wal_dir = {:?}\n\n",
            "[blob_store]\n",
            "kind = \"s3\"\n",
            "region = \"us-east-1\"\n",
            "bucket = \"everstore\"\n",
            "access_key_id = \"rustfsadmin\"\n",
            "secret_access_key = \"rustfsadmin\"\n",
            "unexpected = true\n",
        ),
        catalog_file_url_for_path(catalog_path).unwrap(),
        state_dir,
        wal_dir,
    );
    fs::write(&config_path, contents).unwrap();

    let error = NbdConfig::load(ConfigSource::ExplicitPath(config_path))
        .expect_err("unknown S3 config fields should fail");

    assert!(error.to_string().contains("failed to parse config"));
    assert!(error.to_string().contains("unexpected"));
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
    assert_eq!(config.server.export_queue_depth.get(), 64);
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
    assert_eq!(config.server.export_queue_depth.get(), 64);
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
        "[catalog]\nurl = {:?}\n\n[runtime]\nstate_dir = {:?}\nwal_dir = {:?}\n\n[server]\nexport_runtime = \"serial\"\nexport_queue_depth = 7\n",
        catalog_file_url_for_path(catalog_path).unwrap(),
        state_dir,
        wal_dir,
    );
    fs::write(&config_path, contents).unwrap();

    let config = NbdConfig::load(ConfigSource::ExplicitPath(config_path)).unwrap();

    assert_eq!(config.server.export_runtime, ExportRuntimeKind::Serial);
    assert_eq!(config.server.export_queue_depth.get(), 7);
}

#[test]
fn zero_queue_sizing_is_rejected() {
    let temp = TempRoot::new();
    let state_dir = temp.path().join("state");
    let catalog_path = temp.path().join("catalog.db");
    let wal_dir = state_dir.join("wal");
    let config_path = temp.path().join("config.toml");
    let contents = format!(
        "[catalog]\nurl = {:?}\n\n[runtime]\nstate_dir = {:?}\nwal_dir = {:?}\n\n[server]\nexport_queue_depth = 0\n",
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
