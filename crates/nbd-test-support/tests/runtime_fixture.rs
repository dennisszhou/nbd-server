use nbd_config::{ConfigSource, NbdConfig};
use nbd_test_support::TestRuntime;
use std::fs;

#[test]
fn fixture_writes_loadable_explicit_config() {
    let runtime = TestRuntime::new().unwrap();
    let config = NbdConfig::load(ConfigSource::ExplicitPath(
        runtime.config_path().to_path_buf(),
    ))
    .unwrap();

    assert_eq!(config.runtime.state_dir, runtime.state_dir());
    assert_eq!(config.runtime.wal_dir, runtime.wal_dir());
    assert_eq!(config.catalog.url, runtime.catalog_url());
    assert!(runtime.catalog_url().starts_with("file:"));
}

#[test]
fn fixture_paths_stay_under_runtime_root() {
    let runtime = TestRuntime::new().unwrap();

    runtime.assert_path_inside(runtime.config_path());
    runtime.assert_path_inside(runtime.state_dir());
    runtime.assert_path_inside(runtime.wal_dir());
    runtime.assert_path_inside(runtime.catalog_path());
}

#[test]
fn fixture_removes_runtime_root_on_drop() {
    let runtime = TestRuntime::new().unwrap();
    let root_path = runtime.root_path().to_path_buf();
    let catalog_path = runtime.catalog_path().to_path_buf();

    fs::write(&catalog_path, b"temporary sqlite placeholder").unwrap();
    assert!(root_path.exists());
    assert!(catalog_path.exists());

    drop(runtime);

    assert!(!root_path.exists());
}
