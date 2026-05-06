use nbd_config::ConfigFile;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

static NEXT_TEMP_ID: AtomicU64 = AtomicU64::new(1);

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
    assert!(stdout.contains("[server]"));
    assert!(stdout.contains("[logging]"));
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
