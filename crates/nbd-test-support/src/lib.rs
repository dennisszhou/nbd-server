//! Test support for NBD server integration tests.

#![forbid(unsafe_code)]

use nbd_config::{
    CatalogConfig, ConfigError, LoggingConfig, NbdConfig, RuntimeConfig, ServerConfig,
    catalog_file_url_for_path,
};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};
use thiserror::Error;

/// Isolated runtime state for integration tests.
#[derive(Debug)]
pub struct TestRuntime {
    root: TempDir,
    config_path: PathBuf,
    state_dir: PathBuf,
    wal_dir: PathBuf,
    catalog_path: PathBuf,
    catalog_url: String,
}

impl TestRuntime {
    /// Create a new isolated runtime with config and SQLite catalog paths.
    pub fn new() -> Result<Self, TestRuntimeError> {
        let root = TempDir::new("nbd-runtime")?;
        let state_dir = root.path().join("state");
        let blob_dir = state_dir.join("blobs");
        let wal_dir = state_dir.join("wal");
        let config_path = root.path().join("config.toml");
        let catalog_path = root.path().join("catalog.db");
        let catalog_url = catalog_file_url_for_path(&catalog_path)?;

        fs::create_dir_all(&state_dir).map_err(|source| TestRuntimeError::CreateStateDir {
            path: state_dir.clone(),
            source,
        })?;

        write_config(
            &config_path,
            NbdConfig {
                catalog: CatalogConfig {
                    url: catalog_url.clone(),
                },
                runtime: RuntimeConfig {
                    state_dir: state_dir.clone(),
                    blob_dir: blob_dir.clone(),
                    wal_dir: wal_dir.clone(),
                },
                server: ServerConfig::default(),
                logging: LoggingConfig::default(),
            },
        )?;

        Ok(Self {
            root,
            config_path,
            state_dir,
            wal_dir,
            catalog_path,
            catalog_url,
        })
    }

    pub fn root_path(&self) -> &Path {
        self.root.path()
    }

    pub fn config_path(&self) -> &Path {
        &self.config_path
    }

    pub fn state_dir(&self) -> &Path {
        &self.state_dir
    }

    pub fn wal_dir(&self) -> &Path {
        &self.wal_dir
    }

    pub fn catalog_path(&self) -> &Path {
        &self.catalog_path
    }

    pub fn catalog_url(&self) -> &str {
        &self.catalog_url
    }

    /// Replace the fixture config's server policy.
    pub fn write_server_config(&self, server: ServerConfig) -> Result<(), TestRuntimeError> {
        write_config(
            &self.config_path,
            NbdConfig {
                catalog: CatalogConfig {
                    url: self.catalog_url.clone(),
                },
                runtime: RuntimeConfig {
                    state_dir: self.state_dir.clone(),
                    blob_dir: self.state_dir.join("blobs"),
                    wal_dir: self.wal_dir.clone(),
                },
                server,
                logging: LoggingConfig::default(),
            },
        )
    }

    /// Assert that a path is inside this fixture's root.
    pub fn assert_path_inside(&self, path: impl AsRef<Path>) {
        let path = path.as_ref();
        assert!(
            path.starts_with(self.root.path()),
            "path {} is outside test runtime root {}",
            path.display(),
            self.root.path().display()
        );
    }
}

fn write_config(path: &Path, config: NbdConfig) -> Result<(), TestRuntimeError> {
    let contents = toml::to_string_pretty(&config)
        .map_err(|source| TestRuntimeError::SerializeConfig { source })?;

    fs::write(path, contents).map_err(|source| TestRuntimeError::WriteConfig {
        path: path.to_path_buf(),
        source,
    })
}

/// Errors returned while constructing a test runtime.
#[derive(Debug, Error)]
pub enum TestRuntimeError {
    #[error("failed to create temporary runtime root: {source}")]
    CreateTempRoot { source: io::Error },
    #[error("failed to create state dir {}: {source}", path.display())]
    CreateStateDir { path: PathBuf, source: io::Error },
    #[error("failed to write test config {}: {source}", path.display())]
    WriteConfig { path: PathBuf, source: io::Error },
    #[error("failed to serialize test config: {source}")]
    SerializeConfig { source: toml::ser::Error },
    #[error("{source}")]
    Config {
        #[from]
        source: ConfigError,
    },
}

#[derive(Debug)]
struct TempDir {
    path: PathBuf,
}

impl TempDir {
    fn new(prefix: &str) -> Result<Self, TestRuntimeError> {
        let base = std::env::temp_dir();
        let pid = std::process::id();
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or_default();

        for attempt in 0..100 {
            let path = base.join(format!("{prefix}-{pid}-{nanos}-{attempt}"));
            match fs::create_dir(&path) {
                Ok(()) => return Ok(Self { path }),
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
                Err(source) => return Err(TestRuntimeError::CreateTempRoot { source }),
            }
        }

        let source = io::Error::new(
            io::ErrorKind::AlreadyExists,
            "could not allocate unique temporary runtime root",
        );
        Err(TestRuntimeError::CreateTempRoot { source })
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}
