//! Test support for NBD server integration tests.

#![forbid(unsafe_code)]

use nbd_config::{sqlite_url_for_path, CatalogConfig, ConfigError, NbdConfig, RuntimeConfig};
use std::error::Error;
use std::fmt;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

/// Isolated runtime state for integration tests.
#[derive(Debug)]
pub struct TestRuntime {
    root: TempDir,
    config_path: PathBuf,
    state_dir: PathBuf,
    catalog_path: PathBuf,
    catalog_url: String,
}

impl TestRuntime {
    /// Create a new isolated runtime with config and SQLite catalog paths.
    pub fn new() -> Result<Self, TestRuntimeError> {
        let root = TempDir::new("nbd-runtime")?;
        let state_dir = root.path().join("state");
        let config_path = root.path().join("config.toml");
        let catalog_path = root.path().join("catalog.db");
        let catalog_url = sqlite_url_for_path(&catalog_path)?;

        fs::create_dir_all(&state_dir).map_err(|source| TestRuntimeError::CreateStateDir {
            path: state_dir.clone(),
            source,
        })?;

        let config = NbdConfig {
            catalog: CatalogConfig {
                url: catalog_url.clone(),
            },
            runtime: RuntimeConfig {
                state_dir: state_dir.clone(),
            },
        };
        let contents = toml::to_string_pretty(&config)
            .map_err(|source| TestRuntimeError::SerializeConfig { source })?;

        fs::write(&config_path, contents).map_err(|source| TestRuntimeError::WriteConfig {
            path: config_path.clone(),
            source,
        })?;

        Ok(Self {
            root,
            config_path,
            state_dir,
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

    pub fn catalog_path(&self) -> &Path {
        &self.catalog_path
    }

    pub fn catalog_url(&self) -> &str {
        &self.catalog_url
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

/// Errors returned while constructing a test runtime.
#[derive(Debug)]
pub enum TestRuntimeError {
    CreateTempRoot { source: io::Error },
    CreateStateDir { path: PathBuf, source: io::Error },
    WriteConfig { path: PathBuf, source: io::Error },
    SerializeConfig { source: toml::ser::Error },
    Config { source: ConfigError },
}

impl From<ConfigError> for TestRuntimeError {
    fn from(source: ConfigError) -> Self {
        Self::Config { source }
    }
}

impl fmt::Display for TestRuntimeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CreateTempRoot { source } => {
                write!(f, "failed to create temporary runtime root: {source}")
            }
            Self::CreateStateDir { path, source } => {
                write!(f, "failed to create state dir {}: {source}", path.display())
            }
            Self::WriteConfig { path, source } => {
                write!(
                    f,
                    "failed to write test config {}: {source}",
                    path.display()
                )
            }
            Self::SerializeConfig { source } => {
                write!(f, "failed to serialize test config: {source}")
            }
            Self::Config { source } => write!(f, "{source}"),
        }
    }
}

impl Error for TestRuntimeError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::CreateTempRoot { source }
            | Self::CreateStateDir { source, .. }
            | Self::WriteConfig { source, .. } => Some(source),
            Self::SerializeConfig { source } => Some(source),
            Self::Config { source } => Some(source),
        }
    }
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
