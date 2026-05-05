//! Runtime configuration for the NBD server workspace.

#![forbid(unsafe_code)]

use serde::{Deserialize, Deserializer, Serialize};
use std::env;
use std::error::Error;
use std::fmt;
use std::fs;
use std::io;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};

const CONFIG_DIR: &str = ".nbd";
const CACHE_DIR: &str = ".cache";
const BLOB_DIR: &str = "blobs";
const WAL_DIR: &str = "wal";
const CONFIG_FILE: &str = "config.toml";
const CATALOG_FILE: &str = "catalog.db";
pub const DEFAULT_EXPORT_QUEUE_DEPTH: usize = 128;
pub const DEFAULT_REPLY_QUEUE_CAPACITY: usize = 128;
pub const DEFAULT_LOG_FILE_PATH: &str = "/tmp/nbd/current.log";

/// Complete runtime configuration after startup.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NbdConfig {
    pub catalog: CatalogConfig,
    pub runtime: RuntimeConfig,
    #[serde(default)]
    pub server: ServerConfig,
    #[serde(default)]
    pub logging: LoggingConfig,
}

/// Catalog database configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CatalogConfig {
    pub url: String,
}

/// Local runtime paths used by server-side components.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RuntimeConfig {
    pub state_dir: PathBuf,
    pub blob_dir: PathBuf,
    pub wal_dir: PathBuf,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RuntimeConfigSource {
    state_dir: PathBuf,
    blob_dir: Option<PathBuf>,
    wal_dir: PathBuf,
}

impl<'de> Deserialize<'de> for RuntimeConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let source = RuntimeConfigSource::deserialize(deserializer)?;
        let blob_dir = source
            .blob_dir
            .unwrap_or_else(|| source.state_dir.join(BLOB_DIR));

        Ok(Self {
            state_dir: source.state_dir,
            blob_dir,
            wal_dir: source.wal_dir,
        })
    }
}

/// NBD server runtime policy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServerConfig {
    #[serde(default)]
    pub export_runtime: ExportRuntimeKind,
    #[serde(default = "default_export_queue_depth")]
    pub export_queue_depth: NonZeroUsize,
    #[serde(default)]
    pub connection: ServerConnectionConfig,
}

/// NBD server per-connection runtime policy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServerConnectionConfig {
    #[serde(default = "default_reply_queue_capacity")]
    pub reply_queue_capacity: NonZeroUsize,
}

/// Process logging configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LoggingConfig {
    #[serde(default = "default_log_file_path")]
    pub file_path: PathBuf,
}

/// Export request execution policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum ExportRuntimeKind {
    Serial,
    #[default]
    Concurrent,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            export_runtime: ExportRuntimeKind::default(),
            export_queue_depth: default_export_queue_depth(),
            connection: ServerConnectionConfig::default(),
        }
    }
}

impl Default for ServerConnectionConfig {
    fn default() -> Self {
        Self {
            reply_queue_capacity: default_reply_queue_capacity(),
        }
    }
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            file_path: default_log_file_path(),
        }
    }
}

fn default_export_queue_depth() -> NonZeroUsize {
    NonZeroUsize::new(DEFAULT_EXPORT_QUEUE_DEPTH).expect("default export queue depth is nonzero")
}

fn default_reply_queue_capacity() -> NonZeroUsize {
    NonZeroUsize::new(DEFAULT_REPLY_QUEUE_CAPACITY)
        .expect("default reply queue capacity is nonzero")
}

pub fn default_log_file_path() -> PathBuf {
    PathBuf::from(DEFAULT_LOG_FILE_PATH)
}

/// Where a configuration load should read from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfigSource {
    ExplicitPath(PathBuf),
    DefaultUserPath,
}

/// Errors returned while loading or bootstrapping config.
#[derive(Debug)]
pub enum ConfigError {
    MissingHomeDir,
    NonUtf8Path(PathBuf),
    ReadConfig {
        path: PathBuf,
        source: io::Error,
    },
    WriteConfig {
        path: PathBuf,
        source: io::Error,
    },
    CreateConfigDir {
        path: PathBuf,
        source: io::Error,
    },
    ParseConfig {
        path: PathBuf,
        source: toml::de::Error,
    },
    SerializeDefaultConfig {
        source: toml::ser::Error,
    },
}

impl NbdConfig {
    /// Load configuration from an explicit path or the default user path.
    pub fn load(source: ConfigSource) -> Result<Self, ConfigError> {
        match source {
            ConfigSource::ExplicitPath(path) => load_file(&path),
            ConfigSource::DefaultUserPath => {
                let home = home_dir()?;
                let path = default_config_path_for_home(&home);

                if !path.exists() {
                    bootstrap_default_config(&home, &path)?;
                }

                load_file(&path)
            }
        }
    }

    /// Construct the default config for a specific home directory.
    pub fn default_for_home(home: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let home = home.as_ref();
        let state_dir = default_state_dir_for_home(home);
        let blob_dir = default_blob_dir_for_home(home);
        let wal_dir = default_wal_dir_for_home(home);
        let catalog_path = state_dir.join(CATALOG_FILE);

        Ok(Self {
            catalog: CatalogConfig {
                url: catalog_file_url_for_path(catalog_path)?,
            },
            runtime: RuntimeConfig {
                state_dir,
                blob_dir,
                wal_dir,
            },
            server: ServerConfig::default(),
            logging: LoggingConfig::default(),
        })
    }
}

/// Return the default operator config path for a given home directory.
pub fn default_config_path_for_home(home: impl AsRef<Path>) -> PathBuf {
    default_state_dir_for_home(home).join(CONFIG_FILE)
}

/// Return the default operator state directory for a given home directory.
pub fn default_state_dir_for_home(home: impl AsRef<Path>) -> PathBuf {
    home.as_ref().join(CONFIG_DIR)
}

/// Return the default local blob directory for a generated user config.
pub fn default_blob_dir_for_home(home: impl AsRef<Path>) -> PathBuf {
    home.as_ref().join(CACHE_DIR).join("nbd").join(BLOB_DIR)
}

/// Return the default local WAL directory for a generated user config.
pub fn default_wal_dir_for_home(home: impl AsRef<Path>) -> PathBuf {
    home.as_ref().join(CACHE_DIR).join("nbd").join(WAL_DIR)
}

/// Convert a local SQLite database path into the canonical catalog URL shape.
pub fn catalog_file_url_for_path(path: impl AsRef<Path>) -> Result<String, ConfigError> {
    let path = path.as_ref();
    let path = path
        .to_str()
        .ok_or_else(|| ConfigError::NonUtf8Path(path.to_path_buf()))?;

    Ok(format!("file:{path}"))
}

fn load_file(path: &Path) -> Result<NbdConfig, ConfigError> {
    let contents = fs::read_to_string(path).map_err(|source| ConfigError::ReadConfig {
        path: path.to_path_buf(),
        source,
    })?;

    toml::from_str(&contents).map_err(|source| ConfigError::ParseConfig {
        path: path.to_path_buf(),
        source,
    })
}

fn bootstrap_default_config(home: &Path, path: &Path) -> Result<(), ConfigError> {
    let state_dir = default_state_dir_for_home(home);
    fs::create_dir_all(&state_dir).map_err(|source| ConfigError::CreateConfigDir {
        path: state_dir.clone(),
        source,
    })?;

    let config = NbdConfig::default_for_home(home)?;
    let contents = toml::to_string_pretty(&config)
        .map_err(|source| ConfigError::SerializeDefaultConfig { source })?;

    fs::write(path, contents).map_err(|source| ConfigError::WriteConfig {
        path: path.to_path_buf(),
        source,
    })
}

fn home_dir() -> Result<PathBuf, ConfigError> {
    env::var_os("HOME")
        .map(PathBuf::from)
        .or_else(|| env::var_os("USERPROFILE").map(PathBuf::from))
        .filter(|path| !path.as_os_str().is_empty())
        .ok_or(ConfigError::MissingHomeDir)
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingHomeDir => write!(f, "could not determine user home directory"),
            Self::NonUtf8Path(path) => write!(f, "path is not valid UTF-8: {}", path.display()),
            Self::ReadConfig { path, source } => {
                write!(f, "failed to read config {}: {source}", path.display())
            }
            Self::WriteConfig { path, source } => {
                write!(f, "failed to write config {}: {source}", path.display())
            }
            Self::CreateConfigDir { path, source } => {
                write!(
                    f,
                    "failed to create config directory {}: {source}",
                    path.display()
                )
            }
            Self::ParseConfig { path, source } => {
                write!(f, "failed to parse config {}: {source}", path.display())
            }
            Self::SerializeDefaultConfig { source } => {
                write!(f, "failed to serialize default config: {source}")
            }
        }
    }
}

impl Error for ConfigError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::MissingHomeDir | Self::NonUtf8Path(_) => None,
            Self::ReadConfig { source, .. }
            | Self::WriteConfig { source, .. }
            | Self::CreateConfigDir { source, .. } => Some(source),
            Self::ParseConfig { source, .. } => Some(source),
            Self::SerializeDefaultConfig { source } => Some(source),
        }
    }
}
