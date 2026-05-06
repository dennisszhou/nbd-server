//! Runtime configuration for the NBD server workspace.

#![forbid(unsafe_code)]

use serde::{Deserialize, Deserializer, Serialize};
use std::env;
use std::fmt;
use std::fs;
use std::io;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::OnceLock;
use thiserror::Error;

const CONFIG_DIR: &str = ".nbd";
const CACHE_DIR: &str = ".cache";
const BLOB_DIR: &str = "blobs";
const WAL_DIR: &str = "wal";
const CONFIG_FILE: &str = "config.toml";
const CATALOG_FILE: &str = "catalog.db";
const DEFAULT_CONFIG_TEMPLATE: &str = include_str!("../default-config.toml");

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

impl ExportRuntimeKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Serial => "serial",
            Self::Concurrent => "concurrent",
        }
    }
}

impl fmt::Display for ExportRuntimeKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
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
    template_config().server.export_queue_depth
}

fn default_reply_queue_capacity() -> NonZeroUsize {
    template_config().server.connection.reply_queue_capacity
}

pub fn default_log_file_path() -> PathBuf {
    template_config().logging.file_path.clone()
}

/// Where a configuration load should read from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfigSource {
    ExplicitPath(PathBuf),
    DefaultUserPath,
}

/// Concrete config file path plus the defaults used when that file is absent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigFile {
    path: PathBuf,
    defaults: ConfigDefaults,
}

/// Path values substituted into the compiled default config template.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigDefaults {
    catalog_path: PathBuf,
    state_dir: PathBuf,
    blob_dir: PathBuf,
    wal_dir: PathBuf,
}

/// Config loaded from disk, or generated from defaults when `existed` is false.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoadedConfig {
    path: PathBuf,
    config: NbdConfig,
    existed: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigKey {
    CatalogUrl,
    RuntimeStateDir,
    RuntimeBlobDir,
    RuntimeWalDir,
    ServerExportRuntime,
    ServerExportQueueDepth,
    ServerConnectionReplyQueueCapacity,
    LoggingFilePath,
}

/// Errors returned while loading or bootstrapping config.
#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("could not determine user home directory")]
    MissingHomeDir,
    #[error("path is not valid UTF-8: {0}")]
    NonUtf8Path(PathBuf),
    #[error("failed to read config {}: {source}", path.display())]
    ReadConfig { path: PathBuf, source: io::Error },
    #[error("failed to write config {}: {source}", path.display())]
    WriteConfig { path: PathBuf, source: io::Error },
    #[error("failed to create config directory {}: {source}", path.display())]
    CreateConfigDir { path: PathBuf, source: io::Error },
    #[error("failed to parse config {}: {source}", path.display())]
    ParseConfig {
        path: PathBuf,
        source: toml::de::Error,
    },
    #[error("failed to serialize config: {source}")]
    SerializeConfig { source: toml::ser::Error },
    #[error(
        "unknown config key `{key}`; supported keys: {}",
        ConfigKey::SUPPORTED_KEYS
    )]
    InvalidConfigKey { key: String },
}

impl NbdConfig {
    /// Load configuration from an explicit path or the default user path.
    pub fn load(source: ConfigSource) -> Result<Self, ConfigError> {
        match source {
            ConfigSource::ExplicitPath(path) => ConfigFile::explicit(path).load(),
            ConfigSource::DefaultUserPath => ConfigFile::local()?.load_or_bootstrap(),
        }
        .map(LoadedConfig::into_config)
    }

    /// Construct the default config for a specific home directory.
    pub fn default_for_home(home: impl AsRef<Path>) -> Result<Self, ConfigError> {
        Self::default_for_paths(ConfigDefaults::for_home(home))
    }

    /// Construct the default config from explicit path defaults.
    pub fn default_for_paths(defaults: ConfigDefaults) -> Result<Self, ConfigError> {
        let mut config = template_config().clone();
        config.catalog.url = catalog_file_url_for_path(defaults.catalog_path)?;
        config.runtime.state_dir = defaults.state_dir;
        config.runtime.blob_dir = defaults.blob_dir;
        config.runtime.wal_dir = defaults.wal_dir;
        Ok(config)
    }

    pub fn to_toml_string(&self) -> Result<String, ConfigError> {
        toml::to_string_pretty(self).map_err(|source| ConfigError::SerializeConfig { source })
    }
}

impl ConfigFile {
    pub fn local() -> Result<Self, ConfigError> {
        let home = home_dir()?;
        Ok(Self::for_home(home))
    }

    pub fn explicit(path: impl Into<PathBuf>) -> Self {
        let path = path.into();
        let defaults = ConfigDefaults::for_config_path(&path);
        Self { path, defaults }
    }

    fn for_home(home: impl AsRef<Path>) -> Self {
        let home = home.as_ref();
        Self {
            path: default_config_path_for_home(home),
            defaults: ConfigDefaults::for_home(home),
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn defaults(&self) -> &ConfigDefaults {
        &self.defaults
    }

    pub fn load(&self) -> Result<LoadedConfig, ConfigError> {
        let config = load_file(&self.path)?;
        Ok(LoadedConfig {
            path: self.path.clone(),
            config,
            existed: true,
        })
    }

    pub fn load_or_default(&self) -> Result<LoadedConfig, ConfigError> {
        if self.path.exists() {
            return self.load();
        }

        Ok(LoadedConfig {
            path: self.path.clone(),
            config: self.default_config()?,
            existed: false,
        })
    }

    pub fn load_or_bootstrap(&self) -> Result<LoadedConfig, ConfigError> {
        if self.path.exists() {
            return self.load();
        }

        let config = self.default_config()?;
        if let Some(parent) = non_empty_parent(&self.path) {
            fs::create_dir_all(parent).map_err(|source| ConfigError::CreateConfigDir {
                path: parent.to_path_buf(),
                source,
            })?;
        }
        fs::write(&self.path, config.to_toml_string()?).map_err(|source| {
            ConfigError::WriteConfig {
                path: self.path.clone(),
                source,
            }
        })?;

        Ok(LoadedConfig {
            path: self.path.clone(),
            config,
            existed: false,
        })
    }

    pub fn default_config(&self) -> Result<NbdConfig, ConfigError> {
        NbdConfig::default_for_paths(self.defaults.clone())
    }
}

impl ConfigDefaults {
    pub fn new(
        catalog_path: impl Into<PathBuf>,
        state_dir: impl Into<PathBuf>,
        blob_dir: impl Into<PathBuf>,
        wal_dir: impl Into<PathBuf>,
    ) -> Self {
        Self {
            catalog_path: catalog_path.into(),
            state_dir: state_dir.into(),
            blob_dir: blob_dir.into(),
            wal_dir: wal_dir.into(),
        }
    }

    pub fn for_home(home: impl AsRef<Path>) -> Self {
        let home = home.as_ref();
        let state_dir = default_state_dir_for_home(home);
        Self {
            catalog_path: state_dir.join(CATALOG_FILE),
            state_dir,
            blob_dir: default_blob_dir_for_home(home),
            wal_dir: default_wal_dir_for_home(home),
        }
    }

    pub fn for_config_path(config_path: impl AsRef<Path>) -> Self {
        let parent = config_parent(config_path.as_ref());
        Self {
            catalog_path: parent.join(CATALOG_FILE),
            state_dir: parent.clone(),
            blob_dir: parent.join(BLOB_DIR),
            wal_dir: parent.join(WAL_DIR),
        }
    }

    pub fn catalog_path(&self) -> &Path {
        &self.catalog_path
    }

    pub fn state_dir(&self) -> &Path {
        &self.state_dir
    }

    pub fn blob_dir(&self) -> &Path {
        &self.blob_dir
    }

    pub fn wal_dir(&self) -> &Path {
        &self.wal_dir
    }
}

impl LoadedConfig {
    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn config(&self) -> &NbdConfig {
        &self.config
    }

    pub fn existed(&self) -> bool {
        self.existed
    }

    pub fn into_config(self) -> NbdConfig {
        self.config
    }
}

impl ConfigKey {
    pub const SUPPORTED_KEYS: &'static str = "catalog.url, runtime.state_dir, runtime.blob_dir, runtime.wal_dir, server.export_runtime, server.export_queue_depth, server.connection.reply_queue_capacity, logging.file_path";

    pub fn value(self, config: &NbdConfig) -> String {
        match self {
            Self::CatalogUrl => config.catalog.url.clone(),
            Self::RuntimeStateDir => config.runtime.state_dir.display().to_string(),
            Self::RuntimeBlobDir => config.runtime.blob_dir.display().to_string(),
            Self::RuntimeWalDir => config.runtime.wal_dir.display().to_string(),
            Self::ServerExportRuntime => config.server.export_runtime.to_string(),
            Self::ServerExportQueueDepth => config.server.export_queue_depth.get().to_string(),
            Self::ServerConnectionReplyQueueCapacity => config
                .server
                .connection
                .reply_queue_capacity
                .get()
                .to_string(),
            Self::LoggingFilePath => config.logging.file_path.display().to_string(),
        }
    }
}

impl FromStr for ConfigKey {
    type Err = ConfigError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "catalog.url" => Ok(Self::CatalogUrl),
            "runtime.state_dir" => Ok(Self::RuntimeStateDir),
            "runtime.blob_dir" => Ok(Self::RuntimeBlobDir),
            "runtime.wal_dir" => Ok(Self::RuntimeWalDir),
            "server.export_runtime" => Ok(Self::ServerExportRuntime),
            "server.export_queue_depth" => Ok(Self::ServerExportQueueDepth),
            "server.connection.reply_queue_capacity" => {
                Ok(Self::ServerConnectionReplyQueueCapacity)
            }
            "logging.file_path" => Ok(Self::LoggingFilePath),
            _ => Err(ConfigError::InvalidConfigKey {
                key: value.to_owned(),
            }),
        }
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

fn template_config() -> &'static NbdConfig {
    static TEMPLATE_CONFIG: OnceLock<NbdConfig> = OnceLock::new();
    TEMPLATE_CONFIG.get_or_init(|| {
        toml::from_str(DEFAULT_CONFIG_TEMPLATE)
            .expect("compiled default config template should parse")
    })
}

fn config_parent(path: &Path) -> PathBuf {
    non_empty_parent(path)
        .unwrap_or_else(|| Path::new("."))
        .to_path_buf()
}

fn non_empty_parent(path: &Path) -> Option<&Path> {
    path.parent()
        .filter(|parent| !parent.as_os_str().is_empty())
}

fn home_dir() -> Result<PathBuf, ConfigError> {
    env::var_os("HOME")
        .map(PathBuf::from)
        .or_else(|| env::var_os("USERPROFILE").map(PathBuf::from))
        .filter(|path| !path.as_os_str().is_empty())
        .ok_or(ConfigError::MissingHomeDir)
}
