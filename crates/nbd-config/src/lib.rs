//! Runtime configuration for the NBD server workspace.

#![forbid(unsafe_code)]

use serde::{Deserialize, Serialize};
use std::env;
use std::error::Error;
use std::fmt;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

const CONFIG_DIR: &str = ".nbd";
const CONFIG_FILE: &str = "config.toml";
const CATALOG_FILE: &str = "catalog.db";

/// Complete runtime configuration after startup.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NbdConfig {
    pub catalog: CatalogConfig,
    pub runtime: RuntimeConfig,
    #[serde(default)]
    pub server: ServerConfig,
}

/// Catalog database configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CatalogConfig {
    pub url: String,
}

/// Local runtime paths used by server-side components.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeConfig {
    pub state_dir: PathBuf,
}

/// NBD server runtime policy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct ServerConfig {
    #[serde(default)]
    pub export_runtime: ExportRuntimeKind,
}

/// Export request execution policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum ExportRuntimeKind {
    #[default]
    Serial,
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
        let state_dir = default_state_dir_for_home(home);
        let catalog_path = state_dir.join(CATALOG_FILE);

        Ok(Self {
            catalog: CatalogConfig {
                url: catalog_file_url_for_path(catalog_path)?,
            },
            runtime: RuntimeConfig { state_dir },
            server: ServerConfig::default(),
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
