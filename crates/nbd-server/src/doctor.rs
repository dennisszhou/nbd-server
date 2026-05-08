use nbd_config::{BlobStoreConfig, ConfigFile, NbdConfig};
use nbd_control_plane::{CatalogProvider, CatalogUrl, ListExports, open_catalog};
use serde::Serialize;
use std::fmt;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Serialize)]
pub struct DoctorReport {
    status: DoctorStatus,
    checks: Vec<DoctorCheck>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DoctorStatus {
    Ok,
    Warning,
    Failed,
}

#[derive(Debug, Clone, Serialize)]
pub struct DoctorCheck {
    name: &'static str,
    status: DoctorStatus,
    detail: Option<String>,
    remediation: Option<String>,
}

impl DoctorReport {
    pub fn new(checks: Vec<DoctorCheck>) -> Self {
        let status = if checks
            .iter()
            .any(|check| check.status == DoctorStatus::Failed)
        {
            DoctorStatus::Failed
        } else if checks
            .iter()
            .any(|check| check.status == DoctorStatus::Warning)
        {
            DoctorStatus::Warning
        } else {
            DoctorStatus::Ok
        };

        Self { status, checks }
    }

    pub fn status(&self) -> DoctorStatus {
        self.status
    }

    pub fn checks(&self) -> &[DoctorCheck] {
        &self.checks
    }
}

impl DoctorCheck {
    fn ok(name: &'static str, detail: impl Into<String>) -> Self {
        Self {
            name,
            status: DoctorStatus::Ok,
            detail: Some(detail.into()),
            remediation: None,
        }
    }

    fn warning(
        name: &'static str,
        detail: impl Into<String>,
        remediation: impl Into<String>,
    ) -> Self {
        Self {
            name,
            status: DoctorStatus::Warning,
            detail: Some(detail.into()),
            remediation: Some(remediation.into()),
        }
    }

    fn failed(
        name: &'static str,
        detail: impl Into<String>,
        remediation: impl Into<String>,
    ) -> Self {
        Self {
            name,
            status: DoctorStatus::Failed,
            detail: Some(detail.into()),
            remediation: Some(remediation.into()),
        }
    }

    pub fn name(&self) -> &'static str {
        self.name
    }

    pub fn status(&self) -> DoctorStatus {
        self.status
    }

    pub fn detail(&self) -> Option<&str> {
        self.detail.as_deref()
    }

    pub fn remediation(&self) -> Option<&str> {
        self.remediation.as_deref()
    }
}

impl fmt::Display for DoctorStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Ok => f.write_str("ok"),
            Self::Warning => f.write_str("warning"),
            Self::Failed => f.write_str("failed"),
        }
    }
}

pub async fn check(config_path: Option<PathBuf>) -> DoctorReport {
    let mut checks = Vec::new();
    let Some((config_file, config)) = load_config(config_path, &mut checks) else {
        return DoctorReport::new(checks);
    };

    let Some(catalog_url) = check_catalog_url(&config, &mut checks) else {
        check_configured_paths(&config, &mut checks);
        return DoctorReport::new(checks);
    };

    check_catalog(&catalog_url, &mut checks).await;
    check_configured_paths(&config, &mut checks);
    checks.push(DoctorCheck::ok(
        "config_path",
        config_file.path().display().to_string(),
    ));

    DoctorReport::new(checks)
}

fn load_config(
    config_path: Option<PathBuf>,
    checks: &mut Vec<DoctorCheck>,
) -> Option<(ConfigFile, NbdConfig)> {
    let explicit = config_path.is_some();
    let config_file = match config_path {
        Some(path) => ConfigFile::explicit(path),
        None => match ConfigFile::local() {
            Ok(config_file) => config_file,
            Err(error) => {
                checks.push(DoctorCheck::failed(
                    "config",
                    error.to_string(),
                    "set HOME or pass --config <path>",
                ));
                return None;
            }
        },
    };

    if explicit {
        match config_file.load() {
            Ok(loaded) => {
                checks.push(DoctorCheck::ok(
                    "config",
                    format!("loaded {}", loaded.path().display()),
                ));
                Some((config_file, loaded.into_config()))
            }
            Err(error) => {
                checks.push(DoctorCheck::failed(
                    "config",
                    error.to_string(),
                    "create the config or pass a valid --config path",
                ));
                None
            }
        }
    } else {
        match config_file.load_or_default() {
            Ok(loaded) => {
                if loaded.existed() {
                    checks.push(DoctorCheck::ok(
                        "config",
                        format!("loaded {}", loaded.path().display()),
                    ));
                } else {
                    checks.push(DoctorCheck::warning(
                        "config",
                        format!("{} is missing", loaded.path().display()),
                        "run `nbd-server config init` or let `nbd-server serve` bootstrap it",
                    ));
                }
                Some((config_file, loaded.into_config()))
            }
            Err(error) => {
                checks.push(DoctorCheck::failed(
                    "config",
                    error.to_string(),
                    "fix the config path or default config template",
                ));
                None
            }
        }
    }
}

fn check_catalog_url(config: &NbdConfig, checks: &mut Vec<DoctorCheck>) -> Option<CatalogUrl> {
    match CatalogUrl::parse(&config.catalog.url) {
        Ok(url) => {
            checks.push(DoctorCheck::ok("catalog_url", url.as_str().to_owned()));
            if url.provider() == CatalogProvider::Sqlite {
                checks.push(DoctorCheck::ok("catalog_provider", "sqlite"));
                Some(url)
            } else {
                checks.push(DoctorCheck::failed(
                    "catalog_provider",
                    format!("{:?}", url.provider()),
                    "use a file: SQLite catalog URL; Postgres is not implemented",
                ));
                None
            }
        }
        Err(error) => {
            checks.push(DoctorCheck::failed(
                "catalog_url",
                error.to_string(),
                "set catalog.url to a valid file: URL",
            ));
            None
        }
    }
}

async fn check_catalog(url: &CatalogUrl, checks: &mut Vec<DoctorCheck>) {
    let Ok(path) = url.sqlite_path() else {
        return;
    };

    if !path.exists() {
        checks.push(DoctorCheck::failed(
            "catalog_file",
            format!("{} is missing", path.display()),
            "create and migrate the SQLite catalog",
        ));
        return;
    }
    if !path.is_file() {
        checks.push(DoctorCheck::failed(
            "catalog_file",
            format!("{} is not a regular file", path.display()),
            "set catalog.url to a SQLite database file",
        ));
        return;
    }
    checks.push(DoctorCheck::ok("catalog_file", path.display().to_string()));

    match open_catalog(url).await {
        Ok(handle) => match handle
            .export_catalog()
            .list_exports(ListExports::new(false))
            .await
        {
            Ok(_) => checks.push(DoctorCheck::ok("catalog_schema", "ready")),
            Err(error) => checks.push(DoctorCheck::failed(
                "catalog_schema",
                error.to_string(),
                "apply the catalog migrations",
            )),
        },
        Err(error) => checks.push(DoctorCheck::failed(
            "catalog_open",
            error.to_string(),
            "check catalog.url and SQLite file permissions",
        )),
    }
}

fn check_configured_paths(config: &NbdConfig, checks: &mut Vec<DoctorCheck>) {
    check_directory_location("runtime.state_dir", &config.runtime.state_dir, checks);
    check_directory_location("runtime.blob_dir", &config.runtime.blob_dir, checks);
    check_directory_location("runtime.wal_dir", &config.runtime.wal_dir, checks);
    check_file_location("logging.file_path", &config.logging.file_path, checks);
    check_blob_store(&config.blob_store, checks);
}

fn check_directory_location(name: &'static str, path: &Path, checks: &mut Vec<DoctorCheck>) {
    if path.exists() {
        if path.is_dir() {
            checks.push(DoctorCheck::ok(name, path.display().to_string()));
        } else {
            checks.push(DoctorCheck::failed(
                name,
                format!("{} is not a directory", path.display()),
                "choose a directory path",
            ));
        }
        return;
    }

    if parent_is_directory(path) {
        checks.push(DoctorCheck::ok(
            name,
            format!("{} is missing; parent exists", path.display()),
        ));
    } else {
        checks.push(DoctorCheck::failed(
            name,
            format!("{} is missing and parent is not available", path.display()),
            "create the parent directory or choose another path",
        ));
    }
}

fn check_file_location(name: &'static str, path: &Path, checks: &mut Vec<DoctorCheck>) {
    if path.exists() {
        if path.is_file() {
            checks.push(DoctorCheck::ok(name, path.display().to_string()));
        } else {
            checks.push(DoctorCheck::failed(
                name,
                format!("{} is not a regular file", path.display()),
                "choose a file path",
            ));
        }
        return;
    }

    if parent_is_directory(path) || path.parent().is_some_and(parent_is_creatable) {
        checks.push(DoctorCheck::ok(
            name,
            format!("{} is missing; parent can be created", path.display()),
        ));
    } else {
        checks.push(DoctorCheck::failed(
            name,
            format!("{} parent is not available", path.display()),
            "create the parent directory or choose another path",
        ));
    }
}

fn check_blob_store(blob_store: &BlobStoreConfig, checks: &mut Vec<DoctorCheck>) {
    match blob_store {
        BlobStoreConfig::Local => checks.push(DoctorCheck::ok("blob_store", "local")),
        BlobStoreConfig::S3 { bucket, .. } => checks.push(DoctorCheck::ok(
            "blob_store",
            format!("s3 bucket {bucket}; network reachability not checked"),
        )),
    }
}

fn parent_is_directory(path: &Path) -> bool {
    path.parent().is_none_or(Path::is_dir)
}

fn parent_is_creatable(path: &Path) -> bool {
    path.parent().is_some_and(Path::is_dir)
}
