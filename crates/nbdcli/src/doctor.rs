use nbd_config::{ConfigFile, NbdConfig};
use nbd_control_plane::{CatalogProvider, CatalogUrl, ListExports, open_catalog};
use std::fmt;
use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct DoctorReport {
    status: DoctorStatus,
    checks: Vec<DoctorCheck>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DoctorStatus {
    Ok,
    Warning,
    Failed,
}

#[derive(Debug, Clone)]
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

impl DoctorStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Warning => "warning",
            Self::Failed => "failed",
        }
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
        f.write_str(self.as_str())
    }
}

pub async fn check(config_path: Option<PathBuf>) -> DoctorReport {
    let mut checks = Vec::new();
    let Some(config) = load_config(config_path, &mut checks) else {
        return DoctorReport::new(checks);
    };

    let Some(catalog_url) = check_catalog_url(&config, &mut checks) else {
        return DoctorReport::new(checks);
    };

    check_catalog(&catalog_url, &mut checks).await;
    DoctorReport::new(checks)
}

fn load_config(config_path: Option<PathBuf>, checks: &mut Vec<DoctorCheck>) -> Option<NbdConfig> {
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

    match config_file.load() {
        Ok(loaded) => {
            checks.push(DoctorCheck::ok(
                "config",
                format!("loaded {}", loaded.path().display()),
            ));
            Some(loaded.into_config())
        }
        Err(error) => {
            checks.push(DoctorCheck::failed(
                "config",
                error.to_string(),
                "create the config with `nbd-server config init` or pass --config <path>",
            ));
            None
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
