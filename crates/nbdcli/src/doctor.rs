use nbd_config::{ConfigFile, NbdConfig};
use nbd_control_plane::{CatalogDoctorCheck, CatalogDoctorStatus, CatalogUrl, doctor_catalog};
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

    fn from_catalog(status: CatalogDoctorStatus) -> Self {
        match status {
            CatalogDoctorStatus::Ok => Self::Ok,
            CatalogDoctorStatus::Warning => Self::Warning,
            CatalogDoctorStatus::Failed => Self::Failed,
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

    fn from_catalog(check: CatalogDoctorCheck) -> Self {
        Self {
            name: check.name(),
            status: DoctorStatus::from_catalog(check.status()),
            detail: check.detail().map(ToOwned::to_owned),
            remediation: check.remediation().map(ToOwned::to_owned),
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
            Some(url)
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
    checks.extend(
        doctor_catalog(url)
            .await
            .into_iter()
            .map(DoctorCheck::from_catalog),
    );
}
