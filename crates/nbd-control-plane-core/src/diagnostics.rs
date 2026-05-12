//! Storage-neutral catalog diagnostic records.

use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CatalogDoctorStatus {
    Ok,
    Warning,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogDoctorCheck {
    name: &'static str,
    status: CatalogDoctorStatus,
    detail: Option<String>,
    remediation: Option<String>,
}

impl CatalogDoctorCheck {
    pub fn ok(name: &'static str, detail: impl Into<String>) -> Self {
        Self {
            name,
            status: CatalogDoctorStatus::Ok,
            detail: Some(detail.into()),
            remediation: None,
        }
    }

    pub fn warning(
        name: &'static str,
        detail: impl Into<String>,
        remediation: impl Into<String>,
    ) -> Self {
        Self {
            name,
            status: CatalogDoctorStatus::Warning,
            detail: Some(detail.into()),
            remediation: Some(remediation.into()),
        }
    }

    pub fn failed(
        name: &'static str,
        detail: impl Into<String>,
        remediation: impl Into<String>,
    ) -> Self {
        Self {
            name,
            status: CatalogDoctorStatus::Failed,
            detail: Some(detail.into()),
            remediation: Some(remediation.into()),
        }
    }

    pub fn name(&self) -> &'static str {
        self.name
    }

    pub fn status(&self) -> CatalogDoctorStatus {
        self.status
    }

    pub fn detail(&self) -> Option<&str> {
        self.detail.as_deref()
    }

    pub fn remediation(&self) -> Option<&str> {
        self.remediation.as_deref()
    }
}

impl CatalogDoctorStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Warning => "warning",
            Self::Failed => "failed",
        }
    }
}

impl fmt::Display for CatalogDoctorStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}
