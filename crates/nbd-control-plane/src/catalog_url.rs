//! Catalog URL parsing and provider selection.

use crate::error::{CatalogError, Result};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::path::{Path, PathBuf};
use std::str::FromStr;

/// Runtime catalog provider selected from `catalog.url`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CatalogProvider {
    Sqlite,
    Postgres,
}

/// Parsed runtime catalog URL.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "provider", rename_all = "snake_case")]
pub enum CatalogUrl {
    Sqlite { raw: String, path: PathBuf },
    Postgres { raw: String },
}

impl CatalogUrl {
    pub fn parse(raw: impl AsRef<str>) -> Result<Self> {
        raw.as_ref().parse()
    }

    pub fn provider(&self) -> CatalogProvider {
        match self {
            Self::Sqlite { .. } => CatalogProvider::Sqlite,
            Self::Postgres { .. } => CatalogProvider::Postgres,
        }
    }

    pub fn as_str(&self) -> &str {
        match self {
            Self::Sqlite { raw, .. } | Self::Postgres { raw } => raw,
        }
    }

    pub fn sqlite_path(&self) -> Result<&Path> {
        match self {
            Self::Sqlite { path, .. } => Ok(path),
            Self::Postgres { raw } => Err(CatalogError::unsupported_catalog_provider(
                raw,
                "expected a file: SQLite catalog URL",
            )),
        }
    }
}

impl FromStr for CatalogUrl {
    type Err = CatalogError;

    fn from_str(raw: &str) -> Result<Self> {
        let (scheme, rest) = raw
            .split_once(':')
            .ok_or_else(|| CatalogError::invalid_catalog_url(raw, "missing URL scheme"))?;

        match scheme {
            "file" => {
                if rest.is_empty() {
                    return Err(CatalogError::invalid_catalog_url(
                        raw,
                        "file: URL must include a database path",
                    ));
                }

                Ok(Self::Sqlite {
                    raw: raw.to_owned(),
                    path: PathBuf::from(rest),
                })
            }
            "postgres" | "postgresql" => Ok(Self::Postgres {
                raw: raw.to_owned(),
            }),
            scheme => Err(CatalogError::invalid_catalog_url(
                raw,
                format!("unsupported catalog URL scheme `{scheme}`"),
            )),
        }
    }
}

impl fmt::Display for CatalogUrl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}
