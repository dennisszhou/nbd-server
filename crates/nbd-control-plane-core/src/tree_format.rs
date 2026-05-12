//! Stored tree format ids and pure tree format specs.

use crate::error::{CatalogError, Result};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TreeFormat {
    #[default]
    Bounded32V1,
}

impl FromStr for TreeFormat {
    type Err = CatalogError;

    fn from_str(format: &str) -> Result<Self> {
        match format {
            "bounded_32_v1" => Ok(Self::Bounded32V1),
            format => Err(CatalogError::invalid_field(
                "tree_format",
                format!("invalid tree format `{format}`"),
            )),
        }
    }
}

impl fmt::Display for TreeFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Bounded32V1 => f.write_str("bounded_32_v1"),
        }
    }
}
