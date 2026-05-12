//! Storage-neutral tree ids, chunk refs, and tree snapshot records.

use crate::error::{CatalogError, Result};
use crate::export::{ExportHead, ExportId, ExportLayoutKind, ExportRecord};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;
use uuid::Uuid;

pub const TREE_CHUNK_BYTES: u64 = 32 * 1024 * 1024;
pub const SIMPLE_CHUNK_BYTES: u64 = TREE_CHUNK_BYTES;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NodeId(String);

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct BlobKey(String);

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Timestamp(String);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct WalSeq(u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ChunkIndex(u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TreeNodeKind {
    Internal,
    Leaf,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TreeStorageKind {
    MutableBlob,
    ImmutableBlob,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SimpleChunkRef {
    chunk_index: ChunkIndex,
    blob_key: BlobKey,
    len_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CowChunkRef {
    chunk_index: ChunkIndex,
    blob_key: BlobKey,
    len_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TreeNodeRecord {
    pub id: NodeId,
    pub layout_kind: ExportLayoutKind,
    pub owner_export_id: Option<ExportId>,
    pub kind: TreeNodeKind,
    pub level: u16,
    pub span_start_bytes: u64,
    pub span_len_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TreeEdgeRecord {
    pub parent_node_id: NodeId,
    pub slot: u16,
    pub child_node_id: NodeId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TreeLeafRefRecord {
    pub node_id: NodeId,
    pub storage_kind: TreeStorageKind,
    pub storage_key: BlobKey,
    pub len_bytes: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TreeRecordBatch {
    pub nodes: Vec<TreeNodeRecord>,
    pub edges: Vec<TreeEdgeRecord>,
    pub leaf_refs: Vec<TreeLeafRefRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TreeEdgeLookup {
    pub parent_node_id: NodeId,
    pub slots: Vec<u16>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublishTreeUpdate {
    pub export_id: ExportId,
    pub expected_head: ExportHead,
    pub next_head: ExportHead,
    pub records: TreeRecordBatch,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PublishTreeUpdateOutcome {
    Published(ExportRecord),
    StaleHead(ExportRecord),
}

impl NodeId {
    pub fn new(id: impl Into<String>) -> Result<Self> {
        non_empty_string("node id", id.into()).map(Self)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl BlobKey {
    pub fn new(key: impl Into<String>) -> Result<Self> {
        let key = key.into();
        if key.is_empty() {
            return Err(CatalogError::invalid_field(
                "blob_key",
                "value must not be empty",
            ));
        }
        if key == "." || key == ".." {
            return Err(CatalogError::invalid_field(
                "blob_key",
                "value must not be a relative path component",
            ));
        }
        if key.contains('\0') {
            return Err(CatalogError::invalid_field(
                "blob_key",
                "value must not contain NUL bytes",
            ));
        }
        if key.contains('/') || key.contains('\\') {
            return Err(CatalogError::invalid_field(
                "blob_key",
                "value must be one path component",
            ));
        }
        Ok(Self(key))
    }

    pub fn random() -> Self {
        Self(Uuid::new_v4().to_string())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Timestamp {
    pub fn new(timestamp: impl Into<String>) -> Result<Self> {
        non_empty_string("timestamp", timestamp.into()).map(Self)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl WalSeq {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn zero() -> Self {
        Self(0)
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}

impl ChunkIndex {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}

impl FromStr for TreeNodeKind {
    type Err = CatalogError;

    fn from_str(kind: &str) -> Result<Self> {
        match kind {
            "internal" => Ok(Self::Internal),
            "leaf" => Ok(Self::Leaf),
            kind => Err(CatalogError::invalid_field(
                "kind",
                format!("invalid tree node kind `{kind}`"),
            )),
        }
    }
}

impl FromStr for TreeStorageKind {
    type Err = CatalogError;

    fn from_str(kind: &str) -> Result<Self> {
        match kind {
            "mutable_blob" => Ok(Self::MutableBlob),
            "immutable_blob" => Ok(Self::ImmutableBlob),
            kind => Err(CatalogError::invalid_field(
                "storage_kind",
                format!("invalid tree storage kind `{kind}`"),
            )),
        }
    }
}

impl TreeRecordBatch {
    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty() && self.edges.is_empty() && self.leaf_refs.is_empty()
    }
}

impl PublishTreeUpdateOutcome {
    pub fn record(&self) -> &ExportRecord {
        match self {
            Self::Published(record) | Self::StaleHead(record) => record,
        }
    }

    pub fn into_record(self) -> ExportRecord {
        match self {
            Self::Published(record) | Self::StaleHead(record) => record,
        }
    }
}

impl SimpleChunkRef {
    pub fn new(chunk_index: ChunkIndex, blob_key: BlobKey, len_bytes: u64) -> Result<Self> {
        if len_bytes != SIMPLE_CHUNK_BYTES {
            return Err(CatalogError::invalid_field(
                "len_bytes",
                format!("simple chunks must be exactly {SIMPLE_CHUNK_BYTES} bytes"),
            ));
        }

        Ok(Self {
            chunk_index,
            blob_key,
            len_bytes,
        })
    }

    pub fn chunk_index(&self) -> ChunkIndex {
        self.chunk_index
    }

    pub fn blob_key(&self) -> &BlobKey {
        &self.blob_key
    }

    pub fn len_bytes(&self) -> u64 {
        self.len_bytes
    }
}

impl CowChunkRef {
    pub fn new(chunk_index: ChunkIndex, blob_key: BlobKey, len_bytes: u64) -> Result<Self> {
        if len_bytes != TREE_CHUNK_BYTES {
            return Err(CatalogError::invalid_field(
                "len_bytes",
                format!("cow chunks must be exactly {TREE_CHUNK_BYTES} bytes"),
            ));
        }

        Ok(Self {
            chunk_index,
            blob_key,
            len_bytes,
        })
    }

    pub fn chunk_index(&self) -> ChunkIndex {
        self.chunk_index
    }

    pub fn blob_key(&self) -> &BlobKey {
        &self.blob_key
    }

    pub fn len_bytes(&self) -> u64 {
        self.len_bytes
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl fmt::Display for BlobKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl fmt::Display for Timestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl fmt::Display for WalSeq {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.get())
    }
}

impl fmt::Display for ChunkIndex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.get())
    }
}

impl fmt::Display for TreeNodeKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Internal => f.write_str("internal"),
            Self::Leaf => f.write_str("leaf"),
        }
    }
}

impl fmt::Display for TreeStorageKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MutableBlob => f.write_str("mutable_blob"),
            Self::ImmutableBlob => f.write_str("immutable_blob"),
        }
    }
}

fn non_empty_string(field: &'static str, value: String) -> Result<String> {
    if value.is_empty() {
        return Err(CatalogError::invalid_field(
            field,
            "value must not be empty",
        ));
    }
    if value.contains('\0') {
        return Err(CatalogError::invalid_field(
            field,
            "value must not contain NUL bytes",
        ));
    }
    Ok(value)
}
