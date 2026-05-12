//! Storage-neutral tree ids, chunk refs, and tree snapshot records.

use crate::error::{CatalogError, Result};
use crate::export::{ExportHead, ExportId, ExportLayoutKind, ExportRecord};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt;
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
pub struct SimpleTreeSnapshot {
    export_id: ExportId,
    size_bytes: u64,
    root_node_id: Option<NodeId>,
    chunks: BTreeMap<ChunkIndex, SimpleChunkRef>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CowTreeSnapshot {
    export_id: ExportId,
    size_bytes: u64,
    root_node_id: Option<NodeId>,
    base_wal_seq: WalSeq,
    chunks: BTreeMap<ChunkIndex, CowChunkRef>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublishCompaction {
    export_id: ExportId,
    expected_base: ExportHead,
    compacted_through: WalSeq,
    chunks: Vec<CowChunkRef>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PublishCompactionOutcome {
    Published(ExportRecord),
    AlreadyCovered(ExportRecord),
    StalePlan(ExportRecord),
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

impl SimpleTreeSnapshot {
    pub fn new(
        export_id: ExportId,
        size_bytes: u64,
        root_node_id: Option<NodeId>,
        chunks: BTreeMap<ChunkIndex, SimpleChunkRef>,
    ) -> Result<Self> {
        validate_non_zero("size_bytes", size_bytes)?;
        for (index, chunk) in &chunks {
            if *index != chunk.chunk_index() {
                return Err(CatalogError::invalid_field(
                    "chunks",
                    format!(
                        "map key {} does not match chunk index {}",
                        index,
                        chunk.chunk_index()
                    ),
                ));
            }
        }

        Ok(Self {
            export_id,
            size_bytes,
            root_node_id,
            chunks,
        })
    }

    pub fn export_id(&self) -> &ExportId {
        &self.export_id
    }

    pub fn size_bytes(&self) -> u64 {
        self.size_bytes
    }

    pub fn root_node_id(&self) -> Option<&NodeId> {
        self.root_node_id.as_ref()
    }

    pub fn chunks(&self) -> &BTreeMap<ChunkIndex, SimpleChunkRef> {
        &self.chunks
    }

    pub fn chunk(&self, chunk_index: ChunkIndex) -> Option<&SimpleChunkRef> {
        self.chunks.get(&chunk_index)
    }
}

impl CowTreeSnapshot {
    pub fn new(
        export_id: ExportId,
        size_bytes: u64,
        root_node_id: Option<NodeId>,
        base_wal_seq: WalSeq,
        chunks: BTreeMap<ChunkIndex, CowChunkRef>,
    ) -> Result<Self> {
        validate_non_zero("size_bytes", size_bytes)?;
        if root_node_id.is_none() && !chunks.is_empty() {
            return Err(CatalogError::invalid_field(
                "root_node_id",
                "cow tree snapshots with chunks must have a root node",
            ));
        }
        for (index, chunk) in &chunks {
            if *index != chunk.chunk_index() {
                return Err(CatalogError::invalid_field(
                    "chunks",
                    format!(
                        "map key {} does not match chunk index {}",
                        index,
                        chunk.chunk_index()
                    ),
                ));
            }
            validate_chunk_in_export(chunk.chunk_index(), size_bytes)?;
        }

        Ok(Self {
            export_id,
            size_bytes,
            root_node_id,
            base_wal_seq,
            chunks,
        })
    }

    pub fn export_id(&self) -> &ExportId {
        &self.export_id
    }

    pub fn size_bytes(&self) -> u64 {
        self.size_bytes
    }

    pub fn root_node_id(&self) -> Option<&NodeId> {
        self.root_node_id.as_ref()
    }

    pub fn base_wal_seq(&self) -> WalSeq {
        self.base_wal_seq
    }

    pub fn chunks(&self) -> &BTreeMap<ChunkIndex, CowChunkRef> {
        &self.chunks
    }

    pub fn chunk(&self, chunk_index: ChunkIndex) -> Option<&CowChunkRef> {
        self.chunks.get(&chunk_index)
    }
}

impl PublishCompaction {
    pub fn new(
        export_id: ExportId,
        expected_base: ExportHead,
        compacted_through: WalSeq,
        chunks: Vec<CowChunkRef>,
    ) -> Result<Self> {
        if expected_base.layout_kind() != ExportLayoutKind::CowImmutableTree {
            return Err(CatalogError::invalid_field(
                "expected_base",
                "compaction publication requires a cow_immutable_tree base",
            ));
        }
        if compacted_through <= expected_base.base_wal_seq() {
            return Err(CatalogError::invalid_field(
                "compacted_through",
                "compaction must advance beyond the expected base checkpoint",
            ));
        }
        if chunks.is_empty() {
            return Err(CatalogError::invalid_field(
                "chunks",
                "compaction publication requires at least one cow chunk",
            ));
        }

        let mut seen = BTreeMap::new();
        for chunk in &chunks {
            validate_chunk_in_export(chunk.chunk_index(), expected_base.size_bytes())?;
            if seen
                .insert(chunk.chunk_index(), chunk.blob_key().clone())
                .is_some()
            {
                return Err(CatalogError::invalid_field(
                    "chunk_index",
                    format!("duplicate cow chunk index {}", chunk.chunk_index()),
                ));
            }
        }

        Ok(Self {
            export_id,
            expected_base,
            compacted_through,
            chunks,
        })
    }

    pub fn export_id(&self) -> &ExportId {
        &self.export_id
    }

    pub fn expected_base(&self) -> &ExportHead {
        &self.expected_base
    }

    pub fn compacted_through(&self) -> WalSeq {
        self.compacted_through
    }

    pub fn chunks(&self) -> &[CowChunkRef] {
        &self.chunks
    }
}

impl PublishCompactionOutcome {
    pub fn record(&self) -> &ExportRecord {
        match self {
            Self::Published(meta) | Self::AlreadyCovered(meta) | Self::StalePlan(meta) => meta,
        }
    }

    pub fn into_record(self) -> ExportRecord {
        match self {
            Self::Published(meta) | Self::AlreadyCovered(meta) | Self::StalePlan(meta) => meta,
        }
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

fn validate_non_zero(field: &'static str, value: u64) -> Result<()> {
    if value == 0 {
        return Err(CatalogError::invalid_field(
            field,
            "value must be greater than zero",
        ));
    }
    Ok(())
}

fn validate_chunk_in_export(chunk_index: ChunkIndex, size_bytes: u64) -> Result<()> {
    let start = chunk_index
        .get()
        .checked_mul(TREE_CHUNK_BYTES)
        .ok_or_else(|| {
            CatalogError::invalid_field(
                "chunk_index",
                format!("chunk {chunk_index} overflows byte offset"),
            )
        })?;
    if start >= size_bytes {
        return Err(CatalogError::invalid_field(
            "chunk_index",
            format!("chunk {chunk_index} starts beyond export size {size_bytes}"),
        ));
    }
    Ok(())
}
