//! Domain model for export catalog operations.

use crate::error::{CatalogError, Result};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt;
use std::str::FromStr;
use uuid::Uuid;

pub const SIMPLE_CHUNK_BYTES: u64 = 32 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ExportId(String);

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ExportName(String);

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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExportState {
    Active,
    Deleted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExportEngineKind {
    Memory,
    SimpleDurable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExportLayoutKind {
    MemoryEmpty,
    SimpleMutableTree,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExportHead {
    layout_kind: ExportLayoutKind,
    root_node_id: Option<NodeId>,
    size_bytes: u64,
    checkpoint_wal_seq: WalSeq,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SimpleChunkRef {
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
pub struct CreateExport {
    name: ExportName,
    size_bytes: u64,
    block_size: u64,
    engine_kind: ExportEngineKind,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeleteExport {
    name: ExportName,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InspectExport {
    name: ExportName,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListExports {
    include_deleted: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExportMeta {
    id: ExportId,
    name: ExportName,
    block_size: u64,
    engine_kind: ExportEngineKind,
    state: ExportState,
    head: ExportHead,
    created_at: Timestamp,
    updated_at: Timestamp,
    deleted_at: Option<Timestamp>,
}

impl ExportId {
    pub fn new(id: impl Into<String>) -> Result<Self> {
        non_empty_string("export id", id.into()).map(Self)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl ExportName {
    pub fn new(name: impl Into<String>) -> Result<Self> {
        let name = name.into();
        if name.is_empty() {
            return Err(CatalogError::invalid_export_name(
                name,
                "name must not be empty",
            ));
        }
        if name.contains('\0') {
            return Err(CatalogError::invalid_export_name(
                name,
                "name must not contain NUL bytes",
            ));
        }
        Ok(Self(name))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
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

impl ExportHead {
    pub fn new(
        layout_kind: ExportLayoutKind,
        root_node_id: Option<NodeId>,
        size_bytes: u64,
        checkpoint_wal_seq: WalSeq,
    ) -> Result<Self> {
        validate_non_zero("size_bytes", size_bytes)?;
        if layout_kind == ExportLayoutKind::MemoryEmpty && root_node_id.is_some() {
            return Err(CatalogError::invalid_field(
                "root_node_id",
                "memory_empty export heads must not have a root node",
            ));
        }

        Ok(Self {
            layout_kind,
            root_node_id,
            size_bytes,
            checkpoint_wal_seq,
        })
    }

    pub fn memory_empty(size_bytes: u64) -> Result<Self> {
        Self::new(
            ExportLayoutKind::MemoryEmpty,
            None,
            size_bytes,
            WalSeq::zero(),
        )
    }

    pub fn simple_mutable_tree(size_bytes: u64) -> Result<Self> {
        Self::new(
            ExportLayoutKind::SimpleMutableTree,
            None,
            size_bytes,
            WalSeq::zero(),
        )
    }

    pub fn layout_kind(&self) -> ExportLayoutKind {
        self.layout_kind
    }

    pub fn root_node_id(&self) -> Option<&NodeId> {
        self.root_node_id.as_ref()
    }

    pub fn size_bytes(&self) -> u64 {
        self.size_bytes
    }

    pub fn checkpoint_wal_seq(&self) -> WalSeq {
        self.checkpoint_wal_seq
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

impl CreateExport {
    pub fn new(
        name: ExportName,
        size_bytes: u64,
        block_size: u64,
        engine_kind: ExportEngineKind,
    ) -> Result<Self> {
        validate_non_zero("size_bytes", size_bytes)?;
        validate_non_zero("block_size", block_size)?;

        Ok(Self {
            name,
            size_bytes,
            block_size,
            engine_kind,
        })
    }

    pub fn name(&self) -> &ExportName {
        &self.name
    }

    pub fn size_bytes(&self) -> u64 {
        self.size_bytes
    }

    pub fn block_size(&self) -> u64 {
        self.block_size
    }

    pub fn engine_kind(&self) -> ExportEngineKind {
        self.engine_kind
    }
}

impl DeleteExport {
    pub fn new(name: ExportName) -> Self {
        Self { name }
    }

    pub fn name(&self) -> &ExportName {
        &self.name
    }
}

impl InspectExport {
    pub fn new(name: ExportName) -> Self {
        Self { name }
    }

    pub fn name(&self) -> &ExportName {
        &self.name
    }
}

impl ListExports {
    pub fn new(include_deleted: bool) -> Self {
        Self { include_deleted }
    }

    pub fn active_only() -> Self {
        Self::new(false)
    }

    pub fn include_deleted() -> Self {
        Self::new(true)
    }

    pub fn includes_deleted(&self) -> bool {
        self.include_deleted
    }
}

impl ExportMeta {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: ExportId,
        name: ExportName,
        block_size: u64,
        engine_kind: ExportEngineKind,
        state: ExportState,
        head: ExportHead,
        created_at: Timestamp,
        updated_at: Timestamp,
        deleted_at: Option<Timestamp>,
    ) -> Result<Self> {
        validate_non_zero("block_size", block_size)?;

        Ok(Self {
            id,
            name,
            block_size,
            engine_kind,
            state,
            head,
            created_at,
            updated_at,
            deleted_at,
        })
    }

    pub fn id(&self) -> &ExportId {
        &self.id
    }

    pub fn name(&self) -> &ExportName {
        &self.name
    }

    pub fn size_bytes(&self) -> u64 {
        self.head.size_bytes()
    }

    pub fn block_size(&self) -> u64 {
        self.block_size
    }

    pub fn engine_kind(&self) -> ExportEngineKind {
        self.engine_kind
    }

    pub fn state(&self) -> ExportState {
        self.state
    }

    pub fn head(&self) -> &ExportHead {
        &self.head
    }

    pub fn created_at(&self) -> &Timestamp {
        &self.created_at
    }

    pub fn updated_at(&self) -> &Timestamp {
        &self.updated_at
    }

    pub fn deleted_at(&self) -> Option<&Timestamp> {
        self.deleted_at.as_ref()
    }
}

impl FromStr for ExportState {
    type Err = CatalogError;

    fn from_str(state: &str) -> Result<Self> {
        match state {
            "active" => Ok(Self::Active),
            "deleted" => Ok(Self::Deleted),
            state => Err(CatalogError::InvalidExportState {
                state: state.to_owned(),
            }),
        }
    }
}

impl FromStr for ExportEngineKind {
    type Err = CatalogError;

    fn from_str(engine_kind: &str) -> Result<Self> {
        match engine_kind {
            "memory" => Ok(Self::Memory),
            "simple_durable" => Ok(Self::SimpleDurable),
            engine_kind => Err(CatalogError::InvalidExportEngineKind {
                engine_kind: engine_kind.to_owned(),
            }),
        }
    }
}

impl FromStr for ExportLayoutKind {
    type Err = CatalogError;

    fn from_str(layout_kind: &str) -> Result<Self> {
        match layout_kind {
            "memory_empty" => Ok(Self::MemoryEmpty),
            "simple_mutable_tree" => Ok(Self::SimpleMutableTree),
            layout_kind => Err(CatalogError::invalid_field(
                "layout_kind",
                format!("invalid export layout kind `{layout_kind}`"),
            )),
        }
    }
}

impl fmt::Display for ExportId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl fmt::Display for ExportName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
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

impl fmt::Display for ExportState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Active => f.write_str("active"),
            Self::Deleted => f.write_str("deleted"),
        }
    }
}

impl fmt::Display for ExportEngineKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Memory => f.write_str("memory"),
            Self::SimpleDurable => f.write_str("simple_durable"),
        }
    }
}

impl fmt::Display for ExportLayoutKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MemoryEmpty => f.write_str("memory_empty"),
            Self::SimpleMutableTree => f.write_str("simple_mutable_tree"),
        }
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
