//! Export identity, descriptor, head, and lifecycle types.

use crate::error::{CatalogError, Result};
use crate::tree::{NodeId, Timestamp, WalSeq};
use crate::tree_format::TreeFormat;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ExportId(String);

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ExportName(String);

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
    WalDurable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExportLayoutKind {
    MemoryEmpty,
    SimpleMutableTree,
    CowImmutableTree,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "layout_kind", rename_all = "snake_case")]
pub enum ExportHead {
    MemoryEmpty(MemoryExportHead),
    SimpleMutableTree(SimpleMutableTreeHead),
    CowImmutableTree(CowImmutableTreeHead),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemoryExportHead {
    size_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SimpleMutableTreeHead {
    size_bytes: u64,
    root_node_id: Option<NodeId>,
    tree_format: TreeFormat,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CowImmutableTreeHead {
    size_bytes: u64,
    root_node_id: Option<NodeId>,
    base_wal_seq: WalSeq,
    tree_format: TreeFormat,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExportDescriptor {
    id: ExportId,
    name: ExportName,
    block_size: u64,
    engine_kind: ExportEngineKind,
    state: ExportState,
    created_at: Timestamp,
    updated_at: Timestamp,
    deleted_at: Option<Timestamp>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(transparent)]
pub struct ActiveExportDescriptor {
    descriptor: ExportDescriptor,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateExport {
    name: ExportName,
    size_bytes: u64,
    block_size: u64,
    engine_kind: ExportEngineKind,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CloneExport {
    source: ExportName,
    destination: ExportName,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CloneExportResult {
    source: ExportRecord,
    destination: ExportRecord,
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
pub struct ExportRecord {
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

impl ExportHead {
    pub fn new(
        layout_kind: ExportLayoutKind,
        root_node_id: Option<NodeId>,
        size_bytes: u64,
        base_wal_seq: WalSeq,
    ) -> Result<Self> {
        Self::new_with_tree_format(layout_kind, root_node_id, size_bytes, base_wal_seq, None)
    }

    pub fn new_with_tree_format(
        layout_kind: ExportLayoutKind,
        root_node_id: Option<NodeId>,
        size_bytes: u64,
        base_wal_seq: WalSeq,
        tree_format: Option<TreeFormat>,
    ) -> Result<Self> {
        match layout_kind {
            ExportLayoutKind::MemoryEmpty => {
                if tree_format.is_some() {
                    return Err(CatalogError::invalid_field(
                        "tree_format",
                        "memory_empty export heads must not carry a tree format",
                    ));
                }
                Self::memory_empty_with_base(size_bytes, root_node_id, base_wal_seq)
            }
            ExportLayoutKind::SimpleMutableTree => {
                Self::simple_mutable_tree_with_root_base_and_format(
                    size_bytes,
                    root_node_id,
                    base_wal_seq,
                    tree_format.unwrap_or_default(),
                )
            }
            ExportLayoutKind::CowImmutableTree => Self::cow_immutable_tree_with_root_and_format(
                size_bytes,
                root_node_id,
                base_wal_seq,
                tree_format.unwrap_or_default(),
            ),
        }
    }

    pub fn memory_empty(size_bytes: u64) -> Result<Self> {
        Self::memory_empty_with_base(size_bytes, None, WalSeq::zero())
    }

    pub fn simple_mutable_tree(size_bytes: u64) -> Result<Self> {
        Self::simple_mutable_tree_with_root(size_bytes, None)
    }

    pub fn cow_immutable_tree(size_bytes: u64) -> Result<Self> {
        Self::cow_immutable_tree_with_root(size_bytes, None, WalSeq::zero())
    }

    pub fn simple_mutable_tree_with_root(
        size_bytes: u64,
        root_node_id: Option<NodeId>,
    ) -> Result<Self> {
        Self::simple_mutable_tree_with_root_base_and_format(
            size_bytes,
            root_node_id,
            WalSeq::zero(),
            TreeFormat::default(),
        )
    }

    pub fn cow_immutable_tree_with_root(
        size_bytes: u64,
        root_node_id: Option<NodeId>,
        base_wal_seq: WalSeq,
    ) -> Result<Self> {
        Self::cow_immutable_tree_with_root_and_format(
            size_bytes,
            root_node_id,
            base_wal_seq,
            TreeFormat::default(),
        )
    }

    pub fn cow_immutable_tree_with_root_and_format(
        size_bytes: u64,
        root_node_id: Option<NodeId>,
        base_wal_seq: WalSeq,
        tree_format: TreeFormat,
    ) -> Result<Self> {
        validate_non_zero("size_bytes", size_bytes)?;
        Ok(Self::CowImmutableTree(CowImmutableTreeHead {
            size_bytes,
            root_node_id,
            base_wal_seq,
            tree_format,
        }))
    }

    pub fn layout_kind(&self) -> ExportLayoutKind {
        match self {
            Self::MemoryEmpty(_) => ExportLayoutKind::MemoryEmpty,
            Self::SimpleMutableTree(_) => ExportLayoutKind::SimpleMutableTree,
            Self::CowImmutableTree(_) => ExportLayoutKind::CowImmutableTree,
        }
    }

    pub fn root_node_id(&self) -> Option<&NodeId> {
        match self {
            Self::MemoryEmpty(_) => None,
            Self::SimpleMutableTree(head) => head.root_node_id.as_ref(),
            Self::CowImmutableTree(head) => head.root_node_id.as_ref(),
        }
    }

    pub fn size_bytes(&self) -> u64 {
        match self {
            Self::MemoryEmpty(head) => head.size_bytes,
            Self::SimpleMutableTree(head) => head.size_bytes,
            Self::CowImmutableTree(head) => head.size_bytes,
        }
    }

    pub fn base_wal_seq(&self) -> WalSeq {
        match self {
            Self::MemoryEmpty(_) | Self::SimpleMutableTree(_) => WalSeq::zero(),
            Self::CowImmutableTree(head) => head.base_wal_seq,
        }
    }

    pub fn tree_format(&self) -> Option<TreeFormat> {
        match self {
            Self::MemoryEmpty(_) => None,
            Self::SimpleMutableTree(head) => Some(head.tree_format),
            Self::CowImmutableTree(head) => Some(head.tree_format),
        }
    }

    fn memory_empty_with_base(
        size_bytes: u64,
        root_node_id: Option<NodeId>,
        base_wal_seq: WalSeq,
    ) -> Result<Self> {
        validate_non_zero("size_bytes", size_bytes)?;
        if root_node_id.is_some() {
            return Err(CatalogError::invalid_field(
                "root_node_id",
                "memory_empty export heads must not have a root node",
            ));
        }
        if base_wal_seq != WalSeq::zero() {
            return Err(CatalogError::invalid_field(
                "base_wal_seq",
                "memory_empty export heads must not carry WAL sequence state",
            ));
        }
        Ok(Self::MemoryEmpty(MemoryExportHead { size_bytes }))
    }

    fn simple_mutable_tree_with_root_base_and_format(
        size_bytes: u64,
        root_node_id: Option<NodeId>,
        base_wal_seq: WalSeq,
        tree_format: TreeFormat,
    ) -> Result<Self> {
        validate_non_zero("size_bytes", size_bytes)?;
        if base_wal_seq != WalSeq::zero() {
            return Err(CatalogError::invalid_field(
                "base_wal_seq",
                "simple_mutable_tree export heads must not carry WAL sequence state",
            ));
        }
        Ok(Self::SimpleMutableTree(SimpleMutableTreeHead {
            size_bytes,
            root_node_id,
            tree_format,
        }))
    }
}

impl ExportDescriptor {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: ExportId,
        name: ExportName,
        block_size: u64,
        engine_kind: ExportEngineKind,
        state: ExportState,
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

    pub fn block_size(&self) -> u64 {
        self.block_size
    }

    pub fn engine_kind(&self) -> ExportEngineKind {
        self.engine_kind
    }

    pub fn state(&self) -> ExportState {
        self.state
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

    pub fn into_record(self, head: ExportHead) -> Result<ExportRecord> {
        ExportRecord::new(
            self.id,
            self.name,
            self.block_size,
            self.engine_kind,
            self.state,
            head,
            self.created_at,
            self.updated_at,
            self.deleted_at,
        )
    }
}

impl ActiveExportDescriptor {
    pub fn new(descriptor: ExportDescriptor) -> Result<Self> {
        if descriptor.state() != ExportState::Active {
            return Err(CatalogError::ExportDeleted {
                name: descriptor.name().clone(),
            });
        }

        Ok(Self { descriptor })
    }

    pub fn descriptor(&self) -> &ExportDescriptor {
        &self.descriptor
    }

    pub fn into_descriptor(self) -> ExportDescriptor {
        self.descriptor
    }

    pub fn into_record(self, head: ExportHead) -> Result<ExportRecord> {
        self.descriptor.into_record(head)
    }

    pub fn id(&self) -> &ExportId {
        self.descriptor.id()
    }

    pub fn name(&self) -> &ExportName {
        self.descriptor.name()
    }

    pub fn block_size(&self) -> u64 {
        self.descriptor.block_size()
    }

    pub fn engine_kind(&self) -> ExportEngineKind {
        self.descriptor.engine_kind()
    }

    pub fn state(&self) -> ExportState {
        self.descriptor.state()
    }

    pub fn created_at(&self) -> &Timestamp {
        self.descriptor.created_at()
    }

    pub fn updated_at(&self) -> &Timestamp {
        self.descriptor.updated_at()
    }

    pub fn deleted_at(&self) -> Option<&Timestamp> {
        self.descriptor.deleted_at()
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

impl CloneExport {
    pub fn new(source: ExportName, destination: ExportName) -> Result<Self> {
        if source == destination {
            return Err(CatalogError::invalid_field(
                "destination",
                "clone destination must differ from source",
            ));
        }

        Ok(Self {
            source,
            destination,
        })
    }

    pub fn source(&self) -> &ExportName {
        &self.source
    }

    pub fn destination(&self) -> &ExportName {
        &self.destination
    }
}

impl CloneExportResult {
    pub fn new(source: ExportRecord, destination: ExportRecord) -> Self {
        Self {
            source,
            destination,
        }
    }

    pub fn source(&self) -> &ExportRecord {
        &self.source
    }

    pub fn destination(&self) -> &ExportRecord {
        &self.destination
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

impl ExportRecord {
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
            "wal_durable" => Ok(Self::WalDurable),
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
            "cow_immutable_tree" => Ok(Self::CowImmutableTree),
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
            Self::WalDurable => f.write_str("wal_durable"),
        }
    }
}

impl fmt::Display for ExportLayoutKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MemoryEmpty => f.write_str("memory_empty"),
            Self::SimpleMutableTree => f.write_str("simple_mutable_tree"),
            Self::CowImmutableTree => f.write_str("cow_immutable_tree"),
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

fn validate_non_zero(field: &'static str, value: u64) -> Result<()> {
    if value == 0 {
        return Err(CatalogError::invalid_field(
            field,
            "value must be greater than zero",
        ));
    }
    Ok(())
}
