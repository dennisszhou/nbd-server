//! SQLite row mapping for storage-neutral tree records.

use crate::adapter::{i64_to_u16, i64_to_u64, map_sqlx_error, u16_to_i64, u64_to_i64};
use nbd_control_plane_core::error::Result;
use nbd_control_plane_core::export::{ExportId, ExportLayoutKind};
use nbd_control_plane_core::tree::{
    BlobKey, NodeId, TreeEdgeRecord, TreeLeafRefRecord, TreeNodeKind, TreeNodeRecord,
    TreeStorageKind,
};
use sqlx::Row;
use sqlx::sqlite::SqliteRow;

pub(crate) fn row_to_node(row: &SqliteRow) -> Result<TreeNodeRecord> {
    let layout_kind: String = row.try_get("layout_kind").map_err(map_sqlx_error)?;
    let owner_export_id: Option<String> = row.try_get("owner_export_id").map_err(map_sqlx_error)?;
    let kind: String = row.try_get("kind").map_err(map_sqlx_error)?;

    Ok(TreeNodeRecord {
        id: NodeId::new(row.try_get::<String, _>("id").map_err(map_sqlx_error)?)?,
        layout_kind: layout_kind.parse::<ExportLayoutKind>()?,
        owner_export_id: owner_export_id.map(ExportId::new).transpose()?,
        kind: kind.parse::<TreeNodeKind>()?,
        level: i64_to_u16("level", row.try_get("level").map_err(map_sqlx_error)?)?,
        span_start_bytes: i64_to_u64(
            "span_start_bytes",
            row.try_get("span_start_bytes").map_err(map_sqlx_error)?,
        )?,
        span_len_bytes: i64_to_u64(
            "span_len_bytes",
            row.try_get("span_len_bytes").map_err(map_sqlx_error)?,
        )?,
    })
}

pub(crate) fn row_to_edge(row: &SqliteRow) -> Result<TreeEdgeRecord> {
    Ok(TreeEdgeRecord {
        parent_node_id: NodeId::new(
            row.try_get::<String, _>("parent_node_id")
                .map_err(map_sqlx_error)?,
        )?,
        slot: i64_to_u16("slot", row.try_get("slot").map_err(map_sqlx_error)?)?,
        child_node_id: NodeId::new(
            row.try_get::<String, _>("child_node_id")
                .map_err(map_sqlx_error)?,
        )?,
    })
}

pub(crate) fn row_to_leaf_ref(row: &SqliteRow) -> Result<TreeLeafRefRecord> {
    let storage_kind: String = row.try_get("storage_kind").map_err(map_sqlx_error)?;

    Ok(TreeLeafRefRecord {
        node_id: NodeId::new(
            row.try_get::<String, _>("node_id")
                .map_err(map_sqlx_error)?,
        )?,
        storage_kind: storage_kind.parse::<TreeStorageKind>()?,
        storage_key: BlobKey::new(
            row.try_get::<String, _>("storage_key")
                .map_err(map_sqlx_error)?,
        )?,
        len_bytes: i64_to_u64(
            "len_bytes",
            row.try_get("len_bytes").map_err(map_sqlx_error)?,
        )?,
    })
}

pub(crate) fn node_level_to_i64(level: u16) -> i64 {
    u16_to_i64(level)
}

pub(crate) fn edge_slot_to_i64(slot: u16) -> i64 {
    u16_to_i64(slot)
}

pub(crate) fn bytes_to_i64(field: &'static str, value: u64) -> Result<i64> {
    u64_to_i64(field, value)
}
