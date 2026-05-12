//! SQLite transaction helpers for tree record publication.

use crate::adapter::map_sqlx_error;
use crate::tree_rows::{bytes_to_i64, edge_slot_to_i64, node_level_to_i64};
use nbd_control_plane_core::error::Result;
use nbd_control_plane_core::tree::{Timestamp, TreeRecordBatch};

pub(crate) async fn insert_tree_records(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    records: &TreeRecordBatch,
    now: &Timestamp,
) -> Result<()> {
    for node in &records.nodes {
        sqlx::query(
            r#"
            INSERT INTO tree_nodes (
              id, layout_kind, owner_export_id, kind, level,
              span_start_bytes, span_len_bytes, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            "#,
        )
        .bind(node.id.as_str())
        .bind(node.layout_kind.to_string())
        .bind(node.owner_export_id.as_ref().map(|id| id.as_str()))
        .bind(node.kind.to_string())
        .bind(node_level_to_i64(node.level))
        .bind(bytes_to_i64("span_start_bytes", node.span_start_bytes)?)
        .bind(bytes_to_i64("span_len_bytes", node.span_len_bytes)?)
        .bind(now.as_str())
        .execute(&mut **tx)
        .await
        .map_err(map_sqlx_error)?;
    }

    for edge in &records.edges {
        sqlx::query(
            r#"
            INSERT INTO tree_edges (
              parent_node_id, slot, child_node_id
            )
            VALUES (?, ?, ?)
            "#,
        )
        .bind(edge.parent_node_id.as_str())
        .bind(edge_slot_to_i64(edge.slot))
        .bind(edge.child_node_id.as_str())
        .execute(&mut **tx)
        .await
        .map_err(map_sqlx_error)?;
    }

    for leaf_ref in &records.leaf_refs {
        sqlx::query(
            r#"
            INSERT INTO tree_leaf_refs (
              node_id, storage_kind, storage_key, len_bytes, created_at
            )
            VALUES (?, ?, ?, ?, ?)
            "#,
        )
        .bind(leaf_ref.node_id.as_str())
        .bind(leaf_ref.storage_kind.to_string())
        .bind(leaf_ref.storage_key.as_str())
        .bind(bytes_to_i64("len_bytes", leaf_ref.len_bytes)?)
        .bind(now.as_str())
        .execute(&mut **tx)
        .await
        .map_err(map_sqlx_error)?;
    }

    Ok(())
}
