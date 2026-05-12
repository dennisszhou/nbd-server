//! SQLite implementation of the export catalog.

use nbd_control_plane_core::diagnostics::CatalogDoctorCheck;
use nbd_control_plane_core::error::{CatalogError, Result};
use nbd_control_plane_core::export::{
    ActiveExportDescriptor, CloneExport, CloneExportResult, CreateExport, DeleteExport,
    ExportDescriptor, ExportEngineKind, ExportHead, ExportId, ExportLayoutKind, ExportName,
    ExportRecord, ExportState, InspectExport, ListExports,
};
use nbd_control_plane_core::service::{ExportCatalog, TreeRecordStore};
use nbd_control_plane_core::tree::{
    NodeId, PublishTreeUpdate, PublishTreeUpdateOutcome, Timestamp, TreeEdgeLookup, TreeEdgeRecord,
    TreeLeafRefRecord, TreeNodeRecord, WalSeq,
};
use nbd_control_plane_core::tree_format::TreeFormat;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions, SqliteRow};
use sqlx::{ConnectOptions, Row, SqlitePool};
use std::convert::TryFrom;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

/// SQLite-backed export catalog.
#[derive(Debug, Clone)]
pub struct SQLiteExportCatalog {
    pool: SqlitePool,
}

impl SQLiteExportCatalog {
    pub async fn connect_path(path: impl AsRef<Path>) -> Result<Self> {
        let options = SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(false)
            .foreign_keys(true)
            .disable_statement_logging();
        let pool = SqlitePoolOptions::new()
            .max_connections(5)
            .connect_with(options)
            .await
            .map_err(map_sqlx_error)?;

        Ok(Self { pool })
    }

    pub async fn doctor_path(path: impl AsRef<Path>) -> Vec<CatalogDoctorCheck> {
        let path = path.as_ref();
        if !path.exists() {
            return vec![CatalogDoctorCheck::failed(
                "catalog_file",
                format!("{} is missing", path.display()),
                "create and migrate the SQLite catalog",
            )];
        }
        if !path.is_file() {
            return vec![CatalogDoctorCheck::failed(
                "catalog_file",
                format!("{} is not a regular file", path.display()),
                "set catalog.url to a SQLite database file",
            )];
        }

        let catalog = match Self::connect_path(path).await {
            Ok(catalog) => catalog,
            Err(error) => {
                return vec![
                    CatalogDoctorCheck::ok("catalog_file", path.display().to_string()),
                    CatalogDoctorCheck::failed(
                        "catalog_open",
                        error.to_string(),
                        "check catalog.url and SQLite file permissions",
                    ),
                ];
            }
        };

        let mut checks = vec![
            CatalogDoctorCheck::ok("catalog_file", path.display().to_string()),
            CatalogDoctorCheck::ok("catalog_open", "ready"),
        ];
        match catalog.list_exports(ListExports::new(false)).await {
            Ok(_) => checks.push(CatalogDoctorCheck::ok("catalog_schema", "ready")),
            Err(error) => checks.push(CatalogDoctorCheck::failed(
                "catalog_schema",
                error.to_string(),
                "apply the catalog migrations",
            )),
        }
        checks
    }

    pub fn from_pool(pool: SqlitePool) -> Self {
        Self { pool }
    }

    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }

    async fn fetch_by_name(&self, name: &ExportName) -> Result<ExportRecord> {
        let row = sqlx::query(
            r#"
            SELECT
              e.id,
              e.name,
              e.block_size,
              e.engine_kind,
              e.state,
              e.created_at,
              e.updated_at,
              e.deleted_at,
              h.layout_kind,
              h.root_node_id,
              h.tree_format,
              h.size_bytes,
              h.base_wal_seq
            FROM exports e
            JOIN export_heads h
              ON h.export_id = e.id
            WHERE e.name = ?
            "#,
        )
        .bind(name.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(map_sqlx_error)?;

        row.map(|row| row_to_export_record(&row))
            .unwrap_or_else(|| Err(CatalogError::ExportNotFound { name: name.clone() }))
    }

    async fn fetch_descriptor_by_name(&self, name: &ExportName) -> Result<ExportDescriptor> {
        let row = sqlx::query(
            r#"
            SELECT
              id,
              name,
              block_size,
              engine_kind,
              state,
              created_at,
              updated_at,
              deleted_at
            FROM exports
            WHERE name = ?
            "#,
        )
        .bind(name.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(map_sqlx_error)?;

        row.map(|row| row_to_export_descriptor(&row))
            .unwrap_or_else(|| Err(CatalogError::ExportNotFound { name: name.clone() }))
    }
}

#[async_trait::async_trait]
impl ExportCatalog for SQLiteExportCatalog {
    async fn create_export(&self, request: CreateExport) -> Result<ExportRecord> {
        let export_id = ExportId::new(Uuid::new_v4().to_string())?;
        let now = current_timestamp()?;
        let size_bytes = u64_to_i64("size_bytes", request.size_bytes())?;
        let block_size = u64_to_i64("block_size", request.block_size())?;
        let head = match request.engine_kind() {
            ExportEngineKind::Memory => ExportHead::memory_empty(request.size_bytes())?,
            ExportEngineKind::SimpleDurable => {
                ExportHead::simple_mutable_tree(request.size_bytes())?
            }
            ExportEngineKind::WalDurable => ExportHead::cow_immutable_tree(request.size_bytes())?,
        };

        let mut tx = self.pool.begin().await.map_err(map_sqlx_error)?;

        sqlx::query(
            r#"
            INSERT INTO exports (
              id, name, engine_kind, block_size, state, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, 'active', ?, ?)
            "#,
        )
        .bind(export_id.as_str())
        .bind(request.name().as_str())
        .bind(request.engine_kind().to_string())
        .bind(block_size)
        .bind(now.as_str())
        .bind(now.as_str())
        .execute(&mut *tx)
        .await
        .map_err(|error| {
            if is_unique_violation(&error) {
                CatalogError::ExportAlreadyExists {
                    name: request.name().clone(),
                }
            } else {
                map_sqlx_error(error)
            }
        })?;

        sqlx::query(
            r#"
            INSERT INTO export_heads (
              export_id, layout_kind, root_node_id, tree_format, size_bytes,
              base_wal_seq, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            "#,
        )
        .bind(export_id.as_str())
        .bind(head.layout_kind().to_string())
        .bind(head.root_node_id().map(NodeId::as_str))
        .bind(head.tree_format().map(|format| format.to_string()))
        .bind(size_bytes)
        .bind(u64_to_i64("base_wal_seq", head.base_wal_seq().get())?)
        .bind(now.as_str())
        .execute(&mut *tx)
        .await
        .map_err(map_sqlx_error)?;

        tx.commit().await.map_err(map_sqlx_error)?;

        ExportRecord::new(
            export_id,
            request.name().clone(),
            request.block_size(),
            request.engine_kind(),
            ExportState::Active,
            head,
            now.clone(),
            now,
            None,
        )
    }

    async fn clone_export(&self, request: CloneExport) -> Result<CloneExportResult> {
        let destination_id = ExportId::new(Uuid::new_v4().to_string())?;
        let now = current_timestamp()?;
        let mut tx = self.pool.begin().await.map_err(map_sqlx_error)?;
        let source = fetch_export_record_by_name_in_tx(&mut tx, request.source()).await?;
        if source.state() == ExportState::Deleted {
            return Err(CatalogError::ExportDeleted {
                name: request.source().clone(),
            });
        }
        if source.engine_kind() != ExportEngineKind::WalDurable {
            return Err(CatalogError::invalid_field(
                "source",
                "clone requires a wal_durable source",
            ));
        }
        if source.head().layout_kind() != ExportLayoutKind::CowImmutableTree {
            return Err(CatalogError::invalid_field(
                "source",
                "clone requires a cow_immutable_tree source head",
            ));
        }
        let source_root = source.head().root_node_id().ok_or_else(|| {
            CatalogError::invalid_field("source", "source committed snapshot is empty")
        })?;

        sqlx::query(
            r#"
            INSERT INTO exports (
              id, name, engine_kind, block_size, state, created_at, updated_at
            )
            VALUES (?, ?, 'wal_durable', ?, 'active', ?, ?)
            "#,
        )
        .bind(destination_id.as_str())
        .bind(request.destination().as_str())
        .bind(u64_to_i64("block_size", source.block_size())?)
        .bind(now.as_str())
        .bind(now.as_str())
        .execute(&mut *tx)
        .await
        .map_err(|error| {
            if is_unique_violation(&error) {
                CatalogError::ExportAlreadyExists {
                    name: request.destination().clone(),
                }
            } else {
                map_sqlx_error(error)
            }
        })?;

        sqlx::query(
            r#"
            INSERT INTO export_heads (
              export_id, layout_kind, root_node_id, tree_format, size_bytes,
              base_wal_seq, updated_at
            )
            VALUES (?, 'cow_immutable_tree', ?, ?, ?, 0, ?)
            "#,
        )
        .bind(destination_id.as_str())
        .bind(source_root.as_str())
        .bind(
            source
                .head()
                .tree_format()
                .ok_or_else(|| {
                    CatalogError::invalid_field("tree_format", "clone source missing tree format")
                })?
                .to_string(),
        )
        .bind(u64_to_i64("size_bytes", source.size_bytes())?)
        .bind(now.as_str())
        .execute(&mut *tx)
        .await
        .map_err(map_sqlx_error)?;

        let destination = fetch_export_record_by_id_in_tx(&mut tx, &destination_id).await?;
        tx.commit().await.map_err(map_sqlx_error)?;
        Ok(CloneExportResult::new(source, destination))
    }

    async fn delete_export(&self, request: DeleteExport) -> Result<()> {
        let meta = self.fetch_by_name(request.name()).await?;
        if meta.state() == ExportState::Deleted {
            return Err(CatalogError::ExportDeleted {
                name: request.name().clone(),
            });
        }

        let now = current_timestamp()?;
        let result = sqlx::query(
            r#"
            UPDATE exports
            SET state = 'deleted', updated_at = ?, deleted_at = ?
            WHERE id = ? AND state = 'active'
            "#,
        )
        .bind(now.as_str())
        .bind(now.as_str())
        .bind(meta.id().as_str())
        .execute(&self.pool)
        .await
        .map_err(map_sqlx_error)?;

        if result.rows_affected() == 1 {
            Ok(())
        } else {
            Err(CatalogError::ExportDeleted {
                name: request.name().clone(),
            })
        }
    }

    async fn load_export(&self, name: ExportName) -> Result<ExportRecord> {
        let meta = self.fetch_by_name(&name).await?;
        if meta.state() == ExportState::Deleted {
            Err(CatalogError::ExportDeleted { name })
        } else {
            Ok(meta)
        }
    }

    async fn load_export_descriptor(&self, name: ExportName) -> Result<ActiveExportDescriptor> {
        let descriptor = self.fetch_descriptor_by_name(&name).await?;
        ActiveExportDescriptor::new(descriptor)
    }

    async fn load_export_head(&self, export_id: &ExportId) -> Result<ExportHead> {
        load_export_head(&self.pool, export_id).await
    }

    async fn inspect_export(&self, request: InspectExport) -> Result<ExportRecord> {
        self.fetch_by_name(request.name()).await
    }

    async fn list_exports(&self, request: ListExports) -> Result<Vec<ExportRecord>> {
        let include_deleted = if request.includes_deleted() {
            1_i64
        } else {
            0_i64
        };
        let rows = sqlx::query(
            r#"
            SELECT
              e.id,
              e.name,
              e.block_size,
              e.engine_kind,
              e.state,
              e.created_at,
              e.updated_at,
              e.deleted_at,
              h.layout_kind,
              h.root_node_id,
              h.tree_format,
              h.size_bytes,
              h.base_wal_seq
            FROM exports e
            JOIN export_heads h
              ON h.export_id = e.id
            WHERE (? = 1 OR e.state != 'deleted')
            ORDER BY e.name ASC
            "#,
        )
        .bind(include_deleted)
        .fetch_all(&self.pool)
        .await
        .map_err(map_sqlx_error)?;

        rows.iter().map(row_to_export_record).collect()
    }
}

#[async_trait::async_trait]
impl TreeRecordStore for SQLiteExportCatalog {
    async fn load_node(&self, node_id: &NodeId) -> Result<Option<TreeNodeRecord>> {
        let row = sqlx::query(
            r#"
            SELECT
              id, layout_kind, owner_export_id, kind, level,
              span_start_bytes, span_len_bytes
            FROM tree_nodes
            WHERE id = ?
            "#,
        )
        .bind(node_id.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(map_sqlx_error)?;

        row.as_ref().map(crate::tree_rows::row_to_node).transpose()
    }

    async fn load_nodes(&self, node_ids: &[NodeId]) -> Result<Vec<TreeNodeRecord>> {
        let mut records = Vec::with_capacity(node_ids.len());
        for node_id in node_ids {
            if let Some(record) = self.load_node(node_id).await? {
                records.push(record);
            }
        }
        Ok(records)
    }

    async fn load_child_edges(&self, lookups: &[TreeEdgeLookup]) -> Result<Vec<TreeEdgeRecord>> {
        let mut records = Vec::new();
        for lookup in lookups {
            for slot in &lookup.slots {
                let row = sqlx::query(
                    r#"
                    SELECT parent_node_id, slot, child_node_id
                    FROM tree_edges
                    WHERE parent_node_id = ? AND slot = ?
                    "#,
                )
                .bind(lookup.parent_node_id.as_str())
                .bind(crate::tree_rows::edge_slot_to_i64(*slot))
                .fetch_optional(&self.pool)
                .await
                .map_err(map_sqlx_error)?;

                if let Some(row) = row {
                    records.push(crate::tree_rows::row_to_edge(&row)?);
                }
            }
        }
        Ok(records)
    }

    async fn load_leaf_refs(&self, node_ids: &[NodeId]) -> Result<Vec<TreeLeafRefRecord>> {
        let mut records = Vec::with_capacity(node_ids.len());
        for node_id in node_ids {
            let row = sqlx::query(
                r#"
                SELECT node_id, storage_kind, storage_key, len_bytes
                FROM tree_leaf_refs
                WHERE node_id = ?
                "#,
            )
            .bind(node_id.as_str())
            .fetch_optional(&self.pool)
            .await
            .map_err(map_sqlx_error)?;

            if let Some(row) = row {
                records.push(crate::tree_rows::row_to_leaf_ref(&row)?);
            }
        }
        Ok(records)
    }

    async fn publish_tree_update(
        &self,
        request: PublishTreeUpdate,
    ) -> Result<PublishTreeUpdateOutcome> {
        let now = current_timestamp()?;
        let mut tx = self.pool.begin().await.map_err(map_sqlx_error)?;
        let current = fetch_export_record_by_id_in_tx(&mut tx, &request.export_id).await?;
        if current.state() == ExportState::Deleted {
            return Err(CatalogError::ExportDeleted {
                name: current.name().clone(),
            });
        }
        if current.head() != &request.expected_head {
            tx.commit().await.map_err(map_sqlx_error)?;
            return Ok(PublishTreeUpdateOutcome::StaleHead(current));
        }

        crate::transaction::insert_tree_records(&mut tx, &request.records, &now).await?;

        let next_root = request.next_head.root_node_id().map(NodeId::as_str);
        let next_format = request
            .next_head
            .tree_format()
            .map(|format| format.to_string());
        let expected_root = request.expected_head.root_node_id().map(NodeId::as_str);
        let expected_format = request
            .expected_head
            .tree_format()
            .map(|format| format.to_string());

        let result = sqlx::query(
            r#"
            UPDATE export_heads
            SET
                layout_kind = ?,
                root_node_id = ?,
                tree_format = ?,
                size_bytes = ?,
                base_wal_seq = ?,
                updated_at = ?
            WHERE export_id = ?
              AND layout_kind = ?
              AND root_node_id IS ?
              AND tree_format IS ?
              AND size_bytes = ?
              AND base_wal_seq = ?
            "#,
        )
        .bind(request.next_head.layout_kind().to_string())
        .bind(next_root)
        .bind(next_format)
        .bind(u64_to_i64("size_bytes", request.next_head.size_bytes())?)
        .bind(u64_to_i64(
            "base_wal_seq",
            request.next_head.base_wal_seq().get(),
        )?)
        .bind(now.as_str())
        .bind(request.export_id.as_str())
        .bind(request.expected_head.layout_kind().to_string())
        .bind(expected_root)
        .bind(expected_format)
        .bind(u64_to_i64(
            "size_bytes",
            request.expected_head.size_bytes(),
        )?)
        .bind(u64_to_i64(
            "base_wal_seq",
            request.expected_head.base_wal_seq().get(),
        )?)
        .execute(&mut *tx)
        .await
        .map_err(map_sqlx_error)?;

        if result.rows_affected() == 0 {
            tx.rollback().await.map_err(map_sqlx_error)?;
            let current = fetch_export_record_by_id(&self.pool, &request.export_id).await?;
            return Ok(PublishTreeUpdateOutcome::StaleHead(current));
        }

        sqlx::query(
            r#"
            UPDATE exports
            SET updated_at = ?
            WHERE id = ?
            "#,
        )
        .bind(now.as_str())
        .bind(request.export_id.as_str())
        .execute(&mut *tx)
        .await
        .map_err(map_sqlx_error)?;

        let record = fetch_export_record_by_id_in_tx(&mut tx, &request.export_id).await?;
        tx.commit().await.map_err(map_sqlx_error)?;
        Ok(PublishTreeUpdateOutcome::Published(record))
    }
}

fn export_head_not_found(export_id: &ExportId) -> CatalogError {
    CatalogError::database(format!("export head `{export_id}` not found"))
}

async fn load_export_head(pool: &SqlitePool, export_id: &ExportId) -> Result<ExportHead> {
    let row = sqlx::query(
        r#"
        SELECT layout_kind, root_node_id, tree_format, size_bytes, base_wal_seq
        FROM export_heads
        WHERE export_id = ?
        "#,
    )
    .bind(export_id.as_str())
    .fetch_optional(pool)
    .await
    .map_err(map_sqlx_error)?
    .ok_or_else(|| export_head_not_found(export_id))?;

    row_to_export_head(&row)
}

async fn fetch_export_record_by_id_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    export_id: &ExportId,
) -> Result<ExportRecord> {
    let row = sqlx::query(
        r#"
        SELECT
          e.id,
          e.name,
          e.block_size,
          e.engine_kind,
          e.state,
          e.created_at,
          e.updated_at,
          e.deleted_at,
          h.layout_kind,
          h.root_node_id,
          h.tree_format,
          h.size_bytes,
          h.base_wal_seq
        FROM exports e
        JOIN export_heads h
          ON h.export_id = e.id
        WHERE e.id = ?
        "#,
    )
    .bind(export_id.as_str())
    .fetch_optional(&mut **tx)
    .await
    .map_err(map_sqlx_error)?
    .ok_or_else(|| CatalogError::database(format!("export `{export_id}` not found")))?;

    row_to_export_record(&row)
}

async fn fetch_export_record_by_name_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    name: &ExportName,
) -> Result<ExportRecord> {
    let row = sqlx::query(
        r#"
        SELECT
          e.id,
          e.name,
          e.block_size,
          e.engine_kind,
          e.state,
          e.created_at,
          e.updated_at,
          e.deleted_at,
          h.layout_kind,
          h.root_node_id,
          h.tree_format,
          h.size_bytes,
          h.base_wal_seq
        FROM exports e
        JOIN export_heads h
          ON h.export_id = e.id
        WHERE e.name = ?
        "#,
    )
    .bind(name.as_str())
    .fetch_optional(&mut **tx)
    .await
    .map_err(map_sqlx_error)?
    .ok_or_else(|| CatalogError::ExportNotFound { name: name.clone() })?;

    row_to_export_record(&row)
}

async fn fetch_export_record_by_id(
    pool: &SqlitePool,
    export_id: &ExportId,
) -> Result<ExportRecord> {
    let row = sqlx::query(
        r#"
        SELECT
          e.id,
          e.name,
          e.block_size,
          e.engine_kind,
          e.state,
          e.created_at,
          e.updated_at,
          e.deleted_at,
          h.layout_kind,
          h.root_node_id,
          h.tree_format,
          h.size_bytes,
          h.base_wal_seq
        FROM exports e
        JOIN export_heads h
          ON h.export_id = e.id
        WHERE e.id = ?
        "#,
    )
    .bind(export_id.as_str())
    .fetch_optional(pool)
    .await
    .map_err(map_sqlx_error)?
    .ok_or_else(|| CatalogError::database(format!("export `{export_id}` not found")))?;

    row_to_export_record(&row)
}

fn row_to_export_descriptor(row: &SqliteRow) -> Result<ExportDescriptor> {
    let state: String = row.try_get("state").map_err(map_sqlx_error)?;
    let engine_kind: String = row.try_get("engine_kind").map_err(map_sqlx_error)?;
    let deleted_at: Option<String> = row.try_get("deleted_at").map_err(map_sqlx_error)?;

    ExportDescriptor::new(
        ExportId::new(row.try_get::<String, _>("id").map_err(map_sqlx_error)?)?,
        ExportName::new(row.try_get::<String, _>("name").map_err(map_sqlx_error)?)?,
        i64_to_u64(
            "block_size",
            row.try_get("block_size").map_err(map_sqlx_error)?,
        )?,
        engine_kind.parse::<ExportEngineKind>()?,
        state.parse()?,
        Timestamp::new(
            row.try_get::<String, _>("created_at")
                .map_err(map_sqlx_error)?,
        )?,
        Timestamp::new(
            row.try_get::<String, _>("updated_at")
                .map_err(map_sqlx_error)?,
        )?,
        deleted_at.map(Timestamp::new).transpose()?,
    )
}

fn row_to_export_head(row: &SqliteRow) -> Result<ExportHead> {
    let layout_kind: String = row.try_get("layout_kind").map_err(map_sqlx_error)?;
    let layout_kind = layout_kind.parse::<ExportLayoutKind>()?;
    let root_node_id: Option<String> = row.try_get("root_node_id").map_err(map_sqlx_error)?;
    let tree_format: Option<String> = row.try_get("tree_format").map_err(map_sqlx_error)?;
    let tree_format = match layout_kind {
        ExportLayoutKind::MemoryEmpty => None,
        ExportLayoutKind::SimpleMutableTree | ExportLayoutKind::CowImmutableTree => {
            let tree_format = tree_format.ok_or_else(|| {
                CatalogError::invalid_field("tree_format", "tree-backed export head missing format")
            })?;
            Some(tree_format.parse::<TreeFormat>()?)
        }
    };

    ExportHead::new_with_tree_format(
        layout_kind,
        root_node_id.map(NodeId::new).transpose()?,
        i64_to_u64(
            "size_bytes",
            row.try_get("size_bytes").map_err(map_sqlx_error)?,
        )?,
        WalSeq::new(i64_to_u64(
            "base_wal_seq",
            row.try_get("base_wal_seq").map_err(map_sqlx_error)?,
        )?),
        tree_format,
    )
}

fn row_to_export_record(row: &SqliteRow) -> Result<ExportRecord> {
    let descriptor = row_to_export_descriptor(row)?;
    descriptor.into_record(row_to_export_head(row)?)
}

fn current_timestamp() -> Result<Timestamp> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| {
            CatalogError::database(format!("system clock before UNIX epoch: {error}"))
        })?;

    Timestamp::new(format!("unix_us:{}", duration.as_micros()))
}

pub(crate) fn u64_to_i64(field: &'static str, value: u64) -> Result<i64> {
    i64::try_from(value).map_err(|_| {
        CatalogError::invalid_field(
            field,
            format!("value {value} does not fit in SQLite INTEGER"),
        )
    })
}

pub(crate) fn i64_to_u64(field: &'static str, value: i64) -> Result<u64> {
    u64::try_from(value).map_err(|_| {
        CatalogError::invalid_field(field, format!("database value {value} is negative"))
    })
}

pub(crate) fn u16_to_i64(value: u16) -> i64 {
    i64::from(value)
}

pub(crate) fn i64_to_u16(field: &'static str, value: i64) -> Result<u16> {
    u16::try_from(value).map_err(|_| {
        CatalogError::invalid_field(field, format!("database value {value} does not fit in u16"))
    })
}

fn is_unique_violation(error: &sqlx::Error) -> bool {
    error
        .as_database_error()
        .map(|error| error.is_unique_violation())
        .unwrap_or(false)
}

pub(crate) fn map_sqlx_error(error: sqlx::Error) -> CatalogError {
    CatalogError::database_source(format!("database error: {error}"), error)
}
