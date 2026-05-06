//! SQLite implementation of the export catalog.

use crate::{
    BlobKey, CatalogError, CatalogProvider, CatalogUrl, ChunkIndex, CloneExport, CloneExportResult,
    CowChunkRef, CowTreeMetadataStore, CowTreeSnapshot, CreateExport, DeleteExport, ExportCatalog,
    ExportDescriptor, ExportEngineKind, ExportHead, ExportId, ExportLayoutKind, ExportName,
    ExportRecord, ExportState, InspectExport, ListExports, NodeId, PublishCompaction,
    PublishCompactionOutcome, Result, SimpleChunkRef, SimpleTreeMetadataStore, SimpleTreeSnapshot,
    Timestamp, WalSeq, SIMPLE_CHUNK_BYTES, TREE_CHUNK_BYTES,
};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions, SqliteRow};
use sqlx::{ConnectOptions, Row, SqlitePool};
use std::collections::{BTreeMap, BTreeSet};
use std::convert::TryFrom;
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

/// SQLite-backed export catalog.
#[derive(Debug, Clone)]
pub struct SQLiteExportCatalog {
    pool: SqlitePool,
}

impl SQLiteExportCatalog {
    pub async fn connect(url: &CatalogUrl) -> Result<Self> {
        if url.provider() != CatalogProvider::Sqlite {
            return Err(CatalogError::unsupported_catalog_provider(
                url.as_str(),
                "SQLiteExportCatalog requires a file: catalog URL",
            ));
        }

        let options = SqliteConnectOptions::new()
            .filename(url.sqlite_path()?)
            .create_if_missing(true)
            .foreign_keys(true)
            .disable_statement_logging();
        let pool = SqlitePoolOptions::new()
            .max_connections(5)
            .connect_with(options)
            .await
            .map_err(map_sqlx_error)?;

        Ok(Self { pool })
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
              h.size_bytes,
              h.checkpoint_wal_seq
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
              export_id, layout_kind, root_node_id, size_bytes,
              checkpoint_wal_seq, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?)
            "#,
        )
        .bind(export_id.as_str())
        .bind(head.layout_kind().to_string())
        .bind(head.root_node_id().map(NodeId::as_str))
        .bind(size_bytes)
        .bind(u64_to_i64(
            "checkpoint_wal_seq",
            head.checkpoint_wal_seq().get(),
        )?)
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
              export_id, layout_kind, root_node_id, size_bytes,
              checkpoint_wal_seq, updated_at
            )
            VALUES (?, 'cow_immutable_tree', ?, ?, 0, ?)
            "#,
        )
        .bind(destination_id.as_str())
        .bind(source_root.as_str())
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

    async fn load_export_descriptor(&self, name: ExportName) -> Result<ExportDescriptor> {
        let descriptor = self.fetch_descriptor_by_name(&name).await?;
        if descriptor.state() == ExportState::Deleted {
            Err(CatalogError::ExportDeleted { name })
        } else {
            Ok(descriptor)
        }
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
              h.size_bytes,
              h.checkpoint_wal_seq
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
impl SimpleTreeMetadataStore for SQLiteExportCatalog {
    async fn load_simple_tree(&self, export_id: &ExportId) -> Result<SimpleTreeSnapshot> {
        load_simple_tree_snapshot(&self.pool, export_id).await
    }

    async fn commit_simple_chunks(
        &self,
        export_id: &ExportId,
        chunks: Vec<SimpleChunkRef>,
    ) -> Result<SimpleTreeSnapshot> {
        if chunks.is_empty() {
            return self.load_simple_tree(export_id).await;
        }

        let mut seen = BTreeSet::new();
        let mut pending = BTreeMap::new();
        for chunk in chunks {
            if !seen.insert(chunk.chunk_index()) {
                return Err(CatalogError::invalid_field(
                    "chunk_index",
                    format!("duplicate chunk index {}", chunk.chunk_index()),
                ));
            }
            pending.insert(chunk.chunk_index(), chunk);
        }

        let mut tx = self.pool.begin().await.map_err(map_sqlx_error)?;
        let row = sqlx::query(
            r#"
            SELECT layout_kind, root_node_id, size_bytes
            FROM export_heads
            WHERE export_id = ?
            "#,
        )
        .bind(export_id.as_str())
        .fetch_optional(&mut *tx)
        .await
        .map_err(map_sqlx_error)?
        .ok_or_else(|| export_head_not_found(export_id))?;

        let layout_kind: String = row.try_get("layout_kind").map_err(map_sqlx_error)?;
        let layout_kind = layout_kind.parse::<ExportLayoutKind>()?;
        if layout_kind != ExportLayoutKind::SimpleMutableTree {
            return Err(CatalogError::invalid_field(
                "layout_kind",
                "simple tree metadata requires a simple_mutable_tree export head",
            ));
        }

        let size_bytes = i64_to_u64(
            "size_bytes",
            row.try_get("size_bytes").map_err(map_sqlx_error)?,
        )?;
        for chunk in pending.values() {
            validate_tree_chunk_in_export(chunk.chunk_index(), size_bytes)?;
        }

        let mut root_node_id = row
            .try_get::<Option<String>, _>("root_node_id")
            .map_err(map_sqlx_error)?
            .map(NodeId::new)
            .transpose()?;

        let now = current_timestamp()?;
        if root_node_id.is_none() {
            let root = NodeId::new(Uuid::new_v4().to_string())?;
            sqlx::query(
                r#"
                INSERT INTO tree_nodes (
                  id, layout_kind, owner_export_id, kind, level,
                  span_start_bytes, span_len_bytes, created_at
                )
                VALUES (?, 'simple_mutable_tree', ?, 'internal', 1, 0, ?, ?)
                "#,
            )
            .bind(root.as_str())
            .bind(export_id.as_str())
            .bind(u64_to_i64("size_bytes", size_bytes)?)
            .bind(now.as_str())
            .execute(&mut *tx)
            .await
            .map_err(map_sqlx_error)?;

            sqlx::query(
                r#"
                UPDATE export_heads
                SET root_node_id = ?
                WHERE export_id = ?
                "#,
            )
            .bind(root.as_str())
            .bind(export_id.as_str())
            .execute(&mut *tx)
            .await
            .map_err(map_sqlx_error)?;

            root_node_id = Some(root);
        }

        let root_node_id = root_node_id.expect("root must exist after creation");
        validate_simple_root_in_tx(&mut tx, export_id, &root_node_id).await?;

        for chunk in pending.values() {
            let slot = u64_to_i64("chunk_index", chunk.chunk_index().get())?;
            let existing = sqlx::query(
                r#"
                SELECT child_node_id
                FROM tree_edges
                WHERE parent_node_id = ? AND slot = ?
                "#,
            )
            .bind(root_node_id.as_str())
            .bind(slot)
            .fetch_optional(&mut *tx)
            .await
            .map_err(map_sqlx_error)?;
            if existing.is_some() {
                return Err(CatalogError::invalid_field(
                    "chunk_index",
                    format!("chunk {} is already materialized", chunk.chunk_index()),
                ));
            }

            let leaf = NodeId::new(Uuid::new_v4().to_string())?;
            let span_start = chunk
                .chunk_index()
                .get()
                .checked_mul(SIMPLE_CHUNK_BYTES)
                .ok_or_else(|| {
                    CatalogError::invalid_field(
                        "chunk_index",
                        format!("chunk {} overflows byte offset", chunk.chunk_index()),
                    )
                })?;

            sqlx::query(
                r#"
                INSERT INTO tree_nodes (
                  id, layout_kind, owner_export_id, kind, level,
                  span_start_bytes, span_len_bytes, created_at
                )
                VALUES (?, 'simple_mutable_tree', ?, 'leaf', 0, ?, ?, ?)
                "#,
            )
            .bind(leaf.as_str())
            .bind(export_id.as_str())
            .bind(u64_to_i64("span_start_bytes", span_start)?)
            .bind(u64_to_i64("len_bytes", chunk.len_bytes())?)
            .bind(now.as_str())
            .execute(&mut *tx)
            .await
            .map_err(map_sqlx_error)?;

            sqlx::query(
                r#"
                INSERT INTO tree_leaf_refs (
                  node_id, storage_kind, storage_key, len_bytes, created_at
                )
                VALUES (?, 'mutable_blob', ?, ?, ?)
                "#,
            )
            .bind(leaf.as_str())
            .bind(chunk.blob_key().as_str())
            .bind(u64_to_i64("len_bytes", chunk.len_bytes())?)
            .bind(now.as_str())
            .execute(&mut *tx)
            .await
            .map_err(map_sqlx_error)?;

            sqlx::query(
                r#"
                INSERT INTO tree_edges (parent_node_id, slot, child_node_id)
                VALUES (?, ?, ?)
                "#,
            )
            .bind(root_node_id.as_str())
            .bind(slot)
            .bind(leaf.as_str())
            .execute(&mut *tx)
            .await
            .map_err(map_sqlx_error)?;
        }

        sqlx::query(
            r#"
            UPDATE export_heads
            SET updated_at = ?
            WHERE export_id = ?
            "#,
        )
        .bind(now.as_str())
        .bind(export_id.as_str())
        .execute(&mut *tx)
        .await
        .map_err(map_sqlx_error)?;

        tx.commit().await.map_err(map_sqlx_error)?;
        self.load_simple_tree(export_id).await
    }
}

#[async_trait::async_trait]
impl CowTreeMetadataStore for SQLiteExportCatalog {
    async fn load_cow_tree(&self, export_id: &ExportId) -> Result<CowTreeSnapshot> {
        load_cow_tree_snapshot(&self.pool, export_id).await
    }

    async fn publish_compaction(
        &self,
        request: PublishCompaction,
    ) -> Result<PublishCompactionOutcome> {
        let mut tx = self.pool.begin().await.map_err(map_sqlx_error)?;
        let current = fetch_export_record_by_id_in_tx(&mut tx, request.export_id()).await?;
        if current.state() == ExportState::Deleted {
            return Err(CatalogError::ExportDeleted {
                name: current.name().clone(),
            });
        }
        if current.engine_kind() != ExportEngineKind::WalDurable {
            return Err(CatalogError::invalid_field(
                "engine_kind",
                "compaction publication requires a wal_durable export",
            ));
        }
        if current.head().layout_kind() != ExportLayoutKind::CowImmutableTree {
            return Err(CatalogError::invalid_field(
                "layout_kind",
                "compaction publication requires a cow_immutable_tree export head",
            ));
        }

        if current.head().checkpoint_wal_seq() >= request.compacted_through() {
            tx.commit().await.map_err(map_sqlx_error)?;
            return Ok(PublishCompactionOutcome::AlreadyCovered(current));
        }
        if current.head() != request.expected_base() {
            tx.commit().await.map_err(map_sqlx_error)?;
            return Ok(PublishCompactionOutcome::StalePlan(current));
        }

        let now = current_timestamp()?;
        let root = NodeId::new(Uuid::new_v4().to_string())?;
        sqlx::query(
            r#"
            INSERT INTO tree_nodes (
              id, layout_kind, owner_export_id, kind, level,
              span_start_bytes, span_len_bytes, created_at
            )
            VALUES (?, 'cow_immutable_tree', ?, 'internal', 1, 0, ?, ?)
            "#,
        )
        .bind(root.as_str())
        .bind(request.export_id().as_str())
        .bind(u64_to_i64("size_bytes", current.size_bytes())?)
        .bind(now.as_str())
        .execute(&mut *tx)
        .await
        .map_err(map_sqlx_error)?;

        let reusable_leaf_nodes = load_reusable_cow_leaf_nodes_in_tx(
            &mut tx,
            request.export_id(),
            request.expected_base(),
        )
        .await?;

        for chunk in request.chunks() {
            let span_start = chunk
                .chunk_index()
                .get()
                .checked_mul(TREE_CHUNK_BYTES)
                .ok_or_else(|| {
                    CatalogError::invalid_field(
                        "chunk_index",
                        format!("chunk {} overflows byte offset", chunk.chunk_index()),
                    )
                })?;
            let slot = u64_to_i64("chunk_index", chunk.chunk_index().get())?;

            if let Some(existing) = reusable_leaf_nodes.get(&chunk.chunk_index()) {
                if existing.blob_key.as_str() == chunk.blob_key().as_str()
                    && existing.len_bytes == chunk.len_bytes()
                {
                    sqlx::query(
                        r#"
                        INSERT INTO tree_edges (
                          parent_node_id, slot, child_node_id
                        )
                        VALUES (?, ?, ?)
                        "#,
                    )
                    .bind(root.as_str())
                    .bind(slot)
                    .bind(existing.node_id.as_str())
                    .execute(&mut *tx)
                    .await
                    .map_err(map_sqlx_error)?;
                    continue;
                }
            }

            let leaf = NodeId::new(Uuid::new_v4().to_string())?;
            sqlx::query(
                r#"
                INSERT INTO tree_nodes (
                  id, layout_kind, owner_export_id, kind, level,
                  span_start_bytes, span_len_bytes, created_at
                )
                VALUES (?, 'cow_immutable_tree', ?, 'leaf', 0, ?, ?, ?)
                "#,
            )
            .bind(leaf.as_str())
            .bind(request.export_id().as_str())
            .bind(u64_to_i64("span_start_bytes", span_start)?)
            .bind(u64_to_i64("len_bytes", chunk.len_bytes())?)
            .bind(now.as_str())
            .execute(&mut *tx)
            .await
            .map_err(map_sqlx_error)?;

            sqlx::query(
                r#"
                INSERT INTO tree_leaf_refs (
                  node_id, storage_kind, storage_key, len_bytes, created_at
                )
                VALUES (?, 'immutable_blob', ?, ?, ?)
                "#,
            )
            .bind(leaf.as_str())
            .bind(chunk.blob_key().as_str())
            .bind(u64_to_i64("len_bytes", chunk.len_bytes())?)
            .bind(now.as_str())
            .execute(&mut *tx)
            .await
            .map_err(map_sqlx_error)?;

            sqlx::query(
                r#"
                INSERT INTO tree_edges (parent_node_id, slot, child_node_id)
                VALUES (?, ?, ?)
                "#,
            )
            .bind(root.as_str())
            .bind(slot)
            .bind(leaf.as_str())
            .execute(&mut *tx)
            .await
            .map_err(map_sqlx_error)?;
        }

        let expected_root = request.expected_base().root_node_id().map(NodeId::as_str);
        let update = sqlx::query(
            r#"
            UPDATE export_heads
            SET root_node_id = ?,
                checkpoint_wal_seq = ?,
                updated_at = ?
            WHERE export_id = ?
              AND layout_kind = ?
              AND (
                (root_node_id IS NULL AND ? IS NULL)
                OR root_node_id = ?
              )
              AND size_bytes = ?
              AND checkpoint_wal_seq = ?
            "#,
        )
        .bind(root.as_str())
        .bind(u64_to_i64(
            "checkpoint_wal_seq",
            request.compacted_through().get(),
        )?)
        .bind(now.as_str())
        .bind(request.export_id().as_str())
        .bind(request.expected_base().layout_kind().to_string())
        .bind(expected_root)
        .bind(expected_root)
        .bind(u64_to_i64(
            "size_bytes",
            request.expected_base().size_bytes(),
        )?)
        .bind(u64_to_i64(
            "checkpoint_wal_seq",
            request.expected_base().checkpoint_wal_seq().get(),
        )?)
        .execute(&mut *tx)
        .await
        .map_err(map_sqlx_error)?;

        if update.rows_affected() == 0 {
            tx.rollback().await.map_err(map_sqlx_error)?;
            let current = fetch_export_record_by_id(&self.pool, request.export_id()).await?;
            return Ok(PublishCompactionOutcome::StalePlan(current));
        }

        sqlx::query(
            r#"
            UPDATE exports
            SET updated_at = ?
            WHERE id = ?
            "#,
        )
        .bind(now.as_str())
        .bind(request.export_id().as_str())
        .execute(&mut *tx)
        .await
        .map_err(map_sqlx_error)?;

        let published = fetch_export_record_by_id_in_tx(&mut tx, request.export_id()).await?;
        tx.commit().await.map_err(map_sqlx_error)?;
        Ok(PublishCompactionOutcome::Published(published))
    }
}

async fn load_simple_tree_snapshot(
    pool: &SqlitePool,
    export_id: &ExportId,
) -> Result<SimpleTreeSnapshot> {
    let head = load_export_head(pool, export_id).await?;
    let layout_kind = head.layout_kind();
    if layout_kind != ExportLayoutKind::SimpleMutableTree {
        return Err(CatalogError::invalid_field(
            "layout_kind",
            "simple tree metadata requires a simple_mutable_tree export head",
        ));
    }

    let size_bytes = head.size_bytes();
    let root_node_id = head.root_node_id().cloned();

    let mut chunks = BTreeMap::new();
    if let Some(root_node_id) = root_node_id.as_ref() {
        validate_simple_root(pool, export_id, root_node_id).await?;

        let rows = sqlx::query(
            r#"
            SELECT
              e.slot,
              n.layout_kind,
              n.owner_export_id,
              n.kind,
              n.level,
              r.storage_kind,
              r.storage_key,
              r.len_bytes
            FROM tree_edges e
            JOIN tree_nodes n
              ON n.id = e.child_node_id
            LEFT JOIN tree_leaf_refs r
              ON r.node_id = n.id
            WHERE e.parent_node_id = ?
            ORDER BY e.slot ASC
            "#,
        )
        .bind(root_node_id.as_str())
        .fetch_all(pool)
        .await
        .map_err(map_sqlx_error)?;

        for row in rows {
            let slot = i64_to_u64("chunk_index", row.try_get("slot").map_err(map_sqlx_error)?)?;
            let chunk_index = ChunkIndex::new(slot);
            validate_tree_chunk_in_export(chunk_index, size_bytes)?;
            validate_simple_leaf_row(&row, export_id, chunk_index)?;
            let key = row
                .try_get::<Option<String>, _>("storage_key")
                .map_err(map_sqlx_error)?
                .ok_or_else(|| {
                    CatalogError::database(format!(
                        "simple tree leaf for chunk {chunk_index} is missing storage key"
                    ))
                })?;
            let len_bytes = i64_to_u64(
                "len_bytes",
                row.try_get::<Option<i64>, _>("len_bytes")
                    .map_err(map_sqlx_error)?
                    .ok_or_else(|| {
                        CatalogError::database(format!(
                            "simple tree leaf for chunk {chunk_index} is missing length"
                        ))
                    })?,
            )?;
            let chunk = SimpleChunkRef::new(chunk_index, BlobKey::new(key)?, len_bytes)?;
            chunks.insert(chunk_index, chunk);
        }
    }

    SimpleTreeSnapshot::new(export_id.clone(), size_bytes, root_node_id, chunks)
}

struct ReusableCowLeafNode {
    node_id: NodeId,
    blob_key: BlobKey,
    len_bytes: u64,
}

async fn load_reusable_cow_leaf_nodes_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    export_id: &ExportId,
    head: &ExportHead,
) -> Result<BTreeMap<ChunkIndex, ReusableCowLeafNode>> {
    let Some(root_node_id) = head.root_node_id() else {
        return Ok(BTreeMap::new());
    };

    let rows = sqlx::query(
        r#"
        SELECT
          e.slot,
          e.child_node_id,
          n.layout_kind,
          n.owner_export_id,
          n.kind,
          n.level,
          r.storage_kind,
          r.storage_key,
          r.len_bytes
        FROM tree_edges e
        JOIN tree_nodes n
          ON n.id = e.child_node_id
        LEFT JOIN tree_leaf_refs r
          ON r.node_id = n.id
        WHERE e.parent_node_id = ?
        ORDER BY e.slot ASC
        "#,
    )
    .bind(root_node_id.as_str())
    .fetch_all(&mut **tx)
    .await
    .map_err(map_sqlx_error)?;

    let mut nodes = BTreeMap::new();
    for row in rows {
        let slot = i64_to_u64("chunk_index", row.try_get("slot").map_err(map_sqlx_error)?)?;
        let chunk_index = ChunkIndex::new(slot);
        validate_tree_chunk_in_export(chunk_index, head.size_bytes())?;
        validate_cow_leaf_row(&row, export_id, chunk_index)?;
        let child_node_id = NodeId::new(
            row.try_get::<String, _>("child_node_id")
                .map_err(map_sqlx_error)?,
        )?;
        let key = row
            .try_get::<Option<String>, _>("storage_key")
            .map_err(map_sqlx_error)?
            .ok_or_else(|| {
                CatalogError::database(format!(
                    "cow tree leaf for chunk {chunk_index} is missing storage key"
                ))
            })?;
        let len_bytes = i64_to_u64(
            "len_bytes",
            row.try_get::<Option<i64>, _>("len_bytes")
                .map_err(map_sqlx_error)?
                .ok_or_else(|| {
                    CatalogError::database(format!(
                        "cow tree leaf for chunk {chunk_index} is missing length"
                    ))
                })?,
        )?;
        nodes.insert(
            chunk_index,
            ReusableCowLeafNode {
                node_id: child_node_id,
                blob_key: BlobKey::new(key)?,
                len_bytes,
            },
        );
    }

    Ok(nodes)
}

async fn load_cow_tree_snapshot(
    pool: &SqlitePool,
    export_id: &ExportId,
) -> Result<CowTreeSnapshot> {
    let row = sqlx::query(
        r#"
        SELECT layout_kind, root_node_id, size_bytes, checkpoint_wal_seq
        FROM export_heads
        WHERE export_id = ?
        "#,
    )
    .bind(export_id.as_str())
    .fetch_optional(pool)
    .await
    .map_err(map_sqlx_error)?
    .ok_or_else(|| export_head_not_found(export_id))?;

    let layout_kind: String = row.try_get("layout_kind").map_err(map_sqlx_error)?;
    let layout_kind = layout_kind.parse::<ExportLayoutKind>()?;
    if layout_kind != ExportLayoutKind::CowImmutableTree {
        return Err(CatalogError::invalid_field(
            "layout_kind",
            "cow tree metadata requires a cow_immutable_tree export head",
        ));
    }

    let size_bytes = i64_to_u64(
        "size_bytes",
        row.try_get("size_bytes").map_err(map_sqlx_error)?,
    )?;
    let checkpoint_wal_seq = WalSeq::new(i64_to_u64(
        "checkpoint_wal_seq",
        row.try_get("checkpoint_wal_seq").map_err(map_sqlx_error)?,
    )?);
    let root_node_id = row
        .try_get::<Option<String>, _>("root_node_id")
        .map_err(map_sqlx_error)?
        .map(NodeId::new)
        .transpose()?;

    let mut chunks = BTreeMap::new();
    if let Some(root_node_id) = root_node_id.as_ref() {
        validate_cow_root(pool, export_id, root_node_id).await?;

        let rows = sqlx::query(
            r#"
            SELECT
              e.slot,
              n.layout_kind,
              n.owner_export_id,
              n.kind,
              n.level,
              r.storage_kind,
              r.storage_key,
              r.len_bytes
            FROM tree_edges e
            JOIN tree_nodes n
              ON n.id = e.child_node_id
            LEFT JOIN tree_leaf_refs r
              ON r.node_id = n.id
            WHERE e.parent_node_id = ?
            ORDER BY e.slot ASC
            "#,
        )
        .bind(root_node_id.as_str())
        .fetch_all(pool)
        .await
        .map_err(map_sqlx_error)?;

        for row in rows {
            let slot = i64_to_u64("chunk_index", row.try_get("slot").map_err(map_sqlx_error)?)?;
            let chunk_index = ChunkIndex::new(slot);
            validate_tree_chunk_in_export(chunk_index, size_bytes)?;
            validate_cow_leaf_row(&row, export_id, chunk_index)?;
            let key = row
                .try_get::<Option<String>, _>("storage_key")
                .map_err(map_sqlx_error)?
                .ok_or_else(|| {
                    CatalogError::database(format!(
                        "cow tree leaf for chunk {chunk_index} is missing storage key"
                    ))
                })?;
            let len_bytes = i64_to_u64(
                "len_bytes",
                row.try_get::<Option<i64>, _>("len_bytes")
                    .map_err(map_sqlx_error)?
                    .ok_or_else(|| {
                        CatalogError::database(format!(
                            "cow tree leaf for chunk {chunk_index} is missing length"
                        ))
                    })?,
            )?;
            let chunk = CowChunkRef::new(chunk_index, BlobKey::new(key)?, len_bytes)?;
            chunks.insert(chunk_index, chunk);
        }
    }

    CowTreeSnapshot::new(
        export_id.clone(),
        size_bytes,
        root_node_id,
        checkpoint_wal_seq,
        chunks,
    )
}

async fn validate_simple_root(
    pool: &SqlitePool,
    export_id: &ExportId,
    root_node_id: &NodeId,
) -> Result<()> {
    let row = sqlx::query(
        r#"
        SELECT layout_kind, owner_export_id, kind, level
        FROM tree_nodes
        WHERE id = ?
        "#,
    )
    .bind(root_node_id.as_str())
    .fetch_optional(pool)
    .await
    .map_err(map_sqlx_error)?
    .ok_or_else(|| {
        CatalogError::database(format!("simple tree root `{root_node_id}` was not found"))
    })?;

    validate_simple_root_row(&row, export_id, root_node_id)
}

async fn validate_cow_root(
    pool: &SqlitePool,
    export_id: &ExportId,
    root_node_id: &NodeId,
) -> Result<()> {
    let row = sqlx::query(
        r#"
        SELECT layout_kind, owner_export_id, kind, level
        FROM tree_nodes
        WHERE id = ?
        "#,
    )
    .bind(root_node_id.as_str())
    .fetch_optional(pool)
    .await
    .map_err(map_sqlx_error)?
    .ok_or_else(|| {
        CatalogError::database(format!("cow tree root `{root_node_id}` was not found"))
    })?;

    validate_cow_root_row(&row, export_id, root_node_id)
}

async fn validate_simple_root_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    export_id: &ExportId,
    root_node_id: &NodeId,
) -> Result<()> {
    let row = sqlx::query(
        r#"
        SELECT layout_kind, owner_export_id, kind, level
        FROM tree_nodes
        WHERE id = ?
        "#,
    )
    .bind(root_node_id.as_str())
    .fetch_optional(&mut **tx)
    .await
    .map_err(map_sqlx_error)?
    .ok_or_else(|| {
        CatalogError::database(format!("simple tree root `{root_node_id}` was not found"))
    })?;

    validate_simple_root_row(&row, export_id, root_node_id)
}

fn validate_simple_root_row(
    row: &SqliteRow,
    export_id: &ExportId,
    root_node_id: &NodeId,
) -> Result<()> {
    let layout_kind: String = row.try_get("layout_kind").map_err(map_sqlx_error)?;
    if layout_kind != ExportLayoutKind::SimpleMutableTree.to_string() {
        return Err(CatalogError::database(format!(
            "simple tree root `{root_node_id}` has layout `{layout_kind}`"
        )));
    }

    let owner_export_id: Option<String> = row.try_get("owner_export_id").map_err(map_sqlx_error)?;
    if owner_export_id.as_deref() != Some(export_id.as_str()) {
        return Err(CatalogError::database(format!(
            "simple tree root `{root_node_id}` is not owned by export `{export_id}`"
        )));
    }

    let kind: String = row.try_get("kind").map_err(map_sqlx_error)?;
    if kind != "internal" {
        return Err(CatalogError::database(format!(
            "simple tree root `{root_node_id}` has kind `{kind}`"
        )));
    }

    let level: i64 = row.try_get("level").map_err(map_sqlx_error)?;
    if level != 1 {
        return Err(CatalogError::database(format!(
            "simple tree root `{root_node_id}` has level {level}"
        )));
    }

    Ok(())
}

fn validate_cow_root_row(
    row: &SqliteRow,
    _export_id: &ExportId,
    root_node_id: &NodeId,
) -> Result<()> {
    let layout_kind: String = row.try_get("layout_kind").map_err(map_sqlx_error)?;
    if layout_kind != ExportLayoutKind::CowImmutableTree.to_string() {
        return Err(CatalogError::database(format!(
            "cow tree root `{root_node_id}` has layout `{layout_kind}`"
        )));
    }

    let kind: String = row.try_get("kind").map_err(map_sqlx_error)?;
    if kind != "internal" {
        return Err(CatalogError::database(format!(
            "cow tree root `{root_node_id}` has kind `{kind}`"
        )));
    }

    let level: i64 = row.try_get("level").map_err(map_sqlx_error)?;
    if level != 1 {
        return Err(CatalogError::database(format!(
            "cow tree root `{root_node_id}` has level {level}"
        )));
    }

    Ok(())
}

fn validate_simple_leaf_row(
    row: &SqliteRow,
    export_id: &ExportId,
    chunk_index: ChunkIndex,
) -> Result<()> {
    let layout_kind: String = row.try_get("layout_kind").map_err(map_sqlx_error)?;
    if layout_kind != ExportLayoutKind::SimpleMutableTree.to_string() {
        return Err(CatalogError::database(format!(
            "simple tree leaf for chunk {chunk_index} has layout `{layout_kind}`"
        )));
    }

    let owner_export_id: Option<String> = row.try_get("owner_export_id").map_err(map_sqlx_error)?;
    if owner_export_id.as_deref() != Some(export_id.as_str()) {
        return Err(CatalogError::database(format!(
            "simple tree leaf for chunk {chunk_index} is not owned by export `{export_id}`"
        )));
    }

    let kind: String = row.try_get("kind").map_err(map_sqlx_error)?;
    if kind != "leaf" {
        return Err(CatalogError::database(format!(
            "simple tree child for chunk {chunk_index} has kind `{kind}`"
        )));
    }

    let level: i64 = row.try_get("level").map_err(map_sqlx_error)?;
    if level != 0 {
        return Err(CatalogError::database(format!(
            "simple tree leaf for chunk {chunk_index} has level {level}"
        )));
    }

    let storage_kind: Option<String> = row.try_get("storage_kind").map_err(map_sqlx_error)?;
    if storage_kind.as_deref() != Some("mutable_blob") {
        return Err(CatalogError::database(format!(
            "simple tree leaf for chunk {chunk_index} has invalid storage kind"
        )));
    }

    Ok(())
}

fn validate_cow_leaf_row(
    row: &SqliteRow,
    _export_id: &ExportId,
    chunk_index: ChunkIndex,
) -> Result<()> {
    let layout_kind: String = row.try_get("layout_kind").map_err(map_sqlx_error)?;
    if layout_kind != ExportLayoutKind::CowImmutableTree.to_string() {
        return Err(CatalogError::database(format!(
            "cow tree leaf for chunk {chunk_index} has layout `{layout_kind}`"
        )));
    }

    let kind: String = row.try_get("kind").map_err(map_sqlx_error)?;
    if kind != "leaf" {
        return Err(CatalogError::database(format!(
            "cow tree child for chunk {chunk_index} has kind `{kind}`"
        )));
    }

    let level: i64 = row.try_get("level").map_err(map_sqlx_error)?;
    if level != 0 {
        return Err(CatalogError::database(format!(
            "cow tree leaf for chunk {chunk_index} has level {level}"
        )));
    }

    let storage_kind: Option<String> = row.try_get("storage_kind").map_err(map_sqlx_error)?;
    if storage_kind.as_deref() != Some("immutable_blob") {
        return Err(CatalogError::database(format!(
            "cow tree leaf for chunk {chunk_index} has invalid storage kind"
        )));
    }

    Ok(())
}

fn validate_tree_chunk_in_export(chunk_index: ChunkIndex, size_bytes: u64) -> Result<()> {
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

fn export_head_not_found(export_id: &ExportId) -> CatalogError {
    CatalogError::database(format!("export head `{export_id}` not found"))
}

async fn load_export_head(pool: &SqlitePool, export_id: &ExportId) -> Result<ExportHead> {
    let row = sqlx::query(
        r#"
        SELECT layout_kind, root_node_id, size_bytes, checkpoint_wal_seq
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
          h.size_bytes,
          h.checkpoint_wal_seq
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
          h.size_bytes,
          h.checkpoint_wal_seq
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
          h.size_bytes,
          h.checkpoint_wal_seq
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
    let root_node_id: Option<String> = row.try_get("root_node_id").map_err(map_sqlx_error)?;

    ExportHead::new(
        layout_kind.parse::<ExportLayoutKind>()?,
        root_node_id.map(NodeId::new).transpose()?,
        i64_to_u64(
            "size_bytes",
            row.try_get("size_bytes").map_err(map_sqlx_error)?,
        )?,
        WalSeq::new(i64_to_u64(
            "checkpoint_wal_seq",
            row.try_get("checkpoint_wal_seq").map_err(map_sqlx_error)?,
        )?),
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

fn u64_to_i64(field: &'static str, value: u64) -> Result<i64> {
    i64::try_from(value).map_err(|_| {
        CatalogError::invalid_field(
            field,
            format!("value {value} does not fit in SQLite INTEGER"),
        )
    })
}

fn i64_to_u64(field: &'static str, value: i64) -> Result<u64> {
    u64::try_from(value).map_err(|_| {
        CatalogError::invalid_field(field, format!("database value {value} is negative"))
    })
}

fn is_unique_violation(error: &sqlx::Error) -> bool {
    error
        .as_database_error()
        .map(|error| error.is_unique_violation())
        .unwrap_or(false)
}

fn map_sqlx_error(error: sqlx::Error) -> CatalogError {
    CatalogError::database(format!("database error: {error}"))
}
