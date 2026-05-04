//! SQLite implementation of the export catalog.

use crate::{
    CatalogError, CatalogProvider, CatalogUrl, CreateExport, DeleteExport, ExportCatalog,
    ExportEngineKind, ExportHead, ExportId, ExportLayoutKind, ExportMeta, ExportName, ExportState,
    InspectExport, ListExports, NodeId, Result, Timestamp, WalSeq,
};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions, SqliteRow};
use sqlx::{ConnectOptions, Row, SqlitePool};
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

    async fn fetch_by_name(&self, name: &ExportName) -> Result<ExportMeta> {
        let row = sqlx::query(
            r#"
            SELECT
              e.id,
              e.name,
              g.size_bytes,
              e.block_size,
              e.engine_kind,
              e.state,
              e.created_at,
              e.updated_at,
              e.deleted_at,
              g.root_node_id,
              g.checkpoint_wal_seq
            FROM exports e
            JOIN export_generations g
              ON g.export_id = e.id
            WHERE e.name = ?
            ORDER BY g.generation DESC
            LIMIT 1
            "#,
        )
        .bind(name.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(map_sqlx_error)?;

        row.map(|row| row_to_export_meta(&row))
            .unwrap_or_else(|| Err(CatalogError::ExportNotFound { name: name.clone() }))
    }
}

#[async_trait::async_trait]
impl ExportCatalog for SQLiteExportCatalog {
    async fn create_export(&self, request: CreateExport) -> Result<ExportMeta> {
        let export_id = ExportId::new(Uuid::new_v4().to_string())?;
        let generation_id = Uuid::new_v4().to_string();
        let now = current_timestamp()?;
        let size_bytes = u64_to_i64("size_bytes", request.size_bytes())?;
        let block_size = u64_to_i64("block_size", request.block_size())?;
        let checkpoint_wal_seq = WalSeq::zero();

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
            INSERT INTO export_generations (
              id, export_id, generation, size_bytes, root_node_id,
              checkpoint_wal_seq, created_at
            )
            VALUES (?, ?, ?, ?, NULL, ?, ?)
            "#,
        )
        .bind(generation_id)
        .bind(export_id.as_str())
        .bind(0_i64)
        .bind(size_bytes)
        .bind(u64_to_i64("checkpoint_wal_seq", checkpoint_wal_seq.get())?)
        .bind(now.as_str())
        .execute(&mut *tx)
        .await
        .map_err(map_sqlx_error)?;

        tx.commit().await.map_err(map_sqlx_error)?;

        ExportMeta::new(
            export_id,
            request.name().clone(),
            request.block_size(),
            request.engine_kind(),
            ExportState::Active,
            ExportHead::memory_empty(request.size_bytes())?,
            now.clone(),
            now,
            None,
        )
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

    async fn load_export(&self, name: ExportName) -> Result<ExportMeta> {
        let meta = self.fetch_by_name(&name).await?;
        if meta.state() == ExportState::Deleted {
            Err(CatalogError::ExportDeleted { name })
        } else {
            Ok(meta)
        }
    }

    async fn inspect_export(&self, request: InspectExport) -> Result<ExportMeta> {
        self.fetch_by_name(request.name()).await
    }

    async fn list_exports(&self, request: ListExports) -> Result<Vec<ExportMeta>> {
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
              g.size_bytes,
              e.block_size,
              e.engine_kind,
              e.state,
              e.created_at,
              e.updated_at,
              e.deleted_at,
              g.root_node_id,
              g.checkpoint_wal_seq
            FROM exports e
            JOIN export_generations g
              ON g.export_id = e.id
             AND g.generation = (
              SELECT MAX(g2.generation)
              FROM export_generations g2
              WHERE g2.export_id = e.id
            )
            WHERE (? = 1 OR e.state != 'deleted')
            ORDER BY e.name ASC
            "#,
        )
        .bind(include_deleted)
        .fetch_all(&self.pool)
        .await
        .map_err(map_sqlx_error)?;

        rows.iter().map(row_to_export_meta).collect()
    }
}

fn row_to_export_meta(row: &SqliteRow) -> Result<ExportMeta> {
    let state: String = row.try_get("state").map_err(map_sqlx_error)?;
    let engine_kind: String = row.try_get("engine_kind").map_err(map_sqlx_error)?;
    let root_node_id: Option<String> = row.try_get("root_node_id").map_err(map_sqlx_error)?;
    let deleted_at: Option<String> = row.try_get("deleted_at").map_err(map_sqlx_error)?;

    ExportMeta::new(
        ExportId::new(row.try_get::<String, _>("id").map_err(map_sqlx_error)?)?,
        ExportName::new(row.try_get::<String, _>("name").map_err(map_sqlx_error)?)?,
        i64_to_u64(
            "block_size",
            row.try_get("block_size").map_err(map_sqlx_error)?,
        )?,
        engine_kind.parse::<ExportEngineKind>()?,
        state.parse()?,
        ExportHead::new(
            ExportLayoutKind::MemoryEmpty,
            root_node_id.map(NodeId::new).transpose()?,
            i64_to_u64(
                "size_bytes",
                row.try_get("size_bytes").map_err(map_sqlx_error)?,
            )?,
            WalSeq::new(i64_to_u64(
                "checkpoint_wal_seq",
                row.try_get("checkpoint_wal_seq").map_err(map_sqlx_error)?,
            )?),
        )?,
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
