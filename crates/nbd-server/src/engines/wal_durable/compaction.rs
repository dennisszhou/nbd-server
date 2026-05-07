use crate::error::{Result, ServerError};
use crate::storage::{BlobStore, BlobStoreHandle, put_random_blob};
use crate::wal::ExportWalHandle;
use nbd_control_plane::{
    ChunkIndex, CowChunkRef, CowTreeMetadataStore, CowTreeSnapshot, ExportHead, ExportId,
    ExportLayoutKind, PublishCompaction, PublishCompactionOutcome, TREE_CHUNK_BYTES, WalSeq,
};
use std::collections::BTreeMap;
use std::collections::btree_map::Entry;
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactionResult {
    export_id: ExportId,
    base_wal_seq: WalSeq,
    target_wal_seq: WalSeq,
    compacted_records: u64,
    written_leaf_blobs: u64,
    outcome: CompactionOutcome,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompactionOutcome {
    Published,
    AlreadyCovered,
    StalePlan,
    NoRecords,
}

#[derive(Clone)]
pub struct CowCompactor {
    catalog: Arc<dyn CowTreeMetadataStore>,
    blob_store: BlobStoreHandle,
}

impl CompactionResult {
    pub fn export_id(&self) -> &ExportId {
        &self.export_id
    }

    pub fn base_wal_seq(&self) -> WalSeq {
        self.base_wal_seq
    }

    pub fn target_wal_seq(&self) -> WalSeq {
        self.target_wal_seq
    }

    pub fn compacted_records(&self) -> u64 {
        self.compacted_records
    }

    pub fn written_leaf_blobs(&self) -> u64 {
        self.written_leaf_blobs
    }

    pub fn outcome(&self) -> CompactionOutcome {
        self.outcome
    }
}

impl CowCompactor {
    pub fn new(catalog: Arc<dyn CowTreeMetadataStore>, blob_store: BlobStoreHandle) -> Self {
        Self {
            catalog,
            blob_store,
        }
    }

    pub async fn compact_export(
        &self,
        export_id: &ExportId,
        wal: &ExportWalHandle,
        through_wal_seq: WalSeq,
    ) -> Result<CompactionResult> {
        let snapshot = self
            .catalog
            .load_cow_tree(export_id)
            .await
            .map_err(ServerError::catalog)?;
        let bounds = wal.bounds().await?;
        let target_wal_seq = through_wal_seq.min(bounds.last_durable);
        let base_wal_seq = snapshot.base_wal_seq();

        if target_wal_seq <= base_wal_seq {
            return Ok(CompactionResult {
                export_id: export_id.clone(),
                base_wal_seq,
                target_wal_seq,
                compacted_records: 0,
                written_leaf_blobs: 0,
                outcome: CompactionOutcome::AlreadyCovered,
            });
        }

        let mut replay = wal.replay_range(base_wal_seq, target_wal_seq).await?;
        let mut chunk_images = BTreeMap::new();
        let mut compacted_records = 0u64;
        while let Some(record) = replay.next_record().await? {
            compacted_records += 1;
            apply_record_to_chunks(
                self.blob_store.as_ref(),
                &snapshot,
                &mut chunk_images,
                &record,
            )
            .await?;
        }

        if compacted_records == 0 {
            return Ok(CompactionResult {
                export_id: export_id.clone(),
                base_wal_seq,
                target_wal_seq,
                compacted_records,
                written_leaf_blobs: 0,
                outcome: CompactionOutcome::NoRecords,
            });
        }

        let mut chunks = snapshot.chunks().clone();
        let mut written_leaf_blobs = 0u64;
        for (chunk_index, data) in chunk_images {
            let key = put_random_blob(self.blob_store.as_ref(), &data).await?;
            let chunk = CowChunkRef::new(chunk_index, key, TREE_CHUNK_BYTES)
                .map_err(ServerError::catalog)?;
            chunks.insert(chunk_index, chunk);
            written_leaf_blobs += 1;
        }

        let expected_base = snapshot_to_export_head(&snapshot)?;
        let publication = self
            .catalog
            .publish_compaction(
                PublishCompaction::new(
                    export_id.clone(),
                    expected_base,
                    target_wal_seq,
                    chunks.into_values().collect(),
                )
                .map_err(ServerError::catalog)?,
            )
            .await
            .map_err(ServerError::catalog)?;
        let outcome = match publication {
            PublishCompactionOutcome::Published(_) => CompactionOutcome::Published,
            PublishCompactionOutcome::AlreadyCovered(_) => CompactionOutcome::AlreadyCovered,
            PublishCompactionOutcome::StalePlan(_) => CompactionOutcome::StalePlan,
        };

        Ok(CompactionResult {
            export_id: export_id.clone(),
            base_wal_seq,
            target_wal_seq,
            compacted_records,
            written_leaf_blobs,
            outcome,
        })
    }
}

async fn apply_record_to_chunks(
    blob_store: &dyn BlobStore,
    snapshot: &CowTreeSnapshot,
    chunk_images: &mut BTreeMap<ChunkIndex, Vec<u8>>,
    record: &crate::WalRecord,
) -> Result<()> {
    validate_record_range(snapshot, record)?;
    let payload = record.data();
    let mut copied = 0usize;
    while copied < payload.len() {
        let current_offset = record.range().start() + copied as u64;
        let chunk_index = ChunkIndex::new(current_offset / TREE_CHUNK_BYTES);
        let chunk_offset = (current_offset % TREE_CHUNK_BYTES) as usize;
        let chunk_available = TREE_CHUNK_BYTES as usize - chunk_offset;
        let copy_len = chunk_available.min(payload.len() - copied);
        let chunk = load_or_create_chunk(blob_store, snapshot, chunk_images, chunk_index).await?;
        chunk[chunk_offset..chunk_offset + copy_len]
            .copy_from_slice(&payload[copied..copied + copy_len]);
        copied += copy_len;
    }
    Ok(())
}

async fn load_or_create_chunk<'a>(
    blob_store: &dyn BlobStore,
    snapshot: &CowTreeSnapshot,
    chunk_images: &'a mut BTreeMap<ChunkIndex, Vec<u8>>,
    chunk_index: ChunkIndex,
) -> Result<&'a mut Vec<u8>> {
    match chunk_images.entry(chunk_index) {
        Entry::Occupied(entry) => Ok(entry.into_mut()),
        Entry::Vacant(entry) => {
            let data = match snapshot.chunk(chunk_index) {
                Some(chunk) => {
                    blob_store
                        .get_blob(chunk.blob_key(), 0, TREE_CHUNK_BYTES)
                        .await?
                }
                None => vec![0; TREE_CHUNK_BYTES as usize],
            };
            Ok(entry.insert(data))
        }
    }
}

fn validate_record_range(snapshot: &CowTreeSnapshot, record: &crate::WalRecord) -> Result<()> {
    let end = record
        .range()
        .start()
        .checked_add(record.range().len())
        .ok_or(ServerError::OutOfBounds {
            operation: "compact WAL record",
            offset: record.range().start(),
            length: record.range().len(),
            size_bytes: snapshot.size_bytes(),
        })?;
    if end > snapshot.size_bytes() {
        return Err(ServerError::OutOfBounds {
            operation: "compact WAL record",
            offset: record.range().start(),
            length: record.range().len(),
            size_bytes: snapshot.size_bytes(),
        });
    }
    Ok(())
}

fn snapshot_to_export_head(snapshot: &CowTreeSnapshot) -> Result<ExportHead> {
    ExportHead::new(
        ExportLayoutKind::CowImmutableTree,
        snapshot.root_node_id().cloned(),
        snapshot.size_bytes(),
        snapshot.base_wal_seq(),
    )
    .map_err(ServerError::catalog)
}
