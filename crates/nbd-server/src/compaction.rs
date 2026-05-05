use crate::{
    observability::{self, event, target},
    ExportWalHandle, LocalBlobStore, OpenWal, Result, ServerError, WalDomain, WalProvider,
};
use nbd_control_plane::{
    ChunkIndex, CowChunkRef, CowTreeMetadataStore, CowTreeSnapshot, ExportHead, ExportId,
    ExportLayoutKind, PublishCompaction, PublishCompactionOutcome, WalSeq, TREE_CHUNK_BYTES,
};
use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::mpsc;

const DEFAULT_COMPACTION_QUEUE_CAPACITY: usize = 16;

#[derive(Clone)]
pub struct CompactionManager {
    worker: Arc<CompactionWorker>,
    queue: CompactionQueue,
}

#[derive(Clone)]
struct CompactionQueue {
    sender: mpsc::Sender<CompactionJob>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactionJob {
    export_id: ExportId,
    through: WalSeq,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompactionEnqueueOutcome {
    Queued,
    DroppedFull,
    ShuttingDown,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactionResult {
    export_id: ExportId,
    target_checkpoint: WalSeq,
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
struct CompactionWorker {
    catalog: Arc<dyn CowTreeMetadataStore>,
    wal_provider: Arc<dyn WalProvider>,
    blob_store: LocalBlobStore,
}

impl CompactionManager {
    pub fn new(
        catalog: Arc<dyn CowTreeMetadataStore>,
        wal_provider: Arc<dyn WalProvider>,
        blob_store: LocalBlobStore,
    ) -> Self {
        Self::with_queue_capacity(
            catalog,
            wal_provider,
            blob_store,
            DEFAULT_COMPACTION_QUEUE_CAPACITY,
        )
    }

    pub fn with_queue_capacity(
        catalog: Arc<dyn CowTreeMetadataStore>,
        wal_provider: Arc<dyn WalProvider>,
        blob_store: LocalBlobStore,
        queue_capacity: usize,
    ) -> Self {
        let worker = Arc::new(CompactionWorker {
            catalog,
            wal_provider,
            blob_store,
        });
        let (sender, receiver) = mpsc::channel(queue_capacity);
        let queue = CompactionQueue { sender };
        spawn_worker(worker.clone(), receiver);

        Self { worker, queue }
    }

    pub fn enqueue(&self, job: CompactionJob) -> CompactionEnqueueOutcome {
        match self.queue.sender.try_send(job) {
            Ok(()) => CompactionEnqueueOutcome::Queued,
            Err(mpsc::error::TrySendError::Full(_)) => CompactionEnqueueOutcome::DroppedFull,
            Err(mpsc::error::TrySendError::Closed(_)) => CompactionEnqueueOutcome::ShuttingDown,
        }
    }

    pub async fn compact_export(&self, job: CompactionJob) -> Result<CompactionResult> {
        self.worker.compact_export(job).await
    }
}

impl CompactionJob {
    pub fn new(export_id: ExportId, through: WalSeq) -> Self {
        Self { export_id, through }
    }

    pub fn export_id(&self) -> &ExportId {
        &self.export_id
    }

    pub fn through(&self) -> WalSeq {
        self.through
    }
}

impl CompactionResult {
    pub fn export_id(&self) -> &ExportId {
        &self.export_id
    }

    pub fn target_checkpoint(&self) -> WalSeq {
        self.target_checkpoint
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

impl CompactionWorker {
    async fn compact_export(&self, job: CompactionJob) -> Result<CompactionResult> {
        let snapshot = self
            .catalog
            .load_cow_tree(job.export_id())
            .await
            .map_err(ServerError::catalog)?;
        let wal = self.open_wal(job.export_id()).await?;
        let bounds = wal.bounds().await?;
        let target_checkpoint = job.through().min(bounds.last_durable);
        let base_checkpoint = snapshot.checkpoint_wal_seq();

        if target_checkpoint <= base_checkpoint {
            return Ok(CompactionResult {
                export_id: job.export_id().clone(),
                target_checkpoint,
                compacted_records: 0,
                written_leaf_blobs: 0,
                outcome: CompactionOutcome::AlreadyCovered,
            });
        }

        let mut replay = wal.replay_range(base_checkpoint, target_checkpoint).await?;
        let mut chunk_images = BTreeMap::new();
        let mut compacted_records = 0u64;
        while let Some(record) = replay.next_record().await? {
            compacted_records += 1;
            apply_record_to_chunks(&self.blob_store, &snapshot, &mut chunk_images, &record).await?;
        }

        if compacted_records == 0 {
            return Ok(CompactionResult {
                export_id: job.export_id().clone(),
                target_checkpoint,
                compacted_records,
                written_leaf_blobs: 0,
                outcome: CompactionOutcome::NoRecords,
            });
        }

        let mut chunks = snapshot.chunks().clone();
        let mut written_leaf_blobs = 0u64;
        for (chunk_index, data) in chunk_images {
            let key = self.blob_store.create_blob(&data).await?;
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
                    job.export_id().clone(),
                    expected_base,
                    target_checkpoint,
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
            export_id: job.export_id().clone(),
            target_checkpoint,
            compacted_records,
            written_leaf_blobs,
            outcome,
        })
    }

    async fn open_wal(&self, export_id: &ExportId) -> Result<ExportWalHandle> {
        self.wal_provider
            .open_export(OpenWal::new(WalDomain::for_export_id(export_id.clone())))
            .await
    }
}

async fn apply_record_to_chunks(
    blob_store: &LocalBlobStore,
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
    blob_store: &LocalBlobStore,
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
                        .read_blob(chunk.blob_key(), 0, TREE_CHUNK_BYTES)
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
        snapshot.checkpoint_wal_seq(),
    )
    .map_err(ServerError::catalog)
}

fn spawn_worker(worker: Arc<CompactionWorker>, mut receiver: mpsc::Receiver<CompactionJob>) {
    tokio::spawn(async move {
        while let Some(job) = receiver.recv().await {
            let export_id = job.export_id().clone();
            let through = job.through();
            match worker.compact_export(job).await {
                Ok(result) => {
                    tracing::info!(
                        target: target::STORAGE,
                        event = event::COMPACTION_COMPLETED,
                        service = observability::SERVICE_NAME,
                        server_instance_id = observability::server_instance_id(),
                        pid = observability::pid(),
                        export_id = %result.export_id(),
                        target_checkpoint = result.target_checkpoint().get(),
                        compacted_records = result.compacted_records(),
                        written_leaf_blobs = result.written_leaf_blobs(),
                        outcome = ?result.outcome(),
                    );
                }
                Err(error) => {
                    tracing::warn!(
                        target: target::STORAGE,
                        event = event::COMPACTION_FAILED,
                        service = observability::SERVICE_NAME,
                        server_instance_id = observability::server_instance_id(),
                        pid = observability::pid(),
                        export_id = %export_id,
                        target_checkpoint = through.get(),
                        error = %error,
                    );
                }
            }
        }
    });
}
