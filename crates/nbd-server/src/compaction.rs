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
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

const DEFAULT_COMPACTION_QUEUE_CAPACITY: usize = 16;

#[derive(Clone)]
pub struct CompactionManager {
    worker: Arc<CompactionWorker>,
    queue: CompactionQueue,
    shutdown: Arc<CompactionShutdownState>,
}

#[derive(Clone)]
struct CompactionQueue {
    sender: mpsc::Sender<CompactionJob>,
    accepting: Arc<AtomicBool>,
}

#[derive(Debug)]
struct CompactionShutdownState {
    signal: Mutex<Option<oneshot::Sender<()>>>,
    worker: Mutex<Option<JoinHandle<CompactionWorkerExit>>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactionJob {
    export_id: ExportId,
    through_wal_seq: WalSeq,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompactionEnqueueOutcome {
    Queued,
    DroppedFull,
    ShuttingDown,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CompactionShutdown {
    dropped_pending_jobs: usize,
}

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
struct CompactionWorker {
    compactor: CowCompactor,
    wal_provider: Arc<dyn WalProvider>,
}

#[derive(Clone)]
pub(crate) struct CowCompactor {
    catalog: Arc<dyn CowTreeMetadataStore>,
    blob_store: LocalBlobStore,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CompactionWorkerExit {
    dropped_pending_jobs: usize,
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
        let compactor = CowCompactor::new(catalog, blob_store);
        let worker = Arc::new(CompactionWorker {
            compactor,
            wal_provider,
        });
        let accepting = Arc::new(AtomicBool::new(true));
        let (sender, receiver) = mpsc::channel(queue_capacity);
        let queue = CompactionQueue {
            sender,
            accepting: accepting.clone(),
        };
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let worker_task = spawn_worker(worker.clone(), receiver, shutdown_rx, accepting);
        let shutdown = Arc::new(CompactionShutdownState {
            signal: Mutex::new(Some(shutdown_tx)),
            worker: Mutex::new(Some(worker_task)),
        });

        Self {
            worker,
            queue,
            shutdown,
        }
    }

    pub fn enqueue(&self, job: CompactionJob) -> CompactionEnqueueOutcome {
        if !self.queue.accepting.load(Ordering::Acquire) {
            return CompactionEnqueueOutcome::ShuttingDown;
        }

        match self.queue.sender.try_send(job) {
            Ok(()) => CompactionEnqueueOutcome::Queued,
            Err(mpsc::error::TrySendError::Full(_)) => {
                if self.queue.accepting.load(Ordering::Acquire) {
                    CompactionEnqueueOutcome::DroppedFull
                } else {
                    CompactionEnqueueOutcome::ShuttingDown
                }
            }
            Err(mpsc::error::TrySendError::Closed(_)) => CompactionEnqueueOutcome::ShuttingDown,
        }
    }

    pub async fn compact_export(&self, job: CompactionJob) -> Result<CompactionResult> {
        self.worker.compact_export(job).await
    }

    pub fn request_shutdown(&self) {
        self.queue.accepting.store(false, Ordering::Release);
        let Ok(mut signal) = self.shutdown.signal.lock() else {
            return;
        };
        if let Some(signal) = signal.take() {
            let _ = signal.send(());
        }
    }

    pub async fn shutdown(&self) -> Result<CompactionShutdown> {
        self.request_shutdown();
        let worker = {
            let mut worker =
                self.shutdown
                    .worker
                    .lock()
                    .map_err(|_| ServerError::LockPoisoned {
                        resource: "compaction worker shutdown",
                    })?;
            worker.take()
        };

        let Some(worker) = worker else {
            return Ok(CompactionShutdown::default());
        };
        let worker_exit = worker.await.map_err(|source| {
            ServerError::io(
                "join compaction worker",
                std::io::Error::other(source.to_string()),
            )
        })?;
        Ok(CompactionShutdown {
            dropped_pending_jobs: worker_exit.dropped_pending_jobs,
        })
    }
}

impl CompactionJob {
    pub fn new(export_id: ExportId, through_wal_seq: WalSeq) -> Self {
        Self {
            export_id,
            through_wal_seq,
        }
    }

    pub fn export_id(&self) -> &ExportId {
        &self.export_id
    }

    pub fn through_wal_seq(&self) -> WalSeq {
        self.through_wal_seq
    }
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

impl CompactionShutdown {
    pub fn dropped_pending_jobs(&self) -> usize {
        self.dropped_pending_jobs
    }
}

impl CompactionWorker {
    async fn compact_export(&self, job: CompactionJob) -> Result<CompactionResult> {
        let wal = self.open_wal(job.export_id()).await?;
        self.compactor
            .compact_export(job.export_id(), &wal, job.through_wal_seq())
            .await
    }

    async fn open_wal(&self, export_id: &ExportId) -> Result<ExportWalHandle> {
        self.wal_provider
            .open_export(OpenWal::new(WalDomain::for_export_id(export_id.clone())))
            .await
    }
}

impl CowCompactor {
    pub(crate) fn new(catalog: Arc<dyn CowTreeMetadataStore>, blob_store: LocalBlobStore) -> Self {
        Self {
            catalog,
            blob_store,
        }
    }

    pub(crate) async fn compact_export(
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
            apply_record_to_chunks(&self.blob_store, &snapshot, &mut chunk_images, &record).await?;
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
        snapshot.base_wal_seq(),
    )
    .map_err(ServerError::catalog)
}

fn spawn_worker(
    worker: Arc<CompactionWorker>,
    mut receiver: mpsc::Receiver<CompactionJob>,
    mut shutdown: oneshot::Receiver<()>,
    accepting: Arc<AtomicBool>,
) -> JoinHandle<CompactionWorkerExit> {
    tokio::spawn(async move {
        let mut dropped_pending_jobs = 0usize;
        loop {
            if !accepting.load(Ordering::Acquire) {
                receiver.close();
                dropped_pending_jobs += drain_pending_jobs(&mut receiver);
                break;
            }

            tokio::select! {
                biased;

                _ = &mut shutdown => {
                    accepting.store(false, Ordering::Release);
                    receiver.close();
                    dropped_pending_jobs += drain_pending_jobs(&mut receiver);
                    break;
                }
                job = receiver.recv() => {
                    let Some(job) = job else {
                        break;
                    };
                    process_job(&worker, job).await;
                }
            }
        }

        CompactionWorkerExit {
            dropped_pending_jobs,
        }
    })
}

async fn process_job(worker: &Arc<CompactionWorker>, job: CompactionJob) {
    let export_id = job.export_id().clone();
    let through_wal_seq = job.through_wal_seq();
    match worker.compact_export(job).await {
        Ok(result) => {
            tracing::info!(
                target: target::WAL,
                event = event::WAL_COMPACTION_COMPLETED,
                service = observability::SERVICE_NAME,
                server_instance_id = observability::server_instance_id(),
                pid = observability::pid(),
                export_id = %result.export_id(),
                base_wal_seq = result.base_wal_seq().get(),
                target_wal_seq = result.target_wal_seq().get(),
                compacted_records = result.compacted_records(),
                written_leaf_blobs = result.written_leaf_blobs(),
                outcome = ?result.outcome(),
            );
        }
        Err(error) => {
            tracing::warn!(
                target: target::WAL,
                event = event::WAL_COMPACTION_FAILED,
                service = observability::SERVICE_NAME,
                server_instance_id = observability::server_instance_id(),
                pid = observability::pid(),
                export_id = %export_id,
                target_wal_seq = through_wal_seq.get(),
                error = %error,
            );
        }
    }
}

fn drain_pending_jobs(receiver: &mut mpsc::Receiver<CompactionJob>) -> usize {
    let mut dropped = 0usize;
    while receiver.try_recv().is_ok() {
        dropped += 1;
    }
    dropped
}
