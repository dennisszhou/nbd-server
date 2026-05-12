mod admission;
mod compaction;
mod extent_map;
mod overlay;
mod read_cache;
mod read_view;

pub use admission::WalDurableAdmissionPolicy;
pub use compaction::{CompactionOutcome, CompactionResult, CowCompactor};
pub use read_view::{ExportReadView, RootSnapshot};

use read_view::{CowTreeReader, validate_range, validate_request_range};

use crate::error::{Result, ServerError};
use crate::export::{
    AdmittedExportRequest, ExportAdmissionPolicyHandle, ExportEngine, ExportReply, ExportRequest,
    ExportResult,
};
use crate::observability::{self, event, target};
use crate::range::ByteRange;
use crate::storage::BlobStoreHandle;
use crate::wal::{ExportWalHandle, WalRequest};
use nbd_control_plane::{
    ActiveExportDescriptor, ExportHead, ExportId, ExportLayoutKind, ExportName, ExportRecord,
    NodeId, TreeRecordStore, WalSeq,
};
use std::fmt;
use std::sync::Arc;
use tokio::sync::{Mutex, watch};
use tokio::task::JoinHandle;
use tokio::time::{self, Duration, MissedTickBehavior};

/// WAL-backed durable engine using a retained WAL overlay over committed state.
pub struct WalDurableEngine {
    name: ExportName,
    size_bytes: u64,
    block_size: u64,
    wal: ExportWalHandle,
    read_view: Arc<ExportReadView>,
    compaction: Option<Arc<CompactionCoordinator>>,
    background_compaction: Option<BackgroundCompactionTask>,
    compaction_policy: CompactionPolicy,
    write_lock: Mutex<()>,
}

/// Engine-local compaction lifecycle for one open WAL durable export.
struct CompactionCoordinator {
    export_id: ExportId,
    export_name: ExportName,
    wal: ExportWalHandle,
    compactor: CowCompactor,
    read_view: Arc<ExportReadView>,
    compaction_lock: Mutex<()>,
}

/// Join-once lifecycle handle for one export's background compaction task.
struct BackgroundCompactionTask {
    shutdown: watch::Sender<bool>,
    task: Mutex<Option<JoinHandle<()>>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CompactionPolicy {
    background_wal_debt_threshold_bytes: u64,
    hard_wal_debt_threshold_bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BackgroundCompactionTick {
    SkippedBelowSoftThreshold,
    SkippedAtHardThreshold,
    SkippedBusy,
    SkippedNoSnapshot,
    Compacted(CompactionOutcome),
    Failed,
}

const DEFAULT_BACKGROUND_WAL_DEBT_COMPACTION_THRESHOLD_BYTES: u64 = 512 * 1024 * 1024;
const DEFAULT_BACKGROUND_COMPACTION_INTERVAL: Duration = Duration::from_secs(30);
const DEFAULT_WAL_DEBT_COMPACTION_THRESHOLD_BYTES: u64 = 2 * 1024 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct WalReplaySummary {
    replayed_records: u64,
    replayed_through_wal_seq: WalSeq,
}

impl WalDurableEngine {
    pub async fn open(meta: &ExportRecord, wal: ExportWalHandle) -> Result<Self> {
        if meta.head().layout_kind() != ExportLayoutKind::CowImmutableTree {
            return Err(ServerError::Catalog {
                message: format!(
                    "export `{}` does not have a cow immutable tree head",
                    meta.name()
                ),
                source: None,
            });
        }
        if meta.head().root_node_id().is_some() {
            return Err(ServerError::Catalog {
                message: format!(
                    "export `{}` has a committed COW root that is not readable yet",
                    meta.name()
                ),
                source: None,
            });
        }

        let root = RootSnapshot::from_meta(meta);
        log_root_loaded(meta.id(), meta.name(), &root);
        let read_view = Arc::new(ExportReadView::zero_filled(root.clone()));
        let replay = replay_wal_after(&wal, read_view.as_ref(), &root).await?;
        log_replay_completed(meta.id(), meta.name(), &root, replay);

        Ok(Self {
            name: meta.name().clone(),
            size_bytes: meta.size_bytes(),
            block_size: meta.block_size(),
            wal,
            read_view,
            compaction: None,
            background_compaction: None,
            compaction_policy: CompactionPolicy::default(),
            write_lock: Mutex::new(()),
        })
    }

    pub async fn open_with_cow_tree(
        descriptor: &ActiveExportDescriptor,
        wal: ExportWalHandle,
        blob_store: BlobStoreHandle,
        tree_store: Arc<dyn TreeRecordStore>,
        head: ExportHead,
    ) -> Result<Self> {
        Self::open_with_cow_tree_and_wal_debt_threshold(
            descriptor,
            wal,
            blob_store,
            tree_store,
            head,
            DEFAULT_WAL_DEBT_COMPACTION_THRESHOLD_BYTES,
        )
        .await
    }

    #[doc(hidden)]
    pub async fn open_with_cow_tree_and_wal_debt_threshold(
        descriptor: &ActiveExportDescriptor,
        wal: ExportWalHandle,
        blob_store: BlobStoreHandle,
        tree_store: Arc<dyn TreeRecordStore>,
        head: ExportHead,
        wal_debt_threshold_bytes: u64,
    ) -> Result<Self> {
        Self::open_with_cow_tree_and_compaction_policy(
            descriptor,
            wal,
            blob_store,
            tree_store,
            head,
            CompactionPolicy::with_hard_threshold(wal_debt_threshold_bytes),
            DEFAULT_BACKGROUND_COMPACTION_INTERVAL,
        )
        .await
    }

    async fn open_with_cow_tree_and_compaction_policy(
        descriptor: &ActiveExportDescriptor,
        wal: ExportWalHandle,
        blob_store: BlobStoreHandle,
        tree_store: Arc<dyn TreeRecordStore>,
        head: ExportHead,
        compaction_policy: CompactionPolicy,
        background_interval: Duration,
    ) -> Result<Self> {
        if descriptor.engine_kind() != nbd_control_plane::ExportEngineKind::WalDurable {
            return Err(ServerError::Catalog {
                message: format!("export `{}` is not a wal_durable export", descriptor.name()),
                source: None,
            });
        }

        validate_head_can_open(descriptor, &head)?;
        let root = RootSnapshot::from_head(&head)?;
        let size_bytes = root.size_bytes();
        log_root_loaded(descriptor.id(), descriptor.name(), &root);
        let read_view = Arc::new(ExportReadView::new(
            root.clone(),
            Arc::new(CowTreeReader {
                blob_store: blob_store.clone(),
                store: tree_store.clone(),
            }),
        ));
        let replay = replay_wal_after(&wal, read_view.as_ref(), &root).await?;
        log_replay_completed(descriptor.id(), descriptor.name(), &root, replay);
        let compaction = Arc::new(CompactionCoordinator::new(
            descriptor.id().clone(),
            descriptor.name().clone(),
            wal.clone(),
            tree_store,
            blob_store,
            read_view.clone(),
        ));
        let background_compaction = BackgroundCompactionTask::spawn(
            compaction.clone(),
            compaction_policy,
            background_interval,
        );

        Ok(Self {
            name: descriptor.name().clone(),
            size_bytes,
            block_size: descriptor.block_size(),
            wal,
            read_view,
            compaction: Some(compaction),
            background_compaction: Some(background_compaction),
            compaction_policy,
            write_lock: Mutex::new(()),
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

    pub async fn export_head(&self) -> Result<nbd_control_plane::ExportHead> {
        self.read_view.export_head().await
    }

    pub async fn wal_debt_bytes(&self) -> u64 {
        self.read_view.wal_debt_bytes().await
    }

    async fn read(&self, offset: u64, len: u32) -> Result<Vec<u8>> {
        let range = ByteRange::new(offset, len);
        validate_range("read", range, self.size_bytes)?;
        self.read_view.read(range).await
    }

    async fn write(&self, offset: u64, data: Vec<u8>) -> Result<()> {
        validate_request_range("write", offset, data.len() as u64, self.size_bytes)?;
        if data.is_empty() {
            return Ok(());
        }

        let len = u32::try_from(data.len()).map_err(|_| ServerError::OutOfBounds {
            operation: "write",
            offset,
            length: data.len() as u64,
            size_bytes: self.size_bytes,
        })?;
        let range = ByteRange::new(offset, len);
        let request = WalRequest::new(range, data)?;
        let _write = self.write_lock.lock().await;
        let record = self.wal.append(request).await?;
        self.read_view.apply_wal_record(record).await?;
        self.compact_if_wal_debt_exceeds_threshold().await;
        Ok(())
    }

    fn flush(&self) -> Result<()> {
        Ok(())
    }

    async fn compact_if_wal_debt_exceeds_threshold(&self) {
        let wal_debt_bytes = self.read_view.wal_debt_bytes().await;
        if wal_debt_bytes < self.compaction_policy.hard_wal_debt_threshold_bytes {
            return;
        }
        let Some(compaction) = &self.compaction else {
            return;
        };

        compaction
            .compact_write_pressure_best_effort(
                self.compaction_policy.hard_wal_debt_threshold_bytes,
            )
            .await;
    }
}

impl CompactionPolicy {
    fn with_hard_threshold(hard_wal_debt_threshold_bytes: u64) -> Self {
        Self {
            background_wal_debt_threshold_bytes:
                DEFAULT_BACKGROUND_WAL_DEBT_COMPACTION_THRESHOLD_BYTES,
            hard_wal_debt_threshold_bytes,
        }
    }
}

impl Default for CompactionPolicy {
    fn default() -> Self {
        Self::with_hard_threshold(DEFAULT_WAL_DEBT_COMPACTION_THRESHOLD_BYTES)
    }
}

impl BackgroundCompactionTask {
    fn spawn(
        compaction: Arc<CompactionCoordinator>,
        policy: CompactionPolicy,
        interval: Duration,
    ) -> Self {
        let (shutdown, mut shutdown_rx) = watch::channel(false);
        let task = tokio::spawn(async move {
            let mut ticks = time::interval(interval);
            ticks.set_missed_tick_behavior(MissedTickBehavior::Delay);
            ticks.tick().await;

            loop {
                tokio::select! {
                    changed = shutdown_rx.changed() => {
                        if changed.is_err() || *shutdown_rx.borrow() {
                            break;
                        }
                    }
                    _ = ticks.tick() => {
                        if *shutdown_rx.borrow() {
                            break;
                        }
                        compaction.compact_background_tick(policy).await;
                    }
                }
            }
        });

        Self {
            shutdown,
            task: Mutex::new(Some(task)),
        }
    }

    async fn stop(&self) {
        let _ = self.shutdown.send(true);
        let mut task = self.task.lock().await;
        if let Some(task) = task.take() {
            if let Err(error) = task.await {
                tracing::warn!(
                    target: target::WAL,
                    event = event::WAL_COMPACTION_FAILED,
                    service = observability::SERVICE_NAME,
                    server_instance_id = observability::server_instance_id(),
                    pid = observability::pid(),
                    phase = "background_shutdown",
                    error = %error,
                );
            }
        }
    }
}

#[async_trait::async_trait]
impl ExportEngine for WalDurableEngine {
    fn admission_policy(&self) -> ExportAdmissionPolicyHandle {
        Arc::new(WalDurableAdmissionPolicy::new(self.size_bytes))
    }

    async fn execute_admitted(&self, request: AdmittedExportRequest) -> ExportResult {
        match request.request() {
            ExportRequest::Read { offset, len } => Ok(ExportReply::Read {
                data: self.read(*offset, *len).await?,
            }),
            ExportRequest::Write { .. } => {
                let mut owned = request.into_owned();
                let ExportRequest::Write { offset, data } = owned.take_request() else {
                    unreachable!("matched write request before taking ownership");
                };
                self.write(offset, data).await?;
                Ok(ExportReply::Done)
            }
            ExportRequest::Flush => {
                self.flush()?;
                Ok(ExportReply::Done)
            }
        }
    }

    async fn close(&self) -> Result<()> {
        if let Some(background) = &self.background_compaction {
            background.stop().await;
        }
        if let Some(compaction) = &self.compaction {
            compaction.compact_close_best_effort().await;
        }
        Ok(())
    }
}

impl CompactionCoordinator {
    fn new(
        export_id: ExportId,
        export_name: ExportName,
        wal: ExportWalHandle,
        tree_store: Arc<dyn TreeRecordStore>,
        blob_store: BlobStoreHandle,
        read_view: Arc<ExportReadView>,
    ) -> Self {
        let compactor = CowCompactor::new(tree_store, blob_store);
        Self {
            export_id,
            export_name,
            wal,
            compactor,
            read_view,
            compaction_lock: Mutex::new(()),
        }
    }

    async fn compact_close_best_effort(&self) {
        let snapshot = match self.read_view.capture_compaction_snapshot().await {
            Ok(Some(snapshot)) => snapshot,
            Ok(None) => {
                match self.read_view.export_head().await {
                    Ok(head) => self.prune_published_wal(head.base_wal_seq()).await,
                    Err(error) => {
                        self.log_compaction_failed(
                            WalSeq::zero(),
                            self.read_view.wal_debt_bytes().await,
                            "engine_close",
                            &error,
                        );
                    }
                }
                return;
            }
            Err(error) => {
                self.log_compaction_failed(
                    WalSeq::zero(),
                    self.read_view.wal_debt_bytes().await,
                    "engine_close",
                    &error,
                );
                return;
            }
        };
        self.compact_snapshot_best_effort(snapshot, "engine_close")
            .await;
    }

    async fn compact_snapshot_best_effort(
        &self,
        snapshot: read_view::ReadViewCompactionSnapshot,
        phase: &'static str,
    ) {
        let target_wal_seq = snapshot.target_wal_seq;
        let wal_debt_bytes = snapshot.wal_debt_bytes;
        match self.compact_snapshot(snapshot).await {
            Ok(result) => log_compaction_completed(&self.export_name, &result, phase),
            Err(error) => {
                self.log_compaction_failed(target_wal_seq, wal_debt_bytes, phase, &error);
            }
        }
    }

    async fn compact_background_tick(&self, policy: CompactionPolicy) -> BackgroundCompactionTick {
        let wal_debt_bytes = self.read_view.wal_debt_bytes().await;
        if wal_debt_bytes < policy.background_wal_debt_threshold_bytes {
            return BackgroundCompactionTick::SkippedBelowSoftThreshold;
        }
        if wal_debt_bytes >= policy.hard_wal_debt_threshold_bytes {
            return BackgroundCompactionTick::SkippedAtHardThreshold;
        }

        let Ok(_compaction) = self.compaction_lock.try_lock() else {
            return BackgroundCompactionTick::SkippedBusy;
        };
        let wal_debt_bytes = self.read_view.wal_debt_bytes().await;
        if wal_debt_bytes < policy.background_wal_debt_threshold_bytes {
            return BackgroundCompactionTick::SkippedBelowSoftThreshold;
        }
        if wal_debt_bytes >= policy.hard_wal_debt_threshold_bytes {
            return BackgroundCompactionTick::SkippedAtHardThreshold;
        }

        let snapshot = match self.read_view.capture_compaction_snapshot().await {
            Ok(Some(snapshot)) => snapshot,
            Ok(None) => return BackgroundCompactionTick::SkippedNoSnapshot,
            Err(error) => {
                self.log_compaction_failed(WalSeq::zero(), wal_debt_bytes, "background", &error);
                return BackgroundCompactionTick::Failed;
            }
        };
        let target_wal_seq = snapshot.target_wal_seq;
        let snapshot_wal_debt_bytes = snapshot.wal_debt_bytes;
        match self.compact_snapshot_locked(snapshot).await {
            Ok(result) => {
                let outcome = result.outcome();
                log_compaction_completed(&self.export_name, &result, "background");
                BackgroundCompactionTick::Compacted(outcome)
            }
            Err(error) => {
                self.log_compaction_failed(
                    target_wal_seq,
                    snapshot_wal_debt_bytes,
                    "background",
                    &error,
                );
                BackgroundCompactionTick::Failed
            }
        }
    }

    async fn compact_write_pressure_best_effort(&self, hard_threshold_bytes: u64) {
        let _compaction = self.compaction_lock.lock().await;
        let wal_debt_bytes = self.read_view.wal_debt_bytes().await;
        if wal_debt_bytes < hard_threshold_bytes {
            return;
        }

        let snapshot = match self.read_view.capture_compaction_snapshot().await {
            Ok(Some(snapshot)) => snapshot,
            Ok(None) => return,
            Err(error) => {
                self.log_compaction_failed(
                    WalSeq::zero(),
                    wal_debt_bytes,
                    "write_pressure",
                    &error,
                );
                return;
            }
        };
        let target_wal_seq = snapshot.target_wal_seq;
        let snapshot_wal_debt_bytes = snapshot.wal_debt_bytes;
        match self.compact_snapshot_locked(snapshot).await {
            Ok(result) => log_compaction_completed(&self.export_name, &result, "write_pressure"),
            Err(error) => {
                self.log_compaction_failed(
                    target_wal_seq,
                    snapshot_wal_debt_bytes,
                    "write_pressure",
                    &error,
                );
            }
        }
    }

    async fn compact_snapshot(
        &self,
        snapshot: read_view::ReadViewCompactionSnapshot,
    ) -> Result<CompactionResult> {
        let _compaction = self.compaction_lock.lock().await;
        self.compact_snapshot_locked(snapshot).await
    }

    async fn compact_snapshot_locked(
        &self,
        snapshot: read_view::ReadViewCompactionSnapshot,
    ) -> Result<CompactionResult> {
        let result = self
            .compactor
            .compact_snapshot(&self.export_id, &snapshot)
            .await?;
        self.advance_after_snapshot_compaction(&result, &snapshot)
            .await?;
        Ok(result)
    }

    async fn advance_after_snapshot_compaction(
        &self,
        result: &CompactionResult,
        compaction_snapshot: &read_view::ReadViewCompactionSnapshot,
    ) -> Result<()> {
        match result.outcome() {
            CompactionOutcome::Published => {
                let root =
                    result
                        .published_root()
                        .cloned()
                        .ok_or_else(|| ServerError::Catalog {
                            message: "published WAL compaction did not return a root snapshot"
                                .to_owned(),
                            source: None,
                        })?;
                let prune_through = root.base_wal_seq();
                self.read_view
                    .advance_after_compaction(root, compaction_snapshot)
                    .await?;
                self.prune_published_wal(prune_through).await;
            }
            CompactionOutcome::StalePlan | CompactionOutcome::NoRecords => {}
        }
        Ok(())
    }

    async fn prune_published_wal(&self, prune_through: WalSeq) {
        if let Err(error) = self.wal.prune_through(prune_through).await {
            tracing::warn!(
                target: target::WAL,
                event = event::WAL_COMPACTION_FAILED,
                service = observability::SERVICE_NAME,
                server_instance_id = observability::server_instance_id(),
                pid = observability::pid(),
                export_id = %self.export_id,
                export_name = %self.export_name,
                target_wal_seq = prune_through.get(),
                phase = "wal_prune",
                error = %error,
            );
        }
    }

    fn log_compaction_failed(
        &self,
        target_wal_seq: WalSeq,
        wal_debt_bytes: u64,
        phase: &'static str,
        error: &ServerError,
    ) {
        tracing::warn!(
            target: target::WAL,
            event = event::WAL_COMPACTION_FAILED,
            service = observability::SERVICE_NAME,
            server_instance_id = observability::server_instance_id(),
            pid = observability::pid(),
            export_id = %self.export_id,
            export_name = %self.export_name,
            target_wal_seq = target_wal_seq.get(),
            wal_debt_bytes,
            phase,
            error = %error,
        );
    }
}

async fn replay_wal_after(
    wal: &ExportWalHandle,
    read_view: &ExportReadView,
    root: &RootSnapshot,
) -> Result<WalReplaySummary> {
    let mut replay = wal.replay_after(root.base_wal_seq()).await?;
    let mut replayed_records = 0u64;
    let mut replayed_through_wal_seq = root.base_wal_seq();
    while let Some(record) = replay.next_record().await? {
        replayed_records += 1;
        replayed_through_wal_seq = record.seq();
        read_view.apply_wal_record(record).await?;
    }

    Ok(WalReplaySummary {
        replayed_records,
        replayed_through_wal_seq,
    })
}

fn log_root_loaded(
    export_id: &nbd_control_plane::ExportId,
    name: &ExportName,
    root: &RootSnapshot,
) {
    tracing::info!(
        target: target::WAL,
        event = event::WAL_ROOT_LOADED,
        service = observability::SERVICE_NAME,
        server_instance_id = observability::server_instance_id(),
        pid = observability::pid(),
        export_id = %export_id,
        export_name = %name,
        root_node_id = root_node_id_for_log(root),
        base_wal_seq = root.base_wal_seq().get(),
        size_bytes = root.size_bytes(),
    );
}

fn log_replay_completed(
    export_id: &nbd_control_plane::ExportId,
    name: &ExportName,
    root: &RootSnapshot,
    replay: WalReplaySummary,
) {
    tracing::info!(
        target: target::WAL,
        event = event::WAL_REPLAY_COMPLETED,
        service = observability::SERVICE_NAME,
        server_instance_id = observability::server_instance_id(),
        pid = observability::pid(),
        export_id = %export_id,
        export_name = %name,
        base_wal_seq = root.base_wal_seq().get(),
        replayed_records = replay.replayed_records,
        replayed_through_wal_seq = replay.replayed_through_wal_seq.get(),
    );
}

fn log_compaction_completed(name: &ExportName, result: &CompactionResult, phase: &'static str) {
    tracing::info!(
        target: target::WAL,
        event = event::WAL_COMPACTION_COMPLETED,
        service = observability::SERVICE_NAME,
        server_instance_id = observability::server_instance_id(),
        pid = observability::pid(),
        export_id = %result.export_id(),
        export_name = %name,
        base_wal_seq = result.base_wal_seq().get(),
        target_wal_seq = result.target_wal_seq().get(),
        compacted_records = result.compacted_records(),
        written_leaf_blobs = result.written_leaf_blobs(),
        outcome = ?result.outcome(),
        phase,
    );
}

fn root_node_id_for_log(root: &RootSnapshot) -> &str {
    root.root_node_id().map(NodeId::as_str).unwrap_or("<empty>")
}

impl fmt::Debug for WalDurableEngine {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WalDurableEngine")
            .field("name", &self.name)
            .field("size_bytes", &self.size_bytes)
            .field("block_size", &self.block_size)
            .finish_non_exhaustive()
    }
}

fn validate_head_can_open(descriptor: &ActiveExportDescriptor, head: &ExportHead) -> Result<()> {
    if head.layout_kind() != ExportLayoutKind::CowImmutableTree {
        return Err(ServerError::Catalog {
            message: format!(
                "WAL durable export `{}` requires cow_immutable_tree head, got {}",
                descriptor.name(),
                head.layout_kind()
            ),
            source: None,
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::LocalBlobStore;
    use crate::wal::{LocalWalProvider, OpenWal, WalDomain, WalProvider};
    use nbd_control_plane::{
        ActiveExportDescriptor, CatalogError, CloneExport, CloneExportResult, CreateExport,
        DeleteExport, ExportCatalog, ExportDescriptor, ExportEngineKind, ExportId, ExportName,
        ExportRecord, ExportState, InspectExport, ListExports, PublishTreeUpdate,
        PublishTreeUpdateOutcome, TREE_CHUNK_BYTES, Timestamp, TreeEdgeLookup, TreeEdgeRecord,
        TreeLeafRefRecord, TreeNodeRecord,
    };
    use nbd_test_support::TestRuntime;
    use std::collections::HashMap;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::Notify;
    use tokio::time::timeout;

    #[tokio::test]
    async fn background_tick_skips_below_soft_threshold() {
        let fixture = BackgroundTickFixture::new("background-below-soft", 1024).await;
        fixture.write(0, b"abcd").await;

        let tick = fixture
            .compaction()
            .compact_background_tick(compaction_policy(5, 10))
            .await;

        assert_eq!(tick, BackgroundCompactionTick::SkippedBelowSoftThreshold);
        assert_eq!(fixture.engine.wal_debt_bytes().await, 4);
        assert_eq!(fixture.catalog_base_wal_seq().await, WalSeq::zero());
    }

    #[tokio::test]
    async fn background_tick_compacts_at_soft_threshold_below_hard() {
        let fixture = BackgroundTickFixture::new("background-compact", 1024).await;
        fixture.write(0, b"abcde").await;

        let tick = fixture
            .compaction()
            .compact_background_tick(compaction_policy(5, 10))
            .await;

        assert_eq!(
            tick,
            BackgroundCompactionTick::Compacted(CompactionOutcome::Published),
        );
        assert_eq!(fixture.engine.wal_debt_bytes().await, 0);
        assert_eq!(fixture.catalog_base_wal_seq().await, WalSeq::new(1));
    }

    #[tokio::test]
    async fn background_tick_skips_at_hard_threshold() {
        let fixture = BackgroundTickFixture::new("background-at-hard", 1024).await;
        fixture.write(0, b"abcdefghij").await;

        let tick = fixture
            .compaction()
            .compact_background_tick(compaction_policy(5, 10))
            .await;

        assert_eq!(tick, BackgroundCompactionTick::SkippedAtHardThreshold);
        assert_eq!(fixture.engine.wal_debt_bytes().await, 10);
        assert_eq!(fixture.catalog_base_wal_seq().await, WalSeq::zero());
    }

    #[tokio::test]
    async fn background_tick_skips_when_compaction_lock_is_busy() {
        let fixture = BackgroundTickFixture::new("background-busy", 1024).await;
        fixture.write(0, b"abcde").await;
        let compaction = fixture.compaction();
        let _busy = compaction.compaction_lock.lock().await;

        let tick = compaction
            .compact_background_tick(compaction_policy(5, 10))
            .await;

        assert_eq!(tick, BackgroundCompactionTick::SkippedBusy);
        assert_eq!(fixture.engine.wal_debt_bytes().await, 5);
        assert_eq!(fixture.catalog_base_wal_seq().await, WalSeq::zero());
    }

    #[tokio::test]
    async fn background_task_compacts_on_interval() {
        let fixture = BackgroundTickFixture::new_with_policy(
            "background-task",
            compaction_policy(5, 10),
            Duration::from_millis(10),
        )
        .await;
        fixture.write(0, b"abcde").await;

        wait_for_catalog_base(&fixture, WalSeq::new(1)).await;

        assert_eq!(fixture.engine.wal_debt_bytes().await, 0);
        fixture.engine.close().await.expect("close engine");
    }

    #[tokio::test]
    async fn close_waits_for_in_flight_background_compaction() {
        let (fixture, blocking_store) = BackgroundTickFixture::new_with_blocking_store(
            "background-close-wait",
            compaction_policy(5, 10),
            Duration::from_millis(1),
        )
        .await;
        fixture.write(0, b"abcde").await;
        blocking_store.wait_for_publish_count(1).await;

        let engine = fixture.engine.clone();
        let mut close_task = tokio::spawn(async move { engine.close().await });
        assert!(
            timeout(Duration::from_millis(50), &mut close_task)
                .await
                .is_err(),
            "close completed before in-flight background compaction finished",
        );

        blocking_store.release_first_publish();
        close_task
            .await
            .expect("join close task")
            .expect("close engine");
        assert_eq!(fixture.catalog_base_wal_seq().await, WalSeq::new(1));
    }

    struct BackgroundTickFixture {
        _runtime: TestRuntime,
        catalog: TestCatalog,
        export_id: ExportId,
        engine: Arc<WalDurableEngine>,
    }

    impl BackgroundTickFixture {
        async fn new(name: &str, engine_hard_threshold_bytes: u64) -> Self {
            Self::new_with_policy(
                name,
                CompactionPolicy::with_hard_threshold(engine_hard_threshold_bytes),
                DEFAULT_BACKGROUND_COMPACTION_INTERVAL,
            )
            .await
        }

        async fn new_with_policy(name: &str, policy: CompactionPolicy, interval: Duration) -> Self {
            let runtime = TestRuntime::new().expect("test runtime");
            let catalog = migrated_catalog(&runtime).await;
            let tree_store = Arc::new(catalog.clone()) as Arc<dyn TreeRecordStore>;
            Self::new_with_store(runtime, catalog, name, policy, interval, tree_store).await
        }

        async fn new_with_blocking_store(
            name: &str,
            policy: CompactionPolicy,
            interval: Duration,
        ) -> (Self, Arc<BlockingTreeRecordStore>) {
            let runtime = TestRuntime::new().expect("test runtime");
            let catalog = migrated_catalog(&runtime).await;
            let blocking_store = Arc::new(BlockingTreeRecordStore::new(catalog.clone()));
            let fixture = Self::new_with_store(
                runtime,
                catalog,
                name,
                policy,
                interval,
                blocking_store.clone() as Arc<dyn TreeRecordStore>,
            )
            .await;
            (fixture, blocking_store)
        }

        async fn new_with_store(
            runtime: TestRuntime,
            catalog: TestCatalog,
            name: &str,
            policy: CompactionPolicy,
            interval: Duration,
            tree_store: Arc<dyn TreeRecordStore>,
        ) -> Self {
            let created = catalog
                .create_export(
                    CreateExport::new(
                        ExportName::new(name).expect("export name"),
                        TREE_CHUNK_BYTES,
                        4096,
                        ExportEngineKind::WalDurable,
                    )
                    .expect("create export"),
                )
                .await
                .expect("create wal export");
            let descriptor = catalog
                .load_export_descriptor(created.name().clone())
                .await
                .expect("load descriptor");
            let wal_provider = LocalWalProvider::new(runtime.wal_dir());
            let wal = wal_provider
                .open_export(OpenWal::new(WalDomain::for_export_id(created.id().clone())))
                .await
                .expect("open wal");
            let head = catalog
                .load_export_head(created.id())
                .await
                .expect("load export head");
            let engine = Arc::new(
                WalDurableEngine::open_with_cow_tree_and_compaction_policy(
                    &descriptor,
                    wal,
                    Arc::new(LocalBlobStore::new(runtime.root_path().join("blobs"))),
                    tree_store,
                    head,
                    policy,
                    interval,
                )
                .await
                .expect("open wal durable engine"),
            );

            Self {
                _runtime: runtime,
                catalog,
                export_id: created.id().clone(),
                engine,
            }
        }
    }

    #[derive(Debug, Clone, Default)]
    struct TestCatalog {
        state: Arc<Mutex<TestCatalogState>>,
    }

    #[derive(Debug, Default)]
    struct TestCatalogState {
        exports: HashMap<ExportId, ExportRecord>,
        names: HashMap<String, ExportId>,
        nodes: HashMap<NodeId, TreeNodeRecord>,
        edges: HashMap<(NodeId, u16), TreeEdgeRecord>,
        leaf_refs: HashMap<NodeId, TreeLeafRefRecord>,
    }

    #[async_trait::async_trait]
    impl ExportCatalog for TestCatalog {
        async fn create_export(
            &self,
            request: CreateExport,
        ) -> nbd_control_plane::Result<ExportRecord> {
            let mut state = self.state.lock().expect("test catalog lock");
            if state.names.contains_key(request.name().as_str()) {
                return Err(CatalogError::ExportAlreadyExists {
                    name: request.name().clone(),
                });
            }
            let export_id =
                ExportId::new(format!("{}-id", request.name().as_str())).expect("export id");
            let head = match request.engine_kind() {
                ExportEngineKind::Memory => ExportHead::memory_empty(request.size_bytes())?,
                ExportEngineKind::SimpleDurable => {
                    ExportHead::simple_mutable_tree(request.size_bytes())?
                }
                ExportEngineKind::WalDurable => {
                    ExportHead::cow_immutable_tree(request.size_bytes())?
                }
            };
            let now = Timestamp::new("now").expect("timestamp");
            let record = ExportRecord::new(
                export_id.clone(),
                request.name().clone(),
                request.block_size(),
                request.engine_kind(),
                ExportState::Active,
                head,
                now.clone(),
                now,
                None,
            )?;
            state
                .names
                .insert(record.name().as_str().to_owned(), export_id.clone());
            state.exports.insert(export_id, record.clone());
            Ok(record)
        }

        async fn clone_export(
            &self,
            _request: CloneExport,
        ) -> nbd_control_plane::Result<CloneExportResult> {
            unimplemented!("background compaction unit tests do not clone exports")
        }

        async fn delete_export(&self, _request: DeleteExport) -> nbd_control_plane::Result<()> {
            unimplemented!("background compaction unit tests do not delete exports")
        }

        async fn load_export(&self, name: ExportName) -> nbd_control_plane::Result<ExportRecord> {
            let state = self.state.lock().expect("test catalog lock");
            let export_id = state
                .names
                .get(name.as_str())
                .ok_or_else(|| CatalogError::ExportNotFound { name: name.clone() })?;
            let record = state
                .exports
                .get(export_id)
                .expect("name points at export")
                .clone();
            if record.state() == ExportState::Deleted {
                Err(CatalogError::ExportDeleted { name })
            } else {
                Ok(record)
            }
        }

        async fn load_export_descriptor(
            &self,
            name: ExportName,
        ) -> nbd_control_plane::Result<ActiveExportDescriptor> {
            let record = self.load_export(name).await?;
            ActiveExportDescriptor::new(ExportDescriptor::new(
                record.id().clone(),
                record.name().clone(),
                record.block_size(),
                record.engine_kind(),
                record.state(),
                record.created_at().clone(),
                record.updated_at().clone(),
                record.deleted_at().cloned(),
            )?)
        }

        async fn load_export_head(
            &self,
            export_id: &ExportId,
        ) -> nbd_control_plane::Result<ExportHead> {
            let state = self.state.lock().expect("test catalog lock");
            state
                .exports
                .get(export_id)
                .map(|record| record.head().clone())
                .ok_or_else(|| CatalogError::database(format!("export `{export_id}` not found")))
        }

        async fn inspect_export(
            &self,
            request: InspectExport,
        ) -> nbd_control_plane::Result<ExportRecord> {
            self.load_export(request.name().clone()).await
        }

        async fn list_exports(
            &self,
            _request: ListExports,
        ) -> nbd_control_plane::Result<Vec<ExportRecord>> {
            let state = self.state.lock().expect("test catalog lock");
            Ok(state.exports.values().cloned().collect())
        }
    }

    #[async_trait::async_trait]
    impl TreeRecordStore for TestCatalog {
        async fn load_node(
            &self,
            node_id: &NodeId,
        ) -> nbd_control_plane::Result<Option<TreeNodeRecord>> {
            Ok(self
                .state
                .lock()
                .expect("test catalog lock")
                .nodes
                .get(node_id)
                .cloned())
        }

        async fn load_nodes(
            &self,
            node_ids: &[NodeId],
        ) -> nbd_control_plane::Result<Vec<TreeNodeRecord>> {
            let state = self.state.lock().expect("test catalog lock");
            Ok(node_ids
                .iter()
                .filter_map(|node_id| state.nodes.get(node_id).cloned())
                .collect())
        }

        async fn load_child_edges(
            &self,
            lookups: &[TreeEdgeLookup],
        ) -> nbd_control_plane::Result<Vec<TreeEdgeRecord>> {
            let state = self.state.lock().expect("test catalog lock");
            let mut edges = Vec::new();
            for lookup in lookups {
                for slot in &lookup.slots {
                    if let Some(edge) = state
                        .edges
                        .get(&(lookup.parent_node_id.clone(), *slot))
                        .cloned()
                    {
                        edges.push(edge);
                    }
                }
            }
            Ok(edges)
        }

        async fn load_leaf_refs(
            &self,
            node_ids: &[NodeId],
        ) -> nbd_control_plane::Result<Vec<TreeLeafRefRecord>> {
            let state = self.state.lock().expect("test catalog lock");
            Ok(node_ids
                .iter()
                .filter_map(|node_id| state.leaf_refs.get(node_id).cloned())
                .collect())
        }

        async fn publish_tree_update(
            &self,
            request: PublishTreeUpdate,
        ) -> nbd_control_plane::Result<PublishTreeUpdateOutcome> {
            let mut state = self.state.lock().expect("test catalog lock");
            let current = state
                .exports
                .get(&request.export_id)
                .cloned()
                .ok_or_else(|| {
                    CatalogError::database(format!("export `{}` not found", request.export_id))
                })?;
            if current.state() == ExportState::Deleted {
                return Err(CatalogError::ExportDeleted {
                    name: current.name().clone(),
                });
            }
            if current.head() != &request.expected_head {
                return Ok(PublishTreeUpdateOutcome::StaleHead(current));
            }
            for node in request.records.nodes {
                state.nodes.insert(node.id.clone(), node);
            }
            for edge in request.records.edges {
                state
                    .edges
                    .insert((edge.parent_node_id.clone(), edge.slot), edge);
            }
            for leaf_ref in request.records.leaf_refs {
                state.leaf_refs.insert(leaf_ref.node_id.clone(), leaf_ref);
            }
            let updated = ExportRecord::new(
                current.id().clone(),
                current.name().clone(),
                current.block_size(),
                current.engine_kind(),
                current.state(),
                request.next_head,
                current.created_at().clone(),
                Timestamp::new("updated").expect("timestamp"),
                current.deleted_at().cloned(),
            )?;
            state.exports.insert(request.export_id, updated.clone());
            Ok(PublishTreeUpdateOutcome::Published(updated))
        }
    }

    struct BlockingTreeRecordStore {
        inner: TestCatalog,
        publish_count: AtomicUsize,
        publish_started: Notify,
        release_publish: Notify,
    }

    impl BlockingTreeRecordStore {
        fn new(inner: TestCatalog) -> Self {
            Self {
                inner,
                publish_count: AtomicUsize::new(0),
                publish_started: Notify::new(),
                release_publish: Notify::new(),
            }
        }

        async fn wait_for_publish_count(&self, expected: usize) {
            timeout(Duration::from_secs(5), async {
                loop {
                    if self.publish_count.load(Ordering::SeqCst) >= expected {
                        return;
                    }
                    self.publish_started.notified().await;
                }
            })
            .await
            .expect("wait for blocked compaction publish");
        }

        fn release_first_publish(&self) {
            self.release_publish.notify_waiters();
        }
    }

    #[async_trait::async_trait]
    impl TreeRecordStore for BlockingTreeRecordStore {
        async fn load_node(
            &self,
            node_id: &NodeId,
        ) -> nbd_control_plane::Result<Option<TreeNodeRecord>> {
            self.inner.load_node(node_id).await
        }

        async fn load_nodes(
            &self,
            node_ids: &[NodeId],
        ) -> nbd_control_plane::Result<Vec<TreeNodeRecord>> {
            self.inner.load_nodes(node_ids).await
        }

        async fn load_child_edges(
            &self,
            lookups: &[TreeEdgeLookup],
        ) -> nbd_control_plane::Result<Vec<TreeEdgeRecord>> {
            self.inner.load_child_edges(lookups).await
        }

        async fn load_leaf_refs(
            &self,
            node_ids: &[NodeId],
        ) -> nbd_control_plane::Result<Vec<TreeLeafRefRecord>> {
            self.inner.load_leaf_refs(node_ids).await
        }

        async fn publish_tree_update(
            &self,
            request: PublishTreeUpdate,
        ) -> nbd_control_plane::Result<PublishTreeUpdateOutcome> {
            let count = self.publish_count.fetch_add(1, Ordering::SeqCst) + 1;
            self.publish_started.notify_waiters();
            if count == 1 {
                self.release_publish.notified().await;
            }
            self.inner.publish_tree_update(request).await
        }
    }

    impl BackgroundTickFixture {
        async fn write(&self, offset: u64, data: &[u8]) {
            self.engine
                .write(offset, data.to_vec())
                .await
                .expect("write");
        }

        fn compaction(&self) -> &CompactionCoordinator {
            self.engine.compaction.as_ref().expect("compaction")
        }

        async fn catalog_base_wal_seq(&self) -> WalSeq {
            self.catalog
                .load_export_head(&self.export_id)
                .await
                .expect("load export head")
                .base_wal_seq()
        }
    }

    async fn wait_for_catalog_base(fixture: &BackgroundTickFixture, expected: WalSeq) {
        timeout(Duration::from_secs(5), async {
            loop {
                if fixture.catalog_base_wal_seq().await == expected {
                    return;
                }
                time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("wait for catalog base");
    }

    async fn migrated_catalog(_runtime: &TestRuntime) -> TestCatalog {
        TestCatalog::default()
    }

    fn compaction_policy(soft: u64, hard: u64) -> CompactionPolicy {
        CompactionPolicy {
            background_wal_debt_threshold_bytes: soft,
            hard_wal_debt_threshold_bytes: hard,
        }
    }
}
