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
    ActiveExportDescriptor, CowTreeMetadataStore, CowTreeSnapshot, ExportId, ExportLayoutKind,
    ExportName, ExportRecord, NodeId, WalSeq,
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
    catalog: Arc<dyn CowTreeMetadataStore>,
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
        catalog: Arc<dyn CowTreeMetadataStore>,
    ) -> Result<Self> {
        Self::open_with_cow_tree_and_wal_debt_threshold(
            descriptor,
            wal,
            blob_store,
            catalog,
            DEFAULT_WAL_DEBT_COMPACTION_THRESHOLD_BYTES,
        )
        .await
    }

    #[doc(hidden)]
    pub async fn open_with_cow_tree_and_wal_debt_threshold(
        descriptor: &ActiveExportDescriptor,
        wal: ExportWalHandle,
        blob_store: BlobStoreHandle,
        catalog: Arc<dyn CowTreeMetadataStore>,
        wal_debt_threshold_bytes: u64,
    ) -> Result<Self> {
        Self::open_with_cow_tree_and_compaction_policy(
            descriptor,
            wal,
            blob_store,
            catalog,
            CompactionPolicy::with_hard_threshold(wal_debt_threshold_bytes),
            DEFAULT_BACKGROUND_COMPACTION_INTERVAL,
        )
        .await
    }

    async fn open_with_cow_tree_and_compaction_policy(
        descriptor: &ActiveExportDescriptor,
        wal: ExportWalHandle,
        blob_store: BlobStoreHandle,
        catalog: Arc<dyn CowTreeMetadataStore>,
        compaction_policy: CompactionPolicy,
        background_interval: Duration,
    ) -> Result<Self> {
        if descriptor.engine_kind() != nbd_control_plane::ExportEngineKind::WalDurable {
            return Err(ServerError::Catalog {
                message: format!("export `{}` is not a wal_durable export", descriptor.name()),
                source: None,
            });
        }

        let snapshot = catalog
            .load_cow_tree(descriptor.id())
            .await
            .map_err(ServerError::catalog)?;
        validate_snapshot_can_open(descriptor, &snapshot)?;
        let size_bytes = snapshot.size_bytes();
        let root = RootSnapshot::from_cow_snapshot(snapshot);
        log_root_loaded(descriptor.id(), descriptor.name(), &root);
        let read_view = Arc::new(ExportReadView::new(
            root.clone(),
            Arc::new(CowTreeReader {
                blob_store: blob_store.clone(),
            }),
        ));
        let replay = replay_wal_after(&wal, read_view.as_ref(), &root).await?;
        log_replay_completed(descriptor.id(), descriptor.name(), &root, replay);
        let compaction = Arc::new(CompactionCoordinator::new(
            descriptor.id().clone(),
            descriptor.name().clone(),
            wal.clone(),
            catalog,
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
        catalog: Arc<dyn CowTreeMetadataStore>,
        blob_store: BlobStoreHandle,
        read_view: Arc<ExportReadView>,
    ) -> Self {
        let compactor = CowCompactor::new(catalog.clone(), blob_store);
        Self {
            export_id,
            export_name,
            catalog,
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
            CompactionOutcome::Published | CompactionOutcome::AlreadyCovered => {
                let snapshot = self
                    .catalog
                    .load_cow_tree(&self.export_id)
                    .await
                    .map_err(ServerError::catalog)?;
                let root = RootSnapshot::from_cow_snapshot(snapshot);
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

fn validate_snapshot_can_open(
    descriptor: &ActiveExportDescriptor,
    snapshot: &CowTreeSnapshot,
) -> Result<()> {
    if snapshot.export_id() != descriptor.id() {
        return Err(ServerError::Catalog {
            message: format!(
                "COW snapshot export id `{}` does not match export `{}`",
                snapshot.export_id(),
                descriptor.id()
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
        CatalogUrl, CreateExport, ExportCatalog, ExportEngineKind, ExportName, PublishCompaction,
        PublishCompactionOutcome, SQLiteExportCatalog, TREE_CHUNK_BYTES,
    };
    use nbd_test_support::TestRuntime;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::Notify;
    use tokio::time::timeout;

    const MIGRATIONS: &[&str] = &[include_str!(
        "../../../../../prisma/migrations/20260506000000_baseline/migration.sql"
    )];

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
        catalog: SQLiteExportCatalog,
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
            let cow_tree_store = Arc::new(catalog.clone()) as Arc<dyn CowTreeMetadataStore>;
            Self::new_with_store(runtime, catalog, name, policy, interval, cow_tree_store).await
        }

        async fn new_with_blocking_store(
            name: &str,
            policy: CompactionPolicy,
            interval: Duration,
        ) -> (Self, Arc<BlockingCowTreeStore>) {
            let runtime = TestRuntime::new().expect("test runtime");
            let catalog = migrated_catalog(&runtime).await;
            let blocking_store = Arc::new(BlockingCowTreeStore::new(catalog.clone()));
            let fixture = Self::new_with_store(
                runtime,
                catalog,
                name,
                policy,
                interval,
                blocking_store.clone() as Arc<dyn CowTreeMetadataStore>,
            )
            .await;
            (fixture, blocking_store)
        }

        async fn new_with_store(
            runtime: TestRuntime,
            catalog: SQLiteExportCatalog,
            name: &str,
            policy: CompactionPolicy,
            interval: Duration,
            cow_tree_store: Arc<dyn CowTreeMetadataStore>,
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
            let engine = Arc::new(
                WalDurableEngine::open_with_cow_tree_and_compaction_policy(
                    &descriptor,
                    wal,
                    Arc::new(LocalBlobStore::new(runtime.root_path().join("blobs"))),
                    cow_tree_store,
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

    struct BlockingCowTreeStore {
        inner: SQLiteExportCatalog,
        publish_count: AtomicUsize,
        publish_started: Notify,
        release_publish: Notify,
    }

    impl BlockingCowTreeStore {
        fn new(inner: SQLiteExportCatalog) -> Self {
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
    impl CowTreeMetadataStore for BlockingCowTreeStore {
        async fn load_cow_tree(
            &self,
            export_id: &ExportId,
        ) -> nbd_control_plane::Result<CowTreeSnapshot> {
            self.inner.load_cow_tree(export_id).await
        }

        async fn publish_compaction(
            &self,
            request: PublishCompaction,
        ) -> nbd_control_plane::Result<PublishCompactionOutcome> {
            let count = self.publish_count.fetch_add(1, Ordering::SeqCst) + 1;
            self.publish_started.notify_waiters();
            if count == 1 {
                self.release_publish.notified().await;
            }
            self.inner.publish_compaction(request).await
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
                .load_cow_tree(&self.export_id)
                .await
                .expect("load cow tree")
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

    async fn migrated_catalog(runtime: &TestRuntime) -> SQLiteExportCatalog {
        let url = CatalogUrl::parse(runtime.catalog_url()).expect("catalog URL");
        let catalog = SQLiteExportCatalog::connect(&url)
            .await
            .expect("connect catalog");

        for migration in MIGRATIONS {
            sqlx::raw_sql(migration)
                .execute(catalog.pool())
                .await
                .expect("apply migration");
        }

        catalog
    }

    fn compaction_policy(soft: u64, hard: u64) -> CompactionPolicy {
        CompactionPolicy {
            background_wal_debt_threshold_bytes: soft,
            hard_wal_debt_threshold_bytes: hard,
        }
    }
}
