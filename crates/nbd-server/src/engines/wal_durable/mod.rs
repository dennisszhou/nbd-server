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
use tokio::sync::Mutex;

/// WAL-backed durable engine using a retained WAL overlay over committed state.
pub struct WalDurableEngine {
    name: ExportName,
    size_bytes: u64,
    block_size: u64,
    wal: ExportWalHandle,
    read_view: Arc<ExportReadView>,
    compaction: Option<CompactionCoordinator>,
    wal_debt_threshold_bytes: u64,
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
            wal_debt_threshold_bytes: DEFAULT_WAL_DEBT_COMPACTION_THRESHOLD_BYTES,
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
        let compaction = CompactionCoordinator::new(
            descriptor.id().clone(),
            descriptor.name().clone(),
            wal.clone(),
            catalog,
            blob_store,
            read_view.clone(),
        );

        Ok(Self {
            name: descriptor.name().clone(),
            size_bytes,
            block_size: descriptor.block_size(),
            wal,
            read_view,
            compaction: Some(compaction),
            wal_debt_threshold_bytes,
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
        if wal_debt_bytes < self.wal_debt_threshold_bytes {
            return;
        }
        let Some(compaction) = &self.compaction else {
            return;
        };

        compaction
            .compact_write_pressure_best_effort(self.wal_debt_threshold_bytes)
            .await;
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
