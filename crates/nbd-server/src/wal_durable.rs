use crate::{
    observability::{self, event, target},
    tree_reader::TreeReader,
    AdmissionOp, AdmittedExportRequest, ByteRange, ExportAdmissionPolicy,
    ExportAdmissionPolicyHandle, ExportEngine, ExportReply, ExportRequest, ExportResult,
    ExportWalHandle, LocalBlobStore, Result, ServerError, WalRecord, WalRequest,
};
use nbd_control_plane::{
    CowTreeMetadataStore, CowTreeSnapshot, ExportDescriptor, ExportHead, ExportLayoutKind,
    ExportMeta, ExportName, NodeId, WalSeq, TREE_CHUNK_BYTES,
};
use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};

/// WAL-backed durable engine using a retained WAL overlay over committed state.
pub struct WalDurableEngine {
    name: ExportName,
    size_bytes: u64,
    block_size: u64,
    wal: ExportWalHandle,
    read_view: ExportReadView,
    write_lock: Mutex<()>,
}

#[derive(Debug)]
pub struct WalDurableAdmissionPolicy {
    size_bytes: u64,
}

/// Catalog head snapshot used as the committed read baseline.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RootSnapshot {
    backing: RootBacking,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum RootBacking {
    Zero {
        root_node_id: Option<NodeId>,
        checkpoint_wal_seq: WalSeq,
        size_bytes: u64,
    },
    CowTree(Arc<CowTreeSnapshot>),
}

/// Materialized read view for one open WAL durable export.
pub struct ExportReadView {
    state: RwLock<ExportReadViewState>,
    tree_reader: Arc<dyn TreeReader<RootSnapshot>>,
}

#[derive(Debug, Clone)]
struct ExportReadViewState {
    root: RootSnapshot,
    wal_overlay: BTreeMap<WalSeq, WalRecord>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct WalReplaySummary {
    replayed_records: u64,
    replayed_through_wal_seq: WalSeq,
}

#[derive(Debug)]
struct ZeroTreeReader;

#[derive(Debug)]
struct CowTreeReader {
    blob_store: LocalBlobStore,
}

impl WalDurableEngine {
    pub async fn open(meta: &ExportMeta, wal: ExportWalHandle) -> Result<Self> {
        if meta.head().layout_kind() != ExportLayoutKind::CowImmutableTree {
            return Err(ServerError::Catalog {
                message: format!(
                    "export `{}` does not have a cow immutable tree head",
                    meta.name()
                ),
            });
        }
        if meta.head().root_node_id().is_some() {
            return Err(ServerError::Catalog {
                message: format!(
                    "export `{}` has a committed COW root that is not readable yet",
                    meta.name()
                ),
            });
        }

        let root = RootSnapshot::from_meta(meta);
        log_root_loaded(meta.id(), meta.name(), &root);
        let read_view = ExportReadView::zero_filled(root.clone());
        let replay = replay_wal_after(&wal, &read_view, &root).await?;
        log_replay_completed(meta.id(), meta.name(), &root, replay);

        Ok(Self {
            name: meta.name().clone(),
            size_bytes: meta.size_bytes(),
            block_size: meta.block_size(),
            wal,
            read_view,
            write_lock: Mutex::new(()),
        })
    }

    pub async fn open_with_cow_tree(
        descriptor: &ExportDescriptor,
        wal: ExportWalHandle,
        blob_store: LocalBlobStore,
        catalog: Arc<dyn CowTreeMetadataStore>,
    ) -> Result<Self> {
        if descriptor.engine_kind() != nbd_control_plane::ExportEngineKind::WalDurable {
            return Err(ServerError::Catalog {
                message: format!("export `{}` is not a wal_durable export", descriptor.name()),
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
        let read_view = ExportReadView::new(root.clone(), Arc::new(CowTreeReader { blob_store }));
        let replay = replay_wal_after(&wal, &read_view, &root).await?;
        log_replay_completed(descriptor.id(), descriptor.name(), &root, replay);

        Ok(Self {
            name: descriptor.name().clone(),
            size_bytes,
            block_size: descriptor.block_size(),
            wal,
            read_view,
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

    pub async fn export_head(&self) -> Result<ExportHead> {
        self.read_view.export_head().await
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
        self.read_view.apply_wal_record(record).await
    }

    fn flush(&self) -> Result<()> {
        Ok(())
    }
}

impl WalDurableAdmissionPolicy {
    pub fn new(size_bytes: u64) -> Self {
        Self { size_bytes }
    }
}

impl ExportAdmissionPolicy for WalDurableAdmissionPolicy {
    fn operation_for(&self, request: &ExportRequest) -> Result<AdmissionOp> {
        match request {
            ExportRequest::Read { offset, len } => {
                Ok(AdmissionOp::Read(ByteRange::new(*offset, *len)))
            }
            ExportRequest::Write { offset, data } => {
                let len = u32::try_from(data.len()).map_err(|_| ServerError::OutOfBounds {
                    operation: "write",
                    offset: *offset,
                    length: data.len() as u64,
                    size_bytes: self.size_bytes,
                })?;
                Ok(AdmissionOp::Write(ByteRange::new(*offset, len)))
            }
            ExportRequest::Flush => Ok(AdmissionOp::Flush),
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
}

async fn replay_wal_after(
    wal: &ExportWalHandle,
    read_view: &ExportReadView,
    root: &RootSnapshot,
) -> Result<WalReplaySummary> {
    let mut replay = wal.replay_after(root.checkpoint_wal_seq()).await?;
    let mut replayed_records = 0u64;
    let mut replayed_through_wal_seq = root.checkpoint_wal_seq();
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
        base_wal_seq = root.checkpoint_wal_seq().get(),
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
        base_wal_seq = root.checkpoint_wal_seq().get(),
        replayed_records = replay.replayed_records,
        replayed_through_wal_seq = replay.replayed_through_wal_seq.get(),
    );
}

fn root_node_id_for_log(root: &RootSnapshot) -> &str {
    root.root_node_id().map(NodeId::as_str).unwrap_or("<empty>")
}

impl RootSnapshot {
    fn from_meta(meta: &ExportMeta) -> Self {
        Self {
            backing: RootBacking::Zero {
                root_node_id: meta.head().root_node_id().cloned(),
                checkpoint_wal_seq: meta.head().checkpoint_wal_seq(),
                size_bytes: meta.size_bytes(),
            },
        }
    }

    fn from_cow_snapshot(snapshot: CowTreeSnapshot) -> Self {
        Self {
            backing: RootBacking::CowTree(Arc::new(snapshot)),
        }
    }

    pub fn root_node_id(&self) -> Option<&NodeId> {
        match &self.backing {
            RootBacking::Zero { root_node_id, .. } => root_node_id.as_ref(),
            RootBacking::CowTree(snapshot) => snapshot.root_node_id(),
        }
    }

    pub fn checkpoint_wal_seq(&self) -> WalSeq {
        match &self.backing {
            RootBacking::Zero {
                checkpoint_wal_seq, ..
            } => *checkpoint_wal_seq,
            RootBacking::CowTree(snapshot) => snapshot.checkpoint_wal_seq(),
        }
    }

    pub fn size_bytes(&self) -> u64 {
        match &self.backing {
            RootBacking::Zero { size_bytes, .. } => *size_bytes,
            RootBacking::CowTree(snapshot) => snapshot.size_bytes(),
        }
    }

    fn is_zero_backed(&self) -> bool {
        matches!(&self.backing, RootBacking::Zero { .. })
    }

    fn cow_tree(&self) -> Option<&CowTreeSnapshot> {
        match &self.backing {
            RootBacking::Zero { .. } => None,
            RootBacking::CowTree(snapshot) => Some(snapshot.as_ref()),
        }
    }

    fn to_export_head(&self) -> Result<ExportHead> {
        ExportHead::new(
            ExportLayoutKind::CowImmutableTree,
            self.root_node_id().cloned(),
            self.size_bytes(),
            self.checkpoint_wal_seq(),
        )
        .map_err(ServerError::catalog)
    }
}

impl ExportReadView {
    fn zero_filled(root: RootSnapshot) -> Self {
        Self::new(root, Arc::new(ZeroTreeReader))
    }

    fn new(root: RootSnapshot, tree_reader: Arc<dyn TreeReader<RootSnapshot>>) -> Self {
        Self {
            state: RwLock::new(ExportReadViewState {
                root,
                wal_overlay: BTreeMap::new(),
            }),
            tree_reader,
        }
    }

    async fn export_head(&self) -> Result<ExportHead> {
        self.state.read().await.root.to_export_head()
    }

    pub async fn read(&self, range: ByteRange) -> Result<Vec<u8>> {
        let (root, records) = {
            let state = self.state.read().await;
            validate_range("read", range, state.root.size_bytes())?;
            let records = state
                .wal_overlay
                .values()
                .filter(|record| ranges_overlap(range, record.range()))
                .cloned()
                .collect::<Vec<_>>();
            (state.root.clone(), records)
        };

        let mut data = self.tree_reader.read_committed(&root, range).await?;
        if data.len() as u64 != range.len() {
            return Err(ServerError::wal(
                "read committed backing",
                format!(
                    "backing returned {} bytes for {} byte range",
                    data.len(),
                    range.len()
                ),
            ));
        }

        for record in records {
            overlay_record(&mut data, range, &record)?;
        }
        Ok(data)
    }

    pub async fn apply_wal_record(&self, record: WalRecord) -> Result<()> {
        let mut state = self.state.write().await;
        validate_range("write", record.range(), state.root.size_bytes())?;
        if record.seq() <= state.root.checkpoint_wal_seq() {
            return Err(ServerError::wal(
                "apply WAL record",
                format!(
                    "record sequence {} is at or before checkpoint {}",
                    record.seq(),
                    state.root.checkpoint_wal_seq()
                ),
            ));
        }
        if state.wal_overlay.contains_key(&record.seq()) {
            return Err(ServerError::wal(
                "apply WAL record",
                format!("duplicate WAL sequence {}", record.seq()),
            ));
        }
        state.wal_overlay.insert(record.seq(), record);
        Ok(())
    }
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

impl fmt::Debug for ExportReadView {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ExportReadView").finish_non_exhaustive()
    }
}

#[async_trait::async_trait]
impl TreeReader<RootSnapshot> for ZeroTreeReader {
    async fn read_committed(&self, root: &RootSnapshot, range: ByteRange) -> Result<Vec<u8>> {
        validate_range("read", range, root.size_bytes())?;
        if !root.is_zero_backed() {
            return Err(ServerError::Catalog {
                message: "zero backing reader requires a zero-backed root".to_owned(),
            });
        }
        Ok(vec![0; range.len() as usize])
    }
}

#[async_trait::async_trait]
impl TreeReader<RootSnapshot> for CowTreeReader {
    async fn read_committed(&self, root: &RootSnapshot, range: ByteRange) -> Result<Vec<u8>> {
        validate_range("read", range, root.size_bytes())?;
        let snapshot = root.cow_tree().ok_or_else(|| ServerError::Catalog {
            message: "COW backing reader requires a COW root snapshot".to_owned(),
        })?;

        let mut data = vec![0; range.len() as usize];
        let mut copied = 0usize;
        while copied < data.len() {
            let current_offset = range.start() + copied as u64;
            let chunk_index = nbd_control_plane::ChunkIndex::new(current_offset / TREE_CHUNK_BYTES);
            let chunk_offset = current_offset % TREE_CHUNK_BYTES;
            let chunk_available = TREE_CHUNK_BYTES - chunk_offset;
            let copy_len = chunk_available.min((data.len() - copied) as u64) as usize;

            if let Some(chunk) = snapshot.chunk(chunk_index) {
                let chunk_data = self
                    .blob_store
                    .read_blob(chunk.blob_key(), chunk_offset, copy_len as u64)
                    .await?;
                data[copied..copied + copy_len].copy_from_slice(&chunk_data);
            }

            copied += copy_len;
        }

        Ok(data)
    }
}

fn validate_snapshot_can_open(
    descriptor: &ExportDescriptor,
    snapshot: &CowTreeSnapshot,
) -> Result<()> {
    if snapshot.export_id() != descriptor.id() {
        return Err(ServerError::Catalog {
            message: format!(
                "COW snapshot export id `{}` does not match export `{}`",
                snapshot.export_id(),
                descriptor.id()
            ),
        });
    }
    Ok(())
}

fn validate_range(operation: &'static str, range: ByteRange, size_bytes: u64) -> Result<()> {
    validate_request_range(operation, range.start(), range.len(), size_bytes)
}

fn validate_request_range(
    operation: &'static str,
    offset: u64,
    length: u64,
    size_bytes: u64,
) -> Result<()> {
    let end = offset.checked_add(length).ok_or(ServerError::OutOfBounds {
        operation,
        offset,
        length,
        size_bytes,
    })?;
    if end > size_bytes {
        return Err(ServerError::OutOfBounds {
            operation,
            offset,
            length,
            size_bytes,
        });
    }

    Ok(())
}

fn ranges_overlap(left: ByteRange, right: ByteRange) -> bool {
    left.start() < range_end(right) && right.start() < range_end(left)
}

fn range_end(range: ByteRange) -> u64 {
    range.start().saturating_add(range.len())
}

fn overlay_record(data: &mut [u8], read_range: ByteRange, record: &WalRecord) -> Result<()> {
    let record_range = record.range();
    let start = read_range.start().max(record_range.start());
    let end = range_end(read_range).min(range_end(record_range));
    if start >= end {
        return Ok(());
    }

    let dst_start = usize::try_from(start - read_range.start()).map_err(|_| {
        ServerError::wal("overlay WAL record", "read range offset does not fit usize")
    })?;
    let src_start = usize::try_from(start - record_range.start()).map_err(|_| {
        ServerError::wal(
            "overlay WAL record",
            "record range offset does not fit usize",
        )
    })?;
    let len = usize::try_from(end - start)
        .map_err(|_| ServerError::wal("overlay WAL record", "overlap does not fit usize"))?;

    data[dst_start..dst_start + len].copy_from_slice(&record.data()[src_start..src_start + len]);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use nbd_control_plane::{ChunkIndex, CowChunkRef, ExportId};
    use nbd_test_support::TestRuntime;
    use std::collections::BTreeMap;

    #[tokio::test]
    async fn cow_tree_reader_reads_from_root_snapshot() {
        let runtime = TestRuntime::new().expect("test runtime");
        let blob_store = LocalBlobStore::new(runtime.root_path().join("blobs"));
        let mut chunk_data = vec![0; 4096];
        chunk_data[8..12].copy_from_slice(b"root");
        let key = blob_store
            .create_blob(&chunk_data)
            .await
            .expect("create blob");
        let chunk = CowChunkRef::new(ChunkIndex::new(0), key, TREE_CHUNK_BYTES).expect("cow chunk");
        let snapshot = CowTreeSnapshot::new(
            ExportId::new("export-root-backed").expect("export id"),
            4096,
            Some(NodeId::new("root-node").expect("node id")),
            WalSeq::new(7),
            BTreeMap::from([(ChunkIndex::new(0), chunk)]),
        )
        .expect("cow snapshot");
        let root = RootSnapshot::from_cow_snapshot(snapshot);
        let reader = CowTreeReader { blob_store };

        let data = reader
            .read_committed(&root, ByteRange::new(8, 4))
            .await
            .expect("read committed root");

        assert_eq!(data, b"root");
    }

    #[tokio::test]
    async fn tree_readers_reject_wrong_root_kind() {
        let runtime = TestRuntime::new().expect("test runtime");
        let blob_store = LocalBlobStore::new(runtime.root_path().join("blobs"));
        let zero_root = RootSnapshot {
            backing: RootBacking::Zero {
                root_node_id: None,
                checkpoint_wal_seq: WalSeq::zero(),
                size_bytes: 4096,
            },
        };
        let cow_reader = CowTreeReader {
            blob_store: blob_store.clone(),
        };

        assert!(matches!(
            cow_reader
                .read_committed(&zero_root, ByteRange::new(0, 4))
                .await,
            Err(ServerError::Catalog { .. }),
        ));

        let empty_chunk = vec![0; 4096];
        let key = blob_store
            .create_blob(&empty_chunk)
            .await
            .expect("create blob");
        let chunk = CowChunkRef::new(ChunkIndex::new(0), key, TREE_CHUNK_BYTES).expect("cow chunk");
        let cow_root = RootSnapshot::from_cow_snapshot(
            CowTreeSnapshot::new(
                ExportId::new("export-cow-backed").expect("export id"),
                4096,
                Some(NodeId::new("root-node").expect("node id")),
                WalSeq::new(1),
                BTreeMap::from([(ChunkIndex::new(0), chunk)]),
            )
            .expect("cow snapshot"),
        );

        assert!(matches!(
            ZeroTreeReader
                .read_committed(&cow_root, ByteRange::new(0, 4))
                .await,
            Err(ServerError::Catalog { .. }),
        ));
    }
}
