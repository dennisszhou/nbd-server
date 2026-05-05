use crate::{
    AdmissionOp, AdmittedExportRequest, ByteRange, ExportAdmissionPolicy,
    ExportAdmissionPolicyHandle, ExportEngine, ExportReply, ExportRequest, ExportResult,
    ExportWalHandle, Result, ServerError, WalRecord, WalRequest,
};
use nbd_control_plane::{ExportLayoutKind, ExportMeta, ExportName, NodeId, WalSeq};
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
    root_node_id: Option<NodeId>,
    checkpoint_wal_seq: WalSeq,
    size_bytes: u64,
}

/// Materialized read view for one open WAL durable export.
pub struct ExportReadView {
    state: RwLock<ExportReadViewState>,
    backing: Arc<dyn BackingReader>,
}

#[derive(Debug, Clone)]
struct ExportReadViewState {
    root: RootSnapshot,
    wal_overlay: BTreeMap<WalSeq, WalRecord>,
}

#[async_trait::async_trait]
trait BackingReader: Send + Sync {
    async fn read_committed(&self, root: &RootSnapshot, range: ByteRange) -> Result<Vec<u8>>;
}

#[derive(Debug)]
struct ZeroBackingReader;

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
        let read_view = ExportReadView::zero_filled(root.clone());
        let mut replay = wal.replay_after(root.checkpoint_wal_seq()).await?;
        while let Some(record) = replay.next_record().await? {
            read_view.apply_wal_record(record).await?;
        }

        Ok(Self {
            name: meta.name().clone(),
            size_bytes: meta.size_bytes(),
            block_size: meta.block_size(),
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

impl RootSnapshot {
    fn from_meta(meta: &ExportMeta) -> Self {
        Self {
            root_node_id: meta.head().root_node_id().cloned(),
            checkpoint_wal_seq: meta.head().checkpoint_wal_seq(),
            size_bytes: meta.size_bytes(),
        }
    }

    pub fn root_node_id(&self) -> Option<&NodeId> {
        self.root_node_id.as_ref()
    }

    pub fn checkpoint_wal_seq(&self) -> WalSeq {
        self.checkpoint_wal_seq
    }

    pub fn size_bytes(&self) -> u64 {
        self.size_bytes
    }
}

impl ExportReadView {
    fn zero_filled(root: RootSnapshot) -> Self {
        Self::new(root, Arc::new(ZeroBackingReader))
    }

    fn new(root: RootSnapshot, backing: Arc<dyn BackingReader>) -> Self {
        Self {
            state: RwLock::new(ExportReadViewState {
                root,
                wal_overlay: BTreeMap::new(),
            }),
            backing,
        }
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

        let mut data = self.backing.read_committed(&root, range).await?;
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
impl BackingReader for ZeroBackingReader {
    async fn read_committed(&self, root: &RootSnapshot, range: ByteRange) -> Result<Vec<u8>> {
        validate_range("read", range, root.size_bytes())?;
        Ok(vec![0; range.len() as usize])
    }
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
