use crate::{
    extent_map::ExtentMap,
    observability::{self, event, target},
    read_cache::{CacheInsertPlacement, ReadCache},
    tree_reader::{Block, BlockPart, TreeReader},
    AdmissionOp, AdmittedExportRequest, ByteRange, ExportAdmissionPolicy,
    ExportAdmissionPolicyHandle, ExportEngine, ExportReply, ExportRequest, ExportResult,
    ExportWalHandle, LocalBlobStore, Result, ServerError, WalRecord, WalRequest,
};
use bytes::Bytes;
use nbd_control_plane::{
    CowTreeMetadataStore, CowTreeSnapshot, ExportDescriptor, ExportHead, ExportLayoutKind,
    ExportName, ExportRecord, NodeId, WalSeq, TREE_CHUNK_BYTES,
};
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
        base_wal_seq: WalSeq,
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
    last_applied_seq: WalSeq,
    overlay: OverlayExtentMap,
    cache: ReadCache,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct OverlayExtentMap {
    extents: ExtentMap<OverlayExtent>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct OverlayExtent {
    seq: WalSeq,
    record: Arc<WalRecord>,
    record_offset: u64,
}

#[derive(Debug, Clone)]
struct OverlayReadSlice {
    start: u64,
    end: u64,
    record: Arc<WalRecord>,
    record_offset: u64,
}

#[derive(Debug, Clone)]
struct RetiredOverlayExtent {
    start: u64,
    end: u64,
    seq: WalSeq,
    record: Arc<WalRecord>,
    record_offset: u64,
}

const DEFAULT_READ_CACHE_BYTES: usize = 1024 * 1024 * 1024;

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
    pub async fn open(meta: &ExportRecord, wal: ExportWalHandle) -> Result<Self> {
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

fn root_node_id_for_log(root: &RootSnapshot) -> &str {
    root.root_node_id().map(NodeId::as_str).unwrap_or("<empty>")
}

impl RootSnapshot {
    fn from_meta(meta: &ExportRecord) -> Self {
        Self {
            backing: RootBacking::Zero {
                root_node_id: meta.head().root_node_id().cloned(),
                base_wal_seq: meta.head().base_wal_seq(),
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

    pub fn base_wal_seq(&self) -> WalSeq {
        match &self.backing {
            RootBacking::Zero { base_wal_seq, .. } => *base_wal_seq,
            RootBacking::CowTree(snapshot) => snapshot.base_wal_seq(),
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
            self.base_wal_seq(),
        )
        .map_err(ServerError::catalog)
    }
}

impl OverlayExtentMap {
    fn new() -> Self {
        Self {
            extents: ExtentMap::new(),
        }
    }

    fn insert_record(&mut self, record: Arc<WalRecord>) -> Result<()> {
        let range = record.range();
        let start = range.start();
        let end = range_end(range);
        self.extents.insert_overwrite_with_split(
            start,
            end,
            OverlayExtent {
                seq: record.seq(),
                record,
                record_offset: 0,
            },
            |extent, delta| extent.split_at(delta),
        )?;
        Ok(())
    }

    fn read_slices(&self, range: ByteRange) -> Result<Vec<OverlayReadSlice>> {
        let read_start = range.start();
        let read_end = range_end(range);
        self.extents
            .overlapping(read_start, read_end)?
            .into_iter()
            .map(|extent| {
                let start = read_start.max(extent.start());
                let end = read_end.min(extent.end());
                let record_offset = extent
                    .value()
                    .record_offset
                    .checked_add(start - extent.start())
                    .ok_or_else(|| {
                        ServerError::wal("read overlay extent", "record offset overflowed")
                    })?;
                Ok(OverlayReadSlice {
                    start,
                    end,
                    record: extent.value().record.clone(),
                    record_offset,
                })
            })
            .collect()
    }

    fn visible_through(&self, seq: WalSeq) -> Vec<RetiredOverlayExtent> {
        let mut retired = self
            .extents
            .iter()
            .filter_map(|extent| {
                (extent.value().seq <= seq).then(|| RetiredOverlayExtent {
                    start: extent.start(),
                    end: extent.end(),
                    seq: extent.value().seq,
                    record: extent.value().record.clone(),
                    record_offset: extent.value().record_offset,
                })
            })
            .collect::<Vec<_>>();
        retired.sort_by_key(|extent| (extent.seq, extent.start));
        retired
    }

    fn remove_retired(&mut self, retired: &[RetiredOverlayExtent]) -> Result<()> {
        for extent in retired {
            self.extents
                .remove_range_with_split(extent.start, extent.end, |overlay, delta| {
                    overlay.split_at(delta)
                })?;
        }
        Ok(())
    }

    #[cfg(test)]
    fn debug_extents(&self) -> Vec<(u64, u64, WalSeq, u64)> {
        self.extents
            .iter()
            .map(|extent| {
                (
                    extent.start(),
                    extent.end(),
                    extent.value().seq,
                    extent.value().record_offset,
                )
            })
            .collect()
    }
}

impl OverlayExtent {
    fn split_at(&self, delta: u64) -> Self {
        Self {
            seq: self.seq,
            record: self.record.clone(),
            record_offset: self.record_offset + delta,
        }
    }
}

impl ExportReadView {
    fn zero_filled(root: RootSnapshot) -> Self {
        Self::new(root, Arc::new(ZeroTreeReader))
    }

    fn new(root: RootSnapshot, tree_reader: Arc<dyn TreeReader<RootSnapshot>>) -> Self {
        Self::new_with_cache_budget(root, tree_reader, DEFAULT_READ_CACHE_BYTES)
    }

    fn new_with_cache_budget(
        root: RootSnapshot,
        tree_reader: Arc<dyn TreeReader<RootSnapshot>>,
        cache_bytes: usize,
    ) -> Self {
        let last_applied_seq = root.base_wal_seq();
        Self {
            state: RwLock::new(ExportReadViewState {
                root,
                last_applied_seq,
                overlay: OverlayExtentMap::new(),
                cache: ReadCache::new(cache_bytes),
            }),
            tree_reader,
        }
    }

    async fn export_head(&self) -> Result<ExportHead> {
        self.state.read().await.root.to_export_head()
    }

    pub async fn read(&self, range: ByteRange) -> Result<Vec<u8>> {
        let (root, overlay_slices, cache_slices, tree_misses) = {
            let state = self.state.read().await;
            validate_range("read", range, state.root.size_bytes())?;
            let overlay_slices = state.overlay.read_slices(range)?;
            let overlay_spans = overlay_slices
                .iter()
                .map(|slice| (slice.start, slice.end))
                .collect::<Vec<_>>();
            let mut cache_slices = Vec::new();
            for cache_range in gaps_in_range(range, overlay_spans.clone())? {
                cache_slices.extend(state.cache.read_slices(cache_range)?);
            }
            let mut covered_spans = overlay_spans;
            covered_spans.extend(
                cache_slices
                    .iter()
                    .map(|slice| (slice.start(), slice.end())),
            );
            let tree_misses = gaps_in_range(range, covered_spans)?;
            (
                state.root.clone(),
                overlay_slices,
                cache_slices,
                tree_misses,
            )
        };

        let mut data = vec![0; range.len() as usize];
        for cache_slice in &cache_slices {
            cache_slice.copy_to(range, &mut data)?;
        }

        let mut tree_fills = Vec::new();
        for miss in tree_misses {
            let block = self.tree_reader.read_committed(&root, miss).await?;
            if block.range() != miss {
                return Err(ServerError::wal(
                    "read committed backing",
                    format!(
                        "tree reader returned range {:?} for requested range {:?}",
                        block.range(),
                        miss
                    ),
                ));
            }
            copy_tree_block(&mut data, range, &block)?;
            tree_fills.push(block);
        }

        for overlay in &overlay_slices {
            overlay_record(&mut data, range, overlay)?;
        }

        if !cache_slices.is_empty() || !tree_fills.is_empty() {
            let cache_hits = cache_slices
                .iter()
                .map(|slice| slice.object_id())
                .collect::<Vec<_>>();
            let mut state = self.state.write().await;
            state.cache.promote_hits(cache_hits);
            if state.root == root {
                for block in tree_fills {
                    insert_tree_block_fills(&mut state, &block)?;
                }
            }
        }
        Ok(data)
    }

    pub async fn apply_wal_record(&self, record: WalRecord) -> Result<()> {
        let mut state = self.state.write().await;
        validate_range("write", record.range(), state.root.size_bytes())?;
        if record.seq() <= state.root.base_wal_seq() {
            return Err(ServerError::wal(
                "apply WAL record",
                format!(
                    "record sequence {} is at or before checkpoint {}",
                    record.seq(),
                    state.root.base_wal_seq()
                ),
            ));
        }
        let expected_seq = state
            .last_applied_seq
            .get()
            .checked_add(1)
            .map(WalSeq::new)
            .ok_or_else(|| ServerError::wal("apply WAL record", "WAL sequence overflowed"))?;
        if record.seq() != expected_seq {
            return Err(ServerError::wal(
                "apply WAL record",
                format!("expected WAL sequence {expected_seq}, got {}", record.seq()),
            ));
        }
        let record_range = record.range();
        let record = Arc::new(record);
        state.overlay.insert_record(record)?;
        state.cache.trim_range(record_range)?;
        state.last_applied_seq = expected_seq;
        Ok(())
    }

    pub async fn advance_root(&self, new_root: RootSnapshot) -> Result<()> {
        let mut state = self.state.write().await;
        if new_root.size_bytes() != state.root.size_bytes() {
            return Err(ServerError::wal(
                "advance read-view root",
                format!(
                    "new root size {} does not match current size {}",
                    new_root.size_bytes(),
                    state.root.size_bytes()
                ),
            ));
        }

        let current_checkpoint = state.root.base_wal_seq();
        let new_checkpoint = new_root.base_wal_seq();
        if new_checkpoint < current_checkpoint {
            return Err(ServerError::wal(
                "advance read-view root",
                format!(
                    "new checkpoint {} is before current checkpoint {}",
                    new_checkpoint, current_checkpoint
                ),
            ));
        }
        if new_checkpoint == current_checkpoint {
            return Ok(());
        }
        if new_checkpoint > state.last_applied_seq {
            return Err(ServerError::wal(
                "advance read-view root",
                format!(
                    "new checkpoint {} is beyond last applied WAL sequence {}",
                    new_checkpoint, state.last_applied_seq
                ),
            ));
        }

        let retired = state.overlay.visible_through(new_checkpoint);
        for extent in &retired {
            state.cache.insert_wal_record_slice(
                byte_range_from_bounds(extent.start, extent.end)?,
                extent.record.clone(),
                extent.record_offset,
                CacheInsertPlacement::Cold,
            )?;
        }
        state.overlay.remove_retired(&retired)?;
        state.root = new_root;
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
    async fn read_committed(&self, root: &RootSnapshot, range: ByteRange) -> Result<Block> {
        validate_range("read", range, root.size_bytes())?;
        if !root.is_zero_backed() {
            return Err(ServerError::Catalog {
                message: "zero backing reader requires a zero-backed root".to_owned(),
            });
        }
        let parts = if range.is_empty() {
            Vec::new()
        } else {
            vec![BlockPart::Zero { range }]
        };
        Block::new(range, parts)
    }
}

#[async_trait::async_trait]
impl TreeReader<RootSnapshot> for CowTreeReader {
    async fn read_committed(&self, root: &RootSnapshot, range: ByteRange) -> Result<Block> {
        validate_range("read", range, root.size_bytes())?;
        let snapshot = root.cow_tree().ok_or_else(|| ServerError::Catalog {
            message: "COW backing reader requires a COW root snapshot".to_owned(),
        })?;

        let mut parts = Vec::new();
        let mut copied = 0usize;
        while copied < range.len() as usize {
            let current_offset = range.start() + copied as u64;
            let chunk_index = nbd_control_plane::ChunkIndex::new(current_offset / TREE_CHUNK_BYTES);
            let chunk_offset = current_offset % TREE_CHUNK_BYTES;
            let chunk_available = TREE_CHUNK_BYTES - chunk_offset;
            let copy_len = chunk_available.min(range.len() - copied as u64) as u32;
            let part_range = ByteRange::new(current_offset, copy_len);

            if let Some(chunk) = snapshot.chunk(chunk_index) {
                let chunk_data = self
                    .blob_store
                    .read_blob(chunk.blob_key(), chunk_offset, u64::from(copy_len))
                    .await?;
                parts.push(BlockPart::Data {
                    range: part_range,
                    bytes: Bytes::from(chunk_data),
                });
            } else {
                parts.push(BlockPart::Zero { range: part_range });
            }

            copied += copy_len as usize;
        }

        Block::new(range, parts)
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

fn range_end(range: ByteRange) -> u64 {
    range.start().saturating_add(range.len())
}

fn copy_tree_block(data: &mut [u8], read_range: ByteRange, block: &Block) -> Result<()> {
    let block_range = block.range();
    if range_end(block_range) > range_end(read_range) || block_range.start() < read_range.start() {
        return Err(ServerError::wal(
            "read committed backing",
            format!(
                "tree reader returned range {:?} outside requested range {:?}",
                block_range, read_range
            ),
        ));
    }
    let materialized = block.materialize()?;
    if materialized.len() as u64 != block_range.len() {
        return Err(ServerError::wal(
            "read committed backing",
            format!(
                "backing returned {} bytes for {} byte range",
                materialized.len(),
                block_range.len()
            ),
        ));
    }
    let dst_start = usize::try_from(block_range.start() - read_range.start()).map_err(|_| {
        ServerError::wal(
            "read committed backing",
            "read range offset does not fit usize",
        )
    })?;
    data[dst_start..dst_start + materialized.len()].copy_from_slice(&materialized);
    Ok(())
}

fn insert_tree_block_fills(state: &mut ExportReadViewState, block: &Block) -> Result<()> {
    for part in block.parts() {
        let BlockPart::Data { range, bytes } = part else {
            continue;
        };
        let mut covered_spans = state
            .overlay
            .read_slices(*range)?
            .into_iter()
            .map(|slice| (slice.start, slice.end))
            .collect::<Vec<_>>();
        covered_spans.extend(
            state
                .cache
                .read_slices(*range)?
                .into_iter()
                .map(|slice| (slice.start(), slice.end())),
        );

        for hole in gaps_in_range(*range, covered_spans)? {
            let bytes_start = usize::try_from(hole.start() - range.start()).map_err(|_| {
                ServerError::wal("insert tree cache fill", "slice start does not fit usize")
            })?;
            let bytes_end = bytes_start
                .checked_add(usize::try_from(hole.len()).map_err(|_| {
                    ServerError::wal("insert tree cache fill", "slice length does not fit usize")
                })?)
                .ok_or_else(|| ServerError::wal("insert tree cache fill", "slice overflowed"))?;
            state.cache.insert_bytes(
                hole,
                bytes.slice(bytes_start..bytes_end),
                CacheInsertPlacement::Hot,
            )?;
        }
    }
    Ok(())
}

fn overlay_record(
    data: &mut [u8],
    read_range: ByteRange,
    overlay: &OverlayReadSlice,
) -> Result<()> {
    let start = read_range.start().max(overlay.start);
    let end = range_end(read_range).min(overlay.end);
    if start >= end {
        return Ok(());
    }

    let dst_start = usize::try_from(start - read_range.start()).map_err(|_| {
        ServerError::wal("overlay WAL record", "read range offset does not fit usize")
    })?;
    let src_start =
        usize::try_from(overlay.record_offset + (start - overlay.start)).map_err(|_| {
            ServerError::wal(
                "overlay WAL record",
                "record range offset does not fit usize",
            )
        })?;
    let len = usize::try_from(end - start)
        .map_err(|_| ServerError::wal("overlay WAL record", "overlap does not fit usize"))?;

    data[dst_start..dst_start + len]
        .copy_from_slice(&overlay.record.data()[src_start..src_start + len]);
    Ok(())
}

fn gaps_in_range(range: ByteRange, mut spans: Vec<(u64, u64)>) -> Result<Vec<ByteRange>> {
    let start = range.start();
    let end = range_end(range);
    spans.sort_by_key(|(span_start, _)| *span_start);

    let mut gaps = Vec::new();
    let mut cursor = start;
    for (span_start, span_end) in spans {
        let span_start = span_start.max(start);
        let span_end = span_end.min(end);
        if span_end <= cursor {
            continue;
        }
        if span_start > cursor {
            gaps.push(byte_range_from_bounds(cursor, span_start)?);
        }
        cursor = cursor.max(span_end);
        if cursor == end {
            break;
        }
    }
    if cursor < end {
        gaps.push(byte_range_from_bounds(cursor, end)?);
    }
    Ok(gaps)
}

fn byte_range_from_bounds(start: u64, end: u64) -> Result<ByteRange> {
    let len = end
        .checked_sub(start)
        .ok_or_else(|| ServerError::wal("build byte range", "range end before start"))?;
    let len = u32::try_from(len)
        .map_err(|_| ServerError::wal("build byte range", "range length does not fit u32"))?;
    Ok(ByteRange::new(start, len))
}

#[cfg(test)]
mod tests {
    use super::*;
    use nbd_control_plane::{ChunkIndex, CowChunkRef, ExportId};
    use nbd_test_support::TestRuntime;
    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[tokio::test]
    async fn read_view_overlay_keeps_only_latest_repeated_write() {
        let read_view = ExportReadView::zero_filled(zero_root(4096));
        read_view
            .apply_wal_record(wal_record(1, 0, b"aaaa"))
            .await
            .expect("apply first record");
        read_view
            .apply_wal_record(wal_record(2, 0, b"bbbb"))
            .await
            .expect("apply second record");
        read_view
            .apply_wal_record(wal_record(3, 0, b"cccc"))
            .await
            .expect("apply third record");

        assert_eq!(
            read_view
                .read(ByteRange::new(0, 4))
                .await
                .expect("read latest"),
            b"cccc",
        );
        assert_eq!(
            read_view.state.read().await.overlay.debug_extents(),
            vec![(0, 4, WalSeq::new(3), 0)],
        );
    }

    #[tokio::test]
    async fn read_view_overlay_preserves_middle_split_offsets() {
        let read_view = ExportReadView::zero_filled(zero_root(4096));
        read_view
            .apply_wal_record(wal_record(1, 0, b"abcdefgh"))
            .await
            .expect("apply base record");
        read_view
            .apply_wal_record(wal_record(2, 4, b"ZZ"))
            .await
            .expect("apply middle record");

        assert_eq!(
            read_view
                .read(ByteRange::new(0, 8))
                .await
                .expect("read split"),
            b"abcdZZgh",
        );
        assert_eq!(
            read_view.state.read().await.overlay.debug_extents(),
            vec![
                (0, 4, WalSeq::new(1), 0),
                (4, 6, WalSeq::new(2), 0),
                (6, 8, WalSeq::new(1), 6),
            ],
        );
    }

    #[tokio::test]
    async fn read_view_overlay_requires_contiguous_wal_sequences() {
        let read_view = ExportReadView::zero_filled(zero_root(4096));

        let error = read_view
            .apply_wal_record(wal_record(2, 0, b"skip"))
            .await
            .expect_err("reject skipped WAL sequence");

        assert!(matches!(error, ServerError::Wal { .. }));
    }

    #[tokio::test]
    async fn read_view_cache_serves_warm_tree_reads() {
        let reader = Arc::new(CountingTreeReader::new(Bytes::from_static(
            b"0123456789abcdef",
        )));
        let read_view =
            ExportReadView::new_with_cache_budget(zero_root(4096), reader.clone(), 1024);

        assert_eq!(
            read_view
                .read(ByteRange::new(0, 4))
                .await
                .expect("cold read"),
            b"0123",
        );
        assert_eq!(reader.reads(), 1);
        assert_eq!(
            read_view
                .read(ByteRange::new(0, 4))
                .await
                .expect("warm read"),
            b"0123",
        );
        assert_eq!(reader.reads(), 1);
    }

    #[tokio::test]
    async fn read_view_write_trims_stale_cache_range() {
        let reader = Arc::new(CountingTreeReader::new(Bytes::from_static(
            b"0123456789abcdef",
        )));
        let read_view =
            ExportReadView::new_with_cache_budget(zero_root(4096), reader.clone(), 1024);
        read_view
            .read(ByteRange::new(0, 4))
            .await
            .expect("seed cache");

        read_view
            .apply_wal_record(wal_record(1, 1, b"ZZ"))
            .await
            .expect("apply write");

        assert_eq!(
            read_view
                .read(ByteRange::new(0, 4))
                .await
                .expect("read after write"),
            b"0ZZ3",
        );
        assert_eq!(reader.reads(), 1);
    }

    #[tokio::test]
    async fn read_view_cache_eviction_is_byte_budgeted() {
        let reader = Arc::new(CountingTreeReader::new(Bytes::from_static(
            b"0123456789abcdef",
        )));
        let read_view = ExportReadView::new_with_cache_budget(zero_root(4096), reader.clone(), 4);

        read_view
            .read(ByteRange::new(0, 4))
            .await
            .expect("read first cache object");
        read_view
            .read(ByteRange::new(8, 4))
            .await
            .expect("read second cache object");
        assert_eq!(reader.reads(), 2);

        read_view
            .read(ByteRange::new(0, 4))
            .await
            .expect("read evicted object again");
        assert_eq!(reader.reads(), 3);
    }

    #[tokio::test]
    async fn read_view_advance_root_retires_only_visible_overlay_extents() {
        let reader = Arc::new(CountingTreeReader::new(Bytes::from_static(
            b"bbbb456789abcdef",
        )));
        let read_view =
            ExportReadView::new_with_cache_budget(zero_root(4096), reader.clone(), 1024);
        read_view
            .apply_wal_record(wal_record(1, 0, b"aaaa"))
            .await
            .expect("apply shadowed record");
        read_view
            .apply_wal_record(wal_record(2, 0, b"bbbb"))
            .await
            .expect("apply visible record");

        read_view
            .advance_root(zero_root_at(4096, WalSeq::new(2)))
            .await
            .expect("advance root");

        let state = read_view.state.read().await;
        assert!(state.overlay.debug_extents().is_empty());
        assert_eq!(state.cache.debug_wal_seqs(), vec![2]);
        drop(state);

        assert_eq!(
            read_view
                .read(ByteRange::new(0, 4))
                .await
                .expect("read retired cache"),
            b"bbbb",
        );
        assert_eq!(reader.reads(), 0);
    }

    #[tokio::test]
    async fn read_view_advance_root_rejects_unapplied_checkpoint() {
        let read_view = ExportReadView::zero_filled(zero_root(4096));
        read_view
            .apply_wal_record(wal_record(1, 0, b"aaaa"))
            .await
            .expect("apply record");

        let error = read_view
            .advance_root(zero_root_at(4096, WalSeq::new(2)))
            .await
            .expect_err("reject unapplied checkpoint");

        assert!(matches!(error, ServerError::Wal { .. }));
        assert_eq!(
            read_view.state.read().await.overlay.debug_extents(),
            vec![(0, 4, WalSeq::new(1), 0)],
        );
    }

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

        let block = reader
            .read_committed(&root, ByteRange::new(8, 4))
            .await
            .expect("read committed root");

        assert_eq!(
            block.parts(),
            &[BlockPart::Data {
                range: ByteRange::new(8, 4),
                bytes: Bytes::from_static(b"root"),
            }],
        );
        assert_eq!(block.materialize().expect("materialize"), b"root");
    }

    #[tokio::test]
    async fn cow_tree_reader_splits_large_sparse_reads_on_chunk_boundaries() {
        let runtime = TestRuntime::new().expect("test runtime");
        let blob_store = LocalBlobStore::new(runtime.root_path().join("blobs"));
        let snapshot = CowTreeSnapshot::new(
            ExportId::new("export-root-sparse").expect("export id"),
            TREE_CHUNK_BYTES * 2,
            None,
            WalSeq::new(7),
            BTreeMap::new(),
        )
        .expect("cow snapshot");
        let root = RootSnapshot::from_cow_snapshot(snapshot);
        let reader = CowTreeReader { blob_store };
        let range = ByteRange::new(0, (TREE_CHUNK_BYTES + 16 * 1024 * 1024) as u32);

        let block = reader
            .read_committed(&root, range)
            .await
            .expect("read committed root");

        assert_eq!(
            block.parts(),
            &[
                BlockPart::Zero {
                    range: ByteRange::new(0, TREE_CHUNK_BYTES as u32),
                },
                BlockPart::Zero {
                    range: ByteRange::new(TREE_CHUNK_BYTES, 16 * 1024 * 1024),
                },
            ],
        );
    }

    #[tokio::test]
    async fn tree_readers_reject_wrong_root_kind() {
        let runtime = TestRuntime::new().expect("test runtime");
        let blob_store = LocalBlobStore::new(runtime.root_path().join("blobs"));
        let zero_root = RootSnapshot {
            backing: RootBacking::Zero {
                root_node_id: None,
                base_wal_seq: WalSeq::zero(),
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

    fn zero_root(size_bytes: u64) -> RootSnapshot {
        zero_root_at(size_bytes, WalSeq::zero())
    }

    fn zero_root_at(size_bytes: u64, base_wal_seq: WalSeq) -> RootSnapshot {
        RootSnapshot {
            backing: RootBacking::Zero {
                root_node_id: None,
                base_wal_seq,
                size_bytes,
            },
        }
    }

    fn wal_record(seq: u64, offset: u64, data: &[u8]) -> WalRecord {
        WalRecord::new(
            WalSeq::new(seq),
            ByteRange::new(offset, data.len() as u32),
            data.to_vec(),
        )
        .expect("WAL record")
    }

    #[derive(Debug)]
    struct CountingTreeReader {
        data: Bytes,
        reads: AtomicUsize,
    }

    impl CountingTreeReader {
        fn new(data: Bytes) -> Self {
            Self {
                data,
                reads: AtomicUsize::new(0),
            }
        }

        fn reads(&self) -> usize {
            self.reads.load(Ordering::SeqCst)
        }
    }

    #[async_trait::async_trait]
    impl TreeReader<RootSnapshot> for CountingTreeReader {
        async fn read_committed(&self, _root: &RootSnapshot, range: ByteRange) -> Result<Block> {
            self.reads.fetch_add(1, Ordering::SeqCst);
            let start = usize::try_from(range.start()).expect("range start fits usize");
            let end = start + range.len() as usize;
            Block::new(
                range,
                vec![BlockPart::Data {
                    range,
                    bytes: self.data.slice(start..end),
                }],
            )
        }
    }
}
