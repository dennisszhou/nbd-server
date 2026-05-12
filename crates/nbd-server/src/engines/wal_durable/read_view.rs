use super::{
    overlay::{OverlayExtentMap, OverlayReadSlice},
    read_cache::{CacheInsertPlacement, ReadCache},
};
use crate::engines::tree::{
    Block, BlockPart, LazyTreeMetadataReader, LoadedTreeLeaf, TreeGeometry, TreeReader,
};
use crate::error::{Result, ServerError};
use crate::range::ByteRange;
use crate::storage::BlobStoreHandle;
use crate::wal::WalRecord;
use bytes::Bytes;
use nbd_control_plane::{
    ExportHead, ExportLayoutKind, ExportRecord, NodeId, TREE_CHUNK_BYTES, TreeFormat,
    TreeRecordStore, TreeStorageKind, WalSeq,
};
use std::fmt;
use std::sync::Arc;
use tokio::sync::RwLock;

const DEFAULT_READ_CACHE_BYTES: usize = 1024 * 1024 * 1024;

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
    CowHead {
        root_node_id: Option<NodeId>,
        base_wal_seq: WalSeq,
        size_bytes: u64,
        tree_format: TreeFormat,
    },
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
    wal_debt_bytes: u64,
    overlay: OverlayExtentMap,
    cache: ReadCache,
}

#[derive(Debug, Clone)]
pub(super) struct ReadViewCompactionSnapshot {
    pub(super) root: RootSnapshot,
    pub(super) target_wal_seq: WalSeq,
    pub(super) wal_debt_bytes: u64,
    pub(super) overlay: OverlayExtentMap,
}

#[derive(Debug)]
pub(super) struct ZeroTreeReader;

pub(super) struct CowTreeReader {
    pub(super) blob_store: BlobStoreHandle,
    pub(super) store: Arc<dyn TreeRecordStore>,
}

impl fmt::Debug for CowTreeReader {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CowTreeReader").finish_non_exhaustive()
    }
}

impl RootSnapshot {
    pub(super) fn from_meta(meta: &ExportRecord) -> Self {
        Self {
            backing: RootBacking::Zero {
                root_node_id: meta.head().root_node_id().cloned(),
                base_wal_seq: meta.head().base_wal_seq(),
                size_bytes: meta.size_bytes(),
            },
        }
    }

    pub(super) fn from_head(head: &ExportHead) -> Result<Self> {
        if head.layout_kind() != ExportLayoutKind::CowImmutableTree {
            return Err(ServerError::Catalog {
                message: format!(
                    "WAL root requires cow_immutable_tree head, got {}",
                    head.layout_kind()
                ),
                source: None,
            });
        }
        let tree_format = head.tree_format().ok_or_else(|| ServerError::Catalog {
            message: "WAL root head is missing tree format".to_owned(),
            source: None,
        })?;
        Ok(Self {
            backing: RootBacking::CowHead {
                root_node_id: head.root_node_id().cloned(),
                base_wal_seq: head.base_wal_seq(),
                size_bytes: head.size_bytes(),
                tree_format,
            },
        })
    }

    pub fn root_node_id(&self) -> Option<&NodeId> {
        match &self.backing {
            RootBacking::Zero { root_node_id, .. } => root_node_id.as_ref(),
            RootBacking::CowHead { root_node_id, .. } => root_node_id.as_ref(),
        }
    }

    pub fn base_wal_seq(&self) -> WalSeq {
        match &self.backing {
            RootBacking::Zero { base_wal_seq, .. } => *base_wal_seq,
            RootBacking::CowHead { base_wal_seq, .. } => *base_wal_seq,
        }
    }

    pub fn size_bytes(&self) -> u64 {
        match &self.backing {
            RootBacking::Zero { size_bytes, .. } => *size_bytes,
            RootBacking::CowHead { size_bytes, .. } => *size_bytes,
        }
    }

    fn is_zero_backed(&self) -> bool {
        matches!(&self.backing, RootBacking::Zero { .. })
    }

    pub(super) fn tree_format(&self) -> Option<TreeFormat> {
        match &self.backing {
            RootBacking::Zero { .. } => None,
            RootBacking::CowHead { tree_format, .. } => Some(*tree_format),
        }
    }

    pub(super) fn to_export_head(&self) -> Result<ExportHead> {
        ExportHead::new_with_tree_format(
            ExportLayoutKind::CowImmutableTree,
            self.root_node_id().cloned(),
            self.size_bytes(),
            self.base_wal_seq(),
            self.tree_format(),
        )
        .map_err(ServerError::catalog)
    }
}

impl ExportReadView {
    pub(super) fn zero_filled(root: RootSnapshot) -> Self {
        Self::new(root, Arc::new(ZeroTreeReader))
    }

    pub(super) fn new(root: RootSnapshot, tree_reader: Arc<dyn TreeReader<RootSnapshot>>) -> Self {
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
                wal_debt_bytes: 0,
                overlay: OverlayExtentMap::new(),
                cache: ReadCache::new(cache_bytes),
            }),
            tree_reader,
        }
    }

    pub(super) async fn export_head(&self) -> Result<ExportHead> {
        self.state.read().await.root.to_export_head()
    }

    pub(super) async fn wal_debt_bytes(&self) -> u64 {
        self.state.read().await.wal_debt_bytes
    }

    pub(super) async fn capture_compaction_snapshot(
        &self,
    ) -> Result<Option<ReadViewCompactionSnapshot>> {
        let state = self.state.read().await;
        if state.last_applied_seq <= state.root.base_wal_seq() || state.overlay.is_empty() {
            return Ok(None);
        }

        Ok(Some(ReadViewCompactionSnapshot {
            root: state.root.clone(),
            target_wal_seq: state.last_applied_seq,
            wal_debt_bytes: state.wal_debt_bytes,
            overlay: state.overlay.clone(),
        }))
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
        let debt_bytes = record.data().len() as u64;
        let record = Arc::new(record);
        state.overlay.insert_record(record)?;
        state.cache.trim_range(record_range)?;
        state.last_applied_seq = expected_seq;
        state.wal_debt_bytes = state
            .wal_debt_bytes
            .checked_add(debt_bytes)
            .ok_or_else(|| ServerError::wal("apply WAL record", "WAL debt bytes overflowed"))?;
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
        if new_checkpoint == state.last_applied_seq {
            state.wal_debt_bytes = 0;
        }
        Ok(())
    }

    pub(super) async fn advance_after_compaction(
        &self,
        new_root: RootSnapshot,
        snapshot: &ReadViewCompactionSnapshot,
    ) -> Result<()> {
        let mut state = self.state.write().await;
        if new_root.size_bytes() != state.root.size_bytes() {
            return Err(ServerError::wal(
                "advance read-view root after compaction",
                format!(
                    "new root size {} does not match current size {}",
                    new_root.size_bytes(),
                    state.root.size_bytes()
                ),
            ));
        }

        let current_checkpoint = state.root.base_wal_seq();
        let snapshot_base = snapshot.root.base_wal_seq();
        let snapshot_target = snapshot.target_wal_seq;
        if current_checkpoint >= snapshot_target {
            return Ok(());
        }
        if current_checkpoint != snapshot_base {
            return Err(ServerError::wal(
                "advance read-view root after compaction",
                format!(
                    "current checkpoint {} does not match snapshot base {}",
                    current_checkpoint, snapshot_base
                ),
            ));
        }

        let new_checkpoint = new_root.base_wal_seq();
        if new_checkpoint < snapshot_target {
            return Err(ServerError::wal(
                "advance read-view root after compaction",
                format!(
                    "new checkpoint {} is before snapshot target {}",
                    new_checkpoint, snapshot_target
                ),
            ));
        }
        if new_checkpoint > snapshot_target {
            return Err(ServerError::wal(
                "advance read-view root after compaction",
                format!(
                    "new checkpoint {} is after snapshot target {}",
                    new_checkpoint, snapshot_target
                ),
            ));
        }
        if new_checkpoint > state.last_applied_seq {
            return Err(ServerError::wal(
                "advance read-view root after compaction",
                format!(
                    "new checkpoint {} is beyond last applied WAL sequence {}",
                    new_checkpoint, state.last_applied_seq
                ),
            ));
        }
        if snapshot.wal_debt_bytes > state.wal_debt_bytes {
            return Err(ServerError::wal(
                "advance read-view root after compaction",
                format!(
                    "snapshot WAL debt {} exceeds live WAL debt {}",
                    snapshot.wal_debt_bytes, state.wal_debt_bytes
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
        state.wal_debt_bytes -= snapshot.wal_debt_bytes;
        if new_checkpoint == state.last_applied_seq {
            state.wal_debt_bytes = 0;
        }
        Ok(())
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
                source: None,
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
        if root.is_zero_backed() {
            return Err(ServerError::Catalog {
                message: "COW backing reader requires a COW root".to_owned(),
                source: None,
            });
        }

        let mut parts = Vec::new();
        let mut copied = 0usize;
        while copied < range.len() as usize {
            let current_offset = range.start() + copied as u64;
            let chunk_index = nbd_control_plane::ChunkIndex::new(current_offset / TREE_CHUNK_BYTES);
            let chunk_offset = current_offset % TREE_CHUNK_BYTES;
            let chunk_available = TREE_CHUNK_BYTES - chunk_offset;
            let copy_len = chunk_available.min(range.len() - copied as u64) as u32;
            let part_range = ByteRange::new(current_offset, copy_len);

            if let Some(leaf) = self.load_leaf(root, chunk_index).await? {
                if leaf.leaf_ref().storage_kind != TreeStorageKind::ImmutableBlob {
                    return Err(ServerError::Catalog {
                        message: format!(
                            "COW tree chunk {chunk_index} has storage kind {}",
                            leaf.leaf_ref().storage_kind
                        ),
                        source: None,
                    });
                }
                let chunk_data = self
                    .blob_store
                    .get_blob(
                        &leaf.leaf_ref().storage_key,
                        chunk_offset,
                        u64::from(copy_len),
                    )
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

impl CowTreeReader {
    async fn load_leaf(
        &self,
        root: &RootSnapshot,
        chunk_index: nbd_control_plane::ChunkIndex,
    ) -> Result<Option<LoadedTreeLeaf>> {
        let Some(tree_format) = root.tree_format() else {
            return Ok(None);
        };
        let geometry = TreeGeometry::new(tree_format, root.size_bytes())?;
        let reader = LazyTreeMetadataReader::new(
            self.store.clone(),
            geometry,
            ExportLayoutKind::CowImmutableTree,
            root.root_node_id().cloned(),
        );
        reader.load_leaf(chunk_index).await
    }
}

pub(super) fn validate_range(
    operation: &'static str,
    range: ByteRange,
    size_bytes: u64,
) -> Result<()> {
    validate_request_range(operation, range.start(), range.len(), size_bytes)
}

pub(super) fn validate_request_range(
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

pub(super) fn range_end(range: ByteRange) -> u64 {
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

pub(super) fn byte_range_from_bounds(start: u64, end: u64) -> Result<ByteRange> {
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
    use crate::engines::tree::TreeRecordFactory;
    use crate::storage::{LocalBlobStore, put_random_blob};
    use nbd_control_plane::{
        ExportHead, ExportLayoutKind, PublishTreeUpdate, PublishTreeUpdateOutcome, TreeEdgeLookup,
        TreeEdgeRecord, TreeFormat, TreeLeafRefRecord, TreeNodeRecord, TreeRecordStore,
    };
    use nbd_test_support::TestRuntime;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

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
    async fn compaction_snapshot_is_absent_without_retained_wal() {
        let read_view = ExportReadView::zero_filled(zero_root(4096));

        assert!(
            read_view
                .capture_compaction_snapshot()
                .await
                .expect("capture snapshot")
                .is_none()
        );
    }

    #[tokio::test]
    async fn compaction_snapshot_counts_physical_debt_for_hot_rewrites() {
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

        let snapshot = read_view
            .capture_compaction_snapshot()
            .await
            .expect("capture snapshot")
            .expect("snapshot present");

        assert_eq!(snapshot.root.base_wal_seq(), WalSeq::zero());
        assert_eq!(snapshot.target_wal_seq, WalSeq::new(3));
        assert_eq!(snapshot.wal_debt_bytes, 12);
        assert_eq!(
            snapshot.overlay.debug_extents(),
            vec![(0, 4, WalSeq::new(3), 0)],
        );
    }

    #[tokio::test]
    async fn compaction_snapshot_metadata_clone_reuses_record_arc() {
        let read_view = ExportReadView::zero_filled(zero_root(4096));
        read_view
            .apply_wal_record(wal_record(1, 0, b"aaaa"))
            .await
            .expect("apply record");

        let snapshot = read_view
            .capture_compaction_snapshot()
            .await
            .expect("capture snapshot")
            .expect("snapshot present");
        let snapshot_slice = snapshot
            .overlay
            .read_slices(ByteRange::new(0, 4))
            .expect("read snapshot overlay")
            .pop()
            .expect("snapshot overlay slice");
        let live_slice = read_view
            .state
            .read()
            .await
            .overlay
            .read_slices(ByteRange::new(0, 4))
            .expect("read live overlay")
            .pop()
            .expect("live overlay slice");

        assert!(Arc::ptr_eq(&snapshot_slice.record, &live_slice.record));
    }

    #[tokio::test]
    async fn compaction_snapshot_stays_stable_after_later_writes() {
        let read_view = ExportReadView::zero_filled(zero_root(4096));
        read_view
            .apply_wal_record(wal_record(1, 0, b"aaaa"))
            .await
            .expect("apply first record");
        let snapshot = read_view
            .capture_compaction_snapshot()
            .await
            .expect("capture snapshot")
            .expect("snapshot present");
        read_view
            .apply_wal_record(wal_record(2, 0, b"bbbb"))
            .await
            .expect("apply second record");

        let snapshot_slice = snapshot
            .overlay
            .read_slices(ByteRange::new(0, 4))
            .expect("read snapshot overlay")
            .pop()
            .expect("snapshot overlay slice");

        assert_eq!(snapshot.target_wal_seq, WalSeq::new(1));
        assert_eq!(snapshot.wal_debt_bytes, 4);
        assert_eq!(overlay_slice_bytes(&snapshot_slice), b"aaaa");
        assert_eq!(
            read_view
                .read(ByteRange::new(0, 4))
                .await
                .expect("read latest live view"),
            b"bbbb",
        );
        assert_eq!(
            snapshot.overlay.debug_extents(),
            vec![(0, 4, WalSeq::new(1), 0)],
        );
        assert_eq!(
            read_view.state.read().await.overlay.debug_extents(),
            vec![(0, 4, WalSeq::new(2), 0)],
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
    async fn read_view_compaction_advance_subtracts_only_snapshot_debt() {
        let read_view = ExportReadView::zero_filled(zero_root(4096));
        read_view
            .apply_wal_record(wal_record(1, 0, b"aaaa"))
            .await
            .expect("apply snapshot record");
        let snapshot = read_view
            .capture_compaction_snapshot()
            .await
            .expect("capture snapshot")
            .expect("snapshot present");
        read_view
            .apply_wal_record(wal_record(2, 8, b"bbbb"))
            .await
            .expect("apply later record");

        read_view
            .advance_after_compaction(zero_root_at(4096, WalSeq::new(1)), &snapshot)
            .await
            .expect("advance after compaction");

        let state = read_view.state.read().await;
        assert_eq!(state.root.base_wal_seq(), WalSeq::new(1));
        assert_eq!(state.wal_debt_bytes, 4);
        assert_eq!(
            state.overlay.debug_extents(),
            vec![(8, 12, WalSeq::new(2), 0)],
        );
    }

    #[tokio::test]
    async fn read_view_compaction_advance_rejects_root_beyond_snapshot_target() {
        let read_view = ExportReadView::zero_filled(zero_root(4096));
        read_view
            .apply_wal_record(wal_record(1, 0, b"aaaa"))
            .await
            .expect("apply snapshot record");
        let snapshot = read_view
            .capture_compaction_snapshot()
            .await
            .expect("capture snapshot")
            .expect("snapshot present");
        read_view
            .apply_wal_record(wal_record(2, 8, b"bbbb"))
            .await
            .expect("apply later record");

        let error = read_view
            .advance_after_compaction(zero_root_at(4096, WalSeq::new(2)), &snapshot)
            .await
            .expect_err("reject root beyond snapshot target");

        assert!(matches!(error, ServerError::Wal { .. }));
        let state = read_view.state.read().await;
        assert_eq!(state.root.base_wal_seq(), WalSeq::zero());
        assert_eq!(state.wal_debt_bytes, 8);
        assert_eq!(
            state.overlay.debug_extents(),
            vec![(0, 4, WalSeq::new(1), 0), (8, 12, WalSeq::new(2), 0),],
        );
    }

    #[tokio::test]
    async fn cow_tree_reader_reads_from_root_snapshot() {
        let runtime = TestRuntime::new().expect("test runtime");
        let blob_store: BlobStoreHandle =
            Arc::new(LocalBlobStore::new(runtime.root_path().join("blobs")));
        let mut chunk_data = vec![0; 4096];
        chunk_data[8..12].copy_from_slice(b"root");
        let key = put_random_blob(blob_store.as_ref(), &chunk_data)
            .await
            .expect("create blob");
        let root_node_id = NodeId::new("root-node").expect("root node id");
        let leaf_node_id = NodeId::new("leaf-node").expect("leaf node id");
        let root = cow_root(Some(root_node_id.clone()), 4096, WalSeq::new(7));
        let store = static_cow_store(4096, root_node_id, leaf_node_id, key);
        let reader = CowTreeReader {
            blob_store,
            store: Arc::new(store),
        };

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
        let blob_store: BlobStoreHandle =
            Arc::new(LocalBlobStore::new(runtime.root_path().join("blobs")));
        let root = cow_root(None, TREE_CHUNK_BYTES * 2, WalSeq::new(7));
        let reader = CowTreeReader {
            blob_store,
            store: unused_tree_store(),
        };
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
        let blob_store: BlobStoreHandle =
            Arc::new(LocalBlobStore::new(runtime.root_path().join("blobs")));
        let zero_root = RootSnapshot {
            backing: RootBacking::Zero {
                root_node_id: None,
                base_wal_seq: WalSeq::zero(),
                size_bytes: 4096,
            },
        };
        let cow_reader = CowTreeReader {
            blob_store: blob_store.clone(),
            store: unused_tree_store(),
        };

        assert!(matches!(
            cow_reader
                .read_committed(&zero_root, ByteRange::new(0, 4))
                .await,
            Err(ServerError::Catalog { .. }),
        ));

        let cow_root = cow_root(
            Some(NodeId::new("root-node").expect("root node id")),
            4096,
            WalSeq::new(1),
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

    fn cow_root(
        root_node_id: Option<NodeId>,
        size_bytes: u64,
        base_wal_seq: WalSeq,
    ) -> RootSnapshot {
        let head = ExportHead::new_with_tree_format(
            ExportLayoutKind::CowImmutableTree,
            root_node_id,
            size_bytes,
            base_wal_seq,
            Some(TreeFormat::Bounded32V1),
        )
        .expect("cow head");
        RootSnapshot::from_head(&head).expect("cow root")
    }

    fn static_cow_store(
        size_bytes: u64,
        root_node_id: NodeId,
        leaf_node_id: NodeId,
        key: nbd_control_plane::BlobKey,
    ) -> StaticTreeRecordStore {
        let geometry = TreeGeometry::new(TreeFormat::Bounded32V1, size_bytes).expect("geometry");
        let factory = TreeRecordFactory::new(geometry, ExportLayoutKind::CowImmutableTree, None);
        let root = factory.root_node(root_node_id.clone());
        let leaf_span = geometry
            .child_span(geometry.root_span(), 0)
            .expect("leaf span");
        let leaf = factory.leaf_node(leaf_node_id.clone(), leaf_span);
        let edge = factory.child_edge(root_node_id, 0, leaf_node_id.clone());
        let leaf_ref = factory.leaf_ref(leaf_node_id, TreeStorageKind::ImmutableBlob, key);
        StaticTreeRecordStore {
            root,
            leaf,
            edge,
            leaf_ref,
        }
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

    fn overlay_slice_bytes(slice: &OverlayReadSlice) -> &[u8] {
        let start = slice.record_offset as usize;
        let len = (slice.end - slice.start) as usize;
        &slice.record.data()[start..start + len]
    }

    fn unused_tree_store() -> Arc<dyn TreeRecordStore> {
        Arc::new(UnusedTreeRecordStore)
    }

    struct UnusedTreeRecordStore;

    struct StaticTreeRecordStore {
        root: TreeNodeRecord,
        leaf: TreeNodeRecord,
        edge: TreeEdgeRecord,
        leaf_ref: TreeLeafRefRecord,
    }

    #[async_trait::async_trait]
    impl TreeRecordStore for UnusedTreeRecordStore {
        async fn load_node(
            &self,
            _node_id: &NodeId,
        ) -> nbd_control_plane::Result<Option<TreeNodeRecord>> {
            panic!("unused tree record store should not load nodes")
        }

        async fn load_nodes(
            &self,
            _node_ids: &[NodeId],
        ) -> nbd_control_plane::Result<Vec<TreeNodeRecord>> {
            panic!("unused tree record store should not load nodes")
        }

        async fn load_child_edges(
            &self,
            _lookups: &[TreeEdgeLookup],
        ) -> nbd_control_plane::Result<Vec<TreeEdgeRecord>> {
            panic!("unused tree record store should not load child edges")
        }

        async fn load_leaf_refs(
            &self,
            _node_ids: &[NodeId],
        ) -> nbd_control_plane::Result<Vec<TreeLeafRefRecord>> {
            panic!("unused tree record store should not load leaf refs")
        }

        async fn publish_tree_update(
            &self,
            _request: PublishTreeUpdate,
        ) -> nbd_control_plane::Result<PublishTreeUpdateOutcome> {
            panic!("unused tree record store should not publish tree updates")
        }
    }

    #[async_trait::async_trait]
    impl TreeRecordStore for StaticTreeRecordStore {
        async fn load_node(
            &self,
            node_id: &NodeId,
        ) -> nbd_control_plane::Result<Option<TreeNodeRecord>> {
            if node_id == &self.root.id {
                return Ok(Some(self.root.clone()));
            }
            if node_id == &self.leaf.id {
                return Ok(Some(self.leaf.clone()));
            }
            Ok(None)
        }

        async fn load_nodes(
            &self,
            node_ids: &[NodeId],
        ) -> nbd_control_plane::Result<Vec<TreeNodeRecord>> {
            let mut nodes = Vec::new();
            for node_id in node_ids {
                if let Some(node) = self.load_node(node_id).await? {
                    nodes.push(node);
                }
            }
            Ok(nodes)
        }

        async fn load_child_edges(
            &self,
            lookups: &[TreeEdgeLookup],
        ) -> nbd_control_plane::Result<Vec<TreeEdgeRecord>> {
            let mut edges = Vec::new();
            for lookup in lookups {
                if lookup.parent_node_id == self.edge.parent_node_id
                    && lookup.slots.contains(&self.edge.slot)
                {
                    edges.push(self.edge.clone());
                }
            }
            Ok(edges)
        }

        async fn load_leaf_refs(
            &self,
            node_ids: &[NodeId],
        ) -> nbd_control_plane::Result<Vec<TreeLeafRefRecord>> {
            if node_ids.contains(&self.leaf_ref.node_id) {
                Ok(vec![self.leaf_ref.clone()])
            } else {
                Ok(Vec::new())
            }
        }

        async fn publish_tree_update(
            &self,
            _request: PublishTreeUpdate,
        ) -> nbd_control_plane::Result<PublishTreeUpdateOutcome> {
            panic!("static tree record store should not publish tree updates")
        }
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
            let start = range.start() as usize;
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
