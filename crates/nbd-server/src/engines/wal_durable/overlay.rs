use super::read_view::range_end;
use crate::error::{Result, ServerError};
use crate::range::ByteRange;
use crate::wal::WalRecord;
use nbd_control_plane::WalSeq;
use std::sync::Arc;

use super::extent_map::ExtentMap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct OverlayExtentMap {
    extents: ExtentMap<OverlayExtent>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct OverlayExtent {
    seq: WalSeq,
    record: Arc<WalRecord>,
    record_offset: u64,
}

#[derive(Debug, Clone)]
pub(super) struct OverlayReadSlice {
    pub(super) start: u64,
    pub(super) end: u64,
    pub(super) record: Arc<WalRecord>,
    pub(super) record_offset: u64,
}

#[derive(Debug, Clone)]
pub(super) struct RetiredOverlayExtent {
    pub(super) start: u64,
    pub(super) end: u64,
    pub(super) seq: WalSeq,
    pub(super) record: Arc<WalRecord>,
    pub(super) record_offset: u64,
}

impl OverlayExtentMap {
    pub(super) fn new() -> Self {
        Self {
            extents: ExtentMap::new(),
        }
    }

    pub(super) fn insert_record(&mut self, record: Arc<WalRecord>) -> Result<()> {
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

    pub(super) fn read_slices(&self, range: ByteRange) -> Result<Vec<OverlayReadSlice>> {
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

    pub(super) fn visible_through(&self, seq: WalSeq) -> Vec<RetiredOverlayExtent> {
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

    pub(super) fn remove_retired(&mut self, retired: &[RetiredOverlayExtent]) -> Result<()> {
        for extent in retired {
            self.extents
                .remove_range_with_split(extent.start, extent.end, |overlay, delta| {
                    overlay.split_at(delta)
                })?;
        }
        Ok(())
    }

    #[cfg(test)]
    pub(super) fn debug_extents(&self) -> Vec<(u64, u64, WalSeq, u64)> {
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
