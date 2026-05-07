use crate::error::{Result, ServerError};
use crate::range::ByteRange;
use crate::wal::WalRecord;

use super::extent_map::ExtentMap;
use bytes::Bytes;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::Arc;

pub(crate) const CACHE_BLOCK_BYTES: u64 = 2 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CacheInsertPlacement {
    Hot,
    Cold,
}

#[derive(Debug, Clone)]
pub(crate) struct CacheReadSlice {
    start: u64,
    end: u64,
    object_id: CacheObjectId,
    object_offset: u64,
    payload: CachePayload,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ReadCache {
    extents: ExtentMap<CacheExtent>,
    objects: BTreeMap<CacheObjectId, CacheObject>,
    lru: CacheObjectLru,
    max_bytes: usize,
    charged_bytes: usize,
    next_object_id: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct CacheObjectId(u64);

#[derive(Debug, Clone, PartialEq, Eq)]
struct CacheExtent {
    object_id: CacheObjectId,
    object_offset: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CacheObject {
    logical_start: u64,
    len: u64,
    payload: CachePayload,
    extent_starts: BTreeSet<u64>,
    charged_bytes: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum CachePayload {
    Bytes(Bytes),
    WalRecord { record: Arc<WalRecord> },
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CacheObjectLru {
    order: VecDeque<CacheObjectId>,
}

#[derive(Debug, Clone)]
struct ExistingSource {
    start: u64,
    end: u64,
    object_id: CacheObjectId,
    object_offset: u64,
    payload: CachePayload,
}

impl ReadCache {
    pub(crate) fn new(max_bytes: usize) -> Self {
        Self {
            extents: ExtentMap::new(),
            objects: BTreeMap::new(),
            lru: CacheObjectLru::new(),
            max_bytes,
            charged_bytes: 0,
            next_object_id: 1,
        }
    }

    pub(crate) fn read_slices(&self, range: ByteRange) -> Result<Vec<CacheReadSlice>> {
        let start = range.start();
        let end = range_end(range);
        self.extents
            .overlapping(start, end)?
            .into_iter()
            .map(|extent| {
                let slice_start = start.max(extent.start());
                let slice_end = end.min(extent.end());
                let object = self.objects.get(&extent.value().object_id).ok_or_else(|| {
                    ServerError::wal("read cache", "cache extent references missing object")
                })?;
                let object_offset = extent
                    .value()
                    .object_offset
                    .checked_add(slice_start - extent.start())
                    .ok_or_else(|| ServerError::wal("read cache", "object offset overflowed"))?;
                Ok(CacheReadSlice {
                    start: slice_start,
                    end: slice_end,
                    object_id: extent.value().object_id,
                    object_offset,
                    payload: object.payload.clone(),
                })
            })
            .collect()
    }

    pub(crate) fn insert_bytes(
        &mut self,
        range: ByteRange,
        bytes: Bytes,
        placement: CacheInsertPlacement,
    ) -> Result<()> {
        if bytes.len() as u64 != range.len() {
            return Err(ServerError::wal(
                "insert read cache bytes",
                format!(
                    "payload length {} does not match range length {}",
                    bytes.len(),
                    range.len()
                ),
            ));
        }
        if range.is_empty() {
            return Ok(());
        }

        let range_start = range.start();
        let range_end = range_end(range);
        let mut start = range_start;
        while start < range_end {
            let window_end = cache_block_end(start)?;
            let end = range_end.min(window_end);
            let bytes_start = usize::try_from(start - range_start).map_err(|_| {
                ServerError::wal("insert read cache bytes", "slice start does not fit usize")
            })?;
            let bytes_end = bytes_start
                .checked_add(usize::try_from(end - start).map_err(|_| {
                    ServerError::wal("insert read cache bytes", "slice length does not fit usize")
                })?)
                .ok_or_else(|| ServerError::wal("insert read cache bytes", "slice overflowed"))?;
            self.insert_window_bytes(start, end, bytes.slice(bytes_start..bytes_end), placement)?;
            start = end;
        }
        self.evict_to_budget()?;
        Ok(())
    }

    pub(crate) fn insert_wal_record_slice(
        &mut self,
        range: ByteRange,
        record: Arc<WalRecord>,
        record_offset: u64,
        placement: CacheInsertPlacement,
    ) -> Result<()> {
        let Some(record_end) = record_offset.checked_add(range.len()) else {
            return Err(ServerError::wal(
                "insert read cache WAL record",
                "record offset overflowed",
            ));
        };
        if record_end > record.data().len() as u64 {
            return Err(ServerError::wal(
                "insert read cache WAL record",
                "retired WAL slice exceeds record payload",
            ));
        }
        if range.is_empty() {
            return Ok(());
        }

        let range_start = range.start();
        let range_end = range_end(range);
        let mut start = range_start;
        while start < range_end {
            let window_end = cache_block_end(start)?;
            let end = range_end.min(window_end);
            let slice_offset = record_offset
                .checked_add(start - range_start)
                .ok_or_else(|| {
                    ServerError::wal("insert read cache WAL record", "record offset overflowed")
                })?;
            self.insert_window_wal_record(start, end, record.clone(), slice_offset, placement)?;
            start = end;
        }
        self.evict_to_budget()?;
        Ok(())
    }

    pub(crate) fn trim_range(&mut self, range: ByteRange) -> Result<()> {
        if range.is_empty() {
            return Ok(());
        }
        self.extents.remove_range_with_split(
            range.start(),
            range_end(range),
            |extent, delta| extent.split_at(delta),
        )?;
        self.rebuild_backrefs_and_gc();
        Ok(())
    }

    pub(crate) fn promote_hits(&mut self, hits: impl IntoIterator<Item = CacheObjectId>) {
        for id in hits {
            if self.objects.contains_key(&id) {
                self.lru.promote(id);
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn charged_bytes(&self) -> usize {
        self.charged_bytes
    }

    fn insert_window_bytes(
        &mut self,
        start: u64,
        end: u64,
        bytes: Bytes,
        placement: CacheInsertPlacement,
    ) -> Result<()> {
        debug_assert!(same_cache_block(start, end));
        let sources = self.existing_sources_for_window(start)?;
        let connected = connected_sources(start, end, sources);
        let component_start = connected
            .iter()
            .map(|source| source.start)
            .chain(std::iter::once(start))
            .min()
            .expect("component has new source");
        let component_end = connected
            .iter()
            .map(|source| source.end)
            .chain(std::iter::once(end))
            .max()
            .expect("component has new source");
        let input_ids = connected
            .iter()
            .map(|source| source.object_id)
            .collect::<Vec<_>>();
        let lru_position = self.lru.coldest_position(&input_ids);

        let payload = if connected.is_empty() && component_start == start && component_end == end {
            CachePayload::Bytes(bytes)
        } else {
            CachePayload::Bytes(materialize_merged_bytes(
                component_start,
                component_end,
                start,
                end,
                &bytes,
                &connected,
            )?)
        };

        self.extents
            .remove_range_with_split(component_start, component_end, |extent, delta| {
                extent.split_at(delta)
            })?;

        let object_id = self.insert_object(component_start, component_end, payload, placement);
        self.extents.insert_exact(
            component_start,
            component_end,
            CacheExtent {
                object_id,
                object_offset: 0,
            },
        )?;
        self.rebuild_backrefs_and_gc_with_inherited_lru(object_id, lru_position, placement);
        Ok(())
    }

    fn insert_window_wal_record(
        &mut self,
        start: u64,
        end: u64,
        record: Arc<WalRecord>,
        record_offset: u64,
        placement: CacheInsertPlacement,
    ) -> Result<()> {
        debug_assert!(same_cache_block(start, end));
        let sources = self.existing_sources_for_window(start)?;
        let connected = connected_sources(start, end, sources);
        let component_start = connected
            .iter()
            .map(|source| source.start)
            .chain(std::iter::once(start))
            .min()
            .expect("component has new source");
        let component_end = connected
            .iter()
            .map(|source| source.end)
            .chain(std::iter::once(end))
            .max()
            .expect("component has new source");
        let input_ids = connected
            .iter()
            .map(|source| source.object_id)
            .collect::<Vec<_>>();
        let lru_position = self.lru.coldest_position(&input_ids);

        let keep_wal_record = connected.is_empty()
            && record_offset == 0
            && end - start == record.data().len() as u64
            && record.data().len() as u64 <= CACHE_BLOCK_BYTES;
        let (payload, extent_object_offset) = if keep_wal_record {
            (CachePayload::WalRecord { record }, 0)
        } else {
            (
                CachePayload::Bytes(materialize_merged_wal_bytes(
                    component_start,
                    component_end,
                    start,
                    end,
                    record_offset,
                    &record,
                    &connected,
                )?),
                0,
            )
        };

        self.extents
            .remove_range_with_split(component_start, component_end, |extent, delta| {
                extent.split_at(delta)
            })?;

        let object_id = self.insert_object(component_start, component_end, payload, placement);
        self.extents.insert_exact(
            component_start,
            component_end,
            CacheExtent {
                object_id,
                object_offset: extent_object_offset,
            },
        )?;
        self.rebuild_backrefs_and_gc_with_inherited_lru(object_id, lru_position, placement);
        Ok(())
    }

    fn existing_sources_for_window(&self, start: u64) -> Result<Vec<ExistingSource>> {
        let window_start = cache_block_start(start);
        let window_end = window_start
            .checked_add(CACHE_BLOCK_BYTES)
            .ok_or_else(|| ServerError::wal("read cache", "cache block end overflowed"))?;
        self.extents
            .overlapping(window_start, window_end)?
            .into_iter()
            .map(|extent| {
                let object = self.objects.get(&extent.value().object_id).ok_or_else(|| {
                    ServerError::wal("read cache", "cache extent references missing object")
                })?;
                Ok(ExistingSource {
                    start: extent.start(),
                    end: extent.end(),
                    object_id: extent.value().object_id,
                    object_offset: extent.value().object_offset,
                    payload: object.payload.clone(),
                })
            })
            .collect()
    }

    fn insert_object(
        &mut self,
        start: u64,
        end: u64,
        payload: CachePayload,
        placement: CacheInsertPlacement,
    ) -> CacheObjectId {
        let id = CacheObjectId(self.next_object_id);
        self.next_object_id += 1;
        let charged_bytes = payload.charged_bytes();
        self.charged_bytes += charged_bytes;
        self.objects.insert(
            id,
            CacheObject {
                logical_start: start,
                len: end - start,
                payload,
                extent_starts: BTreeSet::new(),
                charged_bytes,
            },
        );
        match placement {
            CacheInsertPlacement::Hot => self.lru.push_front(id),
            CacheInsertPlacement::Cold => self.lru.push_back(id),
        }
        id
    }

    fn evict_to_budget(&mut self) -> Result<()> {
        while self.charged_bytes > self.max_bytes {
            let Some(id) = self.lru.tail() else {
                break;
            };
            self.evict_object(id)?;
        }
        Ok(())
    }

    fn evict_object(&mut self, id: CacheObjectId) -> Result<()> {
        let Some(object) = self.objects.get(&id) else {
            self.lru.remove(id);
            return Ok(());
        };
        let starts = object.extent_starts.iter().copied().collect::<Vec<_>>();
        for start in starts {
            if let Some(extent) = self.extents.get(start) {
                self.extents.remove_range_with_split(
                    extent.start(),
                    extent.end(),
                    |extent, delta| extent.split_at(delta),
                )?;
            }
        }
        self.remove_object(id);
        Ok(())
    }

    fn rebuild_backrefs_and_gc(&mut self) {
        self.rebuild_backrefs_and_gc_with_inherited_lru(
            CacheObjectId(0),
            None,
            CacheInsertPlacement::Hot,
        );
    }

    fn rebuild_backrefs_and_gc_with_inherited_lru(
        &mut self,
        inherited_id: CacheObjectId,
        inherited_position: Option<usize>,
        placement: CacheInsertPlacement,
    ) {
        for object in self.objects.values_mut() {
            object.extent_starts.clear();
        }
        for extent in self.extents.iter() {
            if let Some(object) = self.objects.get_mut(&extent.value().object_id) {
                object.extent_starts.insert(extent.start());
            }
        }

        let empty_objects = self
            .objects
            .iter()
            .filter_map(|(&id, object)| object.extent_starts.is_empty().then_some(id))
            .collect::<Vec<_>>();
        for id in empty_objects {
            self.remove_object(id);
        }

        if self.objects.contains_key(&inherited_id) {
            self.lru.remove(inherited_id);
            if let Some(position) = inherited_position {
                self.lru.insert_at(position, inherited_id);
            } else {
                match placement {
                    CacheInsertPlacement::Hot => self.lru.push_front(inherited_id),
                    CacheInsertPlacement::Cold => self.lru.push_back(inherited_id),
                }
            }
        }
    }

    fn remove_object(&mut self, id: CacheObjectId) {
        if let Some(object) = self.objects.remove(&id) {
            self.charged_bytes -= object.charged_bytes;
        }
        self.lru.remove(id);
    }

    #[cfg(test)]
    fn debug_extents(&self) -> Vec<(u64, u64, CacheObjectId, u64)> {
        self.extents
            .iter()
            .map(|extent| {
                (
                    extent.start(),
                    extent.end(),
                    extent.value().object_id,
                    extent.value().object_offset,
                )
            })
            .collect()
    }

    #[cfg(test)]
    fn debug_lru(&self) -> Vec<CacheObjectId> {
        self.lru.order.iter().copied().collect()
    }

    #[cfg(test)]
    pub(crate) fn debug_wal_seqs(&self) -> Vec<u64> {
        self.objects
            .values()
            .filter_map(|object| match &object.payload {
                CachePayload::Bytes(_) => None,
                CachePayload::WalRecord { record } => Some(record.seq().get()),
            })
            .collect()
    }
}

impl CacheReadSlice {
    pub(crate) fn start(&self) -> u64 {
        self.start
    }

    pub(crate) fn end(&self) -> u64 {
        self.end
    }

    pub(crate) fn object_id(&self) -> CacheObjectId {
        self.object_id
    }

    pub(crate) fn copy_to(&self, read_range: ByteRange, data: &mut [u8]) -> Result<()> {
        let start = read_range.start().max(self.start);
        let end = range_end(read_range).min(self.end);
        if start >= end {
            return Ok(());
        }
        let dst_start = usize::try_from(start - read_range.start())
            .map_err(|_| ServerError::wal("read cache", "destination offset does not fit usize"))?;
        let src_start = self
            .object_offset
            .checked_add(start - self.start)
            .ok_or_else(|| ServerError::wal("read cache", "source offset overflowed"))?;
        self.payload
            .copy_to(src_start, end - start, &mut data[dst_start..])
    }
}

impl CacheExtent {
    fn split_at(&self, delta: u64) -> Self {
        Self {
            object_id: self.object_id,
            object_offset: self.object_offset + delta,
        }
    }
}

impl CachePayload {
    fn charged_bytes(&self) -> usize {
        match self {
            Self::Bytes(bytes) => bytes.len(),
            Self::WalRecord { record } => record.data().len(),
        }
    }

    fn copy_to(&self, offset: u64, len: u64, dst: &mut [u8]) -> Result<()> {
        let offset = usize::try_from(offset)
            .map_err(|_| ServerError::wal("read cache", "payload offset does not fit usize"))?;
        let len = usize::try_from(len)
            .map_err(|_| ServerError::wal("read cache", "payload length does not fit usize"))?;
        match self {
            Self::Bytes(bytes) => {
                dst[..len].copy_from_slice(&bytes[offset..offset + len]);
            }
            Self::WalRecord { record } => {
                dst[..len].copy_from_slice(&record.data()[offset..offset + len]);
            }
        }
        Ok(())
    }
}

impl CacheObjectLru {
    fn new() -> Self {
        Self {
            order: VecDeque::new(),
        }
    }

    fn push_front(&mut self, id: CacheObjectId) {
        debug_assert!(!self.order.contains(&id));
        self.order.push_front(id);
    }

    fn push_back(&mut self, id: CacheObjectId) {
        debug_assert!(!self.order.contains(&id));
        self.order.push_back(id);
    }

    fn insert_at(&mut self, position: usize, id: CacheObjectId) {
        debug_assert!(!self.order.contains(&id));
        let position = position.min(self.order.len());
        self.order.insert(position, id);
    }

    fn promote(&mut self, id: CacheObjectId) {
        if self.remove(id).is_some() {
            self.push_front(id);
        }
    }

    fn remove(&mut self, id: CacheObjectId) -> Option<usize> {
        let position = self.order.iter().position(|existing| *existing == id)?;
        self.order.remove(position);
        Some(position)
    }

    fn tail(&self) -> Option<CacheObjectId> {
        self.order.back().copied()
    }

    fn coldest_position(&self, ids: &[CacheObjectId]) -> Option<usize> {
        ids.iter()
            .filter_map(|id| self.order.iter().position(|existing| existing == id))
            .max()
    }
}

fn connected_sources(start: u64, end: u64, sources: Vec<ExistingSource>) -> Vec<ExistingSource> {
    let mut component_start = start;
    let mut component_end = end;
    let mut selected = vec![false; sources.len()];

    loop {
        let mut changed = false;
        for (idx, source) in sources.iter().enumerate() {
            if selected[idx] {
                continue;
            }
            if source.start <= component_end && source.end >= component_start {
                selected[idx] = true;
                component_start = component_start.min(source.start);
                component_end = component_end.max(source.end);
                changed = true;
            }
        }
        if !changed {
            break;
        }
    }

    sources
        .into_iter()
        .zip(selected)
        .filter_map(|(source, selected)| selected.then_some(source))
        .collect()
}

fn materialize_merged_bytes(
    component_start: u64,
    component_end: u64,
    new_start: u64,
    new_end: u64,
    new_bytes: &Bytes,
    existing: &[ExistingSource],
) -> Result<Bytes> {
    let len = usize::try_from(component_end - component_start)
        .map_err(|_| ServerError::wal("read cache", "merged cache object too large"))?;
    let mut merged = vec![0; len];
    let mut coverage = Vec::with_capacity(existing.len() + 1);

    for source in existing {
        copy_source(
            component_start,
            source.start,
            source.end,
            source.object_offset,
            &source.payload,
            &mut merged,
        )?;
        coverage.push((source.start, source.end));
    }

    copy_bytes_source(
        component_start,
        new_start,
        new_end,
        0,
        new_bytes,
        &mut merged,
    )?;
    coverage.push((new_start, new_end));
    validate_coverage(component_start, component_end, coverage)?;
    Ok(Bytes::from(merged))
}

fn materialize_merged_wal_bytes(
    component_start: u64,
    component_end: u64,
    new_start: u64,
    new_end: u64,
    record_offset: u64,
    record: &WalRecord,
    existing: &[ExistingSource],
) -> Result<Bytes> {
    let len = usize::try_from(component_end - component_start)
        .map_err(|_| ServerError::wal("read cache", "merged cache object too large"))?;
    let mut merged = vec![0; len];
    let mut coverage = Vec::with_capacity(existing.len() + 1);

    for source in existing {
        copy_source(
            component_start,
            source.start,
            source.end,
            source.object_offset,
            &source.payload,
            &mut merged,
        )?;
        coverage.push((source.start, source.end));
    }

    copy_wal_source(
        component_start,
        new_start,
        new_end,
        record_offset,
        record,
        &mut merged,
    )?;
    coverage.push((new_start, new_end));
    validate_coverage(component_start, component_end, coverage)?;
    Ok(Bytes::from(merged))
}

fn copy_source(
    component_start: u64,
    source_start: u64,
    source_end: u64,
    object_offset: u64,
    payload: &CachePayload,
    merged: &mut [u8],
) -> Result<()> {
    let dst_start = usize::try_from(source_start - component_start)
        .map_err(|_| ServerError::wal("read cache", "merge destination does not fit usize"))?;
    payload.copy_to(
        object_offset,
        source_end - source_start,
        &mut merged[dst_start..],
    )
}

fn copy_bytes_source(
    component_start: u64,
    source_start: u64,
    source_end: u64,
    source_offset: u64,
    bytes: &Bytes,
    merged: &mut [u8],
) -> Result<()> {
    let dst_start = usize::try_from(source_start - component_start)
        .map_err(|_| ServerError::wal("read cache", "merge destination does not fit usize"))?;
    let src_start = usize::try_from(source_offset)
        .map_err(|_| ServerError::wal("read cache", "merge source does not fit usize"))?;
    let len = usize::try_from(source_end - source_start)
        .map_err(|_| ServerError::wal("read cache", "merge length does not fit usize"))?;
    merged[dst_start..dst_start + len].copy_from_slice(&bytes[src_start..src_start + len]);
    Ok(())
}

fn copy_wal_source(
    component_start: u64,
    source_start: u64,
    source_end: u64,
    record_offset: u64,
    record: &WalRecord,
    merged: &mut [u8],
) -> Result<()> {
    let dst_start = usize::try_from(source_start - component_start)
        .map_err(|_| ServerError::wal("read cache", "merge destination does not fit usize"))?;
    let src_start = usize::try_from(record_offset)
        .map_err(|_| ServerError::wal("read cache", "merge source does not fit usize"))?;
    let len = usize::try_from(source_end - source_start)
        .map_err(|_| ServerError::wal("read cache", "merge length does not fit usize"))?;
    merged[dst_start..dst_start + len].copy_from_slice(&record.data()[src_start..src_start + len]);
    Ok(())
}

fn validate_coverage(start: u64, end: u64, mut spans: Vec<(u64, u64)>) -> Result<()> {
    spans.sort_by_key(|(span_start, _)| *span_start);
    let mut covered = start;
    for (span_start, span_end) in spans {
        if span_end <= covered {
            continue;
        }
        if span_start > covered {
            return Err(ServerError::wal(
                "read cache",
                format!("merge sources leave uncovered gap [{covered}, {span_start})"),
            ));
        }
        covered = covered.max(span_end);
    }
    if covered != end {
        return Err(ServerError::wal(
            "read cache",
            format!("merge sources end at {covered}, expected {end}"),
        ));
    }
    Ok(())
}

fn same_cache_block(start: u64, end: u64) -> bool {
    start < end && cache_block_start(start) == cache_block_start(end - 1)
}

fn cache_block_start(offset: u64) -> u64 {
    (offset / CACHE_BLOCK_BYTES) * CACHE_BLOCK_BYTES
}

fn cache_block_end(offset: u64) -> Result<u64> {
    cache_block_start(offset)
        .checked_add(CACHE_BLOCK_BYTES)
        .ok_or_else(|| ServerError::wal("read cache", "cache block end overflowed"))
}

fn range_end(range: ByteRange) -> u64 {
    range.start().saturating_add(range.len())
}

#[cfg(test)]
mod tests {
    use super::*;
    use nbd_control_plane::WalSeq;

    #[test]
    fn adjacent_same_window_fills_merge_and_inherit_lru_position() {
        let mut cache = ReadCache::new(1024);
        cache
            .insert_bytes(
                ByteRange::new(0, 4),
                Bytes::from_static(b"left"),
                CacheInsertPlacement::Hot,
            )
            .expect("insert left");
        let left_id = cache.debug_extents()[0].2;
        cache
            .insert_bytes(
                ByteRange::new(64, 4),
                Bytes::from_static(b"hot!"),
                CacheInsertPlacement::Hot,
            )
            .expect("insert unrelated");

        cache
            .insert_bytes(
                ByteRange::new(4, 5),
                Bytes::from_static(b"right"),
                CacheInsertPlacement::Hot,
            )
            .expect("insert adjacent");

        let extents = cache.debug_extents();
        assert_eq!(extents.len(), 2);
        assert_eq!((extents[0].0, extents[0].1), (0, 9));
        assert_ne!(extents[0].2, left_id);
        assert_eq!(read_all(&cache, ByteRange::new(0, 9)), b"leftright");
        assert_eq!(cache.debug_lru().len(), 2);
        assert_eq!(cache.debug_lru()[1], extents[0].2);
    }

    #[test]
    fn same_window_gap_does_not_merge() {
        let mut cache = ReadCache::new(1024);
        cache
            .insert_bytes(
                ByteRange::new(0, 4),
                Bytes::from_static(b"left"),
                CacheInsertPlacement::Hot,
            )
            .expect("insert left");
        cache
            .insert_bytes(
                ByteRange::new(8, 5),
                Bytes::from_static(b"right"),
                CacheInsertPlacement::Hot,
            )
            .expect("insert right");

        let extents = cache.debug_extents();
        assert_eq!(extents.len(), 2);
        assert_eq!((extents[0].0, extents[0].1), (0, 4));
        assert_eq!((extents[1].0, extents[1].1), (8, 13));
    }

    #[test]
    fn cache_objects_do_not_cross_aligned_cache_blocks() {
        let mut cache = ReadCache::new((CACHE_BLOCK_BYTES * 2) as usize);
        cache
            .insert_bytes(
                ByteRange::new(CACHE_BLOCK_BYTES - 2, 4),
                Bytes::from_static(b"span"),
                CacheInsertPlacement::Hot,
            )
            .expect("insert crossing bytes");

        let extents = cache.debug_extents();
        assert_eq!(extents.len(), 2);
        assert_eq!(
            (extents[0].0, extents[0].1),
            (CACHE_BLOCK_BYTES - 2, CACHE_BLOCK_BYTES)
        );
        assert_eq!(
            (extents[1].0, extents[1].1),
            (CACHE_BLOCK_BYTES, CACHE_BLOCK_BYTES + 2)
        );
    }

    #[test]
    fn eviction_uses_charged_bytes_and_exact_lru() {
        let mut cache = ReadCache::new(8);
        cache
            .insert_bytes(
                ByteRange::new(0, 8),
                Bytes::from_static(b"aaaaaaaa"),
                CacheInsertPlacement::Hot,
            )
            .expect("insert first");
        cache
            .insert_bytes(
                ByteRange::new(16, 8),
                Bytes::from_static(b"bbbbbbbb"),
                CacheInsertPlacement::Hot,
            )
            .expect("insert second");

        assert_eq!(cache.charged_bytes(), 8);
        assert_eq!(cache.debug_extents().len(), 1);
        assert_eq!(
            (cache.debug_extents()[0].0, cache.debug_extents()[0].1),
            (16, 24)
        );
        assert_eq!(cache.debug_lru().len(), 1);
    }

    #[test]
    fn trim_splits_extents_and_drops_empty_objects() {
        let mut cache = ReadCache::new(1024);
        cache
            .insert_bytes(
                ByteRange::new(0, 16),
                Bytes::from_static(b"0123456789abcdef"),
                CacheInsertPlacement::Hot,
            )
            .expect("insert object");

        cache.trim_range(ByteRange::new(4, 8)).expect("trim middle");

        let extents = cache.debug_extents();
        assert_eq!(extents.len(), 2);
        assert_eq!((extents[0].0, extents[0].1, extents[0].3), (0, 4, 0));
        assert_eq!((extents[1].0, extents[1].1, extents[1].3), (12, 16, 12));
        assert_eq!(read_all(&cache, ByteRange::new(0, 4)), b"0123");
        assert_eq!(read_all(&cache, ByteRange::new(12, 4)), b"cdef");

        cache.trim_range(ByteRange::new(0, 16)).expect("trim all");
        assert_eq!(cache.charged_bytes(), 0);
        assert!(cache.debug_extents().is_empty());
        assert!(cache.debug_lru().is_empty());
    }

    #[test]
    fn wal_record_payload_charges_full_record_when_kept() {
        let mut cache = ReadCache::new(1024);
        let record = Arc::new(
            WalRecord::new(WalSeq::new(7), ByteRange::new(0, 4), b"wal!".to_vec())
                .expect("WAL record"),
        );

        cache
            .insert_wal_record_slice(ByteRange::new(0, 4), record, 0, CacheInsertPlacement::Cold)
            .expect("insert WAL cache object");

        assert_eq!(cache.charged_bytes(), 4);
        assert_eq!(cache.debug_wal_seqs(), vec![7]);
        assert_eq!(read_all(&cache, ByteRange::new(0, 4)), b"wal!");
        assert_eq!(cache.debug_lru().len(), 1);
    }

    fn read_all(cache: &ReadCache, range: ByteRange) -> Vec<u8> {
        let mut data = vec![0; range.len() as usize];
        for slice in cache.read_slices(range).expect("cache read") {
            slice.copy_to(range, &mut data).expect("copy cache slice");
        }
        data
    }
}
