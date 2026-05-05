#![allow(dead_code)]

use crate::{Result, ServerError};
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ExtentEntry<V> {
    start: u64,
    end: u64,
    value: V,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ExtentRef<'a, V> {
    start: u64,
    end: u64,
    value: &'a V,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ExtentMap<V> {
    extents: BTreeMap<u64, Extent<V>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Extent<V> {
    end: u64,
    value: V,
}

impl<V> ExtentEntry<V> {
    pub(crate) fn new(start: u64, end: u64, value: V) -> Result<Self> {
        validate_extent(start, end)?;
        Ok(Self { start, end, value })
    }

    pub(crate) fn start(&self) -> u64 {
        self.start
    }

    pub(crate) fn end(&self) -> u64 {
        self.end
    }

    pub(crate) fn value(&self) -> &V {
        &self.value
    }

    pub(crate) fn into_parts(self) -> (u64, u64, V) {
        (self.start, self.end, self.value)
    }
}

impl<'a, V> ExtentRef<'a, V> {
    pub(crate) fn start(&self) -> u64 {
        self.start
    }

    pub(crate) fn end(&self) -> u64 {
        self.end
    }

    pub(crate) fn value(&self) -> &'a V {
        self.value
    }
}

impl<V> Default for ExtentMap<V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<V> ExtentMap<V> {
    pub(crate) fn new() -> Self {
        Self {
            extents: BTreeMap::new(),
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.extents.len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.extents.is_empty()
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = ExtentRef<'_, V>> {
        self.extents
            .iter()
            .map(|(&start, extent)| extent_ref(start, extent))
    }

    pub(crate) fn overlapping(&self, start: u64, end: u64) -> Result<Vec<ExtentRef<'_, V>>> {
        if start == end {
            return Ok(Vec::new());
        }
        validate_extent(start, end)?;

        let mut refs = Vec::new();
        if let Some((&extent_start, extent)) = self.extents.range(..=start).next_back() {
            if extent.end > start {
                refs.push(extent_ref(extent_start, extent));
            }
        }

        for (&extent_start, extent) in self.extents.range(start..) {
            if extent_start >= end {
                break;
            }
            if refs
                .last()
                .is_some_and(|existing: &ExtentRef<'_, V>| existing.start == extent_start)
            {
                continue;
            }
            refs.push(extent_ref(extent_start, extent));
        }
        Ok(refs)
    }

    pub(crate) fn insert_exact(&mut self, start: u64, end: u64, value: V) -> Result<()> {
        validate_extent(start, end)?;
        self.ensure_no_overlap(start, end)?;
        self.extents.insert(start, Extent { end, value });
        debug_assert!(self.validate().is_ok());
        Ok(())
    }

    pub(crate) fn insert_overwrite_with_split<F>(
        &mut self,
        start: u64,
        end: u64,
        value: V,
        split_value: F,
    ) -> Result<Vec<ExtentEntry<V>>>
    where
        V: Clone,
        F: Fn(&V, u64) -> V,
    {
        validate_extent(start, end)?;
        let removed = self.remove_overlapping(start, end)?;
        self.reinsert_tails(start, end, &removed, split_value)?;
        self.insert_exact(start, end, value)?;
        debug_assert!(self.validate().is_ok());
        Ok(removed)
    }

    pub(crate) fn remove_range_with_split<F>(
        &mut self,
        start: u64,
        end: u64,
        split_value: F,
    ) -> Result<Vec<ExtentEntry<V>>>
    where
        V: Clone,
        F: Fn(&V, u64) -> V,
    {
        if start == end {
            return Ok(Vec::new());
        }
        validate_extent(start, end)?;
        let removed = self.remove_overlapping(start, end)?;
        self.reinsert_tails(start, end, &removed, split_value)?;
        debug_assert!(self.validate().is_ok());
        Ok(removed)
    }

    fn remove_overlapping(&mut self, start: u64, end: u64) -> Result<Vec<ExtentEntry<V>>> {
        let keys = self
            .overlapping(start, end)?
            .into_iter()
            .map(|extent| extent.start())
            .collect::<Vec<_>>();
        let mut removed = Vec::with_capacity(keys.len());
        for key in keys {
            let extent = self.extents.remove(&key).ok_or_else(|| {
                invalid_extent(format!("overlapping extent at {key} disappeared"))
            })?;
            removed.push(ExtentEntry::new(key, extent.end, extent.value)?);
        }
        Ok(removed)
    }

    fn reinsert_tails<F>(
        &mut self,
        start: u64,
        end: u64,
        removed: &[ExtentEntry<V>],
        split_value: F,
    ) -> Result<()>
    where
        V: Clone,
        F: Fn(&V, u64) -> V,
    {
        for extent in removed {
            if extent.start < start {
                self.insert_exact(extent.start, start, split_value(&extent.value, 0))?;
            }
            if extent.end > end {
                let delta = end.checked_sub(extent.start).ok_or_else(|| {
                    invalid_extent("right tail starts before removed extent start")
                })?;
                self.insert_exact(end, extent.end, split_value(&extent.value, delta))?;
            }
        }
        Ok(())
    }

    fn ensure_no_overlap(&self, start: u64, end: u64) -> Result<()> {
        if let Some((&previous_start, previous)) = self.extents.range(..=start).next_back() {
            if previous.end > start {
                return Err(invalid_extent(format!(
                    "new extent [{start}, {end}) overlaps existing [{previous_start}, {})",
                    previous.end
                )));
            }
        }
        if let Some((&next_start, next)) = self.extents.range(start..).next() {
            if next_start < end {
                return Err(invalid_extent(format!(
                    "new extent [{start}, {end}) overlaps existing [{next_start}, {})",
                    next.end
                )));
            }
        }
        Ok(())
    }

    fn validate(&self) -> Result<()> {
        let mut previous_end = None;
        for (&start, extent) in &self.extents {
            validate_extent(start, extent.end)?;
            if let Some(end) = previous_end {
                if start < end {
                    return Err(invalid_extent("extent map contains overlapping entries"));
                }
            }
            previous_end = Some(extent.end);
        }
        Ok(())
    }
}

fn extent_ref<V>(start: u64, extent: &Extent<V>) -> ExtentRef<'_, V> {
    ExtentRef {
        start,
        end: extent.end,
        value: &extent.value,
    }
}

fn validate_extent(start: u64, end: u64) -> Result<()> {
    if start >= end {
        return Err(invalid_extent(format!(
            "extent start {start} must be before end {end}"
        )));
    }
    Ok(())
}

fn invalid_extent(message: impl Into<String>) -> ServerError {
    ServerError::wal("extent map", message)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct Payload {
        id: &'static str,
        offset: u64,
    }

    fn payload(id: &'static str, offset: u64) -> Payload {
        Payload { id, offset }
    }

    fn split(payload: &Payload, delta: u64) -> Payload {
        Payload {
            id: payload.id,
            offset: payload.offset + delta,
        }
    }

    fn entries(map: &ExtentMap<Payload>) -> Vec<(u64, u64, Payload)> {
        map.iter()
            .map(|extent| (extent.start(), extent.end(), extent.value().clone()))
            .collect()
    }

    #[test]
    fn overwrite_middle_preserves_left_and_right_tails() {
        let mut map = ExtentMap::new();
        map.insert_exact(0, 8, payload("A", 0)).expect("insert A");

        map.insert_overwrite_with_split(4, 6, payload("B", 0), split)
            .expect("overwrite middle");

        assert_eq!(
            entries(&map),
            vec![
                (0, 4, payload("A", 0)),
                (4, 6, payload("B", 0)),
                (6, 8, payload("A", 6)),
            ],
        );
    }

    #[test]
    fn overwrite_same_range_replaces_existing_extent() {
        let mut map = ExtentMap::new();
        map.insert_exact(0, 4, payload("A", 0)).expect("insert A");

        let removed = map
            .insert_overwrite_with_split(0, 4, payload("B", 0), split)
            .expect("overwrite");

        assert_eq!(
            removed
                .into_iter()
                .map(ExtentEntry::into_parts)
                .collect::<Vec<_>>(),
            vec![(0, 4, payload("A", 0))],
        );
        assert_eq!(entries(&map), vec![(0, 4, payload("B", 0))]);
    }

    #[test]
    fn remove_range_splits_existing_extent() {
        let mut map = ExtentMap::new();
        map.insert_exact(0, 8, payload("A", 0)).expect("insert A");

        map.remove_range_with_split(2, 6, split)
            .expect("remove middle");

        assert_eq!(
            entries(&map),
            vec![(0, 2, payload("A", 0)), (6, 8, payload("A", 6))],
        );
    }

    #[test]
    fn overlapping_includes_extent_that_starts_before_query() {
        let mut map = ExtentMap::new();
        map.insert_exact(0, 8, payload("A", 0)).expect("insert A");
        map.insert_exact(12, 16, payload("B", 0)).expect("insert B");

        let overlapping = map
            .overlapping(6, 14)
            .expect("overlap")
            .into_iter()
            .map(|extent| (extent.start(), extent.end(), extent.value().clone()))
            .collect::<Vec<_>>();

        assert_eq!(
            overlapping,
            vec![(0, 8, payload("A", 0)), (12, 16, payload("B", 0))],
        );
    }

    #[test]
    fn insert_exact_rejects_overlap() {
        let mut map = ExtentMap::new();
        map.insert_exact(4, 8, payload("A", 0)).expect("insert A");

        assert!(map.insert_exact(0, 5, payload("B", 0)).is_err());
        assert!(map.insert_exact(7, 10, payload("B", 0)).is_err());
    }
}
