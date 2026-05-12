#![allow(dead_code)]

use crate::error::{Result, ServerError};
use crate::range::ByteRange;
use nbd_control_plane::{ChunkIndex, TREE_CHUNK_BYTES, TreeFormat};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TreeGeometry {
    format: TreeFormat,
    fanout: u16,
    chunk_bytes: u64,
    size_bytes: u64,
    root_level: u16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TreePath {
    chunk_index: ChunkIndex,
    slots: Vec<u16>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TreeNodeSpan {
    level: u16,
    start_bytes: u64,
    len_bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TreeChunkRange {
    chunk_index: ChunkIndex,
    chunk_offset: u64,
    range: ByteRange,
}

impl TreeGeometry {
    pub(crate) fn new(format: TreeFormat, size_bytes: u64) -> Result<Self> {
        if size_bytes == 0 {
            return Err(invalid_tree("tree size must be non-zero"));
        }

        let (fanout, chunk_bytes) = match format {
            TreeFormat::Bounded32V1 => (32, TREE_CHUNK_BYTES),
        };
        let chunk_count = size_bytes
            .checked_add(chunk_bytes - 1)
            .ok_or_else(|| invalid_tree("tree size overflows chunk count"))?
            / chunk_bytes;
        let mut root_level = 1u16;
        let mut capacity_chunks = u64::from(fanout);
        while capacity_chunks < chunk_count {
            capacity_chunks = capacity_chunks
                .checked_mul(u64::from(fanout))
                .ok_or_else(|| invalid_tree("tree capacity overflows"))?;
            root_level = root_level
                .checked_add(1)
                .ok_or_else(|| invalid_tree("tree level overflows"))?;
        }

        Ok(Self {
            format,
            fanout,
            chunk_bytes,
            size_bytes,
            root_level,
        })
    }

    pub(crate) fn format(self) -> TreeFormat {
        self.format
    }

    pub(crate) fn fanout(self) -> u16 {
        self.fanout
    }

    pub(crate) fn chunk_bytes(self) -> u64 {
        self.chunk_bytes
    }

    pub(crate) fn size_bytes(self) -> u64 {
        self.size_bytes
    }

    pub(crate) fn root_level(self) -> u16 {
        self.root_level
    }

    pub(crate) fn root_span(self) -> TreeNodeSpan {
        TreeNodeSpan {
            level: self.root_level,
            start_bytes: 0,
            len_bytes: self.size_bytes,
        }
    }

    pub(crate) fn node_span(self, level: u16, start_bytes: u64) -> Result<TreeNodeSpan> {
        if level > self.root_level {
            return Err(invalid_tree(format!(
                "node level {level} exceeds root level {}",
                self.root_level
            )));
        }
        if start_bytes >= self.size_bytes {
            return Err(invalid_tree(format!(
                "node starts at {start_bytes}, beyond tree size {}",
                self.size_bytes
            )));
        }
        let full_len = self.full_span_len(level)?;
        let remaining = self.size_bytes - start_bytes;
        Ok(TreeNodeSpan {
            level,
            start_bytes,
            len_bytes: full_len.min(remaining),
        })
    }

    pub(crate) fn child_span(self, parent: TreeNodeSpan, slot: u16) -> Result<TreeNodeSpan> {
        if parent.level == 0 {
            return Err(invalid_tree("leaf nodes do not have children"));
        }
        if slot >= self.fanout {
            return Err(invalid_tree(format!(
                "child slot {slot} exceeds fanout {}",
                self.fanout
            )));
        }
        let child_level = parent.level - 1;
        let child_full_len = self.full_span_len(child_level)?;
        let offset = u64::from(slot)
            .checked_mul(child_full_len)
            .ok_or_else(|| invalid_tree("child span offset overflows"))?;
        let child_start = parent
            .start_bytes
            .checked_add(offset)
            .ok_or_else(|| invalid_tree("child span start overflows"))?;
        self.node_span(child_level, child_start)
    }

    pub(crate) fn path_for_chunk(self, chunk_index: ChunkIndex) -> Result<TreePath> {
        self.validate_chunk_index(chunk_index)?;
        let mut slots = Vec::with_capacity(usize::from(self.root_level));
        for child_level in (0..self.root_level).rev() {
            let divisor = self.chunks_per_node(child_level)?;
            let slot = (chunk_index.get() / divisor) % u64::from(self.fanout);
            let slot =
                u16::try_from(slot).map_err(|_| invalid_tree("path slot does not fit in u16"))?;
            slots.push(slot);
        }
        Ok(TreePath { chunk_index, slots })
    }

    pub(crate) fn chunks_for_range(self, range: ByteRange) -> Result<Vec<TreeChunkRange>> {
        let end = range
            .checked_end()
            .ok_or_else(|| invalid_tree("range end overflows"))?;
        if end > self.size_bytes {
            return Err(ServerError::OutOfBounds {
                operation: "tree range",
                offset: range.start(),
                length: range.len(),
                size_bytes: self.size_bytes,
            });
        }
        if range.is_empty() {
            return Ok(Vec::new());
        }

        let mut chunks = Vec::new();
        let mut copied = 0u64;
        while copied < range.len() {
            let current_offset = range.start() + copied;
            let chunk_index = ChunkIndex::new(current_offset / self.chunk_bytes);
            let chunk_offset = current_offset % self.chunk_bytes;
            let chunk_available = self.chunk_bytes - chunk_offset;
            let copy_len = chunk_available.min(range.len() - copied);
            let copy_len = u32::try_from(copy_len)
                .map_err(|_| invalid_tree("chunk range length does not fit u32"))?;
            chunks.push(TreeChunkRange {
                chunk_index,
                chunk_offset,
                range: ByteRange::new(current_offset, copy_len),
            });
            copied += u64::from(copy_len);
        }
        Ok(chunks)
    }

    fn validate_chunk_index(self, chunk_index: ChunkIndex) -> Result<()> {
        let start = chunk_index
            .get()
            .checked_mul(self.chunk_bytes)
            .ok_or_else(|| invalid_tree("chunk byte offset overflows"))?;
        if start >= self.size_bytes {
            return Err(invalid_tree(format!(
                "chunk {chunk_index} starts beyond tree size {}",
                self.size_bytes
            )));
        }
        Ok(())
    }

    fn full_span_len(self, level: u16) -> Result<u64> {
        self.chunk_bytes
            .checked_mul(self.chunks_per_node(level)?)
            .ok_or_else(|| invalid_tree("node span length overflows"))
    }

    fn chunks_per_node(self, level: u16) -> Result<u64> {
        let mut chunks = 1u64;
        for _ in 0..level {
            chunks = chunks
                .checked_mul(u64::from(self.fanout))
                .ok_or_else(|| invalid_tree("node chunk capacity overflows"))?;
        }
        Ok(chunks)
    }
}

impl TreePath {
    pub(crate) fn chunk_index(&self) -> ChunkIndex {
        self.chunk_index
    }

    pub(crate) fn slots(&self) -> &[u16] {
        &self.slots
    }
}

impl TreeNodeSpan {
    pub(crate) fn level(self) -> u16 {
        self.level
    }

    pub(crate) fn start_bytes(self) -> u64 {
        self.start_bytes
    }

    pub(crate) fn len_bytes(self) -> u64 {
        self.len_bytes
    }
}

impl TreeChunkRange {
    pub(crate) fn chunk_index(self) -> ChunkIndex {
        self.chunk_index
    }

    pub(crate) fn chunk_offset(self) -> u64 {
        self.chunk_offset
    }

    pub(crate) fn range(self) -> ByteRange {
        self.range
    }
}

fn invalid_tree(message: impl Into<String>) -> ServerError {
    ServerError::Catalog {
        message: message.into(),
        source: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bounded_32_v1_uses_three_internal_levels_for_one_tib() {
        let geometry =
            TreeGeometry::new(TreeFormat::Bounded32V1, 1024 * 1024 * 1024 * 1024).unwrap();

        assert_eq!(geometry.fanout(), 32);
        assert_eq!(geometry.size_bytes(), 1024 * 1024 * 1024 * 1024);
        assert_eq!(geometry.root_level(), 3);
        assert_eq!(geometry.root_span().len_bytes(), 1024 * 1024 * 1024 * 1024);

        let path = geometry
            .path_for_chunk(ChunkIndex::new(32 * 32 * 32 - 1))
            .unwrap();
        assert_eq!(path.chunk_index(), ChunkIndex::new(32 * 32 * 32 - 1));
        assert_eq!(path.slots(), &[31, 31, 31]);
    }

    #[test]
    fn chunks_for_range_is_bounded_to_touched_chunks() {
        let geometry = TreeGeometry::new(TreeFormat::Bounded32V1, TREE_CHUNK_BYTES * 4).unwrap();
        let range = ByteRange::new(TREE_CHUNK_BYTES - 4, 12);

        let chunks = geometry.chunks_for_range(range).unwrap();

        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].chunk_index(), ChunkIndex::new(0));
        assert_eq!(chunks[0].chunk_offset(), TREE_CHUNK_BYTES - 4);
        assert_eq!(chunks[0].range(), ByteRange::new(TREE_CHUNK_BYTES - 4, 4));
        assert_eq!(chunks[1].chunk_index(), ChunkIndex::new(1));
        assert_eq!(chunks[1].chunk_offset(), 0);
        assert_eq!(chunks[1].range(), ByteRange::new(TREE_CHUNK_BYTES, 8));
    }
}
