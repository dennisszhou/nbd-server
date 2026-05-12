use super::SimpleMutableTree;
use super::validate_range;
use crate::engines::tree::{Block, BlockPart, TreeReader};
use crate::error::Result;
use crate::range::ByteRange;
use crate::storage::MutableBlobStoreHandle;
use bytes::Bytes;
use nbd_control_plane::{ChunkIndex, SIMPLE_CHUNK_BYTES};

#[derive(Debug)]
pub(super) struct SimpleTreeReader {
    blob_store: MutableBlobStoreHandle,
}

impl SimpleTreeReader {
    pub(super) fn new(blob_store: MutableBlobStoreHandle) -> Self {
        Self { blob_store }
    }
}

#[async_trait::async_trait]
impl TreeReader<SimpleMutableTree> for SimpleTreeReader {
    async fn read_committed(&self, root: &SimpleMutableTree, range: ByteRange) -> Result<Block> {
        validate_range("read", range.start(), range.len(), root.size_bytes().await)?;
        let mut parts = Vec::new();
        let mut copied = 0;

        while copied < range.len() as usize {
            let current_offset = range.start() + copied as u64;
            let chunk_index = ChunkIndex::new(current_offset / SIMPLE_CHUNK_BYTES);
            let chunk_offset = current_offset % SIMPLE_CHUNK_BYTES;
            let chunk_available = SIMPLE_CHUNK_BYTES - chunk_offset;
            let copy_len = chunk_available.min(range.len() - copied as u64) as u32;
            let part_range = ByteRange::new(current_offset, copy_len);

            if let Some(key) = root.lookup_chunk(chunk_index).await? {
                let chunk_data = self
                    .blob_store
                    .get_blob(&key, chunk_offset, u64::from(copy_len))
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
