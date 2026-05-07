use super::validate_range;
use crate::engines::tree::{Block, BlockPart, TreeReader};
use crate::error::Result;
use crate::range::ByteRange;
use crate::storage::MutableBlobStoreHandle;
use bytes::Bytes;
use nbd_control_plane::{ChunkIndex, SIMPLE_CHUNK_BYTES, SimpleTreeSnapshot};

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
impl TreeReader<SimpleTreeSnapshot> for SimpleTreeReader {
    async fn read_committed(&self, root: &SimpleTreeSnapshot, range: ByteRange) -> Result<Block> {
        validate_range("read", range.start(), range.len(), root.size_bytes())?;
        let mut parts = Vec::new();
        let mut copied = 0;

        while copied < range.len() as usize {
            let current_offset = range.start() + copied as u64;
            let chunk_index = ChunkIndex::new(current_offset / SIMPLE_CHUNK_BYTES);
            let chunk_offset = current_offset % SIMPLE_CHUNK_BYTES;
            let chunk_available = SIMPLE_CHUNK_BYTES - chunk_offset;
            let copy_len = chunk_available.min(range.len() - copied as u64) as u32;
            let part_range = ByteRange::new(current_offset, copy_len);

            if let Some(chunk) = root.chunk(chunk_index) {
                let chunk_data = self
                    .blob_store
                    .get_blob(chunk.blob_key(), chunk_offset, u64::from(copy_len))
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{LocalBlobStore, put_random_blob};
    use nbd_control_plane::{ExportId, NodeId, SimpleChunkRef};
    use nbd_test_support::TestRuntime;
    use std::collections::BTreeMap;
    use std::sync::Arc;

    #[tokio::test]
    async fn simple_tree_reader_reads_from_snapshot() {
        let runtime = TestRuntime::new().expect("test runtime");
        let blob_store: MutableBlobStoreHandle =
            Arc::new(LocalBlobStore::new(runtime.root_path().join("blobs")));
        let mut chunk_data = vec![0; 4096];
        chunk_data[8..12].copy_from_slice(b"tree");
        let key = put_random_blob(blob_store.as_ref(), &chunk_data)
            .await
            .expect("create blob");
        let chunk =
            SimpleChunkRef::new(ChunkIndex::new(0), key, SIMPLE_CHUNK_BYTES).expect("chunk ref");
        let snapshot = SimpleTreeSnapshot::new(
            ExportId::new("simple-tree-reader").expect("export id"),
            4096,
            Some(NodeId::new("root-node").expect("node id")),
            BTreeMap::from([(ChunkIndex::new(0), chunk)]),
        )
        .expect("tree snapshot");
        let reader = SimpleTreeReader { blob_store };

        let block = reader
            .read_committed(&snapshot, ByteRange::new(8, 4))
            .await
            .expect("read committed tree");

        assert_eq!(
            block.parts(),
            &[BlockPart::Data {
                range: ByteRange::new(8, 4),
                bytes: Bytes::from_static(b"tree"),
            }],
        );
        assert_eq!(block.materialize().expect("materialize"), b"tree");
    }

    #[tokio::test]
    async fn simple_tree_reader_splits_large_sparse_reads_on_chunk_boundaries() {
        let runtime = TestRuntime::new().expect("test runtime");
        let blob_store: MutableBlobStoreHandle =
            Arc::new(LocalBlobStore::new(runtime.root_path().join("blobs")));
        let snapshot = SimpleTreeSnapshot::new(
            ExportId::new("simple-tree-reader-sparse").expect("export id"),
            SIMPLE_CHUNK_BYTES * 2,
            None,
            BTreeMap::new(),
        )
        .expect("tree snapshot");
        let reader = SimpleTreeReader { blob_store };
        let range = ByteRange::new(0, (SIMPLE_CHUNK_BYTES + 16 * 1024 * 1024) as u32);

        let block = reader
            .read_committed(&snapshot, range)
            .await
            .expect("read committed tree");

        assert_eq!(
            block.parts(),
            &[
                BlockPart::Zero {
                    range: ByteRange::new(0, SIMPLE_CHUNK_BYTES as u32),
                },
                BlockPart::Zero {
                    range: ByteRange::new(SIMPLE_CHUNK_BYTES, 16 * 1024 * 1024),
                },
            ],
        );
    }
}
