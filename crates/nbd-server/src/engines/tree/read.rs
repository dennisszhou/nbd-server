use crate::error::{Result, ServerError};
use crate::range::ByteRange;
use bytes::Bytes;
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Block {
    range: ByteRange,
    parts: Vec<BlockPart>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum BlockPart {
    Data { range: ByteRange, bytes: Bytes },
    Zero { range: ByteRange },
}

#[async_trait::async_trait]
pub(crate) trait TreeReader<R>: fmt::Debug + Send + Sync {
    async fn read_committed(&self, root: &R, range: ByteRange) -> Result<Block>;
}

impl Block {
    pub(crate) fn new(range: ByteRange, parts: Vec<BlockPart>) -> Result<Self> {
        validate_block_parts(range, &parts)?;
        Ok(Self { range, parts })
    }

    pub(crate) fn range(&self) -> ByteRange {
        self.range
    }

    pub(crate) fn parts(&self) -> &[BlockPart] {
        &self.parts
    }

    pub(crate) fn materialize(&self) -> Result<Vec<u8>> {
        let len = usize::try_from(self.range.len())
            .map_err(|_| invalid_block("read range length does not fit usize"))?;
        let mut data = Vec::with_capacity(len);
        for part in self.parts() {
            match part {
                BlockPart::Data { bytes, .. } => data.extend_from_slice(bytes),
                BlockPart::Zero { range } => {
                    let part_len = usize::try_from(range.len())
                        .map_err(|_| invalid_block("zero range length does not fit usize"))?;
                    data.resize(data.len() + part_len, 0);
                }
            }
        }
        Ok(data)
    }
}

impl BlockPart {
    pub(crate) fn range(&self) -> ByteRange {
        match self {
            Self::Data { range, .. } | Self::Zero { range } => *range,
        }
    }
}

fn validate_block_parts(range: ByteRange, parts: &[BlockPart]) -> Result<()> {
    let mut expected_start = range.start();
    let read_end = checked_range_end(range)?;

    for part in parts {
        let part_range = part.range();
        if part_range.start() != expected_start {
            return Err(invalid_block(format!(
                "part starts at {}, expected {}",
                part_range.start(),
                expected_start
            )));
        }
        if let BlockPart::Data { bytes, range } = part {
            if bytes.len() as u64 != range.len() {
                return Err(invalid_block(format!(
                    "data part has {} bytes for {} byte range",
                    bytes.len(),
                    range.len()
                )));
            }
        }
        expected_start = checked_range_end(part_range)?;
        if expected_start > read_end {
            return Err(invalid_block("parts exceed read range"));
        }
    }

    if expected_start != read_end {
        return Err(invalid_block(format!(
            "parts end at {}, expected {}",
            expected_start, read_end
        )));
    }

    Ok(())
}

fn checked_range_end(range: ByteRange) -> Result<u64> {
    range
        .start()
        .checked_add(range.len())
        .ok_or_else(|| invalid_block("range end overflowed"))
}

fn invalid_block(message: impl Into<String>) -> ServerError {
    ServerError::Io {
        context: "block read",
        message: message.into(),
        source: None,
    }
}
