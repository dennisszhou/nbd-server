use crate::{ByteRange, Result, ServerError};
use nbd_control_plane::{ExportId, WalSeq};
use std::collections::VecDeque;
use std::sync::Arc;

pub type ExportWalHandle = Arc<dyn ExportWal>;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct WalDomain {
    export_id: ExportId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpenWal {
    domain: WalDomain,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalRequest {
    range: ByteRange,
    data: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalRecord {
    seq: WalSeq,
    range: ByteRange,
    data: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WalBounds {
    pub pruned_through: WalSeq,
    pub last_durable: WalSeq,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalPruneResult {
    pub requested_through: WalSeq,
    pub pruned_through: WalSeq,
    pub removed_segments: u64,
}

#[derive(Debug, Clone)]
pub struct WalReplay {
    records: VecDeque<WalRecord>,
}

impl WalDomain {
    pub fn for_export_id(export_id: ExportId) -> Self {
        Self { export_id }
    }

    pub fn export_id(&self) -> &ExportId {
        &self.export_id
    }
}

impl OpenWal {
    pub fn new(domain: WalDomain) -> Self {
        Self { domain }
    }

    pub fn domain(&self) -> &WalDomain {
        &self.domain
    }
}

impl WalRequest {
    pub fn new(range: ByteRange, data: Vec<u8>) -> Result<Self> {
        if data.is_empty() {
            return Err(ServerError::wal(
                "create WAL request",
                "write payload must not be empty",
            ));
        }
        if data.len() as u64 != range.len() {
            return Err(ServerError::wal(
                "create WAL request",
                format!(
                    "payload length {} does not match range length {}",
                    data.len(),
                    range.len()
                ),
            ));
        }

        Ok(Self { range, data })
    }

    pub fn range(&self) -> ByteRange {
        self.range
    }

    pub fn data(&self) -> &[u8] {
        &self.data
    }

    pub fn into_parts(self) -> (ByteRange, Vec<u8>) {
        (self.range, self.data)
    }
}

impl WalRecord {
    pub fn new(seq: WalSeq, range: ByteRange, data: Vec<u8>) -> Result<Self> {
        if seq == WalSeq::zero() {
            return Err(ServerError::wal(
                "create WAL record",
                "record sequence must be nonzero",
            ));
        }
        let request = WalRequest::new(range, data)?;
        Ok(Self::from_request(seq, request))
    }

    pub fn from_request(seq: WalSeq, request: WalRequest) -> Self {
        let (range, data) = request.into_parts();
        Self { seq, range, data }
    }

    pub fn seq(&self) -> WalSeq {
        self.seq
    }

    pub fn range(&self) -> ByteRange {
        self.range
    }

    pub fn data(&self) -> &[u8] {
        &self.data
    }

    pub fn into_parts(self) -> (WalSeq, ByteRange, Vec<u8>) {
        (self.seq, self.range, self.data)
    }
}

impl WalBounds {
    pub fn new(pruned_through: WalSeq, last_durable: WalSeq) -> Result<Self> {
        if pruned_through > last_durable {
            return Err(ServerError::wal(
                "create WAL bounds",
                format!(
                    "pruned_through {} is greater than last_durable {}",
                    pruned_through, last_durable
                ),
            ));
        }

        Ok(Self {
            pruned_through,
            last_durable,
        })
    }

    pub fn empty() -> Self {
        Self {
            pruned_through: WalSeq::zero(),
            last_durable: WalSeq::zero(),
        }
    }
}

impl WalPruneResult {
    pub fn new(
        requested_through: WalSeq,
        pruned_through: WalSeq,
        removed_segments: u64,
    ) -> Result<Self> {
        if pruned_through > requested_through {
            return Err(ServerError::wal(
                "create WAL prune result",
                format!(
                    "pruned_through {} is greater than requested_through {}",
                    pruned_through, requested_through
                ),
            ));
        }

        Ok(Self {
            requested_through,
            pruned_through,
            removed_segments,
        })
    }
}

impl WalReplay {
    pub fn empty() -> Self {
        Self::from_records(Vec::new()).expect("empty WAL replay is ordered")
    }

    pub(crate) fn from_records(records: Vec<WalRecord>) -> Result<Self> {
        let mut previous = WalSeq::zero();
        for record in &records {
            if record.seq() <= previous {
                return Err(ServerError::wal(
                    "create WAL replay",
                    "records must be strictly ordered by sequence",
                ));
            }
            previous = record.seq();
        }

        Ok(Self {
            records: VecDeque::from(records),
        })
    }

    pub async fn next_record(&mut self) -> Result<Option<WalRecord>> {
        Ok(self.records.pop_front())
    }
}

#[async_trait::async_trait]
pub trait WalProvider: Send + Sync {
    async fn open_export(&self, request: OpenWal) -> Result<ExportWalHandle>;
}

#[async_trait::async_trait]
pub trait ExportWal: Send + Sync {
    async fn append(&self, request: WalRequest) -> Result<WalRecord>;

    async fn bounds(&self) -> Result<WalBounds>;

    async fn replay_after(&self, after: WalSeq) -> Result<WalReplay>;

    async fn replay_range(&self, after: WalSeq, through: WalSeq) -> Result<WalReplay>;

    async fn prune_through(&self, seq: WalSeq) -> Result<WalPruneResult>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn replay_yields_records_in_order() {
        let first = WalRecord::new(WalSeq::new(1), ByteRange::new(0, 3), b"one".to_vec())
            .expect("first record");
        let second = WalRecord::new(WalSeq::new(2), ByteRange::new(3, 3), b"two".to_vec())
            .expect("second record");
        let mut replay =
            WalReplay::from_records(vec![first.clone(), second.clone()]).expect("ordered replay");

        assert_eq!(replay.next_record().await.expect("next"), Some(first));
        assert_eq!(replay.next_record().await.expect("next"), Some(second));
        assert_eq!(replay.next_record().await.expect("next"), None);
    }

    #[test]
    fn replay_rejects_non_increasing_records() {
        let first = WalRecord::new(WalSeq::new(2), ByteRange::new(0, 3), b"one".to_vec())
            .expect("first record");
        let second = WalRecord::new(WalSeq::new(2), ByteRange::new(3, 3), b"two".to_vec())
            .expect("second record");

        assert!(matches!(
            WalReplay::from_records(vec![first, second]),
            Err(ServerError::Wal {
                context: "create WAL replay",
                ..
            }),
        ));
    }
}
