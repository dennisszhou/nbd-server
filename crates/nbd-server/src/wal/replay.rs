use super::WalRecord;
use crate::error::{Result, ServerError};
use nbd_control_plane::WalSeq;
use std::collections::VecDeque;

#[derive(Debug, Clone)]
pub struct WalReplay {
    records: VecDeque<WalRecord>,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ByteRange;

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
