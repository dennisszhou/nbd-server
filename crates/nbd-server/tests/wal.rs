use nbd_control_plane::{ExportId, WalSeq};
use nbd_server::{
    ByteRange, OpenWal, ServerError, WalBounds, WalDomain, WalPruneResult, WalRecord, WalRequest,
};

#[test]
fn wal_domain_keeps_export_identity() {
    let export_id = ExportId::new("export-a").expect("export id");
    let domain = WalDomain::for_export_id(export_id.clone());
    let request = OpenWal::new(domain.clone());

    assert_eq!(domain.export_id(), &export_id);
    assert_eq!(request.domain(), &domain);
}

#[test]
fn wal_request_requires_non_empty_payload() {
    assert!(matches!(
        WalRequest::new(ByteRange::new(0, 0), Vec::new()),
        Err(ServerError::Wal {
            context: "create WAL request",
            ..
        }),
    ));
}

#[test]
fn wal_request_requires_payload_to_match_range() {
    assert!(matches!(
        WalRequest::new(ByteRange::new(4, 8), b"short".to_vec()),
        Err(ServerError::Wal {
            context: "create WAL request",
            ..
        }),
    ));
}

#[test]
fn wal_request_preserves_range_and_payload() {
    let range = ByteRange::new(4, 5);
    let request = WalRequest::new(range, b"hello".to_vec()).expect("wal request");

    assert_eq!(request.range(), range);
    assert_eq!(request.data(), b"hello");
    assert_eq!(request.into_parts(), (range, b"hello".to_vec()));
}

#[test]
fn wal_record_requires_nonzero_sequence() {
    assert!(matches!(
        WalRecord::new(WalSeq::zero(), ByteRange::new(0, 4), b"data".to_vec()),
        Err(ServerError::Wal {
            context: "create WAL record",
            ..
        }),
    ));
}

#[test]
fn wal_record_can_be_built_from_request() {
    let request = WalRequest::new(ByteRange::new(4, 5), b"hello".to_vec()).expect("wal request");
    let record = WalRecord::from_request(WalSeq::new(7), request);

    assert_eq!(record.seq(), WalSeq::new(7));
    assert_eq!(record.range(), ByteRange::new(4, 5));
    assert_eq!(record.data(), b"hello");
    assert_eq!(
        record.into_parts(),
        (WalSeq::new(7), ByteRange::new(4, 5), b"hello".to_vec()),
    );
}

#[test]
fn wal_bounds_reject_pruned_sequence_after_durable() {
    assert!(matches!(
        WalBounds::new(WalSeq::new(2), WalSeq::new(1)),
        Err(ServerError::Wal {
            context: "create WAL bounds",
            ..
        }),
    ));
}

#[test]
fn empty_wal_bounds_start_at_zero() {
    assert_eq!(
        WalBounds::empty(),
        WalBounds {
            pruned_through: WalSeq::zero(),
            last_durable: WalSeq::zero(),
        },
    );
}

#[test]
fn wal_prune_result_rejects_overstated_cleanup() {
    assert!(matches!(
        WalPruneResult::new(WalSeq::new(1), WalSeq::new(2), 0),
        Err(ServerError::Wal {
            context: "create WAL prune result",
            ..
        }),
    ));
}
