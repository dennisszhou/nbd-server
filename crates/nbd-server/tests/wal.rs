use nbd_control_plane::{ExportId, WalSeq};
use nbd_server::{
    ByteRange, LocalWalProvider, OpenWal, ServerError, WalBounds, WalDomain, WalProvider,
    WalPruneResult, WalRecord, WalReplay, WalRequest,
};
use nbd_test_support::TestRuntime;

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
fn wal_record_preserves_sequence_range_and_payload() {
    let record = WalRecord::new(WalSeq::new(7), ByteRange::new(4, 5), b"hello".to_vec())
        .expect("wal record");

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

#[tokio::test]
async fn local_wal_append_reports_bounds_and_replays() {
    let runtime = TestRuntime::new().expect("runtime");
    let wal = open_local_wal(&runtime, "export-a").await;
    let first = append(&wal, 0, b"hello").await;
    let second = append(&wal, 4096, b"world").await;

    assert_eq!(first.seq(), WalSeq::new(1));
    assert_eq!(second.seq(), WalSeq::new(2));
    assert_eq!(
        wal.bounds().await.expect("bounds"),
        WalBounds {
            pruned_through: WalSeq::zero(),
            last_durable: WalSeq::new(2),
        },
    );
    assert_eq!(
        collect(wal.replay_after(WalSeq::zero()).await.expect("replay")).await,
        vec![first.clone(), second.clone()],
    );
    assert_eq!(
        collect(
            wal.replay_range(WalSeq::new(1), WalSeq::new(2))
                .await
                .expect("replay range"),
        )
        .await,
        vec![second],
    );
}

#[tokio::test]
async fn local_wal_reopen_recovers_durable_records() {
    let runtime = TestRuntime::new().expect("runtime");
    let wal = open_local_wal(&runtime, "export-reopen").await;
    let first = append(&wal, 8, b"persist").await;
    let second = append(&wal, 16, b"again").await;
    drop(wal);

    let reopened = open_local_wal(&runtime, "export-reopen").await;

    assert_eq!(
        reopened.bounds().await.expect("bounds"),
        WalBounds {
            pruned_through: WalSeq::zero(),
            last_durable: WalSeq::new(2),
        },
    );
    assert_eq!(
        collect(reopened.replay_after(WalSeq::zero()).await.expect("replay"),).await,
        vec![first, second],
    );
}

#[tokio::test]
async fn local_wal_encodes_export_id_as_path_component() {
    let runtime = TestRuntime::new().expect("runtime");
    let wal = open_local_wal(&runtime, "../tenant/export-a").await;
    append(&wal, 0, b"data").await;

    assert!(!runtime.wal_dir().join("..").join("tenant").exists());
    let mut entries = tokio::fs::read_dir(runtime.wal_dir())
        .await
        .expect("read wal dir");
    let entry = entries
        .next_entry()
        .await
        .expect("next entry")
        .expect("encoded export dir");
    runtime.assert_path_inside(entry.path());
    assert!(entries.next_entry().await.expect("next entry").is_none());
}

#[tokio::test]
async fn local_wal_rejects_replay_past_durable_bounds() {
    let runtime = TestRuntime::new().expect("runtime");
    let wal = open_local_wal(&runtime, "export-bounds").await;
    append(&wal, 0, b"data").await;

    assert!(matches!(
        wal.replay_range(WalSeq::zero(), WalSeq::new(2)).await,
        Err(ServerError::Wal {
            context: "replay WAL",
            ..
        }),
    ));
}

async fn open_local_wal(runtime: &TestRuntime, export_id: &str) -> nbd_server::ExportWalHandle {
    let provider = LocalWalProvider::new(runtime.wal_dir());
    provider
        .open_export(OpenWal::new(WalDomain::for_export_id(
            ExportId::new(export_id).expect("export id"),
        )))
        .await
        .expect("open local wal")
}

async fn append(wal: &nbd_server::ExportWalHandle, offset: u64, data: &[u8]) -> WalRecord {
    wal.append(
        WalRequest::new(ByteRange::new(offset, data.len() as u32), data.to_vec())
            .expect("wal request"),
    )
    .await
    .expect("append")
}

async fn collect(mut replay: WalReplay) -> Vec<WalRecord> {
    let mut records = Vec::new();
    while let Some(record) = replay.next_record().await.expect("next record") {
        records.push(record);
    }
    records
}
