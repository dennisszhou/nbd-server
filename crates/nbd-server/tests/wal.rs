use nbd_control_plane::{ExportId, WalSeq};
use nbd_server::{
    ByteRange, LocalWalProvider, OpenWal, ServerError, WalBounds, WalDomain, WalProvider,
    WalPruneResult, WalRecord, WalReplay, WalRequest,
};
use nbd_test_support::TestRuntime;
use std::path::{Path, PathBuf};

const SEGMENT_HEADER_LEN: usize = 24;
const RECORD_HEADER_LEN: usize = 40;

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
fn wal_prune_result_records_requested_and_actual_cleanup() {
    assert_eq!(
        WalPruneResult::new(WalSeq::new(1), WalSeq::new(2), 3),
        WalPruneResult {
            requested_through: WalSeq::new(1),
            pruned_through: WalSeq::new(2),
            removed_segments: 3,
        },
    );
}

#[test]
fn local_wal_rejects_header_sized_segment_target() {
    let runtime = TestRuntime::new().expect("runtime");
    let provider =
        LocalWalProvider::with_segment_target_bytes(runtime.wal_dir(), SEGMENT_HEADER_LEN as u64);

    assert!(matches!(
        provider,
        Err(ServerError::Wal {
            context: "create local WAL provider",
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

#[tokio::test]
async fn local_wal_repairs_final_partial_tail() {
    let runtime = TestRuntime::new().expect("runtime");
    let wal = open_local_wal(&runtime, "export-tail").await;
    let first = append(&wal, 0, b"first").await;
    append(&wal, 4096, b"second").await;
    drop(wal);

    let segment = only_segment_file(runtime.wal_dir()).await;
    let len = tokio::fs::metadata(&segment).await.expect("metadata").len();
    truncate_file(&segment, len - 2).await;

    let repaired = open_local_wal(&runtime, "export-tail").await;

    assert_eq!(
        repaired.bounds().await.expect("bounds"),
        WalBounds {
            pruned_through: WalSeq::zero(),
            last_durable: WalSeq::new(1),
        },
    );
    assert_eq!(
        collect(repaired.replay_after(WalSeq::zero()).await.expect("replay")).await,
        vec![first],
    );
    assert_eq!(append(&repaired, 8192, b"new").await.seq(), WalSeq::new(2));
}

#[tokio::test]
async fn local_wal_repairs_final_checksum_tail() {
    let runtime = TestRuntime::new().expect("runtime");
    let wal = open_local_wal(&runtime, "export-checksum-tail").await;
    let first = append(&wal, 0, b"first").await;
    append(&wal, 4096, b"second").await;
    drop(wal);

    let segment = only_segment_file(runtime.wal_dir()).await;
    corrupt_record_payload(&segment, 1, b"first".len()).await;

    let repaired = open_local_wal(&runtime, "export-checksum-tail").await;

    assert_eq!(
        repaired.bounds().await.expect("bounds").last_durable,
        WalSeq::new(1),
    );
    assert_eq!(
        collect(repaired.replay_after(WalSeq::zero()).await.expect("replay")).await,
        vec![first],
    );
}

#[tokio::test]
async fn local_wal_rejects_interior_checksum_corruption() {
    let runtime = TestRuntime::new().expect("runtime");
    let wal = open_local_wal(&runtime, "export-interior").await;
    append(&wal, 0, b"first").await;
    append(&wal, 4096, b"second").await;
    drop(wal);

    let segment = only_segment_file(runtime.wal_dir()).await;
    corrupt_record_payload(&segment, 0, 0).await;

    let provider = LocalWalProvider::new(runtime.wal_dir());
    assert!(matches!(
        provider
            .open_export(OpenWal::new(WalDomain::for_export_id(
                ExportId::new("export-interior").expect("export id"),
            )))
            .await,
        Err(ServerError::Wal {
            context: "read WAL record",
            ..
        }),
    ));
}

#[tokio::test]
async fn local_wal_prunes_full_segment_prefix() {
    let runtime = TestRuntime::new().expect("runtime");
    let wal = open_local_wal_with_segment_target(&runtime, "export-prune-prefix", 48).await;
    append(&wal, 0, b"one").await;
    append(&wal, 4096, b"two").await;
    let third = append(&wal, 8192, b"three").await;

    let result = wal.prune_through(WalSeq::new(2)).await.expect("prune");

    assert_eq!(result.requested_through, WalSeq::new(2));
    assert_eq!(result.pruned_through, WalSeq::new(2));
    assert_eq!(result.removed_segments, 2);
    assert_eq!(
        wal.bounds().await.expect("bounds"),
        WalBounds {
            pruned_through: WalSeq::new(2),
            last_durable: WalSeq::new(3),
        },
    );
    assert!(matches!(
        wal.replay_after(WalSeq::new(1)).await,
        Err(ServerError::Wal {
            context: "replay WAL",
            ..
        }),
    ));
    assert_eq!(
        collect(wal.replay_after(WalSeq::new(2)).await.expect("replay")).await,
        vec![third],
    );

    let no_op = wal.prune_through(WalSeq::new(1)).await.expect("prune");
    assert_eq!(no_op.requested_through, WalSeq::new(1));
    assert_eq!(no_op.pruned_through, WalSeq::new(2));
    assert_eq!(no_op.removed_segments, 0);
}

#[tokio::test]
async fn local_wal_prunes_active_segment_with_header_only_successor() {
    let runtime = TestRuntime::new().expect("runtime");
    let wal = open_local_wal(&runtime, "export-prune-active").await;
    append(&wal, 0, b"one").await;
    append(&wal, 4096, b"two").await;

    let result = wal.prune_through(WalSeq::new(2)).await.expect("prune");

    assert_eq!(result.pruned_through, WalSeq::new(2));
    assert_eq!(result.removed_segments, 1);
    assert_eq!(
        wal.bounds().await.expect("bounds"),
        WalBounds {
            pruned_through: WalSeq::new(2),
            last_durable: WalSeq::new(2),
        },
    );
    assert!(matches!(
        wal.replay_after(WalSeq::zero()).await,
        Err(ServerError::Wal {
            context: "replay WAL",
            ..
        }),
    ));
    assert_eq!(append(&wal, 8192, b"three").await.seq(), WalSeq::new(3));

    drop(wal);
    let reopened = open_local_wal(&runtime, "export-prune-active").await;
    assert_eq!(
        reopened.bounds().await.expect("bounds"),
        WalBounds {
            pruned_through: WalSeq::new(2),
            last_durable: WalSeq::new(3),
        },
    );
}

#[tokio::test]
async fn local_wal_does_not_partially_prune_straddling_segment() {
    let runtime = TestRuntime::new().expect("runtime");
    let wal = open_local_wal(&runtime, "export-prune-straddle").await;
    let first = append(&wal, 0, b"one").await;
    let second = append(&wal, 4096, b"two").await;

    let result = wal.prune_through(WalSeq::new(1)).await.expect("prune");

    assert_eq!(result.pruned_through, WalSeq::zero());
    assert_eq!(result.removed_segments, 0);
    assert_eq!(
        collect(wal.replay_after(WalSeq::zero()).await.expect("replay")).await,
        vec![first, second],
    );
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

async fn open_local_wal_with_segment_target(
    runtime: &TestRuntime,
    export_id: &str,
    segment_target_bytes: u64,
) -> nbd_server::ExportWalHandle {
    let provider =
        LocalWalProvider::with_segment_target_bytes(runtime.wal_dir(), segment_target_bytes)
            .expect("local wal provider");
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

async fn only_segment_file(root: &Path) -> PathBuf {
    let mut export_dirs = tokio::fs::read_dir(root).await.expect("read wal root");
    let export_dir = export_dirs
        .next_entry()
        .await
        .expect("read export dir")
        .expect("export dir")
        .path();
    assert!(
        export_dirs
            .next_entry()
            .await
            .expect("read export dir")
            .is_none()
    );

    let mut segments = tokio::fs::read_dir(export_dir)
        .await
        .expect("read export wal dir");
    let segment = segments
        .next_entry()
        .await
        .expect("read segment")
        .expect("segment")
        .path();
    assert!(segments.next_entry().await.expect("read segment").is_none());
    segment
}

async fn truncate_file(path: &Path, len: u64) {
    let file = tokio::fs::OpenOptions::new()
        .write(true)
        .open(path)
        .await
        .expect("open for truncate");
    file.set_len(len).await.expect("truncate");
    file.sync_all().await.expect("sync truncated file");
}

async fn corrupt_record_payload(path: &Path, record_index: usize, previous_payload_len: usize) {
    let mut data = tokio::fs::read(path).await.expect("read segment");
    let record_offset =
        SEGMENT_HEADER_LEN + record_index * RECORD_HEADER_LEN + previous_payload_len;
    let payload_offset = record_offset + RECORD_HEADER_LEN;
    data[payload_offset] ^= 0xff;
    tokio::fs::write(path, data)
        .await
        .expect("write corrupted segment");
}
