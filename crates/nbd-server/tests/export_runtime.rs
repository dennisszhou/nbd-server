use nbd_control_plane::{
    CommittedRoot, ExportEngineKind, ExportGeneration, ExportId, ExportMeta, ExportName,
    ExportState, Timestamp, WalSeq,
};
use nbd_server::{
    ExportJob, ExportReply, ExportRequest, ExportRuntime, MemoryExportEngine, SerialExportRuntime,
};
use std::sync::Arc;

#[tokio::test]
async fn serial_runtime_executes_accepted_jobs() {
    let meta = export_meta("disk-a", 4096);
    let engine = Arc::new(MemoryExportEngine::new(&meta).unwrap());
    let runtime = SerialExportRuntime::new(meta.clone(), engine);

    assert_eq!(runtime.export_meta(), meta);

    assert_eq!(
        submit(&runtime, ExportRequest::Read { offset: 0, len: 4 },).await,
        ExportReply::Read { data: vec![0; 4] },
    );

    assert_eq!(
        submit(
            &runtime,
            ExportRequest::Write {
                offset: 1,
                data: b"abc".to_vec(),
            },
        )
        .await,
        ExportReply::Done,
    );
    assert_eq!(
        submit(&runtime, ExportRequest::Read { offset: 0, len: 5 },).await,
        ExportReply::Read {
            data: vec![0, b'a', b'b', b'c', 0],
        },
    );
    assert_eq!(
        submit(&runtime, ExportRequest::Flush).await,
        ExportReply::Done
    );
}

#[tokio::test]
async fn serial_runtime_queue_slot_reservation_releases_on_drop() {
    let meta = export_meta("disk-a", 4096);
    let engine = Arc::new(MemoryExportEngine::new(&meta).unwrap());
    let runtime = SerialExportRuntime::with_capacity(meta, engine, 1);

    let first_slot = runtime.reserve().await.expect("reserve first slot");
    let waiter_runtime = runtime.clone();
    let waiter =
        tokio::spawn(async move { waiter_runtime.reserve().await.expect("reserve second slot") });

    tokio::task::yield_now().await;
    assert!(
        !waiter.is_finished(),
        "second reservation should wait while queue depth is exhausted",
    );

    drop(first_slot);
    let second_slot = waiter.await.expect("reservation task");
    drop(second_slot);
}

async fn submit(runtime: &SerialExportRuntime, request: ExportRequest) -> ExportReply {
    let (job, receiver) = ExportJob::oneshot(request);
    runtime.submit(job).await.expect("submit job");
    receiver
        .await
        .expect("runtime reply")
        .expect("export reply")
}

fn export_meta(name: &str, size_bytes: u64) -> ExportMeta {
    ExportMeta::new(
        ExportId::new(format!("export-{name}")).expect("export id"),
        ExportName::new(name).expect("export name"),
        size_bytes,
        4096,
        ExportEngineKind::Memory,
        ExportState::Active,
        CommittedRoot::new(None, WalSeq::zero(), ExportGeneration::zero()),
        Timestamp::new("created").expect("created timestamp"),
        Timestamp::new("updated").expect("updated timestamp"),
        None,
    )
    .expect("export meta")
}
