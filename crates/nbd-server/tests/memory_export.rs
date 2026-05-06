use nbd_control_plane::{
    ExportEngineKind, ExportHead, ExportId, ExportName, ExportRecord, ExportState, Timestamp,
};
use nbd_server::{
    ExportJob, ExportReply, ExportRequest, ExportRuntime, MemoryExportEngine, Result,
    SerialExportRuntime, ServerError, MAX_MEMORY_EXPORT_BYTES,
};
use std::sync::Arc;

#[tokio::test]
async fn memory_export_reads_zeroes_then_written_bytes() {
    let meta = export_record("disk-a", 4096);
    let export = Arc::new(MemoryExportEngine::new(&meta).expect("memory export"));

    assert_eq!(export.name().as_str(), "disk-a");
    assert_eq!(export.size_bytes(), 4096);
    assert_eq!(export.block_size(), 4096);

    let runtime = SerialExportRuntime::new(meta, export);

    assert_eq!(
        submit(&runtime, ExportRequest::Read { offset: 4, len: 5 })
            .await
            .expect("zero read"),
        ExportReply::Read { data: vec![0; 5] },
    );

    submit(
        &runtime,
        ExportRequest::Write {
            offset: 4,
            data: b"hello".to_vec(),
        },
    )
    .await
    .expect("write");

    let mut expected = vec![0; 12];
    expected[4..9].copy_from_slice(b"hello");
    assert_eq!(
        submit(&runtime, ExportRequest::Read { offset: 0, len: 12 })
            .await
            .expect("readback"),
        ExportReply::Read { data: expected },
    );

    assert_eq!(
        submit(&runtime, ExportRequest::Flush).await.expect("flush"),
        ExportReply::Done,
    );
}

#[tokio::test]
async fn memory_export_rejects_out_of_bounds_ranges() {
    let runtime = memory_runtime("disk-a", 8);

    assert!(matches!(
        submit(&runtime, ExportRequest::Read { offset: 7, len: 2 }).await,
        Err(ServerError::OutOfBounds {
            operation: "read",
            offset: 7,
            length: 2,
            size_bytes: 8,
        }),
    ));
    assert!(matches!(
        submit(
            &runtime,
            ExportRequest::Write {
                offset: 8,
                data: b"x".to_vec(),
            },
        )
        .await,
        Err(ServerError::OutOfBounds {
            operation: "write",
            offset: 8,
            length: 1,
            size_bytes: 8,
        }),
    ));
}

#[test]
fn memory_export_rejects_oversized_catalog_exports() {
    let meta = export_record("huge", MAX_MEMORY_EXPORT_BYTES + 1);

    assert!(matches!(
        MemoryExportEngine::new(&meta),
        Err(ServerError::ExportTooLarge {
            size_bytes,
            max_size_bytes,
            ..
        }) if size_bytes == MAX_MEMORY_EXPORT_BYTES + 1
            && max_size_bytes == MAX_MEMORY_EXPORT_BYTES,
    ));
}

async fn submit(runtime: &SerialExportRuntime, request: ExportRequest) -> Result<ExportReply> {
    let queue_slot = runtime.reserve().await?;
    let (job, receiver) = ExportJob::oneshot(request, queue_slot);
    runtime.submit(job).await?;
    let completed = receiver.await.expect("runtime completion");
    let (result, _queue_slot) = completed.into_parts();
    result
}

fn memory_runtime(name: &str, size_bytes: u64) -> SerialExportRuntime {
    let meta = export_record(name, size_bytes);
    let engine = Arc::new(MemoryExportEngine::new(&meta).expect("memory export"));
    SerialExportRuntime::new(meta, engine)
}

fn export_record(name: &str, size_bytes: u64) -> ExportRecord {
    ExportRecord::new(
        ExportId::new(format!("export-{name}")).expect("export id"),
        ExportName::new(name).expect("export name"),
        4096,
        ExportEngineKind::Memory,
        ExportState::Active,
        ExportHead::memory_empty(size_bytes).expect("memory head"),
        Timestamp::new("created").expect("created timestamp"),
        Timestamp::new("updated").expect("updated timestamp"),
        None,
    )
    .expect("export meta")
}
