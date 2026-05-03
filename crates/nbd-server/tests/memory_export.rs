use nbd_control_plane::{
    CommittedRoot, ExportGeneration, ExportId, ExportMeta, ExportName, ExportState, Timestamp,
    WalSeq,
};
use nbd_server::{MemoryExport, ServerError, MAX_MEMORY_EXPORT_BYTES};

#[tokio::test]
async fn memory_export_reads_zeroes_then_written_bytes() {
    let export = MemoryExport::new(&export_meta("disk-a", 4096)).expect("memory export");

    assert_eq!(export.name().as_str(), "disk-a");
    assert_eq!(export.size_bytes(), 4096);
    assert_eq!(export.block_size(), 4096);
    assert_eq!(export.read(4, 5).await.expect("zero read"), vec![0; 5]);

    export.write(4, b"hello").await.expect("write");
    assert_eq!(export.read(0, 12).await.expect("readback"), {
        let mut expected = vec![0; 12];
        expected[4..9].copy_from_slice(b"hello");
        expected
    });

    export.flush().await.expect("flush");
}

#[tokio::test]
async fn memory_export_rejects_out_of_bounds_ranges() {
    let export = MemoryExport::new(&export_meta("disk-a", 8)).expect("memory export");

    assert!(matches!(
        export.read(7, 2).await,
        Err(ServerError::OutOfBounds {
            operation: "read",
            offset: 7,
            length: 2,
            size_bytes: 8,
        }),
    ));
    assert!(matches!(
        export.write(8, b"x").await,
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
    let meta = export_meta("huge", MAX_MEMORY_EXPORT_BYTES + 1);

    assert!(matches!(
        MemoryExport::new(&meta),
        Err(ServerError::ExportTooLarge {
            size_bytes,
            max_size_bytes,
            ..
        }) if size_bytes == MAX_MEMORY_EXPORT_BYTES + 1
            && max_size_bytes == MAX_MEMORY_EXPORT_BYTES,
    ));
}

fn export_meta(name: &str, size_bytes: u64) -> ExportMeta {
    ExportMeta::new(
        ExportId::new(format!("export-{name}")).expect("export id"),
        ExportName::new(name).expect("export name"),
        size_bytes,
        4096,
        ExportState::Active,
        CommittedRoot::new(None, WalSeq::zero(), ExportGeneration::zero()),
        Timestamp::new("created").expect("created timestamp"),
        Timestamp::new("updated").expect("updated timestamp"),
        None,
    )
    .expect("export meta")
}
