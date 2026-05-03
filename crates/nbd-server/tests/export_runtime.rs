use nbd_control_plane::{
    CommittedRoot, ExportEngineKind, ExportGeneration, ExportId, ExportMeta, ExportName,
    ExportState, Timestamp, WalSeq,
};
use nbd_server::{
    ExportEngine, ExportJob, ExportReply, ExportRequest, ExportResult, ExportRuntime,
    MemoryExportEngine, SerialExportRuntime, ServerError,
};
use std::sync::{Arc, Mutex};
use tokio::sync::oneshot;

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

#[tokio::test]
async fn serial_runtime_close_rejects_new_work_and_waits_for_active_job() {
    let meta = export_meta("disk-a", 4096);
    let (entered_tx, entered_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let engine = Arc::new(BlockingEngine::new(entered_tx, release_rx));
    let runtime = SerialExportRuntime::with_capacity(meta, engine, 2);

    let first_slot = runtime.reserve().await.expect("reserve first slot");
    let second_slot = runtime.reserve().await.expect("reserve second slot");
    let (job, receiver) = ExportJob::oneshot(ExportRequest::Flush, first_slot);
    runtime.submit(job).await.expect("submit active job");
    entered_rx.await.expect("engine starts active job");

    let close_runtime = runtime.clone();
    let close_task = tokio::spawn(async move {
        close_runtime.close().await.expect("close runtime");
    });
    tokio::task::yield_now().await;
    assert!(
        !close_task.is_finished(),
        "close should wait for accepted jobs to hand off completion",
    );

    assert!(matches!(
        runtime.reserve().await,
        Err(ServerError::RuntimeClosed { resource }) if resource == "serial export runtime",
    ));

    let (rejected_job, _rejected_receiver) = ExportJob::oneshot(ExportRequest::Flush, second_slot);
    assert!(matches!(
        runtime.submit(rejected_job).await,
        Err(ServerError::RuntimeClosed { resource }) if resource == "serial export runtime",
    ));

    release_tx.send(()).expect("release active job");
    close_task.await.expect("close task");
    let completed = receiver.await.expect("runtime completion");
    let (result, _queue_slot) = completed.into_parts();
    assert_eq!(result.expect("export reply"), ExportReply::Done);

    assert!(matches!(
        runtime.reserve().await,
        Err(ServerError::RuntimeClosed { resource }) if resource == "serial export runtime",
    ));
}

async fn submit(runtime: &SerialExportRuntime, request: ExportRequest) -> ExportReply {
    let queue_slot = runtime.reserve().await.expect("reserve queue slot");
    let (job, receiver) = ExportJob::oneshot(request, queue_slot);
    runtime.submit(job).await.expect("submit job");
    let completed = receiver.await.expect("runtime completion");
    let (result, _queue_slot) = completed.into_parts();
    result.expect("export reply")
}

struct BlockingEngine {
    entered: Mutex<Option<oneshot::Sender<()>>>,
    release: Mutex<Option<oneshot::Receiver<()>>>,
}

impl BlockingEngine {
    fn new(entered: oneshot::Sender<()>, release: oneshot::Receiver<()>) -> Self {
        Self {
            entered: Mutex::new(Some(entered)),
            release: Mutex::new(Some(release)),
        }
    }
}

#[async_trait::async_trait]
impl ExportEngine for BlockingEngine {
    async fn execute(&self, _request: ExportRequest) -> ExportResult {
        if let Some(entered) = self.entered.lock().expect("entered lock").take() {
            let _ = entered.send(());
        }
        let release = self.release.lock().expect("release lock").take();
        if let Some(release) = release {
            let _ = release.await;
        }
        Ok(ExportReply::Done)
    }
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
