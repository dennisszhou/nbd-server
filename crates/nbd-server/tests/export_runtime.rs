use nbd_control_plane::{
    ExportEngineKind, ExportHead, ExportId, ExportName, ExportRecord, ExportState, Timestamp,
};
use nbd_server::{
    AdmittedExportRequest, ConcurrentExportRuntime, ExportAdmissionPolicyHandle, ExportEngine,
    ExportJob, ExportReply, ExportRequest, ExportResult, ExportRuntime, MemoryAdmissionPolicy,
    MemoryExportEngine, SerialExportRuntime, ServerError,
};
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use tokio::sync::oneshot;

#[tokio::test]
async fn serial_runtime_executes_accepted_jobs() {
    let meta = export_record("disk-a", 4096);
    let engine = Arc::new(MemoryExportEngine::new(&meta).unwrap());
    let runtime = SerialExportRuntime::new(meta.clone(), engine);

    assert_eq!(runtime.export_record(), meta);

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
    let meta = export_record("disk-a", 4096);
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
    let meta = export_record("disk-a", 4096);
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

#[tokio::test]
async fn concurrent_runtime_queue_slot_reservation_releases_on_drop() {
    let meta = export_record("disk-a", 4096);
    let (engine, _releases) = GatedEngine::new(0);
    let runtime = ConcurrentExportRuntime::with_capacity(meta, engine, 1);

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
async fn concurrent_runtime_runs_compatible_jobs_concurrently() {
    let meta = export_record("disk-a", 4096);
    let (engine, releases) = GatedEngine::new(2);
    let runtime = ConcurrentExportRuntime::with_capacity(meta, engine.clone(), 2);

    let first_slot = runtime.reserve().await.expect("reserve first slot");
    let second_slot = runtime.reserve().await.expect("reserve second slot");
    let (first_job, first_reply) =
        ExportJob::oneshot(ExportRequest::Read { offset: 0, len: 4 }, first_slot);
    let (second_job, second_reply) =
        ExportJob::oneshot(ExportRequest::Read { offset: 0, len: 4 }, second_slot);

    runtime.submit(first_job).await.expect("submit first read");
    runtime
        .submit(second_job)
        .await
        .expect("submit second read");

    wait_for_started(&engine, 2).await;

    release_all(releases);
    assert_done(first_reply).await;
    assert_done(second_reply).await;
}

#[tokio::test]
async fn concurrent_runtime_serializes_conflicting_jobs_by_admission() {
    let meta = export_record("disk-a", 4096);
    let (engine, mut releases) = GatedEngine::new(2);
    let runtime = ConcurrentExportRuntime::with_capacity(meta, engine.clone(), 2);

    let read_slot = runtime.reserve().await.expect("reserve read slot");
    let write_slot = runtime.reserve().await.expect("reserve write slot");
    let (read_job, read_reply) =
        ExportJob::oneshot(ExportRequest::Read { offset: 0, len: 4 }, read_slot);
    let (write_job, write_reply) = ExportJob::oneshot(
        ExportRequest::Write {
            offset: 0,
            data: b"abcd".to_vec(),
        },
        write_slot,
    );

    runtime.submit(read_job).await.expect("submit read");
    runtime.submit(write_job).await.expect("submit write");
    wait_for_started(&engine, 1).await;
    assert_started_count_stays(&engine, 1).await;

    releases.remove(0).send(()).expect("release read");
    wait_for_started(&engine, 2).await;
    releases.remove(0).send(()).expect("release write");
    assert_done(read_reply).await;
    assert_done(write_reply).await;
}

#[tokio::test]
async fn concurrent_runtime_orders_flush_as_barrier() {
    let meta = export_record("disk-a", 4096);
    let (engine, mut releases) = GatedEngine::new(2);
    let runtime = ConcurrentExportRuntime::with_capacity(meta, engine.clone(), 2);

    let write_slot = runtime.reserve().await.expect("reserve write slot");
    let flush_slot = runtime.reserve().await.expect("reserve flush slot");
    let (write_job, write_reply) = ExportJob::oneshot(
        ExportRequest::Write {
            offset: 0,
            data: b"abcd".to_vec(),
        },
        write_slot,
    );
    let (flush_job, flush_reply) = ExportJob::oneshot(ExportRequest::Flush, flush_slot);

    runtime.submit(write_job).await.expect("submit write");
    runtime.submit(flush_job).await.expect("submit flush");
    wait_for_started(&engine, 1).await;
    assert_started_count_stays(&engine, 1).await;

    releases.remove(0).send(()).expect("release write");
    wait_for_started(&engine, 2).await;
    releases.remove(0).send(()).expect("release flush");
    assert_done(write_reply).await;
    assert_done(flush_reply).await;
}

#[tokio::test]
async fn concurrent_runtime_close_waits_for_accepted_jobs() {
    let meta = export_record("disk-a", 4096);
    let (engine, mut releases) = GatedEngine::new(1);
    let runtime = ConcurrentExportRuntime::with_capacity(meta, engine.clone(), 2);

    let queue_slot = runtime.reserve().await.expect("reserve queue slot");
    let (job, reply) = ExportJob::oneshot(ExportRequest::Flush, queue_slot);
    runtime.submit(job).await.expect("submit flush");
    wait_for_started(&engine, 1).await;

    let close_runtime = runtime.clone();
    let close_task = tokio::spawn(async move {
        close_runtime.close().await.expect("close runtime");
    });
    tokio::task::yield_now().await;
    assert!(
        !close_task.is_finished(),
        "close should wait for accepted concurrent jobs",
    );
    assert!(matches!(
        runtime.reserve().await,
        Err(ServerError::RuntimeClosed { resource }) if resource == "concurrent export runtime",
    ));

    releases.remove(0).send(()).expect("release flush");
    assert_done(reply).await;
    close_task.await.expect("close task");
}

#[tokio::test]
async fn concurrent_runtime_completes_post_acceptance_admission_errors() {
    let meta = export_record("disk-a", 8);
    let (engine, _releases) = GatedEngine::new_with_extent(0, 8);
    let runtime = ConcurrentExportRuntime::with_capacity(meta, engine, 1);

    let queue_slot = runtime.reserve().await.expect("reserve queue slot");
    let (job, reply) = ExportJob::oneshot(ExportRequest::Read { offset: 7, len: 2 }, queue_slot);

    runtime
        .submit(job)
        .await
        .expect("submit out-of-bounds read");
    let completed = reply.await.expect("runtime completion");
    let (result, _queue_slot) = completed.into_parts();
    assert!(matches!(
        result,
        Err(ServerError::OutOfBounds {
            operation: "read",
            offset: 7,
            length: 2,
            size_bytes: 8,
        }),
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

async fn assert_done(receiver: oneshot::Receiver<nbd_server::CompletedExport>) {
    let completed = receiver.await.expect("runtime completion");
    let (result, _queue_slot) = completed.into_parts();
    assert_eq!(result.expect("export reply"), ExportReply::Done);
}

async fn wait_for_started(engine: &GatedEngine, expected: usize) {
    for _ in 0..32 {
        let started = *engine.started.lock().expect("started lock");
        if started >= expected {
            return;
        }
        tokio::task::yield_now().await;
    }

    panic!("engine did not start {expected} request(s)");
}

async fn assert_started_count_stays(engine: &GatedEngine, expected: usize) {
    for _ in 0..8 {
        tokio::task::yield_now().await;
        assert_eq!(*engine.started.lock().expect("started lock"), expected);
    }
}

fn release_all(releases: Vec<oneshot::Sender<()>>) {
    for release in releases {
        release.send(()).expect("release request");
    }
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
    fn admission_policy(&self) -> ExportAdmissionPolicyHandle {
        Arc::new(MemoryAdmissionPolicy::new(4096))
    }

    async fn execute_admitted(&self, _request: AdmittedExportRequest) -> ExportResult {
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

struct GatedEngine {
    extent_bytes: u64,
    started: Mutex<usize>,
    releases: Mutex<VecDeque<oneshot::Receiver<()>>>,
}

impl GatedEngine {
    fn new(gates: usize) -> (Arc<Self>, Vec<oneshot::Sender<()>>) {
        Self::new_with_extent(gates, 4096)
    }

    fn new_with_extent(gates: usize, extent_bytes: u64) -> (Arc<Self>, Vec<oneshot::Sender<()>>) {
        let mut releases = VecDeque::new();
        let mut senders = Vec::new();
        for _ in 0..gates {
            let (sender, receiver) = oneshot::channel();
            senders.push(sender);
            releases.push_back(receiver);
        }

        (
            Arc::new(Self {
                extent_bytes,
                started: Mutex::new(0),
                releases: Mutex::new(releases),
            }),
            senders,
        )
    }
}

#[async_trait::async_trait]
impl ExportEngine for GatedEngine {
    fn admission_policy(&self) -> ExportAdmissionPolicyHandle {
        Arc::new(MemoryAdmissionPolicy::new(self.extent_bytes))
    }

    async fn execute_admitted(&self, _request: AdmittedExportRequest) -> ExportResult {
        *self.started.lock().expect("started lock") += 1;
        let release = self.releases.lock().expect("releases lock").pop_front();
        if let Some(release) = release {
            let _ = release.await;
        }
        Ok(ExportReply::Done)
    }
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
