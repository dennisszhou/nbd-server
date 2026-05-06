use crate::{
    observability::{self, event, target},
    AdmissionPermit, AdmissionWaiter, AdmittedExportRequest, ExportAdmissionCtl,
    ExportAdmissionPolicyHandle, ExportCompletion, ExportEngineHandle, ExportJob, ExportJobContext,
    ExportRequest, ExportResult, Result, ServerError,
};
use nbd_control_plane::ExportRecord;
use std::fmt;
use std::sync::Arc;
use std::sync::Mutex;
use tokio::sync::{mpsc, Notify, OnceCell, OwnedSemaphorePermit, Semaphore};
use tracing::Instrument;

pub const DEFAULT_EXPORT_QUEUE_CAPACITY: usize = 128;

/// Export-owned request execution boundary.
#[async_trait::async_trait]
pub trait ExportRuntime: Send + Sync {
    fn export_record(&self) -> ExportRecord;

    async fn reserve(&self) -> Result<ExportQueueSlot>;

    async fn submit(&self, job: ExportJob) -> Result<()>;

    async fn close(&self) -> Result<()>;
}

pub type ExportRuntimeHandle = Arc<dyn ExportRuntime>;

/// Runtime-owned queue-depth reservation for one accepted export request.
#[derive(Debug)]
pub struct ExportQueueSlot {
    _queue_depth: OwnedSemaphorePermit,
}

/// Runtime policy that executes accepted export jobs one at a time.
#[derive(Debug, Clone)]
pub struct SerialExportRuntime {
    meta: ExportRecord,
    queue_depth: Arc<Semaphore>,
    lifecycle: Arc<SerialRuntimeLifecycle>,
    engine_close: Arc<RuntimeEngineClose>,
    sender: mpsc::Sender<ExportJob>,
}

/// Runtime policy that executes compatible admitted export jobs concurrently.
#[derive(Clone)]
pub struct ConcurrentExportRuntime {
    meta: ExportRecord,
    engine: ExportEngineHandle,
    admission: ExportAdmissionCtl,
    admission_policy: ExportAdmissionPolicyHandle,
    queue_depth: Arc<Semaphore>,
    lifecycle: Arc<ConcurrentRuntimeLifecycle>,
    engine_close: Arc<RuntimeEngineClose>,
}

#[derive(Debug)]
struct SerialRuntimeLifecycle {
    state: Mutex<SerialRuntimeState>,
    empty: Notify,
}

#[derive(Debug)]
struct SerialRuntimeState {
    closed: bool,
    active_jobs: usize,
}

#[derive(Debug)]
struct ConcurrentRuntimeLifecycle {
    state: Mutex<ConcurrentRuntimeState>,
    empty: Notify,
}

#[derive(Debug)]
struct ConcurrentRuntimeState {
    closed: bool,
    active_jobs: usize,
}

struct RuntimeEngineClose {
    engine: ExportEngineHandle,
    result: OnceCell<Result<()>>,
}

struct ConcurrentActiveJob {
    lifecycle: Arc<ConcurrentRuntimeLifecycle>,
    finished: bool,
}

struct RegisteredExportJob {
    context: ExportJobContext,
    request: ExportRequest,
    completion: ExportCompletion,
    queue_slot: ExportQueueSlot,
    waiter: AdmissionWaiter,
}

struct RejectedExportJob {
    context: ExportJobContext,
    result: ExportResult,
    completion: ExportCompletion,
    queue_slot: ExportQueueSlot,
}

enum PreparedExportJob {
    Registered(RegisteredExportJob),
    Rejected(RejectedExportJob),
}

impl SerialExportRuntime {
    pub fn new(meta: ExportRecord, engine: ExportEngineHandle) -> Self {
        Self::with_capacity(meta, engine, DEFAULT_EXPORT_QUEUE_CAPACITY)
    }

    pub fn with_capacity(meta: ExportRecord, engine: ExportEngineHandle, capacity: usize) -> Self {
        let admission = ExportAdmissionCtl::new(meta.size_bytes());
        let admission_policy = engine.admission_policy();
        let queue_depth = Arc::new(Semaphore::new(capacity));
        let engine_close = Arc::new(RuntimeEngineClose::new(engine.clone()));
        let lifecycle = Arc::new(SerialRuntimeLifecycle {
            state: Mutex::new(SerialRuntimeState {
                closed: false,
                active_jobs: 0,
            }),
            empty: Notify::new(),
        });
        let (sender, mut receiver) = mpsc::channel::<ExportJob>(capacity);
        let worker_lifecycle = lifecycle.clone();
        let worker_meta = meta.clone();
        let worker_engine = engine;

        tokio::spawn(async move {
            while let Some(job) = receiver.recv().await {
                let span = observability::request_span(&worker_meta, "serial", job.context());
                execute_admitted_job(
                    worker_meta.clone(),
                    "serial",
                    worker_engine.clone(),
                    admission_policy.clone(),
                    admission.clone(),
                    job,
                )
                .instrument(span)
                .await;
                worker_lifecycle.finish_job();
            }
        });

        Self {
            meta,
            queue_depth,
            lifecycle,
            engine_close,
            sender,
        }
    }
}

impl ConcurrentExportRuntime {
    pub fn new(meta: ExportRecord, engine: ExportEngineHandle) -> Self {
        Self::with_capacity(meta, engine, DEFAULT_EXPORT_QUEUE_CAPACITY)
    }

    pub fn with_capacity(meta: ExportRecord, engine: ExportEngineHandle, capacity: usize) -> Self {
        let admission = ExportAdmissionCtl::new(meta.size_bytes());
        let admission_policy = engine.admission_policy();
        let engine_close = Arc::new(RuntimeEngineClose::new(engine.clone()));
        Self {
            meta,
            engine,
            admission,
            admission_policy,
            queue_depth: Arc::new(Semaphore::new(capacity)),
            lifecycle: Arc::new(ConcurrentRuntimeLifecycle {
                state: Mutex::new(ConcurrentRuntimeState {
                    closed: false,
                    active_jobs: 0,
                }),
                empty: Notify::new(),
            }),
            engine_close,
        }
    }
}

impl RuntimeEngineClose {
    fn new(engine: ExportEngineHandle) -> Self {
        Self {
            engine,
            result: OnceCell::new(),
        }
    }

    async fn close(&self) -> Result<()> {
        self.result
            .get_or_init(|| async { self.engine.close().await })
            .await
            .clone()
    }
}

impl fmt::Debug for RuntimeEngineClose {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RuntimeEngineClose").finish_non_exhaustive()
    }
}

async fn execute_admitted_job(
    meta: ExportRecord,
    runtime_kind: &'static str,
    engine: ExportEngineHandle,
    admission_policy: ExportAdmissionPolicyHandle,
    admission: ExportAdmissionCtl,
    job: ExportJob,
) {
    let context = job.context().clone();
    trace_runtime_task_started(&meta, runtime_kind, &context);
    match prepare_admitted_job(admission_policy, admission, job) {
        PreparedExportJob::Registered(job) => {
            execute_registered_job(&meta, runtime_kind, engine, job).await
        }
        PreparedExportJob::Rejected(job) => complete_rejected_job(job).await,
    }
    trace_runtime_task_completed(&meta, runtime_kind, &context);
}

fn prepare_admitted_job(
    admission_policy: ExportAdmissionPolicyHandle,
    admission: ExportAdmissionCtl,
    job: ExportJob,
) -> PreparedExportJob {
    let (context, request, completion, queue_slot) = job.into_parts();
    let waiter = match admission_policy
        .operation_for(&request)
        .and_then(|op| admission.register(op))
    {
        Ok(waiter) => waiter,
        Err(error) => {
            trace_request_failed(&context, "admission", &error);
            return PreparedExportJob::Rejected(RejectedExportJob {
                context,
                result: Err(error),
                completion,
                queue_slot,
            });
        }
    };

    trace_admission_registered(&context, &waiter);
    PreparedExportJob::Registered(RegisteredExportJob {
        context,
        request,
        completion,
        queue_slot,
        waiter,
    })
}

async fn execute_registered_job(
    meta: &ExportRecord,
    runtime_kind: &'static str,
    engine: ExportEngineHandle,
    job: RegisteredExportJob,
) {
    let RegisteredExportJob {
        context,
        request,
        completion,
        queue_slot,
        waiter,
    } = job;
    let result = match waiter.wait().await {
        Ok(permit) => {
            trace_admission_granted(&context, &permit);
            trace_engine_execute_started(meta, runtime_kind, &context);
            let result = engine
                .execute_admitted(AdmittedExportRequest::new(request, permit, context.clone()))
                .await;
            trace_engine_execute_finished(meta, runtime_kind, &context, &result);
            result
        }
        Err(error) => {
            trace_request_failed(&context, "admission", &error);
            Err(error)
        }
    };
    completion.complete(result, queue_slot).await;
}

async fn complete_rejected_job(job: RejectedExportJob) {
    job.completion.complete(job.result, job.queue_slot).await;
}

fn trace_runtime_submit(
    meta: &ExportRecord,
    runtime_kind: &'static str,
    context: &ExportJobContext,
) {
    tracing::trace!(
        target: target::RUNTIME,
        event = event::RUNTIME_SUBMIT,
        service = observability::SERVICE_NAME,
        server_instance_id = observability::server_instance_id(),
        pid = observability::pid(),
        export_id = %meta.id(),
        export_name = %meta.name(),
        runtime_kind = runtime_kind,
        connection_id = context.connection_id().raw(),
        request_sequence = context.request_sequence().raw(),
        cookie = context.cookie().raw(),
    );
}

fn trace_runtime_task_started(
    meta: &ExportRecord,
    runtime_kind: &'static str,
    context: &ExportJobContext,
) {
    tracing::trace!(
        target: target::RUNTIME,
        event = event::RUNTIME_TASK_STARTED,
        service = observability::SERVICE_NAME,
        server_instance_id = observability::server_instance_id(),
        pid = observability::pid(),
        export_id = %meta.id(),
        export_name = %meta.name(),
        runtime_kind = runtime_kind,
        connection_id = context.connection_id().raw(),
        request_sequence = context.request_sequence().raw(),
        cookie = context.cookie().raw(),
    );
}

fn trace_runtime_task_completed(
    meta: &ExportRecord,
    runtime_kind: &'static str,
    context: &ExportJobContext,
) {
    tracing::trace!(
        target: target::RUNTIME,
        event = event::RUNTIME_TASK_COMPLETED,
        service = observability::SERVICE_NAME,
        server_instance_id = observability::server_instance_id(),
        pid = observability::pid(),
        export_id = %meta.id(),
        export_name = %meta.name(),
        runtime_kind = runtime_kind,
        connection_id = context.connection_id().raw(),
        request_sequence = context.request_sequence().raw(),
        cookie = context.cookie().raw(),
        duration_ms = observability::duration_ms(context.elapsed()),
    );
}

fn trace_admission_registered(context: &ExportJobContext, waiter: &AdmissionWaiter) {
    let op = waiter.op();
    tracing::trace!(
        target: target::ADMISSION,
        event = event::ADMISSION_REGISTERED,
        service = observability::SERVICE_NAME,
        server_instance_id = observability::server_instance_id(),
        pid = observability::pid(),
        admission_ticket = waiter.ticket().as_u64(),
        admission_op = op.kind(),
        range_start = ?op.range().map(crate::ByteRange::start),
        range_len = ?op.range().map(crate::ByteRange::len),
        connection_id = context.connection_id().raw(),
        request_sequence = context.request_sequence().raw(),
        cookie = context.cookie().raw(),
    );
}

fn trace_admission_granted(context: &ExportJobContext, permit: &AdmissionPermit) {
    let op = permit.op();
    tracing::trace!(
        target: target::ADMISSION,
        event = event::ADMISSION_GRANTED,
        service = observability::SERVICE_NAME,
        server_instance_id = observability::server_instance_id(),
        pid = observability::pid(),
        admission_ticket = permit.ticket().as_u64(),
        admission_op = op.kind(),
        range_start = ?op.range().map(crate::ByteRange::start),
        range_len = ?op.range().map(crate::ByteRange::len),
        connection_id = context.connection_id().raw(),
        request_sequence = context.request_sequence().raw(),
        cookie = context.cookie().raw(),
    );
}

fn trace_engine_execute_started(
    meta: &ExportRecord,
    runtime_kind: &'static str,
    context: &ExportJobContext,
) {
    tracing::trace!(
        target: target::ENGINE,
        event = event::ENGINE_EXECUTE_STARTED,
        service = observability::SERVICE_NAME,
        server_instance_id = observability::server_instance_id(),
        pid = observability::pid(),
        export_id = %meta.id(),
        export_name = %meta.name(),
        engine_kind = %meta.engine_kind(),
        runtime_kind = runtime_kind,
        command = context.command(),
        offset = ?context.offset(),
        length = ?context.length(),
        connection_id = context.connection_id().raw(),
        request_sequence = context.request_sequence().raw(),
        cookie = context.cookie().raw(),
    );
}

fn trace_engine_execute_finished(
    meta: &ExportRecord,
    runtime_kind: &'static str,
    context: &ExportJobContext,
    result: &ExportResult,
) {
    match result {
        Ok(_) => {
            let event_name = if context.command() == "flush" {
                event::ENGINE_FLUSH_COMPLETED
            } else {
                event::ENGINE_EXECUTE_COMPLETED
            };
            tracing::trace!(
                target: target::ENGINE,
                event = event_name,
                service = observability::SERVICE_NAME,
                server_instance_id = observability::server_instance_id(),
                pid = observability::pid(),
                export_id = %meta.id(),
                export_name = %meta.name(),
                engine_kind = %meta.engine_kind(),
                runtime_kind = runtime_kind,
                command = context.command(),
                offset = ?context.offset(),
                length = ?context.length(),
                connection_id = context.connection_id().raw(),
                request_sequence = context.request_sequence().raw(),
                cookie = context.cookie().raw(),
                status = "ok",
                duration_ms = observability::duration_ms(context.elapsed()),
            );
        }
        Err(error) => {
            trace_engine_execute_failed(meta, runtime_kind, context, error);
            trace_request_failed(context, "engine", error);
        }
    }
}

fn trace_engine_execute_failed(
    meta: &ExportRecord,
    runtime_kind: &'static str,
    context: &ExportJobContext,
    error: &ServerError,
) {
    observability::request_failure_event!(
        target: target::ENGINE,
        error: error,
        event = event::ENGINE_EXECUTE_FAILED,
        service = observability::SERVICE_NAME,
        server_instance_id = observability::server_instance_id(),
        pid = observability::pid(),
        export_id = %meta.id(),
        export_name = %meta.name(),
        engine_kind = %meta.engine_kind(),
        runtime_kind = runtime_kind,
        command = context.command(),
        offset = ?context.offset(),
        length = ?context.length(),
        connection_id = context.connection_id().raw(),
        request_sequence = context.request_sequence().raw(),
        cookie = context.cookie().raw(),
        status = "error",
        duration_ms = observability::duration_ms(context.elapsed()),
    );
}

fn trace_request_failed(context: &ExportJobContext, phase: &'static str, error: &ServerError) {
    observability::request_failure_event!(
        target: target::REQUEST,
        error: error,
        event = event::REQUEST_FAILED,
        service = observability::SERVICE_NAME,
        server_instance_id = observability::server_instance_id(),
        pid = observability::pid(),
        connection_id = context.connection_id().raw(),
        request_sequence = context.request_sequence().raw(),
        cookie = context.cookie().raw(),
        command = context.command(),
        offset = ?context.offset(),
        length = ?context.length(),
        phase = phase,
        duration_ms = observability::duration_ms(context.elapsed()),
    );
}

impl SerialRuntimeLifecycle {
    fn ensure_open(&self) -> Result<()> {
        let state = self.state()?;
        if state.closed {
            return Err(ServerError::RuntimeClosed {
                resource: "serial export runtime",
            });
        }
        Ok(())
    }

    fn begin_submit(&self) -> Result<()> {
        let mut state = self.state()?;
        if state.closed {
            return Err(ServerError::RuntimeClosed {
                resource: "serial export runtime",
            });
        }

        state.active_jobs += 1;
        Ok(())
    }

    fn finish_job(&self) {
        let Ok(mut state) = self.state.lock() else {
            return;
        };
        state.active_jobs = state
            .active_jobs
            .checked_sub(1)
            .expect("serial runtime active job count underflow");
        if state.active_jobs == 0 {
            self.empty.notify_waiters();
        }
    }

    fn close(&self) -> Result<()> {
        let mut state = self.state()?;
        state.closed = true;
        if state.active_jobs == 0 {
            self.empty.notify_waiters();
        }
        Ok(())
    }

    async fn wait_empty(&self) -> Result<()> {
        loop {
            let notified = self.empty.notified();
            let empty = {
                let state = self.state()?;
                state.active_jobs == 0
            };
            if empty {
                return Ok(());
            }

            notified.await;
        }
    }

    fn state(&self) -> Result<std::sync::MutexGuard<'_, SerialRuntimeState>> {
        self.state.lock().map_err(|_| ServerError::LockPoisoned {
            resource: "serial export runtime lifecycle",
        })
    }
}

impl ConcurrentRuntimeLifecycle {
    fn ensure_open(&self) -> Result<()> {
        let state = self.state()?;
        if state.closed {
            return Err(ServerError::RuntimeClosed {
                resource: "concurrent export runtime",
            });
        }
        Ok(())
    }

    fn begin_submit(self: &Arc<Self>) -> Result<ConcurrentActiveJob> {
        let mut state = self.state()?;
        if state.closed {
            return Err(ServerError::RuntimeClosed {
                resource: "concurrent export runtime",
            });
        }

        state.active_jobs += 1;
        Ok(ConcurrentActiveJob {
            lifecycle: self.clone(),
            finished: false,
        })
    }

    fn finish_job(&self) {
        let Ok(mut state) = self.state.lock() else {
            return;
        };
        state.active_jobs = state
            .active_jobs
            .checked_sub(1)
            .expect("concurrent runtime active job count underflow");
        if state.active_jobs == 0 {
            self.empty.notify_waiters();
        }
    }

    fn close(&self) -> Result<()> {
        let mut state = self.state()?;
        state.closed = true;
        if state.active_jobs == 0 {
            self.empty.notify_waiters();
        }
        Ok(())
    }

    async fn wait_empty(&self) -> Result<()> {
        loop {
            let notified = self.empty.notified();
            let empty = {
                let state = self.state()?;
                state.active_jobs == 0
            };
            if empty {
                return Ok(());
            }

            notified.await;
        }
    }

    fn state(&self) -> Result<std::sync::MutexGuard<'_, ConcurrentRuntimeState>> {
        self.state.lock().map_err(|_| ServerError::LockPoisoned {
            resource: "concurrent export runtime lifecycle",
        })
    }
}

impl ConcurrentActiveJob {
    fn finish(mut self) {
        self.lifecycle.finish_job();
        self.finished = true;
    }
}

impl Drop for ConcurrentActiveJob {
    fn drop(&mut self) {
        if !self.finished {
            self.lifecycle.finish_job();
        }
    }
}

#[async_trait::async_trait]
impl ExportRuntime for SerialExportRuntime {
    fn export_record(&self) -> ExportRecord {
        self.meta.clone()
    }

    async fn reserve(&self) -> Result<ExportQueueSlot> {
        self.lifecycle.ensure_open()?;
        let permit = self
            .queue_depth
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| ServerError::RuntimeClosed {
                resource: "serial export runtime",
            })?;
        self.lifecycle.ensure_open()?;
        Ok(ExportQueueSlot {
            _queue_depth: permit,
        })
    }

    async fn submit(&self, job: ExportJob) -> Result<()> {
        trace_runtime_submit(&self.meta, "serial", job.context());
        self.lifecycle.begin_submit()?;
        self.sender.send(job).await.map_err(|_| {
            self.lifecycle.finish_job();
            ServerError::RuntimeClosed {
                resource: "serial export runtime",
            }
        })
    }

    async fn close(&self) -> Result<()> {
        self.lifecycle.close()?;
        self.queue_depth.close();
        self.lifecycle.wait_empty().await?;
        self.engine_close.close().await
    }
}

#[async_trait::async_trait]
impl ExportRuntime for ConcurrentExportRuntime {
    fn export_record(&self) -> ExportRecord {
        self.meta.clone()
    }

    async fn reserve(&self) -> Result<ExportQueueSlot> {
        self.lifecycle.ensure_open()?;
        let permit = self
            .queue_depth
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| ServerError::RuntimeClosed {
                resource: "concurrent export runtime",
            })?;
        self.lifecycle.ensure_open()?;
        Ok(ExportQueueSlot {
            _queue_depth: permit,
        })
    }

    async fn submit(&self, job: ExportJob) -> Result<()> {
        trace_runtime_submit(&self.meta, "concurrent", job.context());
        let active_job = self.lifecycle.begin_submit()?;
        let span = observability::request_span(&self.meta, "concurrent", job.context());
        let prepared =
            prepare_admitted_job(self.admission_policy.clone(), self.admission.clone(), job);
        let engine = self.engine.clone();
        let meta = self.meta.clone();
        tokio::spawn(
            async move {
                let context = match &prepared {
                    PreparedExportJob::Registered(job) => job.context.clone(),
                    PreparedExportJob::Rejected(job) => job.context.clone(),
                };
                trace_runtime_task_started(&meta, "concurrent", &context);
                match prepared {
                    PreparedExportJob::Registered(job) => {
                        execute_registered_job(&meta, "concurrent", engine, job).await
                    }
                    PreparedExportJob::Rejected(job) => complete_rejected_job(job).await,
                }
                trace_runtime_task_completed(&meta, "concurrent", &context);
                active_job.finish();
            }
            .instrument(span),
        );
        Ok(())
    }

    async fn close(&self) -> Result<()> {
        self.lifecycle.close()?;
        self.queue_depth.close();
        self.lifecycle.wait_empty().await?;
        self.engine_close.close().await
    }
}
