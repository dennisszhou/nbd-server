use crate::{
    AdmissionWaiter, AdmittedExportRequest, ExportAdmissionCtl, ExportAdmissionPolicyHandle,
    ExportCompletion, ExportEngineHandle, ExportJob, ExportJobContext, ExportRequest, ExportResult,
    Result, ServerError,
};
use nbd_control_plane::ExportMeta;
use std::sync::Arc;
use std::sync::Mutex;
use tokio::sync::{mpsc, Notify, OwnedSemaphorePermit, Semaphore};

pub const DEFAULT_EXPORT_QUEUE_CAPACITY: usize = 128;

/// Export-owned request execution boundary.
#[async_trait::async_trait]
pub trait ExportRuntime: Send + Sync {
    fn export_meta(&self) -> ExportMeta;

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
    meta: ExportMeta,
    queue_depth: Arc<Semaphore>,
    lifecycle: Arc<SerialRuntimeLifecycle>,
    sender: mpsc::Sender<ExportJob>,
}

/// Runtime policy that executes compatible admitted export jobs concurrently.
#[derive(Clone)]
pub struct ConcurrentExportRuntime {
    meta: ExportMeta,
    engine: ExportEngineHandle,
    admission: ExportAdmissionCtl,
    admission_policy: ExportAdmissionPolicyHandle,
    queue_depth: Arc<Semaphore>,
    lifecycle: Arc<ConcurrentRuntimeLifecycle>,
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
    pub fn new(meta: ExportMeta, engine: ExportEngineHandle) -> Self {
        Self::with_capacity(meta, engine, DEFAULT_EXPORT_QUEUE_CAPACITY)
    }

    pub fn with_capacity(meta: ExportMeta, engine: ExportEngineHandle, capacity: usize) -> Self {
        let admission = ExportAdmissionCtl::new(meta.size_bytes());
        let admission_policy = engine.admission_policy();
        let queue_depth = Arc::new(Semaphore::new(capacity));
        let lifecycle = Arc::new(SerialRuntimeLifecycle {
            state: Mutex::new(SerialRuntimeState {
                closed: false,
                active_jobs: 0,
            }),
            empty: Notify::new(),
        });
        let (sender, mut receiver) = mpsc::channel::<ExportJob>(capacity);
        let worker_lifecycle = lifecycle.clone();

        tokio::spawn(async move {
            while let Some(job) = receiver.recv().await {
                execute_admitted_job(
                    engine.clone(),
                    admission_policy.clone(),
                    admission.clone(),
                    job,
                )
                .await;
                worker_lifecycle.finish_job();
            }
        });

        Self {
            meta,
            queue_depth,
            lifecycle,
            sender,
        }
    }
}

impl ConcurrentExportRuntime {
    pub fn new(meta: ExportMeta, engine: ExportEngineHandle) -> Self {
        Self::with_capacity(meta, engine, DEFAULT_EXPORT_QUEUE_CAPACITY)
    }

    pub fn with_capacity(meta: ExportMeta, engine: ExportEngineHandle, capacity: usize) -> Self {
        let admission = ExportAdmissionCtl::new(meta.size_bytes());
        let admission_policy = engine.admission_policy();
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
        }
    }
}

async fn execute_admitted_job(
    engine: ExportEngineHandle,
    admission_policy: ExportAdmissionPolicyHandle,
    admission: ExportAdmissionCtl,
    job: ExportJob,
) {
    match prepare_admitted_job(admission_policy, admission, job) {
        PreparedExportJob::Registered(job) => execute_registered_job(engine, job).await,
        PreparedExportJob::Rejected(job) => complete_rejected_job(job).await,
    }
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
            return PreparedExportJob::Rejected(RejectedExportJob {
                context,
                result: Err(error),
                completion,
                queue_slot,
            });
        }
    };

    PreparedExportJob::Registered(RegisteredExportJob {
        context,
        request,
        completion,
        queue_slot,
        waiter,
    })
}

async fn execute_registered_job(engine: ExportEngineHandle, job: RegisteredExportJob) {
    let RegisteredExportJob {
        context: _context,
        request,
        completion,
        queue_slot,
        waiter,
    } = job;
    let result = match waiter.wait().await {
        Ok(permit) => {
            engine
                .execute_admitted(AdmittedExportRequest::new(request, permit))
                .await
        }
        Err(error) => Err(error),
    };
    completion.complete(result, queue_slot).await;
}

async fn complete_rejected_job(job: RejectedExportJob) {
    let _context = job.context;
    job.completion.complete(job.result, job.queue_slot).await;
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
    fn export_meta(&self) -> ExportMeta {
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
        self.lifecycle.wait_empty().await
    }
}

#[async_trait::async_trait]
impl ExportRuntime for ConcurrentExportRuntime {
    fn export_meta(&self) -> ExportMeta {
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
        let active_job = self.lifecycle.begin_submit()?;
        let prepared =
            prepare_admitted_job(self.admission_policy.clone(), self.admission.clone(), job);
        let engine = self.engine.clone();
        tokio::spawn(async move {
            match prepared {
                PreparedExportJob::Registered(job) => execute_registered_job(engine, job).await,
                PreparedExportJob::Rejected(job) => complete_rejected_job(job).await,
            }
            active_job.finish();
        });
        Ok(())
    }

    async fn close(&self) -> Result<()> {
        self.lifecycle.close()?;
        self.queue_depth.close();
        self.lifecycle.wait_empty().await
    }
}
