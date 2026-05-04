use crate::{
    AdmittedExportRequest, ExportAdmissionCtl, ExportAdmissionProfileHandle, ExportEngineHandle,
    ExportJob, Result, ServerError,
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

impl SerialExportRuntime {
    pub fn new(meta: ExportMeta, engine: ExportEngineHandle) -> Self {
        Self::with_capacity(meta, engine, DEFAULT_EXPORT_QUEUE_CAPACITY)
    }

    pub fn with_capacity(meta: ExportMeta, engine: ExportEngineHandle, capacity: usize) -> Self {
        let admission = ExportAdmissionCtl::new(meta.size_bytes());
        let admission_profile = engine.admission_profile();
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
                    admission_profile.clone(),
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

async fn execute_admitted_job(
    engine: ExportEngineHandle,
    admission_profile: ExportAdmissionProfileHandle,
    admission: ExportAdmissionCtl,
    job: ExportJob,
) {
    let (request, completion, queue_slot) = job.into_parts();
    let result = match admission_profile.operation_for(&request) {
        Ok(op) => match admission.register(op) {
            Ok(waiter) => match waiter.wait().await {
                Ok(permit) => {
                    engine
                        .execute_admitted(AdmittedExportRequest::new(request, permit))
                        .await
                }
                Err(error) => Err(error),
            },
            Err(error) => Err(error),
        },
        Err(error) => Err(error),
    };
    completion.complete(result, queue_slot).await;
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
