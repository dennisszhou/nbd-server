use crate::{ExportEngineHandle, ExportJob, Result, ServerError};
use nbd_control_plane::ExportMeta;
use std::sync::Arc;
use tokio::sync::{mpsc, OwnedSemaphorePermit, Semaphore};

pub const DEFAULT_EXPORT_QUEUE_CAPACITY: usize = 128;

/// Export-owned request execution boundary.
#[async_trait::async_trait]
pub trait ExportRuntime: Send + Sync {
    fn export_meta(&self) -> ExportMeta;

    async fn reserve(&self) -> Result<ExportQueueSlot>;

    async fn submit(&self, job: ExportJob) -> Result<()>;
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
    sender: mpsc::Sender<ExportJob>,
}

impl SerialExportRuntime {
    pub fn new(meta: ExportMeta, engine: ExportEngineHandle) -> Self {
        Self::with_capacity(meta, engine, DEFAULT_EXPORT_QUEUE_CAPACITY)
    }

    pub fn with_capacity(meta: ExportMeta, engine: ExportEngineHandle, capacity: usize) -> Self {
        let queue_depth = Arc::new(Semaphore::new(capacity));
        let (sender, mut receiver) = mpsc::channel::<ExportJob>(capacity);

        tokio::spawn(async move {
            while let Some(job) = receiver.recv().await {
                let (request, reply) = job.into_parts();
                reply.send(engine.execute(request).await);
            }
        });

        Self {
            meta,
            queue_depth,
            sender,
        }
    }
}

#[async_trait::async_trait]
impl ExportRuntime for SerialExportRuntime {
    fn export_meta(&self) -> ExportMeta {
        self.meta.clone()
    }

    async fn reserve(&self) -> Result<ExportQueueSlot> {
        self.queue_depth
            .clone()
            .acquire_owned()
            .await
            .map(|permit| ExportQueueSlot {
                _queue_depth: permit,
            })
            .map_err(|_| ServerError::RuntimeClosed {
                resource: "serial export runtime",
            })
    }

    async fn submit(&self, job: ExportJob) -> Result<()> {
        self.sender
            .send(job)
            .await
            .map_err(|_| ServerError::RuntimeClosed {
                resource: "serial export runtime",
            })
    }
}
