use crate::{ExportEngineHandle, ExportJob, Result, ServerError};
use std::sync::Arc;
use tokio::sync::mpsc;

pub const DEFAULT_EXPORT_QUEUE_CAPACITY: usize = 128;

/// Export-owned request execution boundary.
#[async_trait::async_trait]
pub trait ExportRuntime: Send + Sync {
    async fn submit(&self, job: ExportJob) -> Result<()>;
}

pub type ExportRuntimeHandle = Arc<dyn ExportRuntime>;

/// Runtime policy that executes accepted export jobs one at a time.
#[derive(Debug, Clone)]
pub struct SerialExportRuntime {
    sender: mpsc::Sender<ExportJob>,
}

impl SerialExportRuntime {
    pub fn new(engine: ExportEngineHandle) -> Self {
        Self::with_capacity(engine, DEFAULT_EXPORT_QUEUE_CAPACITY)
    }

    pub fn with_capacity(engine: ExportEngineHandle, capacity: usize) -> Self {
        let (sender, mut receiver) = mpsc::channel::<ExportJob>(capacity);

        tokio::spawn(async move {
            while let Some(job) = receiver.recv().await {
                let (request, reply) = job.into_parts();
                reply.send(engine.execute(request).await);
            }
        });

        Self { sender }
    }
}

#[async_trait::async_trait]
impl ExportRuntime for SerialExportRuntime {
    async fn submit(&self, job: ExportJob) -> Result<()> {
        self.sender
            .send(job)
            .await
            .map_err(|_| ServerError::RuntimeClosed {
                resource: "serial export runtime",
            })
    }
}
