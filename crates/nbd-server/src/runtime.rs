use crate::{ExportEngineHandle, ExportJob, Result, ServerError};
use nbd_control_plane::ExportMeta;
use std::sync::Arc;
use tokio::sync::mpsc;

pub const DEFAULT_EXPORT_QUEUE_CAPACITY: usize = 128;

/// Export-owned request execution boundary.
#[async_trait::async_trait]
pub trait ExportRuntime: Send + Sync {
    fn export_meta(&self) -> ExportMeta;

    async fn submit(&self, job: ExportJob) -> Result<()>;
}

pub type ExportRuntimeHandle = Arc<dyn ExportRuntime>;

/// Runtime policy that executes accepted export jobs one at a time.
#[derive(Debug, Clone)]
pub struct SerialExportRuntime {
    meta: ExportMeta,
    sender: mpsc::Sender<ExportJob>,
}

impl SerialExportRuntime {
    pub fn new(meta: ExportMeta, engine: ExportEngineHandle) -> Self {
        Self::with_capacity(meta, engine, DEFAULT_EXPORT_QUEUE_CAPACITY)
    }

    pub fn with_capacity(meta: ExportMeta, engine: ExportEngineHandle, capacity: usize) -> Self {
        let (sender, mut receiver) = mpsc::channel::<ExportJob>(capacity);

        tokio::spawn(async move {
            while let Some(job) = receiver.recv().await {
                let (request, reply) = job.into_parts();
                reply.send(engine.execute(request).await);
            }
        });

        Self { meta, sender }
    }
}

#[async_trait::async_trait]
impl ExportRuntime for SerialExportRuntime {
    fn export_meta(&self) -> ExportMeta {
        self.meta.clone()
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
