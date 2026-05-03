use crate::{runtime::ExportQueueSlot, Result};
use std::sync::Arc;
use tokio::sync::oneshot;

/// Byte-oriented export boundary used by protocol handling.
#[async_trait::async_trait]
pub trait Export: Send + Sync {
    async fn read(&self, offset: u64, len: u32) -> Result<Vec<u8>>;

    async fn write(&self, offset: u64, data: &[u8]) -> Result<()>;

    async fn flush(&self) -> Result<()>;
}

pub type ExportHandle = Arc<dyn Export>;

/// Export request after wire decoding and before backend execution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExportRequest {
    Read { offset: u64, len: u32 },
    Write { offset: u64, data: Vec<u8> },
    Flush,
}

/// Export reply before NBD wire encoding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExportReply {
    Read { data: Vec<u8> },
    Done,
}

pub type ExportResult = Result<ExportReply>;

/// Data behavior for one active export.
#[async_trait::async_trait]
pub trait ExportEngine: Send + Sync {
    async fn execute(&self, request: ExportRequest) -> ExportResult;
}

pub type ExportEngineHandle = Arc<dyn ExportEngine>;

/// Per-request reply target owned by the connection path.
#[derive(Debug)]
pub struct ExportCompletion {
    sender: oneshot::Sender<CompletedExport>,
}

impl ExportCompletion {
    pub fn oneshot() -> (Self, oneshot::Receiver<CompletedExport>) {
        let (sender, receiver) = oneshot::channel();
        (Self { sender }, receiver)
    }

    pub fn complete(self, result: ExportResult, queue_slot: ExportQueueSlot) {
        let _ = self.sender.send(CompletedExport {
            result,
            _queue_slot: queue_slot,
        });
    }
}

/// Completed export work plus the queue slot it still occupies.
#[derive(Debug)]
pub struct CompletedExport {
    result: ExportResult,
    _queue_slot: ExportQueueSlot,
}

impl CompletedExport {
    pub fn into_parts(self) -> (ExportResult, ExportQueueSlot) {
        (self.result, self._queue_slot)
    }
}

/// Work item accepted by an export runtime.
#[derive(Debug)]
pub struct ExportJob {
    request: ExportRequest,
    completion: ExportCompletion,
    queue_slot: ExportQueueSlot,
}

impl ExportJob {
    pub fn new(
        request: ExportRequest,
        completion: ExportCompletion,
        queue_slot: ExportQueueSlot,
    ) -> Self {
        Self {
            request,
            completion,
            queue_slot,
        }
    }

    pub fn oneshot(
        request: ExportRequest,
        queue_slot: ExportQueueSlot,
    ) -> (Self, oneshot::Receiver<CompletedExport>) {
        let (completion, receiver) = ExportCompletion::oneshot();
        (Self::new(request, completion, queue_slot), receiver)
    }

    pub fn into_parts(self) -> (ExportRequest, ExportCompletion, ExportQueueSlot) {
        (self.request, self.completion, self.queue_slot)
    }
}
