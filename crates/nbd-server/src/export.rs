use crate::Result;
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
pub struct ReplySink {
    sender: oneshot::Sender<ExportResult>,
}

impl ReplySink {
    pub fn oneshot() -> (Self, oneshot::Receiver<ExportResult>) {
        let (sender, receiver) = oneshot::channel();
        (Self { sender }, receiver)
    }

    pub fn send(self, result: ExportResult) {
        let _ = self.sender.send(result);
    }
}

/// Work item accepted by an export runtime.
#[derive(Debug)]
pub struct ExportJob {
    request: ExportRequest,
    reply: ReplySink,
}

impl ExportJob {
    pub fn new(request: ExportRequest, reply: ReplySink) -> Self {
        Self { request, reply }
    }

    pub fn oneshot(request: ExportRequest) -> (Self, oneshot::Receiver<ExportResult>) {
        let (reply, receiver) = ReplySink::oneshot();
        (Self::new(request, reply), receiver)
    }

    pub fn into_parts(self) -> (ExportRequest, ReplySink) {
        (self.request, self.reply)
    }
}
