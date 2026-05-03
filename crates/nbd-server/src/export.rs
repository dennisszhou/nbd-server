use crate::{runtime::ExportQueueSlot, Result};
use std::fmt;
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

#[async_trait::async_trait]
pub(crate) trait ExportCompletionSink: Send {
    async fn complete(self: Box<Self>, completed: CompletedExport);
}

/// Per-request reply target owned by the connection path.
pub struct ExportCompletion {
    target: ExportCompletionTarget,
}

enum ExportCompletionTarget {
    OneShot(oneshot::Sender<CompletedExport>),
    Sink(Box<dyn ExportCompletionSink>),
}

impl ExportCompletion {
    pub fn oneshot() -> (Self, oneshot::Receiver<CompletedExport>) {
        let (sender, receiver) = oneshot::channel();
        (
            Self {
                target: ExportCompletionTarget::OneShot(sender),
            },
            receiver,
        )
    }

    pub(crate) fn sink(sink: impl ExportCompletionSink + 'static) -> Self {
        Self {
            target: ExportCompletionTarget::Sink(Box::new(sink)),
        }
    }

    pub async fn complete(self, result: ExportResult, queue_slot: ExportQueueSlot) {
        let completed = CompletedExport {
            result,
            _queue_slot: queue_slot,
        };
        match self.target {
            ExportCompletionTarget::OneShot(sender) => {
                let _ = sender.send(completed);
            }
            ExportCompletionTarget::Sink(sink) => sink.complete(completed).await,
        }
    }
}

impl fmt::Debug for ExportCompletion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ExportCompletion")
            .field("target", &self.target.kind())
            .finish()
    }
}

impl ExportCompletionTarget {
    fn kind(&self) -> &'static str {
        match self {
            Self::OneShot(_) => "oneshot",
            Self::Sink(_) => "sink",
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        memory::MemoryExportEngine,
        runtime::{ExportRuntime, SerialExportRuntime},
    };
    use nbd_control_plane::{
        CommittedRoot, ExportEngineKind, ExportGeneration, ExportId, ExportMeta, ExportName,
        ExportState, Timestamp, WalSeq,
    };
    use tokio::sync::mpsc;

    #[tokio::test]
    async fn connection_completion_holds_slot_until_reply_drop() {
        let meta = export_meta("disk-a", 4096);
        let engine = Arc::new(MemoryExportEngine::new(&meta).expect("memory engine"));
        let runtime = SerialExportRuntime::with_capacity(meta, engine, 1);
        let queue_slot = runtime.reserve().await.expect("reserve queue slot");
        let (sender, mut receiver) = mpsc::channel(1);

        ExportCompletion::sink(TestCompletionSink { sender })
            .complete(Ok(ExportReply::Done), queue_slot)
            .await;

        let waiter_runtime = runtime.clone();
        let waiter =
            tokio::spawn(async move { waiter_runtime.reserve().await.expect("reserve again") });
        tokio::task::yield_now().await;
        assert!(
            !waiter.is_finished(),
            "queued reply should keep the export queue slot occupied",
        );

        let reply = receiver.recv().await.expect("connection reply");
        tokio::task::yield_now().await;
        assert!(
            !waiter.is_finished(),
            "received reply should keep the slot until the writer drops it",
        );

        drop(reply);
        let next_slot = waiter.await.expect("reservation task");
        drop(next_slot);
    }

    #[tokio::test]
    async fn connection_completion_waits_for_reply_queue_capacity() {
        let meta = export_meta("disk-a", 4096);
        let engine = Arc::new(MemoryExportEngine::new(&meta).expect("memory engine"));
        let runtime = SerialExportRuntime::with_capacity(meta, engine, 2);
        let queued_slot = runtime.reserve().await.expect("reserve queued slot");
        let pending_slot = runtime.reserve().await.expect("reserve pending slot");
        let (sender, mut receiver) = mpsc::channel(1);

        sender
            .send(CompletedExport {
                result: Ok(ExportReply::Done),
                _queue_slot: queued_slot,
            })
            .await
            .expect("fill reply queue");

        let completion = ExportCompletion::sink(TestCompletionSink {
            sender: sender.clone(),
        });
        let complete_task = tokio::spawn(async move {
            completion
                .complete(Ok(ExportReply::Done), pending_slot)
                .await;
        });
        tokio::task::yield_now().await;
        assert!(
            !complete_task.is_finished(),
            "completion should wait while the bounded reply queue is full",
        );

        let waiter_runtime = runtime.clone();
        let waiter =
            tokio::spawn(async move { waiter_runtime.reserve().await.expect("reserve again") });
        tokio::task::yield_now().await;
        assert!(
            !waiter.is_finished(),
            "both queue slots should stay occupied while handoff waits",
        );

        let queued_reply = receiver.recv().await.expect("queued reply");
        complete_task.await.expect("completion task");
        tokio::task::yield_now().await;
        assert!(
            !waiter.is_finished(),
            "handoff should move the pending slot into the reply queue",
        );

        drop(queued_reply);
        let next_slot = waiter.await.expect("reservation task");
        drop(next_slot);
        drop(receiver.recv().await.expect("pending reply"));
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

    struct TestCompletionSink {
        sender: mpsc::Sender<CompletedExport>,
    }

    #[async_trait::async_trait]
    impl ExportCompletionSink for TestCompletionSink {
        async fn complete(self: Box<Self>, completed: CompletedExport) {
            let _ = self.sender.send(completed).await;
        }
    }
}
