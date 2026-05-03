use crate::{
    connection::{ConnectionReply, ReplyKind},
    runtime::ExportQueueSlot,
    Result,
};
use nbd_protocol::wire::NbdCookie;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};

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
    target: ExportCompletionTarget,
}

#[derive(Debug)]
enum ExportCompletionTarget {
    OneShot(oneshot::Sender<CompletedExport>),
    Connection(ConnectionExportCompletion),
}

#[derive(Debug)]
struct ConnectionExportCompletion {
    cookie: NbdCookie,
    kind: ReplyKind,
    replies: mpsc::Sender<ConnectionReply>,
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

    pub(crate) fn connection(
        cookie: NbdCookie,
        kind: ReplyKind,
        replies: mpsc::Sender<ConnectionReply>,
    ) -> Self {
        Self {
            target: ExportCompletionTarget::Connection(ConnectionExportCompletion {
                cookie,
                kind,
                replies,
            }),
        }
    }

    pub async fn complete(self, result: ExportResult, queue_slot: ExportQueueSlot) {
        match self.target {
            ExportCompletionTarget::OneShot(sender) => {
                let _ = sender.send(CompletedExport {
                    result,
                    _queue_slot: queue_slot,
                });
            }
            ExportCompletionTarget::Connection(completion) => {
                let reply = ConnectionReply::export_result(
                    completion.cookie,
                    completion.kind,
                    result,
                    queue_slot,
                );
                let _ = completion.replies.send(reply).await;
            }
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
        connection::{ConnectionReply, ReplyKind},
        memory::MemoryExportEngine,
        runtime::{ExportRuntime, SerialExportRuntime},
    };
    use nbd_control_plane::{
        CommittedRoot, ExportEngineKind, ExportGeneration, ExportId, ExportMeta, ExportName,
        ExportState, Timestamp, WalSeq,
    };

    #[tokio::test]
    async fn connection_completion_holds_slot_until_reply_drop() {
        let meta = export_meta("disk-a", 4096);
        let engine = Arc::new(MemoryExportEngine::new(&meta).expect("memory engine"));
        let runtime = SerialExportRuntime::with_capacity(meta, engine, 1);
        let queue_slot = runtime.reserve().await.expect("reserve queue slot");
        let (sender, mut receiver) = mpsc::channel(1);

        ExportCompletion::connection(NbdCookie::new(7), ReplyKind::Simple, sender)
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
            .send(ConnectionReply::export_result(
                NbdCookie::new(10),
                ReplyKind::Simple,
                Ok(ExportReply::Done),
                queued_slot,
            ))
            .await
            .expect("fill reply queue");

        let completion =
            ExportCompletion::connection(NbdCookie::new(11), ReplyKind::Simple, sender.clone());
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
}
