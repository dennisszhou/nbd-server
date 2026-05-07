use crate::{
    Result,
    admission::{AdmissionOp, AdmissionPermit},
    observability::{self, ExportJobContext, event, target},
    runtime::ExportQueueSlot,
};
use nbd_protocol::wire::NbdCookie;
use std::fmt;
use std::sync::Arc;
use tokio::sync::oneshot;

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

/// Backing-store-specific mapping from export requests to admission operations.
pub trait ExportAdmissionPolicy: Send + Sync {
    fn operation_for(&self, request: &ExportRequest) -> Result<AdmissionOp>;
}

pub type ExportAdmissionPolicyHandle = Arc<dyn ExportAdmissionPolicy>;

/// Export request bundled with the admission permit that authorizes it.
#[derive(Debug)]
pub struct AdmittedExportRequest {
    request: Option<ExportRequest>,
    permit: Option<AdmissionPermit>,
    context: ExportJobContext,
}

/// Owned admitted request form for engines that must move request payloads.
#[derive(Debug)]
pub struct OwnedAdmittedExportRequest {
    request: Option<ExportRequest>,
    permit: Option<AdmissionPermit>,
    context: ExportJobContext,
}

impl AdmittedExportRequest {
    pub(crate) fn new(
        request: ExportRequest,
        permit: AdmissionPermit,
        context: ExportJobContext,
    ) -> Self {
        Self {
            request: Some(request),
            permit: Some(permit),
            context,
        }
    }

    /// Return the admitted request while keeping its admission permit live.
    pub fn request(&self) -> &ExportRequest {
        self.request
            .as_ref()
            .expect("admitted export request is present")
    }

    /// Move into the owned form without releasing the admission permit.
    pub fn into_owned(mut self) -> OwnedAdmittedExportRequest {
        OwnedAdmittedExportRequest {
            request: self.request.take(),
            permit: self.permit.take(),
            context: self.context.clone(),
        }
    }
}

impl Drop for AdmittedExportRequest {
    fn drop(&mut self) {
        release_admission_permit(&mut self.permit, &self.context);
    }
}

impl OwnedAdmittedExportRequest {
    /// Return the admitted request while keeping its admission permit live.
    pub fn request(&self) -> &ExportRequest {
        self.request
            .as_ref()
            .expect("owned admitted export request is present")
    }

    /// Take the request payload while keeping the admission permit live.
    pub fn take_request(&mut self) -> ExportRequest {
        self.request
            .take()
            .expect("owned admitted export request can be taken once")
    }
}

impl Drop for OwnedAdmittedExportRequest {
    fn drop(&mut self) {
        release_admission_permit(&mut self.permit, &self.context);
    }
}

fn release_admission_permit(permit: &mut Option<AdmissionPermit>, context: &ExportJobContext) {
    let Some(permit) = permit.take() else {
        return;
    };
    let ticket = permit.ticket();
    let op = permit.op();
    // Release admission before logging so observability cannot delay promotion
    // of later waiters.
    drop(permit);

    tracing::trace!(
        target: target::ADMISSION,
        event = event::ADMISSION_RELEASED,
        service = observability::SERVICE_NAME,
        server_instance_id = observability::server_instance_id(),
        pid = observability::pid(),
        admission_ticket = ticket.as_u64(),
        admission_op = op.kind(),
        range_start = ?op.range().map(crate::ByteRange::start),
        range_len = ?op.range().map(crate::ByteRange::len),
        connection_id = context.connection_id().raw(),
        request_sequence = context.request_sequence().raw(),
        cookie = context.cookie().raw(),
    );
}

/// Data behavior for one active export.
#[async_trait::async_trait]
pub trait ExportEngine: Send + Sync {
    fn admission_policy(&self) -> ExportAdmissionPolicyHandle;

    async fn execute_admitted(&self, request: AdmittedExportRequest) -> ExportResult;

    async fn close(&self) -> Result<()> {
        Ok(())
    }
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
    context: ExportJobContext,
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
        Self::with_context(
            ExportJobContext::internal(NbdCookie::new(0), request.command_name()),
            request,
            completion,
            queue_slot,
        )
    }

    pub fn with_context(
        context: ExportJobContext,
        request: ExportRequest,
        completion: ExportCompletion,
        queue_slot: ExportQueueSlot,
    ) -> Self {
        Self {
            context,
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

    pub fn context(&self) -> &ExportJobContext {
        &self.context
    }

    pub fn into_parts(
        self,
    ) -> (
        ExportJobContext,
        ExportRequest,
        ExportCompletion,
        ExportQueueSlot,
    ) {
        (self.context, self.request, self.completion, self.queue_slot)
    }
}

impl ExportRequest {
    pub fn command_name(&self) -> &'static str {
        match self {
            Self::Read { .. } => "read",
            Self::Write { .. } => "write",
            Self::Flush => "flush",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        admission::{AdmissionOp, ExportAdmissionCtl},
        ByteRange,
        memory::MemoryExportEngine,
        runtime::{ExportRuntime, SerialExportRuntime},
    };
    use nbd_control_plane::{
        ExportEngineKind, ExportHead, ExportId, ExportName, ExportRecord, ExportState, Timestamp,
    };
    use tokio::sync::mpsc;

    #[tokio::test]
    async fn connection_completion_holds_slot_until_reply_drop() {
        let meta = export_record("disk-a", 4096);
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
    async fn connection_completion_waits_for_reply_queue_space() {
        let meta = export_record("disk-a", 4096);
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

    #[tokio::test]
    async fn owned_admitted_request_keeps_permit_after_payload_take() {
        let admission = ExportAdmissionCtl::new(4096);
        let first_permit = admission
            .register(AdmissionOp::Write(ByteRange::new(0, 4)))
            .expect("register first")
            .wait()
            .await
            .expect("first permit");
        let second_waiter = admission
            .register(AdmissionOp::Write(ByteRange::new(0, 4)))
            .expect("register second");
        let context = ExportJobContext::internal(NbdCookie::new(7), "write");
        let admitted = AdmittedExportRequest::new(
            ExportRequest::Write {
                offset: 0,
                data: b"test".to_vec(),
            },
            first_permit,
            context,
        );
        let mut owned = admitted.into_owned();

        let second_task =
            tokio::spawn(async move { second_waiter.wait().await.expect("second permit") });
        tokio::task::yield_now().await;
        assert!(
            !second_task.is_finished(),
            "owned admitted request should still hold admission",
        );

        let request = owned.take_request();
        assert_eq!(
            request,
            ExportRequest::Write {
                offset: 0,
                data: b"test".to_vec(),
            }
        );
        tokio::task::yield_now().await;
        assert!(
            !second_task.is_finished(),
            "taking the payload must not release admission",
        );

        drop(owned);
        let second_permit = second_task.await.expect("second task");
        drop(second_permit);
    }

    fn export_record(name: &str, size_bytes: u64) -> ExportRecord {
        ExportRecord::new(
            ExportId::new(format!("export-{name}")).expect("export id"),
            ExportName::new(name).expect("export name"),
            4096,
            ExportEngineKind::Memory,
            ExportState::Active,
            ExportHead::memory_empty(size_bytes).expect("memory head"),
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
