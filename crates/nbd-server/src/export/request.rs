use super::admission::AdmissionPermit;
use crate::error::Result;
use crate::observability::{self, event, target};
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

static NEXT_CONNECTION_ID: AtomicU64 = AtomicU64::new(1);

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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RequestCookie(u64);

impl RequestCookie {
    pub fn new(raw: u64) -> Self {
        Self(raw)
    }

    pub fn raw(self) -> u64 {
        self.0
    }
}

impl fmt::Display for RequestCookie {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ConnectionId(u64);

impl ConnectionId {
    pub(crate) fn next() -> Self {
        Self(NEXT_CONNECTION_ID.fetch_add(1, Ordering::Relaxed))
    }

    pub(crate) fn internal() -> Self {
        Self(0)
    }

    pub fn raw(self) -> u64 {
        self.0
    }
}

impl fmt::Display for ConnectionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RequestSequence(u64);

impl RequestSequence {
    pub(crate) fn internal() -> Self {
        Self(0)
    }

    pub fn raw(self) -> u64 {
        self.0
    }
}

impl fmt::Display for RequestSequence {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

#[derive(Debug)]
pub(crate) struct RequestSequenceGenerator {
    next: u64,
}

impl RequestSequenceGenerator {
    pub(crate) fn new() -> Self {
        Self { next: 1 }
    }

    pub(crate) fn next(&mut self) -> RequestSequence {
        let sequence = RequestSequence(self.next);
        self.next += 1;
        sequence
    }
}

#[derive(Debug, Clone)]
pub struct ExportJobContext {
    connection_id: ConnectionId,
    request_sequence: RequestSequence,
    cookie: RequestCookie,
    command: &'static str,
    offset: Option<u64>,
    length: Option<u64>,
    reply_kind: &'static str,
    started_at: Instant,
}

impl ExportJobContext {
    pub(crate) fn new(
        connection_id: ConnectionId,
        request_sequence: RequestSequence,
        cookie: RequestCookie,
        command: &'static str,
        offset: Option<u64>,
        length: Option<u64>,
        reply_kind: &'static str,
    ) -> Self {
        Self {
            connection_id,
            request_sequence,
            cookie,
            command,
            offset,
            length,
            reply_kind,
            started_at: Instant::now(),
        }
    }

    pub(crate) fn internal(cookie: RequestCookie, command: &'static str) -> Self {
        Self::new(
            ConnectionId::internal(),
            RequestSequence::internal(),
            cookie,
            command,
            None,
            None,
            "internal",
        )
    }

    pub fn connection_id(&self) -> ConnectionId {
        self.connection_id
    }

    pub fn request_sequence(&self) -> RequestSequence {
        self.request_sequence
    }

    pub fn cookie(&self) -> RequestCookie {
        self.cookie
    }

    pub fn command(&self) -> &'static str {
        self.command
    }

    pub fn offset(&self) -> Option<u64> {
        self.offset
    }

    pub fn length(&self) -> Option<u64> {
        self.length
    }

    pub fn reply_kind(&self) -> &'static str {
        self.reply_kind
    }

    pub fn elapsed(&self) -> Duration {
        self.started_at.elapsed()
    }
}

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

impl ExportRequest {
    pub fn command_name(&self) -> &'static str {
        match self {
            Self::Read { .. } => "read",
            Self::Write { .. } => "write",
            Self::Flush => "flush",
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::export::{AdmissionOp, ExportAdmissionCtl};
    use crate::range::ByteRange;

    #[test]
    fn request_sequence_generator_starts_at_one() {
        let mut generator = RequestSequenceGenerator::new();

        assert_eq!(generator.next().raw(), 1);
        assert_eq!(generator.next().raw(), 2);
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
        let context = ExportJobContext::internal(RequestCookie::new(7), "write");
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
}
