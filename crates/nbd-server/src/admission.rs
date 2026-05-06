use crate::{
    Result, ServerError,
    observability::{self, event, target},
};
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use tokio::sync::oneshot;

/// Logical byte range protected by export admission.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ByteRange {
    start: u64,
    len: u32,
}

impl ByteRange {
    pub fn new(start: u64, len: u32) -> Self {
        Self { start, len }
    }

    pub fn start(self) -> u64 {
        self.start
    }

    pub fn len(self) -> u64 {
        u64::from(self.len)
    }

    pub fn is_empty(self) -> bool {
        self.len == 0
    }

    fn checked_end(self) -> Option<u64> {
        self.start.checked_add(self.len())
    }

    fn end(self) -> u64 {
        self.start.saturating_add(u64::from(self.len))
    }

    fn overlaps(self, other: Self) -> bool {
        self.start < other.end() && other.start < self.end()
    }
}

/// Operation shape used by admission to decide read/write/flush ordering.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdmissionOp {
    Read(ByteRange),
    Write(ByteRange),
    Flush,
}

impl AdmissionOp {
    pub fn kind(self) -> &'static str {
        match self {
            Self::Read(_) => "read",
            Self::Write(_) => "write",
            Self::Flush => "flush",
        }
    }

    pub fn range(self) -> Option<ByteRange> {
        match self {
            Self::Read(range) | Self::Write(range) => Some(range),
            Self::Flush => None,
        }
    }

    fn conflicts(self, other: Self) -> bool {
        match (self, other) {
            (Self::Flush, _) | (_, Self::Flush) => true,
            (Self::Read(_), Self::Read(_)) => false,
            (Self::Read(left), Self::Write(right))
            | (Self::Write(left), Self::Read(right))
            | (Self::Write(left), Self::Write(right)) => left.overlaps(right),
        }
    }
}

/// Volatile accepted-order ticket for an admission request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct AdmissionTicket(u64);

impl AdmissionTicket {
    pub fn as_u64(self) -> u64 {
        self.0
    }
}

/// Per-export semantic admission controller.
#[derive(Debug, Clone)]
pub struct ExportAdmissionCtl {
    inner: Arc<AdmissionInner>,
}

impl ExportAdmissionCtl {
    pub fn new(extent_bytes: u64) -> Self {
        Self {
            inner: Arc::new(AdmissionInner::new(extent_bytes)),
        }
    }

    pub fn register(&self, op: AdmissionOp) -> Result<AdmissionWaiter> {
        let (grant, receiver) = oneshot::channel();
        let ticket = {
            let mut state = self.inner.state()?;
            if let Err(error) = state.validate(op) {
                trace_admission_rejected(op, &error);
                return Err(error);
            }
            let ticket = state.next_ticket();
            state
                .waiting
                .push_back(WaitingAdmission { ticket, op, grant });
            promote(&self.inner, &mut state);
            ticket
        };

        Ok(AdmissionWaiter {
            inner: self.inner.clone(),
            ticket,
            op,
            grant: Some(receiver),
            registered: true,
        })
    }
}

/// Wait handle for a registered admission request.
#[derive(Debug)]
pub struct AdmissionWaiter {
    inner: Arc<AdmissionInner>,
    ticket: AdmissionTicket,
    op: AdmissionOp,
    grant: Option<oneshot::Receiver<AdmissionPermit>>,
    registered: bool,
}

impl AdmissionWaiter {
    pub fn ticket(&self) -> AdmissionTicket {
        self.ticket
    }

    pub fn op(&self) -> AdmissionOp {
        self.op
    }

    pub async fn wait(mut self) -> Result<AdmissionPermit> {
        let grant = self
            .grant
            .take()
            .expect("admission waiter grant taken once");
        match grant.await {
            Ok(permit) => {
                self.registered = false;
                Ok(permit)
            }
            Err(_) => {
                self.registered = false;
                Err(ServerError::RuntimeClosed {
                    resource: "export admission",
                })
            }
        }
    }
}

impl Drop for AdmissionWaiter {
    fn drop(&mut self) {
        if self.registered {
            tracing::trace!(
                target: target::ADMISSION,
                event = event::ADMISSION_CANCELLED,
                service = observability::SERVICE_NAME,
                server_instance_id = observability::server_instance_id(),
                pid = observability::pid(),
                admission_ticket = self.ticket.as_u64(),
                admission_op = self.op.kind(),
                range_start = ?self.op.range().map(ByteRange::start),
                range_len = ?self.op.range().map(ByteRange::len),
            );
            self.inner.cancel(self.ticket);
        }
    }
}

/// RAII permit for an admitted operation.
#[derive(Debug)]
pub struct AdmissionPermit {
    inner: Option<Arc<AdmissionInner>>,
    ticket: AdmissionTicket,
    op: AdmissionOp,
}

impl AdmissionPermit {
    fn new(inner: Arc<AdmissionInner>, ticket: AdmissionTicket, op: AdmissionOp) -> Self {
        Self {
            inner: Some(inner),
            ticket,
            op,
        }
    }

    pub fn ticket(&self) -> AdmissionTicket {
        self.ticket
    }

    pub fn op(&self) -> AdmissionOp {
        self.op
    }

    fn disarm(&mut self) {
        self.inner = None;
    }
}

impl Drop for AdmissionPermit {
    fn drop(&mut self) {
        if let Some(inner) = self.inner.take() {
            inner.release(self.ticket);
        }
    }
}

fn trace_admission_rejected(op: AdmissionOp, error: &ServerError) {
    observability::request_failure_event!(
        target: target::ADMISSION,
        error: error,
        event = event::ADMISSION_REJECTED,
        service = observability::SERVICE_NAME,
        server_instance_id = observability::server_instance_id(),
        pid = observability::pid(),
        admission_op = op.kind(),
        range_start = ?op.range().map(ByteRange::start),
        range_len = ?op.range().map(ByteRange::len),
    );
}

#[derive(Debug)]
struct AdmissionInner {
    state: Mutex<AdmissionState>,
}

impl AdmissionInner {
    fn new(extent_bytes: u64) -> Self {
        Self {
            state: Mutex::new(AdmissionState {
                extent_bytes,
                next_ticket: 0,
                waiting: VecDeque::new(),
                active: Vec::new(),
            }),
        }
    }

    fn cancel(self: &Arc<Self>, ticket: AdmissionTicket) {
        let Ok(mut state) = self.state.lock() else {
            return;
        };
        state.waiting.retain(|waiter| waiter.ticket != ticket);
        promote(self, &mut state);
    }

    fn release(self: Arc<Self>, ticket: AdmissionTicket) {
        let Ok(mut state) = self.state.lock() else {
            return;
        };
        state.active.retain(|active| active.ticket != ticket);
        promote(&self, &mut state);
    }

    fn state(&self) -> Result<std::sync::MutexGuard<'_, AdmissionState>> {
        self.state.lock().map_err(|_| ServerError::LockPoisoned {
            resource: "export admission",
        })
    }
}

#[derive(Debug)]
struct AdmissionState {
    extent_bytes: u64,
    next_ticket: u64,
    waiting: VecDeque<WaitingAdmission>,
    active: Vec<ActiveAdmission>,
}

impl AdmissionState {
    fn validate(&self, op: AdmissionOp) -> Result<()> {
        let (operation, range) = match op {
            AdmissionOp::Read(range) => ("read", range),
            AdmissionOp::Write(range) => ("write", range),
            AdmissionOp::Flush => return Ok(()),
        };

        let end = range.checked_end().ok_or(ServerError::OutOfBounds {
            operation,
            offset: range.start(),
            length: range.len(),
            size_bytes: self.extent_bytes,
        })?;
        if end > self.extent_bytes {
            return Err(ServerError::OutOfBounds {
                operation,
                offset: range.start(),
                length: range.len(),
                size_bytes: self.extent_bytes,
            });
        }

        Ok(())
    }

    fn next_ticket(&mut self) -> AdmissionTicket {
        let ticket = AdmissionTicket(self.next_ticket);
        self.next_ticket += 1;
        ticket
    }
}

#[derive(Debug)]
struct WaitingAdmission {
    ticket: AdmissionTicket,
    op: AdmissionOp,
    grant: oneshot::Sender<AdmissionPermit>,
}

#[derive(Debug)]
struct ActiveAdmission {
    ticket: AdmissionTicket,
    op: AdmissionOp,
}

fn promote(inner: &Arc<AdmissionInner>, state: &mut AdmissionState) {
    promote_with_inner(inner.clone(), state);
}

fn promote_with_inner(inner: Arc<AdmissionInner>, state: &mut AdmissionState) {
    let mut index = 0;
    while index < state.waiting.len() {
        if !is_admissible(state, index) {
            index += 1;
            continue;
        }

        let waiting = state
            .waiting
            .remove(index)
            .expect("waiting admission index exists");
        if waiting.grant.is_closed() {
            continue;
        }

        state.active.push(ActiveAdmission {
            ticket: waiting.ticket,
            op: waiting.op,
        });
        let permit = AdmissionPermit::new(inner.clone(), waiting.ticket, waiting.op);
        if let Err(mut permit) = waiting.grant.send(permit) {
            permit.disarm();
            state
                .active
                .retain(|active| active.ticket != waiting.ticket);
        }
    }
}

fn is_admissible(state: &AdmissionState, index: usize) -> bool {
    let op = state.waiting[index].op;
    let conflicts_active = state.active.iter().any(|active| active.op.conflicts(op));
    let conflicts_earlier_waiter = state
        .waiting
        .iter()
        .take(index)
        .any(|waiting| waiting.op.conflicts(op));

    !conflicts_active && !conflicts_earlier_waiter
}
