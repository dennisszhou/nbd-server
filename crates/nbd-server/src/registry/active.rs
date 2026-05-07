use crate::export::ExportRuntimeHandle;
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT_EXPORT_OWNER_ID: AtomicU64 = AtomicU64::new(1);

/// Active serving owner for one export runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ExportOwner {
    id: ExportOwnerId,
}

impl ExportOwner {
    pub fn unique_connection() -> Self {
        Self {
            id: ExportOwnerId(NEXT_EXPORT_OWNER_ID.fetch_add(1, Ordering::Relaxed)),
        }
    }

    pub fn id(self) -> ExportOwnerId {
        self.id
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ExportOwnerId(u64);

impl ExportOwnerId {
    pub fn raw(self) -> u64 {
        self.0
    }
}

pub(super) enum ActiveExportState {
    Opening { owner: ExportOwner },
    Open(ActiveExport),
    Closing { owner: ExportOwner },
}

pub(super) struct ActiveExport {
    pub(super) owner: ExportOwner,
    pub(super) runtime: ExportRuntimeHandle,
    pub(super) connections: usize,
}
