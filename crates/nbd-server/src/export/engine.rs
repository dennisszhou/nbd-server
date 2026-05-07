use super::request::{AdmittedExportRequest, ExportRequest, ExportResult};
use crate::{Result, admission::AdmissionOp};
use std::sync::Arc;

/// Backing-store-specific mapping from export requests to admission operations.
pub trait ExportAdmissionPolicy: Send + Sync {
    fn operation_for(&self, request: &ExportRequest) -> Result<AdmissionOp>;
}

pub type ExportAdmissionPolicyHandle = Arc<dyn ExportAdmissionPolicy>;

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
