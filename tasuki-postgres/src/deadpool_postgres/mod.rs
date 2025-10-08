mod backend;
#[allow(unused, clippy::manual_async_fn)]
mod queries;

pub use backend::{DeadPoolPostgresDriver, Error, ErrorKind, OutTxContext};

impl From<queries::TasukiJobStatus> for crate::backend::JobStatus {
    fn from(value: queries::TasukiJobStatus) -> Self {
        match value {
            queries::TasukiJobStatus::Pending => crate::backend::JobStatus::Pending,
            queries::TasukiJobStatus::Running => crate::backend::JobStatus::Running,
            queries::TasukiJobStatus::Completed => crate::backend::JobStatus::Completed,
            queries::TasukiJobStatus::Failed => crate::backend::JobStatus::Failed,
            queries::TasukiJobStatus::Canceled => crate::backend::JobStatus::Canceled,
        }
    }
}

impl From<crate::backend::JobStatus> for queries::TasukiJobStatus {
    fn from(value: crate::backend::JobStatus) -> Self {
        match value {
            crate::backend::JobStatus::Pending => queries::TasukiJobStatus::Pending,
            crate::backend::JobStatus::Running => queries::TasukiJobStatus::Running,
            crate::backend::JobStatus::Completed => queries::TasukiJobStatus::Completed,
            crate::backend::JobStatus::Failed => queries::TasukiJobStatus::Failed,
            crate::backend::JobStatus::Canceled => queries::TasukiJobStatus::Canceled,
        }
    }
}
