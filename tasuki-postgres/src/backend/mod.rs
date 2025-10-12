mod backend_impl;

pub use backend_impl::{BackEnd, Error, ErrorKind, OutTxContext, PostgresDriver};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum JobStatus {
    Pending,
    Running,
    Completed,
    Failed,
    Canceled,
}

impl JobStatus {
    pub(crate) fn as_str(&self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Canceled => "canceled",
        }
    }
}

impl From<crate::queries::TasukiJobStatus> for JobStatus {
    fn from(value: crate::queries::TasukiJobStatus) -> Self {
        match value {
            crate::queries::TasukiJobStatus::Pending => JobStatus::Pending,
            crate::queries::TasukiJobStatus::Running => JobStatus::Running,
            crate::queries::TasukiJobStatus::Completed => JobStatus::Completed,
            crate::queries::TasukiJobStatus::Failed => JobStatus::Failed,
            crate::queries::TasukiJobStatus::Canceled => JobStatus::Canceled,
        }
    }
}

impl From<JobStatus> for crate::queries::TasukiJobStatus {
    fn from(value: JobStatus) -> Self {
        match value {
            JobStatus::Pending => crate::queries::TasukiJobStatus::Pending,
            JobStatus::Running => crate::queries::TasukiJobStatus::Running,
            JobStatus::Completed => crate::queries::TasukiJobStatus::Completed,
            JobStatus::Failed => crate::queries::TasukiJobStatus::Failed,
            JobStatus::Canceled => crate::queries::TasukiJobStatus::Canceled,
        }
    }
}

pub(crate) fn exponential_backoff(
    base_delay: std::time::Duration,
    retries: u32,
) -> std::time::Duration {
    let pow = 2u32.pow(retries);

    base_delay.saturating_mul(pow)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct LostLeaseError;

impl std::fmt::Display for LostLeaseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("lost lease for job")
    }
}

impl std::error::Error for LostLeaseError {}

pub(crate) async fn sleep(duration: std::time::Duration) {
    #[cfg(feature = "rt-tokio")]
    {
        tokio::time::sleep(duration).await;
    }

    #[cfg(not(feature = "rt-tokio"))]
    {
        futures_timer::Delay::new(duration).await;
    }
}
