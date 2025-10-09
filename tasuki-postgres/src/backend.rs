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
        return;
    }

    #[cfg(not(feature = "rt-tokio"))]
    {
        futures_timer::Delay::new(duration).await;
        return;
    }
}
