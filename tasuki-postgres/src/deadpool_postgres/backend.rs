use tasuki_core::{BackEndContext, BackEndDriver, backend::HeartbeatStop};

use crate::backend::JobStatus;

use super::queries;

pub struct DeadPoolPostgresDriver;
impl BackEndDriver for DeadPoolPostgresDriver {
    type Error = Error;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum ErrorKind {
    DataBase,
    Pool,
    Decode,
    LostLease,
}

#[derive(Debug)]
pub struct Error {
    kind: ErrorKind,
    inner: Box<dyn std::error::Error + Send + 'static>,
}

impl Error {
    pub fn kind(&self) -> ErrorKind {
        self.kind
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.inner.fmt(f)
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.inner.as_ref())
    }
}

impl From<tokio_postgres::Error> for Error {
    fn from(value: tokio_postgres::Error) -> Self {
        Self {
            kind: ErrorKind::DataBase,
            inner: Box::new(value),
        }
    }
}

impl From<deadpool_postgres::PoolError> for Error {
    fn from(value: deadpool_postgres::PoolError) -> Self {
        Self {
            kind: ErrorKind::Pool,
            inner: Box::new(value),
        }
    }
}

impl From<serde_json::Error> for Error {
    fn from(value: serde_json::Error) -> Self {
        Self {
            kind: ErrorKind::Decode,
            inner: Box::new(value),
        }
    }
}

#[derive(Debug)]
pub struct OutTxContext {
    id: uuid::Uuid,
    pool: deadpool_postgres::Pool,
    lease_token: uuid::Uuid,
    interval: std::time::Duration,
    lease_interval: crate::PgInterval,
}

impl BackEndContext for OutTxContext {
    type Driver = DeadPoolPostgresDriver;
    async fn heartbeat(&mut self) -> HeartbeatStop {
        const BACKOFF_BASE: std::time::Duration = std::time::Duration::from_millis(100);

        let interval = self.interval / 3;
        let mut retry_count = 0;

        loop {
            let client = match self.pool.get().await {
                Ok(client) => client,
                Err(error) => {
                    retry_count += 1;
                    tracing::warn!(job_id = %self.id, error = %error, retry_count, "failed to get client from pool; backing off");
                    let backoff = crate::backend::exponential_backoff(BACKOFF_BASE, retry_count)
                        .min(interval);
                    tokio::time::sleep(backoff).await;
                    continue;
                }
            };
            let res = queries::HeartBeatJob::builder()
                .lease_interval(self.lease_interval)
                .id(self.id)
                .lease_token(Some(self.lease_token))
                .build()
                .query_opt(&client)
                .await;

            let row = match res {
                Ok(Some(row)) => row,
                Ok(None) => {
                    tracing::error!(job_id = %self.id, lease_token = %self.lease_token, "lost job lease");
                    return HeartbeatStop;
                }
                Err(error) => {
                    retry_count += 1;
                    tracing::warn!(job_id = %self.id, error = %error, retry_count, "cannot heartbeat job; backing off");
                    let backoff = crate::backend::exponential_backoff(BACKOFF_BASE, retry_count)
                        .min(interval);
                    tokio::time::sleep(backoff).await;
                    continue;
                }
            };

            let status = JobStatus::from(row.status);
            match status {
                JobStatus::Running => {
                    retry_count = 0;
                    tokio::time::sleep(interval).await;
                }
                JobStatus::Canceled => {
                    tracing::info!(job_id = %self.id, "job canceled");
                    return HeartbeatStop;
                }
                other => {
                    tracing::error!(job_id = %self.id, status = other.as_str(), "unexpected status. expected 'running'");
                    return HeartbeatStop;
                }
            }
        }
    }

    async fn complete(self) -> Result<(), <Self::Driver as BackEndDriver>::Error> {
        let client = self.pool.get().await?;
        let res = queries::CompleteJob::builder()
            .id(self.id)
            .lease_token(Some(self.lease_token))
            .build()
            .execute(&client)
            .await?;

        if res == 0 {
            return Err(Error {
                kind: ErrorKind::LostLease,
                inner: Box::new(crate::backend::LostLeaseError),
            });
        }
        Ok(())
    }

    async fn cancel(self) -> Result<(), <Self::Driver as BackEndDriver>::Error> {
        let client = self.pool.get().await?;
        let res = queries::CancelJob::builder()
            .id(self.id)
            .lease_token(Some(self.lease_token))
            .build()
            .execute(&client)
            .await?;

        if res == 0 {
            return Err(Error {
                kind: ErrorKind::LostLease,
                inner: Box::new(crate::backend::LostLeaseError),
            });
        }
        Ok(())
    }

    async fn retry(
        self,
        retry_after: Option<std::time::Duration>,
    ) -> Result<(), <Self::Driver as BackEndDriver>::Error> {
        let retry_after = retry_after.and_then(|v| crate::PgInterval::try_from(v).ok());
        let client = self.pool.get().await?;
        let res = queries::RetryJob::builder()
            .interval(retry_after)
            .id(self.id)
            .lease_token(Some(self.lease_token))
            .build()
            .execute(&client)
            .await?;

        if res == 0 {
            return Err(Error {
                kind: ErrorKind::LostLease,
                inner: Box::new(crate::backend::LostLeaseError),
            });
        }
        Ok(())
    }
}
