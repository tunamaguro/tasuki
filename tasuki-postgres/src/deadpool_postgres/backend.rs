use crate::backend::JobStatus;
use futures::{StreamExt, TryStreamExt};
use serde::de::DeserializeOwned;
use tasuki_core::{BackEndContext, BackEndDriver, BackEndPoller, Job, backend::HeartbeatStop};

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
                    crate::backend::sleep(backoff).await;
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
                    crate::backend::sleep(backoff).await;
                    continue;
                }
            };

            let status = JobStatus::from(row.status);
            match status {
                JobStatus::Running => {
                    retry_count = 0;
                    crate::backend::sleep(interval).await;
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

#[derive(Debug, Clone)]
pub struct BackEnd<T> {
    pool: deadpool_postgres::Pool,
    queue_name: std::borrow::Cow<'static, str>,
    lease_time: std::time::Duration,
    marker: std::marker::PhantomData<fn() -> T>,
}

impl<T> BackEnd<T>
where
    T: DeserializeOwned + Send + 'static,
{
    async fn poll_job_inner(
        &mut self,
        batch_size: usize,
    ) -> Result<Vec<Result<Job<T, OutTxContext>, Error>>, Error> {
        const DEFAULT_LEASE_TIME: crate::PgInterval = crate::PgInterval {
            microseconds: 30 * 1000 * 1000,
            days: 0,
            months: 0,
        };
        let lease_interval =
            crate::PgInterval::try_from(self.lease_time).unwrap_or(DEFAULT_LEASE_TIME);
        let builder = queries::GetAvailableJobs::builder()
            .lease_interval(lease_interval)
            .queue_name(&self.queue_name)
            .batch_size(i32::try_from(batch_size).unwrap_or(32))
            .build();

        let client = self.pool.get().await?;
        let stmt = client
            .prepare_cached(queries::GetAvailableJobs::QUERY)
            .await?;
        let st = client.query_raw(&stmt, builder.as_slice()).await?;
        let row_st = st
            .map_ok(|row| queries::GetAvailableJobsRow::from_row(&row))
            .map(|res| res.flatten().map_err(Error::from));

        let result = row_st
            .map(|row| {
                let row = row?;
                let data = serde_json::from_value::<T>(row.job_data)?;
                let context = OutTxContext {
                    id: row.id,
                    pool: self.pool.clone(),
                    lease_token: row.lease_token,
                    interval: self.lease_time,
                    lease_interval,
                };
                Ok::<_, Error>(Job::from_parts(data, context))
            })
            .collect::<Vec<_>>()
            .await;

        Ok(result)
    }
}

impl<T> BackEndPoller for BackEnd<T>
where
    T: DeserializeOwned + Send + 'static,
{
    type Driver = DeadPoolPostgresDriver;
    type Data = T;
    type Context = OutTxContext;
    async fn poll_job(
        &mut self,
        batch_size: usize,
    ) -> Vec<Result<Job<Self::Data, Self::Context>, <Self::Driver as BackEndDriver>::Error>> {
        match self.poll_job_inner(batch_size).await {
            Ok(v) => v,
            Err(e) => vec![Err(e)],
        }
    }
}
