//! Client utilities for enqueuing jobs into the Tasuki queue.

use serde::Serialize;
use std::future::Future;

use crate::queries;

/// Job status
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobStatus {
    Pending,
    Running,
    Completed,
    Failed,
    Canceled,
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

#[derive(Debug, Clone, PartialEq, Eq)]
/// Minimal job information for client listings.
pub struct JobInfo {
    pub id: sqlx::types::Uuid,
    pub status: JobStatus,
}

#[derive(Debug, Clone)]
/// Options to list jobs belonging to the client's queue.
pub struct ListJobsOptions {
    /// Cursor for keyset pagination. When provided, entries created strictly
    /// before the cursor's creation time are returned.
    pub cursor_job_id: Option<sqlx::types::Uuid>,
    /// Maximum number of rows to return.
    pub page_size: u32,
}

/// Configuration for inserting a job into the queue.
///
/// The generic `T` represents the job payload that will be serialized and
/// stored in the database.  A job can be customised with the number of times it
/// may be retried before being considered failed.
pub struct InsertJob<T> {
    data: T,
    max_attempts: u16,
    /// Delay before the job becomes eligible for execution.
    ///
    /// Defaults to zero (immediate scheduling). If greater than zero, the job
    /// will be scheduled at `now() + delay` in the database.
    delay: std::time::Duration,
}

impl<T> InsertJob<T> {
    /// Default maximum number of attempts a job may be retried.
    const DEFAULT_MAX_ATTEMPTS: u16 = 25;

    /// Create a new `InsertJob` wrapping the provided payload.
    pub const fn new(data: T) -> Self {
        Self {
            data,
            max_attempts: Self::DEFAULT_MAX_ATTEMPTS,
            delay: std::time::Duration::from_secs(0),
        }
    }

    /// Set how many times the job may be retried.
    pub fn max_attempts(self, max_attempts: u16) -> Self {
        Self {
            max_attempts,
            ..self
        }
    }

    /// Delay the job's execution by the provided duration.
    pub fn delay(self, delay: std::time::Duration) -> Self {
        Self { delay, ..self }
    }

    /// Extract the wrapped job payload.
    pub fn into_inner(self) -> T {
        self.data
    }
}

impl<T> From<T> for InsertJob<T> {
    fn from(value: T) -> Self {
        InsertJob::new(value)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
/// Categories of errors that can occur when inserting a job.
pub enum ErrorKind {
    /// An error was returned by the database layer.
    Database,
    /// Serialization of the job data failed.
    Encode,
}

#[derive(Debug)]
/// Error type returned by [`Client`] operations.
pub struct Error {
    kind: ErrorKind,
    inner: Box<dyn std::error::Error + Send + 'static>,
}

impl Error {
    fn new_database(error: Box<dyn std::error::Error + Send + 'static>) -> Self {
        Error {
            kind: ErrorKind::Database,
            inner: error,
        }
    }

    /// Return the category of this error.
    pub fn kind(&self) -> ErrorKind {
        self.kind
    }
}

impl From<sqlx::Error> for Error {
    fn from(value: sqlx::Error) -> Self {
        Self::new_database(Box::new(value))
    }
}

impl From<serde_json::Error> for Error {
    fn from(value: serde_json::Error) -> Self {
        Self {
            kind: ErrorKind::Encode,
            inner: Box::new(value),
        }
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

/// A handle used to enqueue jobs into a PostgreSQL-backed queue.
#[derive(Debug)]
pub struct Client<T> {
    pool: sqlx::PgPool,
    queue_name: std::borrow::Cow<'static, str>,
    data_type: std::marker::PhantomData<T>,
}

impl<T> Clone for Client<T> {
    fn clone(&self) -> Self {
        Self {
            pool: self.pool.clone(),
            queue_name: self.queue_name.clone(),
            data_type: std::marker::PhantomData,
        }
    }
}

impl<T> Client<T> {
    /// Create a new client bound to the given connection pool.
    pub fn new(pool: sqlx::PgPool) -> Self {
        Self {
            pool,
            queue_name: super::DEFAULT_QUEUE_NAME.into(),
            data_type: std::marker::PhantomData,
        }
    }

    /// Specify the queue name used when inserting jobs.
    pub fn queue_name<S>(self, queue_name: S) -> Self
    where
        S: Into<std::borrow::Cow<'static, str>>,
    {
        Self {
            queue_name: queue_name.into(),
            ..self
        }
    }

    /// Aggregate job counts for the client's queue name.
    ///
    /// Returns counts by status. If the queue has no rows yet, zeros are returned.
    pub async fn aggregate_status(&self) -> Result<QueueStats, Error> {
        let r = queries::AggregateQueueStat::builder()
            .queue_name(self.queue_name.as_ref())
            .build()
            .query_one(&self.pool)
            .await?;

        let stats = QueueStats {
            queue_name: self.queue_name.to_string(),
            pending: r.pending,
            running: r.running,
            completed: r.completed,
            failed: r.failed,
            canceled: r.canceled,
        };

        Ok(stats)
    }

    /// List jobs for the configured queue using keyset pagination.
    ///
    /// Returns at most `page_size` records ordered by `created_at DESC, id DESC`.
    pub async fn list_jobs(&self, opts: &ListJobsOptions) -> Result<Vec<JobInfo>, Error> {
        // Convert page size to i32, clamping on overflow.
        let page_size = i32::try_from(opts.page_size).unwrap_or(i32::MAX);

        let rows = queries::ListJobs::builder()
            .cursor_job_id(opts.cursor_job_id)
            .queue_name(Some(self.queue_name.as_ref()))
            .page_size(page_size)
            .build()
            .query_many(&self.pool)
            .await?;

        let jobs = rows
            .into_iter()
            .map(|r| JobInfo {
                id: r.id,
                status: JobStatus::from(r.status),
            })
            .collect();

        Ok(jobs)
    }
}

impl<T> Client<T>
where
    T: Serialize + Sync,
{
    /// Insert a job into the queue using the client's connection pool.
    pub async fn insert(&self, data: &InsertJob<T>) -> Result<(), Error> {
        self.insert_tx(data, &self.pool).await?;

        Ok(())
    }

    /// Insert a job using an existing transaction or connection.
    #[allow(clippy::manual_async_fn)]
    pub fn insert_tx<'a, 'c, 'data, A>(
        &self,
        data: &'data InsertJob<T>,
        tx: A,
    ) -> impl Future<Output = Result<(), Error>> + Send
    where
        A: sqlx::Acquire<'c, Database = sqlx::Postgres> + Send + 'a,
    {
        async move {
            let value = serde_json::to_value(&data.data)?;

            let mut conn = tx.acquire().await?;

            let interval =
                sqlx::postgres::types::PgInterval::try_from(data.delay).map_err(|e| Error {
                    kind: ErrorKind::Database,
                    inner: e,
                })?;

            queries::InsertJobOne::builder()
                .job_data(&value)
                .max_attempts(data.max_attempts.into())
                .queue_name(&self.queue_name)
                .interval(&interval)
                .build()
                .execute(&mut *conn)
                .await?;

            queries::AddJobNotify::builder()
                .queue_name(&self.queue_name)
                .channel_name(crate::NOTIFY_CHANNEL_NAME)
                .build()
                .execute(&mut *conn)
                .await?;

            Ok(())
        }
    }

    /// Insert multiple jobs into the queue using the client's connection pool.
    ///
    /// This method begins and commits its own transaction internally. For
    /// batching within an existing transaction, use `insert_batch_tx`.
    pub async fn insert_batch<'job, I>(&self, data: I) -> Result<(), Error>
    where
        I: IntoIterator<Item = &'job InsertJob<T>> + Send,
        I::IntoIter: Send,
        T: 'job,
    {
        let mut tx = self.pool.begin().await?;
        self.insert_batch_tx(data, &mut tx).await?;
        tx.commit().await?;

        Ok(())
    }

    /// Insert multiple jobs using an existing transaction or connection.
    ///
    /// - `data`: Iterator of job descriptors to enqueue.
    /// - `tx`: A transaction/connection that implements `sqlx::Acquire`.
    ///
    /// Uses PostgreSQL COPY for efficient bulk insert and emits a single NOTIFY
    /// after all rows are inserted to wake listeners.
    #[allow(clippy::manual_async_fn)]
    pub fn insert_batch_tx<'a, 'c, 'job, A, I>(
        &self,
        data: I,
        tx: A,
    ) -> impl Future<Output = Result<(), Error>> + Send
    where
        A: sqlx::Acquire<'c, Database = sqlx::Postgres> + Send + 'a,
        I: IntoIterator<Item = &'job InsertJob<T>> + Send,
        I::IntoIter: Send,
        T: 'job,
    {
        async move {
            let mut conn = tx.acquire().await?;
            {
                let now = std::time::SystemTime::now();
                let mut sink = queries::InsertJobMany::copy_in_tx(&mut conn).await?;
                for job in data {
                    let value = serde_json::to_value(&job.data)?;
                    let scheduled_at = crate::PgDateTime(now + job.delay);
                    queries::InsertJobMany::builder()
                        .job_data(&value)
                        .max_attempts(job.max_attempts.into())
                        .queue_name(&self.queue_name)
                        .scheduled_at(scheduled_at)
                        .build()
                        .write(&mut sink)
                        .await
                        .map_err(|error| Error::new_database(error))?;
                }
                sink.finish()
                    .await
                    .map_err(|error| Error::new_database(error))?;
            }

            queries::AddJobNotify::builder()
                .queue_name(&self.queue_name)
                .channel_name(crate::NOTIFY_CHANNEL_NAME)
                .build()
                .execute(&mut *conn)
                .await?;

            Ok(())
        }
    }

    /// Cancel a job by `id` regardless of its status.
    ///
    /// This is intended for administrative use (no lease token required).
    pub async fn cancel(&self, id: sqlx::types::Uuid) -> Result<(), Error> {
        self.cancel_tx(id, &self.pool).await?;
        Ok(())
    }

    /// Cancel a job using an existing transaction or connection (admin).
    #[allow(clippy::manual_async_fn)]
    pub fn cancel_tx<'a, 'c, A>(
        &self,
        id: sqlx::types::Uuid,
        tx: A,
    ) -> impl Future<Output = Result<(), Error>> + Send + 'a
    where
        A: sqlx::Acquire<'c, Database = sqlx::Postgres> + Send + 'a,
    {
        async move {
            let mut conn = tx.acquire().await?;
            // Idempotent admin cancel: no error if job is already finalized
            // (0 rows affected). It ensures job ends up in `canceled` state.
            let _ = queries::CancelJobById::builder()
                .id(id)
                .build()
                .execute(&mut *conn)
                .await?;
            Ok(())
        }
    }
}

#[derive(sqlx::FromRow, Debug, Clone, PartialEq, Eq)]
/// Aggregate counts of jobs by status for a queue.
pub struct QueueStats {
    pub queue_name: String,
    pub pending: i64,
    pub running: i64,
    pub completed: i64,
    pub failed: i64,
    pub canceled: i64,
}
