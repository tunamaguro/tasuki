//! Client utilities for enqueuing jobs into the Tasuki queue.

use serde::Serialize;

use crate::queries;

/// Configuration for inserting a job into the queue.
///
/// The generic `T` represents the job payload that will be serialized and
/// stored in the database.  A job can be customised with the number of times it
/// may be retried before being considered failed.
pub struct InsertJob<T> {
    data: T,
    max_attempts: u16,
}

impl<T> InsertJob<T> {
    /// Default maximum number of attempts a job may be retried.
    const DEFAULT_MAX_ATTEMPTS: u16 = 25;

    /// Create a new `InsertJob` wrapping the provided payload.
    pub const fn new(data: T) -> Self {
        Self {
            data,
            max_attempts: Self::DEFAULT_MAX_ATTEMPTS,
        }
    }

    /// Set how many times the job may be retried.
    pub fn max_attempts(self, max_attempts: u16) -> Self {
        Self {
            max_attempts,
            ..self
        }
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
    DataBase,
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
            kind: ErrorKind::Encode,
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

#[derive(Debug)]
/// A handle used to enqueue jobs into a PostgreSQL-backed queue.
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
            queue_name: super::TASUKI_DEFAULT_QUEUE_NAME.into(),
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
}

impl<T> Client<T>
where
    T: Serialize + Send + Sync + 'static,
{
    /// Insert a job into the queue using the client's connection pool.
    pub async fn insert(&self, data: InsertJob<T>) -> Result<(), Error> {
        let mut tx = self.pool.begin().await?;
        self.insert_tx(data, &mut tx).await?;
        tx.commit().await?;

        Ok(())
    }

    /// Insert a job using an existing transaction or connection.
    #[allow(clippy::manual_async_fn)]
    pub fn insert_tx<'a, 'c, A>(
        &self,
        data: InsertJob<T>,
        tx: A,
    ) -> impl Future<Output = Result<(), Error>>
    where
        A: sqlx::Acquire<'c, Database = sqlx::Postgres> + Send + 'a,
    {
        async move {
            let value = serde_json::to_value(data.data)?;

            let mut conn = tx.acquire().await?;

            queries::InsertJobOne::builder()
                .job_data(&value)
                .max_attempts(data.max_attempts.into())
                .queue_name(&self.queue_name)
                .build()
                .execute(&mut *conn)
                .await?;

            queries::AddJobNotify::builder()
                .queue_name(&self.queue_name)
                .channel_name(crate::worker::Listener::CHANNEL_NAME)
                .build()
                .execute(&mut *conn)
                .await?;

            Ok(())
        }
    }

    pub async fn insert_batch<I>(&self, data: I) -> Result<(), Error>
    where
        I: IntoIterator<Item = InsertJob<T>>,
    {
        let mut tx = self.pool.begin().await?;
        self.insert_batch_tx(data, &mut tx).await?;
        tx.commit().await?;

        Ok(())
    }

    pub fn insert_batch_tx<'a, 'c, A, I>(
        &self,
        data: I,
        tx: A,
    ) -> impl Future<Output = Result<(), Error>>
    where
        A: sqlx::Acquire<'c, Database = sqlx::Postgres> + Send + 'a,
        I: IntoIterator<Item = InsertJob<T>>,
    {
        async move {
            let mut conn = tx.acquire().await?;
            {
                let mut sink = queries::InsertJobMany::copy_in_tx(&mut conn).await?;
                for job in data {
                    let value = serde_json::to_value(job.data)?;
                    queries::InsertJobMany::builder()
                        .job_data(&value)
                        .max_attempts(job.max_attempts.into())
                        .queue_name(&self.queue_name)
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
                .channel_name(crate::worker::Listener::CHANNEL_NAME)
                .build()
                .execute(&mut *conn)
                .await?;

            Ok(())
        }
    }
}
