use super::queries;

pub struct InsertJob<T> {
    data: T,
    max_attempts: u16,
    delay: std::time::Duration,
}

impl<T> InsertJob<T> {
    const DEFAULT_MAX_ATTEMPTS: u16 = 25;

    pub const fn new(data: T) -> Self {
        Self {
            data,
            max_attempts: Self::DEFAULT_MAX_ATTEMPTS,
            delay: std::time::Duration::from_secs(0),
        }
    }

    pub fn max_attempts(self, max_attempts: u16) -> Self {
        Self {
            max_attempts,
            ..self
        }
    }

    pub fn delay(self, delay: std::time::Duration) -> Self {
        Self { delay, ..self }
    }

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
pub enum ErrorKind {
    DataBase,
    Pool,
    Encode,
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
            kind: ErrorKind::Encode,
            inner: Box::new(value),
        }
    }
}

#[derive(Debug)]
pub struct Client<T> {
    pool: deadpool_postgres::Pool,
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
    pub fn new(pool: deadpool_postgres::Pool) -> Self {
        Self {
            pool,
            queue_name: crate::DEFAULT_QUEUE_NAME.into(),
            data_type: std::marker::PhantomData,
        }
    }

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
    T: serde::Serialize + Sync,
{
    async fn notify<C: deadpool_postgres::GenericClient>(&self, client: &C) -> Result<(), Error> {
        queries::AddJobNotify::builder()
            .queue_name(&self.queue_name)
            .channel_name(crate::NOTIFY_CHANNEL_NAME)
            .build()
            .execute(client)
            .await?;
        Ok(())
    }

    pub async fn insert(&self, job: InsertJob<T>) -> Result<(), Error> {
        let client = self.pool.get().await?;
        self.insert_tx(job, &client).await
    }

    pub async fn insert_tx<C>(&self, job: InsertJob<T>, client: &C) -> Result<(), Error>
    where
        C: deadpool_postgres::GenericClient,
    {
        let value = serde_json::to_value(&job.data)?;
        let delay = crate::PgInterval::try_from(job.delay).map_err(|_| Error {
            kind: ErrorKind::Encode,
            inner: Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "delay is too large",
            )),
        })?;

        queries::InsertJobOne::builder()
            .job_data(&value)
            .max_attempts(job.max_attempts.into())
            .queue_name(&self.queue_name)
            .interval(delay)
            .build()
            .execute(client)
            .await?;

        self.notify(client).await?;

        Ok(())
    }

    async fn insert_jobs_copy_in<'job, I>(
        &self,
        jobs: I,
        writer: tokio_postgres::binary_copy::BinaryCopyInWriter,
    ) -> Result<(), Error>
    where
        I: IntoIterator<Item = &'job InsertJob<T>> + Send,
        I::IntoIter: Send,
        T: 'job,
    {
        let now = std::time::SystemTime::now();

        futures::pin_mut!(writer);
        for job in jobs {
            let value = serde_json::to_value(&job.data)?;
            let scheduled_at = now + job.delay;
            let q = queries::InsertJobMany::builder()
                .job_data(&value)
                .max_attempts(job.max_attempts.into())
                .queue_name(&self.queue_name)
                .scheduled_at(&scheduled_at)
                .build();
            writer.as_mut().write(&q.as_slice()).await?;
        }

        writer.finish().await?;
        Ok(())
    }

    const QUERY_TYPES: &[tokio_postgres::types::Type] = &[
        tokio_postgres::types::Type::INT4,
        tokio_postgres::types::Type::JSONB,
        tokio_postgres::types::Type::TEXT,
        tokio_postgres::types::Type::TIMESTAMPTZ,
    ];

    pub async fn insert_batch<'job, I>(&self, jobs: I) -> Result<(), Error>
    where
        I: IntoIterator<Item = &'job InsertJob<T>> + Send,
        I::IntoIter: Send,
        T: 'static,
    {
        let client = self.pool.get().await?;

        let sink = client.copy_in(queries::InsertJobMany::QUERY).await?;
        let writer = tokio_postgres::binary_copy::BinaryCopyInWriter::new(sink, Self::QUERY_TYPES);
        self.insert_jobs_copy_in(jobs, writer).await?;

        self.notify(&client).await?;
        Ok(())
    }

    pub async fn insert_batch_tx<'job, I, C>(
        &self,
        jobs: I,
        tx: &deadpool_postgres::Transaction<'_>,
    ) -> Result<(), Error>
    where
        I: IntoIterator<Item = &'job InsertJob<T>> + Send,
        I::IntoIter: Send,
        C: deadpool_postgres::GenericClient,
        T: 'job,
    {
        let sink = tx.copy_in(queries::InsertJobMany::QUERY).await?;
        let writer = tokio_postgres::binary_copy::BinaryCopyInWriter::new(sink, Self::QUERY_TYPES);
        self.insert_jobs_copy_in(jobs, writer).await?;

        self.notify(tx).await?;
        Ok(())
    }
}
