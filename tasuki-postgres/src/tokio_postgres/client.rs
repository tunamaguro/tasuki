use super::{ClientAccess, queries};
use crate::{InsertJob, tokio_postgres::client};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum ErrorKind {
    DataBase,
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
impl From<serde_json::Error> for Error {
    fn from(value: serde_json::Error) -> Self {
        Self {
            kind: ErrorKind::Encode,
            inner: Box::new(value),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Client<C, T> {
    client: C,
    queue_name: std::borrow::Cow<'static, str>,
    marker: std::marker::PhantomData<fn() -> T>,
}

impl<C, T> Client<C, T>
where
    C: ClientAccess,
{
    pub fn new(client: C) -> Self {
        Self {
            client,
            queue_name: crate::DEFAULT_QUEUE_NAME.into(),
            marker: std::marker::PhantomData,
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

impl<C, T> Client<C, T>
where
    C: ClientAccess,
    T: serde::Serialize,
{
    async fn notify<U>(&self, client: &U) -> Result<(), Error>
    where
        U: tokio_postgres::GenericClient,
    {
        queries::AddJobNotify::builder()
            .queue_name(&self.queue_name)
            .channel_name(crate::NOTIFY_CHANNEL_NAME)
            .build()
            .execute(client)
            .await?;
        Ok(())
    }

    pub async fn insert(&self, job: &InsertJob<T>) -> Result<(), Error> {
        let client = self.client.client().await;
        self.insert_tx(job, &*client).await
    }

    pub async fn insert_tx<U>(&self, job: &InsertJob<T>, client: &U) -> Result<(), Error>
    where
        U: tokio_postgres::GenericClient,
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

    async fn insert_jobs_copy_in<I, Job>(
        &self,
        jobs: I,
        writer: tokio_postgres::binary_copy::BinaryCopyInWriter,
    ) -> Result<(), Error>
    where
        I: IntoIterator<Item = Job> + Send,
        I::IntoIter: Send,
        Job: AsRef<InsertJob<T>> + Send,
    {
        let now = std::time::SystemTime::now();

        futures::pin_mut!(writer);
        for job in jobs {
            let job = job.as_ref();
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

    pub async fn insert_batch<I, Job>(&self, jobs: I) -> Result<(), Error>
    where
        I: IntoIterator<Item = Job> + Send,
        I::IntoIter: Send,
        Job: AsRef<InsertJob<T>> + Send,
    {
        let client = self.client.client().await;
        let sink = client.copy_in(queries::InsertJobMany::QUERY).await?;
        let writer = tokio_postgres::binary_copy::BinaryCopyInWriter::new(sink, Self::QUERY_TYPES);
        self.insert_jobs_copy_in(jobs, writer).await?;

        self.notify(&*client).await?;
        Ok(())
    }

    pub async fn insert_batch_tx<I, Job>(
        &self,
        jobs: I,
        tx: &tokio_postgres::Transaction<'_>,
    ) -> Result<(), Error>
    where
        I: IntoIterator<Item = Job> + Send,
        I::IntoIter: Send,
        Job: AsRef<InsertJob<T>> + Send,
    {
        let sink = tx.copy_in(queries::InsertJobMany::QUERY).await?;
        let writer = tokio_postgres::binary_copy::BinaryCopyInWriter::new(sink, Self::QUERY_TYPES);
        self.insert_jobs_copy_in(jobs, writer).await?;

        self.notify(tx).await?;
        Ok(())
    }
}
