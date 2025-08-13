use futures::{FutureExt, Stream, StreamExt};
use pin_project_lite::pin_project;
use serde::de::DeserializeOwned;

use crate::queries;

pin_project! {
    struct Ticker {
        #[pin]
        inner: futures_timer::Delay,
        period: std::time::Duration,
    }
}

impl Ticker {
    fn new(period: std::time::Duration) -> Self {
        Self {
            inner: futures_timer::Delay::new(period),
            period,
        }
    }
}

impl Stream for Ticker {
    type Item = ();

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let mut this = self.project();
        let poll = this.inner.as_mut().poll(cx);
        if poll.is_ready() {
            this.inner.reset(*this.period);
        }
        poll.map(Some)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum ErrorKind {
    DataBase,
    Decode,
}

#[derive(Debug)]
struct Error {
    #[allow(unused)]
    kind: ErrorKind,
    inner: Box<dyn std::error::Error + 'static>,
}

impl From<sqlx::Error> for Error {
    fn from(value: sqlx::Error) -> Self {
        Self {
            kind: ErrorKind::DataBase,
            inner: value.into(),
        }
    }
}

impl From<serde_json::Error> for Error {
    fn from(value: serde_json::Error) -> Self {
        Self {
            kind: ErrorKind::Decode,
            inner: value.into(),
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

const LEASE_DURATION: std::time::Duration = std::time::Duration::from_secs(30);
static LEASE_INTERVAL: std::sync::LazyLock<sqlx::postgres::types::PgInterval> =
    std::sync::LazyLock::new(|| LEASE_DURATION.try_into().unwrap());

#[derive(Debug)]
struct OutTxContext {
    id: sqlx::types::Uuid,
    pool: sqlx::PgPool,
}

impl OutTxContext {
    async fn heartbeat(&self) -> Result<(), Error> {
        queries::HeartBeatJob::builder()
            .id(self.id)
            .lease_interval(&LEASE_INTERVAL)
            .build()
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn complete(&self) -> Result<(), Error> {
        queries::CompleteJob::builder()
            .id(self.id)
            .build()
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn cancel(&self) -> Result<(), Error> {
        queries::CancelJob::builder()
            .id(self.id)
            .build()
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn retry(&self, retry_after: Option<std::time::Duration>) -> Result<(), Error> {
        let duration = retry_after.unwrap_or(std::time::Duration::from_secs(15));
        let interval =
            sqlx::postgres::types::PgInterval::try_from(duration).map_err(|e| Error {
                kind: ErrorKind::DataBase,
                inner: e,
            })?;
        queries::RetryJob::builder()
            .id(self.id)
            .interval(&interval)
            .build()
            .execute(&self.pool)
            .await?;

        Ok(())
    }
}

struct Job<T> {
    context: OutTxContext,
    data: T,
}

#[derive(Debug, Clone)]
pub struct BackEnd {
    pool: sqlx::PgPool,
}

impl BackEnd {
    pub fn new(pool: sqlx::PgPool) -> BackEnd {
        BackEnd { pool }
    }

    async fn get_job<T>(&self, batch_size: u16) -> Vec<Result<Job<T>, Error>>
    where
        T: DeserializeOwned,
    {
        let builder = queries::GetAvailableJobs::builder()
            .batch_size(batch_size.into())
            .lease_interval(&LEASE_INTERVAL)
            .build();

        builder
            .query_as()
            .fetch(&self.pool)
            .map(|res| match res {
                Ok(row) => {
                    let data = serde_json::from_value::<T>(row.job_data)?;
                    let context = OutTxContext {
                        id: row.id,
                        pool: self.pool.clone(),
                    };
                    Ok(Job { context, data })
                }
                Err(e) => Err(Error::from(e)),
            })
            .collect::<Vec<_>>()
            .await
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum JobResult {
    Complete,
    Retry(Option<std::time::Duration>),
    Cancel,
}

pub trait JobHandler<T, S>: Send + Sync + Clone + 'static {
    type Future: Future<Output = JobResult> + Send;
    fn call(&self, data: T, context: S) -> Self::Future;
}

#[derive(Debug, Clone)]
pub struct Worker<Tick, F, T> {
    tick: Tick,
    concurrent: usize,
    job_handler: F,
    data_type: std::marker::PhantomData<T>,
}

impl<Tick, F, T> Worker<Tick, F, T>
where
    Tick: Stream,
    F: JobHandler<T, ()>,
    T: DeserializeOwned,
{
    fn data_stream(tick: Tick, backend: &BackEnd) -> impl Stream<Item = Result<Job<T>, Error>> {
        let backend = backend.clone();

        tick.then(move |_| {
            let backend = backend.clone();
            async move { backend.get_job(8).await }
        })
        .flat_map(futures::stream::iter)
    }

    pub async fn run(self, backend: &BackEnd) {
        let data_stream = Self::data_stream(self.tick, backend);
        let filtered = data_stream.filter_map(|result| async {
            result
                .inspect_err(|error| tracing::error!(error = %error, "Failed to fetch job"))
                .ok()
        });

        let runner = filtered.map(|job| async {
            let Job { context, data } = job;
            tracing::trace!("Start handler");
            let result = {
                let hb_every = LEASE_DURATION / 3;
                let mut ticker = Ticker::new(hb_every).fuse();

                let mut handler_fut = self.job_handler.call(data, ()).boxed().fuse();
                loop {
                    futures::select! {
                        res = handler_fut => break res,
                        _ = ticker.next() =>{
                            let _res = context.heartbeat().await.inspect_err(
                                |error| tracing::error!(error = %error, job_id = %context.id, "Failed to heartbeat job"),
                            );
                        }
                    }
                }
            };
            tracing::trace!("Finish handler");

            let _ =match result {
                JobResult::Complete => {context.complete().await.inspect_err(   |error| tracing::error!(error = %error, "Failed to complete job"))},
                JobResult::Retry(duration) =>  {context.retry(duration).await.inspect_err(|error| tracing::error!(error = %error, "Failed to retry job"))},
                JobResult::Cancel => {context.cancel().await.inspect_err(|error|tracing::error!(error = %error, "Failed to cancel job"))},
            };
        });

        let fut = runner
            .buffer_unordered(self.concurrent)
            .for_each(|_| async {});

        fut.await
    }
}
