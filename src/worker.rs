use futures::{FutureExt, Stream, StreamExt};
use pin_project_lite::pin_project;
use serde::de::DeserializeOwned;

use crate::queries;

pin_project! {
    pub struct Ticker {
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
    inner: Box<dyn std::error::Error + Send + 'static>,
}

impl From<sqlx::Error> for Error {
    fn from(value: sqlx::Error) -> Self {
        Self {
            kind: ErrorKind::DataBase,
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
    queue_name: std::borrow::Cow<'static, str>,
}

impl BackEnd {
    pub fn new(pool: sqlx::PgPool) -> BackEnd {
        BackEnd {
            pool,
            queue_name: super::TASUKI_DEFAULT_QUEUE_NAME.into(),
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

    async fn get_job<T>(&self, batch_size: u16) -> Vec<Result<Job<T>, Error>>
    where
        T: DeserializeOwned,
    {
        let builder = queries::GetAvailableJobs::builder()
            .batch_size(batch_size.into())
            .lease_interval(&LEASE_INTERVAL)
            .queue_name(&self.queue_name)
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

pub trait JobHandler<T, S, M>: Send + Sync + Clone + 'static {
    type Future: Future<Output = JobResult> + Send;
    fn call(self, data: T, context: S) -> Self::Future;
}

pub struct JobData<T>(pub T);
pub struct WorkerContext<S>(pub S);

impl<F, Fut, T, S> JobHandler<T, S, ()> for F
where
    F: FnOnce() -> Fut + Clone + Send + Sync + 'static,
    Fut: Future<Output = JobResult> + Send,
{
    type Future = Fut;

    fn call(self, _data: T, _context: S) -> Self::Future {
        self()
    }
}

impl<F, Fut, T, S> JobHandler<T, S, JobData<T>> for F
where
    F: FnOnce(JobData<T>) -> Fut + Clone + Send + Sync + 'static,
    Fut: Future<Output = JobResult> + Send,
{
    type Future = Fut;

    fn call(self, data: T, _context: S) -> Self::Future {
        self(JobData(data))
    }
}

impl<F, Fut, T, S> JobHandler<T, S, WorkerContext<S>> for F
where
    F: FnOnce(WorkerContext<S>) -> Fut + Clone + Send + Sync + 'static,
    Fut: Future<Output = JobResult> + Send,
{
    type Future = Fut;

    fn call(self, _data: T, context: S) -> Self::Future {
        self(WorkerContext(context))
    }
}

impl<F, Fut, T, S> JobHandler<T, S, (JobData<T>, WorkerContext<S>)> for F
where
    F: FnOnce(JobData<T>, WorkerContext<S>) -> Fut + Clone + Send + Sync + 'static,
    Fut: Future<Output = JobResult> + Send,
{
    type Future = Fut;

    fn call(self, data: T, context: S) -> Self::Future {
        self(JobData(data), WorkerContext(context))
    }
}

#[derive(Debug, Clone)]
pub struct Worker<Tick, F, Ctx, T, M> {
    tick: Tick,
    concurrent: usize,
    job_handler: F,
    context: Ctx,
    data_type: std::marker::PhantomData<(T, M)>,
}

impl<Tick, F, Ctx, T, M> Worker<Tick, F, Ctx, T, M>
where
    Tick: Stream,
    F: JobHandler<T, Ctx, M>,
    Ctx: Clone,
    T: DeserializeOwned,
{
    fn data_stream(tick: Tick, backend: BackEnd) -> impl Stream<Item = Result<Job<T>, Error>> {
        let backend = backend.clone();

        tick.then(move |_| {
            let backend = backend.clone();
            async move { backend.get_job(8).await }
        })
        .flat_map(futures::stream::iter)
    }

    pub async fn run(self, backend: BackEnd) {
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

                let mut handler_fut = self.job_handler.clone().call(data, self.context.clone()).boxed().fuse();
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

pub struct WorkerBuilder<Tick = Ticker, Ctx = ()> {
    tick: Tick,
    context: Ctx,
    concurrent: usize,
}

impl Default for WorkerBuilder<Ticker, ()> {
    fn default() -> Self {
        Self::new()
    }
}

impl WorkerBuilder<Ticker, ()> {
    pub fn new() -> Self {
        let ticker = Ticker::new(std::time::Duration::from_secs(1));
        WorkerBuilder {
            tick: ticker,
            context: (),
            concurrent: 8,
        }
    }
}

impl<Tick, Ctx> WorkerBuilder<Tick, Ctx> {
    pub fn tick<Tick2>(self, tick: Tick2) -> WorkerBuilder<Tick2, Ctx>
    where
        Tick2: Stream,
    {
        WorkerBuilder {
            tick,
            context: self.context,
            concurrent: self.concurrent,
        }
    }

    pub fn context<Ctx2>(self, context: Ctx2) -> WorkerBuilder<Tick, Ctx2>
    where
        Ctx2: Clone,
    {
        WorkerBuilder {
            context,
            tick: self.tick,
            concurrent: self.concurrent,
        }
    }

    pub fn concurrent(self, concurrent: usize) -> Self {
        Self { concurrent, ..self }
    }
}

impl<Ctx> WorkerBuilder<Ticker, Ctx> {
    pub fn polling_interval(self, period: std::time::Duration) -> Self {
        let ticker = Ticker::new(period);
        WorkerBuilder {
            tick: ticker,
            ..self
        }
    }
}

impl<Tick, Ctx> WorkerBuilder<Tick, Ctx>
where
    Tick: Stream,
    Ctx: Clone,
{
    pub fn build_noarg<F>(self, f: F) -> Worker<Tick, F, Ctx, (), ()>
    where
        F: JobHandler<(), Ctx, ()>,
    {
        Worker {
            tick: self.tick,
            concurrent: self.concurrent,
            job_handler: f,
            context: self.context,
            data_type: std::marker::PhantomData,
        }
    }

    pub fn build_with_data<F, T>(self, f: F) -> Worker<Tick, F, Ctx, T, JobData<T>>
    where
        F: JobHandler<T, Ctx, JobData<T>>,
        T: DeserializeOwned + 'static,
    {
        Worker {
            tick: self.tick,
            concurrent: self.concurrent,
            job_handler: f,
            context: self.context,
            data_type: std::marker::PhantomData,
        }
    }

    pub fn build_with_ctx<F, T>(self, f: F) -> Worker<Tick, F, Ctx, T, Ctx>
    where
        F: JobHandler<T, Ctx, WorkerContext<Ctx>>,
        T: DeserializeOwned + 'static,
    {
        Worker {
            tick: self.tick,
            concurrent: self.concurrent,
            job_handler: f,
            context: self.context,
            data_type: std::marker::PhantomData,
        }
    }

    pub fn build_with_both<F, T>(self, f: F) -> Worker<Tick, F, Ctx, T, (JobData<T>, Ctx)>
    where
        F: JobHandler<T, Ctx, (JobData<T>, WorkerContext<Ctx>)>,
        T: DeserializeOwned + 'static,
    {
        Worker {
            tick: self.tick,
            concurrent: self.concurrent,
            job_handler: f,
            context: self.context,
            data_type: std::marker::PhantomData,
        }
    }
}
