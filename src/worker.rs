//! Utilities for polling and processing queued jobs.
//!
//! The worker module provides types for fetching jobs from the database
//! and executing them with user supplied handlers.  A [`Worker`] polls a
//! [`BackEnd`] at a fixed interval and drives job handlers to completion
//! while periodically sending heartbeats to maintain job leases.

use futures::{FutureExt, Stream, StreamExt};
use pin_project_lite::pin_project;
use serde::de::DeserializeOwned;

use crate::queries;

pin_project! {
    /// Stream that yields at a fixed period and is used to drive polling or
    /// heartbeat logic within a [`Worker`].
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
/// Categorization of failures that may occur while processing jobs.
pub enum ErrorKind {
    /// Errors originating from database interactions.
    DataBase,
    /// Errors that happen while decoding job payloads.
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
/// Backend for fetching and updating jobs in the database.
///
/// A `BackEnd` wraps a [`sqlx::PgPool`] and the name of the queue to pull
/// jobs from. It is consumed by [`Worker`] to obtain work items.
pub struct BackEnd {
    pool: sqlx::PgPool,
    queue_name: std::borrow::Cow<'static, str>,
}

impl BackEnd {
    /// Create a new backend using the given connection pool.
    pub fn new(pool: sqlx::PgPool) -> BackEnd {
        BackEnd {
            pool,
            queue_name: super::TASUKI_DEFAULT_QUEUE_NAME.into(),
        }
    }

    /// Override the queue name from which jobs are fetched.
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

    fn into_datastream<S, T>(
        self,
        tick: S,
        batch_size: u16,
    ) -> impl Stream<Item = Result<Job<T>, Error>>
    where
        S: Stream,
        T: DeserializeOwned,
    {
        tick.then(move |_| {
            let backend = self.clone();
            async move { backend.get_job::<T>(batch_size).await }
        })
        .flat_map(futures::stream::iter)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
/// Outcome produced by a job handler.
pub enum JobResult {
    /// Mark the job as successfully completed.
    Complete,
    /// Requeue the job after an optional delay.
    Retry(Option<std::time::Duration>),
    /// Cancel the job without retrying.
    Cancel,
}

/// Trait implemented by functions that process a job.
///
/// The `M` type parameter determines which combination of [`JobData`] and
/// [`WorkerContext`] the handler expects. The associated [`Data`] type
/// specifies the payload that the job carries.
pub trait JobHandler<M>: Send + Sync + Clone + 'static {
    /// The job data type handled by this function.
    type Data;
    /// Type of the shared context provided to the handler.
    type Context;

    /// Future returned by the handler.
    type Future: Future<Output = JobResult> + Send;

    /// Invoke the handler with the job data and worker context.
    fn call(self, data: Self::Data, context: Self::Context) -> Self::Future;
}

/// Wrapper passed to handlers that request the job payload.
pub struct JobData<T>(pub T);

/// Wrapper passed to handlers that require access to shared context.
pub struct WorkerContext<S>(pub S);

impl<F, Fut> JobHandler<()> for F
where
    F: FnOnce() -> Fut + Clone + Send + Sync + 'static,
    Fut: Future<Output = JobResult> + Send,
{
    type Data = serde_json::Value;
    type Context = ();
    type Future = Fut;

    fn call(self, _data: Self::Data, _context: Self::Context) -> Self::Future {
        self()
    }
}

impl<F, Fut, T> JobHandler<JobData<T>> for F
where
    F: FnOnce(JobData<T>) -> Fut + Clone + Send + Sync + 'static,
    Fut: Future<Output = JobResult> + Send,
{
    type Data = T;
    type Context = ();
    type Future = Fut;

    fn call(self, data: Self::Data, _context: Self::Context) -> Self::Future {
        self(JobData(data))
    }
}

impl<F, Fut, S> JobHandler<WorkerContext<S>> for F
where
    F: FnOnce(WorkerContext<S>) -> Fut + Clone + Send + Sync + 'static,
    Fut: Future<Output = JobResult> + Send,
{
    type Data = serde_json::Value;
    type Context = S;
    type Future = Fut;

    fn call(self, _data: Self::Data, context: Self::Context) -> Self::Future {
        self(WorkerContext(context))
    }
}

impl<F, Fut, T, S> JobHandler<(JobData<T>, WorkerContext<S>)> for F
where
    F: FnOnce(JobData<T>, WorkerContext<S>) -> Fut + Clone + Send + Sync + 'static,
    Fut: Future<Output = JobResult> + Send,
{
    type Data = T;
    type Context = S;
    type Future = Fut;

    fn call(self, data: Self::Data, context: Self::Context) -> Self::Future {
        self(JobData(data), WorkerContext(context))
    }
}

#[derive(Debug, Clone)]
/// Polls for jobs and executes them using the provided handler.
///
/// The generic parameters allow customizing the ticker stream (`Tick`),
/// job handler (`F`), shared context (`Ctx`) and marker type (`M`) that
/// specifies which arguments are supplied to the handler.
pub struct Worker<Tick, F, Ctx, M> {
    tick: Tick,
    concurrent: usize,
    job_handler: F,
    context: Ctx,
    _marker: std::marker::PhantomData<M>,
}

impl<Tick, F, Ctx, M> Worker<Tick, F, Ctx, M>
where
    Tick: Stream,
    F: JobHandler<M, Context = Ctx>,
    F::Data: DeserializeOwned + 'static,
    Ctx: Clone,
{
    pub fn with_graceful_shutdown<Signal>(self, signal: Signal) -> Worker<impl Stream, F, Ctx, M>
    where
        Signal: Future,
    {
        Worker {
            tick: self.tick.take_until(signal),
            concurrent: self.concurrent,
            job_handler: self.job_handler,
            context: self.context,
            _marker: self._marker,
        }
    }

    /// Start polling the backend and executing jobs until the stream
    /// terminates.
    pub async fn run(self, backend: BackEnd) {
        let data_stream = backend.into_datastream(self.tick, 8);

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

/// Builder for configuring and constructing [`Worker`] instances.
///
/// By default it polls every second with no shared context and a
/// concurrency limit of eight.
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
    /// Create a new builder with default configuration.
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
    /// Replace the ticker driving the polling loop.
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

    /// Provide shared context that will be passed to every job handler.
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

    /// Set the maximum number of jobs processed concurrently.
    pub fn concurrent(self, concurrent: usize) -> Self {
        Self { concurrent, ..self }
    }
}

impl<Ctx> WorkerBuilder<Ticker, Ctx> {
    /// Adjust how frequently the worker polls for new jobs.
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
    pub fn build<F, M>(self, f: F) -> Worker<Tick, F, Ctx, M>
    where
        F: JobHandler<M, Context = Ctx>,
    {
        Worker {
            tick: self.tick,
            concurrent: self.concurrent,
            job_handler: f,
            context: self.context,
            _marker: std::marker::PhantomData,
        }
    }
}
