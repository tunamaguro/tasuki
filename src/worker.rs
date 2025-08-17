//! Utilities for polling and processing queued jobs.
//!
//! The worker module provides types for fetching jobs from the database
//! and executing them with user supplied handlers.  A [`Worker`] polls a
//! [`BackEnd`] at a fixed interval and drives job handlers to completion
//! while periodically sending heartbeats to maintain job leases.

use futures::{FutureExt, SinkExt, Stream, StreamExt, TryStreamExt};
use pin_project_lite::pin_project;
use serde::{Deserialize, de::DeserializeOwned};

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
pub struct Error {
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

#[derive(Debug)]
pub struct Listener {
    inner: sqlx::postgres::PgListener,
    publishers: std::collections::HashMap<String, Publisher>,
}

#[derive(Debug, Deserialize)]
pub struct ChannelData {
    q: String,
}

impl Listener {
    pub(crate) const CHANNEL_NAME: &str = "tasuki_jobs";
    pub async fn new(pool: sqlx::PgPool) -> Result<Self, sqlx::Error> {
        let mut listener = sqlx::postgres::PgListener::connect_with(&pool).await?;
        listener.listen(Self::CHANNEL_NAME).await?;

        Ok(Self {
            inner: listener,
            publishers: Default::default(),
        })
    }

    pub async fn listen(self) -> Result<(), Error> {
        let mut stream = self.inner.into_stream();
        let mut publisers = self.publishers;

        loop {
            match stream.try_next().await {
                Ok(Some(notification)) => {
                    let payload = notification.payload();
                    let data = serde_json::from_str::<ChannelData>(payload).inspect_err(|error|tracing::error!(error = %error,"Cannot deserialize job notify message"));
                    let Ok(data) = data else {
                        continue;
                    };

                    if let Some(publisher) = publisers.get_mut(&data.q) {
                        let queue_name = publisher.name.as_ref();
                        let _ = publisher.sender.send(()).await.inspect_err(|error|tracing::error!(error = %error, queue_name = queue_name, "Cannot send job notify"));
                    }
                }
                Ok(None) => {
                    break;
                }
                Err(error) => {
                    tracing::error!(error = %error, "Error happen when receive message from channel");
                }
            };
        }

        Ok(())
    }

    fn subscribe(&mut self, queue_name: impl Into<std::borrow::Cow<'static, str>>) -> Subscribe {
        let queue_name = queue_name.into();
        let (tx, rx) = futures::channel::mpsc::channel(32);
        let publisher = Publisher {
            name: queue_name,
            sender: tx,
        };
        self.publishers
            .insert(publisher.name.to_string(), publisher);

        Subscribe { receiver: rx }
    }
}

#[derive(Debug)]
struct Publisher {
    sender: futures::channel::mpsc::Sender<()>,
    name: std::borrow::Cow<'static, str>,
}

pin_project! {
    #[derive(Debug)]
    pub struct Subscribe {
        #[pin]
        receiver: futures::channel::mpsc::Receiver<()>,
    }
}

impl Stream for Subscribe {
    type Item = ();
    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let this = self.project();
        this.receiver.poll_next(cx)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.receiver.size_hint()
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

    #[allow(dead_code)]
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
    backend: BackEnd,
    _marker: std::marker::PhantomData<M>,
}

impl<Tick, F, Ctx, M> Worker<Tick, F, Ctx, M>
where
    Tick: Stream,
    F: JobHandler<M, Context = Ctx>,
    F::Data: DeserializeOwned + 'static,
    Ctx: Clone,
{
    pub fn with_graceful_shutdown<Signal>(
        self,
        signal: Signal,
    ) -> WorkerWithGracefulShutdown<impl Stream, F, Ctx, M, Signal>
    where
        Signal: Future,
    {
        WorkerWithGracefulShutdown {
            tick: self.tick,
            concurrent: self.concurrent,
            job_handler: self.job_handler,
            context: self.context,
            backend: self.backend,
            _marker: self._marker,
            signal,
        }
    }

    pub fn subscribe(self, listener: &mut Listener) -> WorkerWithSubscribe<impl Stream, F, Ctx, M> {
        let subscribe = listener.subscribe(self.backend.queue_name.clone());
        let tick_stream = futures::stream::select(self.tick.map(|_| ()), subscribe);

        WorkerWithSubscribe {
            tick: tick_stream,
            concurrent: self.concurrent,
            job_handler: self.job_handler,
            context: self.context,
            backend: self.backend,
            _marker: self._marker,
        }
    }

    /// Start polling the backend and executing jobs until the stream
    /// terminates.
    pub async fn run(self) {
        run_worker(
            self.tick,
            self.job_handler,
            self.context,
            self.backend,
            self.concurrent,
        )
        .await
    }
}

pub struct WorkerWithSubscribe<Tick, F, Ctx, M> {
    tick: Tick,
    concurrent: usize,
    job_handler: F,
    context: Ctx,
    backend: BackEnd,
    _marker: std::marker::PhantomData<M>,
}

impl<Tick, F, Ctx, M> WorkerWithSubscribe<Tick, F, Ctx, M>
where
    Tick: Stream,
    F: JobHandler<M, Context = Ctx>,
    F::Data: DeserializeOwned + 'static,
    Ctx: Clone,
{
    pub fn with_graceful_shutdown<Signal>(
        self,
        signal: Signal,
    ) -> WorkerWithGracefulShutdown<impl Stream, F, Ctx, M, Signal>
    where
        Signal: Future,
    {
        WorkerWithGracefulShutdown {
            tick: self.tick,
            concurrent: self.concurrent,
            job_handler: self.job_handler,
            context: self.context,
            backend: self.backend,
            _marker: self._marker,
            signal,
        }
    }

    pub async fn run(self) {
        run_worker(
            self.tick,
            self.job_handler,
            self.context,
            self.backend,
            self.concurrent,
        )
        .await
    }
}

pub struct WorkerWithGracefulShutdown<Tick, F, Ctx, M, Signal> {
    tick: Tick,
    concurrent: usize,
    job_handler: F,
    context: Ctx,
    backend: BackEnd,
    _marker: std::marker::PhantomData<M>,
    signal: Signal,
}

impl<Tick, F, Ctx, M, Signal> WorkerWithGracefulShutdown<Tick, F, Ctx, M, Signal>
where
    Tick: Stream,
    F: JobHandler<M, Context = Ctx>,
    F::Data: DeserializeOwned + 'static,
    Ctx: Clone,
    Signal: Future,
{
    pub async fn run(self) {
        let tick = self.tick.take_until(self.signal);
        run_worker(
            tick,
            self.job_handler,
            self.context,
            self.backend,
            self.concurrent,
        )
        .await
    }
}

async fn run_worker<Tick, F, M, Ctx>(
    tick: Tick,
    handler: F,
    worker_context: Ctx,
    backend: BackEnd,
    concurrent: usize,
) where
    Tick: Stream,
    F: JobHandler<M, Context = Ctx>,
    F::Data: DeserializeOwned,
    Ctx: Clone,
{
    // Helper to run a single job with heartbeat and finalization
    async fn run_one_job<F, M, Ctx>(job: Job<F::Data>, handler: F, worker_context: Ctx) -> ()
    where
        F: JobHandler<M, Context = Ctx>,
        Ctx: Clone,
    {
        let Job {
            context: job_context,
            data,
        } = job;
        tracing::trace!("Start handler");
        let result = {
            let hb_every = LEASE_DURATION / 3;
            let mut ticker = Ticker::new(hb_every).fuse();

            let mut handler_fut = handler
                .clone()
                .call(data, worker_context.clone())
                .boxed()
                .fuse();
            loop {
                futures::select! {
                    res = handler_fut => break res,
                    _ = ticker.next() =>{
                        let _res = job_context.heartbeat().await.inspect_err(
                            |error| tracing::error!(error = %error, job_id = %job_context.id, "Failed to heartbeat job"),
                        );
                    }
                }
            }
        };
        tracing::trace!("Finish handler");

        let _ = match result {
            JobResult::Complete => job_context
                .complete()
                .await
                .inspect_err(|error| tracing::error!(error = %error, "Failed to complete job")),
            JobResult::Retry(duration) => job_context
                .retry(duration)
                .await
                .inspect_err(|error| tracing::error!(error = %error, "Failed to retry job")),
            JobResult::Cancel => job_context
                .cancel()
                .await
                .inspect_err(|error| tracing::error!(error = %error, "Failed to cancel job")),
        };
    }

    let tick = tick.fuse();
    futures::pin_mut!(tick);
    let mut tasks = futures::stream::FuturesUnordered::new();
    let mut in_flight: usize = 0;

    loop {
        futures::select! {
            tick_val = tick.next() => {
                // If tick stream ended (e.g., graceful shutdown), stop fetching
                if tick_val.is_none() { break; }

                let free = concurrent.saturating_sub(in_flight) as u16;
                if free > 0 {
                    let results = backend.get_job::<F::Data>(free).await;
                    for res in results {
                        match res {
                            Ok(job) => {
                                in_flight += 1;
                                let fut = run_one_job::<F, M, Ctx>(job, handler.clone(), worker_context.clone());
                                tasks.push(fut);
                            }
                            Err(error) => {
                                tracing::error!(error = %error, "Failed to fetch job");
                            }
                        }
                    }
                }
            },
            _ = tasks.next() => {
                in_flight = in_flight.saturating_sub(1);
            },
        }
    }

    // Drain remaining tasks
    while tasks.next().await.is_some() {}
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
    pub fn build<F, M>(self, backend: BackEnd, f: F) -> Worker<Tick, F, Ctx, M>
    where
        F: JobHandler<M, Context = Ctx>,
    {
        Worker {
            tick: self.tick,
            concurrent: self.concurrent,
            job_handler: f,
            context: self.context,
            backend,
            _marker: std::marker::PhantomData,
        }
    }
}
