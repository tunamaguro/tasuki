#[allow(unused, clippy::manual_async_fn)]
mod queries;

const TASUKI_DEFAULT_QUEUE_NAME: &str = "tasuki_default";

pub mod client;
pub use client::{Client, Error as ClientError, InsertJob};

pub mod worker;
use futures::{FutureExt, Stream, StreamExt};
pub use worker::BackEnd;

use crate::worker::Ticker;

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
    type Data: Send + 'static;
    /// Type of the shared context provided to the handler.
    type Context: Send + 'static;

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
    T: Send + 'static,
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
    S: Send + 'static,
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
    T: Send + 'static,
    S: Send + 'static,
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

pub trait BackEndDriver: Send {
    type Error: std::error::Error + Send;
}

#[trait_variant::make(BackEndContext: Send)]
pub trait LocalBackEndContext {
    type Driver: BackEndDriver;
    fn heartbeat_interval(&mut self) -> std::time::Duration;
    async fn heartbeat(&mut self) -> Result<(), <Self::Driver as BackEndDriver>::Error>;
    async fn complete(self) -> Result<(), <Self::Driver as BackEndDriver>::Error>;
    async fn cancel(self) -> Result<(), <Self::Driver as BackEndDriver>::Error>;
    async fn retry(
        self,
        retry_after: Option<std::time::Duration>,
    ) -> Result<(), <Self::Driver as BackEndDriver>::Error>;
}

pub struct Job<Data, Context> {
    pub(crate) data: Data,
    pub(crate) context: Context,
}

impl<Data, Context> Job<Data, Context> {
    fn split_parts(self) -> (Data, Context) {
        (self.data, self.context)
    }
}

#[trait_variant::make(BackEndPoller: Send)]
pub trait LocalBackEndPoller {
    type Driver: BackEndDriver;
    type Data: Send + 'static;
    type Context: BackEndContext + Send + 'static;

    async fn poll_job(
        &mut self,
        batch_size: usize,
    ) -> Vec<Result<Job<Self::Data, Self::Context>, <Self::Driver as BackEndDriver>::Error>>;
}

pub trait TickStream: Stream<Item = ()> + Send {}

impl<St> TickStream for St where St: Stream<Item = ()> + Send {}

pub struct Worker<Tick, Poller, F, M>
where
    Tick: TickStream,
    F: JobHandler<M>,
    Poller: BackEndPoller<Data = F::Data>,
{
    tick: Tick,
    poller: Poller,
    handler: F,
    context: F::Context,
    concurrent: usize,
}

impl<Tick, Poller, F, M> Worker<Tick, Poller, F, M>
where
    Tick: TickStream,
    F: JobHandler<M>,
    F::Context: Clone,
    Poller: BackEndPoller<Data = F::Data>,
{
    pub fn backend_ref(&self) -> &Poller {
        &self.poller
    }

    pub fn modify_stream<ModFn, Tick2>(self, func: ModFn) -> Worker<Tick2, Poller, F, M>
    where
        ModFn: FnOnce(Tick) -> Tick2,
        Tick2: TickStream,
    {
        let Self {
            tick,
            poller,
            handler,
            context,
            concurrent,
        } = self;

        let tick2 = func(tick);

        Worker {
            tick: tick2,
            poller,
            handler,
            context,
            concurrent,
        }
    }

    pub fn with_graceful_shutdown<Signal>(
        self,
        signal: Signal,
    ) -> Worker<impl TickStream, Poller, F, M>
    where
        Signal: Future<Output = ()> + Send,
    {
        self.modify_stream(|st| st.take_until(signal))
    }

    pub fn run(self) -> impl Future<Output = ()> + Send {
        run_worker(
            self.tick,
            self.handler,
            self.context,
            self.poller,
            self.concurrent,
        )
    }
}

async fn run_worker<Tick, Poller, F, M>(
    tick: Tick,
    handler: F,
    worker_context: F::Context,
    mut poller: Poller,
    concurrent: usize,
) where
    Tick: TickStream,
    F: JobHandler<M>,
    F::Context: Clone,
    Poller: BackEndPoller<Data = F::Data>,
{
    let mut tick = tick.boxed().fuse();
    let mut tasks = futures::stream::FuturesUnordered::new();
    let mut in_flight = 0;

    loop {
        futures::select! {
            tick_val = tick.next() => {
                // If tick stream ended (e.g., graceful shutdown), stop fetching
                if tick_val.is_none() { break; }

                let free = concurrent.saturating_sub(in_flight);
                if free == 0 {
                    continue;
                }

                let polled_jobs = poller.poll_job(free).await;
                for job in polled_jobs {
                    match job {
                        Ok(job) => {
                            in_flight += 1;
                            let fut = handle_one_job::<F,M,Poller>(job,handler.clone(),worker_context.clone());
                            tasks.push(fut);
                        },
                        Err(error) => {
                            tracing::error!(error = %error, "Failed to fetch job");
                        },
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

async fn handle_one_job<F, M, Poller>(
    job: Job<F::Data, <Poller as BackEndPoller>::Context>,
    handler: F,
    worker_context: F::Context,
) where
    F: JobHandler<M>,
    Poller: BackEndPoller<Data = F::Data>,
{
    let (data, mut context) = job.split_parts();
    tracing::trace!("Start handler");
    let job_result = {
        let mut ticker = Ticker::new(BackEndContext::heartbeat_interval(&mut context)).fuse();
        let mut handler_fut = handler.call(data, worker_context).boxed().fuse();
        loop {
            futures::select! {
                res = handler_fut => break res,
                _ = ticker.next() =>{
                    let _res = BackEndContext::heartbeat(&mut context).await.inspect_err(
                        |error| tracing::error!(error = %error, "Failed to heartbeat job"),
                    );
                }
            }
        }
    };
    tracing::trace!("Finish handler");

    let _ = match job_result {
        JobResult::Complete => BackEndContext::complete(context)
            .await
            .inspect_err(|error| tracing::error!(error = %error, "Failed to complete job")),
        JobResult::Retry(duration) => BackEndContext::retry(context, duration)
            .await
            .inspect_err(|error| tracing::error!(error = %error, "Failed to retry job")),
        JobResult::Cancel => BackEndContext::cancel(context)
            .await
            .inspect_err(|error| tracing::error!(error = %error, "Failed to cancel job")),
    };
}

pub struct WorkerBuilder<Tick = (), Handler = (), M = (), Ctx = ()> {
    tick: Tick,
    concurrent: usize,
    handler: Handler,
    context: Ctx,
    marker: std::marker::PhantomData<fn() -> M>,
}

impl WorkerBuilder<(), (), (), ()> {
    pub fn new(interval: std::time::Duration) -> WorkerBuilder<Ticker, (), (), ()> {
        Self::new_with_tick(Ticker::new(interval))
    }

    pub fn new_with_tick<Tick>(tick: Tick) -> WorkerBuilder<Tick, (), (), ()> {
        WorkerBuilder {
            tick,
            concurrent: 4,
            handler: (),
            context: (),
            marker: std::marker::PhantomData,
        }
    }
}

impl<Tick, Handler, M, Ctx> WorkerBuilder<Tick, Handler, M, Ctx> {
    pub fn concurrent(self, concurrent: usize) -> Self {
        let Self {
            tick,
            concurrent: _,
            handler,
            marker,
            context,
        } = self;
        WorkerBuilder {
            tick,
            concurrent,
            handler,
            marker,
            context,
        }
    }
}

impl<Tick, Ctx> WorkerBuilder<Tick, (), (), Ctx> {
    pub fn handler<F, M>(self, handler: F) -> WorkerBuilder<Tick, F, M, Ctx>
    where
        F: JobHandler<M>,
    {
        let Self {
            tick,
            concurrent,
            handler: _,
            marker: _,
            context,
        } = self;
        WorkerBuilder {
            tick,
            concurrent,
            handler,
            marker: std::marker::PhantomData,
            context,
        }
    }
}

impl<Tick, Handler, M> WorkerBuilder<Tick, Handler, M, ()> {
    pub fn context<Ctx>(self, context: Ctx) -> WorkerBuilder<Tick, Handler, M, Ctx>
    where
        Ctx: Clone + Send,
    {
        let Self {
            tick,
            concurrent,
            handler,
            marker,
            context: _,
        } = self;
        WorkerBuilder {
            tick,
            concurrent,
            handler,
            marker,
            context,
        }
    }
}

impl<Tick, Handler, M> WorkerBuilder<Tick, Handler, M, Handler::Context>
where
    Tick: TickStream,
    Handler: JobHandler<M>,
{
    pub fn build<BackEnd>(self, backend: BackEnd) -> Worker<Tick, BackEnd, Handler, M>
    where
        BackEnd: BackEndPoller<Data = Handler::Data>,
    {
        let Self {
            tick,
            concurrent,
            handler,
            context,
            marker: _,
        } = self;
        Worker {
            tick,
            poller: backend,
            handler,
            context,
            concurrent,
        }
    }
}
