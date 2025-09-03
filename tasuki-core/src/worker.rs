//! Worker engine and builder.
//!
//! Periodic polling, bounded concurrency, heartbeats while running, explicit
//! completion. Spawning is pluggable.
use crate::{
    JobHandler, JobResult,
    backend::{BackEndContext, BackEndPoller, Job},
    utils::Ticker,
};
use futures::{FutureExt as _, Stream, StreamExt as _};

/// How job futures are executed (inline, Tokio, etc.).
pub trait JobSpawner {
    type JobHandle<Fut>: Future<Output = ()> + Send + 'static
    where
        Fut: Future<Output = ()> + Send + 'static;
    fn spawn<Fut>(fut: Fut) -> Self::JobHandle<Fut>
    where
        Fut: Future<Output = ()> + Send + 'static;
}

/// Minimal spawner that runs jobs inline (deterministic tests, no runtime).
pub struct InlineSpawner;

impl JobSpawner for InlineSpawner {
    type JobHandle<Fut>
        = Fut
    where
        Fut: Future<Output = ()> + Send + 'static;
    fn spawn<Fut>(fut: Fut) -> Self::JobHandle<Fut>
    where
        Fut: Future<Output = ()> + Send + 'static,
    {
        fut
    }
}

/// Stream that wakes the worker to poll the backend.
pub trait TickStream: Stream<Item = ()> + Send {}

impl<St> TickStream for St where St: Stream<Item = ()> + Send {}

/// Worker + tick stream + backend + handler + concurrency.
pub struct Worker<Tick, Poller, F, M, Sp>
where
    Tick: TickStream,
    F: JobHandler<M>,
    Poller: BackEndPoller<Data = F::Data>,
    Sp: JobSpawner,
{
    tick: Tick,
    poller: Poller,
    handler: F,
    context: F::Context,
    concurrent: usize,
    marker: std::marker::PhantomData<fn() -> Sp>,
}

impl<Tick, Poller, F, M, Sp> Worker<Tick, Poller, F, M, Sp>
where
    Tick: TickStream,
    F: JobHandler<M>,
    F::Context: Clone,
    M: 'static,
    Poller: BackEndPoller<Data = F::Data> + 'static,
    Sp: JobSpawner,
{
    pub fn backend_ref(&self) -> &Poller {
        // Expose backend for composition (e.g., subscribe via listener).
        &self.poller
    }

    /// Replace the tick stream (compose with NOTIFY, throttle, etc.).
    pub fn modify_stream<ModFn, Tick2>(self, func: ModFn) -> Worker<Tick2, Poller, F, M, Sp>
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
            marker,
        } = self;

        let tick2 = func(tick);

        Worker {
            tick: tick2,
            poller,
            handler,
            context,
            concurrent,
            marker,
        }
    }

    /// Add a shutdown signal and drain in-flight jobs.
    pub fn with_graceful_shutdown<Signal>(
        self,
        signal: Signal,
    ) -> WorkerWithGracefulShutdown<Tick, Poller, F, M, Signal, Sp>
    where
        Signal: Future<Output = ()> + Send,
    {
        let Self {
            tick,
            poller,
            handler,
            context,
            concurrent,
            marker: _,
        } = self;
        WorkerWithGracefulShutdown {
            tick,
            poller,
            handler,
            context,
            concurrent,
            signal,
            marker: std::marker::PhantomData,
        }
    }

    /// Run until the tick stream ends (or forever). No prefetch.
    pub fn run(self) -> impl Future<Output = ()> + Send {
        run_worker::<_, _, _, _, _, Sp>(
            self.tick,
            self.handler,
            self.context,
            self.poller,
            self.concurrent,
            std::future::pending::<()>(),
        )
    }
}

/// Worker variant that reacts to a shutdown signal and drains tasks.
pub struct WorkerWithGracefulShutdown<Tick, Poller, F, M, Signal, Sp>
where
    Tick: TickStream,
    F: JobHandler<M>,
    Poller: BackEndPoller<Data = F::Data>,
    Signal: Future<Output = ()> + Send,
    Sp: JobSpawner,
{
    tick: Tick,
    poller: Poller,
    handler: F,
    context: F::Context,
    concurrent: usize,
    signal: Signal,
    marker: std::marker::PhantomData<fn() -> Sp>,
}

impl<Tick, Poller, F, M, Signal, Sp> WorkerWithGracefulShutdown<Tick, Poller, F, M, Signal, Sp>
where
    Tick: TickStream,
    F: JobHandler<M>,
    F::Context: Clone,
    M: 'static,
    Poller: BackEndPoller<Data = F::Data> + 'static,
    Signal: Future<Output = ()> + Send,
    Sp: JobSpawner,
{
    /// Run until shutdown, then drain tasks.
    pub fn run(self) -> impl Future<Output = ()> + Send {
        run_worker::<_, _, _, _, _, Sp>(
            self.tick,
            self.handler,
            self.context,
            self.poller,
            self.concurrent,
            self.signal,
        )
    }
}

/// Core loop: fetch when capacity, spawn, heartbeat, finalize.
async fn run_worker<Tick, Poller, F, M, Signal, Sp>(
    tick: Tick,
    handler: F,
    worker_context: F::Context,
    mut poller: Poller,
    concurrent: usize,
    signal: Signal,
) where
    Tick: TickStream,
    F: JobHandler<M>,
    F::Context: Clone,
    M: 'static,
    Poller: BackEndPoller<Data = F::Data> + 'static,
    Signal: Future + Send,
    Sp: JobSpawner,
{
    futures::pin_mut!(tick);
    futures::pin_mut!(signal);
    let mut tick = tick.fuse();
    // Track in-flight jobs; FuturesUnordered for fair progress across tasks.
    let mut tasks = futures::stream::FuturesUnordered::new();
    let mut signal = signal.fuse();
    loop {
        futures::select! {
            tick_val = tick.next() => {
                // If tick stream ended (e.g., graceful shutdown), stop fetching
                if tick_val.is_none() { break; }

                // Backpressure: fetch only when capacity is free.
                let free = concurrent.saturating_sub(tasks.len());
                if free == 0 {
                    continue;
                }

                let polled_jobs = poller.poll_job(free).await;
                for job in polled_jobs {
                    match job {
                        Ok(job) => {
                            let fut = handle_one_job::<F,M,Poller>(job,handler.clone(),worker_context.clone());
                            tasks.push(<Sp as JobSpawner>::spawn(fut));
                        },
                        Err(error) => {
                            tracing::error!(error = %error, "Failed to fetch job");
                        },
                    }
                }

            },
            _ = tasks.next() => { },
            _ = signal => {
                // Predictable shutdown: stop fetching, drain in-flight tasks.
                tracing::trace!("received graceful shutdown signal. waiting for {} job(s) to finish", tasks.len());
                break;
            }
        }
    }

    // Drain remaining tasks
    while tasks.next().await.is_some() {}
}

/// Run one job with heartbeats, then persist the outcome.
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
        let heartbeat = context.heartbeat();
        futures::pin_mut!(heartbeat);
        let handler_fut = handler.call(data, worker_context);
        futures::pin_mut!(handler_fut);

        let mut heartbeat = heartbeat.fuse();
        let mut handler_fut = handler_fut.fuse();

        loop {
            futures::select! {
                res = handler_fut => break res,
                res = heartbeat => {
                    match res {
                        crate::backend::Heartbeat::Continue => continue,
                        crate::backend::Heartbeat::Stop => {
                            tracing::debug!("heartbeat return stop");
                            return ;
                        },
                    }
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

pub struct WorkerBuilder<Tick = (), Handler = (), M = (), Ctx = (), Sp = InlineSpawner> {
    /// Builder for `Worker`. Prefer explicit configuration over defaults.
    tick: Tick,
    concurrent: usize,
    handler: Handler,
    context: Ctx,
    marker: std::marker::PhantomData<fn() -> (M, Sp)>,
}

impl WorkerBuilder {
    /// Poll every `interval`.
    pub fn new(interval: std::time::Duration) -> WorkerBuilder<Ticker, (), (), (), InlineSpawner> {
        Self::new_with_tick(Ticker::new(interval))
    }

    /// Use a custom tick stream.
    pub fn new_with_tick<Tick>(tick: Tick) -> WorkerBuilder<Tick, (), (), (), InlineSpawner> {
        WorkerBuilder {
            tick,
            concurrent: 4,
            handler: (),
            context: (),
            marker: std::marker::PhantomData,
        }
    }
}

impl<Tick, Handler, M, Ctx, Sp> WorkerBuilder<Tick, Handler, M, Ctx, Sp> {
    /// Set concurrency (max in-flight jobs).
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

impl<Tick, Ctx, Sp> WorkerBuilder<Tick, (), (), Ctx, Sp> {
    /// Provide the job handler.
    pub fn handler<F, M>(self, handler: F) -> WorkerBuilder<Tick, F, M, Ctx, Sp>
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

impl<Tick, Handler, M, Sp> WorkerBuilder<Tick, Handler, M, (), Sp> {
    /// Attach shared context cloned for each job.
    pub fn context<Ctx>(self, context: Ctx) -> WorkerBuilder<Tick, Handler, M, Ctx, Sp>
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
impl<Tick, Handler, M, Ctx, Sp> WorkerBuilder<Tick, Handler, M, Ctx, Sp> {
    /// Choose how to spawn jobs (inline, Tokio, ...).
    pub fn job_spawner<Sp2>(self, _spawner: Sp2) -> WorkerBuilder<Tick, Handler, M, Ctx, Sp2>
    where
        Sp2: JobSpawner,
    {
        let Self {
            tick,
            concurrent,
            handler,
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

impl<Tick, Handler, M, Sp> WorkerBuilder<Tick, Handler, M, Handler::Context, Sp>
where
    Tick: TickStream,
    Handler: JobHandler<M>,
    Sp: JobSpawner,
{
    /// Finalize the worker with a backend that can poll the handler's data type.
    pub fn build<BackEnd>(self, backend: BackEnd) -> Worker<Tick, BackEnd, Handler, M, Sp>
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
            marker: std::marker::PhantomData,
        }
    }
}
