//! Core contract between worker and storage backend.

pub mod backend;
pub mod utils;
pub mod worker;

#[cfg(feature = "rt-tokio")]
mod tokio_spawner;
pub use tokio_spawner::TokioSpawner;

pub use backend::{BackEndContext, BackEndDriver, BackEndPoller, Job};
pub use worker::{Worker, WorkerBuilder, WorkerWithGracefulShutdown};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
/// Outcome a handler wants to persist.
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

/// Explicitly opt-in to receive the payload.
pub struct JobData<T>(pub T);

/// Explicitly opt-in to receive shared context (e.g., pools, config).
pub struct WorkerContext<S>(pub S);

impl<F, Fut> JobHandler<()> for F
where
    F: FnOnce() -> Fut + Clone + Send + Sync + 'static,
    Fut: Future<Output = JobResult> + Send,
{
    type Data = ();
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
    type Data = ();
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
