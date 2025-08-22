//! Backend-facing traits: lease jobs, heartbeat, and persist outcomes.
//!
//! Small surface, strong separation: the worker drives; the backend stores.
//!
//! Why:
//! - Backend owns lease semantics and heartbeat cadence (storage knows best).
//! - Finalization methods consume `self` to forbid double-commit by type.
//! - Polling yields per-job results to avoid head-of-line blocking on errors.
mod tmp {
    /// Backend marker carrying the backend-specific error type.
    pub trait BackEndDriver: Send {
        type Error: std::error::Error + Send;
    }

    /// Per-job context for heartbeats and finalization.
    ///
    /// Why: scope the authority to update a job to a single handle.
    #[trait_variant::make(BackEndContext: Send)]
    pub trait LocalBackEndContext {
        type Driver: BackEndDriver;
        #[allow(unused)]
        fn heartbeat_interval(&mut self) -> std::time::Duration;
        #[allow(unused)]
        async fn heartbeat(&mut self) -> Result<(), <Self::Driver as BackEndDriver>::Error>;
        #[allow(unused)]
        async fn complete(self) -> Result<(), <Self::Driver as BackEndDriver>::Error>;
        #[allow(unused)]
        async fn cancel(self) -> Result<(), <Self::Driver as BackEndDriver>::Error>;
        #[allow(unused)]
        async fn retry(
            self,
            retry_after: Option<std::time::Duration>,
        ) -> Result<(), <Self::Driver as BackEndDriver>::Error>;
    }

    /// Pair of job payload and backend context.
    pub struct Job<Data, Context> {
        data: Data,
        context: Context,
    }

    impl<Data, Context> Job<Data, Context> {
        /// Separate payload and context for handler and bookkeeping.
        pub fn split_parts(self) -> (Data, Context) {
            (self.data, self.context)
        }

        /// Build a job from payload and context.
        pub fn from_parts(data: Data, context: Context) -> Self {
            Self { data, context }
        }
    }

    #[trait_variant::make(BackEndPoller: Send)]
    pub trait LocalBackEndPoller {
        type Driver: BackEndDriver;
        type Data: Send + 'static;
        type Context: BackEndContext + Send + 'static;

        #[allow(unused)]
        async fn poll_job(
            &mut self,
            batch_size: usize,
        ) -> Vec<Result<Job<Self::Data, Self::Context>, <Self::Driver as BackEndDriver>::Error>>;
    }
}

pub use tmp::{BackEndContext, BackEndDriver, BackEndPoller, Job};
