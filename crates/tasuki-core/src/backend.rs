mod tmp {
    /// Backend marker carrying the backend-specific error type.
    pub trait BackEndDriver: Send {
        type Error: std::error::Error + Send;
    }

    /// Heartbeat result
    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
    pub enum Heartbeat {
        /// continue job
        Continue,
        /// stop job. No finalization
        Stop,
    }

    /// Per-job context for heartbeats and finalization.
    #[trait_variant::make(BackEndContext: Send)]
    pub trait LocalBackEndContext {
        type Driver: BackEndDriver;

        #[allow(unused)]
        async fn heartbeat(&mut self) -> Heartbeat;
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

pub use tmp::{BackEndContext, BackEndDriver, BackEndPoller, Heartbeat, Job};
