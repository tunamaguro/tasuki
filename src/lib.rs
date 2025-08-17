#[allow(unused, clippy::manual_async_fn)]
mod queries;

const TASUKI_DEFAULT_QUEUE_NAME: &str = "tasuki_default";

pub mod client;
pub use client::{Client, Error as ClientError, InsertJob};

pub mod worker;
pub use worker::{BackEnd, JobData, JobHandler, JobResult, Worker, WorkerBuilder, WorkerContext};
