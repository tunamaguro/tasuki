pub use sqlx::PgPool;
pub use tasuki_core;

#[allow(unused, clippy::manual_async_fn)]
mod queries;

pub mod backend;
pub mod client;

pub use backend::{BackEnd, Listener, WorkerWithListenerExt};
pub use client::{Client, InsertJob};

const DEFAULT_QUEUE_NAME: &str = "tasuki_default";
const NOTIFY_CHANNEL_NAME: &str = "tasuki_jobs";
