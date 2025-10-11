mod backend;
mod client;

#[allow(unused, clippy::manual_async_fn)]
mod queries;

use futures::future::BoxFuture;
use std::ops::Deref;

impl From<queries::TasukiJobStatus> for crate::backend::JobStatus {
    fn from(value: queries::TasukiJobStatus) -> Self {
        match value {
            queries::TasukiJobStatus::Pending => crate::backend::JobStatus::Pending,
            queries::TasukiJobStatus::Running => crate::backend::JobStatus::Running,
            queries::TasukiJobStatus::Completed => crate::backend::JobStatus::Completed,
            queries::TasukiJobStatus::Failed => crate::backend::JobStatus::Failed,
            queries::TasukiJobStatus::Canceled => crate::backend::JobStatus::Canceled,
        }
    }
}

impl From<crate::backend::JobStatus> for queries::TasukiJobStatus {
    fn from(value: crate::backend::JobStatus) -> Self {
        match value {
            crate::backend::JobStatus::Pending => queries::TasukiJobStatus::Pending,
            crate::backend::JobStatus::Running => queries::TasukiJobStatus::Running,
            crate::backend::JobStatus::Completed => queries::TasukiJobStatus::Completed,
            crate::backend::JobStatus::Failed => queries::TasukiJobStatus::Failed,
            crate::backend::JobStatus::Canceled => queries::TasukiJobStatus::Canceled,
        }
    }
}

pub trait ClientAccess: Clone + Send + Sync + 'static {
    type Handle<'a>: Deref<Target = tokio_postgres::Client> + Send
    where
        Self: 'a;
    type Fut<'a>: std::future::Future<Output = Self::Handle<'a>> + Send
    where
        Self: 'a;

    fn client<'a>(&'a self) -> Self::Fut<'a>;
}

impl ClientAccess for std::sync::Arc<tokio_postgres::Client> {
    type Handle<'a>
        = &'a tokio_postgres::Client
    where
        Self: 'a;
    type Fut<'a>
        = std::future::Ready<Self::Handle<'a>>
    where
        Self: 'a;

    fn client<'a>(&'a self) -> Self::Fut<'a> {
        std::future::ready(self.as_ref())
    }
}

impl ClientAccess for std::sync::Arc<tokio::sync::Mutex<tokio_postgres::Client>> {
    type Handle<'a>
        = tokio::sync::MutexGuard<'a, tokio_postgres::Client>
    where
        Self: 'a;
    // avoid issue https://github.com/rust-lang/rust/issues/100013
    type Fut<'a>
        = BoxFuture<'a, Self::Handle<'a>>
    where
        Self: 'a;

    fn client<'a>(&'a self) -> Self::Fut<'a> {
        Box::pin(async move { self.lock().await })
    }
}
