pub mod backend;
pub mod client;
mod pg_type;

#[allow(unused, clippy::manual_async_fn)]
mod queries;

const DEFAULT_QUEUE_NAME: &str = "tasuki_default";
#[allow(unused)]
const NOTIFY_CHANNEL_NAME: &str = "tasuki_jobs";

pub trait ClientAccess: Clone + Send + Sync + 'static {
    type Handle<'a>: std::ops::Deref<Target = ::tokio_postgres::Client> + Send
    where
        Self: 'a;
    type Error: std::error::Error + Send + Sync + 'static;
    type Fut<'a>: std::future::Future<Output = Result<Self::Handle<'a>, Self::Error>> + Send
    where
        Self: 'a;

    fn client<'a>(&'a self) -> Self::Fut<'a>;
}

impl ClientAccess for std::sync::Arc<::tokio_postgres::Client> {
    type Handle<'a>
        = &'a ::tokio_postgres::Client
    where
        Self: 'a;
    type Error = std::convert::Infallible;
    type Fut<'a>
        = std::future::Ready<Result<Self::Handle<'a>, Self::Error>>
    where
        Self: 'a;

    fn client<'a>(&'a self) -> Self::Fut<'a> {
        std::future::ready(Ok(self.as_ref()))
    }
}

impl ClientAccess for std::sync::Arc<futures::lock::Mutex<tokio_postgres::Client>> {
    type Handle<'a>
        = futures::lock::MutexGuard<'a, tokio_postgres::Client>
    where
        Self: 'a;
    type Error = std::convert::Infallible;
    type Fut<'a>
        = futures::future::BoxFuture<'a, Result<Self::Handle<'a>, Self::Error>>
    where
        Self: 'a;

    fn client<'a>(&'a self) -> Self::Fut<'a> {
        Box::pin(async move { Ok(self.lock().await) })
    }
}
#[cfg(feature = "rt-tokio")]
mod tokio_impl {
    use crate::ClientAccess;
    impl ClientAccess for std::sync::Arc<tokio::sync::Mutex<tokio_postgres::Client>> {
        type Handle<'a>
            = tokio::sync::MutexGuard<'a, ::tokio_postgres::Client>
        where
            Self: 'a;
        type Error = std::convert::Infallible;
        type Fut<'a>
            = futures::future::BoxFuture<'a, Result<Self::Handle<'a>, Self::Error>>
        where
            Self: 'a;

        fn client<'a>(&'a self) -> Self::Fut<'a> {
            Box::pin(async move { Ok(self.lock().await) })
        }
    }
}

#[cfg(feature = "deadpool-postgres")]
mod deadpool_impl {
    use crate::ClientAccess;

    pub struct DeadPoolObjectWrapper(pub deadpool_postgres::Object);

    impl std::ops::Deref for DeadPoolObjectWrapper {
        type Target = ::tokio_postgres::Client;

        fn deref(&self) -> &Self::Target {
            &self.0
        }
    }

    impl ClientAccess for deadpool_postgres::Pool {
        type Handle<'a>
            = DeadPoolObjectWrapper
        where
            Self: 'a;
        type Error = deadpool_postgres::PoolError;
        type Fut<'a>
            = futures::future::BoxFuture<'a, Result<Self::Handle<'a>, Self::Error>>
        where
            Self: 'a;
        fn client<'a>(&'a self) -> Self::Fut<'a> {
            Box::pin(async move {
                let obj = self.get().await?;
                Ok(DeadPoolObjectWrapper(obj))
            })
        }
    }
}
