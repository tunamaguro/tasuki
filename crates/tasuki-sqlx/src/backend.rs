use crate::queries;
use futures::{FutureExt as _, SinkExt as _, Stream, StreamExt as _, TryStreamExt as _};
use pin_project_lite::pin_project;
use serde::{Deserialize, de::DeserializeOwned};
use tasuki_core::{
    BackEndContext, BackEndDriver, BackEndPoller, Job, JobHandler, Worker,
    utils::{Throttle, Ticker},
    worker::{JobSpawner, TickStream},
};

pub struct PostgresDriver;
impl BackEndDriver for PostgresDriver {
    type Error = Error;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
/// Categorization of failures that may occur while processing jobs.
pub enum ErrorKind {
    /// Errors originating from database interactions.
    DataBase,
    /// Errors that happen while decoding job payloads.
    Decode,
    /// The worker lost its lease/ownership (token mismatch or 0 rows affected).
    LostLease,
}

#[derive(Debug)]
pub struct Error {
    #[allow(unused)]
    kind: ErrorKind,
    inner: Box<dyn std::error::Error + Send + 'static>,
}

impl Error {
    fn new_database(error: Box<dyn std::error::Error + Send>) -> Self {
        Error {
            kind: ErrorKind::DataBase,
            inner: error,
        }
    }
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
struct LostLeaseError;

impl std::fmt::Display for LostLeaseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("lost lease for job")
    }
}

impl std::error::Error for LostLeaseError {}

#[derive(Debug)]
pub struct OutTxContext {
    id: sqlx::types::Uuid,
    pool: sqlx::PgPool,
    lease_token: sqlx::types::Uuid,
    interval: std::time::Duration,
    lease_interval: sqlx::postgres::types::PgInterval,
}

impl BackEndContext for OutTxContext {
    type Driver = PostgresDriver;
    fn heartbeat_interval(&mut self) -> std::time::Duration {
        self.interval
    }
    async fn heartbeat(&mut self) -> Result<(), <Self::Driver as BackEndDriver>::Error> {
        let res = queries::HeartBeatJob::builder()
            .lease_interval(&self.lease_interval)
            .id(self.id)
            .lease_token(Some(self.lease_token))
            .build()
            .execute(&self.pool)
            .await?;

        if res.rows_affected() == 0 {
            return Err(Error {
                kind: ErrorKind::LostLease,
                inner: Box::new(LostLeaseError),
            });
        }
        Ok(())
    }
    async fn complete(self) -> Result<(), <Self::Driver as BackEndDriver>::Error> {
        let res = queries::CompleteJob::builder()
            .id(self.id)
            .lease_token(Some(self.lease_token))
            .build()
            .execute(&self.pool)
            .await?;

        if res.rows_affected() == 0 {
            return Err(Error {
                kind: ErrorKind::LostLease,
                inner: Box::new(LostLeaseError),
            });
        }
        Ok(())
    }
    async fn cancel(self) -> Result<(), <Self::Driver as BackEndDriver>::Error> {
        let res = queries::CancelJob::builder()
            .id(self.id)
            .lease_token(Some(self.lease_token))
            .build()
            .execute(&self.pool)
            .await?;

        if res.rows_affected() == 0 {
            return Err(Error {
                kind: ErrorKind::LostLease,
                inner: Box::new(LostLeaseError),
            });
        }
        Ok(())
    }
    async fn retry(
        self,
        retry_after: Option<std::time::Duration>,
    ) -> Result<(), <Self::Driver as BackEndDriver>::Error> {
        let interval = retry_after
            .map(|duration| {
                sqlx::postgres::types::PgInterval::try_from(duration).map_err(|e| Error {
                    kind: ErrorKind::DataBase,
                    inner: e,
                })
            })
            .transpose()?;
        let res = queries::RetryJob::builder()
            .interval(interval.as_ref())
            .id(self.id)
            .lease_token(Some(self.lease_token))
            .build()
            .execute(&self.pool)
            .await?;

        if res.rows_affected() == 0 {
            return Err(Error {
                kind: ErrorKind::LostLease,
                inner: Box::new(LostLeaseError),
            });
        }
        Ok(())
    }
}

/// Backend for fetching and updating jobs from postgres
#[derive(Debug, Clone)]
pub struct BackEnd<T> {
    pool: sqlx::PgPool,
    queue_name: std::borrow::Cow<'static, str>,
    lease_time: std::time::Duration,
    marker: std::marker::PhantomData<fn() -> T>,
}

impl<T> BackEnd<T> {
    pub const fn new(pool: sqlx::PgPool) -> Self {
        Self {
            pool,
            queue_name: std::borrow::Cow::Borrowed(crate::DEFAULT_QUEUE_NAME),
            marker: std::marker::PhantomData,
            lease_time: std::time::Duration::from_secs(30),
        }
    }

    pub async fn listener(&self) -> Result<Listener, sqlx::Error> {
        Listener::new(self.pool.clone()).await
    }
}

impl<T> BackEndPoller for BackEnd<T>
where
    T: DeserializeOwned + Send + 'static,
{
    type Driver = PostgresDriver;
    type Data = T;
    type Context = OutTxContext;

    async fn poll_job(
        &mut self,
        batch_size: usize,
    ) -> Vec<Result<Job<Self::Data, Self::Context>, <Self::Driver as BackEndDriver>::Error>> {
        let lease_interval = sqlx::postgres::types::PgInterval::try_from(self.lease_time)
            .map_err(|error| Error::new_database(error));

        let lease_interval = match lease_interval {
            Ok(v) => v,
            Err(error) => return vec![Err(error)],
        };

        let builder = queries::GetAvailableJobs::builder()
            .lease_interval(&lease_interval)
            .queue_name(&self.queue_name)
            .batch_size(i32::try_from(batch_size).unwrap_or(32))
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
                        lease_token: row.lease_token,
                        interval: self.lease_time,
                        lease_interval,
                    };
                    Ok(Job::from_parts(data, context))
                }
                Err(db_error) => Err(Error::from(db_error)),
            })
            .collect::<Vec<_>>()
            .await
    }
}

#[derive(Debug)]
pub struct Listener {
    inner: sqlx::postgres::PgListener,
    publishers: std::collections::HashMap<String, Publisher>,
}

impl Listener {
    async fn new(pool: sqlx::PgPool) -> Result<Self, sqlx::Error> {
        let mut listener = sqlx::postgres::PgListener::connect_with(&pool).await?;
        listener.listen(crate::NOTIFY_CHANNEL_NAME).await?;

        Ok(Self {
            inner: listener,
            publishers: Default::default(),
        })
    }

    /// Listen for notifications until the provided `signal` completes.
    ///
    /// This is a graceful variant of [`listen`]; it exits when either
    /// the database notification stream ends, an error occurs, or the
    /// `signal` resolves (e.g., on shutdown).
    pub async fn listen_until<Signal>(self, signal: Signal) -> Result<(), Error>
    where
        Signal: std::future::Future,
    {
        let mut stream = self.inner.into_stream().fuse();
        let mut publisers = self.publishers;
        let signal = signal.fuse();
        futures::pin_mut!(signal);

        loop {
            futures::select! {
                _ = &mut signal => {
                    break;
                }
                msg = stream.try_next() => {
                    match msg {
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
                    }
                }
            }
        }

        Ok(())
    }

    pub async fn listen(self) -> Result<(), Error> {
        self.listen_until(std::future::pending::<()>()).await
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

#[derive(Deserialize)]
struct ChannelData {
    q: String,
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

type ThrottleTick<Tick> = Throttle<futures::stream::Select<Tick, Subscribe>, Ticker>;
pub trait WorkerWithListenerExt<Tick, F, M, Sp>
where
    Tick: TickStream,
    F: JobHandler<M>,
    F::Data: DeserializeOwned,
    M: 'static,
    F::Context: Clone,
    Sp: JobSpawner,
{
    fn subscribe(
        self,
        listener: &mut Listener,
    ) -> Worker<futures::stream::Select<Tick, Subscribe>, BackEnd<F::Data>, F, M, Sp>;

    fn subscribe_with_throttle(
        self,
        listener: &mut Listener,
        duration: std::time::Duration,
        count: usize,
    ) -> Worker<ThrottleTick<Tick>, BackEnd<F::Data>, F, M, Sp>;
}

impl<Tick, F, M, Sp> WorkerWithListenerExt<Tick, F, M, Sp>
    for Worker<Tick, BackEnd<F::Data>, F, M, Sp>
where
    Tick: TickStream,
    F: JobHandler<M>,
    F::Data: DeserializeOwned,
    F::Context: Clone,
    M: 'static,
    Sp: JobSpawner,
{
    fn subscribe(
        self,
        listener: &mut Listener,
    ) -> Worker<futures::stream::Select<Tick, Subscribe>, BackEnd<F::Data>, F, M, Sp> {
        let backend = self.backend_ref();
        let subscribe = listener.subscribe(backend.queue_name.clone());

        self.modify_stream(|tick| futures::stream::select(tick, subscribe))
    }

    fn subscribe_with_throttle(
        self,
        listener: &mut Listener,
        duration: std::time::Duration,
        count: usize,
    ) -> Worker<ThrottleTick<Tick>, BackEnd<F::Data>, F, M, Sp> {
        let backend = self.backend_ref();
        let subscribe = listener.subscribe(backend.queue_name.clone());

        self.modify_stream(|tick| {
            let st = futures::stream::select(tick, subscribe);
            Throttle::<futures::stream::Select<Tick, Subscribe>, Ticker>::new(st, duration, count)
        })
    }
}
