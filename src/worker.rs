//! Utilities for polling and processing queued jobs.
//!
//! The worker module provides types for fetching jobs from the database
//! and executing them with user supplied handlers.  A [`Worker`] polls a
//! [`BackEnd`] at a fixed interval and drives job handlers to completion
//! while periodically sending heartbeats to maintain job leases.

use futures::{FutureExt, SinkExt, Stream, StreamExt, TryStreamExt};
use pin_project_lite::pin_project;
use serde::{Deserialize, de::DeserializeOwned};

use crate::queries;

pin_project! {
    /// Stream that yields at a fixed period and is used to drive polling or
    /// heartbeat logic within a [`Worker`].
    pub struct Ticker {
        #[pin]
        inner: futures_timer::Delay,
        period: std::time::Duration,
    }
}

impl Ticker {
    pub(crate) fn new(period: std::time::Duration) -> Self {
        Self {
            inner: futures_timer::Delay::new(period),
            period,
        }
    }
}

impl Stream for Ticker {
    type Item = ();

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let mut this = self.project();
        let poll = this.inner.as_mut().poll(cx);
        if poll.is_ready() {
            this.inner.reset(*this.period);
        }
        poll.map(Some)
    }
}

pin_project! {
    /// Rate-limits the stream; items beyond the limit are dropped.
    pub struct Throttle<St, Tick> {
        #[pin]
        inner: St,
        #[pin]
        ticker: Tick,
        max_count: usize,
        // Number of items issued in the current window
        issued_in_window: usize,
    }
}

impl<St, Tick> Throttle<St, Tick> {
    /// Create a throttling adapter over `stream`.
    ///
    /// - `duration`: length of each rate-limiting window.
    /// - `max_count`: maximum number of items to forward per window.
    fn new(stream: St, duration: std::time::Duration, max_count: usize) -> Throttle<St, Ticker> {
        Throttle::new_with_tick(stream, Ticker::new(duration), max_count)
    }

    /// Create a throttling adapter with a custom ticker stream.
    fn new_with_tick(stream: St, ticker: Tick, max_count: usize) -> Throttle<St, Tick> {
        Throttle {
            inner: stream,
            ticker,
            max_count,
            issued_in_window: 0,
        }
    }
}

trait ThrottleExt: Stream {
    /// Forward at most `max_count` items per `duration`; drop excess.
    ///
    /// ## Note
    /// No buffering is performed.
    fn throttle(self, duration: std::time::Duration, max_count: usize) -> Throttle<Self, Ticker>
    where
        Self: Sized,
    {
        Throttle::<Self, Ticker>::new(self, duration, max_count)
    }

    /// Same as [`throttle`] but with a custom ticker stream.
    #[allow(unused)]
    fn throttle_with_tick<Tick>(self, ticker: Tick, max_count: usize) -> Throttle<Self, Tick>
    where
        Self: Sized,
        Tick: Stream,
    {
        Throttle::new_with_tick(self, ticker, max_count)
    }
}

impl<St, Tick> Stream for Throttle<St, Tick>
where
    St: Stream,
    Tick: Stream,
{
    type Item = St::Item;
    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let mut this = self.project();

        // Waker semantics: when throttled, we return Pending after polling the
        // ticker, so the task is woken by the next tick. When not throttled, we
        // poll the inner stream and forward readiness.

        // First, poll the ticker to see if a new window started.
        // If it ticked, reset the counter so a fresh burst is allowed.
        match this.ticker.as_mut().poll_next(cx) {
            std::task::Poll::Ready(Some(_)) => {
                *this.issued_in_window = 0;
            }
            std::task::Poll::Ready(None) => {
                return std::task::Poll::Ready(None);
            }
            std::task::Poll::Pending => {}
        }

        // Otherwise forward to inner stream and count one if we yield an item.
        match this.inner.as_mut().poll_next(cx) {
            std::task::Poll::Ready(Some(item)) => {
                if *this.issued_in_window >= *this.max_count {
                    return std::task::Poll::Pending;
                }

                *this.issued_in_window += 1;
                std::task::Poll::Ready(Some(item))
            }
            other => other,
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        // Delegates size hints; throttle can only reduce throughput.
        self.inner.size_hint()
    }
}

// Allow any Stream to call `.throttle(...)`.
impl<St> ThrottleExt for St where St: Stream {}

pub struct PostgresDriver;
impl crate::BackEndDriver for PostgresDriver {
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
struct MsgError(&'static str);

impl std::fmt::Display for MsgError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}

impl std::error::Error for MsgError {}

#[derive(Debug)]
pub struct Listener {
    inner: sqlx::postgres::PgListener,
    publishers: std::collections::HashMap<String, Publisher>,
}

impl Listener {
    pub(crate) const CHANNEL_NAME: &str = "tasuki_jobs";
    async fn new(pool: sqlx::PgPool) -> Result<Self, sqlx::Error> {
        let mut listener = sqlx::postgres::PgListener::connect_with(&pool).await?;
        listener.listen(Self::CHANNEL_NAME).await?;

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

        #[derive(Debug, Deserialize)]
        struct ChannelData {
            q: String,
        }

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

type ThrottleTick<Tick> = Throttle<futures::stream::Select<Tick, Subscribe>, Ticker>;
pub trait WorkerWithListenerExt<Tick, F, M, Sp>
where
    Tick: crate::TickStream,
    F: crate::JobHandler<M>,
    F::Data: DeserializeOwned,
    M: 'static,
    F::Context: Clone,
    Sp: crate::JobSpawner,
{
    fn subscribe(
        self,
        listener: &mut Listener,
    ) -> crate::Worker<futures::stream::Select<Tick, Subscribe>, BackEnd<F::Data>, F, M, Sp>;

    fn subscribe_with_throttle(
        self,
        listener: &mut Listener,
        duration: std::time::Duration,
        count: usize,
    ) -> crate::Worker<ThrottleTick<Tick>, BackEnd<F::Data>, F, M, Sp>;
}

impl<Tick, F, M, Sp> WorkerWithListenerExt<Tick, F, M, Sp>
    for crate::Worker<Tick, BackEnd<F::Data>, F, M, Sp>
where
    Tick: crate::TickStream,
    F: crate::JobHandler<M>,
    F::Data: DeserializeOwned,
    F::Context: Clone,
    M: 'static,
    Sp: crate::JobSpawner,
{
    fn subscribe(
        self,
        listener: &mut Listener,
    ) -> crate::Worker<futures::stream::Select<Tick, Subscribe>, BackEnd<F::Data>, F, M, Sp> {
        let backend = self.backend_ref();
        let subscribe = listener.subscribe(backend.queue_name.clone());

        self.modify_stream(|tick| futures::stream::select(tick, subscribe))
    }

    fn subscribe_with_throttle(
        self,
        listener: &mut Listener,
        duration: std::time::Duration,
        count: usize,
    ) -> crate::Worker<ThrottleTick<Tick>, BackEnd<F::Data>, F, M, Sp> {
        let backend = self.backend_ref();
        let subscribe = listener.subscribe(backend.queue_name.clone());

        self.modify_stream(|tick| {
            futures::stream::select(tick, subscribe).throttle(duration, count)
        })
    }
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

#[derive(Debug)]
pub struct OutTxContext {
    id: sqlx::types::Uuid,
    pool: sqlx::PgPool,
    lease_token: sqlx::types::Uuid,
    interval: std::time::Duration,
    lease_interval: sqlx::postgres::types::PgInterval,
}

impl crate::BackEndContext for OutTxContext {
    type Driver = PostgresDriver;
    fn heartbeat_interval(&mut self) -> std::time::Duration {
        self.interval
    }
    async fn heartbeat(&mut self) -> Result<(), <Self::Driver as crate::BackEndDriver>::Error> {
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
    async fn complete(self) -> Result<(), <Self::Driver as crate::BackEndDriver>::Error> {
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
    async fn cancel(self) -> Result<(), <Self::Driver as crate::BackEndDriver>::Error> {
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
    ) -> Result<(), <Self::Driver as crate::BackEndDriver>::Error> {
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
type Job<T> = crate::Job<T, OutTxContext>;

#[derive(Debug, Clone)]
/// Backend for fetching and updating jobs in the database.
///
/// A `BackEnd` wraps a [`sqlx::PgPool`] and the name of the queue to pull
/// jobs from. It is consumed by [`Worker`] to obtain work items.
pub struct BackEnd<T> {
    pool: sqlx::PgPool,
    queue_name: std::borrow::Cow<'static, str>,
    lease_time: std::time::Duration,
    marker: std::marker::PhantomData<fn() -> T>,
}

impl<T> crate::BackEndPoller for BackEnd<T>
where
    T: DeserializeOwned + Send + 'static,
{
    type Driver = PostgresDriver;
    type Data = T;
    type Context = OutTxContext;

    async fn poll_job(
        &mut self,
        batch_size: usize,
    ) -> Vec<
        Result<
            crate::Job<Self::Data, Self::Context>,
            <Self::Driver as crate::BackEndDriver>::Error,
        >,
    > {
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
                    let Some(lease_token) = row.lease_token else {
                        return Err(Error {
                            kind: ErrorKind::DataBase,
                            inner: Box::new(MsgError("lease_token is NULL")),
                        });
                    };
                    let context = OutTxContext {
                        id: row.id,
                        pool: self.pool.clone(),
                        lease_token,
                        interval: self.lease_time,
                        lease_interval,
                    };
                    Ok(Job { context, data })
                }
                Err(db_error) => Err(Error::from(db_error)),
            })
            .collect::<Vec<_>>()
            .await
    }
}

impl<T> BackEnd<T> {
    pub const fn new(pool: sqlx::PgPool) -> Self {
        Self {
            pool,
            queue_name: std::borrow::Cow::Borrowed(super::TASUKI_DEFAULT_QUEUE_NAME),
            marker: std::marker::PhantomData,
            lease_time: std::time::Duration::from_secs(30),
        }
    }

    pub async fn listener(&self) -> Result<Listener, sqlx::Error> {
        Listener::new(self.pool.clone()).await
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    // Helper stream that yields an infinite counter as fast as it's polled.
    fn make_counter() -> impl Stream<Item = usize> {
        futures::stream::unfold(0usize, |i| async move { Some((i, i + 1)) })
    }

    #[tokio::test(start_paused = true)]
    async fn throttle_limits_items_per_window() {
        // Window = 200ms, max = 2
        let counter_st = make_counter();
        let interval = tokio::time::interval(Duration::from_millis(200));

        let throttled =
            counter_st.throttle_with_tick(tokio_stream::wrappers::IntervalStream::new(interval), 2);
        tokio::pin!(throttled);

        // First window allows 2 items
        let v = throttled.next().await.unwrap();
        assert_eq!(v, 0);
        let v = throttled.next().await.unwrap();
        assert_eq!(v, 1);

        // Further item within the same window should be throttled (dropped/pending)
        // Ensure we don't get a third item before the 200ms window elapses
        let res = tokio::time::timeout(Duration::from_millis(199), throttled.next()).await;
        assert!(res.is_err(), "item was yielded before the window reset");

        // Advance into the next window boundary
        tokio::time::advance(Duration::from_millis(1)).await; // total advanced: 200ms

        // After window resets, next item arrives
        let _ = throttled.next().await.unwrap();
        // And the second item in the new window also arrives
        let _ = throttled.next().await.unwrap();

        // A third item in the same window should again be throttled
        let res = tokio::time::timeout(Duration::from_millis(199), throttled.next()).await;
        assert!(res.is_err(), "more than max items passed in a window");
    }

    #[tokio::test(start_paused = true)]
    async fn throttle_zero_limit_yields_nothing() {
        // Window = 200ms, max = 0 -> nothing should ever pass through
        let counter_st = make_counter();
        let interval = tokio::time::interval(Duration::from_millis(200));

        let throttled =
            counter_st.throttle_with_tick(tokio_stream::wrappers::IntervalStream::new(interval), 0);
        tokio::pin!(throttled);

        // Verify across several windows that no item is yielded
        for _ in 0..3 {
            let res = tokio::time::timeout(Duration::from_millis(199), throttled.next()).await;
            assert!(res.is_err(), "stream yielded despite zero limit");
            // Cross window boundary
            tokio::time::advance(Duration::from_millis(1)).await;
        }
    }
}
