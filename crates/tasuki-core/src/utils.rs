//! Utility streams for timing and rate-limiting.
//!
//! `Ticker`: periodic wake-ups. `Throttle`: cap items per window (no buffering).
//!
//! Why: keep time-based behavior explicit and bounded. Prefer dropping over
//! buffering to avoid hidden latency and memory growth.
use futures::Stream;
use pin_project_lite::pin_project;

pin_project! {
    /// Fixed-period stream to drive polling or heartbeats.
    ///
    /// Why: a simple pulse under our control. We reset on ready to reduce
    /// drift when consumers stall briefly.
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
    /// Rate-limit; drop items beyond the limit (no buffering).
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
    /// Create a "N per D" throttler over `stream`.
    pub fn new(
        stream: St,
        duration: std::time::Duration,
        max_count: usize,
    ) -> Throttle<St, Ticker> {
        Throttle::new_with_tick(stream, Ticker::new(duration), max_count)
    }

    /// Same as `new` but with a custom ticker.
    pub fn new_with_tick(stream: St, ticker: Tick, max_count: usize) -> Throttle<St, Tick> {
        Throttle {
            inner: stream,
            ticker,
            max_count,
            issued_in_window: 0,
        }
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

pub trait ThrottleExt: Stream {
    /// Forward at most `max_count` per `duration`; drop excess (no buffering).
    fn throttle(self, duration: std::time::Duration, max_count: usize) -> Throttle<Self, Ticker>
    where
        Self: Sized,
    {
        Throttle::<Self, Ticker>::new(self, duration, max_count)
    }

    /// Same as `throttle` but with a custom ticker.
    #[allow(unused)]
    fn throttle_with_tick<Tick>(self, ticker: Tick, max_count: usize) -> Throttle<Self, Tick>
    where
        Self: Sized,
        Tick: Stream,
    {
        Throttle::new_with_tick(self, ticker, max_count)
    }
}
