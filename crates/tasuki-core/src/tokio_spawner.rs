//! Tokio-based job spawner.
//!
//! Why: isolate runtime concerns. We log job panics to surface failures
//! without taking the whole worker down.
use pin_project_lite::pin_project;

use crate::worker::JobSpawner;

/// Spawn jobs onto the Tokio runtime.
pub struct TokioSpawner;

pin_project! {
    /// Wrap Tokio's `JoinHandle<()>` and log panics instead of bubbling them.
    pub struct TokioJoinHandle{
        #[pin]
        handle: tokio::task::JoinHandle<()>
    }
}

impl Future for TokioJoinHandle {
    type Output = ();

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let this = self.project();
        match this.handle.poll(cx) {
            std::task::Poll::Ready(result) => {
                if let Err(error) = result {
                    tracing::error!(error = %error, "job panic happened");
                };
                std::task::Poll::Ready(())
            }
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
    }
}

impl JobSpawner for TokioSpawner {
    type JobHandle<Fut>
        = TokioJoinHandle
    where
        Fut: Future<Output = ()> + Send + 'static;

    fn spawn<Fut>(fut: Fut) -> Self::JobHandle<Fut>
    where
        Fut: Future<Output = ()> + Send + 'static,
    {
        TokioJoinHandle {
            handle: tokio::spawn(fut),
        }
    }
}
