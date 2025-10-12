use std::{marker::PhantomData, ops::Deref};

use futures::{
    StreamExt,
    channel::mpsc,
    stream::{BoxStream, LocalBoxStream},
};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_postgres::{Client, Connection};

pub trait MakeConnection<S, T>
where
    S: AsyncRead + AsyncWrite + Unpin,
    T: AsyncRead + AsyncWrite + Unpin,
{
    /// create a new connection
    ///
    /// `PgListener` try reconnect until return `None`
    fn make_connection(
        &self,
    ) -> impl std::future::Future<
        Output = Option<Result<(Client, Connection<S, T>), tokio_postgres::Error>>,
    >;
}

impl<F, Fut, S, T> MakeConnection<S, T> for F
where
    F: Fn() -> Fut,
    Fut: std::future::Future<
            Output = Option<Result<(Client, Connection<S, T>), tokio_postgres::Error>>,
        >,
    S: AsyncRead + AsyncWrite + Unpin,
    T: AsyncRead + AsyncWrite + Unpin,
{
    fn make_connection(
        &self,
    ) -> impl std::future::Future<
        Output = Option<Result<(Client, Connection<S, T>), tokio_postgres::Error>>,
    > {
        self()
    }
}

pub struct NoopMakeConnection;
impl<S, T> MakeConnection<S, T> for NoopMakeConnection
where
    S: AsyncRead + AsyncWrite + Unpin,
    T: AsyncRead + AsyncWrite + Unpin,
{
    fn make_connection(
        &self,
    ) -> impl std::future::Future<
        Output = Option<Result<(Client, Connection<S, T>), tokio_postgres::Error>>,
    > {
        async { None }
    }
}

struct ConnectionWrapper {
    connection_stream:
        LocalBoxStream<'static, Result<tokio_postgres::AsyncMessage, tokio_postgres::Error>>,
    client: tokio_postgres::Client,
}

impl ConnectionWrapper {
    fn new<S, T>(client: tokio_postgres::Client, mut connection: Connection<S, T>) -> Self
    where
        S: AsyncRead + AsyncWrite + Unpin + 'static,
        T: AsyncRead + AsyncWrite + Unpin + 'static,
    {
        let stream = futures::stream::poll_fn(move |cx| connection.poll_message(cx));
        let connection_stream = Box::pin(stream);
        Self {
            connection_stream,
            client,
        }
    }

    async fn recv(
        &mut self,
    ) -> Option<Result<tokio_postgres::AsyncMessage, tokio_postgres::Error>> {
        self.connection_stream.next().await
    }
}

impl Deref for ConnectionWrapper {
    type Target = tokio_postgres::Client;

    fn deref(&self) -> &Self::Target {
        &self.client
    }
}

struct PgListener<F, S, T> {
    conn: Option<ConnectionWrapper>,
    make_connection: F,
    marker: PhantomData<fn() -> (S, T)>,
}

impl<F, S, T> PgListener<F, S, T>
where
    F: MakeConnection<S, T>,
    S: AsyncRead + AsyncWrite + Unpin + 'static,
    T: AsyncRead + AsyncWrite + Unpin + 'static,
{
    fn new(make_connection: F) -> Self {
        Self {
            conn: None,
            make_connection,
            marker: PhantomData,
        }
    }

    fn from_connection(
        client: tokio_postgres::Client,
        connection: Connection<S, T>,
    ) -> PgListener<NoopMakeConnection, S, T> {
        PgListener {
            conn: Some(ConnectionWrapper::new(client, connection)),
            make_connection: NoopMakeConnection,
            marker: PhantomData,
        }
    }
}

struct Publisher {
    sender: futures::channel::mpsc::Sender<()>,
}

pin_project_lite::pin_project! {
    #[derive(Debug)]
    pub struct Subscribe {
        #[pin]
        receiver: futures::channel::mpsc::Receiver<()>,
    }
}

impl futures::Stream for Subscribe {
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
