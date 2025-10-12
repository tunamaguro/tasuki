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
    /// `PgListenPool` try reconnect until return `None`
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

pin_project_lite::pin_project! {
    struct ConnectionWrapper<S,T>{
        conn: tokio_postgres::Connection<S,T>
    }
}

impl<S, T> futures::Stream for ConnectionWrapper<S, T>
where
    S: AsyncRead + AsyncWrite + Unpin,
    T: AsyncRead + AsyncWrite + Unpin,
{
    type Item = Result<tokio_postgres::AsyncMessage, tokio_postgres::Error>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let this = self.project();
        this.conn.poll_message(cx)
    }
}

struct PgListenConnection<S, T> {
    conn: ConnectionWrapper<S, T>,
    client: tokio_postgres::Client,
}

impl<S, T> PgListenConnection<S, T>
where
    S: AsyncRead + AsyncWrite + Unpin,
    T: AsyncRead + AsyncWrite + Unpin,
{
    fn new(client: tokio_postgres::Client, connection: Connection<S, T>) -> Self {
        Self {
            conn: ConnectionWrapper { conn: connection },
            client,
        }
    }

    async fn recv(
        &mut self,
    ) -> Result<Option<tokio_postgres::Notification>, tokio_postgres::Error> {
        loop {
            match self.conn.next().await {
                Some(Ok(message)) => match message {
                    tokio_postgres::AsyncMessage::Notice(notice) => {
                        tracing::info!("{}: {}", notice.severity(), notice.message());
                    }
                    tokio_postgres::AsyncMessage::Notification(notification) => {
                        break Ok(Some(notification));
                    }
                    _ => {}
                },
                Some(Err(err)) => break Err(err),
                None => break Ok(None),
            }
        }
    }
}

impl<S, T> Deref for PgListenConnection<S, T> {
    type Target = tokio_postgres::Client;

    fn deref(&self) -> &Self::Target {
        &self.client
    }
}

struct PgListenPool<F, S, T> {
    conn: Option<PgListenConnection<S, T>>,
    make_connection: F,
    channels: Vec<std::borrow::Cow<'static, str>>,
}

impl<F, S, T> PgListenPool<F, S, T>
where
    F: MakeConnection<S, T>,
    S: AsyncRead + AsyncWrite + Unpin,
    T: AsyncRead + AsyncWrite + Unpin,
{
    fn new(make_connection: F) -> Self {
        Self {
            conn: None,
            make_connection,
            channels: Default::default(),
        }
    }

    fn from_connection(
        client: tokio_postgres::Client,
        connection: Connection<S, T>,
    ) -> PgListenPool<NoopMakeConnection, S, T> {
        PgListenPool {
            conn: Some(PgListenConnection::new(client, connection)),
            make_connection: NoopMakeConnection,
            channels: Default::default(),
        }
    }

    async fn reconnect(
        &mut self,
    ) -> Result<Option<&mut PgListenConnection<S, T>>, tokio_postgres::Error> {
        let conn = self.make_connection.make_connection().await.transpose()?;
        match conn {
            Some(conn) => {
                let query = build_listen_all_query(self.channels.iter());
                conn.0.simple_query(&query).await?;
                self.conn = Some(PgListenConnection::new(conn.0, conn.1));
                Ok(self.conn.as_mut())
            }
            None => Ok(None),
        }
    }

    async fn connect(
        &mut self,
    ) -> Result<Option<&mut PgListenConnection<S, T>>, tokio_postgres::Error> {
        if self.conn.is_some() {
            return Ok(self.conn.as_mut());
        }

        // TODO: Noneが返されるまで再接続を繰り返す
        self.reconnect().await
    }

    async fn recv(
        &mut self,
    ) -> Result<Option<tokio_postgres::Notification>, tokio_postgres::Error> {
        if let Some(conn) = self.connect().await? {
            conn.recv().await
        } else {
            Ok(None)
        }
    }

    async fn listen(
        &mut self,
        channel_name: impl Into<std::borrow::Cow<'static, str>>,
    ) -> Result<(), tokio_postgres::Error> {
        let channel_name: std::borrow::Cow<'static, str> = channel_name.into();
        if let Some(conn) = self.connect().await? {
            conn.simple_query(&build_listen_all_query(std::iter::once(&channel_name)))
                .await?;
        }
        self.channels.push(channel_name);
        Ok(())
    }
}

// Copy `ident` and `build_listen_all_query` from sqlx-postgres(v0.8.6)
// Link: https://github.com/launchbadge/sqlx/blob/bab1b022bd56a64f9a08b46b36b97c5cff19d77e/sqlx-postgres/src/listener.rs
//
// Copyright (c) 2020 LaunchBadge, LLC
// Permission is hereby granted, free of charge, to any
// person obtaining a copy of this software and associated
// documentation files (the "Software"), to deal in the
// Software without restriction, including without
// limitation the rights to use, copy, modify, merge,
// publish, distribute, sublicense, and/or sell copies of
// the Software, and to permit persons to whom the Software
// is furnished to do so, subject to the following
// conditions:
//
// The above copyright notice and this permission notice
// shall be included in all copies or substantial portions
// of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF
// ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED
// TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
// PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT
// SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
// CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
// OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR
// IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
// DEALINGS IN THE SOFTWARE.
fn ident(mut name: &str) -> String {
    // If the input string contains a NUL byte, we should truncate the
    // identifier.
    if let Some(index) = name.find('\0') {
        name = &name[..index];
    }

    // Any double quotes must be escaped
    name.replace('"', "\"\"")
}

fn build_listen_all_query(channels: impl IntoIterator<Item = impl AsRef<str>>) -> String {
    channels.into_iter().fold(String::new(), |mut acc, chan| {
        acc.push_str(r#"LISTEN ""#);
        acc.push_str(&ident(chan.as_ref()));
        acc.push_str(r#"";"#);
        acc
    })
}
