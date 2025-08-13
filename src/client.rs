use serde::Serialize;

use crate::queries;

pub struct InsertJob<T> {
    data: T,
    max_attempts: u16,
}

impl<T> InsertJob<T> {
    const DEFAULT_MAX_ATTEMPTS: u16 = 25;

    pub const fn new(data: T) -> Self {
        Self {
            data,
            max_attempts: Self::DEFAULT_MAX_ATTEMPTS,
        }
    }

    pub fn max_attempts(self, max_attempts: u16) -> Self {
        Self {
            max_attempts,
            ..self
        }
    }

    pub fn into_inner(self) -> T {
        self.data
    }
}

impl<T> From<T> for InsertJob<T> {
    fn from(value: T) -> Self {
        InsertJob::new(value)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum ErrorKind {
    DataBase,
    Encode,
}

#[derive(Debug)]
pub struct Error {
    kind: ErrorKind,
    inner: Box<dyn std::error::Error + Send + 'static>,
}

impl Error {
    pub fn kind(&self) -> ErrorKind {
        self.kind
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
            kind: ErrorKind::Encode,
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
pub struct Client<T> {
    pool: sqlx::PgPool,
    data_type: std::marker::PhantomData<T>,
}

impl<T> Clone for Client<T> {
    fn clone(&self) -> Self {
        Self {
            pool: self.pool.clone(),
            data_type: std::marker::PhantomData,
        }
    }
}

impl<T> Client<T>
where
    T: Serialize + Send + 'static,
{
    pub fn new(pool: sqlx::PgPool) -> Self {
        Self {
            pool,
            data_type: std::marker::PhantomData,
        }
    }

    pub fn insert(
        &self,
        data: InsertJob<T>,
    ) -> impl Future<Output = Result<(), Error>> + Send + '_ {
        Self::insert_tx(data, &self.pool)
    }

    pub async fn insert_tx<'a, 'c, A>(
        data: InsertJob<T>,
        tx: A,
    ) -> Result<(), Error>
    where
        A: sqlx::Acquire<'c, Database = sqlx::Postgres> + Send + 'a,
    {
        let value = serde_json::to_value(data.data)?;
        queries::InsertJobOne::builder()
            .job_data(&value)
            .max_attempts(data.max_attempts.into())
            .build()
            .execute(tx)
            .await?;
        Ok(())
    }
}
