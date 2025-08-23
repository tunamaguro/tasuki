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

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct PgDateTime(pub std::time::SystemTime);

impl sqlx::Type<sqlx::Postgres> for PgDateTime {
    fn type_info() -> <sqlx::Postgres as sqlx::Database>::TypeInfo {
        // COPY FROM https://github.com/postgres/postgres/blob/master/src/include/catalog/pg_type.dat#L306-L311
        use sqlx::postgres;
        postgres::PgTypeInfo::with_name("timestamptz")
    }
}

impl<'q> sqlx::Encode<'q, sqlx::Postgres> for PgDateTime {
    fn encode_by_ref(
        &self,
        buf: &mut <sqlx::Postgres as sqlx::Database>::ArgumentBuffer<'q>,
    ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
        const OUT_OF_RANGE_MESSAGE: &str = "timestamp out of range for PostgreSQL i64 micros";

        let pg_us = match self.0.duration_since(*POSTGRESQL_TIMESTAMP) {
            Ok(d) => i64::try_from(d.as_micros()).map_err(|_| OUT_OF_RANGE_MESSAGE)?,
            Err(e) => {
                let micro = e.duration().as_micros();
                i64::try_from(micro)
                    .map(|v| -v)
                    .map_err(|_| OUT_OF_RANGE_MESSAGE)?
            }
        };

        sqlx::Encode::<sqlx::Postgres>::encode(pg_us, buf)
    }

    fn size_hint(&self) -> usize {
        std::mem::size_of::<i64>()
    }
}

/// TIMESTAMPTZは`2000-01-01 00:00:00`からのマイクロ秒で表現されている
/// これはUNIXタイムスタンプから`2000-01-01 00:00:00`までの経過時間
/// https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html
const POSTGRESQL_EPOCH_DURATION: std::time::Duration = std::time::Duration::from_secs(946684800);
const POSTGRESQL_TIMESTAMP: std::cell::LazyCell<std::time::SystemTime> =
    std::cell::LazyCell::new(|| std::time::SystemTime::UNIX_EPOCH + POSTGRESQL_EPOCH_DURATION);
