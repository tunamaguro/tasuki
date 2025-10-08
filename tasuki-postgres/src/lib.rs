mod backend;
mod deadpool_postgres;

use bytes::{Buf, BufMut, BytesMut};

#[derive(Debug, Clone, Copy, PartialEq)]
struct PgInterval {
    /// Number of microseconds
    pub microseconds: i64,
    /// Number of days
    pub days: i32,
    /// Number of months
    pub months: i32,
}

impl<'q> postgres_types::FromSql<'q> for PgInterval {
    fn from_sql(
        ty: &postgres_types::Type,
        raw: &'q [u8],
    ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        if !Self::accepts(ty) {
            return Err(format!("expected INTERVAL, got {ty}").into());
        }

        const EXPECTED_BUFFER_SIZE: usize = 16; // i64 + i32 x 2
        let mut buf = raw;
        if buf.len() != EXPECTED_BUFFER_SIZE {
            return Err("Invalid buffer size".into());
        }

        let microseconds = buf.get_i64();
        let days = buf.get_i32();
        let months = buf.get_i32();

        Ok(Self {
            microseconds,
            days,
            months,
        })
    }

    fn accepts(ty: &postgres_types::Type) -> bool {
        ty == &postgres_types::Type::INTERVAL
    }
}

impl postgres_types::ToSql for PgInterval {
    fn to_sql(
        &self,
        ty: &postgres_types::Type,
        out: &mut BytesMut,
    ) -> Result<postgres_types::IsNull, Box<dyn std::error::Error + Sync + Send>>
    where
        Self: Sized,
    {
        if !Self::accepts(ty) {
            return Err(format!("expected INTERVAL, got {ty}").into());
        }

        out.put_i64(self.microseconds);
        out.put_i32(self.days);
        out.put_i32(self.months);
        Ok(postgres_types::IsNull::No)
    }

    fn accepts(ty: &postgres_types::Type) -> bool
    where
        Self: Sized,
    {
        ty == &postgres_types::Type::INTERVAL
    }

    postgres_types::to_sql_checked!();
}

impl std::ops::Add for PgInterval {
    type Output = Self;

    fn add(mut self, rhs: Self) -> Self::Output {
        self.microseconds = self.microseconds.saturating_add(rhs.microseconds);
        self.days = self.days.saturating_add(rhs.days);
        self.months = self.months.saturating_add(rhs.months);
        self
    }
}

impl TryFrom<std::time::Duration> for PgInterval {
    type Error = std::num::TryFromIntError;

    fn try_from(value: std::time::Duration) -> Result<Self, Self::Error> {
        let microseconds = i64::try_from(value.as_micros())?;
        Ok(PgInterval {
            microseconds,
            days: 0,
            months: 0,
        })
    }
}

#[derive(Debug, Clone, Copy)]
struct PgVoid;

impl<'q> postgres_types::FromSql<'q> for PgVoid {
    fn from_sql(
        ty: &postgres_types::Type,
        raw: &'q [u8],
    ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        if !Self::accepts(ty) {
            return Err(format!("expected VOID, got {ty}").into());
        }
        if raw.len() != 0 {
            return Err("Invalid buffer size".into());
        }

        Ok(PgVoid)
    }

    fn accepts(ty: &postgres_types::Type) -> bool {
        ty == &postgres_types::Type::VOID
    }
}

impl postgres_types::ToSql for PgVoid {
    fn to_sql(
        &self,
        ty: &postgres_types::Type,
        _out: &mut BytesMut,
    ) -> Result<postgres_types::IsNull, Box<dyn std::error::Error + Sync + Send>>
    where
        Self: Sized,
    {
        if !Self::accepts(ty) {
            return Err(format!("expected VOID, got {ty}").into());
        }
        Ok(postgres_types::IsNull::No)
    }

    fn accepts(ty: &postgres_types::Type) -> bool
    where
        Self: Sized,
    {
        ty == &postgres_types::Type::VOID
    }

    postgres_types::to_sql_checked!();
}
