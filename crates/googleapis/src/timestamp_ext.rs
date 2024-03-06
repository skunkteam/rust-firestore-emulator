use std::{fmt, sync::Mutex};

use chrono::{DateTime, NaiveDateTime, TimeZone, Utc};
use thiserror::Error;

use crate::google::protobuf::Timestamp;

#[derive(Debug, Error)]
#[error("Invalid token: {0:?}")]
pub struct InvalidTokenError(Vec<u8>);

// Convenience to be able to use tonic's `Status::invalid_argument()``.
impl From<InvalidTokenError> for String {
    fn from(val: InvalidTokenError) -> Self {
        val.to_string()
    }
}

#[derive(Debug, Error)]
#[error("Timestamp out of range")]
pub struct TimestampOutOfRangeError;

// Convenience to be able to use tonic's `Status::invalid_argument()``.
impl From<TimestampOutOfRangeError> for String {
    fn from(val: TimestampOutOfRangeError) -> Self {
        val.to_string()
    }
}

impl Timestamp {
    /// Returns the current time as a `Timestamp` with the added guarantee that all calls will
    /// return a strictly higher timestamp than all calls before that (for the execution of the
    /// program).
    pub fn now() -> Timestamp {
        static LAST: Mutex<Timestamp> = Mutex::new(Timestamp {
            seconds: 0,
            nanos:   0,
        });
        let mut last = LAST.lock().unwrap();
        let mut timestamp = Utc::now().into();
        if timestamp <= *last {
            timestamp = Timestamp {
                seconds: last.seconds,
                nanos:   last.nanos + 1,
            }
        }
        last.clone_from(&timestamp);
        timestamp
    }

    pub fn from_token(token: Vec<u8>) -> Result<Self, InvalidTokenError> {
        let nanos = i64::from_ne_bytes(token.try_into().map_err(InvalidTokenError)?);
        Ok(NaiveDateTime::from_timestamp_nanos(nanos).unwrap().into())
    }

    pub fn get_token(&self) -> Result<Vec<u8>, TimestampOutOfRangeError> {
        Ok(NaiveDateTime::try_from(self)?
            .timestamp_nanos_opt()
            .ok_or(TimestampOutOfRangeError)?
            .to_ne_bytes()
            .into())
    }
}

impl fmt::Display for Timestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match NaiveDateTime::try_from(self) {
            Ok(dt) => dt.and_utc().format("%+").fmt(f),
            Err(TimestampOutOfRangeError) => f.write_str("<Timestamp outside common era>"),
        }
    }
}

impl From<&NaiveDateTime> for Timestamp {
    fn from(dt: &NaiveDateTime) -> Self {
        Timestamp {
            seconds: dt.timestamp(),
            nanos:   dt.timestamp_subsec_nanos() as i32,
        }
    }
}

impl From<NaiveDateTime> for Timestamp {
    fn from(dt: NaiveDateTime) -> Self {
        (&dt).into()
    }
}

impl<Tz: TimeZone> From<&DateTime<Tz>> for Timestamp {
    fn from(dt: &DateTime<Tz>) -> Self {
        dt.naive_utc().into()
    }
}

impl<Tz: TimeZone> From<DateTime<Tz>> for Timestamp {
    fn from(dt: DateTime<Tz>) -> Self {
        (&dt).into()
    }
}

impl TryFrom<&Timestamp> for NaiveDateTime {
    type Error = TimestampOutOfRangeError;

    fn try_from(value: &Timestamp) -> Result<Self, Self::Error> {
        let nanos = u32::try_from(value.nanos).map_err(|_| TimestampOutOfRangeError)?;
        NaiveDateTime::from_timestamp_opt(value.seconds, nanos).ok_or(TimestampOutOfRangeError)
    }
}

#[cfg(test)]
mod tests {
    use std::array;

    use chrono::NaiveDate;
    use itertools::Itertools;
    use tonic::Status;

    use super::*;

    #[test]
    fn tonic_status_compat() {
        Status::invalid_argument(InvalidTokenError(vec![1, 2, 3]));
        Status::invalid_argument(TimestampOutOfRangeError);
        assert_eq!(
            InvalidTokenError(vec![1, 2, 3]).to_string(),
            "Invalid token: [1, 2, 3]"
        );
        assert_eq!(
            TimestampOutOfRangeError.to_string(),
            "Timestamp out of range"
        );
    }

    #[test]
    fn now_monotonically_increasing() {
        let timestamps: [Timestamp; 100] = array::from_fn(|_| Timestamp::now());
        for (a, b) in timestamps.into_iter().tuple_windows() {
            assert!(a < b, "a should be strictly less than b, always");
        }
    }

    #[test]
    fn to_from_token() {
        let ndt = NaiveDate::from_ymd_opt(2001, 2, 3)
            .unwrap()
            .and_hms_milli_opt(4, 5, 6, 789)
            .unwrap();
        let timestamp: Timestamp = ndt.into();
        let token = timestamp.get_token().unwrap();
        assert_eq!(token.len(), 8);
        let timestamp2 = Timestamp::from_token(token).unwrap();
        assert_eq!(timestamp, timestamp2);
    }

    #[test]
    fn timestamp_display() {
        let timestamp: Timestamp = NaiveDate::from_ymd_opt(2001, 2, 3)
            .unwrap()
            .and_hms_milli_opt(4, 5, 6, 789)
            .unwrap()
            .into();
        assert_eq!(timestamp.to_string(), "2001-02-03T04:05:06.789+00:00");

        let timestamp = Timestamp {
            seconds: i64::MAX,
            nanos:   0,
        };
        assert_eq!(timestamp.to_string(), "<Timestamp outside common era>");
    }
}
