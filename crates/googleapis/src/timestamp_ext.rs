use std::{fmt, sync::Mutex};

use thiserror::Error;
use time::{format_description::well_known::Iso8601, Duration, OffsetDateTime};

use crate::google::protobuf::Timestamp;

#[derive(Clone, Debug, Error)]
#[error("Invalid token")]
pub struct InvalidTokenError;

// Convenience to be able to use tonic's `Status::invalid_argument()``.
impl From<InvalidTokenError> for String {
    fn from(val: InvalidTokenError) -> Self {
        val.to_string()
    }
}

#[derive(Clone, Debug, Error)]
#[error("Timestamp out of range")]
pub struct TimestampOutOfRangeError(#[from] time::error::ComponentRange);

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
        let mut timestamp = OffsetDateTime::now_utc().into();
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
        let nanos = i128::from_ne_bytes(token.try_into().map_err(|_| InvalidTokenError)?);
        Ok(OffsetDateTime::from_unix_timestamp_nanos(nanos)
            .map_err(|_| InvalidTokenError)?
            .into())
    }

    pub fn get_token(self) -> Result<Vec<u8>, TimestampOutOfRangeError> {
        Ok(OffsetDateTime::try_from(self)?
            .unix_timestamp_nanos()
            .to_ne_bytes()
            .to_vec())
    }
}

impl fmt::Display for Timestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // TODO: Change to non allocating after: https://github.com/time-rs/time/issues/375
        let formatted_str = OffsetDateTime::try_from(*self)
            .ok()
            .and_then(|dt| dt.format(&Iso8601::DEFAULT).ok());
        let formatted_str = formatted_str
            .as_deref()
            .unwrap_or("<Timestamp outside common era>");
        f.write_str(formatted_str)
    }
}

impl From<&OffsetDateTime> for Timestamp {
    fn from(dt: &OffsetDateTime) -> Self {
        Timestamp {
            seconds: dt.unix_timestamp(),
            nanos:   dt.nanosecond() as i32,
        }
    }
}

impl From<OffsetDateTime> for Timestamp {
    fn from(dt: OffsetDateTime) -> Self {
        (&dt).into()
    }
}

impl TryFrom<Timestamp> for OffsetDateTime {
    type Error = TimestampOutOfRangeError;

    fn try_from(value: Timestamp) -> Result<Self, Self::Error> {
        let date_time = OffsetDateTime::from_unix_timestamp(value.seconds)?
            + Duration::nanoseconds(value.nanos as i64);
        Ok(date_time)
    }
}

#[cfg(test)]
mod tests {
    use std::array;

    use itertools::Itertools;
    use rstest::rstest;
    use time::macros::datetime;

    use super::*;

    const TOKEN_LEN: usize = 16;

    #[rstest]
    #[case(vec![])]
    // Using 127 to get a high number because the token is signed (0 would mean 1970-01-01,
    // 255 results in a -1_i128 which would mean end of 1769-12-31)
    #[case(vec![127; TOKEN_LEN])]
    fn invalid_tokens(#[case] token: Vec<u8>) {
        assert!(matches!(
            Timestamp::from_token(token),
            Err(InvalidTokenError)
        ));
    }

    #[rstest]
    #[case(Timestamp { seconds: i64::MAX, nanos: 0 })]
    #[case(Timestamp { seconds: i64::MIN, nanos: 0 })]
    fn out_of_range_timestamps(#[case] timestamp: Timestamp) {
        println!("{:?}", OffsetDateTime::try_from(timestamp));
        assert!(matches!(
            OffsetDateTime::try_from(timestamp),
            Err(TimestampOutOfRangeError(_))
        ));
    }

    #[test]
    fn tonic_status_compat() {
        assert_eq!(String::from(InvalidTokenError), "Invalid token");

        let out_of_range = OffsetDateTime::try_from(Timestamp {
            seconds: i64::MAX,
            nanos:   0,
        })
        .unwrap_err();
        assert_eq!(String::from(out_of_range), "Timestamp out of range");
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
        let ndt = datetime!(2001-02-03 04:05:06.123456789 UTC);
        let timestamp: Timestamp = ndt.into();
        let token = timestamp.get_token().unwrap();
        assert_eq!(token.len(), TOKEN_LEN);
        let timestamp2 = Timestamp::from_token(token).unwrap();
        assert_eq!(timestamp, timestamp2);
    }

    #[test]
    fn timestamp_display() {
        let timestamp: Timestamp = datetime!(2001-02-03 04:05:06.123456789 UTC).into();
        assert_eq!(timestamp.to_string(), "2001-02-03T04:05:06.123456789Z");

        let timestamp = Timestamp {
            seconds: i64::MAX,
            nanos:   0,
        };
        assert_eq!(timestamp.to_string(), "<Timestamp outside common era>");
    }
}
