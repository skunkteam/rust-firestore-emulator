use std::{fmt, sync::Mutex};

use thiserror::Error;
use time::{Duration, OffsetDateTime, format_description::well_known::Iso8601};

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
pub struct TimestampOutOfRangeError(Option<time::error::ComponentRange>);

impl From<time::error::ComponentRange> for TimestampOutOfRangeError {
    fn from(err: time::error::ComponentRange) -> Self {
        Self(Some(err))
    }
}

// Convenience to be able to use tonic's `Status::invalid_argument()``.
impl From<TimestampOutOfRangeError> for String {
    fn from(val: TimestampOutOfRangeError) -> Self {
        val.to_string()
    }
}

fn next_monotonic(last: Timestamp, now: Timestamp) -> Timestamp {
    if now > last {
        now
    } else if last.nanos >= 999_999_999 {
        Timestamp {
            seconds: last.seconds + 1,
            nanos:   0,
        }
    } else {
        Timestamp {
            seconds: last.seconds,
            nanos:   last.nanos + 1,
        }
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
        let timestamp = next_monotonic(*last, OffsetDateTime::now_utc().into());
        *last = timestamp;
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
        if !(0..1_000_000_000).contains(&value.nanos) {
            return Err(TimestampOutOfRangeError(None));
        }
        let date_time = OffsetDateTime::from_unix_timestamp(value.seconds)?
            + Duration::nanoseconds(i64::from(value.nanos));
        Ok(date_time)
    }
}

#[cfg(test)]
mod tests {
    use std::array;

    use googletest::prelude::*;
    use itertools::Itertools;
    use rstest::rstest;
    use time::macros::datetime;

    use super::*;

    const TOKEN_LEN: usize = 16;

    #[gtest]
    #[rstest]
    #[case(vec![])]
    // Using 127 to get a high number because the token is signed (0 would mean 1970-01-01,
    // 255 results in a -1_i128 which would mean end of 1769-12-31)
    #[case(vec![127; TOKEN_LEN])]
    fn invalid_tokens(#[case] token: Vec<u8>) {
        expect_that!(Timestamp::from_token(token), err(pat!(InvalidTokenError)));
    }

    #[gtest]
    #[rstest]
    #[case(Timestamp { seconds: i64::MAX, nanos: 0 })]
    #[case(Timestamp { seconds: i64::MIN, nanos: 0 })]
    #[case(Timestamp { seconds: 1_700_000_000, nanos: -1 })]
    #[case(Timestamp { seconds: 1_700_000_000, nanos: i32::MIN })]
    #[case(Timestamp { seconds: 1_700_000_000, nanos: 1_000_000_000 })]
    #[case(Timestamp { seconds: 1_700_000_000, nanos: i32::MAX })]
    fn out_of_range_timestamps(#[case] timestamp: Timestamp) {
        expect_that!(
            OffsetDateTime::try_from(timestamp),
            err(pat!(TimestampOutOfRangeError(_)))
        );
        expect_that!(
            timestamp.get_token(),
            err(pat!(TimestampOutOfRangeError(_)))
        );
    }

    #[gtest]
    #[rstest]
    #[case::advances_when_now_is_ahead(
        Timestamp { seconds: 10, nanos: 999_999_999 },
        Timestamp { seconds: 11, nanos: 5 },
        Timestamp { seconds: 11, nanos: 5 },
    )]
    #[case::increments_nanos_when_clock_stalls(
        Timestamp { seconds: 10, nanos: 41 },
        Timestamp { seconds: 10, nanos: 41 },
        Timestamp { seconds: 10, nanos: 42 },
    )]
    #[case::rolls_over_at_max_nanosecond(
        Timestamp { seconds: 10, nanos: 999_999_999 },
        Timestamp { seconds: 10, nanos: 999_999_999 },
        Timestamp { seconds: 11, nanos: 0 },
    )]
    #[case::rolls_over_when_wall_clock_steps_backward_at_max_nanos(
        Timestamp { seconds: 10, nanos: 999_999_999 },
        Timestamp { seconds: 9, nanos: 500_000_000 },
        Timestamp { seconds: 11, nanos: 0 },
    )]
    fn next_monotonic_preserves_valid_nanosecond_bounds(
        #[case] last: Timestamp,
        #[case] now: Timestamp,
        #[case] expected: Timestamp,
    ) {
        let next = next_monotonic(last, now);
        expect_eq!(next, expected);
        expect_gt!(next, last);
        expect_true!((0..1_000_000_000).contains(&next.nanos));
    }

    #[gtest]
    fn tonic_status_compat() {
        expect_that!(InvalidTokenError, displays_as(eq("Invalid token")));
        expect_that!(
            tonic::Status::invalid_argument(InvalidTokenError).message(),
            eq("Invalid token")
        );

        let out_of_range = OffsetDateTime::try_from(Timestamp {
            seconds: i64::MAX,
            nanos:   0,
        })
        .unwrap_err();
        expect_that!(
            out_of_range.clone(),
            displays_as(eq("Timestamp out of range"))
        );
        expect_that!(
            tonic::Status::invalid_argument(out_of_range).message(),
            eq("Timestamp out of range")
        );
    }

    #[gtest]
    fn now_monotonically_increasing() {
        let timestamps: [Timestamp; 100] = array::from_fn(|_| Timestamp::now());
        for [a, b] in timestamps.into_iter().array_windows() {
            expect_lt!(a, b, "a should be strictly less than b, always");
        }
    }

    #[gtest]
    fn to_from_token() {
        let ndt = datetime!(2001-02-03 04:05:06.123456789 UTC);
        let timestamp: Timestamp = ndt.into();
        let token = timestamp.get_token().unwrap();
        expect_that!(token, len(eq(TOKEN_LEN)));
        let timestamp2 = Timestamp::from_token(token).unwrap();
        expect_eq!(timestamp, timestamp2);
    }

    #[gtest]
    fn timestamp_display() {
        let timestamp: Timestamp = datetime!(2001-02-03 04:05:06.123456789 UTC).into();
        expect_that!(timestamp, displays_as(eq("2001-02-03T04:05:06.123456789Z")));

        let timestamp = Timestamp {
            seconds: i64::MAX,
            nanos:   0,
        };
        expect_that!(timestamp, displays_as(eq("<Timestamp outside common era>")));
    }

    mod proptests {
        use proptest::prelude::*;
        use time::OffsetDateTime;

        use super::{Timestamp, next_monotonic};

        proptest! {
            #[test]
            fn proptest_next_monotonic_strictly_increases_and_keeps_valid_nanos(
                last_secs in -62_135_596_800_i64..253_402_300_798_i64,
                last_nanos in 0_i32..1_000_000_000_i32,
                now_secs in -62_135_596_800_i64..=253_402_300_799_i64,
                now_nanos in 0_i32..1_000_000_000_i32,
            ) {
                let last = Timestamp { seconds: last_secs, nanos: last_nanos };
                let now = Timestamp { seconds: now_secs, nanos: now_nanos };
                let next = next_monotonic(last, now);
                prop_assert!(next > last);
                prop_assert!(next >= now);
                prop_assert!((0..1_000_000_000).contains(&next.nanos));
                let token = next.get_token().unwrap();
                let roundtripped = Timestamp::from_token(token).unwrap();
                prop_assert_eq!(roundtripped, next);
            }

            #[test]
            fn proptest_invalid_nanos_always_rejected(
                secs in -62_135_596_800_i64..=253_402_300_799_i64,
                bad_nanos in prop_oneof![i32::MIN..0_i32, 1_000_000_000_i32..=i32::MAX],
            ) {
                let ts = Timestamp { seconds: secs, nanos: bad_nanos };
                prop_assert!(OffsetDateTime::try_from(ts).is_err());
                prop_assert!(ts.get_token().is_err());
            }
        }
    }
}
