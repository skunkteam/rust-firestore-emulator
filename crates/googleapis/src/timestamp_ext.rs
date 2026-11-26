use std::{
    fmt,
    sync::{Mutex, PoisonError},
};

use thiserror::Error;
use time::{
    Duration, OffsetDateTime,
    format_description::{FormatItem, well_known::Iso8601},
    macros::format_description,
};

use crate::google::protobuf::Timestamp;

/// Minimum valid Proto3 `Timestamp` seconds (`0001-01-01T00:00:00Z`).
pub const MIN_TIMESTAMP_SECONDS: i64 = -62_135_596_800;
/// Maximum valid Proto3 `Timestamp` seconds (`9999-12-31T23:59:59Z`).
pub const MAX_TIMESTAMP_SECONDS: i64 = 253_402_300_799;

const NANOS_PER_SECOND: i128 = 1_000_000_000;
/// Minimum valid Proto3 `Timestamp` Unix nanoseconds (`0001-01-01T00:00:00.000000000Z`).
const MIN_TIMESTAMP_NANOS: i128 = MIN_TIMESTAMP_SECONDS as i128 * NANOS_PER_SECOND;
/// Maximum valid Proto3 `Timestamp` Unix nanoseconds (`9999-12-31T23:59:59.999999999Z`).
const MAX_TIMESTAMP_NANOS: i128 =
    MAX_TIMESTAMP_SECONDS as i128 * NANOS_PER_SECOND + (NANOS_PER_SECOND - 1);

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

fn next_from_mutex(mutex: &Mutex<Timestamp>, now: Timestamp) -> Timestamp {
    let mut last = mutex.lock().unwrap_or_else(PoisonError::into_inner);
    let timestamp = next_monotonic(*last, now);
    *last = timestamp;
    timestamp
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
        let now = OffsetDateTime::now_utc().into();
        next_from_mutex(&LAST, now)
    }

    pub fn from_token(token: Vec<u8>) -> Result<Self, InvalidTokenError> {
        let nanos = i128::from_be_bytes(token.try_into().map_err(|_| InvalidTokenError)?);
        if !(MIN_TIMESTAMP_NANOS..=MAX_TIMESTAMP_NANOS).contains(&nanos) {
            return Err(InvalidTokenError);
        }
        Ok(Self {
            seconds: nanos.div_euclid(NANOS_PER_SECOND) as i64,
            nanos:   nanos.rem_euclid(NANOS_PER_SECOND) as i32,
        })
    }

    pub fn get_token(self) -> Result<Vec<u8>, TimestampOutOfRangeError> {
        if !(MIN_TIMESTAMP_SECONDS..=MAX_TIMESTAMP_SECONDS).contains(&self.seconds)
            || !(0..1_000_000_000).contains(&self.nanos)
        {
            return Err(TimestampOutOfRangeError(None));
        }
        let nanos = i128::from(self.seconds) * NANOS_PER_SECOND + i128::from(self.nanos);
        Ok(nanos.to_be_bytes().to_vec())
    }

    /// Formats this timestamp as UTC `YYYYMMDDTHHMMSSZ`, falling back to Unix epoch
    /// (`19700101T000000Z`) when outside Proto3 `0001-01-01T00:00:00Z`..=
    /// `9999-12-31T23:59:59.999999999Z` range or when nanoseconds are out of range.
    pub fn format_utc_compact(self) -> String {
        const COMPACT_UTC_FORMAT: &[FormatItem<'static>] =
            format_description!("[year][month][day]T[hour][minute][second]Z");
        let dt = if (MIN_TIMESTAMP_SECONDS..=MAX_TIMESTAMP_SECONDS).contains(&self.seconds) {
            OffsetDateTime::try_from(self).unwrap_or(OffsetDateTime::UNIX_EPOCH)
        } else {
            OffsetDateTime::UNIX_EPOCH
        };
        dt.format(COMPACT_UTC_FORMAT)
            .expect("valid UTC timestamp format")
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
        for pair in timestamps.windows(2) {
            expect_lt!(pair[0], pair[1], "a should be strictly less than b, always");
        }
    }

    #[gtest]
    fn to_from_token() {
        let ndt = datetime!(2001-02-03 04:05:06.123456789 UTC);
        let timestamp: Timestamp = ndt.into();
        let token = timestamp.get_token().unwrap();
        expect_that!(token, len(eq(TOKEN_LEN)));
        expect_eq!(token, ndt.unix_timestamp_nanos().to_be_bytes().to_vec());
        let timestamp2 = Timestamp::from_token(token).unwrap();
        expect_eq!(timestamp, timestamp2);
    }

    #[gtest]
    fn poisoned_monotonic_mutex_recovers_and_preserves_monotonicity() {
        let mutex = Mutex::new(Timestamp {
            seconds: 100,
            nanos:   500,
        });
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let _guard = mutex.lock().unwrap();
                panic!("simulated panic while holding timestamp mutex");
            }))
            .is_err()
        );
        assert!(mutex.is_poisoned());

        let t1 = next_from_mutex(
            &mutex,
            Timestamp {
                seconds: 90,
                nanos:   0,
            },
        );
        expect_eq!(
            t1,
            Timestamp {
                seconds: 100,
                nanos:   501,
            }
        );

        let t2 = next_from_mutex(
            &mutex,
            Timestamp {
                seconds: 105,
                nanos:   0,
            },
        );
        expect_eq!(
            t2,
            Timestamp {
                seconds: 105,
                nanos:   0,
            }
        );
    }

    #[gtest]
    fn token_encoding_is_big_endian_and_enforces_proto3_year_bounds() {
        // Big-endian stability: epoch zero, -1ns (pre-1970), and Proto3 min/max bounds.
        let epoch = Timestamp {
            seconds: 0,
            nanos:   0,
        };
        expect_eq!(epoch.get_token().unwrap(), vec![0u8; TOKEN_LEN]);
        expect_eq!(Timestamp::from_token(vec![0u8; TOKEN_LEN]).unwrap(), epoch);

        let pre_epoch_1ns = Timestamp {
            seconds: -1,
            nanos:   999_999_999,
        };
        let pre_epoch_dt = datetime!(1969-12-31 23:59:59.999999999 UTC);
        expect_eq!(
            pre_epoch_1ns.get_token().unwrap(),
            pre_epoch_dt.unix_timestamp_nanos().to_be_bytes().to_vec()
        );
        expect_eq!(pre_epoch_1ns.get_token().unwrap(), vec![0xffu8; TOKEN_LEN]);
        expect_eq!(
            Timestamp::from_token(vec![0xffu8; TOKEN_LEN]).unwrap(),
            pre_epoch_1ns
        );

        // Positive timestamp big-endian byte layout (most-significant byte first).
        let one_sec = Timestamp {
            seconds: 1,
            nanos:   0,
        };
        expect_eq!(
            one_sec.get_token().unwrap(),
            1_000_000_000_i128.to_be_bytes().to_vec()
        );

        // Exact Proto3 lower/upper bounds succeed and round-trip.
        let min_proto3 = Timestamp {
            seconds: MIN_TIMESTAMP_SECONDS,
            nanos:   0,
        };
        let max_proto3 = Timestamp {
            seconds: MAX_TIMESTAMP_SECONDS,
            nanos:   999_999_999,
        };
        for bound in [min_proto3, max_proto3] {
            let dt = OffsetDateTime::try_from(bound).unwrap();
            let token = bound.get_token().unwrap();
            expect_eq!(token, dt.unix_timestamp_nanos().to_be_bytes().to_vec());
            expect_eq!(Timestamp::from_token(token).unwrap(), bound);
        }

        // Pre-0001 and post-9999 timestamps are rejected by get_token and from_token.
        for oob_ts in [
            Timestamp {
                seconds: MIN_TIMESTAMP_SECONDS - 1,
                nanos:   999_999_999,
            },
            Timestamp {
                seconds: MIN_TIMESTAMP_SECONDS - 1,
                nanos:   0,
            },
            Timestamp {
                seconds: MAX_TIMESTAMP_SECONDS + 1,
                nanos:   0,
            },
        ] {
            expect_that!(oob_ts.get_token(), err(pat!(TimestampOutOfRangeError(_))));
        }

        for oob_nanos in [
            MIN_TIMESTAMP_NANOS - 1,
            MAX_TIMESTAMP_NANOS + 1,
            i128::MIN,
            i128::MAX,
        ] {
            expect_that!(
                Timestamp::from_token(oob_nanos.to_be_bytes().to_vec()),
                err(pat!(InvalidTokenError))
            );
        }
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

    #[gtest]
    fn timestamp_format_utc_compact() {
        let timestamp: Timestamp = datetime!(2026-11-26 09:05:06.123456789 UTC).into();
        expect_eq!(timestamp.format_utc_compact(), "20261126T090506Z");

        let min_ts = Timestamp {
            seconds: -62_135_596_800,
            nanos:   0,
        };
        expect_eq!(min_ts.format_utc_compact(), "00010101T000000Z");

        let max_ts = Timestamp {
            seconds: 253_402_300_799,
            nanos:   999_999_999,
        };
        expect_eq!(max_ts.format_utc_compact(), "99991231T235959Z");

        for out_of_range in [
            Timestamp {
                seconds: i64::MAX,
                nanos:   0,
            },
            Timestamp {
                seconds: i64::MIN,
                nanos:   0,
            },
            Timestamp {
                seconds: MIN_TIMESTAMP_SECONDS - 1,
                nanos:   999_999_999,
            },
            Timestamp {
                seconds: MIN_TIMESTAMP_SECONDS - 1,
                nanos:   0,
            },
            Timestamp {
                seconds: -62_167_219_200,
                nanos:   0,
            },
            Timestamp {
                seconds: MAX_TIMESTAMP_SECONDS + 1,
                nanos:   0,
            },
            Timestamp {
                seconds: 1_700_000_000,
                nanos:   -1,
            },
            Timestamp {
                seconds: 1_700_000_000,
                nanos:   1_000_000_000,
            },
        ] {
            expect_eq!(out_of_range.format_utc_compact(), "19700101T000000Z");
        }
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
