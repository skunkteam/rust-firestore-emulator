use std::{sync::Mutex, time::SystemTime};

use prost_types::Timestamp;
use tonic::Status;

use crate::google::firestore::v1::target;

const NANOS_PER_SECOND: i128 = 1_000_000_000;

pub fn timestamp_nanos(ts: &Timestamp) -> i128 {
    ts.seconds as i128 * NANOS_PER_SECOND + ts.nanos as i128
}

pub fn timestamp_from_nanos(nanos: i128) -> Timestamp {
    Timestamp {
        seconds: (nanos / NANOS_PER_SECOND) as _,
        nanos:   (nanos % NANOS_PER_SECOND) as _,
    }
}

/// Returns the current time as a `Timestamp` with the added guarantee that all calls will return a
/// strictly higher timestamp than all calls before that (for the execution of the program).
pub fn timestamp() -> Timestamp {
    static LAST: Mutex<Timestamp> = Mutex::new(Timestamp {
        seconds: 0,
        nanos:   0,
    });
    let mut last = LAST.lock().unwrap();
    let mut timestamp = SystemTime::now().into();
    if timestamp_nanos(&timestamp) <= timestamp_nanos(&last) {
        timestamp = Timestamp {
            seconds: last.seconds,
            nanos:   last.nanos + 1,
        }
    }
    last.clone_from(&timestamp);
    timestamp
}

impl TryFrom<target::ResumeType> for Timestamp {
    type Error = Status;

    fn try_from(value: target::ResumeType) -> Result<Self, Self::Error> {
        match value {
            target::ResumeType::ResumeToken(token) => {
                let token = token
                    .try_into()
                    .map_err(|_| Status::invalid_argument("invalid resume token"))?;
                Ok(timestamp_from_nanos(i128::from_ne_bytes(token)))
            }
            target::ResumeType::ReadTime(time) => Ok(time),
        }
    }
}
