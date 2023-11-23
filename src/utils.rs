use prost_types::Timestamp;
use std::cmp;

#[macro_export]
macro_rules! unimplemented {
    ($name:expr) => {{
        let msg = concat!($name, " is not supported yet (", file!(), ":", line!(), ")");
        eprintln!("{msg}");
        return Err(Status::unimplemented(msg));
    }};
}

#[macro_export]
macro_rules! unimplemented_option {
    ($val:expr) => {
        if $val.is_some() {
            $crate::unimplemented!(stringify!($val))
        }
    };
}

#[macro_export]
macro_rules! unimplemented_collection {
    ($val:expr) => {
        if !$val.is_empty() {
            $crate::unimplemented!(stringify!($val))
        }
    };
}

#[macro_export]
macro_rules! unimplemented_bool {
    ($val:expr) => {
        if $val {
            $crate::unimplemented!(stringify!($val))
        }
    };
}

#[derive(Eq, PartialEq)]
pub struct CmpTimestamp<'a>(pub &'a Timestamp);

impl<'a> PartialOrd for CmpTimestamp<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<'a> Ord for CmpTimestamp<'a> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        match self.0.seconds.cmp(&other.0.seconds) {
            result @ (cmp::Ordering::Less | cmp::Ordering::Greater) => result,
            cmp::Ordering::Equal => self.0.nanos.cmp(&other.0.nanos),
        }
    }
}
