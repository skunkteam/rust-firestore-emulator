macro_rules! unimplemented {
    ($name:expr) => {{
        use tonic::Status;
        let msg = format!("{} is not supported yet ({}:{})", $name, file!(), line!());
        eprintln!("{msg}");
        return Err(Status::unimplemented(msg));
    }};
}

macro_rules! unimplemented_option {
    ($val:expr) => {
        if $val.is_some() {
            unimplemented!(stringify!($val));
        }
    };
}

macro_rules! unimplemented_collection {
    ($val:expr) => {
        if !$val.is_empty() {
            unimplemented!(stringify!($val));
        }
    };
}

macro_rules! unimplemented_bool {
    ($val:expr) => {
        if $val {
            unimplemented!(stringify!($val));
        }
    };
}

macro_rules! mandatory {
    ($val:expr) => {
        $val.ok_or_else(|| Status::invalid_argument(concat!("missing ", stringify!($val))))?
    };
}
