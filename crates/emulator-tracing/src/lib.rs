use thiserror::Error;
use time::{error::IndeterminateOffset, macros::format_description, UtcOffset};
use tracing_subscriber::{
    filter::ParseError,
    fmt::{format::FmtSpan, time::OffsetTime},
    layer::SubscriberExt,
    reload,
    util::SubscriberInitExt,
    EnvFilter, Layer, Registry,
};

#[allow(missing_copy_implementations)]
#[derive(Debug, Error)]
pub enum TracingStartError {
    #[error("cannot determine local timezone")]
    CannotGetLocalTimezone(#[from] IndeterminateOffset),
}

#[derive(Debug)]
pub struct DefaultTracing {
    reload_handle: reload::Handle<EnvFilter, Registry>,
}

impl DefaultTracing {
    pub fn start() -> Result<Self, TracingStartError> {
        let time_offset = UtcOffset::current_local_offset()?;
        let time_format = format_description!("[hour]:[minute]:[second].[subsecond digits:6]");

        let filter = EnvFilter::from_default_env();
        let (filter, reload_handle) = reload::Layer::new(filter);
        let registry = tracing_subscriber::registry().with(
            tracing_subscriber::fmt::layer()
                .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE)
                .with_timer(OffsetTime::new(time_offset, time_format))
                .with_filter(filter),
        );

        #[cfg(feature = "console")]
        let registry = registry.with(console_subscriber::spawn());

        registry.init();

        Ok(Self { reload_handle })
    }
}

#[derive(Debug, Error)]
pub enum SetLogLevelsError {
    #[error(transparent)]
    InvalidDirectives(#[from] ParseError),
    #[error("could not set new log-levels")]
    ReloadError(#[from] reload::Error),
}

pub trait Tracing {
    fn set_log_levels(&self, dirs: &str) -> Result<(), SetLogLevelsError>;
}

impl Tracing for DefaultTracing {
    fn set_log_levels(&self, dirs: &str) -> Result<(), SetLogLevelsError> {
        let new_filter = EnvFilter::try_new(dirs)?;
        self.reload_handle.reload(new_filter)?;
        Ok(())
    }
}
