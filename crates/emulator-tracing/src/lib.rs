use std::{
    fmt::Debug,
    io::Sink,
    mem,
    sync::{Arc, Mutex},
};

use metadata_filter::Filter;
use thiserror::Error;
use time::{
    error::IndeterminateOffset, format_description::FormatItem, macros::format_description,
    UtcOffset,
};
use tracing_subscriber::{
    fmt::{
        format::FmtSpan,
        time::OffsetTime,
        writer::{EitherWriter, MutexGuardWriter},
        MakeWriter,
    },
    prelude::*,
    reload, EnvFilter, Layer, Registry,
};

mod metadata_filter;

#[allow(missing_copy_implementations)]
#[derive(Debug, Error)]
pub enum TracingStartError {
    #[error("cannot determine local timezone")]
    CannotGetLocalTimezone(#[from] IndeterminateOffset),
}

type BoxedLayer = Box<dyn Layer<Registry> + Send + Sync + 'static>;

pub struct DefaultTracing {
    reload_handle: reload::Handle<Vec<BoxedLayer>, Registry>,
    timer: OffsetTime<&'static [FormatItem<'static>]>,
}

impl Debug for DefaultTracing {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DefaultTracing").finish_non_exhaustive()
    }
}

impl DefaultTracing {
    /// Will start tracing by setting the global default dispatcher for all tracing events for the
    /// duration of the entire program.
    pub fn start() -> Result<&'static Self, TracingStartError> {
        let timer = {
            let time_offset = UtcOffset::current_local_offset()?;
            let time_format = format_description!("[hour]:[minute]:[second].[subsecond digits:6]");
            OffsetTime::new(time_offset, time_format)
        };
        let default_layer = tracing_subscriber::fmt::layer()
            .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE)
            .with_timer(timer.clone())
            .with_filter(EnvFilter::from_default_env())
            .boxed();
        let layers = vec![default_layer];
        let (layer, reload_handle) = reload::Layer::new(layers);
        let registry = tracing_subscriber::registry().with(layer);

        #[cfg(feature = "console")]
        let registry = registry.with(console_subscriber::spawn());

        registry.init();

        Ok(Box::leak(Box::new(Self {
            reload_handle,
            timer,
        })))
    }
}

#[derive(Debug, Error)]
pub enum SetLogLevelsError {
    #[error(transparent)]
    InvalidDirectives(#[from] metadata_filter::ParseError),
    #[error("could not set new log-levels")]
    ReloadError(#[from] reload::Error),
}

pub trait Tracing: Send + Sync {
    fn subscribe(&self, dirs: &str) -> Result<impl DynamicSubscriber, SetLogLevelsError>;
}

pub trait DynamicSubscriber: Send {
    fn consume(&self) -> Vec<u8>;
    fn stop(self) -> Vec<u8>;
}

impl Tracing for DefaultTracing {
    fn subscribe(&self, directives: &str) -> Result<impl DynamicSubscriber, SetLogLevelsError> {
        let filter = directives.parse()?;
        let buffer = Default::default();
        let buffer_writer = BufferWriter {
            buffer: Arc::clone(&buffer),
            filter,
        };
        // Cannot use a filter layer here because of: https://github.com/tokio-rs/tracing/issues/1629
        // If that gets fixes we would really like that, for now just filter at the writer.
        let layer = tracing_subscriber::fmt::layer()
            .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE)
            .with_timer(self.timer.clone())
            .with_writer(buffer_writer)
            .boxed();
        let ptr = ptr(&layer);

        self.reload_handle.modify(|layers| layers.push(layer))?;
        Ok(DefaultDynamicSubscriber {
            ptr,
            tracing: self,
            buffer,
        })
    }
}

struct DefaultDynamicSubscriber<'a> {
    ptr:     usize,
    tracing: &'a DefaultTracing,
    buffer:  Arc<Mutex<Vec<u8>>>,
}

impl DynamicSubscriber for DefaultDynamicSubscriber<'_> {
    fn consume(&self) -> Vec<u8> {
        mem::take(&mut self.buffer.lock().unwrap())
    }

    fn stop(self) -> Vec<u8> {
        self.consume()
    }
}

impl Drop for DefaultDynamicSubscriber<'_> {
    fn drop(&mut self) {
        self.tracing
            .reload_handle
            .modify(|layers| {
                let idx = layers
                    .iter()
                    .position(|l| ptr(l) == self.ptr)
                    .expect("layer went missing in subscribers");
                layers.swap_remove(idx);
            })
            .expect("could not deactivate dynamic subscriber")
    }
}

#[derive(Debug)]
struct BufferWriter {
    buffer: Arc<Mutex<Vec<u8>>>,
    filter: Filter,
}

impl<'a> MakeWriter<'a> for BufferWriter {
    type Writer = EitherWriter<MutexGuardWriter<'a, Vec<u8>>, Sink>;

    fn make_writer(&'a self) -> Self::Writer {
        panic!("always call make_writer_for");
    }

    fn make_writer_for(&'a self, meta: &tracing::Metadata<'_>) -> Self::Writer {
        if self.filter.allows(meta) {
            println!("Found something to log from {}", meta.target());
            EitherWriter::some(self.buffer.make_writer_for(meta))
        } else {
            EitherWriter::none()
        }
    }
}

// Disabled clippy::borrowed_box, because in this case we explicitly want the Box and not some
// possibly random reference
#[allow(clippy::borrowed_box)]
fn ptr<T: ?Sized>(b: &Box<T>) -> usize {
    b.as_ref() as *const T as *const () as usize
}
