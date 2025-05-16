use std::{
    fmt::Debug,
    sync::{Arc, Mutex},
};

use bytes::{BufMut, Bytes, BytesMut, buf::Writer};
use metadata_filter::Filter;
use thiserror::Error;
use time::{
    UtcOffset, error::IndeterminateOffset, format_description::FormatItem,
    macros::format_description,
};
use tracing_subscriber::{
    EnvFilter, Layer, Registry,
    fmt::{MakeWriter, format::FmtSpan, time::OffsetTime, writer::MutexGuardWriter},
    prelude::*,
    reload,
};

mod metadata_filter;

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
    pub fn start() -> &'static Self {
        let timer = {
            let time_offset =
                UtcOffset::current_local_offset().unwrap_or_else(|_: IndeterminateOffset| {
                    eprintln!("Note: could not determine local timezone, logging in UTC.");
                    UtcOffset::UTC
                });
            let time_format = format_description!("[hour]:[minute]:[second].[subsecond digits:6]");
            OffsetTime::new(time_offset, time_format)
        };

        let (layer, reload_handle) = {
            let default_layer = tracing_subscriber::fmt::layer()
                .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE)
                .with_timer(timer.clone())
                .with_filter(EnvFilter::from_default_env())
                .boxed();
            let layers = vec![default_layer];
            reload::Layer::new(layers)
        };

        let registry = tracing_subscriber::registry().with(layer);

        #[cfg(feature = "console")]
        let registry = registry.with(console_subscriber::spawn());

        registry.init();

        Box::leak(Box::new(Self {
            reload_handle,
            timer,
        }))
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
    fn subscribe(&self, dirs: &str) -> Result<impl DynamicSubscriber + '_, SetLogLevelsError>;
}

pub trait DynamicSubscriber: Send {
    fn drain(&mut self) -> Bytes;
}

impl Tracing for DefaultTracing {
    fn subscribe(
        &self,
        directives: &str,
    ) -> Result<impl DynamicSubscriber + '_, SetLogLevelsError> {
        let filter: Filter = directives.parse()?;
        let buffer = SharedBuffer::new();
        // Cannot use a filter layer here because of: https://github.com/tokio-rs/tracing/issues/1629
        // If that gets fixes we would really like that, for now just filter at the writer.
        let buffer_writer = buffer
            .clone()
            .with_filter(move |metadata| filter.allows(metadata));
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
    buffer:  SharedBuffer,
}

impl DynamicSubscriber for DefaultDynamicSubscriber<'_> {
    fn drain(&mut self) -> Bytes {
        self.buffer.drain()
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

#[derive(Clone)]
struct SharedBuffer {
    buffer: Arc<Mutex<Writer<BytesMut>>>,
}

impl SharedBuffer {
    fn new() -> Self {
        Self {
            buffer: Arc::new(Mutex::new(BytesMut::new().writer())),
        }
    }

    fn drain(&self) -> Bytes {
        self.buffer.lock().unwrap().get_mut().split().freeze()
    }
}
impl<'a> MakeWriter<'a> for SharedBuffer {
    type Writer = MutexGuardWriter<'a, Writer<BytesMut>>;

    fn make_writer(&'a self) -> Self::Writer {
        self.buffer.make_writer()
    }
}

// Disabled clippy::borrowed_box, because in this case we explicitly want the Box and not some
// possibly random reference
#[allow(clippy::borrowed_box)]
fn ptr<T: ?Sized>(b: &Box<T>) -> usize {
    b.as_ref() as *const T as *const () as usize
}
