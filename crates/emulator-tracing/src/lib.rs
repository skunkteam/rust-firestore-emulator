use std::{
    fmt::Debug,
    sync::{Arc, Mutex, OnceLock},
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
    /// duration of the entire program. Calling `start()` more than once within the same OS
    /// process returns the existing global handle instead of panicking.
    pub fn start() -> &'static Self {
        static INSTANCE: OnceLock<DefaultTracing> = OnceLock::new();
        INSTANCE.get_or_init(|| {
            let timer = {
                let time_offset =
                    UtcOffset::current_local_offset().unwrap_or_else(|_: IndeterminateOffset| {
                        eprintln!("Note: could not determine local timezone, logging in UTC.");
                        UtcOffset::UTC
                    });
                let time_format =
                    format_description!("[hour]:[minute]:[second].[subsecond digits:6]");
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

            Self {
                reload_handle,
                timer,
            }
        })
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
        // Use `FmtSpan::NEW` (not `FmtSpan::CLOSE`) on dynamically attached layers: `fmt::Layer`
        // stores a single shared `Timings` counter in `Registry` span extensions for
        // `FmtSpan::CLOSE`, so pushing a second `FmtSpan::CLOSE` layer while another thread
        // is inside an entered span causes `timings.entered` to underflow on exit
        // (`fmt_layer.rs`).
        let layer = tracing_subscriber::fmt::layer()
            .with_span_events(FmtSpan::NEW)
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

#[cfg(test)]
mod tests {
    use std::ptr;

    use googletest::prelude::*;

    use super::{DefaultTracing, DynamicSubscriber, SetLogLevelsError, Tracing};

    #[gtest]
    fn start_is_idempotent_and_captures_filtered_events() {
        let first = DefaultTracing::start();
        let second = DefaultTracing::start();
        expect_true!(ptr::eq(first, second));

        let mut sub = second
            .subscribe("emulator_tracing_test_target=debug")
            .unwrap();
        tracing::info!(target: "other_target", "should be filtered out");
        tracing::debug!(target: "emulator_tracing_test_target", "hello dynamic capture");

        let captured = String::from_utf8(sub.drain().to_vec()).unwrap();
        expect_that!(captured, contains_substring("hello dynamic capture"));
        expect_that!(captured, not(contains_substring("should be filtered out")));

        // Subsequent drain without new matching events is empty.
        expect_true!(sub.drain().is_empty());
        drop(sub);

        // Invalid directive fails cleanly without poisoning the global handle.
        let bad = first.subscribe("not valid=directive");
        expect_that!(
            bad.err(),
            some(pat!(SetLogLevelsError::InvalidDirectives(_)))
        );

        // Regression test: attaching and dropping a dynamic subscriber while an instrumented span
        // is already entered on a thread must never underflow `tracing_subscriber`'s
        // `Timings` counter.
        let active_span = tracing::info_span!(target: "emulator_tracing_test_target", "in_flight");
        let entered = active_span.enter();
        let mid_span_sub = first
            .subscribe("emulator_tracing_test_target=trace")
            .unwrap();
        drop(entered);
        drop(active_span);
        drop(mid_span_sub);
    }
}
