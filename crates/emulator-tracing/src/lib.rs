use std::{
    collections::VecDeque,
    fmt::Debug,
    io, mem,
    sync::{Arc, Mutex, MutexGuard, OnceLock, PoisonError},
};

use bytes::Bytes;
use metadata_filter::Filter;
use thiserror::Error;
use time::{
    UtcOffset, error::IndeterminateOffset, format_description::FormatItem,
    macros::format_description,
};
use tracing_subscriber::{
    EnvFilter, Layer, Registry,
    fmt::{MakeWriter, format::FmtSpan, time::OffsetTime},
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

const DEFAULT_MAX_BUFFER_BYTES: usize = 16 * 1024 * 1024;

const fn is_utf8_cont_byte(b: u8) -> bool {
    (b & 0b1100_0000) == 0b1000_0000
}

#[derive(Clone)]
struct SharedBuffer {
    buffer:    Arc<Mutex<VecDeque<u8>>>,
    max_bytes: usize,
}

impl SharedBuffer {
    fn new() -> Self {
        Self::with_max_bytes(DEFAULT_MAX_BUFFER_BYTES)
    }

    fn with_max_bytes(max_bytes: usize) -> Self {
        Self {
            buffer: Arc::new(Mutex::new(VecDeque::new())),
            max_bytes,
        }
    }

    fn drain(&self) -> Bytes {
        let mut guard = self.buffer.lock().unwrap_or_else(PoisonError::into_inner);
        if guard.is_empty() {
            return Bytes::new();
        }
        Bytes::from(Vec::from(mem::take(&mut *guard)))
    }
}

struct SharedBufferWriter<'a> {
    guard:     MutexGuard<'a, VecDeque<u8>>,
    max_bytes: usize,
}

impl io::Write for SharedBufferWriter<'_> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if self.max_bytes == 0 || buf.is_empty() {
            return Ok(buf.len());
        }
        if buf.len() >= self.max_bytes {
            let truncated = !self.guard.is_empty() || buf.len() > self.max_bytes;
            self.guard.clear();
            let mut start = buf.len() - self.max_bytes;
            if truncated {
                while start < buf.len() && is_utf8_cont_byte(buf[start]) {
                    start += 1;
                }
            }
            self.guard.extend(&buf[start..]);
        } else {
            let overflow = self
                .guard
                .len()
                .saturating_add(buf.len())
                .saturating_sub(self.max_bytes);
            if overflow > 0 {
                self.guard.drain(..overflow);
            }
            self.guard.extend(buf);
            if overflow > 0 {
                while self.guard.front().is_some_and(|&b| is_utf8_cont_byte(b)) {
                    self.guard.pop_front();
                }
            }
        }
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<'a> MakeWriter<'a> for SharedBuffer {
    type Writer = SharedBufferWriter<'a>;

    fn make_writer(&'a self) -> Self::Writer {
        SharedBufferWriter {
            guard:     self.buffer.lock().unwrap_or_else(PoisonError::into_inner),
            max_bytes: self.max_bytes,
        }
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

    use super::{DefaultTracing, DynamicSubscriber, SetLogLevelsError, SharedBuffer, Tracing};

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

    #[gtest]
    fn shared_buffer_caps_retained_bytes_and_recovers_from_poison() {
        use std::io::Write;

        use tracing_subscriber::fmt::MakeWriter;

        let buf = SharedBuffer::with_max_bytes(10);
        {
            let mut w = buf.make_writer();
            w.write_all(b"123456").unwrap();
            w.write_all(b"7890abcd").unwrap();
        }
        expect_eq!(buf.drain().as_ref(), b"567890abcd");

        // Single write exceeding max_bytes keeps the newest tail.
        {
            let mut w = buf.make_writer();
            w.write_all(b"0123456789XYZ").unwrap();
        }
        expect_eq!(buf.drain().as_ref(), b"3456789XYZ");

        // Poisoning the inner mutex does not panic subsequent make_writer or drain calls.
        let poisoned = buf.clone();
        let panic_res = std::thread::spawn(move || {
            let _guard = poisoned.make_writer();
            panic!("intentional writer panic");
        })
        .join();
        expect_true!(panic_res.is_err());
        {
            let mut w = buf.make_writer();
            w.write_all(b"ok").unwrap();
        }
        expect_eq!(buf.drain().as_ref(), b"ok");
    }

    #[gtest]
    fn shared_buffer_truncation_preserves_valid_utf8_boundaries() {
        use std::io::Write;

        use tracing_subscriber::fmt::MakeWriter;

        let buf = SharedBuffer::with_max_bytes(10);
        {
            let mut w = buf.make_writer();
            w.write_all("éééé".as_bytes()).unwrap();
            w.write_all(b"ab").unwrap();
            w.write_all(b"z").unwrap();
        }
        let drained = buf.drain();
        assert!(
            std::str::from_utf8(&drained).is_ok(),
            "overflow truncation must preserve UTF-8 boundaries: {drained:?}"
        );

        let single_buf = SharedBuffer::with_max_bytes(10);
        {
            let mut w = single_buf.make_writer();
            w.write_all("ab🌃12345678".as_bytes()).unwrap();
        }
        let single_drained = single_buf.drain();
        assert!(
            std::str::from_utf8(&single_drained).is_ok(),
            "single-write tail truncation must preserve UTF-8 boundaries: {single_drained:?}"
        );
    }

    #[gtest]
    fn shared_buffer_sustained_writes_at_capacity_avoid_quadratic_memmove() {
        use std::{
            io::Write,
            time::{Duration, Instant},
        };

        use tracing_subscriber::fmt::MakeWriter;

        let buf = SharedBuffer::new();
        {
            let mut w = buf.make_writer();
            w.write_all(&vec![b'x'; 16 * 1024 * 1024]).unwrap();
        }

        let line = [b'a'; 64];
        let start = Instant::now();
        for _ in 0..1_500 {
            let mut w = buf.make_writer();
            w.write_all(&line).unwrap();
        }
        let elapsed = start.elapsed();
        assert!(
            elapsed < Duration::from_millis(250),
            "1500 small writes at capacity took {elapsed:?} due to quadratic memmove"
        );
    }
}
