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
use tracing::{Event, Metadata, level_filters::LevelFilter, span, subscriber::Interest};
use tracing_subscriber::{
    EnvFilter, Layer, Registry,
    fmt::{MakeWriter, format::FmtSpan, time::OffsetTime},
    layer::Context,
    prelude::*,
    reload,
};

mod metadata_filter;

type BoxedLayer = Box<dyn Layer<Registry> + Send + Sync + 'static>;

pub struct DefaultTracing {
    reload_handle: reload::Handle<LayerUnion, Registry>,
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
                let default_layer = EnvFilteredLayer::new(
                    EnvFilter::from_default_env(),
                    tracing_subscriber::fmt::layer()
                        .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE)
                        .with_timer(timer.clone()),
                )
                .boxed();
                reload::Layer::new(LayerUnion(vec![default_layer]))
            };

            tracing_subscriber::registry().with(layer).init();

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

/// Composes independently filtered layers with union (`OR`) semantics for `register_callsite`,
/// `enabled`, `event_enabled`, and `max_level_hint`.
///
/// Unlike `Vec<BoxedLayer>` (whose `Layer::enabled` and `Layer::event_enabled` use `.all(...)`,
/// letting disjoint dynamic subscribers veto each other) and `tracing_subscriber::filter::Filtered`
/// (whose `register_callsite` always returns `Interest::always()` and cannot be added dynamically
/// due to `tokio-rs/tracing#1629`), each child layer enforces its own filter in `on_*` callbacks
/// while `LayerUnion` exposes the true union interest and max level to `Registry`.
struct LayerUnion(Vec<BoxedLayer>);

impl Layer<Registry> for LayerUnion {
    fn register_callsite(&self, metadata: &'static Metadata<'static>) -> Interest {
        let mut interest = Interest::never();
        for layer in &self.0 {
            let next = layer.register_callsite(metadata);
            if (interest.is_sometimes() && next.is_always())
                || (interest.is_never() && !next.is_never())
            {
                interest = next;
            }
        }
        interest
    }

    fn enabled(&self, metadata: &Metadata<'_>, ctx: Context<'_, Registry>) -> bool {
        self.0
            .iter()
            .any(|layer| layer.enabled(metadata, ctx.clone()))
    }

    fn event_enabled(&self, event: &Event<'_>, ctx: Context<'_, Registry>) -> bool {
        self.0
            .iter()
            .any(|layer| layer.event_enabled(event, ctx.clone()))
    }

    fn max_level_hint(&self) -> Option<LevelFilter> {
        let mut max_level = LevelFilter::OFF;
        for layer in &self.0 {
            max_level = max_level.max(layer.max_level_hint()?);
        }
        Some(max_level)
    }

    fn on_new_span(&self, attrs: &span::Attributes<'_>, id: &span::Id, ctx: Context<'_, Registry>) {
        for layer in &self.0 {
            layer.on_new_span(attrs, id, ctx.clone());
        }
    }

    fn on_record(&self, span: &span::Id, values: &span::Record<'_>, ctx: Context<'_, Registry>) {
        for layer in &self.0 {
            layer.on_record(span, values, ctx.clone());
        }
    }

    fn on_follows_from(&self, span: &span::Id, follows: &span::Id, ctx: Context<'_, Registry>) {
        for layer in &self.0 {
            layer.on_follows_from(span, follows, ctx.clone());
        }
    }

    fn on_event(&self, event: &Event<'_>, ctx: Context<'_, Registry>) {
        for layer in &self.0 {
            layer.on_event(event, ctx.clone());
        }
    }

    fn on_enter(&self, id: &span::Id, ctx: Context<'_, Registry>) {
        for layer in &self.0 {
            layer.on_enter(id, ctx.clone());
        }
    }

    fn on_exit(&self, id: &span::Id, ctx: Context<'_, Registry>) {
        for layer in &self.0 {
            layer.on_exit(id, ctx.clone());
        }
    }

    fn on_close(&self, id: span::Id, ctx: Context<'_, Registry>) {
        for layer in &self.0 {
            layer.on_close(id.clone(), ctx.clone());
        }
    }

    fn on_id_change(&self, old: &span::Id, new: &span::Id, ctx: Context<'_, Registry>) {
        for layer in &self.0 {
            layer.on_id_change(old, new, ctx.clone());
        }
    }
}

struct DefaultSpanEnabled;

struct EnvFilteredLayer<L> {
    filter: EnvFilter,
    inner:  L,
}

impl<L> EnvFilteredLayer<L> {
    fn new(filter: EnvFilter, inner: L) -> Self {
        Self { filter, inner }
    }

    fn span_enabled(&self, id: &span::Id, ctx: &Context<'_, Registry>) -> bool {
        let Some(span) = ctx.span(id) else {
            return false;
        };
        let extensions = span.extensions();
        let enabled = extensions.get::<DefaultSpanEnabled>().is_some();
        drop(extensions);
        enabled
    }
}

impl<L: Layer<Registry>> Layer<Registry> for EnvFilteredLayer<L> {
    fn register_callsite(&self, metadata: &'static Metadata<'static>) -> Interest {
        let interest = Layer::<Registry>::register_callsite(&self.filter, metadata);
        if interest.is_never() {
            return Interest::never();
        }
        let inner = self.inner.register_callsite(metadata);
        if inner.is_never() {
            Interest::never()
        } else if interest.is_sometimes() || inner.is_sometimes() {
            Interest::sometimes()
        } else {
            Interest::always()
        }
    }

    fn enabled(&self, metadata: &Metadata<'_>, ctx: Context<'_, Registry>) -> bool {
        Layer::<Registry>::enabled(&self.filter, metadata, ctx.clone())
            && self.inner.enabled(metadata, ctx)
    }

    fn event_enabled(&self, event: &Event<'_>, ctx: Context<'_, Registry>) -> bool {
        Layer::<Registry>::enabled(&self.filter, event.metadata(), ctx.clone())
            && Layer::<Registry>::event_enabled(&self.filter, event, ctx.clone())
            && self.inner.event_enabled(event, ctx)
    }

    fn max_level_hint(&self) -> Option<LevelFilter> {
        Layer::<Registry>::max_level_hint(&self.filter)
    }

    fn on_new_span(&self, attrs: &span::Attributes<'_>, id: &span::Id, ctx: Context<'_, Registry>) {
        Layer::<Registry>::on_new_span(&self.filter, attrs, id, ctx.clone());
        if Layer::<Registry>::enabled(&self.filter, attrs.metadata(), ctx.clone())
            && self.inner.enabled(attrs.metadata(), ctx.clone())
        {
            if let Some(span) = ctx.span(id) {
                let mut extensions = span.extensions_mut();
                extensions.insert(DefaultSpanEnabled);
                drop(extensions);
            }
            self.inner.on_new_span(attrs, id, ctx);
        }
    }

    fn on_record(&self, span: &span::Id, values: &span::Record<'_>, ctx: Context<'_, Registry>) {
        Layer::<Registry>::on_record(&self.filter, span, values, ctx.clone());
        if self.span_enabled(span, &ctx) {
            self.inner.on_record(span, values, ctx);
        }
    }

    fn on_follows_from(&self, span: &span::Id, follows: &span::Id, ctx: Context<'_, Registry>) {
        if self.span_enabled(span, &ctx) {
            self.inner.on_follows_from(span, follows, ctx);
        }
    }

    fn on_event(&self, event: &Event<'_>, ctx: Context<'_, Registry>) {
        if Layer::<Registry>::enabled(&self.filter, event.metadata(), ctx.clone())
            && Layer::<Registry>::event_enabled(&self.filter, event, ctx.clone())
            && self.inner.event_enabled(event, ctx.clone())
        {
            self.inner.on_event(event, ctx);
        }
    }

    fn on_enter(&self, id: &span::Id, ctx: Context<'_, Registry>) {
        Layer::<Registry>::on_enter(&self.filter, id, ctx.clone());
        if self.span_enabled(id, &ctx) {
            self.inner.on_enter(id, ctx);
        }
    }

    fn on_exit(&self, id: &span::Id, ctx: Context<'_, Registry>) {
        Layer::<Registry>::on_exit(&self.filter, id, ctx.clone());
        if self.span_enabled(id, &ctx) {
            self.inner.on_exit(id, ctx);
        }
    }

    fn on_close(&self, id: span::Id, ctx: Context<'_, Registry>) {
        let enabled = self.span_enabled(&id, &ctx);
        Layer::<Registry>::on_close(&self.filter, id.clone(), ctx.clone());
        if enabled {
            self.inner.on_close(id, ctx);
        }
    }

    fn on_id_change(&self, old: &span::Id, new: &span::Id, ctx: Context<'_, Registry>) {
        if self.span_enabled(old, &ctx) || self.span_enabled(new, &ctx) {
            self.inner.on_id_change(old, new, ctx);
        }
    }
}

struct DynamicFilteredLayer<L> {
    filter: Filter,
    inner:  L,
}

impl<L: Layer<Registry>> DynamicFilteredLayer<L> {
    fn new(filter: Filter, inner: L) -> Self {
        Self { filter, inner }
    }

    fn span_allowed(&self, id: &span::Id, ctx: &Context<'_, Registry>) -> bool {
        ctx.span(id).is_some_and(|span| {
            self.filter.allows(span.metadata()) && self.inner.enabled(span.metadata(), ctx.clone())
        })
    }
}

impl<L: Layer<Registry>> Layer<Registry> for DynamicFilteredLayer<L> {
    fn register_callsite(&self, metadata: &'static Metadata<'static>) -> Interest {
        if self.filter.allows(metadata) {
            self.inner.register_callsite(metadata)
        } else {
            Interest::never()
        }
    }

    fn enabled(&self, metadata: &Metadata<'_>, ctx: Context<'_, Registry>) -> bool {
        self.filter.allows(metadata) && self.inner.enabled(metadata, ctx)
    }

    fn event_enabled(&self, event: &Event<'_>, ctx: Context<'_, Registry>) -> bool {
        self.filter.allows(event.metadata()) && self.inner.event_enabled(event, ctx)
    }

    fn max_level_hint(&self) -> Option<LevelFilter> {
        Some(self.filter.max_level())
    }

    fn on_new_span(&self, attrs: &span::Attributes<'_>, id: &span::Id, ctx: Context<'_, Registry>) {
        if self.filter.allows(attrs.metadata()) && self.inner.enabled(attrs.metadata(), ctx.clone())
        {
            self.inner.on_new_span(attrs, id, ctx);
        }
    }

    fn on_record(&self, span: &span::Id, values: &span::Record<'_>, ctx: Context<'_, Registry>) {
        if self.span_allowed(span, &ctx) {
            self.inner.on_record(span, values, ctx);
        }
    }

    fn on_follows_from(&self, span: &span::Id, follows: &span::Id, ctx: Context<'_, Registry>) {
        if self.span_allowed(span, &ctx) {
            self.inner.on_follows_from(span, follows, ctx);
        }
    }

    fn on_event(&self, event: &Event<'_>, ctx: Context<'_, Registry>) {
        if self.filter.allows(event.metadata()) && self.inner.event_enabled(event, ctx.clone()) {
            self.inner.on_event(event, ctx);
        }
    }

    fn on_enter(&self, id: &span::Id, ctx: Context<'_, Registry>) {
        if self.span_allowed(id, &ctx) {
            self.inner.on_enter(id, ctx);
        }
    }

    fn on_exit(&self, id: &span::Id, ctx: Context<'_, Registry>) {
        if self.span_allowed(id, &ctx) {
            self.inner.on_exit(id, ctx);
        }
    }

    fn on_close(&self, id: span::Id, ctx: Context<'_, Registry>) {
        if self.span_allowed(&id, &ctx) {
            self.inner.on_close(id, ctx);
        }
    }

    fn on_id_change(&self, old: &span::Id, new: &span::Id, ctx: Context<'_, Registry>) {
        if self.span_allowed(old, &ctx) || self.span_allowed(new, &ctx) {
            self.inner.on_id_change(old, new, ctx);
        }
    }
}

impl Tracing for DefaultTracing {
    fn subscribe(
        &self,
        directives: &str,
    ) -> Result<impl DynamicSubscriber + '_, SetLogLevelsError> {
        let filter: Filter = directives.parse()?;
        let buffer = SharedBuffer::new();
        // Use `FmtSpan::NEW` (not `FmtSpan::CLOSE`) on dynamically attached layers: `fmt::Layer`
        // stores a single shared `Timings` counter in `Registry` span extensions for
        // `FmtSpan::CLOSE`, so pushing a second `FmtSpan::CLOSE` layer while another thread
        // is inside an entered span causes `timings.entered` to underflow on exit
        // (`fmt_layer.rs`).
        let fmt_layer = tracing_subscriber::fmt::layer()
            .with_span_events(FmtSpan::NEW)
            .with_timer(self.timer.clone())
            .with_writer(buffer.clone());
        let layer = DynamicFilteredLayer::new(filter, fmt_layer).boxed();
        let ptr = ptr(&layer);

        self.reload_handle.modify(|layers| layers.0.push(layer))?;
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
                    .0
                    .iter()
                    .position(|l| ptr(l) == self.ptr)
                    .expect("layer went missing in subscribers");
                layers.0.swap_remove(idx);
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
    use tracing::{
        Callsite, Level, Metadata, callsite::Identifier, field::FieldSet,
        level_filters::LevelFilter, metadata::Kind, subscriber::Interest,
    };
    use tracing_subscriber::{
        EnvFilter, Layer, Registry, fmt::format::FmtSpan, prelude::*, reload,
    };

    use super::{
        DefaultTracing, DynamicFilteredLayer, DynamicSubscriber, EnvFilteredLayer, Filter,
        LayerUnion, SetLogLevelsError, SharedBuffer, Tracing,
    };

    struct StaticCallsite(&'static Metadata<'static>);

    impl Callsite for StaticCallsite {
        fn set_interest(&self, _interest: Interest) {}

        fn metadata(&self) -> &Metadata<'_> {
            self.0
        }
    }

    macro_rules! static_meta {
        ($target:literal, $level:expr) => {{
            static CALLSITE: StaticCallsite = StaticCallsite(&META);
            static META: Metadata<'static> = Metadata::new(
                "evt",
                $target,
                $level,
                None,
                None,
                None,
                FieldSet::new(&[], Identifier(&CALLSITE)),
                Kind::EVENT,
            );
            &META
        }};
    }

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
        // Verify OffsetTime formatted a HH:MM:SS.ffffff timestamp prefix (modulo ANSI codes).
        let has_timestamp = captured.as_bytes().windows(15).any(|b| {
            b[2] == b':'
                && b[5] == b':'
                && b[8] == b'.'
                && b.iter()
                    .enumerate()
                    .all(|(i, ch)| matches!(i, 2 | 5 | 8) || ch.is_ascii_digit())
        });
        expect_true!(has_timestamp);

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

    #[gtest]
    fn concurrent_dynamic_subscribers_with_disjoint_filters_do_not_leak() {
        let tracing = DefaultTracing::start();

        let mut sub_alpha = tracing
            .subscribe("emulator_tracing_disjoint_alpha=debug")
            .unwrap();
        let mut sub_beta = tracing
            .subscribe("emulator_tracing_disjoint_beta=info")
            .unwrap();

        let span_alpha = tracing::debug_span!(
            target: "emulator_tracing_disjoint_alpha",
            "alpha_span",
            val = tracing::field::Empty
        );
        {
            let _enter = span_alpha.enter();
            span_alpha.record("val", 42);
            tracing::debug!(
                target: "emulator_tracing_disjoint_alpha",
                "alpha debug event"
            );
        }

        let span_beta = tracing::info_span!(
            target: "emulator_tracing_disjoint_beta",
            "beta_span",
            val = tracing::field::Empty
        );
        {
            let _enter = span_beta.enter();
            span_beta.record("val", 99);
            tracing::info!(
                target: "emulator_tracing_disjoint_beta",
                "beta info event"
            );
            tracing::debug!(
                target: "emulator_tracing_disjoint_beta",
                "beta debug suppressed"
            );
        }

        let alpha_out = String::from_utf8(sub_alpha.drain().to_vec()).unwrap();
        let beta_out = String::from_utf8(sub_beta.drain().to_vec()).unwrap();

        expect_that!(alpha_out, contains_substring("alpha_span"));
        expect_that!(alpha_out, contains_substring("alpha debug event"));
        expect_that!(alpha_out, not(contains_substring("beta_span")));
        expect_that!(alpha_out, not(contains_substring("beta info event")));
        expect_that!(alpha_out, not(contains_substring("beta debug suppressed")));

        expect_that!(beta_out, contains_substring("beta_span"));
        expect_that!(beta_out, contains_substring("beta info event"));
        expect_that!(beta_out, not(contains_substring("alpha_span")));
        expect_that!(beta_out, not(contains_substring("alpha debug event")));
        expect_that!(beta_out, not(contains_substring("beta debug suppressed")));
    }

    #[gtest]
    fn active_default_layer_events_do_not_leak_into_narrower_dynamic_subscriber() {
        let default_buf = SharedBuffer::new();
        let dynamic_buf = SharedBuffer::new();

        let default_layer = EnvFilteredLayer::new(
            EnvFilter::new("emulator_tracing_leak_broad=debug,emulator_tracing_leak_narrow=debug"),
            tracing_subscriber::fmt::layer()
                .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE)
                .with_writer(default_buf.clone()),
        )
        .boxed();
        let (reload_layer, reload_handle) = reload::Layer::new(LayerUnion(vec![default_layer]));
        let subscriber = tracing_subscriber::registry().with(reload_layer);

        let narrow_filter: Filter = "emulator_tracing_leak_narrow=warn".parse().unwrap();
        let dynamic_layer = DynamicFilteredLayer::new(
            narrow_filter,
            tracing_subscriber::fmt::layer()
                .with_span_events(FmtSpan::NEW)
                .with_writer(dynamic_buf.clone()),
        )
        .boxed();
        reload_handle
            .modify(|layers| layers.0.push(dynamic_layer))
            .unwrap();

        tracing::subscriber::with_default(subscriber, || {
            let broad_span = tracing::info_span!(
                target: "emulator_tracing_leak_broad",
                "broad_default_span"
            );
            {
                let _enter = broad_span.enter();
                tracing::info!(target: "emulator_tracing_leak_broad", "broad default event");
                tracing::debug!(target: "emulator_tracing_leak_broad", "broad debug event");
            }

            let narrow_info_span = tracing::info_span!(
                target: "emulator_tracing_leak_narrow",
                "narrow_info_span"
            );
            {
                let _enter = narrow_info_span.enter();
                tracing::info!(
                    target: "emulator_tracing_leak_narrow",
                    "narrow info suppressed"
                );
            }

            let narrow_warn_span = tracing::warn_span!(
                target: "emulator_tracing_leak_narrow",
                "narrow_warn_span"
            );
            {
                let _enter = narrow_warn_span.enter();
                tracing::warn!(
                    target: "emulator_tracing_leak_narrow",
                    "narrow warn captured"
                );
            }
        });

        let default_out = String::from_utf8(default_buf.drain().to_vec()).unwrap();
        let dynamic_out = String::from_utf8(dynamic_buf.drain().to_vec()).unwrap();

        // Confirm default_layer actually enabled and received all broad + narrow events.
        expect_that!(default_out, contains_substring("broad_default_span"));
        expect_that!(default_out, contains_substring("broad default event"));
        expect_that!(default_out, contains_substring("narrow info suppressed"));
        expect_that!(default_out, contains_substring("narrow warn captured"));

        // Confirm narrower dynamic layer captured ONLY the allowed WARN span/event.
        expect_that!(dynamic_out, contains_substring("narrow_warn_span"));
        expect_that!(dynamic_out, contains_substring("narrow warn captured"));
        expect_that!(dynamic_out, not(contains_substring("broad_default_span")));
        expect_that!(dynamic_out, not(contains_substring("broad default event")));
        expect_that!(dynamic_out, not(contains_substring("broad debug event")));
        expect_that!(dynamic_out, not(contains_substring("narrow_info_span")));
        expect_that!(
            dynamic_out,
            not(contains_substring("narrow info suppressed"))
        );

        // Also verify against global DefaultTracing where default_layer enables ERROR by default.
        let global = DefaultTracing::start();
        let mut sub = global
            .subscribe("emulator_tracing_global_narrow=info")
            .unwrap();
        tracing::error!(
            target: "emulator_tracing_global_unrelated",
            "unrelated global error"
        );
        tracing::info!(
            target: "emulator_tracing_global_narrow",
            "allowed global info"
        );
        let global_out = String::from_utf8(sub.drain().to_vec()).unwrap();
        expect_that!(global_out, contains_substring("allowed global info"));
        expect_that!(
            global_out,
            not(contains_substring("unrelated global error"))
        );
    }

    #[gtest]
    fn dynamic_filtered_layer_callsite_interest_and_max_level_hint() {
        let filter: Filter = "emulator_tracing_direct=info,emulator_tracing_direct::noisy=off"
            .parse()
            .unwrap();
        let expected_max = filter.max_level();
        expect_eq!(expected_max, LevelFilter::INFO);

        let layer = DynamicFilteredLayer::new(
            filter,
            tracing_subscriber::fmt::layer().with_writer(SharedBuffer::new()),
        );

        expect_eq!(
            Layer::<Registry>::max_level_hint(&layer),
            Some(expected_max)
        );
        expect_true!(
            Layer::<Registry>::register_callsite(
                &layer,
                static_meta!("unrelated_target", Level::ERROR)
            )
            .is_never()
        );
        expect_true!(
            Layer::<Registry>::register_callsite(
                &layer,
                static_meta!("emulator_tracing_direct::noisy::sub", Level::ERROR)
            )
            .is_never()
        );
        expect_true!(
            Layer::<Registry>::register_callsite(
                &layer,
                static_meta!("emulator_tracing_direct", Level::DEBUG)
            )
            .is_never()
        );
        expect_true!(
            Layer::<Registry>::register_callsite(
                &layer,
                static_meta!("emulator_tracing_direct", Level::INFO)
            )
            .is_always()
        );

        for silent in ["off", ""] {
            let silent_filter: Filter = silent.parse().unwrap();
            let silent_max = silent_filter.max_level();
            expect_eq!(silent_max, LevelFilter::OFF);
            let silent_layer = DynamicFilteredLayer::new(
                silent_filter,
                tracing_subscriber::fmt::layer().with_writer(SharedBuffer::new()),
            );
            expect_eq!(
                Layer::<Registry>::max_level_hint(&silent_layer),
                Some(LevelFilter::OFF)
            );
            expect_true!(
                Layer::<Registry>::register_callsite(
                    &silent_layer,
                    static_meta!("emulator_tracing_direct", Level::ERROR)
                )
                .is_never()
            );
        }
    }

    #[gtest]
    fn layer_union_preserves_callsite_never_max_level_hint_and_dynamic_env_filter() {
        use tracing::Subscriber;

        let default_buf = SharedBuffer::new();
        let dyn_buf = SharedBuffer::new();

        let default_layer = EnvFilteredLayer::new(
            EnvFilter::new("emulator_tracing_union_default=error,[union_dyn_span]=debug"),
            tracing_subscriber::fmt::layer()
                .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE)
                .with_writer(default_buf.clone()),
        )
        .boxed();
        let (reload_layer, reload_handle) = reload::Layer::new(LayerUnion(vec![default_layer]));
        let subscriber = tracing_subscriber::registry().with(reload_layer);

        // With a narrow dynamic subscriber attached alongside EnvFilter:
        let dynamic_layer = DynamicFilteredLayer::new(
            "emulator_tracing_union_sub=info".parse().unwrap(),
            tracing_subscriber::fmt::layer()
                .with_span_events(FmtSpan::NEW)
                .with_writer(dyn_buf.clone()),
        )
        .boxed();
        reload_handle
            .modify(|layers| layers.0.push(dynamic_layer))
            .unwrap();

        // Check a static-only default_layer + dynamic_layer stack returns Interest::never()
        // for unmatched targets and exact max_level_hint() before and after removal.
        let static_default = EnvFilteredLayer::new(
            EnvFilter::new("error"),
            tracing_subscriber::fmt::layer().with_writer(SharedBuffer::new()),
        )
        .boxed();
        let (static_reload, static_handle) = reload::Layer::new(LayerUnion(vec![static_default]));
        let static_sub = tracing_subscriber::registry().with(static_reload);
        expect_eq!(static_sub.max_level_hint(), Some(LevelFilter::ERROR));
        expect_true!(
            static_sub
                .register_callsite(static_meta!("unmatched_target", Level::INFO))
                .is_never()
        );

        let info_sub = DynamicFilteredLayer::new(
            "emulator_tracing_only_this=info".parse().unwrap(),
            tracing_subscriber::fmt::layer().with_writer(SharedBuffer::new()),
        )
        .boxed();
        let info_ptr = super::ptr(&info_sub);
        static_handle
            .modify(|layers| layers.0.push(info_sub))
            .unwrap();
        expect_eq!(static_sub.max_level_hint(), Some(LevelFilter::INFO));
        expect_true!(
            static_sub
                .register_callsite(static_meta!("unmatched_target", Level::INFO))
                .is_never()
        );
        expect_true!(
            static_sub
                .register_callsite(static_meta!("emulator_tracing_only_this", Level::INFO))
                .is_always()
        );

        static_handle
            .modify(|layers| {
                let idx = layers
                    .0
                    .iter()
                    .position(|l| super::ptr(l) == info_ptr)
                    .unwrap();
                layers.0.swap_remove(idx);
            })
            .unwrap();
        expect_eq!(static_sub.max_level_hint(), Some(LevelFilter::ERROR));

        // Verify Interest::sometimes() dynamic EnvFilter span filter does not get vetoed by
        // disjoint DynamicFilteredLayer (and vice versa).
        tracing::subscriber::with_default(subscriber, || {
            let dyn_span = tracing::debug_span!(
                target: "emulator_tracing_union_other",
                "union_dyn_span"
            );
            {
                let _enter = dyn_span.enter();
                tracing::debug!(
                    target: "emulator_tracing_union_other",
                    "inside dynamic env span"
                );
            }
            tracing::info!(
                target: "emulator_tracing_union_sub",
                "dynamic sub info outside span"
            );
        });

        let default_out = String::from_utf8(default_buf.drain().to_vec()).unwrap();
        let dyn_out = String::from_utf8(dyn_buf.drain().to_vec()).unwrap();

        expect_that!(default_out, contains_substring("inside dynamic env span"));
        expect_that!(
            default_out,
            not(contains_substring("dynamic sub info outside span"))
        );

        expect_that!(dyn_out, contains_substring("dynamic sub info outside span"));
        expect_that!(dyn_out, not(contains_substring("inside dynamic env span")));
    }

    #[gtest]
    fn layer_union_honors_event_enabled_across_env_and_dynamic_layers() {
        use std::sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        };

        use tracing::{
            Event,
            field::{Field, Visit},
        };
        use tracing_subscriber::layer::Context;

        struct KeepFieldVisitor {
            keep: bool,
        }

        impl Visit for KeepFieldVisitor {
            fn record_bool(&mut self, field: &Field, value: bool) {
                if field.name() == "keep" {
                    self.keep = value;
                }
            }

            fn record_debug(&mut self, _field: &Field, _value: &dyn std::fmt::Debug) {}
        }

        struct EventFieldFilterLayer<L> {
            inner: L,
            on_event_calls: Arc<AtomicUsize>,
        }

        impl<L: Layer<Registry>> Layer<Registry> for EventFieldFilterLayer<L> {
            fn event_enabled(&self, event: &Event<'_>, ctx: Context<'_, Registry>) -> bool {
                let mut visitor = KeepFieldVisitor { keep: false };
                event.record(&mut visitor);
                visitor.keep && self.inner.event_enabled(event, ctx)
            }

            fn on_event(&self, event: &Event<'_>, ctx: Context<'_, Registry>) {
                self.on_event_calls.fetch_add(1, Ordering::SeqCst);
                self.inner.on_event(event, ctx);
            }
        }

        let default_buf = SharedBuffer::new();
        let default_calls = Arc::new(AtomicUsize::new(0));
        let unrelated_dyn_buf = SharedBuffer::new();
        let matching_dyn_buf = SharedBuffer::new();
        let field_filtered_dyn_buf = SharedBuffer::new();
        let field_filtered_dyn_calls = Arc::new(AtomicUsize::new(0));

        let default_layer = EnvFilteredLayer::new(
            EnvFilter::new("emulator_tracing_evt_filter=info"),
            EventFieldFilterLayer {
                inner: tracing_subscriber::fmt::layer().with_writer(default_buf.clone()),
                on_event_calls: Arc::clone(&default_calls),
            },
        )
        .boxed();
        let unrelated_dyn = DynamicFilteredLayer::new(
            "emulator_tracing_evt_other=info".parse().unwrap(),
            tracing_subscriber::fmt::layer().with_writer(unrelated_dyn_buf.clone()),
        )
        .boxed();

        let (reload_layer, reload_handle) =
            reload::Layer::new(LayerUnion(vec![default_layer, unrelated_dyn]));
        let dispatch = tracing::Dispatch::new(tracing_subscriber::registry().with(reload_layer));

        // Phase 1: Neither EnvFilteredLayer's inner event_enabled nor unrelated_dyn accepts
        // `keep = false`, so LayerUnion::event_enabled short-circuits without invoking on_event.
        tracing::dispatcher::with_default(&dispatch, || {
            tracing::info!(
                target: "emulator_tracing_evt_filter",
                keep = false,
                "dropped by event_enabled"
            );
            tracing::info!(
                target: "emulator_tracing_evt_filter",
                keep = true,
                "kept by env event_enabled"
            );
        });

        expect_eq!(default_calls.load(Ordering::SeqCst), 1);
        let default_out_1 = String::from_utf8(default_buf.drain().to_vec()).unwrap();
        expect_that!(
            default_out_1,
            contains_substring("kept by env event_enabled")
        );
        expect_that!(
            default_out_1,
            not(contains_substring("dropped by event_enabled"))
        );
        expect_true!(unrelated_dyn_buf.drain().is_empty());

        // Phase 2: Add a matching dynamic subscriber AND a field-filtered dynamic subscriber.
        // Now LayerUnion::event_enabled returns `true` for `keep = false` (because matching_dyn
        // accepts it), yet EnvFilteredLayer::on_event and field_filtered_dyn::on_event MUST still
        // skip the event while matching_dyn captures it.
        let matching_dyn = DynamicFilteredLayer::new(
            "emulator_tracing_evt_filter=info".parse().unwrap(),
            tracing_subscriber::fmt::layer().with_writer(matching_dyn_buf.clone()),
        )
        .boxed();
        let field_filtered_dyn = DynamicFilteredLayer::new(
            "emulator_tracing_evt_filter=info".parse().unwrap(),
            EventFieldFilterLayer {
                inner: tracing_subscriber::fmt::layer().with_writer(field_filtered_dyn_buf.clone()),
                on_event_calls: Arc::clone(&field_filtered_dyn_calls),
            },
        )
        .boxed();
        reload_handle
            .modify(|layers| {
                layers.0.push(matching_dyn);
                layers.0.push(field_filtered_dyn);
            })
            .unwrap();

        tracing::dispatcher::with_default(&dispatch, || {
            tracing::info!(
                target: "emulator_tracing_evt_filter",
                keep = false,
                "captured only by matching dynamic sub"
            );
            tracing::info!(
                target: "emulator_tracing_evt_filter",
                keep = true,
                "captured by all matching layers"
            );
        });

        expect_eq!(default_calls.load(Ordering::SeqCst), 2);
        expect_eq!(field_filtered_dyn_calls.load(Ordering::SeqCst), 1);

        let default_out_2 = String::from_utf8(default_buf.drain().to_vec()).unwrap();
        let matching_dyn_out = String::from_utf8(matching_dyn_buf.drain().to_vec()).unwrap();
        let field_filtered_dyn_out =
            String::from_utf8(field_filtered_dyn_buf.drain().to_vec()).unwrap();

        expect_that!(
            default_out_2,
            contains_substring("captured by all matching layers")
        );
        expect_that!(
            default_out_2,
            not(contains_substring("captured only by matching dynamic sub"))
        );

        expect_that!(
            matching_dyn_out,
            contains_substring("captured only by matching dynamic sub")
        );
        expect_that!(
            matching_dyn_out,
            contains_substring("captured by all matching layers")
        );

        expect_that!(
            field_filtered_dyn_out,
            contains_substring("captured by all matching layers")
        );
        expect_that!(
            field_filtered_dyn_out,
            not(contains_substring("captured only by matching dynamic sub"))
        );
    }

    #[gtest]
    fn layer_union_honors_inner_span_enabled_across_env_and_dynamic_layers() {
        use std::sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        };

        use tracing::span;
        use tracing_subscriber::layer::Context;

        #[derive(Default)]
        struct SpanCounters {
            new_span: AtomicUsize,
            record:   AtomicUsize,
            enter:    AtomicUsize,
            exit:     AtomicUsize,
            close:    AtomicUsize,
        }

        struct SpanRejectLayer<L> {
            inner: L,
            rejected_span_name: &'static str,
            counters: Arc<SpanCounters>,
        }

        impl<L: Layer<Registry>> Layer<Registry> for SpanRejectLayer<L> {
            fn register_callsite(&self, metadata: &'static Metadata<'static>) -> Interest {
                self.inner.register_callsite(metadata)
            }

            fn enabled(&self, metadata: &Metadata<'_>, ctx: Context<'_, Registry>) -> bool {
                !(metadata.is_span() && metadata.name() == self.rejected_span_name)
                    && self.inner.enabled(metadata, ctx)
            }

            fn on_new_span(
                &self,
                attrs: &span::Attributes<'_>,
                id: &span::Id,
                ctx: Context<'_, Registry>,
            ) {
                self.counters.new_span.fetch_add(1, Ordering::SeqCst);
                self.inner.on_new_span(attrs, id, ctx);
            }

            fn on_record(
                &self,
                id: &span::Id,
                values: &span::Record<'_>,
                ctx: Context<'_, Registry>,
            ) {
                self.counters.record.fetch_add(1, Ordering::SeqCst);
                self.inner.on_record(id, values, ctx);
            }

            fn on_enter(&self, id: &span::Id, ctx: Context<'_, Registry>) {
                self.counters.enter.fetch_add(1, Ordering::SeqCst);
                self.inner.on_enter(id, ctx);
            }

            fn on_exit(&self, id: &span::Id, ctx: Context<'_, Registry>) {
                self.counters.exit.fetch_add(1, Ordering::SeqCst);
                self.inner.on_exit(id, ctx);
            }

            fn on_close(&self, id: span::Id, ctx: Context<'_, Registry>) {
                self.counters.close.fetch_add(1, Ordering::SeqCst);
                self.inner.on_close(id, ctx);
            }
        }

        let env_buf = SharedBuffer::new();
        let env_counters = Arc::new(SpanCounters::default());
        let rejecting_dyn_buf = SharedBuffer::new();
        let rejecting_dyn_counters = Arc::new(SpanCounters::default());
        let accepting_dyn_buf = SharedBuffer::new();

        let env_layer = EnvFilteredLayer::new(
            EnvFilter::new("emulator_tracing_span_filter=info"),
            SpanRejectLayer {
                inner: tracing_subscriber::fmt::layer()
                    .with_span_events(FmtSpan::NEW)
                    .with_writer(env_buf.clone()),
                rejected_span_name: "rejected_span",
                counters: Arc::clone(&env_counters),
            },
        )
        .boxed();
        let rejecting_dyn = DynamicFilteredLayer::new(
            "emulator_tracing_span_filter=info".parse().unwrap(),
            SpanRejectLayer {
                inner: tracing_subscriber::fmt::layer()
                    .with_span_events(FmtSpan::NEW)
                    .with_writer(rejecting_dyn_buf.clone()),
                rejected_span_name: "rejected_span",
                counters: Arc::clone(&rejecting_dyn_counters),
            },
        )
        .boxed();
        let accepting_dyn = DynamicFilteredLayer::new(
            "emulator_tracing_span_filter=info".parse().unwrap(),
            tracing_subscriber::fmt::layer()
                .with_span_events(FmtSpan::NEW)
                .with_writer(accepting_dyn_buf.clone()),
        )
        .boxed();

        let (reload_layer, _reload_handle) =
            reload::Layer::new(LayerUnion(vec![env_layer, rejecting_dyn, accepting_dyn]));
        let dispatch = tracing::Dispatch::new(tracing_subscriber::registry().with(reload_layer));

        tracing::dispatcher::with_default(&dispatch, || {
            let rejected = tracing::info_span!(
                target: "emulator_tracing_span_filter",
                "rejected_span",
                field = tracing::field::Empty
            );
            {
                let _enter = rejected.enter();
                rejected.record("field", 1);
            }
            drop(rejected);

            let accepted = tracing::info_span!(
                target: "emulator_tracing_span_filter",
                "accepted_span",
                field = tracing::field::Empty
            );
            {
                let _enter = accepted.enter();
                accepted.record("field", 2);
            }
            drop(accepted);
        });

        for counters in [&env_counters, &rejecting_dyn_counters] {
            expect_eq!(counters.new_span.load(Ordering::SeqCst), 1);
            expect_eq!(counters.record.load(Ordering::SeqCst), 1);
            expect_eq!(counters.enter.load(Ordering::SeqCst), 1);
            expect_eq!(counters.exit.load(Ordering::SeqCst), 1);
            expect_eq!(counters.close.load(Ordering::SeqCst), 1);
        }

        let env_out = String::from_utf8(env_buf.drain().to_vec()).unwrap();
        let rejecting_dyn_out = String::from_utf8(rejecting_dyn_buf.drain().to_vec()).unwrap();
        let accepting_dyn_out = String::from_utf8(accepting_dyn_buf.drain().to_vec()).unwrap();

        expect_that!(env_out, contains_substring("accepted_span"));
        expect_that!(env_out, not(contains_substring("rejected_span")));
        expect_that!(rejecting_dyn_out, contains_substring("accepted_span"));
        expect_that!(rejecting_dyn_out, not(contains_substring("rejected_span")));
        expect_that!(accepting_dyn_out, contains_substring("rejected_span"));
        expect_that!(accepting_dyn_out, contains_substring("accepted_span"));
    }
}
