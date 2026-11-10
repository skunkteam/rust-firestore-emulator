# emulator-tracing (`src`)

## Overview
Implements the `emulator-tracing` crate's global `tracing` subscriber initialization and dynamic runtime log-level subscription mechanism. It configures the process-wide `tracing_subscriber` registry and allows callers to dynamically attach and drain filtered in-memory formatted log streams without restarting the global subscriber.

## Interface
- `[lib.rs:DefaultTracing]` — Public struct managing the `reload::Handle<Vec<BoxedLayer>, Registry>` and local/UTC `OffsetTime` formatter.
- `[lib.rs:DefaultTracing::start]` — `pub fn start() -> &'static Self`: Initializes the global `tracing_subscriber` registry with an `EnvFilter::from_default_env()` fmt layer (`FmtSpan::NEW | FmtSpan::CLOSE`), optional `console_subscriber::spawn()` layer when feature `console` (`[Cargo.toml:console]`) is enabled, and returns a `'static` reference.
- `[lib.rs:Tracing]` — `pub trait Tracing: Send + Sync` providing `fn subscribe(&self, dirs: &str) -> Result<impl DynamicSubscriber + '_, SetLogLevelsError>` (implemented for `DefaultTracing`).
- `[lib.rs:DynamicSubscriber]` — `pub trait DynamicSubscriber: Send` providing `fn drain(&mut self) -> Bytes` to consume formatted log bytes accumulated by a dynamic subscriber.
- `[lib.rs:SetLogLevelsError]` — Public error enum with variants `InvalidDirectives(#[from] metadata_filter::ParseError)` and `ReloadError(#[from] reload::Error)`.
- `[metadata_filter.rs:ParseError]` — Public directive parsing error enum with variants `InvalidDirective(String)` and `InvalidSpanFilter(String)`.

## Invariants
- `[lib.rs:DefaultTracing::start]` installs the global default tracing dispatcher via `registry.init()` and leaks the `DefaultTracing` handle with `Box::leak`; it must only be invoked once per process.
- `[lib.rs:DefaultDynamicSubscriber::drop]` expects the subscriber's boxed layer pointer (`[lib.rs:ptr]`) to remain present in `reload_handle`'s layer vector until dropped, panicking if the layer is missing or reload modification fails.
- `[metadata_filter.rs:DIRECTIVE_RE]` and `[metadata_filter.rs:Directive::from_str]` require comma-separated directives matching either a global level (`trace|debug|info|warn|error|off|[0-5]`, case-insensitive) or `target[=level]` (`target` matching `[\w:-]+`, defaulting to `LevelFilter::TRACE`); `[metadata_filter.rs:Directive::allows]` permits events/spans whose level is `<= directive.level` and whose metadata target starts with `directive.target`.

## Side Effects
- `[lib.rs:DefaultTracing::start]` checks the local UTC offset via `UtcOffset::current_local_offset()` (writing a fallback note to `stderr` via `eprintln!` on `IndeterminateOffset`), reads `RUST_LOG` from the environment via `EnvFilter::from_default_env()`, optionally spawns the Tokio console subscriber under `[Cargo.toml:console]`, initializes the global `tracing` dispatcher (`registry.init()`), and leaks `Box<DefaultTracing>`.
- `[lib.rs:Tracing::subscribe]` and `[lib.rs:DefaultDynamicSubscriber::drop]` mutate the active `tracing_subscriber::reload::Layer` vector via `reload_handle.modify(...)` and buffer formatted log output in an in-memory `Arc<Mutex<Writer<BytesMut>>>` (`[lib.rs:SharedBuffer]`).

## Verification
- Run: `cargo test -p emulator-tracing`
