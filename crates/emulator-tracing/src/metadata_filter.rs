use std::{str::FromStr, sync::LazyLock};

use ctreg::regex;
use itertools::Itertools;
use thiserror::Error;
use tracing::{Metadata, level_filters::LevelFilter};

static DIRECTIVE_RE: LazyLock<DirectiveRe> = LazyLock::new(DirectiveRe::new);
regex! {
    DirectiveRe =
        r"(?x)
            # Either a single global level
        ^(?<global_level>(?i:trace|debug|info|warn|error|off|[0-5]))$
        |
            # Or a target with an optional level (defaults to trace)
        ^(?<target>[\w:-]+) (?: = (?<level>(?i:trace|debug|info|warn|error|off|[0-5])) )?$
        "
}

#[derive(Debug, Error)]
pub enum ParseError {
    #[error("invalid directive: {0:?}")]
    InvalidDirective(String),
}

/// Filter that uses the same syntax as the [`EnvFilter`](tracing_subscriber::EnvFilter), but works
/// outside of the layer infrastructure. This is a workaround for the following issue:
/// https://github.com/tokio-rs/tracing/issues/1629 which prevents us from adding layers with filters
/// after initialization. When that issue is/ fixed we can remove this workaround and simply add a
/// layer with an ordinary `EnvFilter`.
#[derive(Debug)]
pub(crate) struct Filter {
    directives: Vec<Directive>,
}

impl Filter {
    pub(crate) fn allows(&self, meta: &Metadata<'_>) -> bool {
        self.directives.iter().any(|dir| dir.allows(meta))
    }
}

impl FromStr for Filter {
    type Err = ParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let directives: Vec<_> = s
            .split(',')
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .map(Directive::from_str)
            .try_collect()?;
        Ok(Self { directives })
    }
}

#[derive(Debug)]
struct Directive {
    target: Option<String>,
    level:  LevelFilter,
}

impl FromStr for Directive {
    type Err = ParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let caps = DIRECTIVE_RE
            .captures(s)
            .ok_or_else(|| ParseError::InvalidDirective(s.to_owned()))?;

        if let Some(level) = caps.global_level {
            return Ok(Directive {
                target: None,
                level:  level
                    .content
                    .parse()
                    .expect("regex should have prevented a parse error"),
            });
        }

        let target = caps.target.map(|cap| cap.content.to_owned());

        let level = caps
            .level
            .map(|cap| {
                cap.content
                    .parse()
                    .expect("regex should have prevented a parse error")
            })
            .unwrap_or(LevelFilter::TRACE);

        Ok(Self { target, level })
    }
}

impl Directive {
    fn allows(&self, meta: &Metadata<'_>) -> bool {
        if meta.level() > &self.level {
            return false;
        }

        // Does this directive have a target filter, and does it match the
        // metadata's target?
        if let Some(ref target) = self.target
            && !meta.target().starts_with(target)
        {
            return false;
        }

        true
    }
}

#[cfg(test)]
mod tests {
    use googletest::prelude::*;
    use rstest::rstest;
    use tracing::{
        Callsite, Level, Metadata, callsite::Identifier, field::FieldSet, metadata::Kind,
        subscriber::Interest,
    };

    use super::{Filter, ParseError};

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
    #[rstest]
    #[case("off", false, false, false)]
    #[case("0", false, false, false)]
    #[case("error", true, false, false)]
    #[case("WARN", true, false, false)]
    #[case("info", true, true, false)]
    #[case("debug", true, true, true)]
    #[case("trace", true, true, true)]
    #[case("5", true, true, true)]
    fn global_level_directives(
        #[case] directive: &str,
        #[case] allows_error: bool,
        #[case] allows_info: bool,
        #[case] allows_debug: bool,
    ) {
        let filter: Filter = directive.parse().unwrap();
        expect_eq!(
            filter.allows(static_meta!("any::mod", Level::ERROR)),
            allows_error
        );
        expect_eq!(
            filter.allows(static_meta!("any::mod", Level::INFO)),
            allows_info
        );
        expect_eq!(
            filter.allows(static_meta!("any::mod", Level::DEBUG)),
            allows_debug
        );
    }

    #[gtest]
    fn target_prefix_and_comma_separated_directives() {
        let filter: Filter = " warn , tower_http::trace=debug , bare_target , "
            .parse()
            .unwrap();

        // Global `warn` allows WARN/ERROR everywhere, blocks INFO on unrelated targets.
        expect_true!(filter.allows(static_meta!("other_crate", Level::WARN)));
        expect_false!(filter.allows(static_meta!("other_crate", Level::INFO)));

        // Target prefix `tower_http::trace=debug` allows DEBUG on sub-targets, blocks TRACE.
        expect_true!(filter.allows(static_meta!("tower_http::trace::on_request", Level::DEBUG)));
        expect_false!(filter.allows(static_meta!("tower_http::trace::on_request", Level::TRACE)));

        // Bare target without `=level` defaults to TRACE.
        expect_true!(filter.allows(static_meta!("bare_target::child", Level::TRACE)));
    }

    #[gtest]
    #[rstest]
    #[case("tower_http=invalid_level")]
    #[case("my_target[span_name]=debug")]
    #[case("has spaces=info")]
    #[case("=debug")]
    fn invalid_directives_return_parse_error(#[case] bad: &str) {
        let err = bad.parse::<Filter>().unwrap_err();
        expect_that!(err, pat!(ParseError::InvalidDirective(_)));
        expect_that!(err.to_string(), contains_substring("invalid directive"));
    }
}
