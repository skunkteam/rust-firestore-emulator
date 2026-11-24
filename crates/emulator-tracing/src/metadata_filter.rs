use std::str::FromStr;

use thiserror::Error;
use tracing::{Metadata, level_filters::LevelFilter};

#[derive(Debug, Error)]
pub enum ParseError {
    #[error("invalid directive: {0:?}")]
    InvalidDirective(String),
}

/// Filter that uses the same directive syntax as [`EnvFilter`](tracing_subscriber::EnvFilter),
/// evaluated directly by `DynamicFilteredLayer` without `tracing_subscriber::filter::Filtered`'s
/// registration-time `FilterId` bitmask (working around
/// <https://github.com/tokio-rs/tracing/issues/1629> for dynamically reloaded layers).
#[derive(Debug)]
pub(crate) struct Filter {
    directives: Vec<Directive>,
}

impl Filter {
    pub(crate) fn allows(&self, meta: &Metadata<'_>) -> bool {
        self.directives
            .iter()
            .filter(|dir| dir.matches_target(meta))
            .max_by_key(|dir| dir.target.as_deref().map_or(0, str::len))
            .is_some_and(|dir| meta.level() <= &dir.level)
    }

    pub(crate) fn max_level(&self) -> LevelFilter {
        self.directives
            .iter()
            .map(|dir| dir.level)
            .max()
            .unwrap_or(LevelFilter::OFF)
    }
}

impl FromStr for Filter {
    type Err = ParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let directives = s
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(Directive::from_str)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self { directives })
    }
}

#[derive(Debug)]
struct Directive {
    target: Option<String>,
    level:  LevelFilter,
}

fn parse_level(s: &str) -> Option<LevelFilter> {
    if s.eq_ignore_ascii_case("trace") || s == "5" {
        Some(LevelFilter::TRACE)
    } else if s.eq_ignore_ascii_case("debug") || s == "4" {
        Some(LevelFilter::DEBUG)
    } else if s.eq_ignore_ascii_case("info") || s == "3" {
        Some(LevelFilter::INFO)
    } else if s.eq_ignore_ascii_case("warn") || s == "2" {
        Some(LevelFilter::WARN)
    } else if s.eq_ignore_ascii_case("error") || s == "1" {
        Some(LevelFilter::ERROR)
    } else if s.eq_ignore_ascii_case("off") || s == "0" {
        Some(LevelFilter::OFF)
    } else {
        None
    }
}

fn is_valid_target(s: &str) -> bool {
    !s.is_empty()
        && s.chars()
            .all(|c| c.is_alphanumeric() || matches!(c, '_' | ':' | '-'))
}

impl FromStr for Directive {
    type Err = ParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let invalid = || ParseError::InvalidDirective(s.to_owned());

        if let Some((target, level_str)) = s.split_once('=') {
            if !is_valid_target(target) {
                return Err(invalid());
            }
            let level = parse_level(level_str).ok_or_else(invalid)?;
            return Ok(Self {
                target: Some(target.to_owned()),
                level,
            });
        }

        if let Some(level) = parse_level(s) {
            return Ok(Self {
                target: None,
                level,
            });
        }

        if is_valid_target(s) {
            return Ok(Self {
                target: Some(s.to_owned()),
                level:  LevelFilter::TRACE,
            });
        }

        Err(invalid())
    }
}

impl Directive {
    fn matches_target(&self, meta: &Metadata<'_>) -> bool {
        match &self.target {
            Some(target) => meta.target().starts_with(target),
            None => true,
        }
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
    #[case("off", [false, false, false, false, false])]
    #[case("0", [false, false, false, false, false])]
    #[case("error", [true, false, false, false, false])]
    #[case("1", [true, false, false, false, false])]
    #[case("WARN", [true, true, false, false, false])]
    #[case("2", [true, true, false, false, false])]
    #[case("info", [true, true, true, false, false])]
    #[case("3", [true, true, true, false, false])]
    #[case("debug", [true, true, true, true, false])]
    #[case("4", [true, true, true, true, false])]
    #[case("trace", [true, true, true, true, true])]
    #[case("5", [true, true, true, true, true])]
    fn global_level_directives(#[case] directive: &str, #[case] expected: [bool; 5]) {
        let filter: Filter = directive.parse().unwrap();
        let actual = [
            filter.allows(static_meta!("any::mod", Level::ERROR)),
            filter.allows(static_meta!("any::mod", Level::WARN)),
            filter.allows(static_meta!("any::mod", Level::INFO)),
            filter.allows(static_meta!("any::mod", Level::DEBUG)),
            filter.allows(static_meta!("any::mod", Level::TRACE)),
        ];
        expect_eq!(actual, expected);
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
    #[case("tower_http=")]
    #[case("tower_http=6")]
    #[case("tower_http=info=debug")]
    #[case("my_target[span_name]=debug")]
    #[case("my_target[span_name]")]
    #[case("has spaces=info")]
    #[case("has spaces")]
    #[case("=debug")]
    #[case("=invalid=filter=")]
    #[case("invalid/slash=info")]
    #[case("invalid/slash")]
    fn invalid_directives_return_parse_error(#[case] bad: &str) {
        let err = bad.parse::<Filter>().unwrap_err();
        expect_that!(err, pat!(ParseError::InvalidDirective(_)));
        expect_that!(err.to_string(), contains_substring("invalid directive"));
    }

    #[gtest]
    fn most_specific_and_later_directives_override_broader_rules() {
        let off_override: Filter = "info,tower_http=off".parse().unwrap();
        expect_true!(off_override.allows(static_meta!("emulator_grpc", Level::INFO)));
        expect_false!(off_override.allows(static_meta!("tower_http::trace", Level::ERROR)));
        expect_false!(off_override.allows(static_meta!("tower_http::trace", Level::INFO)));

        let level_override: Filter = "debug,emulator_grpc=error".parse().unwrap();
        expect_true!(level_override.allows(static_meta!("other", Level::DEBUG)));
        expect_false!(level_override.allows(static_meta!("emulator_grpc::rpc", Level::WARN)));
        expect_true!(level_override.allows(static_meta!("emulator_grpc::rpc", Level::ERROR)));

        let subtarget_off: Filter = "tower_http=debug,tower_http::trace=off".parse().unwrap();
        expect_true!(subtarget_off.allows(static_meta!("tower_http::other", Level::DEBUG)));
        expect_false!(
            subtarget_off.allows(static_meta!("tower_http::trace::on_req", Level::ERROR))
        );

        let global_last_wins: Filter = "debug,info".parse().unwrap();
        expect_false!(global_last_wins.allows(static_meta!("any", Level::DEBUG)));
        expect_true!(global_last_wins.allows(static_meta!("any", Level::INFO)));
    }

    #[gtest]
    #[rstest]
    #[case("info,tower_http=off", tracing::level_filters::LevelFilter::INFO)]
    #[case("off", tracing::level_filters::LevelFilter::OFF)]
    #[case("", tracing::level_filters::LevelFilter::OFF)]
    #[case("warn,emulator_grpc=debug", tracing::level_filters::LevelFilter::DEBUG)]
    #[case("bare_target", tracing::level_filters::LevelFilter::TRACE)]
    #[case("error", tracing::level_filters::LevelFilter::ERROR)]
    fn max_level_returns_highest_directive_verbosity(
        #[case] directives: &str,
        #[case] expected: tracing::level_filters::LevelFilter,
    ) {
        let filter: Filter = directives.parse().unwrap();
        expect_eq!(filter.max_level(), expected);
    }
}
