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
    #[error("invalid span filter: {0:?}")]
    InvalidSpanFilter(String),
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
