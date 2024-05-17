use std::str::FromStr;

use ctreg::regex;
use itertools::Itertools;
use once_cell::sync::Lazy;
use thiserror::Error;
use tracing::{level_filters::LevelFilter, Metadata};

static DIRECTIVE_RE: Lazy<DirectiveRe> = Lazy::new(DirectiveRe::new);
regex! {
    DirectiveRe =
        r"(?x)
        ^(?P<global_level>(?i:trace|debug|info|warn|error|off|[0-5]))$ |
            #                 ^^^.
            #                     `note: we match log level names case-insensitively
        ^
        (?: # target name or span name
            (?P<target>[\w:-]+)|(?P<span>\[[^\]]*\])
        ) # TODO: support combination of target name and span name
        (?: # level or nothing
            =(?P<level>(?i:trace|debug|info|warn|error|off|[0-5]))?
                #          ^^^.
                #              `note: we match log level names case-insensitively
        )?
        $
        "
}

static SPAN_PART_RE: Lazy<SpanPartRe> = Lazy::new(SpanPartRe::new);
regex! { SpanPartRe = r"^(?P<name>[^\]\{]+)?(?:\{(?P<fields>[^\}]*)\})?$" }

// TODO: support field filters
// regex! {
//      FieldFilterRe =
//             r"(?x)
//         (
//             # field name
//             [[:word:]][[[:word:]]\.]*
//             # value part (optional)
//             (?:=[^,]+)?
//         )
//         # trailing comma or EOS
//         (?:,\s?|$)
//         "
// }

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
    max_level:  LevelFilter,
    directives: Vec<Directive>,
}

impl Filter {
    pub(crate) fn allows(&self, meta: &Metadata<'_>) -> bool {
        if meta.level() > &self.max_level {
            return false;
        }
        self.directives.iter().any(|d| d.allows(meta))
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
        let max_level = directives
            .iter()
            .map(|d| d.level)
            .max()
            .unwrap_or(LevelFilter::OFF);
        Ok(Self {
            max_level,
            directives,
        })
    }
}

#[derive(Debug)]
struct Directive {
    in_span: Option<String>,
    #[allow(unused)]
    fields:  Vec<FieldMatch>,
    target:  Option<String>,
    level:   LevelFilter,
}

impl FromStr for Directive {
    type Err = ParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let caps = DIRECTIVE_RE
            .captures(s)
            .ok_or_else(|| ParseError::InvalidDirective(s.to_owned()))?;

        if let Some(level) = caps.global_level {
            return Ok(Directive {
                in_span: None,
                fields:  vec![],
                target:  None,
                level:   level
                    .content
                    .parse()
                    .expect("regex should have prevented a parse error"),
            });
        }

        let target = caps.target.map(|cap| cap.content.to_owned());

        let (in_span, fields) = caps
            .span
            .map(|cap| {
                let cap = cap.content.trim_matches(|c| c == '[' || c == ']');
                let caps = SPAN_PART_RE
                    .captures(cap)
                    .ok_or_else(|| ParseError::InvalidSpanFilter(cap.to_owned()))?;
                let span = caps.name.map(|cap| cap.content.to_owned());
                //  TODO:
                let fields = match caps.fields {
                    Some(cap) => {
                        return Err(ParseError::InvalidSpanFilter(format!(
                            "field filters are not yet supported: {}",
                            cap.content
                        )));
                    }
                    None => vec![],
                };
                Ok((span, fields))
            })
            .transpose()?
            .unwrap_or((None, vec![]));

        let level = caps
            .level
            .map(|cap| {
                cap.content
                    .parse()
                    .expect("regex should have prevented a parse error")
            })
            .unwrap_or(LevelFilter::TRACE);

        Ok(Self {
            in_span,
            fields,
            target,
            level,
        })
    }
}

impl Directive {
    fn allows(&self, meta: &Metadata<'_>) -> bool {
        if meta.level() > &self.level {
            return false;
        }

        // Does this directive have a target filter, and does it match the
        // metadata's target?
        if let Some(ref target) = self.target {
            if !meta.target().starts_with(target) {
                return false;
            }
        }

        // Do we have a name filter, and does it match the metadata's name?
        if let Some(ref name) = self.in_span {
            if name != meta.name() {
                return false;
            }
        }

        // Does the metadata define all the fields that this directive cares about?
        // TODO: Support field matching
        // let actual_fields = meta.fields();
        // for expected_field in &self.fields {
        //     // Does the actual field set (from the metadata) contain this field?
        //     if actual_fields.field(&expected_field.name).is_none() {
        //         return false;
        //     }
        // }

        true
    }
}

#[derive(Debug)]
struct FieldMatch {
    // name: String,
}
