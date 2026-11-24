use std::{cell::RefCell, collections::HashMap, future::Future};

use googleapis::google::{
    firestore::v1::{
        Value,
        structured_query::{field_filter, unary_filter},
    },
    protobuf::Timestamp,
    r#type::LatLng,
};
use proptest::{prelude::*, sample::select};

use super::{
    builders::{
        PipelineSpec, QuerySpec, SortDirection, field_filter, field_ref_val, fn_val, unary_filter,
    },
    client::TestDb,
    env::{DOCUMENTS_ROOT, TestEnv},
};

/// Minimum valid Proto3 `Timestamp` seconds (`0001-01-01T00:00:00Z`).
const MIN_TIMESTAMP_SECONDS: i64 = -62_135_596_800;
/// Maximum valid Proto3 `Timestamp` seconds (`9999-12-31T23:59:59Z`).
const MAX_TIMESTAMP_SECONDS: i64 = 253_402_300_799;

/// Fixed 16-ID vocabulary preventing unbounded `string_cache::DefaultAtom` global interning across
/// multi-case `proptest` suites.
pub(crate) const DOC_ID_VOCAB: [&str; 16] = [
    "doc_00", "doc_01", "doc_02", "doc_03", "doc_04", "doc_05", "doc_06", "doc_07", "doc_08",
    "doc_09", "doc_10", "doc_11", "doc_12", "doc_13", "doc_14", "doc_15",
];

pub(crate) const FIELD_VOCAB: [&str; 6] = ["alpha", "beta", "gamma", "num", "tag", "flag"];

const TAG_VOCAB: [&str; 4] = ["north", "south", "east", "west"];

/// Builds a `ProptestConfig` defaulting to `64` cases while honoring any `PROPTEST_CASES` env
/// override (such as `PROPTEST_CASES=128` in CI).
pub(crate) fn proptest_config() -> ProptestConfig {
    let cases = std::env::var("PROPTEST_CASES")
        .ok()
        .and_then(|raw| raw.parse::<u32>().ok())
        .unwrap_or(64);
    ProptestConfig::with_cases(cases)
}

pub(crate) fn arb_doc_id() -> impl Strategy<Value = &'static str> {
    select(&DOC_ID_VOCAB[..])
}

pub(crate) fn arb_field_name() -> impl Strategy<Value = &'static str> {
    select(&FIELD_VOCAB[..])
}

/// Generates storable, wire-roundtrippable `Value` instances respecting Firestore constraints:
/// valid `ReferenceValue`s, `1..=4` non-NaN vectors (including `-0.0` and `±Infinity`), extreme
/// `i64` boundaries, canonical `f64::NAN`, RFC 3339-clamped `Timestamp`s (including negative
/// epochs), edge-case strings, and non-nested arrays (`[[...]]` is forbidden by Firestore).
pub(crate) fn arb_value() -> impl Strategy<Value = Value> {
    let arb_i64 = prop_oneof![
        3 => -1_000i64..=1_000i64,
        1 => select(
            &[
                i64::MIN,
                i64::MAX,
                -(1i64 << 53) - 1,
                -(1i64 << 53),
                -(1i64 << 53) + 1,
                (1i64 << 53) - 1,
                1i64 << 53,
                (1i64 << 53) + 1,
            ][..]
        ),
    ];

    let arb_f64_non_nan = prop_oneof![
        6 => (-500i32..=500i32).prop_map(|n| f64::from(n) * 0.25),
        1 => Just(0.0),
        1 => Just(-0.0),
        1 => Just(f64::INFINITY),
        1 => Just(f64::NEG_INFINITY),
        2 => select(&[f64::MIN_POSITIVE, 5e-324_f64, f64::MIN, f64::MAX][..]),
    ];

    // Emit canonical `f64::NAN` only (never `-f64::NAN` or payload NaNs) so ProtoJSON roundtrips
    // remain bit-identical under `Value::fields_storage_eq`.
    let arb_double = prop_oneof![
        9 => arb_f64_non_nan.clone(),
        1 => Just(f64::NAN),
    ]
    .prop_map(Value::from);

    let arb_timestamp = (
        prop_oneof![
            MIN_TIMESTAMP_SECONDS..=MAX_TIMESTAMP_SECONDS,
            select(&[MIN_TIMESTAMP_SECONDS, -1i64, 0i64, MAX_TIMESTAMP_SECONDS][..]),
        ],
        0i32..1_000_000_000i32,
    )
        .prop_map(|(seconds, nanos)| Value::from(Timestamp { seconds, nanos }));

    let arb_string = select(
        &[
            "",
            "amber",
            "cobalt",
            "crimson",
            "jade",
            "a\0b",
            "line1\nline2\r\n\ttab",
            r#"quote"and\backslash"#,
            "\x01\x08\x0c\x1f",
            "héllo 🌍 café 🚀 日本語",
        ][..],
    )
    .prop_map(Value::from);

    let scalar = prop_oneof![
        Just(Value::null()),
        any::<bool>().prop_map(Value::from),
        arb_i64.prop_map(Value::from),
        arb_double,
        arb_string,
        prop::collection::vec(any::<u8>(), 0..6).prop_map(Value::from),
        arb_timestamp,
        arb_doc_id().prop_map(|id| Value::reference(format!("{DOCUMENTS_ROOT}/ref_col/{id}"))),
        (-80i32..=80i32, -170i32..=170i32).prop_map(|(lat, lng)| {
            Value::from(LatLng {
                latitude:  f64::from(lat) * 0.5,
                longitude: f64::from(lng) * 0.5,
            })
        }),
    ];

    let vector = prop::collection::vec(arb_f64_non_nan, 1..=4).prop_map(Value::vector);

    let nested_map = prop::collection::hash_map(
        arb_field_name().prop_map(str::to_string),
        prop_oneof![
            4 => scalar.clone(),
            1 => vector.clone(),
        ],
        0..4,
    )
    .prop_map(Value::from);

    let non_array = prop_oneof![
        4 => scalar,
        1 => vector,
        1 => nested_map,
    ];

    prop_oneof![
        3 => non_array.clone(),
        1 => prop::collection::vec(non_array.clone(), 0..4).prop_map(Value::from),
        1 => prop::collection::hash_map(
            arb_field_name().prop_map(str::to_string),
            prop_oneof![
                3 => non_array.clone(),
                1 => prop::collection::vec(non_array, 0..3).prop_map(Value::from),
            ],
            0..4,
        )
        .prop_map(Value::from),
    ]
}

/// Generates query operand values weighted toward common elements emitted inside `arb_value`
/// alongside arbitrary storable `Value`s so `alpha` filters exercise both match hits and complex
/// cross-type boundaries.
pub(crate) fn arb_alpha_filter_value() -> impl Strategy<Value = Value> {
    prop_oneof![
        1 => Just(Value::null()),
        1 => any::<bool>().prop_map(Value::from),
        2 => (-4i64..=4i64).prop_map(Value::from),
        2 => prop_oneof![
            (-4i32..=4i32).prop_map(|n| Value::from(f64::from(n) * 0.25)),
            Just(Value::from(0.0)),
            Just(Value::from(-0.0)),
            Just(Value::from(f64::NAN)),
            Just(Value::from(f64::INFINITY)),
            Just(Value::from(f64::NEG_INFINITY)),
        ],
        2 => select(&["", "amber", "cobalt", "crimson", "jade"][..]).prop_map(Value::from),
        1 => arb_doc_id().prop_map(|id| Value::reference(format!("{DOCUMENTS_ROOT}/ref_col/{id}"))),
        2 => arb_value(),
    ]
}

/// Generates `num` cursor bounds mixing `IntegerValue` and `DoubleValue` (including fractional
/// `.5` values, `5.0`/`5.5`, `-0.0`, `±Infinity`, and `NaN`) to test cross-type cursor slicing.
pub(crate) fn arb_cursor_num() -> impl Strategy<Value = Value> {
    prop_oneof![
        3 => (-55i64..=55i64).prop_map(Value::integer),
        3 => (-110i32..=110i32).prop_map(|n| Value::double(f64::from(n) * 0.5)),
        1 => select(
            &[
                -0.0_f64,
                0.0_f64,
                5.0_f64,
                5.5_f64,
                -5.5_f64,
                f64::NEG_INFINITY,
                f64::INFINITY,
                f64::NAN,
            ][..],
        )
        .prop_map(Value::double),
    ]
}

pub(crate) fn arb_doc_fields() -> impl Strategy<Value = HashMap<String, Value>> {
    (
        prop::option::of(-50i64..=50i64),
        prop::option::of(select(&TAG_VOCAB[..])),
        prop::option::of(any::<bool>()),
        prop::option::of(arb_value()),
    )
        .prop_map(|(num, tag, flag, alpha)| {
            let mut fields = HashMap::new();
            if let Some(n) = num {
                fields.insert("num".to_string(), Value::from(n));
            }
            if let Some(t) = tag {
                fields.insert("tag".to_string(), Value::from(t));
            }
            if let Some(f) = flag {
                fields.insert("flag".to_string(), Value::from(f));
            }
            if let Some(a) = alpha {
                fields.insert("alpha".to_string(), a);
            }
            fields
        })
}

#[derive(Clone, Debug)]
pub(crate) enum GeneratedQueryKind {
    NumGreaterOrEqual(i64),
    NumLessThan(i64),
    TagEquals(&'static str),
    TagNotEquals(&'static str),
    EnterpriseTagNotEqualsMissingIncluded(&'static str),
    TagIn(&'static str, Option<&'static str>),
    FlagEquals(bool),
    AlphaIsNull,
    AlphaIsNan,
    ArrayContains(Value),
    AlphaEquals(Value),
    AlphaLessThan(Value),
    AlphaGreaterOrEqual(Value),
    AlphaIn(Vec<Value>),
    AlphaOrderBy { descending: bool },
}

impl GeneratedQueryKind {
    /// Returns `true` for query shapes valid only in Enterprise Edition (e.g., `!=` without
    /// ordering on the inequality field, which includes documents missing the field).
    pub(crate) fn is_enterprise_only(&self) -> bool {
        matches!(self, Self::EnterpriseTagNotEqualsMissingIncluded(_))
    }

    /// Returns `true` when Standard Edition and Enterprise Edition `StructuredQuery` must yield
    /// identical matching documents (i.e., not Enterprise-only and not matching top-level
    /// `null`/`NaN` equality or range comparisons).
    pub(crate) fn same_across_editions(&self) -> bool {
        match self {
            Self::EnterpriseTagNotEqualsMissingIncluded(_) => false,
            Self::ArrayContains(v) | Self::AlphaEquals(v) | Self::AlphaGreaterOrEqual(v) => {
                !v.is_null() && !v.is_nan()
            }
            Self::AlphaIn(vals) => !vals.iter().any(|v| v.is_null() || v.is_nan()),
            Self::NumGreaterOrEqual(_)
            | Self::NumLessThan(_)
            | Self::TagEquals(_)
            | Self::TagNotEquals(_)
            | Self::TagIn(..)
            | Self::FlagEquals(_)
            | Self::AlphaIsNull
            | Self::AlphaIsNan
            | Self::AlphaLessThan(_)
            | Self::AlphaOrderBy { .. } => true,
        }
    }

    pub(crate) fn to_query_spec(
        &self,
        col: &str,
        limit: Option<usize>,
        offset: usize,
    ) -> QuerySpec {
        let mut spec = match self {
            Self::NumGreaterOrEqual(threshold) => QuerySpec::collection(col)
                .where_filter(field_filter(
                    "num",
                    field_filter::Operator::GreaterThanOrEqual,
                    *threshold,
                ))
                .order_asc("num")
                .order_asc("__name__"),
            Self::NumLessThan(threshold) => QuerySpec::collection(col)
                .where_filter(field_filter(
                    "num",
                    field_filter::Operator::LessThan,
                    *threshold,
                ))
                .order_desc("num")
                .order_desc("__name__"),
            Self::TagEquals(tag) => QuerySpec::collection(col)
                .where_filter(field_filter("tag", field_filter::Operator::Equal, *tag))
                .order_asc("num")
                .order_asc("__name__"),
            Self::TagNotEquals(tag) => QuerySpec::collection(col)
                .where_filter(field_filter("tag", field_filter::Operator::NotEqual, *tag))
                .order_asc("tag")
                .order_asc("__name__"),
            Self::EnterpriseTagNotEqualsMissingIncluded(tag) => QuerySpec::collection(col)
                .where_filter(field_filter("tag", field_filter::Operator::NotEqual, *tag))
                .order_asc("__name__"),
            Self::TagIn(first, second) => {
                let values = match second {
                    Some(second) => vec![Value::from(*first), Value::from(*second)],
                    None => vec![Value::from(*first)],
                };
                QuerySpec::collection(col)
                    .where_filter(field_filter("tag", field_filter::Operator::In, values))
                    .order_asc("__name__")
            }
            Self::FlagEquals(flag) => QuerySpec::collection(col)
                .where_filter(field_filter("flag", field_filter::Operator::Equal, *flag))
                .order_asc("num")
                .order_asc("__name__"),
            Self::AlphaIsNull => QuerySpec::collection(col)
                .where_filter(unary_filter("alpha", unary_filter::Operator::IsNull))
                .order_asc("__name__"),
            Self::AlphaIsNan => QuerySpec::collection(col)
                .where_filter(unary_filter("alpha", unary_filter::Operator::IsNan))
                .order_asc("__name__"),
            Self::ArrayContains(val) => QuerySpec::collection(col)
                .where_filter(field_filter(
                    "alpha",
                    field_filter::Operator::ArrayContains,
                    val.clone(),
                ))
                .order_asc("__name__"),
            Self::AlphaEquals(val) => QuerySpec::collection(col)
                .where_filter(field_filter(
                    "alpha",
                    field_filter::Operator::Equal,
                    val.clone(),
                ))
                .order_asc("__name__"),
            Self::AlphaLessThan(val) => QuerySpec::collection(col)
                .where_filter(field_filter(
                    "alpha",
                    field_filter::Operator::LessThan,
                    val.clone(),
                ))
                .order_desc("alpha")
                .order_desc("__name__"),
            Self::AlphaGreaterOrEqual(val) => QuerySpec::collection(col)
                .where_filter(field_filter(
                    "alpha",
                    field_filter::Operator::GreaterThanOrEqual,
                    val.clone(),
                ))
                .order_asc("alpha")
                .order_asc("__name__"),
            Self::AlphaIn(vals) => QuerySpec::collection(col)
                .where_filter(field_filter(
                    "alpha",
                    field_filter::Operator::In,
                    Value::array(vals.clone()),
                ))
                .order_asc("__name__"),
            Self::AlphaOrderBy { descending } => {
                if *descending {
                    QuerySpec::collection(col)
                        .order_desc("alpha")
                        .order_desc("__name__")
                } else {
                    QuerySpec::collection(col)
                        .order_asc("alpha")
                        .order_asc("__name__")
                }
            }
        };
        if offset > 0 {
            spec = spec.offset(i32::try_from(offset).expect("offset fits in i32"));
        }
        if let Some(lim) = limit {
            spec = spec.limit(i32::try_from(lim).expect("limit fits in i32"));
        }
        spec
    }

    pub(crate) fn to_pipeline_spec(
        &self,
        col: &str,
        limit: Option<usize>,
        offset: usize,
    ) -> Option<PipelineSpec> {
        let present =
            |field: &str| fn_val("not", vec![fn_val("is_absent", vec![field_ref_val(field)])]);
        let mut spec = match self {
            Self::NumGreaterOrEqual(t) => PipelineSpec::collection(col)
                .where_expr(fn_val(
                    "and",
                    vec![
                        present("num"),
                        fn_val(
                            "greater_than_or_equal",
                            vec![field_ref_val("num"), (*t).into()],
                        ),
                    ],
                ))
                .sort([
                    ("num", SortDirection::Asc),
                    ("__name__", SortDirection::Asc),
                ]),
            Self::NumLessThan(t) => PipelineSpec::collection(col)
                .where_expr(fn_val(
                    "and",
                    vec![
                        present("num"),
                        fn_val("less_than", vec![field_ref_val("num"), (*t).into()]),
                    ],
                ))
                .sort([
                    ("num", SortDirection::Desc),
                    ("__name__", SortDirection::Desc),
                ]),
            Self::TagEquals(tag) => PipelineSpec::collection(col)
                .where_expr(fn_val(
                    "and",
                    vec![
                        present("num"),
                        fn_val("equal", vec![field_ref_val("tag"), (*tag).into()]),
                    ],
                ))
                .sort([
                    ("num", SortDirection::Asc),
                    ("__name__", SortDirection::Asc),
                ]),
            Self::TagNotEquals(tag) => PipelineSpec::collection(col)
                .where_expr(fn_val(
                    "and",
                    vec![
                        present("tag"),
                        fn_val("not_equal", vec![field_ref_val("tag"), (*tag).into()]),
                    ],
                ))
                .sort([
                    ("tag", SortDirection::Asc),
                    ("__name__", SortDirection::Asc),
                ]),
            Self::EnterpriseTagNotEqualsMissingIncluded(tag) => PipelineSpec::collection(col)
                .where_expr(fn_val(
                    "not_equal",
                    vec![field_ref_val("tag"), (*tag).into()],
                ))
                .sort_asc("__name__"),
            Self::TagIn(first, second) => {
                let pred = match second {
                    Some(second) => fn_val(
                        "or",
                        vec![
                            fn_val("equal", vec![field_ref_val("tag"), (*first).into()]),
                            fn_val("equal", vec![field_ref_val("tag"), (*second).into()]),
                        ],
                    ),
                    None => fn_val("equal", vec![field_ref_val("tag"), (*first).into()]),
                };
                PipelineSpec::collection(col)
                    .where_expr(pred)
                    .sort_asc("__name__")
            }
            Self::FlagEquals(flag) => PipelineSpec::collection(col)
                .where_expr(fn_val(
                    "and",
                    vec![
                        present("num"),
                        fn_val("equal", vec![field_ref_val("flag"), (*flag).into()]),
                    ],
                ))
                .sort([
                    ("num", SortDirection::Asc),
                    ("__name__", SortDirection::Asc),
                ]),
            Self::AlphaIsNull => PipelineSpec::collection(col)
                .where_expr(fn_val("equal", vec![field_ref_val("alpha"), Value::null()]))
                .sort_asc("__name__"),
            Self::AlphaIsNan => PipelineSpec::collection(col)
                .where_expr(fn_val(
                    "equal",
                    vec![field_ref_val("alpha"), Value::double(f64::NAN)],
                ))
                .sort_asc("__name__"),
            // Pipeline `array_contains` is not yet implemented in `pipeline::function`.
            Self::ArrayContains(_) => return None,
            Self::AlphaEquals(val) => PipelineSpec::collection(col)
                .where_expr(fn_val("equal", vec![field_ref_val("alpha"), val.clone()]))
                .sort_asc("__name__"),
            Self::AlphaLessThan(val) => PipelineSpec::collection(col)
                .where_expr(fn_val(
                    "and",
                    vec![
                        present("alpha"),
                        fn_val("less_than", vec![field_ref_val("alpha"), val.clone()]),
                    ],
                ))
                .sort([
                    ("alpha", SortDirection::Desc),
                    ("__name__", SortDirection::Desc),
                ]),
            Self::AlphaGreaterOrEqual(val) => PipelineSpec::collection(col)
                .where_expr(fn_val(
                    "and",
                    vec![
                        present("alpha"),
                        fn_val(
                            "greater_than_or_equal",
                            vec![field_ref_val("alpha"), val.clone()],
                        ),
                    ],
                ))
                .sort([
                    ("alpha", SortDirection::Asc),
                    ("__name__", SortDirection::Asc),
                ]),
            Self::AlphaIn(vals) => {
                let pred = if vals.len() == 1 {
                    fn_val("equal", vec![field_ref_val("alpha"), vals[0].clone()])
                } else {
                    fn_val(
                        "or",
                        vals.iter()
                            .cloned()
                            .map(|v| fn_val("equal", vec![field_ref_val("alpha"), v]))
                            .collect(),
                    )
                };
                PipelineSpec::collection(col)
                    .where_expr(pred)
                    .sort_asc("__name__")
            }
            Self::AlphaOrderBy { descending } => {
                let dir = if *descending {
                    SortDirection::Desc
                } else {
                    SortDirection::Asc
                };
                PipelineSpec::collection(col)
                    .where_expr(present("alpha"))
                    .sort([("alpha", dir), ("__name__", dir)])
            }
        };
        if offset > 0 {
            spec = spec.offset(i64::try_from(offset).expect("offset fits in i64"));
        }
        if let Some(lim) = limit {
            spec = spec.limit(i64::try_from(lim).expect("limit fits in i64"));
        }
        Some(spec)
    }
}

pub(crate) fn arb_query_kind() -> impl Strategy<Value = GeneratedQueryKind> {
    let arb_tag = select(&TAG_VOCAB[..]);
    prop_oneof![
        (-50i64..=50i64).prop_map(GeneratedQueryKind::NumGreaterOrEqual),
        (-50i64..=50i64).prop_map(GeneratedQueryKind::NumLessThan),
        arb_tag.clone().prop_map(GeneratedQueryKind::TagEquals),
        arb_tag.clone().prop_map(GeneratedQueryKind::TagNotEquals),
        arb_tag
            .clone()
            .prop_map(GeneratedQueryKind::EnterpriseTagNotEqualsMissingIncluded),
        (arb_tag.clone(), prop::option::of(arb_tag))
            .prop_map(|(a, b)| GeneratedQueryKind::TagIn(a, b)),
        any::<bool>().prop_map(GeneratedQueryKind::FlagEquals),
        Just(GeneratedQueryKind::AlphaIsNull),
        Just(GeneratedQueryKind::AlphaIsNan),
        arb_alpha_filter_value().prop_map(GeneratedQueryKind::ArrayContains),
        arb_alpha_filter_value().prop_map(GeneratedQueryKind::AlphaEquals),
        arb_alpha_filter_value().prop_map(GeneratedQueryKind::AlphaLessThan),
        arb_alpha_filter_value().prop_map(GeneratedQueryKind::AlphaGreaterOrEqual),
        prop::collection::vec(arb_alpha_filter_value(), 1..=3)
            .prop_map(GeneratedQueryKind::AlphaIn),
        any::<bool>().prop_map(|descending| GeneratedQueryKind::AlphaOrderBy { descending }),
    ]
}

/// Reusable single-runtime sequential runner for async `proptest!` blocks: builds one shared
/// Tokio current-thread runtime and one `FirestoreProject` per edition, clearing the `(default)`
/// database at the start of every generated case.
#[derive(Debug)]
pub(crate) struct AsyncProptestEnv {
    rt: tokio::runtime::Runtime,
    db: TestDb,
}

impl AsyncProptestEnv {
    pub(crate) fn new(enterprise: bool) -> Self {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("proptest current_thread runtime");
        let env = if enterprise {
            TestEnv::enterprise()
        } else {
            TestEnv::new()
        };
        let db = env.build_db();
        Self { rt, db }
    }

    pub(crate) fn run_case<T, F, Fut>(&mut self, f: F) -> Result<T, TestCaseError>
    where
        F: FnOnce(TestDb) -> Fut,
        Fut: Future<Output = Result<T, TestCaseError>>,
    {
        let db = self.db.clone();
        self.rt.block_on(async move {
            db.clear_default_db().await;
            f(db).await
        })
    }
}

thread_local! {
    static STANDARD_PROPTEST_ENV: RefCell<AsyncProptestEnv> =
        RefCell::new(AsyncProptestEnv::new(false));
    static ENTERPRISE_PROPTEST_ENV: RefCell<AsyncProptestEnv> =
        RefCell::new(AsyncProptestEnv::new(true));
}

pub(crate) fn run_standard_proptest<T, F, Fut>(f: F) -> Result<T, TestCaseError>
where
    F: FnOnce(TestDb) -> Fut,
    Fut: Future<Output = Result<T, TestCaseError>>,
{
    STANDARD_PROPTEST_ENV.with(|cell| cell.borrow_mut().run_case(f))
}

pub(crate) fn run_enterprise_proptest<T, F, Fut>(f: F) -> Result<T, TestCaseError>
where
    F: FnOnce(TestDb) -> Fut,
    Fut: Future<Output = Result<T, TestCaseError>>,
{
    ENTERPRISE_PROPTEST_ENV.with(|cell| cell.borrow_mut().run_case(f))
}
