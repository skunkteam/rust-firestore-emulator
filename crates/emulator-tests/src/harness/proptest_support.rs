use std::{collections::HashMap, future::Future};

use googleapis::google::{
    firestore::v1::{Value, structured_query::field_filter},
    protobuf::Timestamp,
    r#type::LatLng,
};
use proptest::{prelude::*, sample::select};

use super::{
    builders::{QuerySpec, field_filter},
    client::TestDb,
    env::TestEnv,
};

/// Fixed 16-ID vocabulary preventing unbounded `string_cache::DefaultAtom` global interning across
/// multi-case `proptest` suites.
pub(crate) const DOC_ID_VOCAB: [&str; 16] = [
    "doc_00", "doc_01", "doc_02", "doc_03", "doc_04", "doc_05", "doc_06", "doc_07", "doc_08",
    "doc_09", "doc_10", "doc_11", "doc_12", "doc_13", "doc_14", "doc_15",
];

pub(crate) const FIELD_VOCAB: [&str; 6] = ["alpha", "beta", "gamma", "num", "tag", "flag"];

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

/// Generates storable, wire-roundtrippable `Value` instances without nested arrays (`[[...]]`),
/// which Firestore forbids in stored documents.
pub(crate) fn arb_value() -> impl Strategy<Value = Value> {
    let scalar = prop_oneof![
        Just(Value::null()),
        any::<bool>().prop_map(Value::from),
        (-1_000i64..=1_000i64).prop_map(Value::from),
        prop_oneof![
            (-500i32..=500i32).prop_map(|n| Value::from(f64::from(n) * 0.25)),
            Just(Value::from(0.0)),
            Just(Value::from(-0.0)),
            Just(Value::from(f64::NAN)),
        ],
        select(&["amber", "cobalt", "crimson", "jade", ""][..]).prop_map(Value::from),
        prop::collection::vec(any::<u8>(), 0..6).prop_map(Value::from),
        (0i64..2_000_000_000i64, 0i32..1_000_000_000i32)
            .prop_map(|(seconds, nanos)| Value::from(Timestamp { seconds, nanos })),
        (-80i32..=80i32, -170i32..=170i32).prop_map(|(lat, lng)| {
            Value::from(LatLng {
                latitude:  f64::from(lat) * 0.5,
                longitude: f64::from(lng) * 0.5,
            })
        }),
    ];

    let map_of_scalars = prop::collection::hash_map(
        arb_field_name().prop_map(str::to_string),
        scalar.clone(),
        0..4,
    )
    .prop_map(Value::from);

    let non_array = prop_oneof![scalar, map_of_scalars];

    prop_oneof![
        non_array.clone(),
        prop::collection::vec(non_array.clone(), 0..4).prop_map(Value::from),
        prop::collection::hash_map(
            arb_field_name().prop_map(str::to_string),
            prop_oneof![
                non_array.clone(),
                prop::collection::vec(non_array, 0..3).prop_map(Value::from),
            ],
            0..4,
        )
        .prop_map(Value::from),
    ]
}

pub(crate) fn arb_doc_fields() -> impl Strategy<Value = HashMap<String, Value>> {
    (
        -50i64..=50i64,
        select(&["north", "south", "east", "west"][..]),
        any::<bool>(),
        arb_value(),
    )
        .prop_map(|(num, tag, flag, extra)| {
            HashMap::from([
                ("num".to_string(), Value::from(num)),
                ("tag".to_string(), Value::from(tag)),
                ("flag".to_string(), Value::from(flag)),
                ("alpha".to_string(), extra),
            ])
        })
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum GeneratedQueryKind {
    NumGreaterOrEqual(i64),
    NumLessThan(i64),
    TagEquals(&'static str),
    FlagEquals(bool),
}

impl GeneratedQueryKind {
    pub(crate) fn to_query_spec(self, col: &str) -> QuerySpec {
        match self {
            Self::NumGreaterOrEqual(threshold) => QuerySpec::collection(col)
                .where_filter(field_filter(
                    "num",
                    field_filter::Operator::GreaterThanOrEqual,
                    threshold,
                ))
                .order_asc("num"),
            Self::NumLessThan(threshold) => QuerySpec::collection(col)
                .where_filter(field_filter(
                    "num",
                    field_filter::Operator::LessThan,
                    threshold,
                ))
                .order_desc("num"),
            Self::TagEquals(tag) => QuerySpec::collection(col)
                .where_filter(field_filter("tag", field_filter::Operator::Equal, tag))
                .order_asc("num"),
            Self::FlagEquals(flag) => QuerySpec::collection(col)
                .where_filter(field_filter("flag", field_filter::Operator::Equal, flag))
                .order_asc("num"),
        }
    }
}

pub(crate) fn arb_query_kind() -> impl Strategy<Value = GeneratedQueryKind> {
    prop_oneof![
        (-50i64..=50i64).prop_map(GeneratedQueryKind::NumGreaterOrEqual),
        (-50i64..=50i64).prop_map(GeneratedQueryKind::NumLessThan),
        select(&["north", "south", "east", "west"][..]).prop_map(GeneratedQueryKind::TagEquals),
        any::<bool>().prop_map(GeneratedQueryKind::FlagEquals),
    ]
}

/// Reusable single-runtime sequential runner for async `proptest!` blocks: builds one shared
/// Tokio current-thread runtime and one `FirestoreProject` per `proptest` function, clearing the
/// `(default)` database at the start of every generated case.
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

    pub(crate) fn run_case<F, Fut>(&mut self, f: F) -> Result<(), TestCaseError>
    where
        F: FnOnce(TestDb) -> Fut,
        Fut: Future<Output = Result<(), TestCaseError>>,
    {
        let db = self.db.clone();
        self.rt.block_on(async move {
            db.clear_default_db().await;
            f(db).await
        })
    }
}
