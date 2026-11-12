/// Constructs a Firestore [`Value`](crate::google::firestore::v1::Value) while preserving exact
/// wire-type distinctions (`IntegerValue` vs. `DoubleValue`, `BytesValue`, `TimestampValue`,
/// `GeoPointValue`, `ReferenceValue`, `NaN`).
#[macro_export]
macro_rules! val {
    (@array [$($elems:expr,)*]) => {
        ::std::vec![$($elems,)*]
    };
    (@array [$($elems:expr,)*] null $(, $($rest:tt)*)?) => {
        $crate::val!(@array [$($elems,)* $crate::val!(null),] $($($rest)*)?)
    };
    (@array [$($elems:expr,)*] [$($arr:tt)*] $(, $($rest:tt)*)?) => {
        $crate::val!(@array [$($elems,)* $crate::val!([$($arr)*]),] $($($rest)*)?)
    };
    (@array [$($elems:expr,)*] {$($map:tt)*} $(, $($rest:tt)*)?) => {
        $crate::val!(@array [$($elems,)* $crate::val!({$($map)*}),] $($($rest)*)?)
    };
    (@array [$($elems:expr,)*] $next:expr, $($rest:tt)*) => {
        $crate::val!(@array [$($elems,)* $crate::val!($next),] $($rest)*)
    };
    (@array [$($elems:expr,)*] $last:expr) => {
        $crate::val!(@array [$($elems,)* $crate::val!($last),])
    };
    (null) => {
        $crate::google::firestore::v1::Value {
            value_type: ::core::option::Option::Some(
                $crate::google::firestore::v1::value::ValueType::NullValue(0),
            ),
        }
    };
    ([$($elems:tt)*]) => {
        $crate::google::firestore::v1::Value {
            value_type: ::core::option::Option::Some(
                $crate::google::firestore::v1::value::ValueType::ArrayValue(
                    $crate::google::firestore::v1::ArrayValue {
                        values: $crate::val!(@array [] $($elems)*),
                    },
                ),
            ),
        }
    };
    ({$($entries:tt)*}) => {
        $crate::google::firestore::v1::Value {
            value_type: ::core::option::Option::Some(
                $crate::google::firestore::v1::value::ValueType::MapValue(
                    $crate::google::firestore::v1::MapValue {
                        fields: $crate::fields!({ $($entries)* }),
                    },
                ),
            ),
        }
    };
    ($val:expr) => {
        $crate::google::firestore::v1::Value::from($val)
    };
}

/// Constructs a `HashMap<String, Value>` suitable for Firestore `Document::fields` or `MapValue`.
#[macro_export]
macro_rules! fields {
    (@map [$($entries:expr,)*]) => {
        ::std::collections::HashMap::<
            ::std::string::String,
            $crate::google::firestore::v1::Value,
        >::from([$($entries,)*])
    };
    (@map [$($entries:expr,)*] $key:tt : null $(, $($rest:tt)*)?) => {
        $crate::fields!(
            @map [
                $($entries,)*
                (::std::convert::Into::<::std::string::String>::into($key), $crate::val!(null)),
            ]
            $($($rest)*)?
        )
    };
    (@map [$($entries:expr,)*] $key:tt : [$($arr:tt)*] $(, $($rest:tt)*)?) => {
        $crate::fields!(
            @map [
                $($entries,)*
                (::std::convert::Into::<::std::string::String>::into($key), $crate::val!([$($arr)*])),
            ]
            $($($rest)*)?
        )
    };
    (@map [$($entries:expr,)*] $key:tt : {$($sub:tt)*} $(, $($rest:tt)*)?) => {
        $crate::fields!(
            @map [
                $($entries,)*
                (::std::convert::Into::<::std::string::String>::into($key), $crate::val!({$($sub)*})),
            ]
            $($($rest)*)?
        )
    };
    (@map [$($entries:expr,)*] $key:tt : $val:expr, $($rest:tt)*) => {
        $crate::fields!(
            @map [
                $($entries,)*
                (::std::convert::Into::<::std::string::String>::into($key), $crate::val!($val)),
            ]
            $($rest)*
        )
    };
    (@map [$($entries:expr,)*] $key:tt : $val:expr) => {
        $crate::fields!(
            @map [
                $($entries,)*
                (::std::convert::Into::<::std::string::String>::into($key), $crate::val!($val)),
            ]
        )
    };
    () => {
        $crate::fields!(@map [])
    };
    ({ $($entries:tt)* }) => {
        $crate::fields!(@map [] $($entries)*)
    };
    ($key:tt : $($rest:tt)+) => {
        $crate::fields!(@map [] $key : $($rest)+)
    };
}

/// Constructs a Firestore [`Document`](crate::google::firestore::v1::Document) with empty
/// timestamps (`None`) and either an empty `name` or a caller-provided resource name.
#[macro_export]
macro_rules! doc {
    () => {
        $crate::doc!({})
    };
    ({ $($fields:tt)* }) => {
        $crate::google::firestore::v1::Document {
            name: ::std::string::String::new(),
            fields: $crate::fields!({ $($fields)* }),
            create_time: ::core::option::Option::None,
            update_time: ::core::option::Option::None,
        }
    };
    ($name:expr, { $($fields:tt)* } $(,)?) => {
        $crate::google::firestore::v1::Document {
            name: ::std::convert::Into::<::std::string::String>::into($name),
            fields: $crate::fields!({ $($fields)* }),
            create_time: ::core::option::Option::None,
            update_time: ::core::option::Option::None,
        }
    };
    ($key:tt : $($rest:tt)+) => {
        $crate::doc!({ $key : $($rest)+ })
    };
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use bytes::Bytes;
    use googletest::prelude::*;

    use crate::google::{
        firestore::v1::{Document, Value, value::ValueType},
        protobuf::Timestamp,
        r#type::LatLng,
    };

    #[gtest]
    fn val_macro_exercises_every_rule_and_preserves_wire_types() {
        let empty_arr = val!([]);
        expect_true!(empty_arr.storage_eq(&Value::array(vec![])));

        let empty_map = val!({});
        expect_true!(empty_map.storage_eq(&Value::map(HashMap::new())));

        let single_null_arr = val!([null]);
        expect_true!(single_null_arr.storage_eq(&Value::array(vec![Value::null()])));

        let trailing_comma_arr = val!([1, 2,]);
        expect_true!(
            trailing_comma_arr
                .storage_eq(&Value::array(vec![Value::integer(1), Value::integer(2)]))
        );

        let ts = Timestamp {
            seconds: 1_700_000_000,
            nanos:   500,
        };
        let geo = LatLng {
            latitude:  52.37,
            longitude: 4.89,
        };
        let complex = val!([
            null,
            true,
            42,
            42.0,
            -0.0,
            f64::NAN,
            "hello",
            b"raw",
            Bytes::from_static(b"bytes"),
            ts,
            geo,
            Value::reference("projects/p/databases/(default)/documents/users/alice".to_string()),
            Some(99),
            None::<i64>,
            [10, 20],
            { "nested": "ok" }
        ]);
        let items = complex.as_array().unwrap();
        expect_eq!(items.len(), 16);
        expect_true!(items[0].storage_eq(&Value::null()));
        expect_true!(items[1].storage_eq(&Value::boolean(true)));
        expect_true!(items[2].storage_eq(&Value::integer(42)));
        expect_true!(items[3].storage_eq(&Value::double(42.0)));
        expect_false!(items[2].storage_eq(&items[3]));
        expect_true!(items[4].storage_eq(&Value::double(-0.0)));
        expect_false!(items[4].storage_eq(&Value::double(0.0)));
        expect_true!(items[5].is_nan());
        expect_true!(items[6].storage_eq(&Value::string("hello")));
        expect_true!(items[7].storage_eq(&Value::bytes(&b"raw"[..])));
        expect_true!(items[8].storage_eq(&Value::bytes(&b"bytes"[..])));
        expect_true!(items[9].storage_eq(&Value::timestamp(ts)));
        expect_true!(items[10].storage_eq(&Value {
            value_type: Some(ValueType::GeoPointValue(geo)),
        }));
        expect_eq!(
            items[11].as_reference(),
            Some("projects/p/databases/(default)/documents/users/alice")
        );
        expect_true!(items[12].storage_eq(&Value::integer(99)));
        expect_true!(items[13].storage_eq(&Value::null()));
        expect_true!(
            items[14].storage_eq(&Value::array(vec![Value::integer(10), Value::integer(20)]))
        );
        expect_true!(items[15].storage_eq(&Value::map(HashMap::from([(
            "nested".to_string(),
            Value::string("ok"),
        )]))));
    }

    #[gtest]
    fn fields_and_doc_macros_exercise_every_invocation_form() {
        let f_empty = fields! {};
        expect_true!(f_empty.is_empty());

        let f_braced = fields! {
            "n": null,
            "arr": [1, 2],
            "sub": { "x": true },
            "i": 7,
            "s": "last"
        };
        let f_paren = fields!({
            "n": null,
            "arr": [1, 2],
            "sub": { "x": true },
            "i": 7,
            "s": "last",
        });
        expect_true!(Value::fields_storage_eq(&f_braced, &f_paren));

        let d_empty: Document = doc! {};
        expect_eq!(d_empty.name, "");
        expect_true!(d_empty.fields.is_empty());
        expect_eq!(d_empty.create_time, None);
        expect_eq!(d_empty.update_time, None);

        let d_braced = doc! {
            "name": "Alice",
            "age": 42,
            "score": 42.0,
        };
        let d_paren = doc!({
            "name": "Alice",
            "age": 42,
            "score": 42.0
        });
        expect_eq!(d_braced.name, "");
        expect_true!(Value::fields_storage_eq(&d_braced.fields, &d_paren.fields));

        let d_named = doc!(
            "projects/p/databases/(default)/documents/users/alice",
            {
                "name": "Alice",
                "tags": ["admin", null],
                "meta": { "v": 1 },
            },
        );
        expect_eq!(
            d_named.name,
            "projects/p/databases/(default)/documents/users/alice"
        );
        expect_true!(d_named.fields["name"].storage_eq(&Value::string("Alice")));
    }

    #[cfg(feature = "client")]
    #[gtest]
    fn client_feature_generates_firestore_client_type() {
        fn _assert_client_exists<T>(
            _: Option<crate::google::firestore::v1::firestore_client::FirestoreClient<T>>,
        ) {
        }
        _assert_client_exists::<tonic::transport::Channel>(None);
    }
}
