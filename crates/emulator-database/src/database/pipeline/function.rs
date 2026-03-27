use googleapis::google::firestore::v1::{Document, Function, Value};
use itertools::Itertools;

use crate::{
    GenericDatabaseError,
    error::Result,
    pipeline::{expressions, get_exact_expr_args},
    unimplemented_collection,
};

pub(super) fn evaluate(
    Function {
        name,
        args,
        options,
    }: &Function,
    doc: &Document,
) -> Result<Option<Value>> {
    unimplemented_collection!(options);

    if name == "is_error" {
        let [arg] = get_exact_expr_args(args, name)?;
        let is_err = expressions::evaluate(arg, doc).is_err();
        return Ok(Some(Value::boolean(is_err)));
    }
    if name == "is_absent" {
        let [arg] = get_exact_expr_args(args, name)?;
        let is_absent = expressions::maybe_evaluate(arg, doc)?.is_none();
        return Ok(Some(Value::boolean(is_absent)));
    }

    let args: Vec<_> = args
        .iter()
        .map(|arg| expressions::evaluate(arg, doc))
        .try_collect()?;

    match name.as_str() {
        // String functions
        "string_concat" => {
            let mut res = String::new();
            for a in args {
                if a.is_null() {
                    return Ok(Some(Value::null()));
                }
                let s = a.as_string().ok_or_else(|| {
                    GenericDatabaseError::invalid_argument(
                        "string_concat arguments must evaluate to strings",
                    )
                })?;
                res.push_str(s);
            }
            Ok(Some(Value::string(res)))
        }
        "to_upper" | "to_lower" => {
            let [a] = get_exact_expr_args(&args, name)?;
            if a.is_null() {
                return Ok(Some(Value::null()));
            }
            let a = a.as_string().ok_or_else(|| {
                GenericDatabaseError::invalid_argument("Argument must be a string")
            })?;
            let res = if name == "to_upper" {
                a.to_uppercase()
            } else {
                a.to_lowercase()
            };
            Ok(Some(Value::string(res)))
        }
        "char_length" => {
            let [a] = get_exact_expr_args(&args, name)?;
            if a.is_null() {
                return Ok(Some(Value::null()));
            }
            let s = a.as_string().ok_or_else(|| {
                GenericDatabaseError::invalid_argument("Argument must be a string")
            })?;
            Ok(Some(Value::integer(s.chars().count() as i64)))
        }
        "split" => {
            let [a, sep] = get_exact_expr_args(&args, name)?;
            if a.is_null() || sep.is_null() {
                return Ok(Some(Value::null()));
            }
            let a = a.as_string().ok_or_else(|| {
                GenericDatabaseError::invalid_argument("Argument must be a string")
            })?;
            let sep = sep.as_string().ok_or_else(|| {
                GenericDatabaseError::invalid_argument("Argument must be a string")
            })?;
            let parts: Vec<Value> = a.split(sep).map(Value::string).collect();
            Ok(Some(Value::array(parts)))
        }

        // Number functions
        "add" | "subtract" | "multiply" | "divide" | "mod" | "pow" => {
            let [a, b] = get_exact_expr_args(&args, name)?;
            if a.is_null() || b.is_null() {
                return Ok(Some(Value::null()));
            }
            if let [Some(a), Some(b)] = [a.as_integer(), b.as_integer()] {
                let res = match name.as_str() {
                    "add" => a + b,
                    "subtract" => a - b,
                    "multiply" => a * b,
                    "divide" => {
                        if b == 0 {
                            return Err(GenericDatabaseError::invalid_argument("Division by zero"));
                        }
                        a / b
                    }
                    "mod" => {
                        if b == 0 {
                            return Err(GenericDatabaseError::invalid_argument("Modulo by zero"));
                        }
                        a % b
                    }
                    "pow" => a.pow(b as u32),
                    _ => unreachable!(),
                };
                return Ok(Some(Value::integer(res)));
            }

            let a = a.as_double().ok_or_else(|| {
                GenericDatabaseError::invalid_argument("First argument must be a number")
            })?;
            let b = b.as_double().ok_or_else(|| {
                GenericDatabaseError::invalid_argument("Second argument must be a number")
            })?;

            let res = match name.as_str() {
                "add" => a + b,
                "subtract" => a - b,
                "multiply" => a * b,
                "divide" => {
                    if b == 0.0 {
                        return Err(GenericDatabaseError::invalid_argument("Division by zero"));
                    }
                    a / b
                }
                "mod" => {
                    if b == 0.0 {
                        return Err(GenericDatabaseError::invalid_argument("Modulo by zero"));
                    }
                    a % b
                }
                "pow" => a.powf(b),
                _ => unreachable!(),
            };
            Ok(Some(Value::double(res)))
        }
        "abs" | "ceil" | "floor" | "round" | "sqrt" | "log10" | "ln" => {
            let [a] = get_exact_expr_args(&args, name)?;
            if a.is_null() {
                return Ok(Some(Value::null()));
            }
            // TODO: support integer arithmetic when argument is an integer
            let a = a.as_double().ok_or_else(|| {
                GenericDatabaseError::invalid_argument("Argument must be a number")
            })?;

            let res = match name.as_str() {
                "abs" => a.abs(),
                "ceil" => a.ceil(),
                "floor" => a.floor(),
                "round" => a.round(),
                "sqrt" => {
                    if a < 0.0 {
                        return Err(GenericDatabaseError::invalid_argument("Negative sqrt"));
                    }
                    a.sqrt()
                }
                "log10" => {
                    if a <= 0.0 {
                        return Err(GenericDatabaseError::invalid_argument("Non-positive log10"));
                    }
                    a.log10()
                }
                "ln" => {
                    if a <= 0.0 {
                        return Err(GenericDatabaseError::invalid_argument("Non-positive ln"));
                    }
                    a.ln()
                }
                _ => unreachable!(),
            };
            Ok(Some(Value::double(res)))
        }

        // Logic functions
        "not" => {
            let [a] = get_exact_expr_args(&args, name)?;
            let a = a.as_boolean().ok_or_else(|| {
                GenericDatabaseError::invalid_argument("First logic argument must be a boolean")
            })?;
            Ok(Some(Value::boolean(!a)))
        }
        "and" | "or" => {
            let [a, b] = get_exact_expr_args(&args, name)?;
            let a = a.as_boolean().ok_or_else(|| {
                GenericDatabaseError::invalid_argument("First logic argument must be a boolean")
            })?;
            let b = b.as_boolean().ok_or_else(|| {
                GenericDatabaseError::invalid_argument("Second logic argument must be a boolean")
            })?;
            let res = if name == "and" { a && b } else { a || b };
            Ok(Some(Value::boolean(res)))
        }

        // Comparison functions
        "equal"
        | "not_equal"
        | "greater_than"
        | "greater_than_or_equal"
        | "less_than"
        | "less_than_or_equal" => {
            let [a, b] = get_exact_expr_args(&args, name)?;
            let res = match name.as_str() {
                "equal" => a == b,
                "not_equal" => a != b,
                "greater_than" => a > b,
                "greater_than_or_equal" => a >= b,
                "less_than" => a < b,
                "less_than_or_equal" => a <= b,
                _ => unreachable!(),
            };
            Ok(Some(Value::boolean(res)))
        }

        // Array functions
        "array_length" => {
            let [arg] = get_exact_expr_args(&args, name)?;
            let arr = arg.as_array().ok_or_else(|| {
                GenericDatabaseError::invalid_argument("array_length requires an array argument")
            })?;
            Ok(Some(Value::integer(arr.len() as i64)))
        }
        "array" => Ok(Some(Value::array(args))),
        "array_concat" => {
            let mut result = vec![];
            for arg in args {
                let arr = arg.into_array().ok_or_else(|| {
                    GenericDatabaseError::invalid_argument(
                        "array_concat requires an array argument",
                    )
                })?;
                result.extend(arr);
            }
            Ok(Some(Value::array(result)))
        }

        _ => Err(GenericDatabaseError::not_implemented(format!(
            "function '{}' not supported inside expressions yet. Args: {:?}",
            name, args
        ))),
    }
}

// "coalesce" => {
//     for arg in args {
//         if !arg.is_null() {
//             return Ok(Some(arg));
//         }
//     }
//     Ok(Some(Value::null()))
// }
// "min" | "max" => {
//     if args.is_empty() {
//         return Err(GenericDatabaseError::invalid_argument(format!(
//             "{} requires at least one argument",
//             f.name
//         )));
//     }
//     let mut min_val = eval_args[0].clone();
//     for arg in eval_args.iter().skip(1) {
//         if arg.is_null() {
//             continue;
//         }
//         if min_val.is_null() || arg.cmp(&min_val) < cmp::Ordering::Equal {
//             min_val = arg.clone();
//         }
//     }
//     Ok(Some(min_val))
// }
// "sum" => {
//     // TODO: support integer arithmetic when arguments are integers
//     let mut sum = 0.0;
//     for arg in args {
//         if arg.is_null() {
//             continue;
//         }
//         sum += arg.as_double().ok_or_else(|| {
//             GenericDatabaseError::invalid_argument("Argument must be a number")
//         })?;
//     }
//     Ok(Some(Value::double(sum)))
// }
// "avg" => {
//     let mut sum = 0.0;
//     let mut count = 0;
//     for arg in args {
//         if arg.is_null() {
//             continue;
//         }
//         sum += arg.as_double().ok_or_else(|| {
//             GenericDatabaseError::invalid_argument("Argument must be a number")
//         })?;
//         count += 1;
//     }
//     if count == 0 {
//         return Ok(Some(Value::null()));
//     }
//     Ok(Some(Value::double(sum / count as f64)))
// }
// "count" => {
//     let count = args.iter().filter(|arg| !arg.is_null()).count() as i64;
//     Ok(Some(Value::integer(count)))
// }
// "length" => {
//     let [a] = get_exact_expr_args(&args, name)?;
//     if a.is_null() {
//         return Ok(Some(Value::null()));
//     }
//     let len = match a.value_type() {
//         ValueType::StringValue(s) => s.len() as i64,
//         ValueType::ArrayValue(ArrayValue { values }) => values.len() as i64,
//         ValueType::MapValue(MapValue { fields }) => fields.len() as i64,
//         _ => {
//             return Err(GenericDatabaseError::invalid_argument(
//                 "Argument must be a string, array, or map",
//             ));
//         }
//     };
//     Ok(Some(Value::integer(len)))
// }
// "reverse" => {
//     let [a] = get_exact_expr_args(&args, name)?;
//     if a.is_null() {
//         return Ok(Some(Value::null()));
//     }
//     let a = a.as_string().ok_or_else(|| {
//         GenericDatabaseError::invalid_argument("Argument must be a string")
//     })?;
//     let res = a.chars().rev().collect::<String>();
//     Ok(Some(Value::string(res)))
// }
// "sort" => {
//     let [a] = get_exact_expr_args(&args, &name)?;
//     if a.is_null() {
//         return Ok(Some(Value::null()));
//     }
//     let res = match a.value_type() {
//         ValueType::StringValue(_) => {
//             let mut chars: Vec<char> = a.as_string().unwrap().chars().collect();
//             chars.sort_unstable();
//             Value::string(chars.into_iter().collect::<String>())
//         }
//         ValueType::ArrayValue(ArrayValue { values }) => {
//             let mut sorted_values = values.clone();
//             sorted_values.sort_unstable();
//             Value::array(sorted_values)
//         }
//         _ => {
//             return Err(GenericDatabaseError::invalid_argument(
//                 "Argument must be a string or array",
//             ));
//         }
//     };
//     Ok(Some(res))
// }
// "slice" => {
//     let [a, start, end] = get_exact_expr_args(&args, &name)?;
//     if a.is_null() {
//         return Ok(Some(Value::null()));
//     }
//     let start = start.as_integer().ok_or_else(|| {
//         GenericDatabaseError::invalid_argument("Start index must be an integer")
//     })?;
//     let end = end.as_integer().ok_or_else(|| {
//         GenericDatabaseError::invalid_argument("End index must be an integer")
//     })?;
//     let res = match a.value_type() {
//         ValueType::StringValue(s) => {
//             let start = cmp::max(0, start) as usize;
//             let end = cmp::min(s.len() as i64, end) as usize;
//             if start >= end {
//                 Value::string("")
//             } else {
//                 Value::string(s.chars().skip(start).take(end -
// start).collect::<String>())             }
//         }
//         ValueType::ArrayValue(ArrayValue { values }) => {
//             let start = cmp::max(0, start) as usize;
//             let end = cmp::min(values.len() as i64, end) as usize;
//             if start >= end {
//                 Value::array(vec![])
//             } else {
//                 Value::array(values[start..end].to_vec())
//             }
//         }
//         _ => {
//             return Err(GenericDatabaseError::invalid_argument(
//                 "Argument must be a string or array",
//             ));
//         }
//     };
//     Ok(Some(res))
// }
// "substring" => {
//     let [a, start, length] = get_exact_expr_args(&args, &name)?;
//     if a.is_null() {
//         return Ok(Some(Value::null()));
//     }
//     let start = start.as_integer().ok_or_else(|| {
//         GenericDatabaseError::invalid_argument("Start index must be an integer")
//     })?;
//     let length = length.as_integer().ok_or_else(|| {
//         GenericDatabaseError::invalid_argument("Length must be an integer")
//         f.name
//     )))
// }
