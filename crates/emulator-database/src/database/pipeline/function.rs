use googleapis::google::firestore::v1::{Document, Function, Value, value::ValueType};
use itertools::Itertools;

use crate::{
    GenericDatabaseError,
    database::vector::{self, DistanceMeasure},
    error::Result,
    pipeline::{expressions, get_exact_expr_args},
    unimplemented, unimplemented_collection,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum LogicBinaryOp {
    And,
    Or,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ComparisonOp {
    Equal,
    NotEqual,
    GreaterThan,
    GreaterThanOrEqual,
    LessThan,
    LessThanOrEqual,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CaseOp {
    Upper,
    Lower,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum IntArithmeticOp {
    Add,
    Subtract,
    Multiply,
    Divide,
    Mod,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BinaryArithmeticOp {
    Int(IntArithmeticOp),
    Pow,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum UnaryIntPreservingMathOp {
    Abs,
    Ceil,
    Floor,
    Round,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum UnaryFloatMathOp {
    Sqrt,
    Log10,
    Ln,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum UnaryMathOp {
    IntPreserving(UnaryIntPreservingMathOp),
    Float(UnaryFloatMathOp),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BuiltinFunction {
    IsError,
    IsAbsent,
    VectorDistance(DistanceMeasure),
    VectorLength,
    Not,
    Logic(LogicBinaryOp),
    Comparison(ComparisonOp),
    StringConcat,
    StringCase(CaseOp),
    CharLength,
    Split,
    BinaryArithmetic(BinaryArithmeticOp),
    UnaryMath(UnaryMathOp),
    ArrayLength,
    Array,
    ArrayConcat,
}

impl BuiltinFunction {
    fn from_name(name: &str) -> Option<Self> {
        Some(match name {
            "is_error" => Self::IsError,
            "is_absent" => Self::IsAbsent,
            "cosine_distance" => Self::VectorDistance(DistanceMeasure::Cosine),
            "euclidean_distance" => Self::VectorDistance(DistanceMeasure::Euclidean),
            "dot_product" => Self::VectorDistance(DistanceMeasure::DotProduct),
            "vector_length" => Self::VectorLength,
            "not" => Self::Not,
            "and" => Self::Logic(LogicBinaryOp::And),
            "or" => Self::Logic(LogicBinaryOp::Or),
            "equal" => Self::Comparison(ComparisonOp::Equal),
            "not_equal" => Self::Comparison(ComparisonOp::NotEqual),
            "greater_than" => Self::Comparison(ComparisonOp::GreaterThan),
            "greater_than_or_equal" => Self::Comparison(ComparisonOp::GreaterThanOrEqual),
            "less_than" => Self::Comparison(ComparisonOp::LessThan),
            "less_than_or_equal" => Self::Comparison(ComparisonOp::LessThanOrEqual),
            "string_concat" => Self::StringConcat,
            "to_upper" => Self::StringCase(CaseOp::Upper),
            "to_lower" => Self::StringCase(CaseOp::Lower),
            "char_length" => Self::CharLength,
            "split" => Self::Split,
            "add" => Self::BinaryArithmetic(BinaryArithmeticOp::Int(IntArithmeticOp::Add)),
            "subtract" => {
                Self::BinaryArithmetic(BinaryArithmeticOp::Int(IntArithmeticOp::Subtract))
            }
            "multiply" => {
                Self::BinaryArithmetic(BinaryArithmeticOp::Int(IntArithmeticOp::Multiply))
            }
            "divide" => Self::BinaryArithmetic(BinaryArithmeticOp::Int(IntArithmeticOp::Divide)),
            "mod" => Self::BinaryArithmetic(BinaryArithmeticOp::Int(IntArithmeticOp::Mod)),
            "pow" => Self::BinaryArithmetic(BinaryArithmeticOp::Pow),
            "abs" => Self::UnaryMath(UnaryMathOp::IntPreserving(UnaryIntPreservingMathOp::Abs)),
            "ceil" => Self::UnaryMath(UnaryMathOp::IntPreserving(UnaryIntPreservingMathOp::Ceil)),
            "floor" => Self::UnaryMath(UnaryMathOp::IntPreserving(UnaryIntPreservingMathOp::Floor)),
            "round" => Self::UnaryMath(UnaryMathOp::IntPreserving(UnaryIntPreservingMathOp::Round)),
            "sqrt" => Self::UnaryMath(UnaryMathOp::Float(UnaryFloatMathOp::Sqrt)),
            "log10" => Self::UnaryMath(UnaryMathOp::Float(UnaryFloatMathOp::Log10)),
            "ln" => Self::UnaryMath(UnaryMathOp::Float(UnaryFloatMathOp::Ln)),
            "array_length" => Self::ArrayLength,
            "array" => Self::Array,
            "array_concat" => Self::ArrayConcat,
            _ => return None,
        })
    }
}

fn pipeline_type_name(v: &Value) -> &'static str {
    match v.value_type.as_ref() {
        None | Some(ValueType::NullValue(_)) => "NULL",
        Some(ValueType::BooleanValue(_)) => "BOOLEAN",
        Some(ValueType::IntegerValue(_)) => "INT64",
        Some(ValueType::DoubleValue(_)) => "FLOAT64",
        Some(ValueType::TimestampValue(_)) => "TIMESTAMP",
        Some(ValueType::StringValue(_)) => "STRING",
        Some(ValueType::BytesValue(_)) => "BYTES",
        Some(ValueType::ReferenceValue(_)) => "PATH",
        Some(ValueType::GeoPointValue(_)) => "GEOPOINT",
        Some(ValueType::ArrayValue(_)) => "ARRAY",
        Some(ValueType::MapValue(_)) => "MAP",
        Some(ValueType::FieldReferenceValue(_)) => "FIELD_REFERENCE",
        // TODO: verify exact Cloud Firestore type name once pipeline variables are supported.
        Some(ValueType::VariableReferenceValue(_)) => "VARIABLE_REFERENCE",
        Some(ValueType::FunctionValue(_)) => "FUNCTION",
        Some(ValueType::PipelineValue(_)) => "PIPELINE",
    }
}

fn expected_vector_error(fn_name: &str, v: &Value) -> GenericDatabaseError {
    GenericDatabaseError::invalid_argument(format!(
        "The function {fn_name}(...) requires `Vector` but got `{}`.",
        pipeline_type_name(v)
    ))
}

fn eval_one_arg(args: &[Value], name: &str, doc: &Document) -> Result<Value> {
    let [arg] = get_exact_expr_args(args, name)?;
    expressions::evaluate(arg, doc)
}

fn eval_two_args(args: &[Value], name: &str, doc: &Document) -> Result<[Value; 2]> {
    let [a_expr, b_expr] = get_exact_expr_args(args, name)?;
    Ok([
        expressions::evaluate(a_expr, doc)?,
        expressions::evaluate(b_expr, doc)?,
    ])
}

fn eval_two_maybe_args(args: &[Value], name: &str, doc: &Document) -> Result<[Option<Value>; 2]> {
    let [a_expr, b_expr] = get_exact_expr_args(args, name)?;
    Ok([
        expressions::maybe_evaluate(a_expr, doc)?,
        expressions::maybe_evaluate(b_expr, doc)?,
    ])
}

fn eval_variadic_args(args: &[Value], doc: &Document) -> Result<Vec<Value>> {
    args.iter()
        .map(|arg| expressions::evaluate(arg, doc))
        .try_collect()
}

pub(super) fn evaluate(
    Function {
        name,
        args,
        options,
    }: &Function,
    doc: &Document,
) -> Result<Option<Value>> {
    unimplemented_collection!(options);

    let Some(func) = BuiltinFunction::from_name(name) else {
        unimplemented!(format!(
            "function {name}({})",
            args.iter().map(|v| format!("{v:?}")).join(", ")
        ));
    };

    match func {
        BuiltinFunction::IsError => eval_is_error(args, name, doc),
        BuiltinFunction::IsAbsent => eval_is_absent(args, name, doc),
        BuiltinFunction::VectorDistance(measure) => eval_vector_distance(measure, args, doc),
        BuiltinFunction::VectorLength => eval_vector_length(args, name, doc),
        BuiltinFunction::Not => eval_not(args, name, doc),
        BuiltinFunction::Logic(op) => eval_logic(op, args, name, doc),
        BuiltinFunction::Comparison(op) => eval_comparison(op, args, name, doc),
        BuiltinFunction::StringConcat => eval_string_concat(args, doc),
        BuiltinFunction::StringCase(op) => eval_string_case(op, args, name, doc),
        BuiltinFunction::CharLength => eval_char_length(args, name, doc),
        BuiltinFunction::Split => eval_split(args, name, doc),
        BuiltinFunction::BinaryArithmetic(op) => eval_binary_arithmetic(op, args, name, doc),
        BuiltinFunction::UnaryMath(op) => eval_unary_math(op, args, name, doc),
        BuiltinFunction::ArrayLength => eval_array_length(args, name, doc),
        BuiltinFunction::Array => Ok(Some(Value::array(eval_variadic_args(args, doc)?))),
        BuiltinFunction::ArrayConcat => eval_array_concat(args, doc),
    }
}

fn eval_is_error(args: &[Value], name: &str, doc: &Document) -> Result<Option<Value>> {
    let [arg] = get_exact_expr_args(args, name)?;
    match expressions::evaluate(arg, doc) {
        Ok(_) => Ok(Some(Value::boolean(false))),
        Err(GenericDatabaseError::InvalidArgument(_)) => Ok(Some(Value::boolean(true))),
        Err(err) => Err(err),
    }
}

fn eval_is_absent(args: &[Value], name: &str, doc: &Document) -> Result<Option<Value>> {
    let [arg] = get_exact_expr_args(args, name)?;
    let is_absent = expressions::maybe_evaluate(arg, doc)?.is_none();
    Ok(Some(Value::boolean(is_absent)))
}

fn eval_vector_distance(
    measure: DistanceMeasure,
    args: &[Value],
    doc: &Document,
) -> Result<Option<Value>> {
    let fn_name = measure.fn_name();
    let [a, b] = eval_two_maybe_args(args, fn_name, doc)?;
    let (Some(a), Some(b)) = (a, b) else {
        return Ok(Some(Value::null()));
    };
    if a.is_null() || b.is_null() {
        return Ok(Some(Value::null()));
    }
    let a_vals = a
        .iter_vector_f64()
        .ok_or_else(|| expected_vector_error(fn_name, &a))?;
    let b_vals = b
        .iter_vector_f64()
        .ok_or_else(|| expected_vector_error(fn_name, &b))?;
    if a_vals.len() != b_vals.len() {
        return Err(GenericDatabaseError::invalid_argument(format!(
            "{fn_name}(...) arguments must be the same length."
        )));
    }
    let dist = vector::compute_distance(measure, a_vals, b_vals)?;
    Ok(Some(Value::double(dist)))
}

fn eval_vector_length(args: &[Value], name: &str, doc: &Document) -> Result<Option<Value>> {
    let [arg] = get_exact_expr_args(args, name)?;
    let Some(v) = expressions::maybe_evaluate(arg, doc)? else {
        return Ok(Some(Value::null()));
    };
    if v.is_null() {
        return Ok(Some(Value::null()));
    }
    let vals = v
        .as_vector_values()
        .ok_or_else(|| expected_vector_error(name, &v))?;
    Ok(Some(Value::integer(vals.len() as i64)))
}

fn eval_not(args: &[Value], name: &str, doc: &Document) -> Result<Option<Value>> {
    let val = eval_one_arg(args, name, doc)?;
    if val.is_null() {
        return Ok(Some(Value::null()));
    }
    let b = val.as_boolean().ok_or_else(|| {
        GenericDatabaseError::invalid_argument("First logic argument must be a boolean")
    })?;
    Ok(Some(Value::boolean(!b)))
}

fn eval_logic(
    op: LogicBinaryOp,
    args: &[Value],
    name: &str,
    doc: &Document,
) -> Result<Option<Value>> {
    if args.len() < 2 {
        return Err(GenericDatabaseError::invalid_argument(format!(
            "{name} expects at least 2 arguments"
        )));
    }
    let short_circuit_on = op == LogicBinaryOp::Or;
    let mut saw_null = false;
    for (idx, arg) in args.iter().enumerate() {
        let val = expressions::evaluate(arg, doc)?;
        if val.is_null() {
            saw_null = true;
            continue;
        }
        let b = val.as_boolean().ok_or_else(|| {
            GenericDatabaseError::invalid_argument(format!(
                "Logic argument {} must be a boolean",
                idx + 1
            ))
        })?;
        if b == short_circuit_on {
            return Ok(Some(Value::boolean(b)));
        }
    }
    Ok(Some(if saw_null {
        Value::null()
    } else {
        Value::boolean(!short_circuit_on)
    }))
}

fn eval_comparison(
    op: ComparisonOp,
    args: &[Value],
    name: &str,
    doc: &Document,
) -> Result<Option<Value>> {
    let [a, b] = eval_two_maybe_args(args, name, doc)?;
    let res = match (a, b) {
        (None, None) => matches!(
            op,
            ComparisonOp::Equal | ComparisonOp::LessThanOrEqual | ComparisonOp::GreaterThanOrEqual
        ),
        (None, Some(_)) | (Some(_), None) => op == ComparisonOp::NotEqual,
        (Some(a), Some(b)) => match op {
            ComparisonOp::Equal => a == b,
            ComparisonOp::NotEqual => a != b,
            ComparisonOp::GreaterThan => a.is_compatible_with(&b) && a > b,
            ComparisonOp::GreaterThanOrEqual => a.is_compatible_with(&b) && a >= b,
            ComparisonOp::LessThan => a.is_compatible_with(&b) && a < b,
            ComparisonOp::LessThanOrEqual => a.is_compatible_with(&b) && a <= b,
        },
    };
    Ok(Some(Value::boolean(res)))
}

fn eval_string_concat(args: &[Value], doc: &Document) -> Result<Option<Value>> {
    // Evaluate all operands first so expression errors (including `NotImplemented`) take
    // precedence over sibling type errors.
    let evaluated = eval_variadic_args(args, doc)?;
    let mut res = String::new();
    let mut saw_null = false;
    for a in evaluated {
        if a.is_null() {
            saw_null = true;
            continue;
        }
        let s = a.as_string().ok_or_else(|| {
            GenericDatabaseError::invalid_argument(
                "string_concat arguments must evaluate to strings",
            )
        })?;
        if !saw_null {
            res.push_str(s);
        }
    }
    Ok(Some(if saw_null {
        Value::null()
    } else {
        Value::string(res)
    }))
}

fn eval_string_case(
    op: CaseOp,
    args: &[Value],
    name: &str,
    doc: &Document,
) -> Result<Option<Value>> {
    let a = eval_one_arg(args, name, doc)?;
    if a.is_null() {
        return Ok(Some(Value::null()));
    }
    let s = a
        .as_string()
        .ok_or_else(|| GenericDatabaseError::invalid_argument("Argument must be a string"))?;
    let res = match op {
        CaseOp::Upper => s.to_uppercase(),
        CaseOp::Lower => s.to_lowercase(),
    };
    Ok(Some(Value::string(res)))
}

fn eval_char_length(args: &[Value], name: &str, doc: &Document) -> Result<Option<Value>> {
    let a = eval_one_arg(args, name, doc)?;
    if a.is_null() {
        return Ok(Some(Value::null()));
    }
    let s = a
        .as_string()
        .ok_or_else(|| GenericDatabaseError::invalid_argument("Argument must be a string"))?;
    Ok(Some(Value::integer(s.chars().count() as i64)))
}

fn eval_split(args: &[Value], name: &str, doc: &Document) -> Result<Option<Value>> {
    let [a, sep] = eval_two_args(args, name, doc)?;
    if a.is_null() || sep.is_null() {
        if (!a.is_null() && a.as_string().is_none())
            || (!sep.is_null() && sep.as_string().is_none())
        {
            return Err(GenericDatabaseError::invalid_argument(
                "Argument must be a string",
            ));
        }
        return Ok(Some(Value::null()));
    }
    let a = a
        .as_string()
        .ok_or_else(|| GenericDatabaseError::invalid_argument("Argument must be a string"))?;
    let sep = sep
        .as_string()
        .ok_or_else(|| GenericDatabaseError::invalid_argument("Argument must be a string"))?;
    let parts: Vec<Value> = if sep.is_empty() {
        if a.is_empty() {
            vec![Value::string("")]
        } else {
            a.chars().map(|c| Value::string(c.to_string())).collect()
        }
    } else {
        a.split(sep).map(Value::string).collect()
    };
    Ok(Some(Value::array(parts)))
}

fn eval_binary_arithmetic(
    op: BinaryArithmeticOp,
    args: &[Value],
    name: &str,
    doc: &Document,
) -> Result<Option<Value>> {
    let [a, b] = eval_two_args(args, name, doc)?;
    if a.is_null() || b.is_null() {
        if (!a.is_null() && !a.is_number()) || (!b.is_null() && !b.is_number()) {
            return Err(GenericDatabaseError::invalid_argument(
                "Arguments must be numbers",
            ));
        }
        return Ok(Some(Value::null()));
    }
    if let BinaryArithmeticOp::Int(int_op) = op
        && let [Some(a), Some(b)] = [a.as_integer(), b.as_integer()]
    {
        let res = match int_op {
            IntArithmeticOp::Add => a
                .checked_add(b)
                .ok_or_else(|| GenericDatabaseError::invalid_argument("Integer overflow in add"))?,
            IntArithmeticOp::Subtract => a.checked_sub(b).ok_or_else(|| {
                GenericDatabaseError::invalid_argument("Integer overflow in subtract")
            })?,
            IntArithmeticOp::Multiply => a.checked_mul(b).ok_or_else(|| {
                GenericDatabaseError::invalid_argument("Integer overflow in multiply")
            })?,
            IntArithmeticOp::Divide => {
                if b == 0 {
                    return Err(GenericDatabaseError::invalid_argument("Division by zero"));
                }
                a.checked_div(b).ok_or_else(|| {
                    GenericDatabaseError::invalid_argument("Integer overflow in divide")
                })?
            }
            IntArithmeticOp::Mod => {
                if b == 0 {
                    return Err(GenericDatabaseError::invalid_argument("Modulo by zero"));
                }
                a.checked_rem(b).ok_or_else(|| {
                    GenericDatabaseError::invalid_argument("Integer overflow in mod")
                })?
            }
        };
        return Ok(Some(Value::integer(res)));
    }

    let a = a
        .as_double()
        .ok_or_else(|| GenericDatabaseError::invalid_argument("First argument must be a number"))?;
    let b = b.as_double().ok_or_else(|| {
        GenericDatabaseError::invalid_argument("Second argument must be a number")
    })?;

    let res = match op {
        BinaryArithmeticOp::Int(IntArithmeticOp::Add) => a + b,
        BinaryArithmeticOp::Int(IntArithmeticOp::Subtract) => a - b,
        BinaryArithmeticOp::Int(IntArithmeticOp::Multiply) => a * b,
        BinaryArithmeticOp::Int(IntArithmeticOp::Divide) => {
            if b == 0.0 {
                return Err(GenericDatabaseError::invalid_argument("Division by zero"));
            }
            a / b
        }
        BinaryArithmeticOp::Int(IntArithmeticOp::Mod) => {
            if b == 0.0 {
                return Err(GenericDatabaseError::invalid_argument("Modulo by zero"));
            }
            a % b
        }
        BinaryArithmeticOp::Pow => {
            if a == 0.0 && b < 0.0 {
                return Err(GenericDatabaseError::invalid_argument(
                    "pow division by zero",
                ));
            }
            let res = a.powf(b);
            if res.is_nan() && !a.is_nan() && !b.is_nan() {
                return Err(GenericDatabaseError::invalid_argument("pow invalid domain"));
            }
            res
        }
    };
    Ok(Some(Value::double(res)))
}

fn eval_unary_math(
    op: UnaryMathOp,
    args: &[Value],
    name: &str,
    doc: &Document,
) -> Result<Option<Value>> {
    let a = eval_one_arg(args, name, doc)?;
    if a.is_null() {
        return Ok(Some(Value::null()));
    }
    if let UnaryMathOp::IntPreserving(int_op) = op
        && let Some(i) = a.as_integer()
    {
        let res = match int_op {
            UnaryIntPreservingMathOp::Abs => i
                .checked_abs()
                .ok_or_else(|| GenericDatabaseError::invalid_argument("Integer overflow in abs"))?,
            UnaryIntPreservingMathOp::Ceil
            | UnaryIntPreservingMathOp::Floor
            | UnaryIntPreservingMathOp::Round => i,
        };
        return Ok(Some(Value::integer(res)));
    }
    let a = a
        .as_double()
        .ok_or_else(|| GenericDatabaseError::invalid_argument("Argument must be a number"))?;

    let res = match op {
        UnaryMathOp::IntPreserving(UnaryIntPreservingMathOp::Abs) => a.abs(),
        UnaryMathOp::IntPreserving(UnaryIntPreservingMathOp::Ceil) => a.ceil(),
        UnaryMathOp::IntPreserving(UnaryIntPreservingMathOp::Floor) => a.floor(),
        UnaryMathOp::IntPreserving(UnaryIntPreservingMathOp::Round) => a.round(),
        UnaryMathOp::Float(UnaryFloatMathOp::Sqrt) => {
            if a < 0.0 {
                return Err(GenericDatabaseError::invalid_argument("Negative sqrt"));
            }
            a.sqrt()
        }
        UnaryMathOp::Float(UnaryFloatMathOp::Log10) => {
            if a <= 0.0 {
                return Err(GenericDatabaseError::invalid_argument("Non-positive log10"));
            }
            a.log10()
        }
        UnaryMathOp::Float(UnaryFloatMathOp::Ln) => {
            if a <= 0.0 {
                return Err(GenericDatabaseError::invalid_argument("Non-positive ln"));
            }
            a.ln()
        }
    };
    Ok(Some(Value::double(res)))
}

fn eval_array_length(args: &[Value], name: &str, doc: &Document) -> Result<Option<Value>> {
    let arg = eval_one_arg(args, name, doc)?;
    if arg.is_null() {
        return Ok(Some(Value::null()));
    }
    let arr = arg.as_array().ok_or_else(|| {
        GenericDatabaseError::invalid_argument("array_length requires an array argument")
    })?;
    Ok(Some(Value::integer(arr.len() as i64)))
}

fn eval_array_concat(args: &[Value], doc: &Document) -> Result<Option<Value>> {
    // Evaluate all operands first so expression errors (including `NotImplemented`) take
    // precedence over sibling type errors.
    let evaluated = eval_variadic_args(args, doc)?;
    let mut result = Vec::new();
    let mut saw_null = false;
    for arg in evaluated {
        if arg.is_null() {
            saw_null = true;
            continue;
        }
        let arr = arg.into_array().ok_or_else(|| {
            GenericDatabaseError::invalid_argument("array_concat requires an array argument")
        })?;
        if !saw_null {
            result.extend(arr);
        }
    }
    Ok(Some(if saw_null {
        Value::null()
    } else {
        Value::array(result)
    }))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    fn fn_expr(name: &str, args: Vec<Value>) -> Value {
        Value {
            value_type: Some(ValueType::FunctionValue(Function {
                name: name.to_string(),
                args,
                options: HashMap::new(),
            })),
        }
    }

    fn field_expr(path: &str) -> Value {
        Value {
            value_type: Some(ValueType::FieldReferenceValue(path.to_string())),
        }
    }

    fn eval_fn(name: &str, args: Vec<Value>) -> Result<Option<Value>> {
        eval_fn_on(&Document::default(), name, args)
    }

    fn eval_fn_on(doc: &Document, name: &str, args: Vec<Value>) -> Result<Option<Value>> {
        evaluate(
            &Function {
                name: name.to_string(),
                args,
                options: HashMap::new(),
            },
            doc,
        )
    }

    #[test]
    fn pow_one_nan_and_nan_zero_evaluate_to_one() {
        for args in [
            vec![Value::double(1.0), Value::double(f64::NAN)],
            vec![Value::integer(1), Value::double(f64::NAN)],
            vec![Value::double(f64::NAN), Value::double(0.0)],
            vec![Value::double(f64::NAN), Value::integer(0)],
        ] {
            let val = eval_fn("pow", args.clone())
                .unwrap()
                .expect("pow should return a value");
            assert!(
                val.storage_eq(&Value::double(1.0)),
                "expected 1.0 for pow({args:?}), got {val:?}"
            );
        }
    }

    #[test]
    fn string_concat_and_split_with_null_still_reject_non_string_arguments() {
        for (name, args) in [
            ("string_concat", vec![Value::null(), Value::integer(123)]),
            (
                "string_concat",
                vec![Value::string("a"), Value::null(), Value::integer(123)],
            ),
            ("split", vec![Value::null(), Value::integer(123)]),
            ("split", vec![Value::integer(123), Value::null()]),
        ] {
            let res = eval_fn(name, args.clone());
            assert!(
                matches!(res, Err(GenericDatabaseError::InvalidArgument(_))),
                "expected InvalidArgument for {name}({args:?}), got {res:?}"
            );
        }
    }

    #[test]
    fn precedence_options_before_unknown_before_arity_before_arg_evaluation() {
        // Non-empty options fails with NotImplemented even on unknown fn or wrong arity.
        for (name, args) in [
            ("unknown_fn", vec![]),
            ("add", vec![Value::integer(1)]),
            ("is_error", vec![]),
        ] {
            let err = evaluate(
                &Function {
                    name: name.to_string(),
                    args,
                    options: HashMap::from([("opt".to_string(), Value::boolean(true))]),
                },
                &Document::default(),
            )
            .unwrap_err();
            assert!(matches!(err, GenericDatabaseError::NotImplemented(_)));
        }

        // Unknown function rejects before evaluating failing/invalid arguments (and escapes
        // is_error).
        let div_zero = fn_expr("divide", vec![Value::integer(1), Value::integer(0)]);
        assert!(matches!(
            eval_fn("unknown_fn", vec![div_zero.clone()]),
            Err(GenericDatabaseError::NotImplemented(_))
        ));
        assert!(matches!(
            eval_fn("is_error", vec![fn_expr("unknown_fn", vec![div_zero])]),
            Err(GenericDatabaseError::NotImplemented(_))
        ));

        // Arity check runs before evaluating arguments (`unknown_fn()` is never evaluated).
        let unknown = fn_expr("unknown_fn", vec![]);
        for (name, args) in [
            (
                "add",
                vec![Value::integer(1), Value::integer(2), unknown.clone()],
            ),
            ("and", vec![unknown.clone()]),
            ("or", vec![unknown.clone()]),
            ("is_absent", vec![unknown.clone(), Value::integer(1)]),
            ("vector_length", vec![]),
            ("cosine_distance", vec![Value::vector([1.0])]),
            ("not", vec![]),
            ("equal", vec![Value::integer(1)]),
            ("to_upper", vec![]),
            ("split", vec![Value::string("a")]),
            ("abs", vec![]),
            ("array_length", vec![]),
        ] {
            let err = eval_fn(name, args.clone()).unwrap_err();
            assert!(
                matches!(err, GenericDatabaseError::InvalidArgument(_)),
                "expected arity InvalidArgument for {name}({args:?}), got {err:?}"
            );
        }

        // `is_error` catches arity errors on inner calls, but rejects its own bad arity.
        let caught_arity = eval_fn(
            "is_error",
            vec![fn_expr(
                "add",
                vec![Value::integer(1), Value::integer(2), unknown],
            )],
        )
        .unwrap();
        assert_eq!(caught_arity, Some(Value::boolean(true)));
        assert!(matches!(
            eval_fn("is_error", vec![Value::integer(1), Value::integer(2)]),
            Err(GenericDatabaseError::InvalidArgument(_))
        ));
    }

    #[test]
    fn is_error_and_is_absent_semantics() {
        let doc = Document {
            fields: HashMap::from([
                ("n".to_string(), Value::null()),
                ("v".to_string(), Value::integer(7)),
            ]),
            ..Default::default()
        };
        assert_eq!(
            eval_fn_on(&doc, "is_error", vec![field_expr("missing")]).unwrap(),
            Some(Value::boolean(false))
        );
        assert_eq!(
            eval_fn_on(&doc, "is_error", vec![field_expr("n")]).unwrap(),
            Some(Value::boolean(false))
        );
        assert_eq!(
            eval_fn_on(
                &doc,
                "is_error",
                vec![fn_expr(
                    "divide",
                    vec![Value::integer(1), Value::integer(0)]
                )],
            )
            .unwrap(),
            Some(Value::boolean(true))
        );

        assert_eq!(
            eval_fn_on(&doc, "is_absent", vec![field_expr("missing")]).unwrap(),
            Some(Value::boolean(true))
        );
        assert_eq!(
            eval_fn_on(&doc, "is_absent", vec![field_expr("n")]).unwrap(),
            Some(Value::boolean(false))
        );
        assert_eq!(
            eval_fn_on(&doc, "is_absent", vec![field_expr("v")]).unwrap(),
            Some(Value::boolean(false))
        );
        assert!(matches!(
            eval_fn_on(
                &doc,
                "is_absent",
                vec![fn_expr(
                    "divide",
                    vec![Value::integer(1), Value::integer(0)]
                )],
            ),
            Err(GenericDatabaseError::InvalidArgument(_))
        ));
        assert!(matches!(
            eval_fn_on(&doc, "is_absent", vec![fn_expr("unknown_fn", vec![])]),
            Err(GenericDatabaseError::NotImplemented(_))
        ));
    }

    #[test]
    fn kleene_logic_short_circuits_before_errors_and_unknown_fns() {
        let div_zero = fn_expr("divide", vec![Value::integer(1), Value::integer(0)]);
        let unknown = fn_expr("unknown_fn", vec![]);

        assert_eq!(
            eval_fn("and", vec![Value::boolean(false), div_zero.clone()]).unwrap(),
            Some(Value::boolean(false))
        );
        assert_eq!(
            eval_fn("and", vec![Value::boolean(false), unknown.clone()]).unwrap(),
            Some(Value::boolean(false))
        );
        assert_eq!(
            eval_fn("or", vec![Value::boolean(true), div_zero]).unwrap(),
            Some(Value::boolean(true))
        );
        assert_eq!(
            eval_fn("or", vec![Value::boolean(true), unknown]).unwrap(),
            Some(Value::boolean(true))
        );
        assert_eq!(
            eval_fn("and", vec![Value::null(), Value::boolean(false)]).unwrap(),
            Some(Value::boolean(false))
        );
        assert_eq!(
            eval_fn("and", vec![Value::boolean(true), field_expr("missing")]).unwrap(),
            Some(Value::null())
        );
        assert_eq!(
            eval_fn("or", vec![Value::null(), Value::boolean(true)]).unwrap(),
            Some(Value::boolean(true))
        );
        assert_eq!(
            eval_fn("or", vec![Value::boolean(false), field_expr("missing")]).unwrap(),
            Some(Value::null())
        );
        assert_eq!(
            eval_fn("not", vec![field_expr("missing")]).unwrap(),
            Some(Value::null())
        );
        assert_eq!(
            eval_fn("not", vec![Value::null()]).unwrap(),
            Some(Value::null())
        );

        assert_eq!(
            eval_fn("and", vec![Value::boolean(true), Value::integer(1)]).unwrap_err(),
            GenericDatabaseError::invalid_argument("Logic argument 2 must be a boolean")
        );
        assert_eq!(
            eval_fn("not", vec![Value::integer(1)]).unwrap_err(),
            GenericDatabaseError::invalid_argument("First logic argument must be a boolean")
        );
    }

    #[test]
    fn comparisons_preserve_absent_vs_null_and_type_incompatibility() {
        let missing = field_expr("missing");
        assert_eq!(
            eval_fn("equal", vec![missing.clone(), missing.clone()]).unwrap(),
            Some(Value::boolean(true))
        );
        assert_eq!(
            eval_fn("equal", vec![missing.clone(), Value::null()]).unwrap(),
            Some(Value::boolean(false))
        );
        assert_eq!(
            eval_fn("not_equal", vec![missing.clone(), Value::null()]).unwrap(),
            Some(Value::boolean(true))
        );
        assert_eq!(
            eval_fn("less_than_or_equal", vec![missing.clone(), missing.clone()]).unwrap(),
            Some(Value::boolean(true))
        );
        assert_eq!(
            eval_fn(
                "greater_than_or_equal",
                vec![missing.clone(), missing.clone()]
            )
            .unwrap(),
            Some(Value::boolean(true))
        );
        assert_eq!(
            eval_fn("less_than", vec![missing.clone(), missing.clone()]).unwrap(),
            Some(Value::boolean(false))
        );
        assert_eq!(
            eval_fn("greater_than", vec![missing.clone(), missing.clone()]).unwrap(),
            Some(Value::boolean(false))
        );
        assert_eq!(
            eval_fn("less_than", vec![missing.clone(), Value::integer(5)]).unwrap(),
            Some(Value::boolean(false))
        );
        assert_eq!(
            eval_fn("less_than", vec![Value::null(), missing]).unwrap(),
            Some(Value::boolean(false))
        );
        assert_eq!(
            eval_fn("less_than", vec![Value::integer(5), Value::string("a")]).unwrap(),
            Some(Value::boolean(false))
        );
        assert_eq!(
            eval_fn("equal", vec![Value::integer(1), Value::double(1.0)]).unwrap(),
            Some(Value::boolean(true))
        );
    }

    #[test]
    fn array_preserves_nulls_and_zero_arg_variadics_work() {
        assert_eq!(
            eval_fn("array", vec![]).unwrap(),
            Some(Value::array(vec![]))
        );
        assert_eq!(
            eval_fn(
                "array",
                vec![Value::null(), field_expr("missing"), Value::integer(1)],
            )
            .unwrap(),
            Some(Value::array(vec![
                Value::null(),
                Value::null(),
                Value::integer(1),
            ]))
        );
        assert_eq!(
            eval_fn("string_concat", vec![]).unwrap(),
            Some(Value::string(""))
        );
        assert_eq!(
            eval_fn("array_concat", vec![]).unwrap(),
            Some(Value::array(vec![]))
        );
    }

    #[test]
    fn vector_distance_vs_strict_sibling_null_propagation_semantics() {
        let div_zero = fn_expr("divide", vec![Value::integer(1), Value::integer(0)]);
        let unknown = fn_expr("unknown_fn", vec![]);
        let missing = field_expr("missing");

        // Vector distances evaluate both args (propagating arg2 error even when arg1 is
        // null/absent), then short-circuit to NULL without validating the non-null sibling's vector
        // type.
        for fn_name in ["cosine_distance", "euclidean_distance", "dot_product"] {
            assert!(matches!(
                eval_fn(fn_name, vec![Value::null(), div_zero.clone()]),
                Err(GenericDatabaseError::InvalidArgument(_))
            ));
            assert!(matches!(
                eval_fn(fn_name, vec![missing.clone(), unknown.clone()]),
                Err(GenericDatabaseError::NotImplemented(_))
            ));
            assert_eq!(
                eval_fn(fn_name, vec![Value::null(), Value::string("not_a_vector")]).unwrap(),
                Some(Value::null())
            );
            assert_eq!(
                eval_fn(
                    fn_name,
                    vec![Value::string("not_a_vector"), missing.clone()]
                )
                .unwrap(),
                Some(Value::null())
            );
        }
        assert_eq!(
            eval_fn(
                "cosine_distance",
                vec![Value::string("s"), Value::vector([1.0])],
            )
            .unwrap_err(),
            GenericDatabaseError::invalid_argument(
                "The function cosine_distance(...) requires `Vector` but got `STRING`."
            )
        );
        assert_eq!(
            eval_fn(
                "euclidean_distance",
                vec![Value::vector([1.0]), Value::vector([1.0, 2.0])],
            )
            .unwrap_err(),
            GenericDatabaseError::invalid_argument(
                "euclidean_distance(...) arguments must be the same length."
            )
        );

        // Strict-sibling null-propagators still reject wrong sibling types when another arg is
        // null/absent.
        for op in ["add", "subtract", "multiply", "divide", "mod", "pow"] {
            for args in [
                vec![Value::null(), Value::string("s")],
                vec![Value::string("s"), Value::null()],
                vec![missing.clone(), Value::boolean(true)],
            ] {
                assert_eq!(
                    eval_fn(op, args).unwrap_err(),
                    GenericDatabaseError::invalid_argument("Arguments must be numbers")
                );
            }
        }
        for args in [
            vec![Value::null(), Value::integer(1)],
            vec![missing.clone(), Value::string("s")],
            vec![
                Value::array(vec![Value::integer(1)]),
                Value::null(),
                Value::string("s"),
            ],
        ] {
            assert_eq!(
                eval_fn("array_concat", args).unwrap_err(),
                GenericDatabaseError::invalid_argument("array_concat requires an array argument")
            );
        }

        // Expression evaluation errors in later args beat sibling type errors in variadics too.
        assert!(matches!(
            eval_fn("string_concat", vec![Value::integer(123), unknown.clone()]),
            Err(GenericDatabaseError::NotImplemented(_))
        ));
        assert!(matches!(
            eval_fn("array_concat", vec![Value::integer(123), unknown]),
            Err(GenericDatabaseError::NotImplemented(_))
        ));

        // Unary null-propagators return NULL on missing/null and exact vector diagnostic otherwise.
        for fn_name in [
            "vector_length",
            "not",
            "to_upper",
            "to_lower",
            "char_length",
            "abs",
            "ceil",
            "floor",
            "round",
            "sqrt",
            "log10",
            "ln",
            "array_length",
        ] {
            assert_eq!(
                eval_fn(fn_name, vec![missing.clone()]).unwrap(),
                Some(Value::null())
            );
            assert_eq!(
                eval_fn(fn_name, vec![Value::null()]).unwrap(),
                Some(Value::null())
            );
        }
        assert_eq!(
            eval_fn("vector_length", vec![Value::string("s")]).unwrap_err(),
            GenericDatabaseError::invalid_argument(
                "The function vector_length(...) requires `Vector` but got `STRING`."
            )
        );
        assert_eq!(
            eval_fn("split", vec![Value::string(""), Value::string("")]).unwrap(),
            Some(Value::array(vec![Value::string("")]))
        );
        assert_eq!(
            eval_fn("split", vec![Value::string("ab"), Value::string("")]).unwrap(),
            Some(Value::array(vec![Value::string("a"), Value::string("b")]))
        );
    }
}
