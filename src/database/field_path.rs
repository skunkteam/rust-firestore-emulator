use crate::googleapis::google::firestore::v1::{value::ValueType, *};
use std::collections::HashMap;
use std::mem::take;
use tonic::{Result, Status};

pub struct FieldPath(Vec<String>);

impl FieldPath {
    pub fn new(path: &str) -> Result<Self> {
        if path.is_empty() {
            return Err(Status::invalid_argument("invalid empty field path"));
        }
        // Poor man's parser for now.
        let mut elements = vec![];
        let mut iter = path.chars();
        let mut next = String::new();
        let mut inside_backticks = false;
        while let Some(ch) = iter.next() {
            match ch {
                '.' if !inside_backticks => {
                    elements.push(take(&mut next));
                }
                '`' if inside_backticks => {
                    inside_backticks = false;
                    if !matches!(iter.next(), Some('.')) {
                        return Err(Status::invalid_argument(format!(
                            "invalid field path: {path}"
                        )));
                    }
                    elements.push(take(&mut next));
                }
                '`' => {
                    inside_backticks = true;
                }
                '\\' if inside_backticks => next.push(iter.next().ok_or_else(|| {
                    Status::invalid_argument(format!("invalid field path: {path}"))
                })?),
                ch => next.push(ch),
            }
        }
        if !next.is_empty() {
            elements.push(next);
        }
        Ok(Self(elements))
    }

    pub fn get_value<'a>(&self, fields: &'a HashMap<String, Value>) -> Option<&'a Value> {
        let mut current_value = None;
        let mut current_map = Some(fields);
        for key in &self.0 {
            let Some(map) = current_map else {
                return None;
            };
            current_value = map.get(key);
            current_map = match current_value {
                Some(Value {
                    value_type: Some(ValueType::MapValue(MapValue { fields })),
                }) => Some(fields),
                _ => None,
            }
        }
        current_value
    }

    pub fn set_value(&self, fields: &mut HashMap<String, Value>, new_value: Value) {
        transform_value(&self.0, fields, |_| new_value);
    }

    pub fn delete_value(&self, fields: &mut HashMap<String, Value>) {
        delete_value(&self.0, fields)
    }

    pub fn transform_value<'a>(
        &self,
        fields: &'a mut HashMap<String, Value>,
        transform: impl FnOnce(Option<Value>) -> Value,
    ) -> &'a Value {
        transform_value(&self.0, fields, transform)
    }

    pub fn try_transform_value<'a, E>(
        &self,
        fields: &'a mut HashMap<String, Value>,
        transform: impl FnOnce(Option<Value>) -> Result<Value, E>,
    ) -> Result<&'a Value, E> {
        try_transform_value(&self.0, fields, transform)
    }
}

fn delete_value(path: &[String], fields: &mut HashMap<String, Value>) {
    match path {
        [] => unreachable!(),
        [key] => {
            fields.remove(key);
        }
        [first, path @ ..] => {
            let step = fields.get_mut(first);
            if let Some(Value {
                value_type: Some(ValueType::MapValue(MapValue { fields })),
            }) = step
            {
                delete_value(path, fields);
            }
        }
    }
}

fn transform_value<'a>(
    path: &[String],
    fields: &'a mut HashMap<String, Value>,
    transform: impl FnOnce(Option<Value>) -> Value,
) -> &'a Value {
    match path {
        [] => unreachable!(),
        [key] => {
            let old_value = fields.remove(key);
            fields.insert(key.to_string(), transform(old_value));
            &fields[key]
        }
        [first, path @ ..] => {
            // Ugly, must be a better way....
            let value_mut = fields
                .entry(first.to_string())
                .and_modify(|cur| match cur {
                    Value {
                        value_type: Some(ValueType::MapValue(_)),
                    } => (),
                    _ => {
                        *cur = new_map();
                    }
                })
                .or_insert_with(new_map);
            let fields = if let Value {
                value_type: Some(ValueType::MapValue(MapValue { fields })),
            } = value_mut
            {
                fields
            } else {
                unreachable!()
            };
            transform_value(path, fields, transform)
        }
    }
}

fn try_transform_value<'a, E>(
    path: &[String],
    fields: &'a mut HashMap<String, Value>,
    transform: impl FnOnce(Option<Value>) -> Result<Value, E>,
) -> Result<&'a Value, E> {
    match path {
        [] => unreachable!(),
        [key] => {
            let old_value = fields.remove(key);
            fields.insert(key.to_string(), transform(old_value)?);
            Ok(&fields[key])
        }
        [first, path @ ..] => {
            // Ugly, must be a better way....
            let value_mut = fields
                .entry(first.to_string())
                .and_modify(|cur| match cur {
                    Value {
                        value_type: Some(ValueType::MapValue(_)),
                    } => (),
                    _ => {
                        *cur = new_map();
                    }
                })
                .or_insert_with(new_map);
            let fields = if let Value {
                value_type: Some(ValueType::MapValue(MapValue { fields })),
            } = value_mut
            {
                fields
            } else {
                unreachable!()
            };
            try_transform_value(path, fields, transform)
        }
    }
}

fn new_map() -> Value {
    Value {
        value_type: Some(ValueType::MapValue(MapValue {
            fields: Default::default(),
        })),
    }
}
