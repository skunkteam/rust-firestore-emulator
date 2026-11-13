use std::collections::HashMap;

use googleapis::google::{
    firestore::v1::{
        Cursor, Document, DocumentMask, DocumentTransform, Function, Pipeline, Precondition,
        StructuredPipeline, StructuredQuery, Value, Write, WriteResult,
        document_transform::{FieldTransform, field_transform::TransformType},
        pipeline::Stage,
        precondition::ConditionType,
        structured_aggregation_query::{Aggregation, aggregation},
        structured_query::{
            CollectionSelector, CompositeFilter, Direction, FieldFilter, FieldReference, Filter,
            Order, Projection, UnaryFilter, composite_filter, field_filter, filter::FilterType,
            unary_filter,
        },
        value::ValueType,
        write,
    },
    protobuf::{Int32Value, Int64Value, Timestamp},
};
use tonic::Status;

use super::{
    client::TestDb,
    env::DOCUMENTS_ROOT,
    stream::{AggregationOutput, BatchGetOutput, PipelineOutput, QueryStreamOutput},
};

/// Expands a relative document or collection path (`"users/alice"`) into a full resource name
/// under `projects/test-project/databases/(default)/documents/...`. Passes already-qualified
/// `projects/...` resource names through unchanged.
pub(crate) fn doc_path(rel_or_full: &str) -> String {
    if rel_or_full.starts_with("projects/") {
        rel_or_full.to_string()
    } else {
        let trimmed = rel_or_full.trim_start_matches('/');
        if trimmed.is_empty() {
            DOCUMENTS_ROOT.to_string()
        } else {
            format!("{DOCUMENTS_ROOT}/{trimmed}")
        }
    }
}

/// Splits a relative document path (`"users/alice"` or `"users/alice/posts/p1"`) into
/// `(parent_resource_name, collection_id, document_id)` for `CreateDocumentRequest`.
pub(crate) fn split_doc_parent_collection_id(rel_doc: &str) -> (String, String, String) {
    let trimmed = rel_doc.trim_matches('/');
    let parts: Vec<&str> = trimmed.split('/').collect();
    assert!(
        parts.len() >= 2 && parts.len().is_multiple_of(2),
        "expected relative document path with even segments, got {rel_doc:?}"
    );
    let doc_id = parts[parts.len() - 1].to_string();
    let col_id = parts[parts.len() - 2].to_string();
    let parent = if parts.len() == 2 {
        DOCUMENTS_ROOT.to_string()
    } else {
        format!("{DOCUMENTS_ROOT}/{}", parts[..parts.len() - 2].join("/"))
    };
    (parent, col_id, doc_id)
}

/// Splits a relative collection path (`"users"` or `"users/alice/posts"`) into
/// `(parent_resource_name, collection_id)` for `ListDocuments`, `RunQuery`, and `Listen`.
pub(crate) fn split_parent_and_collection(rel_col: &str) -> (String, String) {
    let trimmed = rel_col.trim_matches('/');
    let parts: Vec<&str> = trimmed.split('/').collect();
    assert!(
        !trimmed.is_empty() && parts.len() % 2 == 1,
        "expected relative collection path with odd segments, got {rel_col:?}"
    );
    let col_id = parts[parts.len() - 1].to_string();
    let parent = if parts.len() == 1 {
        DOCUMENTS_ROOT.to_string()
    } else {
        format!("{DOCUMENTS_ROOT}/{}", parts[..parts.len() - 1].join("/"))
    };
    (parent, col_id)
}

pub(crate) fn doc_mask<I, S>(fields: I) -> DocumentMask
where
    I: IntoIterator<Item = S>,
    S: Into<String>,
{
    DocumentMask {
        field_paths: fields.into_iter().map(Into::into).collect(),
    }
}

pub(crate) fn pre_exists(exists: bool) -> Precondition {
    Precondition {
        condition_type: Some(ConditionType::Exists(exists)),
    }
}

pub(crate) fn pre_update_time(ts: Timestamp) -> Precondition {
    Precondition {
        condition_type: Some(ConditionType::UpdateTime(ts)),
    }
}

pub(crate) fn field_filter(
    field: &str,
    op: field_filter::Operator,
    value: impl Into<Value>,
) -> Filter {
    Filter {
        filter_type: Some(FilterType::FieldFilter(FieldFilter {
            field: Some(FieldReference {
                field_path: field.to_string(),
            }),
            op:    op as i32,
            value: Some(value.into()),
        })),
    }
}

pub(crate) fn unary_filter(field: &str, op: unary_filter::Operator) -> Filter {
    Filter {
        filter_type: Some(FilterType::UnaryFilter(UnaryFilter {
            op: op as i32,
            operand_type: Some(unary_filter::OperandType::Field(FieldReference {
                field_path: field.to_string(),
            })),
        })),
    }
}

pub(crate) fn and_filters(filters: Vec<Filter>) -> Filter {
    Filter {
        filter_type: Some(FilterType::CompositeFilter(CompositeFilter {
            op: composite_filter::Operator::And as i32,
            filters,
        })),
    }
}

pub(crate) fn or_filters(filters: Vec<Filter>) -> Filter {
    Filter {
        filter_type: Some(FilterType::CompositeFilter(CompositeFilter {
            op: composite_filter::Operator::Or as i32,
            filters,
        })),
    }
}

/// Typed `StructuredQuery` builder lowering relative collection paths to Protobuf messages.
#[derive(Clone, Debug)]
pub(crate) struct QuerySpec {
    parent: String,
    query:  StructuredQuery,
}

impl QuerySpec {
    pub(crate) fn collection(rel_col: &str) -> Self {
        let (parent, collection_id) = split_parent_and_collection(rel_col);
        Self {
            parent,
            query: StructuredQuery {
                from: vec![CollectionSelector {
                    collection_id,
                    all_descendants: false,
                }],
                ..Default::default()
            },
        }
    }

    pub(crate) fn collection_group(collection_id: &str) -> Self {
        Self {
            parent: DOCUMENTS_ROOT.to_string(),
            query:  StructuredQuery {
                from: vec![CollectionSelector {
                    collection_id:   collection_id.to_string(),
                    all_descendants: true,
                }],
                ..Default::default()
            },
        }
    }

    pub(crate) fn where_filter(mut self, filter: Filter) -> Self {
        self.query.r#where = Some(filter);
        self
    }

    pub(crate) fn order_asc(mut self, field: &str) -> Self {
        self.query.order_by.push(Order {
            field:     Some(FieldReference {
                field_path: field.to_string(),
            }),
            direction: Direction::Ascending as i32,
        });
        self
    }

    pub(crate) fn order_desc(mut self, field: &str) -> Self {
        self.query.order_by.push(Order {
            field:     Some(FieldReference {
                field_path: field.to_string(),
            }),
            direction: Direction::Descending as i32,
        });
        self
    }

    pub(crate) fn select<I, S>(mut self, fields: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.query.select = Some(Projection {
            fields: fields
                .into_iter()
                .map(|f| FieldReference {
                    field_path: f.into(),
                })
                .collect(),
        });
        self
    }

    pub(crate) fn offset(mut self, offset: i32) -> Self {
        self.query.offset = offset;
        self
    }

    pub(crate) fn limit(mut self, limit: i32) -> Self {
        self.query.limit = Some(Int32Value { value: limit });
        self
    }

    pub(crate) fn start_at(mut self, values: Vec<Value>, before: bool) -> Self {
        self.query.start_at = Some(Cursor { values, before });
        self
    }

    pub(crate) fn end_at(mut self, values: Vec<Value>, before: bool) -> Self {
        self.query.end_at = Some(Cursor { values, before });
        self
    }

    pub(crate) fn into_parts(self) -> (String, StructuredQuery) {
        (self.parent, self.query)
    }
}

/// Typed builder for `RunAggregationQuery` aggregations (`Count`, `Sum`, `Avg`).
#[derive(Clone, Debug, Default)]
pub(crate) struct AggregationSpec {
    aggregations: Vec<Aggregation>,
}

impl AggregationSpec {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn count(mut self, alias: &str) -> Self {
        self.aggregations.push(Aggregation {
            alias:    alias.to_string(),
            operator: Some(aggregation::Operator::Count(aggregation::Count {
                up_to: None,
            })),
        });
        self
    }

    pub(crate) fn count_up_to(mut self, alias: &str, up_to: i64) -> Self {
        self.aggregations.push(Aggregation {
            alias:    alias.to_string(),
            operator: Some(aggregation::Operator::Count(aggregation::Count {
                up_to: Some(Int64Value { value: up_to }),
            })),
        });
        self
    }

    pub(crate) fn sum(mut self, alias: &str, field: &str) -> Self {
        self.aggregations.push(Aggregation {
            alias:    alias.to_string(),
            operator: Some(aggregation::Operator::Sum(aggregation::Sum {
                field: Some(FieldReference {
                    field_path: field.to_string(),
                }),
            })),
        });
        self
    }

    pub(crate) fn avg(mut self, alias: &str, field: &str) -> Self {
        self.aggregations.push(Aggregation {
            alias:    alias.to_string(),
            operator: Some(aggregation::Operator::Avg(aggregation::Avg {
                field: Some(FieldReference {
                    field_path: field.to_string(),
                }),
            })),
        });
        self
    }

    pub(crate) fn into_vec(self) -> Vec<Aggregation> {
        self.aggregations
    }
}

pub(crate) fn field_ref_val(field: &str) -> Value {
    Value {
        value_type: Some(ValueType::FieldReferenceValue(field.to_string())),
    }
}

pub(crate) fn fn_val(name: &str, args: Vec<Value>) -> Value {
    Value {
        value_type: Some(ValueType::FunctionValue(Function {
            name: name.to_string(),
            args,
            options: HashMap::new(),
        })),
    }
}

/// Typed `StructuredPipeline` builder for `ExecutePipeline`.
#[derive(Clone, Debug, Default)]
pub(crate) struct PipelineSpec {
    stages: Vec<Stage>,
}

impl PipelineSpec {
    pub(crate) fn empty() -> Self {
        Self::default()
    }

    pub(crate) fn collection(col_path: &str) -> Self {
        let normalized = format!("/{}", col_path.trim_start_matches('/'));
        Self {
            stages: vec![Stage {
                name:    "collection".to_string(),
                args:    vec![Value::reference(normalized)],
                options: HashMap::new(),
            }],
        }
    }

    pub(crate) fn raw_stage(mut self, name: &str, args: Vec<Value>) -> Self {
        self.stages.push(Stage {
            name: name.to_string(),
            args,
            options: HashMap::new(),
        });
        self
    }

    pub(crate) fn where_expr(self, expr: Value) -> Self {
        self.raw_stage("where", vec![expr])
    }

    pub(crate) fn select(self, fields: HashMap<String, Value>) -> Self {
        self.raw_stage("select", vec![Value::map(fields)])
    }

    pub(crate) fn add_fields(self, fields: HashMap<String, Value>) -> Self {
        self.raw_stage("add_fields", vec![Value::map(fields)])
    }

    pub(crate) fn remove_fields<I, S>(self, fields: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let args = fields
            .into_iter()
            .map(|f| field_ref_val(f.as_ref()))
            .collect();
        self.raw_stage("remove_fields", args)
    }

    pub(crate) fn sort_asc(self, field: &str) -> Self {
        self.raw_stage(
            "sort",
            vec![Value::map(HashMap::from([
                ("expression".to_string(), field_ref_val(field)),
                ("direction".to_string(), Value::string("ascending")),
            ]))],
        )
    }

    pub(crate) fn sort_desc(self, field: &str) -> Self {
        self.raw_stage(
            "sort",
            vec![Value::map(HashMap::from([
                ("expression".to_string(), field_ref_val(field)),
                ("direction".to_string(), Value::string("descending")),
            ]))],
        )
    }

    pub(crate) fn limit(self, n: i64) -> Self {
        self.raw_stage("limit", vec![Value::integer(n)])
    }

    pub(crate) fn offset(self, n: i64) -> Self {
        self.raw_stage("offset", vec![Value::integer(n)])
    }

    pub(crate) fn distinct(self, fields: HashMap<String, Value>) -> Self {
        self.raw_stage("distinct", vec![Value::map(fields)])
    }

    pub(crate) fn aggregate(
        self,
        aggs: HashMap<String, Value>,
        group_by: Option<HashMap<String, Value>>,
    ) -> Self {
        let mut args = vec![Value::map(aggs)];
        if let Some(groups) = group_by {
            args.push(Value::map(groups));
        }
        self.raw_stage("aggregate", args)
    }

    pub(crate) fn into_structured(self) -> StructuredPipeline {
        StructuredPipeline {
            pipeline: Some(Pipeline {
                stages: self.stages,
            }),
            options:  HashMap::new(),
        }
    }
}

pub(crate) fn tf_server_timestamp(field_path: &str) -> FieldTransform {
    FieldTransform {
        field_path:     field_path.to_string(),
        transform_type: Some(TransformType::SetToServerValue(
            googleapis::google::firestore::v1::document_transform::field_transform::ServerValue::RequestTime
                as i32,
        )),
    }
}

pub(crate) fn tf_increment(field_path: &str, val: impl Into<Value>) -> FieldTransform {
    FieldTransform {
        field_path:     field_path.to_string(),
        transform_type: Some(TransformType::Increment(val.into())),
    }
}

pub(crate) fn tf_append_missing(field_path: &str, values: Vec<Value>) -> FieldTransform {
    FieldTransform {
        field_path:     field_path.to_string(),
        transform_type: Some(TransformType::AppendMissingElements(
            googleapis::google::firestore::v1::ArrayValue { values },
        )),
    }
}

pub(crate) fn tf_remove_all(field_path: &str, values: Vec<Value>) -> FieldTransform {
    FieldTransform {
        field_path:     field_path.to_string(),
        transform_type: Some(TransformType::RemoveAllFromArray(
            googleapis::google::firestore::v1::ArrayValue { values },
        )),
    }
}

pub(crate) fn tf_maximum(field_path: &str, val: impl Into<Value>) -> FieldTransform {
    FieldTransform {
        field_path:     field_path.to_string(),
        transform_type: Some(TransformType::Maximum(val.into())),
    }
}

pub(crate) fn tf_minimum(field_path: &str, val: impl Into<Value>) -> FieldTransform {
    FieldTransform {
        field_path:     field_path.to_string(),
        transform_type: Some(TransformType::Minimum(val.into())),
    }
}

/// Typed builder for `Commit` and `BatchWrite` `Write` vectors.
#[derive(Clone, Debug, Default)]
pub(crate) struct WriteBatchSpec {
    writes: Vec<Write>,
}

impl WriteBatchSpec {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn put(mut self, rel_doc: &str, fields: HashMap<String, Value>) -> Self {
        self.writes.push(Write {
            update_mask: None,
            update_transforms: vec![],
            current_document: None,
            operation: Some(write::Operation::Update(Document {
                name: doc_path(rel_doc),
                fields,
                create_time: None,
                update_time: None,
            })),
        });
        self
    }

    pub(crate) fn update_with(
        mut self,
        rel_doc: &str,
        fields: HashMap<String, Value>,
        mask: Option<Vec<&str>>,
        transforms: Vec<FieldTransform>,
        current_document: Option<Precondition>,
    ) -> Self {
        self.writes.push(Write {
            update_mask: mask.map(doc_mask),
            update_transforms: transforms,
            current_document,
            operation: Some(write::Operation::Update(Document {
                name: doc_path(rel_doc),
                fields,
                create_time: None,
                update_time: None,
            })),
        });
        self
    }

    pub(crate) fn delete(mut self, rel_doc: &str, current_document: Option<Precondition>) -> Self {
        self.writes.push(Write {
            update_mask: None,
            update_transforms: vec![],
            current_document,
            operation: Some(write::Operation::Delete(doc_path(rel_doc))),
        });
        self
    }

    pub(crate) fn standalone_transform(
        mut self,
        rel_doc: &str,
        field_transforms: Vec<FieldTransform>,
    ) -> Self {
        self.writes.push(Write {
            update_mask: None,
            update_transforms: vec![],
            current_document: None,
            operation: Some(write::Operation::Transform(DocumentTransform {
                document: doc_path(rel_doc),
                field_transforms,
            })),
        });
        self
    }

    pub(crate) fn raw(mut self, write: Write) -> Self {
        self.writes.push(write);
        self
    }

    pub(crate) fn into_writes(self) -> Vec<Write> {
        self.writes
    }
}

/// Stateful transaction handle buffering writes for `Commit` or `Rollback`.
#[derive(Debug)]
pub(crate) struct TxnClient {
    db:     TestDb,
    txn_id: Vec<u8>,
    writes: Vec<Write>,
}

impl TxnClient {
    pub(crate) fn new(db: TestDb, txn_id: Vec<u8>) -> Self {
        Self {
            db,
            txn_id,
            writes: Vec::new(),
        }
    }

    pub(crate) fn id(&self) -> &[u8] {
        &self.txn_id
    }

    pub(crate) async fn get(&mut self, rel_doc: &str) -> Result<Document, Status> {
        self.db.get_in_txn(rel_doc, &self.txn_id).await
    }

    pub(crate) async fn batch_get<I, S>(&mut self, rel_docs: I) -> Result<BatchGetOutput, Status>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.db.batch_get_in_txn(rel_docs, &self.txn_id).await
    }

    pub(crate) async fn query(&mut self, spec: QuerySpec) -> Result<QueryStreamOutput, Status> {
        self.db.query_in_txn(spec, &self.txn_id).await
    }

    pub(crate) async fn aggregate(
        &mut self,
        spec: QuerySpec,
        aggs: AggregationSpec,
    ) -> Result<AggregationOutput, Status> {
        self.db.aggregate_in_txn(spec, aggs, &self.txn_id).await
    }

    pub(crate) async fn pipeline(&mut self, spec: PipelineSpec) -> Result<PipelineOutput, Status> {
        self.db.pipeline_in_txn(spec, &self.txn_id).await
    }

    pub(crate) fn put(&mut self, rel_doc: &str, fields: HashMap<String, Value>) -> &mut Self {
        self.writes.push(Write {
            update_mask: None,
            update_transforms: vec![],
            current_document: None,
            operation: Some(write::Operation::Update(Document {
                name: doc_path(rel_doc),
                fields,
                create_time: None,
                update_time: None,
            })),
        });
        self
    }

    pub(crate) fn delete(
        &mut self,
        rel_doc: &str,
        current_document: Option<Precondition>,
    ) -> &mut Self {
        self.writes.push(Write {
            update_mask: None,
            update_transforms: vec![],
            current_document,
            operation: Some(write::Operation::Delete(doc_path(rel_doc))),
        });
        self
    }

    pub(crate) fn stage_writes(&mut self, batch: WriteBatchSpec) -> &mut Self {
        self.writes.extend(batch.into_writes());
        self
    }

    pub(crate) async fn commit(mut self) -> Result<(Option<Timestamp>, Vec<WriteResult>), Status> {
        self.db.commit_raw(&self.txn_id, self.writes).await
    }

    pub(crate) async fn rollback(mut self) -> Result<(), Status> {
        self.db.rollback_raw(&self.txn_id).await
    }
}
