use emulator_database::read_consistency::ReadConsistency;
use googleapis::google::{
    firestore::v1::{DocumentMask, Precondition, precondition},
    protobuf::Timestamp,
};

use crate::error::{RestError, Result};

#[derive(Debug, Default)]
pub(super) struct DocumentQueryParams {
    pub(super) document_id: Option<String>,
    pub(super) page_size: Option<i32>,
    pub(super) page_token: Option<String>,
    pub(super) order_by: Option<String>,
    pub(super) show_missing: Option<bool>,
    mask: Vec<String>,
    has_mask_param: bool,
    update_mask: Vec<String>,
    has_update_mask_param: bool,
    current_document_exists: Option<bool>,
    current_document_update_time: Option<Timestamp>,
    read_time: Option<Timestamp>,
    transaction: Option<String>,
}

impl DocumentQueryParams {
    pub(super) fn parse(raw_query: Option<&str>) -> Result<Self> {
        let Some(query) = raw_query else {
            return Ok(Self::default());
        };
        let mut params = Self::default();
        for (key, val) in parse_query_pairs(query)? {
            match key.as_str() {
                "documentId" | "document_id" => params.document_id = Some(val),
                "pageSize" | "page_size" => {
                    let parsed = val.parse::<i32>().map_err(|_| {
                        RestError::bad_request(format!("Invalid pageSize value: {val}"))
                    })?;
                    params.page_size = Some(parsed);
                }
                "pageToken" | "page_token" => params.page_token = Some(val),
                "orderBy" | "order_by" => params.order_by = Some(val),
                "showMissing" | "show_missing" => {
                    params.show_missing = Some(parse_bool_param("showMissing", &val)?);
                }
                "mask.fieldPaths" | "mask.field_paths" => {
                    params.has_mask_param = true;
                    params.mask.push(val);
                }
                "updateMask.fieldPaths"
                | "updateMask.field_paths"
                | "update_mask.fieldPaths"
                | "update_mask.field_paths" => {
                    params.has_update_mask_param = true;
                    params.update_mask.push(val);
                }
                "currentDocument.exists" | "current_document.exists" => {
                    params.current_document_exists =
                        Some(parse_bool_param("currentDocument.exists", &val)?);
                }
                "currentDocument.updateTime"
                | "currentDocument.update_time"
                | "current_document.updateTime"
                | "current_document.update_time" => {
                    let ts: Timestamp = serde_json::from_value(serde_json::Value::String(val))
                        .map_err(|err| {
                            RestError::bad_request(format!(
                                "Invalid currentDocument.updateTime: {err}"
                            ))
                        })?;
                    params.current_document_update_time = Some(ts);
                }
                "readTime" | "read_time" => {
                    let ts: Timestamp = serde_json::from_value(serde_json::Value::String(val))
                        .map_err(|err| {
                            RestError::bad_request(format!("Invalid readTime: {err}"))
                        })?;
                    params.read_time = Some(ts);
                }
                "transaction" => {
                    if val.is_empty() {
                        return Err(RestError::bad_request("transaction cannot be empty"));
                    }
                    params.transaction = Some(val);
                }
                "requestOptions.requestTags"
                | "requestOptions.request_tags"
                | "request_options.requestTags"
                | "request_options.request_tags" => {
                    return Err(RestError::not_implemented(
                        "request_options is not supported yet",
                    ));
                }
                _ => {}
            }
        }
        Ok(params)
    }

    pub(super) fn mask(&self) -> Option<DocumentMask> {
        self.has_mask_param.then(|| DocumentMask {
            field_paths: if self.mask.iter().all(String::is_empty) {
                Vec::new()
            } else {
                self.mask.clone()
            },
        })
    }

    pub(super) fn update_mask(&self) -> Option<DocumentMask> {
        self.has_update_mask_param.then(|| DocumentMask {
            field_paths: if self.update_mask.iter().all(String::is_empty) {
                Vec::new()
            } else {
                self.update_mask.clone()
            },
        })
    }

    pub(super) fn read_consistency(&self) -> Result<ReadConsistency> {
        match (self.read_time, self.transaction.as_deref()) {
            (Some(_), Some(_)) => Err(RestError::bad_request(
                "Cannot specify both readTime and transaction",
            )),
            (Some(ts), None) => Ok(ReadConsistency::ReadTime(ts)),
            (None, Some(_)) => Err(RestError::not_implemented(
                "transactional reads over REST are not implemented yet",
            )),
            (None, None) => Ok(ReadConsistency::Default),
        }
    }

    pub(super) fn precondition(&self) -> Result<Option<Precondition>> {
        match (
            self.current_document_exists,
            self.current_document_update_time,
        ) {
            (Some(_), Some(_)) => Err(RestError::bad_request(
                "currentDocument cannot specify both exists and updateTime",
            )),
            (Some(exists), None) => Ok(Some(Precondition {
                condition_type: Some(precondition::ConditionType::Exists(exists)),
            })),
            (None, Some(ts)) => Ok(Some(Precondition {
                condition_type: Some(precondition::ConditionType::UpdateTime(ts)),
            })),
            (None, None) => Ok(None),
        }
    }
}

fn parse_bool_param(name: &str, val: &str) -> Result<bool> {
    match val {
        "true" | "1" => Ok(true),
        "false" | "0" => Ok(false),
        _ => Err(RestError::bad_request(format!(
            "Invalid boolean query parameter {name}: {val}"
        ))),
    }
}

pub(in crate::routes) fn decode_raw_path_segments(raw_path: &str) -> Result<String> {
    let mut decoded_segments = Vec::new();
    for raw_seg in raw_path.split('/') {
        let seg = percent_decode_path_segment(raw_seg)?;
        if seg.contains('/') {
            return Err(RestError::bad_request("Path segment cannot contain '/'"));
        }
        decoded_segments.push(seg);
    }
    Ok(decoded_segments.join("/"))
}

pub(in crate::routes) fn parse_query_pairs(raw_query: &str) -> Result<Vec<(String, String)>> {
    let mut pairs = Vec::new();
    for pair in raw_query.split('&') {
        if pair.is_empty() {
            continue;
        }
        let (raw_k, raw_v) = pair.split_once('=').unwrap_or((pair, ""));
        let key = percent_decode(raw_k)?;
        let val = percent_decode(raw_v)?;
        pairs.push((key, val));
    }
    Ok(pairs)
}

pub(in crate::routes) fn percent_decode(input: &str) -> Result<String> {
    percent_decode_inner(input, true)
}

pub(super) fn percent_decode_path_segment(input: &str) -> Result<String> {
    percent_decode_inner(input, false)
}

fn percent_decode_inner(input: &str, plus_as_space: bool) -> Result<String> {
    let bytes = input.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'+' if plus_as_space => {
                out.push(b' ');
                i += 1;
            }
            b'%' => {
                if i + 2 >= bytes.len() {
                    return Err(RestError::bad_request(format!(
                        "Incomplete percent-encoding in string: {input}"
                    )));
                }
                let hi = hex_val(bytes[i + 1]).ok_or_else(|| {
                    RestError::bad_request(format!("Invalid percent-encoding in: {input}"))
                })?;
                let lo = hex_val(bytes[i + 2]).ok_or_else(|| {
                    RestError::bad_request(format!("Invalid percent-encoding in: {input}"))
                })?;
                out.push((hi << 4) | lo);
                i += 3;
            }
            b => {
                out.push(b);
                i += 1;
            }
        }
    }
    String::from_utf8(out)
        .map_err(|_| RestError::bad_request("URL component contains invalid UTF-8"))
}

fn hex_val(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}
