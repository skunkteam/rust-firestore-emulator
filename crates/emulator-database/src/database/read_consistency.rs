use googleapis::google::{
    firestore::v1::{
        TransactionOptions, batch_get_documents_request, execute_pipeline_request,
        get_document_request, list_documents_request, run_aggregation_query_request,
        run_query_request,
    },
    protobuf::Timestamp,
};

use crate::{database::transaction::TransactionId, error::GenericDatabaseError};

#[derive(Clone, Copy, Debug)]
pub enum ReadConsistency {
    Default,
    ReadTime(Timestamp),
    Transaction(TransactionId),
}

impl ReadConsistency {
    /// Returns `true` if the read consistency is [`Transaction`].
    ///
    /// [`Transaction`]: ReadConsistency::Transaction
    #[must_use]
    pub fn is_transaction(&self) -> bool {
        matches!(self, Self::Transaction(..))
    }
}

/// Decomposed consistency selector for streaming RPCs that may either read under an existing
/// [`ReadConsistency`] or start a new transaction with [`TransactionOptions`].
#[derive(Clone, Debug)]
pub enum StreamingConsistency {
    Existing(ReadConsistency),
    NewTransaction(TransactionOptions),
}

/// Validates a client-supplied `read_time` timestamp against Firestore rules.
pub(crate) fn validate_read_time(time: Timestamp) -> crate::error::Result<Timestamp> {
    if time.seconds < 0 {
        return Err(GenericDatabaseError::invalid_argument(format!(
            "Invalid read_time: seconds ({}) must be non-negative",
            time.seconds
        )));
    }
    if !(0..1_000_000_000).contains(&time.nanos) {
        return Err(GenericDatabaseError::invalid_argument(format!(
            "Invalid read_time: nanos ({}) must be in [0, 999_999_999]",
            time.nanos
        )));
    }
    if time > Timestamp::now() {
        return Err(GenericDatabaseError::invalid_argument(
            "Invalid read_time: read_time cannot be in the future",
        ));
    }
    Ok(time)
}

macro_rules! impl_try_from_read_consistency_selector {
    ($lib:ident) => {
        impl TryFrom<Option<$lib::ConsistencySelector>> for ReadConsistency {
            type Error = GenericDatabaseError;

            fn try_from(value: Option<$lib::ConsistencySelector>) -> Result<Self, Self::Error> {
                let result = match value {
                    None => Self::Default,
                    Some($lib::ConsistencySelector::ReadTime(time)) => {
                        Self::ReadTime(validate_read_time(time)?)
                    }
                    Some($lib::ConsistencySelector::Transaction(id)) => {
                        Self::Transaction(id.try_into()?)
                    }
                };
                Ok(result)
            }
        }
    };
}

macro_rules! impl_try_from_streaming_consistency_selector {
    ($lib:ident) => {
        impl TryFrom<Option<$lib::ConsistencySelector>> for StreamingConsistency {
            type Error = GenericDatabaseError;

            fn try_from(value: Option<$lib::ConsistencySelector>) -> Result<Self, Self::Error> {
                let result = match value {
                    None => Self::Existing(ReadConsistency::Default),
                    Some($lib::ConsistencySelector::ReadTime(time)) => {
                        Self::Existing(ReadConsistency::ReadTime(validate_read_time(time)?))
                    }
                    Some($lib::ConsistencySelector::Transaction(id)) => {
                        Self::Existing(ReadConsistency::Transaction(id.try_into()?))
                    }
                    Some($lib::ConsistencySelector::NewTransaction(opts)) => {
                        Self::NewTransaction(opts)
                    }
                };
                Ok(result)
            }
        }
    };
}

impl_try_from_read_consistency_selector!(get_document_request);
impl_try_from_read_consistency_selector!(list_documents_request);

impl_try_from_streaming_consistency_selector!(batch_get_documents_request);
impl_try_from_streaming_consistency_selector!(run_query_request);
impl_try_from_streaming_consistency_selector!(run_aggregation_query_request);
impl_try_from_streaming_consistency_selector!(execute_pipeline_request);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_read_time_accepts_epoch_and_current_time() {
        assert!(
            validate_read_time(Timestamp {
                seconds: 0,
                nanos:   0,
            })
            .is_ok()
        );
        assert!(validate_read_time(Timestamp::now()).is_ok());
    }

    #[test]
    fn validate_read_time_rejects_negative_invalid_nanos_and_future() {
        let now = Timestamp::now();
        for invalid in [
            Timestamp {
                seconds: -1,
                nanos:   0,
            },
            Timestamp {
                seconds: 1,
                nanos:   -1,
            },
            Timestamp {
                seconds: 1,
                nanos:   1_000_000_000,
            },
            Timestamp {
                seconds: now.seconds + 60,
                nanos:   0,
            },
        ] {
            let err = validate_read_time(invalid).unwrap_err();
            assert!(
                matches!(err, GenericDatabaseError::InvalidArgument(_)),
                "expected InvalidArgument for {invalid:?}, got {err:?}"
            );
        }
    }

    #[test]
    fn try_from_consistency_selector_validates_read_time() {
        let future = Timestamp {
            seconds: Timestamp::now().seconds + 60,
            nanos:   0,
        };
        let err = ReadConsistency::try_from(Some(
            get_document_request::ConsistencySelector::ReadTime(future),
        ))
        .unwrap_err();
        assert!(matches!(err, GenericDatabaseError::InvalidArgument(_)));

        let valid = ReadConsistency::try_from(Some(
            get_document_request::ConsistencySelector::ReadTime(Timestamp {
                seconds: 0,
                nanos:   0,
            }),
        ))
        .unwrap();
        assert!(matches!(
            valid,
            ReadConsistency::ReadTime(Timestamp {
                seconds: 0,
                nanos:   0,
            })
        ));
    }

    #[test]
    fn streaming_consistency_decomposes_all_streaming_selectors() {
        let epoch = Timestamp {
            seconds: 0,
            nanos:   0,
        };
        let opts = TransactionOptions::READ_WRITE;

        assert!(matches!(
            StreamingConsistency::try_from(
                None::<batch_get_documents_request::ConsistencySelector>
            )
            .unwrap(),
            StreamingConsistency::Existing(ReadConsistency::Default)
        ));
        assert!(matches!(
            StreamingConsistency::try_from(Some(
                batch_get_documents_request::ConsistencySelector::NewTransaction(opts.clone())
            ))
            .unwrap(),
            StreamingConsistency::NewTransaction(_)
        ));
        assert!(matches!(
            StreamingConsistency::try_from(Some(run_query_request::ConsistencySelector::ReadTime(
                epoch
            )))
            .unwrap(),
            StreamingConsistency::Existing(ReadConsistency::ReadTime(Timestamp {
                seconds: 0,
                nanos:   0,
            }))
        ));
        assert!(matches!(
            StreamingConsistency::try_from(Some(
                run_aggregation_query_request::ConsistencySelector::NewTransaction(opts.clone())
            ))
            .unwrap(),
            StreamingConsistency::NewTransaction(_)
        ));
        assert!(matches!(
            StreamingConsistency::try_from(Some(
                execute_pipeline_request::ConsistencySelector::NewTransaction(opts)
            ))
            .unwrap(),
            StreamingConsistency::NewTransaction(_)
        ));
        assert!(matches!(
            StreamingConsistency::try_from(Some(
                execute_pipeline_request::ConsistencySelector::Transaction(vec![1, 2, 3])
            ))
            .unwrap_err(),
            GenericDatabaseError::InvalidArgument(_)
        ));
    }
}
