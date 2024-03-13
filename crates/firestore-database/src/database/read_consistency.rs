use googleapis::google::{
    firestore::v1::{
        batch_get_documents_request, get_document_request, list_documents_request,
        run_aggregation_query_request, run_query_request,
    },
    protobuf::Timestamp,
};

use crate::{database::transaction::TransactionId, error::GenericDatabaseError};

#[derive(Clone, Debug)]
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

macro_rules! impl_try_from_consistency_selector {
    ($lib:ident) => {
        impl TryFrom<Option<$lib::ConsistencySelector>> for ReadConsistency {
            type Error = GenericDatabaseError;

            fn try_from(value: Option<$lib::ConsistencySelector>) -> Result<Self, Self::Error> {
                let result = match value {
                    None => ReadConsistency::Default,
                    Some($lib::ConsistencySelector::ReadTime(time)) => {
                        ReadConsistency::ReadTime(time)
                    }
                    Some($lib::ConsistencySelector::Transaction(id)) => {
                        ReadConsistency::Transaction(id.try_into()?)
                    }
                    #[allow(unreachable_patterns)]
                    _ => {
                        return Err(GenericDatabaseError::internal(concat!(
                            stringify!($lib),
                            "::ConsistencySelector::NewTransaction should be handled by caller"
                        )));
                    }
                };
                Ok(result)
            }
        }
    };
}

impl_try_from_consistency_selector!(batch_get_documents_request);
impl_try_from_consistency_selector!(get_document_request);
impl_try_from_consistency_selector!(list_documents_request);
impl_try_from_consistency_selector!(run_query_request);
impl_try_from_consistency_selector!(run_aggregation_query_request);
