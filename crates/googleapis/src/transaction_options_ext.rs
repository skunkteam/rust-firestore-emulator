use crate::google::firestore::v1::{
    TransactionOptions,
    transaction_options::{Mode, ReadWrite},
};

impl TransactionOptions {
    pub const READ_WRITE: Self = Self {
        mode: Some(Mode::ReadWrite(ReadWrite {
            retry_transaction: vec![],
        })),
    };
}
