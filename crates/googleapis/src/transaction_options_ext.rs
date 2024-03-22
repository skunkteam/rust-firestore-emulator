use crate::google::firestore::v1::{
    transaction_options::{Mode, ReadWrite},
    TransactionOptions,
};

impl TransactionOptions {
    pub const READ_WRITE: Self = Self {
        mode: Some(Mode::ReadWrite(ReadWrite {
            retry_transaction: vec![],
        })),
    };
}
