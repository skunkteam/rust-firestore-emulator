use std::{collections::BTreeMap, sync::Arc};

use googleapis::google::protobuf::Timestamp;

use super::LockSession;
use crate::{database::reference::DocumentRef, error::CommitError};

/// RAII shared read lock capability on a single [`DocumentRef`].
///
/// While armed, dropping `ReadLease` decrements the active read-lease count for the owning
/// session and releases the document read lock once no retained or active hold remains.
#[derive(Debug)]
pub(crate) struct ReadLease {
    session: Arc<LockSession>,
    doc_ref: DocumentRef,
    armed:   bool,
}

impl ReadLease {
    pub(super) fn new(session: Arc<LockSession>, doc_ref: DocumentRef) -> Self {
        Self {
            session,
            doc_ref,
            armed: true,
        }
    }

    pub(crate) fn doc_ref(&self) -> &DocumentRef {
        &self.doc_ref
    }

    pub(crate) fn check_active(&self) -> Result<(), CommitError> {
        self.session.check_read_active()
    }

    #[cfg(test)]
    pub(crate) fn is_wounded(&self) -> bool {
        self.session.is_wounded()
    }

    pub(super) fn session(&self) -> &Arc<LockSession> {
        &self.session
    }

    pub(super) fn disarm(mut self) {
        self.armed = false;
    }
}

impl Drop for ReadLease {
    fn drop(&mut self) {
        if self.armed {
            self.session.release_read_lease(&self.doc_ref);
        }
    }
}

/// RAII exclusive write lock capability on a single [`DocumentRef`].
#[derive(Debug)]
pub(crate) struct WriteLease {
    session: Arc<LockSession>,
    doc_ref: DocumentRef,
    acquire_time: Timestamp,
    armed: bool,
}

impl WriteLease {
    pub(super) fn new(
        session: Arc<LockSession>,
        doc_ref: DocumentRef,
        acquire_time: Timestamp,
    ) -> Self {
        Self {
            session,
            doc_ref,
            acquire_time,
            armed: true,
        }
    }

    pub(crate) fn doc_ref(&self) -> &DocumentRef {
        &self.doc_ref
    }

    pub(crate) fn acquire_time(&self) -> Timestamp {
        self.acquire_time
    }

    pub(crate) fn check_active(&self) -> Result<(), CommitError> {
        self.session.check_active()
    }

    #[cfg(test)]
    pub(crate) fn is_wounded(&self) -> bool {
        self.session.is_wounded()
    }

    pub(super) fn session(&self) -> &Arc<LockSession> {
        &self.session
    }

    pub(super) fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for WriteLease {
    fn drop(&mut self) {
        if self.armed {
            self.session.release_write_lease(&self.doc_ref);
        }
    }
}

/// Sealed commit capability owning all exclusive target [`WriteLease`]s for a commit apply phase.
///
/// Constructing `CommitSeal` transitions the owning [`LockSession`] to non-woundable `Committing`
/// and releases all un-upgraded retained read holds; dropping `CommitSeal` disarms individual
/// [`WriteLease`]s and releases all remaining session locks atomically in one critical section.
#[derive(Debug)]
pub(crate) struct CommitSeal {
    session: Arc<LockSession>,
    writes:  BTreeMap<DocumentRef, WriteLease>,
}

impl CommitSeal {
    pub(super) fn new(
        session: Arc<LockSession>,
        writes: BTreeMap<DocumentRef, WriteLease>,
    ) -> Self {
        Self { session, writes }
    }

    pub(crate) fn len(&self) -> usize {
        self.writes.len()
    }

    #[cfg(test)]
    pub(crate) fn is_empty(&self) -> bool {
        self.writes.is_empty()
    }

    pub(crate) fn get(&self, doc: &DocumentRef) -> Option<&WriteLease> {
        self.writes.get(doc)
    }

    pub(crate) fn doc_refs(&self) -> impl Iterator<Item = &DocumentRef> + '_ {
        self.writes.keys()
    }

    pub(crate) fn check_committing_active(&self) -> Result<(), CommitError> {
        self.session.check_committing_active()
    }
}

impl Drop for CommitSeal {
    fn drop(&mut self) {
        for lease in self.writes.values_mut() {
            lease.disarm();
        }
        self.session.close();
    }
}

/// RAII cancellation guard for a parked or recently-promoted waiter across `.await`.
#[derive(Debug)]
pub(super) struct WaiterGuard {
    session: Arc<LockSession>,
    wait_id: u64,
    armed:   bool,
}

impl WaiterGuard {
    pub(super) fn new(session: Arc<LockSession>, wait_id: u64) -> Self {
        Self {
            session,
            wait_id,
            armed: true,
        }
    }

    pub(super) fn wait_id(&self) -> u64 {
        self.wait_id
    }

    pub(super) fn disarm(mut self) {
        self.armed = false;
    }
}

impl Drop for WaiterGuard {
    fn drop(&mut self) {
        if self.armed {
            self.session.revoke_waiter(self.wait_id);
        }
    }
}
