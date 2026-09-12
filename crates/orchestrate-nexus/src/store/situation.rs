//! Where this Nexus is actually bound, as its own store records it.
//!
//! The metadata tree says where the Nexus intends to listen; this says where
//! it did, and which file it did it from. The record is written once both
//! sockets are up and is never read as configuration — only compared, on the
//! next open, against the store actually opened. That comparison is the whole
//! defence against a copied store binding the sockets of the Nexus it was
//! copied from, and it lives in `OrchestrateStore::open` because a store must
//! not hand out socket paths it has no right to before anyone can ask.
//!
//! Writing it is also what completes a move. A declared relocation is an
//! instruction to accept one open at one address; the record written by that
//! open is the address being accepted. Both happen in one commit, so a store
//! can never come to rest with the old address recorded and the instruction
//! still standing, and an instruction can never outlive the move it named.

use nexus::{Situating, Situation};
use sema_engine::{QueryPlan, RecordKey};

use crate::store::{
    OrchestrateStore, StoreError,
    record::{RELOCATION_KEY, StoredSituation},
    relocation::Relocates,
};

/// A store knows where it was last bound, and is told once it is bound again.
pub trait Situates {
    fn situation(&self) -> Result<Option<Situation>, StoreError>;
    fn situate(&mut self, bound_socket_vector: Vec<String>) -> Result<(), StoreError>;
}

impl Situates for OrchestrateStore {
    fn situation(&self) -> Result<Option<Situation>, StoreError> {
        match self
            .engine
            .match_records(QueryPlan::all(self.situation))?
            .records()
        {
            [] => Ok(None),
            [recorded] => Ok(Some(recorded.situation.clone())),
            rows => Err(StoreError::SituationInvariant { count: rows.len() }),
        }
    }

    /// The store is the one this store was opened as, not one the caller
    /// supplies: the record's only job is to say which file was where, and a
    /// caller that could name it could name it wrongly.
    fn situate(&mut self, bound_socket_vector: Vec<String>) -> Result<(), StoreError> {
        let record = StoredSituation {
            situation: Situation::situated(self.store.clone(), bound_socket_vector),
        };
        // The first bind of a store's life asserts the row; every later one
        // replaces it. There is exactly one, keyed by a constant: the
        // question it answers has one answer.
        let mut commit = self.engine.begin_atomic_commit();
        commit = match self.situation()? {
            None => commit.assert(self.situation, record),
            Some(_) => commit.mutate(self.situation, record),
        };
        // A declaration is spent by the move it declared, in the commit that
        // completes it. What is left standing after this is nothing, so a
        // later copy of this store finds no licence in it.
        if self.relocation()?.is_some() {
            commit = commit.retract(self.relocation, RecordKey::new(RELOCATION_KEY));
        }
        self.engine.commit_atomic(commit)?;
        Ok(())
    }
}
