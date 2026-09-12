//! Where this Nexus is actually bound, as its own store records it.
//!
//! The metadata tree says where the Nexus intends to listen; this says where
//! it did. The record is written once both sockets are up and is never read
//! as configuration — only compared, on the next open, against the path the
//! store was actually opened at. That comparison is the whole defence
//! against a copied store binding the sockets of the Nexus it was copied
//! from, and it lives in `OrchestrateStore::open` because a store must not
//! hand out socket paths it has no right to before anyone can ask.

use nexus::{Situating, Situation};
use sema_engine::{Assertion, Mutation, QueryPlan};

use crate::store::{OrchestrateStore, StoreError, record::StoredSituation};

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

    /// The store path is the one this store was opened at, not one the caller
    /// supplies: the record's only job is to say where the store was, and a
    /// caller that could name it could name it wrongly.
    fn situate(&mut self, bound_socket_vector: Vec<String>) -> Result<(), StoreError> {
        let record = StoredSituation {
            situation: Situation::situated(self.store_path.clone(), bound_socket_vector),
        };
        let table = self.situation;
        // The first bind of a store's life asserts the row; every later one
        // replaces it. There is exactly one, keyed by a constant: the
        // question it answers has one answer.
        match self.situation()? {
            None => {
                self.engine.assert(Assertion::new(table, record))?;
            }
            Some(_) => {
                self.engine.mutate(Mutation::new(table, record))?;
            }
        }
        Ok(())
    }
}
