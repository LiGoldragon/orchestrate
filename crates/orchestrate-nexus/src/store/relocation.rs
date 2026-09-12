//! The declaration a store carries that it was moved here.
//!
//! Reading it is all the Nexus does. Writing it is `crate::recovery`'s, which
//! runs against the store file with no Nexus at all — the only place it can
//! run, because the refusal it answers happens before a socket is bound and
//! the meta socket a client would ask through is named by the store whose
//! right to it is exactly what is in doubt.
//!
//! Spending it is `Situates::situate`'s, in the commit that records the new
//! address.

use nexus::Relocation;
use sema_engine::QueryPlan;

use crate::store::{OrchestrateStore, StoreError};

/// A store knows whether a move has been declared for it and not yet
/// completed.
pub trait Relocates {
    fn relocation(&self) -> Result<Option<Relocation>, StoreError>;
}

impl Relocates for OrchestrateStore {
    fn relocation(&self) -> Result<Option<Relocation>, StoreError> {
        match self
            .engine
            .match_records(QueryPlan::all(self.relocation))?
            .records()
        {
            [] => Ok(None),
            [declared] => Ok(Some(declared.relocation.clone())),
            rows => Err(StoreError::RelocationInvariant { count: rows.len() }),
        }
    }
}
