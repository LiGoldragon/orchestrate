//! The Orchestrate Nexus ordinary-state ontology.
//!
//! `Lock` is the durable coordination fact. `LockId` is assigned by the
//! Nexus and is the sole release target; `FlowId` attributes the fact but
//! grants no authority. The store owner implements the transitions below and
//! answers in the generated contract's own values, so no transport-specific
//! mapping can become a second contract.

use signal_orchestrate::{
    Lock, LockRequest, Observation, ObserveSelection, Response as OrdinaryResponse,
};

use crate::store::StoreError;

/// Atomically records one complete Lock or returns its typed rejection.
pub trait Locks {
    fn lock(&mut self, request: LockRequest) -> Result<OrdinaryResponse, StoreError>;
}

/// Removes exactly the Lock named by its durable, non-reusable identity.
pub trait Releases {
    fn release(&mut self, lock_id: i64) -> Result<OrdinaryResponse, StoreError>;
}

/// Captures one complete point-in-time ordinary-state observation, which is
/// what a new subscriber receives on open and what every change re-reads.
pub trait Observes {
    fn observe(&self, selection: ObserveSelection) -> Result<Observation, StoreError>;
}

/// The complete durable fact accepted and returned by the Nexus.
pub trait IdentifiesLock {
    fn lock_id(&self) -> &i64;
}

impl IdentifiesLock for Lock {
    fn lock_id(&self) -> &i64 {
        &self.lock_id
    }
}
