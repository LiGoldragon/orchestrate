//! The Orchestrate Nexus ordinary-state ontology.
//!
//! `Lock` is the durable coordination fact.  `LockId` is assigned by the
//! Nexus and is the sole release target; `FlowId` attributes the fact but
//! grants no authority.  The one store owner implements the three domain
//! transitions below, while transport only carries their generated Signal
//! values.

use signal_orchestrate::{
    Lock, LockId, LockRequest, Observation, ObserveSelection, OrchestrateReply,
};

use crate::store::StoreError;

/// Atomically records one complete Lock or returns its typed rejection.
pub trait Locks {
    fn lock(&mut self, request: LockRequest) -> Result<OrchestrateReply, StoreError>;
}

/// Removes exactly the Lock named by its durable, non-reusable identity.
pub trait Releases {
    fn release(&mut self, lock_id: LockId) -> Result<OrchestrateReply, StoreError>;
}

/// Captures one complete point-in-time ordinary-state observation.
pub trait Observes {
    fn observe(&self, selection: ObserveSelection) -> Result<Observation, StoreError>;
}

/// The complete durable fact accepted and returned by the Nexus.
pub trait IdentifiesLock {
    fn lock_id(&self) -> &LockId;
}

impl IdentifiesLock for Lock {
    fn lock_id(&self) -> &LockId {
        &self.lock_id
    }
}
