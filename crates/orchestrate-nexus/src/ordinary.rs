//! The Orchestrate Nexus ordinary-state ontology.
//!
//! `Lock` is the durable coordination fact. `LockId` is assigned by the
//! Nexus and is the sole release target; `FlowId` attributes the fact but
//! grants no authority. The store owner implements the transitions below and
//! answers in the generated contract's own values: a transition yields what
//! its input's output carries, or a refusal of the contract, so no
//! transport-specific mapping can become a second contract. A store that
//! cannot carry a transition out, a commit that failed among them, fails it
//! with `StoreError`, and the core answers that as the contract's own
//! `Refused.Store`.

use signal_orchestrate::{
    Lock, LockId, LockRequest, Observation, ObserveSelection, Output as OrdinaryOutput,
};

use crate::store::StoreError;

/// Atomically records one complete Lock or returns its typed rejection.
pub trait Locks {
    fn lock(&mut self, request: LockRequest) -> Result<Result<Lock, OrdinaryOutput>, StoreError>;
}

/// Removes exactly the Lock named by its durable, non-reusable identity.
pub trait Releases {
    fn release(&mut self, lock_id: LockId) -> Result<Result<Lock, OrdinaryOutput>, StoreError>;
}

/// Captures one complete point-in-time ordinary-state observation, which is
/// what a new subscriber receives on open and what every change re-reads.
pub trait Observes {
    fn observe(&self, selection: ObserveSelection) -> Result<Observation, StoreError>;
}
