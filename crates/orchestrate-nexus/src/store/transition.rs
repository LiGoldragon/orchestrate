//! The durable transitions of the Orchestrate Nexus.

use memory_engine::{QueryPlan, RecordKey, Retraction};
use meta_signal_orchestrate::Output as MetaOutput;
use nexus::Configurable;
use signal_orchestrate::{
    ConfigurationReceipt, ConfigurationRejection, ConfigurationRejectionReason, Lock, LockId,
    LockOverlap, LockPath, LockRejection, LockRequest, MetaConfigureDone, Observation,
    ObserveSelection, OrchestrateNexusConfiguration, Output as OrdinaryOutput, ReleaseRejection,
};

use crate::{
    configuration::Validates,
    ordinary::{Locks, Observes, Releases},
    store::{
        KeepsMetadata, OrchestrateStore, StoreError,
        normalize::{NormalizedLockRequest, NormalizesLockRequests},
        record::{StoredAllocator, StoredLock},
    },
};

/// Reads the Locks a plan selects, in the order Observe declares.
trait ReadsCurrentLocks {
    fn locks_selected_by(&self, plan: QueryPlan<StoredLock>) -> Result<Vec<Lock>, StoreError>;

    /// Every Lock the store currently holds.
    fn current_locks(&self) -> Result<Vec<Lock>, StoreError>;
}

impl ReadsCurrentLocks for OrchestrateStore {
    fn locks_selected_by(&self, plan: QueryPlan<StoredLock>) -> Result<Vec<Lock>, StoreError> {
        let mut locks: Vec<_> = self
            .engine
            .match_records(plan)?
            .records()
            .iter()
            .map(|stored| stored.lock.clone())
            .collect();
        locks.sort_by(|left, right| {
            left.lock_name
                .cmp(&right.lock_name)
                .then_with(|| left.lock_id.cmp(&right.lock_id))
        });
        Ok(locks)
    }

    fn current_locks(&self) -> Result<Vec<Lock>, StoreError> {
        self.locks_selected_by(QueryPlan::all(self.locks))
    }
}

impl Locks for OrchestrateStore {
    fn lock(&mut self, request: LockRequest) -> Result<Result<Lock, OrdinaryOutput>, StoreError> {
        // A request whose own paths are at fault is its rejection, not a
        // store that failed.
        let request = match NormalizedLockRequest::from_request(request) {
            Ok(request) => request,
            Err(rejection) => return Ok(Err(OrdinaryOutput::LockRejected(rejection))),
        };
        // The engine reads only the held Locks this request conflicts with;
        // the first of them in Observe's order is the one the refusal names.
        let conflicting =
            self.locks_selected_by(QueryPlan::filtered(self.locks, request.clone()))?;
        if let Some(holder) = conflicting.into_iter().next() {
            let rejection = if request.duplicates_name_of(&holder) {
                LockRejection::DuplicateName(holder)
            } else if let Some(path) = request.overlapping_path_of(&holder) {
                LockRejection::PathOverlap(LockOverlap {
                    lock_path: LockPath(path),
                    lock: holder,
                })
            } else {
                unreachable!("the conflict predicate admits only a conflicting Lock")
            };
            return Ok(Err(OrdinaryOutput::LockRejected(rejection)));
        }
        let allocator = match self
            .engine
            .match_records(QueryPlan::all(self.allocator))?
            .records()
        {
            [row] => row.clone(),
            rows => return Err(StoreError::LockIdAllocatorInvariant { count: rows.len() }),
        };
        let next_lock_id = allocator
            .lock_id
            .0
            .checked_add(1)
            .map(LockId)
            .ok_or(StoreError::LockIdExhausted)?;
        let lock = request.into_lock(allocator.lock_id);
        self.engine.commit_atomic(
            self.engine
                .begin_atomic_commit()
                .assert(self.locks, StoredLock { lock: lock.clone() })
                .mutate(
                    self.allocator,
                    StoredAllocator {
                        lock_id: next_lock_id,
                    },
                ),
        )?;
        Ok(Ok(lock))
    }
}

impl Releases for OrchestrateStore {
    fn release(&mut self, lock_id: LockId) -> Result<Result<Lock, OrdinaryOutput>, StoreError> {
        let key = RecordKey::new(lock_id.0.to_string());
        let stored = match self
            .engine
            .match_records(QueryPlan::key(self.locks, key.clone()))?
            .records()
        {
            [] => {
                return Ok(Err(OrdinaryOutput::ReleaseRejected(
                    ReleaseRejection::UnknownLockId,
                )));
            }
            [row] => row.clone(),
            _ => unreachable!("Lock IDs are keys"),
        };
        self.engine.retract(Retraction::new(self.locks, key))?;
        Ok(Ok(stored.lock))
    }
}

impl Observes for OrchestrateStore {
    fn observe(&self, selection: ObserveSelection) -> Result<Observation, StoreError> {
        match selection {
            ObserveSelection::Locks => Ok(Observation::Locks(self.current_locks()?)),
        }
    }
}

/// The three configuration transitions: one ordinary, two privileged.
///
/// Ordinary Configure stays open only while the privileged Configure has
/// never been done; the meta surface alone closes it and reopens it. The
/// durable record of that fact is the metadata tree, and every transition
/// here writes it before it answers. Each answers with the receipt its
/// output carries, or its own contract's refusal.
pub trait Configures {
    fn receipt(&self) -> ConfigurationReceipt;
    fn ordinary_configure(
        &mut self,
        configuration: OrchestrateNexusConfiguration,
    ) -> Result<Result<ConfigurationReceipt, OrdinaryOutput>, StoreError>;
    fn meta_configure(
        &mut self,
        configuration: OrchestrateNexusConfiguration,
    ) -> Result<Result<ConfigurationReceipt, MetaOutput>, StoreError>;
    fn reverse_meta_configuration(&mut self) -> Result<ConfigurationReceipt, StoreError>;
}

impl Configures for OrchestrateStore {
    fn receipt(&self) -> ConfigurationReceipt {
        ConfigurationReceipt {
            orchestrate_nexus_configuration: self.state().desired_configuration().clone(),
            meta_configure_done: MetaConfigureDone(self.state().meta_configure_occurred()),
        }
    }

    fn ordinary_configure(
        &mut self,
        configuration: OrchestrateNexusConfiguration,
    ) -> Result<Result<ConfigurationReceipt, OrdinaryOutput>, StoreError> {
        if let Err(reason) = configuration.validated() {
            return Ok(Err(OrdinaryOutput::ConfigurationRefused(
                ConfigurationRejection {
                    configuration_rejection_reason: reason,
                },
            )));
        }
        if self
            .state_mut()
            .ordinary_configure_if_unset(configuration)
            .is_err()
        {
            return Ok(Err(OrdinaryOutput::ConfigurationRefused(
                ConfigurationRejection {
                    configuration_rejection_reason:
                        ConfigurationRejectionReason::MetaConfigureOccurred,
                },
            )));
        }
        self.persist_state()?;
        Ok(Ok(self.receipt()))
    }

    fn meta_configure(
        &mut self,
        configuration: OrchestrateNexusConfiguration,
    ) -> Result<Result<ConfigurationReceipt, MetaOutput>, StoreError> {
        if configuration.validated().is_err() {
            return Ok(Err(MetaOutput::ConfigurationRejected(
                meta_signal_orchestrate::ConfigurationRejection {
                    configuration_rejection_reason:
                        meta_signal_orchestrate::ConfigurationRejectionReason::InvalidConfiguration,
                },
            )));
        }
        self.state_mut().meta_configure(configuration);
        self.persist_state()?;
        Ok(Ok(self.receipt()))
    }

    fn reverse_meta_configuration(&mut self) -> Result<ConfigurationReceipt, StoreError> {
        self.state_mut().meta_reverse();
        self.persist_state()?;
        Ok(self.receipt())
    }
}
