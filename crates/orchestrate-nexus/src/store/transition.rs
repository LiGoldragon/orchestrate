//! The durable transitions of the Orchestrate Nexus.

use nexus::Configurable;
use sema_engine::{QueryPlan, RecordKey, Retraction};
use signal_orchestrate::{
    ConfigurationReceipt, ConfigurationRejection, ConfigurationRejectionReason, Lock, LockOverlap,
    LockRejection, LockRequest, Observation, ObserveSelection, OrchestrateNexusConfiguration,
    ReleaseRejection, Response as OrdinaryResponse,
};

use crate::{
    configuration::{ConfigurationOutcome, Configured, Validates},
    ordinary::{Locks, Observes, Releases},
    store::{
        KeepsMetadata, OrchestrateStore, StoreError,
        normalize::{NormalizedLockRequest, NormalizesLockRequests},
        record::{StoredAllocator, StoredConfiguration, StoredLock, Storing},
    },
};

/// Reads every Lock the store currently holds, in the order Observe declares.
trait ReadsCurrentLocks {
    fn current_locks(&self) -> Result<Vec<Lock>, StoreError>;
}

impl ReadsCurrentLocks for OrchestrateStore {
    fn current_locks(&self) -> Result<Vec<Lock>, StoreError> {
        let mut locks: Vec<_> = self
            .engine
            .match_records(QueryPlan::all(self.locks))?
            .records()
            .iter()
            .map(|stored| stored.clone().into_public())
            .collect();
        locks.sort_by(|left, right| {
            left.lock_name
                .cmp(&right.lock_name)
                .then_with(|| left.lock_id.cmp(&right.lock_id))
        });
        Ok(locks)
    }
}

impl Locks for OrchestrateStore {
    fn lock(&mut self, request: LockRequest) -> Result<OrdinaryResponse, StoreError> {
        let request = NormalizedLockRequest::from_request(request)?;
        for holder in self.current_locks()? {
            if request.duplicates_name_of(&holder) {
                return Ok(OrdinaryResponse::LockRejected(
                    LockRejection::DuplicateName(holder),
                ));
            }
            if let Some(path) = request.overlapping_path_of(&holder) {
                return Ok(OrdinaryResponse::LockRejected(LockRejection::PathOverlap(
                    LockOverlap {
                        lock_path: path,
                        lock: holder,
                    },
                )));
            }
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
            .next_lock_id
            .checked_add(1)
            .ok_or(StoreError::LockIdExhausted)?;
        let lock = request.into_lock(allocator.next_lock_id);
        self.engine.commit_atomic(
            self.engine
                .begin_atomic_commit()
                .assert(self.locks, StoredLock::from_public(&lock))
                .mutate(self.allocator, StoredAllocator { next_lock_id }),
        )?;
        Ok(OrdinaryResponse::Locked(lock))
    }
}

impl Releases for OrchestrateStore {
    fn release(&mut self, lock_id: i64) -> Result<OrdinaryResponse, StoreError> {
        let key = RecordKey::new(lock_id.to_string());
        let stored = match self
            .engine
            .match_records(QueryPlan::key(self.locks, key.clone()))?
            .records()
        {
            [] => {
                return Ok(OrdinaryResponse::ReleaseRejected(
                    ReleaseRejection::UnknownLockId,
                ));
            }
            [row] => row.clone(),
            _ => unreachable!("Lock IDs are keys"),
        };
        self.engine.retract(Retraction::new(self.locks, key))?;
        Ok(OrdinaryResponse::Released(stored.into_public()))
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
/// here writes it before it answers.
pub trait Configures {
    fn receipt(&self) -> ConfigurationReceipt;
    fn ordinary_configure(
        &mut self,
        configuration: OrchestrateNexusConfiguration,
    ) -> Result<OrdinaryResponse, StoreError>;
    fn meta_configure(
        &mut self,
        configuration: OrchestrateNexusConfiguration,
    ) -> Result<ConfigurationOutcome, StoreError>;
    fn reverse_meta_configuration(&mut self) -> Result<ConfigurationOutcome, StoreError>;
}

impl Configures for OrchestrateStore {
    fn receipt(&self) -> ConfigurationReceipt {
        ConfigurationReceipt {
            orchestrate_nexus_configuration: self
                .state()
                .desired_configuration()
                .clone()
                .into_public(),
            meta_configure_done: self.state().meta_configure_occurred(),
        }
    }

    fn ordinary_configure(
        &mut self,
        configuration: OrchestrateNexusConfiguration,
    ) -> Result<OrdinaryResponse, StoreError> {
        if let Err(reason) = configuration.validated() {
            return Ok(OrdinaryResponse::ConfigurationRefused(
                ConfigurationRejection {
                    configuration_rejection_reason: reason,
                },
            ));
        }
        let stored = StoredConfiguration::from_public(&configuration);
        if self
            .state_mut()
            .ordinary_configure_if_unset(stored)
            .is_err()
        {
            return Ok(OrdinaryResponse::ConfigurationRefused(
                ConfigurationRejection {
                    configuration_rejection_reason:
                        ConfigurationRejectionReason::MetaConfigureOccurred,
                },
            ));
        }
        self.persist_state()?;
        Ok(OrdinaryResponse::ConfigurationAccepted(self.receipt()))
    }

    fn meta_configure(
        &mut self,
        configuration: OrchestrateNexusConfiguration,
    ) -> Result<ConfigurationOutcome, StoreError> {
        if configuration.validated().is_err() {
            return Ok(ConfigurationOutcome::Invalid);
        }
        let stored = StoredConfiguration::from_public(&configuration);
        self.state_mut().meta_configure(stored);
        self.persist_state()?;
        Ok(ConfigurationOutcome::Configured(Configured::Closed(
            self.receipt(),
        )))
    }

    fn reverse_meta_configuration(&mut self) -> Result<ConfigurationOutcome, StoreError> {
        self.state_mut().meta_reverse();
        self.persist_state()?;
        Ok(ConfigurationOutcome::Configured(Configured::Reopened(
            self.receipt(),
        )))
    }
}
