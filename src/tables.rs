//! Durable metadata for the path-lock registry.

use std::sync::atomic::{AtomicBool, Ordering};

use datom::{PathLockPathConstructing, PathLockViewing};
use sema_engine::{
    Engine, EngineOpen, EngineRecord, FamilyName, QueryPlan, RecordKey, SchemaHash, SchemaVersion,
    TableDescriptor, TableName, TableReference, VersionedStoreName, VersioningPolicy,
};
use signal_orchestrate::{
    NativePathLock, NativePathLockPath, PathLock, PathLockPath, PathLockRegistered,
    PathLockRegistrationRejected, PathLockRegistrationRejection,
};

use crate::{Error, Result, StoreLocation};

const PATH_LOCK_SCHEMA_VERSION: SchemaVersion = SchemaVersion::new(1);
const PATH_LOCKS: TableName = TableName::new("active_path_locks");

/// The registry stores the binary contract carrier only. Its native Datom
/// conversion owns all textual validation and normalization.
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct StoredPathLock {
    name: String,
    pub lock: PathLock,
}

impl EngineRecord for StoredPathLock {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.name.clone())
    }
}

impl StoredPathLock {
    fn new(lock: PathLock, name: String) -> Self {
        Self { name, lock }
    }

    fn native(&self) -> Result<NativePathLock> {
        Ok(self.lock.clone().try_into()?)
    }
}

/// The isolated, breaking path-lock store family. It deliberately has no
/// relation to historic orchestration rows.
pub struct OrchestrateTables {
    engine: Engine,
    path_locks: TableReference<StoredPathLock>,
    atomic_commit_failure_for_test: AtomicBool,
}

impl OrchestrateTables {
    pub fn open(store: &StoreLocation) -> Result<Self> {
        let mut engine = Engine::open(
            EngineOpen::new(store.as_path(), PATH_LOCK_SCHEMA_VERSION).with_versioning(
                VersioningPolicy::new(VersionedStoreName::new("orchestrate-path-locks")),
            ),
        )?;
        let path_locks = engine.register_table(TableDescriptor::new(
            PATH_LOCKS,
            FamilyName::new("path-lock"),
            SchemaHash::for_label("orchestrate-path-lock-v1"),
        ))?;
        Ok(Self {
            engine,
            path_locks,
            atomic_commit_failure_for_test: AtomicBool::new(false),
        })
    }

    pub fn active_path_locks(&self) -> Result<Vec<StoredPathLock>> {
        Ok(self
            .engine
            .match_records(QueryPlan::all(self.path_locks))?
            .records()
            .to_vec())
    }

    /// Decide all conflicts before constructing the one durable assertion.
    /// Therefore rejection and storage failure leave the registry unchanged.
    pub fn register_path_lock(
        &self,
        lock: PathLock,
    ) -> Result<std::result::Result<PathLockRegistered, PathLockRegistrationRejected>> {
        let requested: NativePathLock = lock.clone().try_into()?;
        for existing in self.active_path_locks()? {
            let holder = existing.native()?;
            if holder.name() == requested.name() {
                return Ok(Err(PathLockRegistrationRejected {
                    requested: lock,
                    reason: PathLockRegistrationRejection::DuplicateActiveName {
                        holder: existing.lock,
                    },
                }));
            }
            if let Some(path) = requested.paths().iter().find(|requested_path| {
                holder
                    .paths()
                    .iter()
                    .any(|held_path| Self::paths_overlap(requested_path, held_path))
            }) {
                return Ok(Err(PathLockRegistrationRejected {
                    requested: lock,
                    reason: PathLockRegistrationRejection::PathOverlap {
                        path: PathLockPath::try_from(NativePathLockPath::try_new(path.clone())?)?,
                        holder: existing.lock,
                    },
                }));
            }
        }

        let stored = StoredPathLock::new(lock.clone(), requested.name().into());
        let commit = self
            .engine
            .begin_atomic_commit()
            .assert(self.path_locks, stored);
        if self
            .atomic_commit_failure_for_test
            .swap(false, Ordering::AcqRel)
        {
            return Err(Error::InjectedAtomicCommitFailure);
        }
        self.engine.commit_atomic(commit)?;
        Ok(Ok(PathLockRegistered { lock }))
    }

    #[doc(hidden)]
    pub fn fail_next_atomic_commit_for_test(&self) {
        self.atomic_commit_failure_for_test
            .store(true, Ordering::Release);
    }

    fn paths_overlap(left: &str, right: &str) -> bool {
        left == right || Self::is_ancestor(left, right) || Self::is_ancestor(right, left)
    }

    fn is_ancestor(ancestor: &str, descendant: &str) -> bool {
        ancestor == "/"
            || descendant
                .strip_prefix(ancestor)
                .is_some_and(|suffix| suffix.starts_with('/'))
    }
}

#[cfg(test)]
mod tests {
    use super::OrchestrateTables;

    #[test]
    fn overlap_respects_path_component_boundaries() {
        assert!(OrchestrateTables::paths_overlap("/a", "/a/b"));
        assert!(OrchestrateTables::paths_overlap("/", "/a"));
        assert!(!OrchestrateTables::paths_overlap("/a", "/ab"));
        assert!(!OrchestrateTables::is_ancestor("/a/b", "/a"));
    }
}
