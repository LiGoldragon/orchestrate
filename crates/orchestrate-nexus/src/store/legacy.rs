//! The pre-0.25 PathLock family, read only to refuse a store that still holds
//! one.
//!
//! This is a guard, never an importer: no row here is ever materialized as a
//! Lock, because a PathLock has no Flow and inventing one would attribute a
//! coordination fact to a flow that never claimed it. `orchestrate-upgrade-
//! preflight` reports the count without opening any other family, and the
//! Nexus refuses to start while the count is nonzero.

use std::path::Path;

use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use sema_engine::{
    Engine, EngineOpen, EngineRecord, FamilyName, QueryPlan, RecordKey, SchemaHash,
    TableDescriptor, TableName, TableReference,
};

use super::{error::StoreError, record::SCHEMA_VERSION};

const LEGACY_TABLE: TableName = TableName::new("active_path_locks");

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Clone)]
struct LegacyName(String);
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Clone)]
struct LegacyPath(String);
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Clone)]
struct LegacyPaths(Vec<LegacyPath>);
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Clone)]
struct LegacyReason(String);
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Clone)]
struct LegacyLock {
    path_lock_name: LegacyName,
    path_lock_paths: LegacyPaths,
    path_lock_description: LegacyReason,
}
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Clone)]
struct LegacyStoredLock {
    name: String,
    lock: LegacyLock,
}

impl EngineRecord for LegacyStoredLock {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.name.clone())
    }
}

/// Read-only evidence about the pre-0.25 ordinary-state table.
pub struct LegacyStorePreflight {
    active_lock_count: usize,
}

/// Inspects a store for active PathLock rows.
pub trait PreflightsLegacyStore: Sized {
    fn inspect(store_path: &Path) -> Result<Self, StoreError>;
    fn active_lock_count(&self) -> usize;
}

impl PreflightsLegacyStore for LegacyStorePreflight {
    fn inspect(store_path: &Path) -> Result<Self, StoreError> {
        if !store_path.exists() {
            return Ok(Self {
                active_lock_count: 0,
            });
        }
        let mut engine = Engine::open(EngineOpen::new(
            store_path.display().to_string(),
            SCHEMA_VERSION,
        ))?;
        Ok(Self {
            active_lock_count: engine.count_active_path_locks()?,
        })
    }

    fn active_lock_count(&self) -> usize {
        self.active_lock_count
    }
}

/// An open engine can say how many pre-0.25 PathLock rows it still carries.
pub trait CountsActivePathLocks {
    fn count_active_path_locks(&mut self) -> Result<usize, StoreError>;
}

impl CountsActivePathLocks for Engine {
    fn count_active_path_locks(&mut self) -> Result<usize, StoreError> {
        if !self.catalog().is_registered(&LEGACY_TABLE) {
            // A store that never held the family holds no rows of it, and
            // asking must not add an empty family to its durable catalog.
            return Ok(0);
        }
        let legacy: TableReference<LegacyStoredLock> =
            self.register_table(TableDescriptor::new(
                LEGACY_TABLE,
                FamilyName::new("orchestrate-path-lock"),
                SchemaHash::for_label("orchestrate-path-lock-v1"),
            ))?;
        Ok(self.match_records(QueryPlan::all(legacy))?.records().len())
    }
}

#[cfg(test)]
mod tests {
    use sema_engine::Assertion;
    use signal_orchestrate::OrchestrateNexusConfiguration;

    use super::*;
    use crate::store::{OpensStore, OrchestrateStore};

    fn defaults(directory: &Path) -> OrchestrateNexusConfiguration {
        OrchestrateNexusConfiguration {
            ordinary_socket_path: directory.join("ordinary.sock").display().to_string(),
            meta_socket_path: directory.join("meta.sock").display().to_string(),
        }
    }

    #[test]
    fn preflight_does_not_create_a_missing_store() {
        let directory = tempfile::tempdir().expect("temporary preflight directory");
        let store_path = directory.path().join("missing.sema");
        let preflight = <LegacyStorePreflight as PreflightsLegacyStore>::inspect(&store_path)
            .expect("inspect missing store");
        assert_eq!(preflight.active_lock_count(), 0);
        assert!(!store_path.exists(), "read-only preflight creates no store");
    }

    #[test]
    fn a_store_with_active_path_locks_is_counted_and_refused() {
        let directory = tempfile::tempdir().expect("temporary legacy store");
        let store_path = directory.path().join("legacy.sema");
        let mut engine =
            Engine::open(EngineOpen::new(&store_path, SCHEMA_VERSION)).expect("open legacy store");
        let legacy = engine
            .register_table(TableDescriptor::new(
                LEGACY_TABLE,
                FamilyName::new("orchestrate-path-lock"),
                SchemaHash::for_label("orchestrate-path-lock-v1"),
            ))
            .expect("register legacy family");
        engine
            .assert(Assertion::new(
                legacy,
                LegacyStoredLock {
                    name: "active".to_owned(),
                    lock: LegacyLock {
                        path_lock_name: LegacyName("active".to_owned()),
                        path_lock_paths: LegacyPaths(vec![LegacyPath("/owned".to_owned())]),
                        path_lock_description: LegacyReason("legacy".to_owned()),
                    },
                },
            ))
            .expect("write legacy fixture");
        drop(engine);

        let preflight = <LegacyStorePreflight as PreflightsLegacyStore>::inspect(&store_path)
            .expect("inspect legacy store");
        assert_eq!(preflight.active_lock_count(), 1);
        assert!(matches!(
            OrchestrateStore::open(&store_path, defaults(directory.path())),
            Err(StoreError::LegacyActiveLocks { count: 1 })
        ));
    }
}
