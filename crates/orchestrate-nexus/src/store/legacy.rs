//! The pre-0.25 PathLock family, read only to refuse a store that still holds
//! one.
//!
//! This is a guard, never an importer: no row here is ever materialized as a
//! Lock, because a PathLock has no Flow and inventing one would attribute a
//! coordination fact to a flow that never claimed it. `orchestrate-upgrade-
//! preflight` reports the count without opening any other family, and the
//! Nexus refuses to start while the count is nonzero.

use std::path::Path;

use memory_engine::{
    Engine, EngineOpen, EngineRecord, FamilyName, QueryPlan, RecordKey, SchemaHash, SemaConversion,
    TableDescriptor, TableName, TableReference,
};

use super::{ReportsUpgrade, RepresentsStorePath, error::StoreError, record::SCHEMA_VERSION};
use crate::memory::StoredPathLock;

const LEGACY_TABLE: TableName = TableName::new("active_path_locks");

/// A PathLock row is keyed by its name.
impl EngineRecord for StoredPathLock {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.path_lock_name.0.clone())
    }
}

/// Read-only evidence about the pre-0.25 ordinary-state table.
///
/// Read-only but for one thing: a store sema-engine wrote is upgraded to the
/// memory format by the engine's open, as the Nexus's own open would, and
/// the preflight carries that upgrade so it can say so.
pub struct LegacyStorePreflight {
    active_lock_count: usize,
    sema_conversion: Option<SemaConversion>,
}

/// Inspects a store for active PathLock rows.
pub trait PreflightsLegacyStore: Sized {
    fn inspect(store_path: &Path) -> Result<Self, StoreError>;
    fn active_lock_count(&self) -> usize;
}

impl PreflightsLegacyStore for LegacyStorePreflight {
    fn inspect(store_path: &Path) -> Result<Self, StoreError> {
        let exact_store_path = store_path.exact_store_path()?;
        if !store_path.try_exists()? {
            return Ok(Self {
                active_lock_count: 0,
                sema_conversion: None,
            });
        }
        let mut engine = Engine::open(EngineOpen::new(exact_store_path, SCHEMA_VERSION))?;
        // The engine may have upgraded the file from the sema format; a
        // refusal from here on carries that upgrade, as the Nexus's does.
        let sema_conversion = engine.sema_conversion().cloned();
        let active_lock_count = engine
            .count_active_path_locks()
            .map_err(|refusal| refusal.after_sema_conversion(sema_conversion.clone()))?;
        Ok(Self {
            active_lock_count,
            sema_conversion,
        })
    }

    fn active_lock_count(&self) -> usize {
        self.active_lock_count
    }
}

impl ReportsUpgrade for LegacyStorePreflight {
    fn sema_conversion(&self) -> Option<&SemaConversion> {
        self.sema_conversion.as_ref()
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
        let legacy: TableReference<StoredPathLock> = self.register_table(TableDescriptor::new(
            LEGACY_TABLE,
            FamilyName::new("orchestrate-path-lock"),
            SchemaHash::for_label("orchestrate-path-lock-v1"),
        ))?;
        Ok(self.match_records(QueryPlan::all(legacy))?.records().len())
    }
}

#[cfg(test)]
mod tests {
    use memory_engine::Assertion;
    use signal_orchestrate::{
        ConfigurationPath, MetaSocketPath, OrchestrateNexusConfiguration, OrdinarySocketPath,
    };

    use super::*;
    use crate::{
        memory::{PathLock, PathLockDescription, PathLockName, PathLockPath, PathLockPaths},
        store::{OpensStore, OrchestrateStore},
    };

    fn defaults(directory: &Path) -> OrchestrateNexusConfiguration {
        OrchestrateNexusConfiguration {
            ordinary_socket_path: OrdinarySocketPath(ConfigurationPath(
                directory.join("ordinary.sock").display().to_string(),
            )),
            meta_socket_path: MetaSocketPath(ConfigurationPath(
                directory.join("meta.sock").display().to_string(),
            )),
        }
    }

    #[test]
    fn preflight_does_not_create_a_missing_store() {
        let directory = tempfile::tempdir().expect("temporary preflight directory");
        let store_path = directory.path().join("missing.memory");
        let preflight = <LegacyStorePreflight as PreflightsLegacyStore>::inspect(&store_path)
            .expect("inspect missing store");
        assert_eq!(preflight.active_lock_count(), 0);
        assert!(!store_path.exists(), "read-only preflight creates no store");
    }

    #[test]
    fn a_store_with_active_path_locks_is_counted_and_refused() {
        let directory = tempfile::tempdir().expect("temporary legacy store");
        let store_path = directory.path().join("legacy.memory");
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
                StoredPathLock {
                    path_lock_name: PathLockName("active".to_owned()),
                    path_lock: PathLock {
                        path_lock_name: PathLockName("active".to_owned()),
                        path_lock_paths: PathLockPaths(vec![PathLockPath("/owned".to_owned())]),
                        path_lock_description: PathLockDescription("legacy".to_owned()),
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
