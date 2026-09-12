//! The one thing a 0.30 store needs to become a 0.32 store.
//!
//! 0.30 and 0.31 keep the desired configuration in its own family,
//! `orchestrate_configuration_v2`, and keep no record of whether the
//! privileged Configure was ever done — that record did not exist. 0.32
//! keeps both in the standard Nexus metadata tree instead.
//!
//! So the cutover is one read: a store that already carries a configuration
//! row seeds its metadata tree from it rather than from the executable's
//! defaults, and the row is retracted in the same commit. Locks and the id
//! allocator are untouched; their families and record layouts are identical
//! across 0.30, 0.31 and 0.32, so there is nothing to move.
//!
//! Nothing older is read. The pre-0.30 families this repository once carried
//! importers for were emptied by 0.30 itself before it was deployed, and the
//! importer was written against a record shape that had already been retired
//! when it was written.

use sema_engine::{
    Engine, FamilyName, QueryPlan, RecordKey, SchemaHash, TableDescriptor, TableReference,
};

use super::{
    error::StoreError,
    record::{CONFIGURATION_KEY, CONFIGURATION_TABLE, Familial, StoredConfiguration},
};

impl sema_engine::EngineRecord for StoredConfiguration {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(CONFIGURATION_KEY)
    }
}

impl Familial for StoredConfiguration {
    fn descriptor() -> TableDescriptor<Self> {
        TableDescriptor::new(
            CONFIGURATION_TABLE,
            FamilyName::new("orchestrate-configuration"),
            SchemaHash::for_label("orchestrate-configuration-v2"),
        )
    }
}

/// The configuration a previous generation left behind, if it left one.
pub struct CarriedConfiguration {
    configuration: Option<StoredConfiguration>,
    table: TableReference<StoredConfiguration>,
}

/// Reads what the previous generation stored, and clears it once carried.
pub trait Carrying: Sized {
    fn read_from(engine: &mut Engine) -> Result<Self, StoreError>;
    fn configuration(&self) -> Option<&StoredConfiguration>;
    fn table(&self) -> TableReference<StoredConfiguration>;
    fn is_present(&self) -> bool {
        self.configuration().is_some()
    }
}

impl Carrying for CarriedConfiguration {
    fn read_from(engine: &mut Engine) -> Result<Self, StoreError> {
        let table: TableReference<StoredConfiguration> =
            engine.register_table(StoredConfiguration::descriptor())?;
        let configuration = engine
            .match_records(QueryPlan::all(table))?
            .records()
            .first()
            .cloned();
        Ok(Self {
            configuration,
            table,
        })
    }

    fn configuration(&self) -> Option<&StoredConfiguration> {
        self.configuration.as_ref()
    }

    fn table(&self) -> TableReference<StoredConfiguration> {
        self.table
    }
}

#[cfg(test)]
mod tests {
    use nexus::Configurable;
    use sema_engine::{Assertion, Engine, EngineOpen};
    use signal_orchestrate::{Lock, Observation, ObserveSelection, OrchestrateNexusConfiguration};

    use super::*;
    use crate::{
        ordinary::Observes,
        store::{
            KeepsMetadata, OpensStore, OrchestrateStore,
            record::{Familial, SCHEMA_VERSION, StoredAllocator, StoredLock, Storing},
        },
    };

    /// Writes a store in exactly the shape deployed 0.30.0 and released
    /// 0.31.0 leave behind: a separate configuration family, Locks and an
    /// allocator in the families this generation still uses, and no metadata
    /// tree, because none existed.
    fn previous_generation_store(store_path: &std::path::Path, held: &Lock) {
        let mut engine = Engine::open(EngineOpen::new(store_path, SCHEMA_VERSION)).expect("open");
        let configurations = engine
            .register_table(StoredConfiguration::descriptor())
            .expect("register the previous configuration family");
        engine
            .assert(Assertion::new(
                configurations,
                StoredConfiguration {
                    ordinary_socket: "/run/previous-ordinary.sock".to_owned(),
                    meta_socket: "/run/previous-meta.sock".to_owned(),
                },
            ))
            .expect("write the previous configuration");
        let locks = engine
            .register_table(StoredLock::descriptor())
            .expect("register the Lock family");
        engine
            .assert(Assertion::new(locks, StoredLock::from_public(held)))
            .expect("write a held Lock");
        let allocator = engine
            .register_table(StoredAllocator::descriptor())
            .expect("register the allocator family");
        engine
            .assert(Assertion::new(
                allocator,
                StoredAllocator { next_lock_id: 8 },
            ))
            .expect("write the allocator");
    }

    #[test]
    fn a_previous_generation_store_carries_its_configuration_and_its_locks() {
        let directory = tempfile::tempdir().expect("temporary store");
        let store_path = directory.path().join("previous.sema");
        let held = Lock {
            lock_id: 7,
            lock_name: "held".to_owned(),
            flow_id: "flow-857335".to_owned(),
            lock_path_vector: vec!["/held".to_owned()],
            lock_reason: "held across the cutover".to_owned(),
        };
        previous_generation_store(&store_path, &held);

        let defaults = OrchestrateNexusConfiguration {
            ordinary_socket_path: "/run/default-ordinary.sock".to_owned(),
            meta_socket_path: "/run/default-meta.sock".to_owned(),
        };
        let (store, configuration) =
            OrchestrateStore::open(&store_path, defaults.clone()).expect("open the previous store");
        assert_eq!(
            configuration,
            OrchestrateNexusConfiguration {
                ordinary_socket_path: "/run/previous-ordinary.sock".to_owned(),
                meta_socket_path: "/run/previous-meta.sock".to_owned(),
            },
            "the metadata tree is seeded from the previous generation's row, not from the defaults"
        );
        assert!(
            !store.state().meta_configure_occurred(),
            "the previous generation kept no such record, so the cutover cannot claim one"
        );
        assert_eq!(
            store.observe(ObserveSelection::Locks).expect("observe"),
            Observation::Locks(vec![held.clone()]),
            "Locks carry across untouched"
        );
        drop(store);

        // The row is retracted in the seeding commit, so a second open reads
        // the metadata tree and never the previous family again.
        let mut engine =
            Engine::open(EngineOpen::new(&store_path, SCHEMA_VERSION)).expect("reopen");
        let carried = CarriedConfiguration::read_from(&mut engine).expect("read");
        assert!(!carried.is_present(), "the previous row is cleared, once");
        drop(engine);

        let (store, configuration) =
            OrchestrateStore::open(&store_path, defaults).expect("reopen the store");
        assert_eq!(
            configuration.ordinary_socket_path,
            "/run/previous-ordinary.sock"
        );
        assert_eq!(
            store.observe(ObserveSelection::Locks).expect("observe"),
            Observation::Locks(vec![held])
        );
    }
}
