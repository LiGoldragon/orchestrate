//! A store this generation did not write.
//!
//! The open is one write: every family this Nexus keeps is declared to the
//! engine, so an open the engine refuses on a family has written nothing at
//! all.
//!
//! That a store the previous release served resumes under this one is
//! witnessed at process level by orchestrate-test's
//! `orchestrate-previous-orchestrate` scenario, which starts the released
//! Nexus and then this one on the same state directory. It cannot be a test
//! here: the previous release links signal 7.0.0, this one signal 8.0.0, and
//! signal's `links = "signal"` admits one signal per Cargo graph.

use orchestrate_nexus::{
    OpensStore, OrchestrateStore,
    store::{
        StoreError,
        record::{LOCKS_TABLE, METADATA_TABLE, SCHEMA_VERSION, StoredLock},
    },
};
use sema_engine::{Engine, EngineOpen, FamilyName, SchemaHash, TableDescriptor};
use signal_orchestrate::OrchestrateNexusConfiguration;

trait Fixture {
    fn defaults(&self) -> OrchestrateNexusConfiguration;
}

impl Fixture for std::path::Path {
    fn defaults(&self) -> OrchestrateNexusConfiguration {
        OrchestrateNexusConfiguration {
            ordinary_socket_path: self.join("ordinary.sock").display().to_string(),
            meta_socket_path: self.join("meta.sock").display().to_string(),
        }
    }
}

#[test]
fn an_open_refused_on_a_family_writes_nothing() {
    let directory = tempfile::tempdir().expect("isolated store directory");
    let store_path = directory.path().join("orchestrate.sema");
    {
        // A store whose Lock family is some other generation's, which this
        // one neither is nor evolves from.
        let mut engine =
            Engine::open(EngineOpen::new(&store_path, SCHEMA_VERSION)).expect("open the store");
        engine
            .register_table(TableDescriptor::<StoredLock>::new(
                LOCKS_TABLE,
                FamilyName::new("orchestrate-lock"),
                SchemaHash::for_label("orchestrate-lock-from-elsewhere"),
            ))
            .expect("register a foreign Lock family");
    }

    let error = OrchestrateStore::open(&store_path, directory.path().defaults())
        .err()
        .expect("a foreign family refuses the open");
    assert!(matches!(error, StoreError::Engine(_)), "found {error:?}");

    let engine =
        Engine::open(EngineOpen::new(&store_path, SCHEMA_VERSION)).expect("reopen the store");
    assert!(
        !engine.catalog().is_registered(&METADATA_TABLE),
        "the refused open registered no family and seeded no metadata"
    );
}
