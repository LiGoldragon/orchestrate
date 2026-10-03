//! A store this generation did not write.
//!
//! Two things about the open are witnessed here. A store served by the
//! previous release, 0.36.0, with its own sema-engine, resumes under this one
//! with its Locks, its configuration and its guard intact. And the open is
//! one write: every family this Nexus keeps is declared to the engine, so an
//! open the engine refuses on a family has written nothing at all.

use std::path::Path;

use nexus::{Identifying, Situated};
use orchestrate_nexus::{
    Configures, OpensStore, OrchestrateStore, Situates,
    ordinary::{Locks, Observes},
    recovery::{DeclaresRelocation, StoreRelocation},
    store::{
        StoreError,
        record::{LOCKS_TABLE, METADATA_TABLE, SCHEMA_VERSION, StoredLock},
    },
};
use orchestrate_nexus_0_36_0 as previous;
use sema_engine::{Engine, EngineOpen, FamilyName, SchemaHash, TableDescriptor};
use signal_orchestrate::{
    Lock, LockOverlap, LockRejection, LockRequest, Observation, ObserveSelection,
    OrchestrateNexusConfiguration, Response,
};

trait Fixture {
    fn defaults(&self) -> OrchestrateNexusConfiguration;
    fn configured(&self) -> OrchestrateNexusConfiguration;
    fn request(&self, name: &str, segment: &str) -> LockRequest;
}

impl Fixture for Path {
    fn defaults(&self) -> OrchestrateNexusConfiguration {
        OrchestrateNexusConfiguration {
            ordinary_socket_path: self.join("ordinary.sock").display().to_string(),
            meta_socket_path: self.join("meta.sock").display().to_string(),
        }
    }

    fn configured(&self) -> OrchestrateNexusConfiguration {
        OrchestrateNexusConfiguration {
            ordinary_socket_path: self.join("configured-ordinary.sock").display().to_string(),
            meta_socket_path: self.join("configured-meta.sock").display().to_string(),
        }
    }

    fn request(&self, name: &str, segment: &str) -> LockRequest {
        LockRequest {
            lock_name: name.to_owned(),
            flow_id: "generations".to_owned(),
            lock_path_vector: vec![self.join(segment).display().to_string()],
            lock_reason: "store generations".to_owned(),
        }
    }
}

/// Serves a store the way a 0.36.0 Nexus does: opens it, takes a meta
/// Configure, holds one Lock, and records where it was bound. Returns the
/// Lock it holds.
trait ServedByPrevious {
    fn served_by_previous(&self, store_path: &Path) -> Lock;
}

impl ServedByPrevious for Path {
    fn served_by_previous(&self, store_path: &Path) -> Lock {
        use previous::{Configures as _, OpensStore as _, Situates as _, ordinary::Locks as _};
        let (mut store, _) = previous::OrchestrateStore::open(store_path, self.defaults())
            .expect("0.36.0 opens a fresh store");
        store
            .meta_configure(self.configured())
            .expect("0.36.0 takes a meta Configure");
        let Response::Locked(held) = store
            .lock(self.request("held", "held"))
            .expect("0.36.0 takes a Lock")
        else {
            panic!("0.36.0 refused a Lock on an empty store");
        };
        let configured = self.configured();
        store
            .situate(vec![
                configured.ordinary_socket_path,
                configured.meta_socket_path,
            ])
            .expect("0.36.0 records where it was bound");
        held
    }
}

#[test]
fn a_store_served_by_0_36_0_resumes_with_its_locks_and_configuration() {
    let directory = tempfile::tempdir().expect("isolated store directory");
    let store_path = directory.path().join("orchestrate.sema");
    let held = directory.path().served_by_previous(&store_path);

    let (mut store, configuration) =
        OrchestrateStore::open(&store_path, directory.path().defaults())
            .expect("this generation opens a store 0.36.0 served");
    assert_eq!(configuration, directory.path().configured());
    assert!(
        store.receipt().meta_configure_done,
        "the meta Configure 0.36.0 took keeps ordinary Configure shut"
    );
    assert_eq!(
        store
            .situation()
            .expect("read the record")
            .expect("0.36.0 recorded its bind")
            .store()
            .path(),
        store_path.display().to_string(),
    );
    let Observation::Locks(locks) = store
        .observe(ObserveSelection::Locks)
        .expect("observe the carried Locks");
    assert_eq!(locks, vec![held.clone()]);

    let overlapping = store
        .lock(directory.path().request("overlapping", "held"))
        .expect("ask for a held path");
    assert_eq!(
        overlapping,
        Response::LockRejected(LockRejection::PathOverlap(LockOverlap {
            lock_path: directory.path().join("held").display().to_string(),
            lock: held.clone(),
        })),
    );
    let Response::Locked(next) = store
        .lock(directory.path().request("next", "free"))
        .expect("ask for a free path")
    else {
        panic!("a free path was refused");
    };
    assert_eq!(next.lock_id, held.lock_id + 1, "the allocator is carried");
}

#[test]
fn a_copy_of_a_store_served_by_0_36_0_is_refused_until_its_move_is_declared() {
    let directory = tempfile::tempdir().expect("isolated store directory");
    let original = directory.path().join("original.sema");
    directory.path().served_by_previous(&original);

    let carried = directory.path().join("carried.sema");
    std::fs::copy(&original, &carried).expect("carry the bytes across");
    std::fs::remove_file(&original).expect("and remove the original");

    let error = OrchestrateStore::open(&carried, directory.path().defaults())
        .err()
        .expect("the guard 0.36.0 wrote still holds");
    assert!(
        matches!(error, StoreError::CarriedStore { .. }),
        "found {error:?}"
    );

    StoreRelocation::declare(&carried).expect("declare the move");
    OrchestrateStore::open(&carried, directory.path().defaults())
        .expect("the declared move is admitted");
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
