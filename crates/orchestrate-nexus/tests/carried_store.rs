//! A store carried away from where it was bound.
//!
//! The Nexus's socket paths live in its store, which is what makes a meta
//! Configure of them mean anything. It is also what makes the store
//! dangerous to copy: a copy opened by the same user on the same machine
//! carries the production socket paths and would bind them, taking the live
//! service's sockets out from under it.
//!
//! So a Nexus writes down which store file it actually opened, and refuses to
//! bind a store that has been opened somewhere else.

use std::path::Path;

use orchestrate_nexus::{
    OpensStore, OrchestrateStore,
    store::{Situates, StoreError},
};
use signal_orchestrate::OrchestrateNexusConfiguration;

trait Defaults {
    fn defaults(&self) -> OrchestrateNexusConfiguration;
}

impl Defaults for Path {
    fn defaults(&self) -> OrchestrateNexusConfiguration {
        OrchestrateNexusConfiguration {
            ordinary_socket_path: self.join("ordinary.sock").display().to_string(),
            meta_socket_path: self.join("meta.sock").display().to_string(),
        }
    }
}

/// Opens a store at `store_path` and records it as bound there, which is what
/// a Nexus does once both its sockets are up.
trait Serves {
    fn served(&self, store_path: &Path) -> OrchestrateNexusConfiguration;
}

impl Serves for Path {
    fn served(&self, store_path: &Path) -> OrchestrateNexusConfiguration {
        let (mut store, configuration) =
            OrchestrateStore::open(store_path, self.defaults()).expect("open a fresh store");
        store
            .situate(vec![
                configuration.ordinary_socket_path.clone(),
                configuration.meta_socket_path.clone(),
            ])
            .expect("record where the sockets were bound");
        configuration
    }
}

#[test]
fn a_store_reopened_where_it_was_bound_resumes_without_complaint() {
    let directory = tempfile::tempdir().expect("isolated store directory");
    let store_path = directory.path().join("original.sema");
    let bound = directory.path().served(&store_path);

    let (store, resumed) = OrchestrateStore::open(&store_path, directory.path().defaults())
        .expect("the same store at the same path is not a copy");
    assert_eq!(resumed, bound);
    assert_eq!(
        store
            .situation()
            .expect("read the record back")
            .expect("a bound store has one")
            .store_path,
        store_path.display().to_string(),
    );
}

#[test]
fn a_store_copied_to_a_second_path_refuses_to_open_and_names_both_paths() {
    let directory = tempfile::tempdir().expect("isolated store directory");
    let original = directory.path().join("original.sema");
    directory.path().served(&original);

    let copy = directory.path().join("copy.sema");
    std::fs::copy(&original, &copy).expect("copy the store the way a backup would");

    let error = OrchestrateStore::open(&copy, directory.path().defaults())
        .err()
        .expect("a carried store refuses to open");
    let StoreError::CarriedStore { recorded, opened } = error else {
        panic!("expected a carried-store refusal, found {error:?}");
    };
    assert_eq!(recorded, original.display().to_string());
    assert_eq!(opened, copy.display().to_string());
}

#[test]
fn a_store_that_was_never_bound_is_not_a_copy() {
    let directory = tempfile::tempdir().expect("isolated store directory");
    let store_path = directory.path().join("fresh.sema");
    let (store, _) = OrchestrateStore::open(&store_path, directory.path().defaults())
        .expect("a fresh store opens");
    assert!(
        store.situation().expect("read the record").is_none(),
        "nothing has bound it yet, so there is nothing for a later open to \
         compare itself against"
    );
    drop(store);
    OrchestrateStore::open(&store_path, directory.path().defaults())
        .expect("and reopening it is not refused");
}
