//! A store found somewhere other than where it was bound.
//!
//! The Nexus's socket paths live in its store, which is what makes a meta
//! Configure of them mean anything. It is also what makes the store
//! dangerous to copy: a copy opened by the same user on the same machine
//! carries the production socket paths and would bind them, taking the live
//! service's sockets out from under it.
//!
//! So a Nexus writes down which store file it actually opened — the file, by
//! the identity the filesystem gives it, not merely the name it was reached
//! by. That distinction is what lets the three cases come apart: a store at
//! its own address resumes, the same file at a new address has plainly moved,
//! and a *different* file at a new address is a copy and is refused.
//!
//! A move that did not carry the file — across a filesystem, or through
//! `tar`, or out of a snapshot — is indistinguishable from a copy, and is
//! refused with it. What the owner does about that is `relocation.rs`.

use std::path::Path;

use nexus::{Identifying, Situated};
use orchestrate_nexus::{
    OpensStore, OrchestrateStore,
    store::{Relocates, Situates, StoreError},
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
/// a Nexus does once both its sockets are up. The store is closed again on
/// the way out, so the caller may move or copy the file.
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
            .store()
            .path(),
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

#[test]
fn a_store_moved_with_its_file_intact_opens_and_re_records_its_new_address() {
    let directory = tempfile::tempdir().expect("isolated store directory");
    let original = directory.path().join("original.sema");
    directory.path().served(&original);

    // `mv` within a filesystem. One file, one claimant, a new address: the
    // record recognises itself and nobody has to say anything.
    let moved = directory.path().join("moved.sema");
    std::fs::rename(&original, &moved).expect("move the store");

    let (mut store, _) = OrchestrateStore::open(&moved, directory.path().defaults())
        .expect("one file at a new address is a move, not a copy");
    store
        .situate(vec!["/scratch/ordinary.sock".to_owned()])
        .expect("bind and record");
    assert_eq!(
        store
            .situation()
            .expect("read the record back")
            .expect("a bound store has one")
            .store()
            .path(),
        moved.display().to_string(),
        "the record now names where it actually is, so a copy taken from \
         here is measured against this address and not the old one"
    );
}

#[test]
fn a_store_whose_bytes_were_carried_without_its_file_is_refused_until_declared() {
    let directory = tempfile::tempdir().expect("isolated store directory");
    let original = directory.path().join("original.sema");
    directory.path().served(&original);

    // What a cross-filesystem `mv` does, and `tar`, and a snapshot restore:
    // new bytes in a new file, the old one gone. No record can tell this
    // from a duplication, so it is refused with one.
    let carried = directory.path().join("carried.sema");
    std::fs::copy(&original, &carried).expect("carry the bytes across");
    std::fs::remove_file(&original).expect("and remove the original");

    let error = OrchestrateStore::open(&carried, directory.path().defaults())
        .err()
        .expect("the origin being gone is not consent");
    assert!(
        matches!(error, StoreError::CarriedStore { .. }),
        "found {error:?}"
    );
}

#[test]
fn a_declared_move_is_admitted_and_the_declaration_is_spent_by_the_bind() {
    let directory = tempfile::tempdir().expect("isolated store directory");
    let original = directory.path().join("original.sema");
    directory.path().served(&original);

    let carried = directory.path().join("carried.sema");
    std::fs::copy(&original, &carried).expect("carry the bytes across");
    std::fs::remove_file(&original).expect("and remove the original");
    carried.declare();

    let (mut store, _) = OrchestrateStore::open(&carried, directory.path().defaults())
        .expect("the declared move is admitted");
    assert!(
        store.relocation().expect("read the declaration").is_some(),
        "it is still standing while the move is in progress: a bind that \
         fails must not cost the operator the declaration"
    );
    store
        .situate(vec!["/scratch/ordinary.sock".to_owned()])
        .expect("bind and record");
    assert!(
        store.relocation().expect("read the declaration").is_none(),
        "and the commit that records the new address spends it"
    );
    drop(store);

    // With the declaration spent and the address re-recorded, a copy taken
    // from the relocated store finds no standing licence.
    let third = directory.path().join("third.sema");
    std::fs::copy(&carried, &third).expect("copy the relocated store");
    let error = OrchestrateStore::open(&third, directory.path().defaults())
        .err()
        .expect("a declaration admits one move and not a class of them");
    assert!(
        matches!(error, StoreError::CarriedStore { .. }),
        "found {error:?}"
    );
}

#[test]
fn a_declaration_does_not_admit_a_third_path() {
    let directory = tempfile::tempdir().expect("isolated store directory");
    let original = directory.path().join("original.sema");
    directory.path().served(&original);

    let carried = directory.path().join("carried.sema");
    std::fs::copy(&original, &carried).expect("carry the bytes across");
    std::fs::remove_file(&original).expect("and remove the original");
    carried.declare();

    // A copy taken *after* the declaration, before the move was completed.
    // It carries the declaration and is at neither end of it.
    let third = directory.path().join("third.sema");
    std::fs::copy(&carried, &third).expect("copy the declared store");

    let error = OrchestrateStore::open(&third, directory.path().defaults())
        .err()
        .expect("a declaration names one destination");
    let StoreError::UnrelatedRelocation {
        declared_origin,
        declared_destination,
        recorded,
        opened,
    } = error
    else {
        panic!("expected an unrelated-relocation refusal, found {error:?}");
    };
    assert_eq!(declared_origin, original.display().to_string());
    assert_eq!(declared_destination, carried.display().to_string());
    assert_eq!(recorded, original.display().to_string());
    assert_eq!(opened, third.display().to_string());
}

/// Declares the move the way an operator does, through the same code
/// `orchestrate-relocate` runs. The warrant it checks is `relocation.rs`'s
/// subject; here it is only the means of getting a declaration written.
trait Declares {
    fn declare(&self);
}

impl Declares for Path {
    fn declare(&self) {
        use orchestrate_nexus::recovery::{DeclaresRelocation, StoreRelocation};
        StoreRelocation::declare(self).expect("declare the move");
    }
}
