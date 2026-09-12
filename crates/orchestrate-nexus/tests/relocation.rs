//! What `orchestrate-relocate` will and will not declare.
//!
//! The declaration is the entitlement — the owner saying "this is the one
//! store, and it is here now". These are the checks that make it hard to say
//! falsely: the original must be gone, and nothing may be holding the sockets
//! the relocated store is about to bind. Neither is the guard; both are what
//! the guard rests on.
//!
//! Every refusal here leaves the store exactly as it was, which each test
//! asserts, because a recovery tool that half-wrote a declaration would be
//! worse than one that refused.

use std::path::Path;

use orchestrate_nexus::{
    OpensStore, OrchestrateStore,
    recovery::{DeclaresRelocation, RecoveryError, StoreRelocation},
    store::{Relocates, Situates},
};
use signal_orchestrate::OrchestrateNexusConfiguration;

/// A scratch Nexus's worth of setup: a store bound to socket paths under this
/// directory, then closed so the file can be moved about.
trait Serves {
    fn sockets(&self) -> OrchestrateNexusConfiguration;
    fn served(&self, store_path: &Path);
    fn declaration(&self, store_path: &Path) -> Option<nexus::Relocation>;
}

impl Serves for Path {
    fn sockets(&self) -> OrchestrateNexusConfiguration {
        OrchestrateNexusConfiguration {
            ordinary_socket_path: self.join("ordinary.sock").display().to_string(),
            meta_socket_path: self.join("meta.sock").display().to_string(),
        }
    }

    fn served(&self, store_path: &Path) {
        let (mut store, configuration) =
            OrchestrateStore::open(store_path, self.sockets()).expect("open a fresh store");
        store
            .situate(vec![
                configuration.ordinary_socket_path,
                configuration.meta_socket_path,
            ])
            .expect("record where the sockets were bound");
    }

    /// Reads whatever declaration the store carries, through a plain open —
    /// which only works for a store that is not refused, so the callers that
    /// need it on a refused store read it at the original address.
    fn declaration(&self, store_path: &Path) -> Option<nexus::Relocation> {
        let (store, _) =
            OrchestrateStore::open(store_path, self.sockets()).expect("open the store to read it");
        store.relocation().expect("read the declaration")
    }
}

#[test]
fn a_copy_cannot_be_declared_a_move_while_the_original_is_still_there() {
    let directory = tempfile::tempdir().expect("isolated store directory");
    let original = directory.path().join("original.sema");
    directory.path().served(&original);

    let copy = directory.path().join("copy.sema");
    std::fs::copy(&original, &copy).expect("copy the store the way a backup would");

    let error = StoreRelocation::declare(&copy)
        .err()
        .expect("a copy is not a move");
    let RecoveryError::OriginStillPresent { origin } = error else {
        panic!("expected an origin-still-present refusal, found {error:?}");
    };
    assert_eq!(origin, original.display().to_string());
    assert!(
        directory.path().declaration(&original).is_none(),
        "and nothing was written: two stores existed, so there was no one \
         store for a declaration to be about"
    );
}

#[test]
fn a_move_is_declared_once_the_original_is_gone_and_the_sockets_are_free() {
    let directory = tempfile::tempdir().expect("isolated store directory");
    let original = directory.path().join("original.sema");
    directory.path().served(&original);

    // A move that did not carry the file: the case the declaration exists
    // for. `cp` then `rm` is what a cross-filesystem `mv` does.
    let moved = directory.path().join("moved.sema");
    std::fs::copy(&original, &moved).expect("carry the bytes across");
    std::fs::remove_file(&original).expect("and remove the original");

    let declared = StoreRelocation::declare(&moved).expect("declare the move");
    assert_eq!(declared.origin(), original.display().to_string());
    assert_eq!(declared.destination(), moved.display().to_string());
    let sockets = directory.path().sockets();
    assert!(
        declared
            .vacated_socket_vector()
            .contains(&sockets.ordinary_socket_path)
            && declared
                .vacated_socket_vector()
                .contains(&sockets.meta_socket_path),
        "and it reports the paths it found free, which are the paths the \
         relocated Nexus is about to bind: {:?}",
        declared.vacated_socket_vector()
    );
}

#[test]
fn a_relocation_is_refused_while_something_still_holds_a_socket() {
    use orchestrate_nexus::transport::socket::{Claiming, SocketClaim};

    let directory = tempfile::tempdir().expect("isolated store directory");
    let original = directory.path().join("original.sema");
    directory.path().served(&original);

    let moved = directory.path().join("moved.sema");
    std::fs::copy(&original, &moved).expect("carry the bytes across");
    std::fs::remove_file(&original).expect("and remove the original");

    // A store can be unlinked while the Nexus serving from it runs on, so
    // the origin being gone is not proof that nothing is serving. The claim
    // is what says so, and this is the claim a live Nexus would be holding.
    let sockets = directory.path().sockets();
    let held = SocketClaim::claim(Path::new(&sockets.ordinary_socket_path)).expect("hold a socket");

    let error = StoreRelocation::declare(&moved)
        .err()
        .expect("something is serving where this store is about to");
    let RecoveryError::SocketStillClaimed { socket_path } = error else {
        panic!("expected a socket-still-claimed refusal, found {error:?}");
    };
    assert_eq!(socket_path, sockets.ordinary_socket_path);

    // And once it lets go, the same declaration is made.
    drop(held);
    StoreRelocation::declare(&moved).expect("the paths are free now");
}

#[test]
fn a_store_at_its_own_recorded_address_has_nothing_to_declare() {
    let directory = tempfile::tempdir().expect("isolated store directory");
    let store_path = directory.path().join("original.sema");
    directory.path().served(&store_path);

    let error = StoreRelocation::declare(&store_path)
        .err()
        .expect("nothing has moved");
    let RecoveryError::Settled { origin } = error else {
        panic!("expected a settled refusal, found {error:?}");
    };
    assert_eq!(origin, store_path.display().to_string());
    assert!(
        directory.path().declaration(&store_path).is_none(),
        "a store that is where it belongs is not given a licence to be \
         somewhere else"
    );
}

#[test]
fn a_move_that_carried_the_file_needs_no_declaration_and_is_told_so() {
    let directory = tempfile::tempdir().expect("isolated store directory");
    let original = directory.path().join("original.sema");
    directory.path().served(&original);

    let moved = directory.path().join("moved.sema");
    std::fs::rename(&original, &moved).expect("move the store, file and all");

    let error = StoreRelocation::declare(&moved)
        .err()
        .expect("the Nexus will open this without being asked");
    let RecoveryError::AlreadyRecognised {
        origin,
        destination,
    } = error
    else {
        panic!("expected an already-recognised refusal, found {error:?}");
    };
    assert_eq!(origin, original.display().to_string());
    assert_eq!(destination, moved.display().to_string());
    assert!(
        directory.path().declaration(&moved).is_none(),
        "and no declaration is left lying about for a later copy to find"
    );
}

#[test]
fn a_store_that_never_served_has_no_address_to_be_moved_from() {
    let directory = tempfile::tempdir().expect("isolated store directory");
    let store_path = directory.path().join("fresh.sema");
    let (store, _) =
        OrchestrateStore::open(&store_path, directory.path().sockets()).expect("a fresh store");
    drop(store);

    let error = StoreRelocation::declare(&store_path)
        .err()
        .expect("never bound is never moved");
    assert!(
        matches!(error, RecoveryError::NeverBound { .. }),
        "found {error:?}"
    );
}

#[test]
fn there_is_nothing_to_declare_about_a_path_with_no_store_at_it() {
    let directory = tempfile::tempdir().expect("isolated store directory");
    let absent = directory.path().join("absent.sema");

    let error = StoreRelocation::declare(&absent)
        .err()
        .expect("no store, no declaration");
    assert!(
        matches!(error, RecoveryError::NoStore { .. }),
        "found {error:?}"
    );
    assert!(
        !absent.exists(),
        "and the refusal did not bring a store into being, the way an open \
         would have"
    );
}

#[test]
fn declaring_twice_leaves_one_declaration() {
    let directory = tempfile::tempdir().expect("isolated store directory");
    let original = directory.path().join("original.sema");
    directory.path().served(&original);

    let moved = directory.path().join("moved.sema");
    std::fs::copy(&original, &moved).expect("carry the bytes across");
    std::fs::remove_file(&original).expect("and remove the original");

    StoreRelocation::declare(&moved).expect("declare the move");
    StoreRelocation::declare(&moved).expect("declare it again");
    let standing = directory
        .path()
        .declaration(&moved)
        .expect("the store carries one");
    assert_eq!(
        standing,
        nexus::Relocation {
            origin: original.display().to_string(),
            destination: moved.display().to_string(),
        },
        "a second run replaces the first rather than leaving two moves \
         declared for one store, which the invariant would then refuse"
    );
}
