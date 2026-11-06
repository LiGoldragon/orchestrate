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

use std::{
    ffi::OsString,
    io::ErrorKind,
    os::unix::{ffi::OsStringExt, fs::symlink},
    path::{Path, PathBuf},
};

use memory_engine::{Engine, EngineOpen, FamilyName, SchemaHash, TableDescriptor};
use orchestrate_nexus::{
    OpensStore, OrchestrateStore,
    store::{
        LegacyStorePreflight, PreflightsLegacyStore, StoreError,
        record::{LOCKS_TABLE, METADATA_TABLE, SCHEMA_VERSION, StoredLock},
    },
};
use signal_orchestrate::OrchestrateNexusConfiguration;
use signal_orchestrate::{ConfigurationPath, MetaSocketPath, OrdinarySocketPath};

trait Fixture {
    fn defaults(&self) -> OrchestrateNexusConfiguration;
    fn non_utf8_store(&self, label: &[u8]) -> PathBuf;
}

impl Fixture for Path {
    fn defaults(&self) -> OrchestrateNexusConfiguration {
        OrchestrateNexusConfiguration {
            ordinary_socket_path: OrdinarySocketPath(ConfigurationPath(
                self.join("ordinary.sock").display().to_string(),
            )),
            meta_socket_path: MetaSocketPath(ConfigurationPath(
                self.join("meta.sock").display().to_string(),
            )),
        }
    }

    fn non_utf8_store(&self, label: &[u8]) -> PathBuf {
        let mut bytes = self.as_os_str().as_encoded_bytes().to_vec();
        bytes.push(b'/');
        bytes.extend_from_slice(label);
        bytes.push(0xff);
        PathBuf::from(OsString::from_vec(bytes))
    }
}

#[test]
fn a_store_path_without_a_parent_is_a_typed_open_error() {
    let directory = tempfile::tempdir().expect("isolated store directory");
    let error = OrchestrateStore::open(Path::new("/"), directory.path().defaults())
        .err()
        .expect("a filesystem root cannot name a store file");

    assert!(
        matches!(error, StoreError::Filesystem(ref source) if source.kind() == ErrorKind::InvalidInput),
        "found {error:?}"
    );
}

#[test]
fn store_open_refuses_a_non_utf8_path_without_opening_a_lossy_alternative() {
    let directory = tempfile::tempdir().expect("isolated store directory");
    let store_path = directory.path().non_utf8_store(b"open-");
    let lossy = PathBuf::from(store_path.to_string_lossy().into_owned());
    let error = OrchestrateStore::open(&store_path, directory.path().defaults())
        .err()
        .expect("the store path cannot be represented exactly");

    assert!(
        matches!(error, StoreError::NonUtf8StorePath { .. }),
        "found {error:?}"
    );
    assert!(!lossy.exists(), "no replacement-character store is opened");
}

#[test]
fn legacy_preflight_refuses_a_non_utf8_path_without_inspecting_a_lossy_alternative() {
    let directory = tempfile::tempdir().expect("isolated store directory");
    let store_path = directory.path().non_utf8_store(b"preflight-");
    let lossy = PathBuf::from(store_path.to_string_lossy().into_owned());
    let error = LegacyStorePreflight::inspect(&store_path)
        .err()
        .expect("the store path cannot be represented exactly");

    assert!(
        matches!(error, StoreError::NonUtf8StorePath { .. }),
        "found {error:?}"
    );
    assert!(
        !lossy.exists(),
        "no replacement-character store is inspected"
    );
}

#[test]
fn legacy_preflight_reports_an_indeterminate_path_instead_of_calling_it_absent() {
    let directory = tempfile::tempdir().expect("isolated store directory");
    let first = directory.path().join("first.memory");
    let second = directory.path().join("second.memory");
    symlink(&second, &first).expect("first half of a symlink loop");
    symlink(&first, &second).expect("second half of a symlink loop");
    let error = LegacyStorePreflight::inspect(&first)
        .err()
        .expect("the store's existence cannot be determined");

    assert!(
        matches!(error, StoreError::Filesystem(_)),
        "found {error:?}"
    );
    assert_eq!(
        std::fs::read_link(&first).expect("the first link remains"),
        second
    );
    assert_eq!(
        std::fs::read_link(&second).expect("the second link remains"),
        first
    );
}

#[test]
fn an_open_refused_on_a_family_writes_nothing() {
    let directory = tempfile::tempdir().expect("isolated store directory");
    let store_path = directory.path().join("orchestrate.memory");
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
