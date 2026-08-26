//! Behavioral contract for the ordinary Lock surface.
//!
//! This fixture deliberately addresses the generated 1/5 Signal vocabulary.
//! The Nexus owns the transitions; textual Datom spelling belongs only to the
//! CLI fixture once the generated projection is available.

use std::path::PathBuf;

use meta_signal_orchestrate::{Configure, MetaSocketPath, OrdinarySocketPath};
use orchestrate::OrchestrateStore;
use signal_orchestrate::{
    FlowId, Lock, LockName, LockOverlap, LockPath, LockPaths, LockReason, LockRejection,
    LockRequest, LockSnapshot, Locks, Observation, ObserveSelection, OrchestrateReply,
    OrchestrateRequest, ReleaseRejection,
};

struct StoreFixture {
    _directory: tempfile::TempDir,
    store: OrchestrateStore,
}

impl StoreFixture {
    fn new() -> Self {
        let directory = tempfile::tempdir().expect("isolated Nexus store");
        let configuration = Configure {
            ordinary_socket_path: OrdinarySocketPath(
                directory.path().join("ordinary.sock").display().to_string(),
            ),
            meta_socket_path: MetaSocketPath(
                directory.path().join("meta.sock").display().to_string(),
            ),
        };
        let (store, _) =
            OrchestrateStore::open(&directory.path().join("orchestrate.sema"), configuration)
                .expect("open isolated store");
        Self {
            _directory: directory,
            store,
        }
    }

    fn lock_request(&self, name: &str, paths: &[&str]) -> LockRequest {
        LockRequest {
            lock_name: LockName(name.to_owned()),
            flow_id: FlowId("test-flow".to_owned()),
            lock_paths: LockPaths(
                paths
                    .iter()
                    .map(|path| LockPath((*path).to_owned()))
                    .collect(),
            ),
            lock_reason: LockReason("behavioral proof".to_owned()),
        }
    }

    fn request(&mut self, request: OrchestrateRequest) -> OrchestrateReply {
        self.store.ordinary(request).expect("ordinary transition")
    }
}

fn path(fixture: &StoreFixture, segment: &str) -> String {
    fixture
        ._directory
        .path()
        .join(segment)
        .display()
        .to_string()
}

fn locked(reply: OrchestrateReply) -> Lock {
    match reply {
        OrchestrateReply::Locked(lock) => lock,
        other => panic!("expected Locked reply, found {other:?}"),
    }
}

#[test]
fn locks_are_atomic_complete_and_released_by_durable_id() {
    let mut fixture = StoreFixture::new();
    let first = path(&fixture, "first");
    let second = path(&fixture, "second");
    let request = fixture.lock_request("alpha", &[&first, &second]);
    let acquired = locked(fixture.request(OrchestrateRequest::Lock(request)));

    assert_eq!(acquired.lock_name, LockName("alpha".to_owned()));
    assert_eq!(acquired.flow_id, FlowId("test-flow".to_owned()));
    assert_eq!(
        acquired.lock_paths,
        LockPaths(vec![LockPath(first), LockPath(second)])
    );
    assert_eq!(
        acquired.lock_reason,
        LockReason("behavioral proof".to_owned())
    );

    assert_eq!(
        fixture.request(OrchestrateRequest::Release(acquired.lock_id.clone())),
        OrchestrateReply::Released(acquired),
    );
}

#[test]
fn duplicate_names_and_overlapping_paths_are_typed_refusals() {
    let mut fixture = StoreFixture::new();
    let owned = path(&fixture, "owned");
    let held_request = fixture.lock_request("alpha", &[&owned]);
    let held = locked(fixture.request(OrchestrateRequest::Lock(held_request)));

    let duplicate = fixture.lock_request("alpha", &[&path(&fixture, "elsewhere")]);
    assert_eq!(
        fixture.request(OrchestrateRequest::Lock(duplicate)),
        OrchestrateReply::LockRejected(LockRejection::DuplicateName(held.clone())),
    );
    let requested = format!("{owned}/child");
    let overlap = fixture.lock_request("beta", &[&requested]);
    assert_eq!(
        fixture.request(OrchestrateRequest::Lock(overlap)),
        OrchestrateReply::LockRejected(LockRejection::PathOverlap(LockOverlap {
            lock_path: LockPath(requested),
            lock: held,
        })),
    );
    let independently_available = path(&fixture, "independently-available");
    let mixed = fixture.lock_request("gamma", &[&independently_available, &owned]);
    assert!(matches!(
        fixture.request(OrchestrateRequest::Lock(mixed)),
        OrchestrateReply::LockRejected(LockRejection::PathOverlap(_)),
    ));
    let after_rejection = fixture.lock_request("delta", &[&independently_available]);
    assert!(matches!(
        fixture.request(OrchestrateRequest::Lock(after_rejection)),
        OrchestrateReply::Locked(_),
    ));
}

#[test]
fn current_observation_is_a_name_then_id_ordered_point_in_time_snapshot() {
    let mut fixture = StoreFixture::new();
    let beta_request = fixture.lock_request("beta", &[&path(&fixture, "beta")]);
    let beta = locked(fixture.request(OrchestrateRequest::Lock(beta_request)));
    let alpha_request = fixture.lock_request("alpha", &[&path(&fixture, "alpha")]);
    let alpha = locked(fixture.request(OrchestrateRequest::Lock(alpha_request)));

    assert_eq!(
        fixture.request(OrchestrateRequest::Observe(ObserveSelection::Locks)),
        OrchestrateReply::Observed(Observation::Locks(LockSnapshot {
            locks: Locks(vec![alpha, beta]),
        })),
    );
}

#[test]
fn released_ids_never_reach_a_later_lock_after_restart() {
    let directory = tempfile::tempdir().expect("isolated Nexus store");
    let store_path = directory.path().join("orchestrate.sema");
    let configuration = Configure {
        ordinary_socket_path: OrdinarySocketPath(
            directory.path().join("ordinary.sock").display().to_string(),
        ),
        meta_socket_path: MetaSocketPath(directory.path().join("meta.sock").display().to_string()),
    };
    let (mut first_store, _) =
        OrchestrateStore::open(&store_path, configuration.clone()).expect("open store");
    let first = match first_store
        .ordinary(OrchestrateRequest::Lock(LockRequest {
            lock_name: LockName("alpha".to_owned()),
            flow_id: FlowId("first-flow".to_owned()),
            lock_paths: LockPaths(vec![LockPath(pathbuf(&directory, "first"))]),
            lock_reason: LockReason("first".to_owned()),
        }))
        .expect("acquire first lock")
    {
        OrchestrateReply::Locked(lock) => lock,
        other => panic!("expected Locked reply, found {other:?}"),
    };
    assert_eq!(
        first_store
            .ordinary(OrchestrateRequest::Release(first.lock_id.clone()))
            .expect("release first lock"),
        OrchestrateReply::Released(first.clone()),
    );
    drop(first_store);

    let (mut reopened, _) =
        OrchestrateStore::open(&store_path, configuration).expect("reopen store");
    let later = locked(
        reopened
            .ordinary(OrchestrateRequest::Lock(LockRequest {
                lock_name: LockName("alpha".to_owned()),
                flow_id: FlowId("later-flow".to_owned()),
                lock_paths: LockPaths(vec![LockPath(pathbuf(&directory, "later"))]),
                lock_reason: LockReason("later".to_owned()),
            }))
            .expect("acquire later lock"),
    );
    assert_ne!(first.lock_id, later.lock_id);
    assert_eq!(
        reopened
            .ordinary(OrchestrateRequest::Release(first.lock_id))
            .expect("reject stale release"),
        OrchestrateReply::ReleaseRejected(ReleaseRejection::UnknownLockId),
    );
}

fn pathbuf(directory: &tempfile::TempDir, segment: &str) -> String {
    PathBuf::from(directory.path())
        .join(segment)
        .display()
        .to_string()
}
