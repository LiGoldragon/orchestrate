//! Behavioral contract for the ordinary 1/6 Lock surface.

use std::path::PathBuf;

use meta_signal_orchestrate::Configure;
use orchestrate::{OrchestrateStore, ordinary::OrdinaryOutcome};
use signal_orchestrate::{
    FlowId, Lock, LockOverlap, LockPaths, LockRejection, LockRequest, Locks, Observation,
    ObserveSelection, Refusal, ReleaseRejection, Reply, Request,
};

struct StoreFixture {
    directory: tempfile::TempDir,
    store: OrchestrateStore,
}

impl StoreFixture {
    fn new() -> Self {
        let directory = tempfile::tempdir().expect("isolated Nexus store");
        let configuration = configuration(&directory);
        let (store, _) =
            OrchestrateStore::open(&directory.path().join("orchestrate.sema"), configuration)
                .expect("open isolated store");
        Self { directory, store }
    }

    fn lock_request(&self, name: &str, paths: &[&str]) -> LockRequest {
        LockRequest {
            lock_name: name.try_into().expect("test Lock name"),
            flow_id: "test-flow".try_into().expect("test Flow ID"),
            lock_paths: LockPaths(
                paths
                    .iter()
                    .map(|path| (*path).try_into().expect("test absolute path"))
                    .collect(),
            ),
            lock_reason: "behavioral proof".try_into().expect("test reason"),
        }
    }

    fn request(&mut self, request: Request) -> OrdinaryOutcome {
        self.store.ordinary(request).expect("ordinary transition")
    }
}

fn configuration(directory: &tempfile::TempDir) -> Configure {
    Configure {
        ordinary_socket_path: directory
            .path()
            .join("ordinary.sock")
            .display()
            .to_string()
            .try_into()
            .expect("temporary socket path"),
        meta_socket_path: directory
            .path()
            .join("meta.sock")
            .display()
            .to_string()
            .try_into()
            .expect("temporary socket path"),
    }
}

fn path(fixture: &StoreFixture, segment: &str) -> String {
    fixture.directory.path().join(segment).display().to_string()
}

fn reply(outcome: OrdinaryOutcome) -> Reply {
    match outcome {
        OrdinaryOutcome::Reply(reply) => reply,
        OrdinaryOutcome::Refusal(refusal) => panic!("expected reply, found refusal {refusal:?}"),
    }
}

fn refusal(outcome: OrdinaryOutcome) -> Refusal {
    match outcome {
        OrdinaryOutcome::Reply(reply) => panic!("expected refusal, found reply {reply:?}"),
        OrdinaryOutcome::Refusal(refusal) => refusal,
    }
}

fn locked(outcome: OrdinaryOutcome) -> Lock {
    match reply(outcome) {
        Reply::Locked(lock) => lock,
        other => panic!("expected Locked reply, found {other:?}"),
    }
}

#[test]
fn locks_are_atomic_complete_and_released_by_durable_id() {
    let mut fixture = StoreFixture::new();
    let first = path(&fixture, "first");
    let second = path(&fixture, "second");
    let acquired = locked(fixture.request(Request::Lock(
        fixture.lock_request("alpha", &[&first, &second]),
    )));

    assert_eq!(acquired.lock_name.as_ref(), "alpha");
    assert_eq!(acquired.flow_id.as_ref(), "test-flow");
    assert_eq!(
        acquired
            .lock_paths
            .0
            .iter()
            .map(AsRef::as_ref)
            .collect::<Vec<_>>(),
        vec![first.as_str(), second.as_str()]
    );
    assert_eq!(acquired.lock_reason.as_ref(), "behavioral proof");
    assert_eq!(
        reply(fixture.request(Request::Release(acquired.lock_id.clone()))),
        Reply::Released(acquired),
    );
}

#[test]
fn duplicate_names_and_overlapping_paths_are_typed_refusals() {
    let mut fixture = StoreFixture::new();
    let owned = path(&fixture, "owned");
    let held = locked(fixture.request(Request::Lock(fixture.lock_request("alpha", &[&owned]))));

    assert_eq!(
        refusal(fixture.request(Request::Lock(
            fixture.lock_request("alpha", &[&path(&fixture, "elsewhere")])
        ))),
        Refusal::LockRejected(LockRejection::DuplicateName(held.clone())),
    );
    let requested = format!("{owned}/child");
    assert_eq!(
        refusal(fixture.request(Request::Lock(fixture.lock_request("beta", &[&requested])))),
        Refusal::LockRejected(LockRejection::PathOverlap(LockOverlap {
            lock_path: requested.as_str().try_into().expect("test path"),
            lock: held,
        })),
    );
    let independently_available = path(&fixture, "independently-available");
    assert!(matches!(
        refusal(fixture.request(Request::Lock(
            fixture.lock_request("gamma", &[&independently_available, &owned])
        ))),
        Refusal::LockRejected(LockRejection::PathOverlap(_)),
    ));
    assert!(matches!(
        reply(fixture.request(Request::Lock(
            fixture.lock_request("delta", &[&independently_available])
        ))),
        Reply::Locked(_),
    ));
}

#[test]
fn observe_locks_is_a_name_then_id_ordered_point_in_time_value() {
    let mut fixture = StoreFixture::new();
    let beta = locked(fixture.request(Request::Lock(
        fixture.lock_request("beta", &[&path(&fixture, "beta")]),
    )));
    let alpha = locked(fixture.request(Request::Lock(
        fixture.lock_request("alpha", &[&path(&fixture, "alpha")]),
    )));

    assert_eq!(
        reply(fixture.request(Request::Observe(ObserveSelection::Locks))),
        Reply::Observed(Observation::Locks(Locks(vec![alpha, beta]))),
    );
}

#[test]
fn released_ids_never_reach_a_later_lock_after_restart() {
    let directory = tempfile::tempdir().expect("isolated Nexus store");
    let store_path = directory.path().join("orchestrate.sema");
    let configuration = configuration(&directory);
    let (mut first_store, _) =
        OrchestrateStore::open(&store_path, configuration.clone()).expect("open store");
    let first = locked(
        first_store
            .ordinary(Request::Lock(lock_request(
                "alpha",
                "first-flow",
                pathbuf(&directory, "first"),
                "first",
            )))
            .expect("acquire first lock"),
    );
    assert_eq!(
        reply(
            first_store
                .ordinary(Request::Release(first.lock_id.clone()))
                .expect("release first lock")
        ),
        Reply::Released(first.clone()),
    );
    drop(first_store);

    let (mut reopened, _) =
        OrchestrateStore::open(&store_path, configuration).expect("reopen store");
    let later = locked(
        reopened
            .ordinary(Request::Lock(lock_request(
                "alpha",
                "later-flow",
                pathbuf(&directory, "later"),
                "later",
            )))
            .expect("acquire later lock"),
    );
    assert_ne!(first.lock_id, later.lock_id);
    assert_eq!(
        refusal(
            reopened
                .ordinary(Request::Release(first.lock_id))
                .expect("reject stale release")
        ),
        Refusal::ReleaseRejected(ReleaseRejection::UnknownLockId),
    );
}

fn lock_request(name: &str, flow: &str, path: String, reason: &str) -> LockRequest {
    LockRequest {
        lock_name: name.try_into().expect("test Lock name"),
        flow_id: FlowId::try_from(flow).expect("test Flow ID"),
        lock_paths: LockPaths(vec![path.try_into().expect("test absolute path")]),
        lock_reason: reason.try_into().expect("test reason"),
    }
}

fn pathbuf(directory: &tempfile::TempDir, segment: &str) -> String {
    PathBuf::from(directory.path())
        .join(segment)
        .display()
        .to_string()
}
