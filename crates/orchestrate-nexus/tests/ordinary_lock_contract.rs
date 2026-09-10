//! Behavioral contract for the ordinary Lock surface.

use std::path::PathBuf;

use meta_signal_orchestrate::Configure;
use orchestrate_nexus::{HandlesOrdinary, OpensStore, OrchestrateStore, ordinary::OrdinaryOutcome};
use signal_orchestrate::{
    Lock, LockOverlap, LockRejection, LockRequest, Observation, ObserveSelection, Query,
    ReleaseRejection, Response,
};

struct StoreFixture {
    directory: tempfile::TempDir,
    store: OrchestrateStore,
}

trait CreatesStoreFixture: Sized {
    fn create() -> Self;
}

impl CreatesStoreFixture for StoreFixture {
    fn create() -> Self {
        let directory = tempfile::tempdir().expect("isolated Nexus store");
        let configuration = directory.configuration();
        let (store, _) =
            OrchestrateStore::open(&directory.path().join("orchestrate.sema"), configuration)
                .expect("open isolated store");
        Self { directory, store }
    }
}

trait ConfiguresFixture {
    fn configuration(&self) -> Configure;
}

impl ConfiguresFixture for tempfile::TempDir {
    fn configuration(&self) -> Configure {
        Configure {
            ordinary_socket_path: self.path().join("ordinary.sock").display().to_string(),
            meta_socket_path: self.path().join("meta.sock").display().to_string(),
        }
    }
}

trait ExercisesStore {
    fn lock_request(&self, name: &str, paths: &[&str]) -> LockRequest;
    fn request(&mut self, query: Query) -> OrdinaryOutcome;
    fn path(&self, segment: &str) -> String;
}

impl ExercisesStore for StoreFixture {
    fn lock_request(&self, name: &str, paths: &[&str]) -> LockRequest {
        LockRequest {
            lock_name: name.to_owned(),
            flow_id: "test-flow".to_owned(),
            lock_path_vector: paths.iter().map(|path| (*path).to_owned()).collect(),
            lock_reason: "behavioral proof".to_owned(),
        }
    }

    fn request(&mut self, query: Query) -> OrdinaryOutcome {
        self.store.ordinary(query).expect("ordinary transition")
    }

    fn path(&self, segment: &str) -> String {
        self.directory.path().join(segment).display().to_string()
    }
}

trait ReadsOutcome {
    fn response(self) -> Response;
    fn locked(self) -> Lock;
}

impl ReadsOutcome for OrdinaryOutcome {
    fn response(self) -> Response {
        match self {
            Self::Response(response) => response,
        }
    }

    fn locked(self) -> Lock {
        match self.response() {
            Response::Locked(lock) => lock,
            other => panic!("expected Locked response, found {other:?}"),
        }
    }
}

#[test]
fn locks_are_atomic_complete_and_released_by_durable_id() {
    let mut fixture = StoreFixture::create();
    let first = fixture.path("first");
    let second = fixture.path("second");
    let acquired = fixture
        .request(Query::Lock(
            fixture.lock_request("alpha", &[&first, &second]),
        ))
        .locked();

    assert_eq!(acquired.lock_name, "alpha");
    assert_eq!(acquired.flow_id, "test-flow");
    assert_eq!(acquired.lock_path_vector, vec![first, second]);
    assert_eq!(acquired.lock_reason, "behavioral proof");
    assert_eq!(
        fixture.request(Query::Release(acquired.lock_id)).response(),
        Response::Released(acquired),
    );
}

#[test]
fn duplicate_names_and_overlapping_paths_are_typed_refusals() {
    let mut fixture = StoreFixture::create();
    let owned = fixture.path("owned");
    let held = fixture
        .request(Query::Lock(fixture.lock_request("alpha", &[&owned])))
        .locked();

    let elsewhere = fixture.path("elsewhere");
    assert_eq!(
        fixture
            .request(Query::Lock(fixture.lock_request("alpha", &[&elsewhere]),))
            .response(),
        Response::LockRejected(LockRejection::DuplicateName(held.clone())),
    );
    let requested = format!("{owned}/child");
    assert_eq!(
        fixture
            .request(Query::Lock(fixture.lock_request("beta", &[&requested]),))
            .response(),
        Response::LockRejected(LockRejection::PathOverlap(LockOverlap {
            lock_path: requested.clone(),
            lock: held,
        })),
    );

    let independent = fixture.path("independent");
    assert!(matches!(
        fixture
            .request(Query::Lock(
                fixture.lock_request("gamma", &[&independent, &owned]),
            ))
            .response(),
        Response::LockRejected(LockRejection::PathOverlap(_)),
    ));
    assert!(matches!(
        fixture
            .request(Query::Lock(fixture.lock_request("delta", &[&independent]),))
            .response(),
        Response::Locked(_),
    ));
}

#[test]
fn observe_locks_is_name_then_id_ordered() {
    let mut fixture = StoreFixture::create();
    let beta_path = fixture.path("beta");
    let beta = fixture
        .request(Query::Lock(fixture.lock_request("beta", &[&beta_path])))
        .locked();
    let alpha_path = fixture.path("alpha");
    let alpha = fixture
        .request(Query::Lock(fixture.lock_request("alpha", &[&alpha_path])))
        .locked();

    assert_eq!(
        fixture
            .request(Query::Observe(ObserveSelection::Locks))
            .response(),
        Response::Observed(Observation::Locks(vec![alpha, beta])),
    );
}

#[test]
fn released_ids_never_reach_a_later_lock_after_restart() {
    let directory = tempfile::tempdir().expect("isolated Nexus store");
    let store_path = directory.path().join("orchestrate.sema");
    let configuration = directory.configuration();
    let (mut first_store, _) =
        OrchestrateStore::open(&store_path, configuration.clone()).expect("open store");
    let first = first_store
        .ordinary(Query::Lock(directory.lock_request(
            "alpha",
            "first-flow",
            directory.path_string("first"),
            "first",
        )))
        .expect("acquire first lock")
        .locked();
    assert_eq!(
        first_store
            .ordinary(Query::Release(first.lock_id))
            .expect("release first lock")
            .response(),
        Response::Released(first.clone()),
    );
    drop(first_store);

    let (mut reopened, _) =
        OrchestrateStore::open(&store_path, configuration).expect("reopen store");
    let later = reopened
        .ordinary(Query::Lock(directory.lock_request(
            "alpha",
            "later-flow",
            directory.path_string("later"),
            "later",
        )))
        .expect("acquire later lock")
        .locked();
    assert_ne!(first.lock_id, later.lock_id);
    assert_eq!(
        reopened
            .ordinary(Query::Release(first.lock_id))
            .expect("reject stale release")
            .response(),
        Response::ReleaseRejected(ReleaseRejection::UnknownLockId),
    );
}

trait JoinsFixturePath {
    fn path_string(&self, segment: &str) -> String;
    fn lock_request(&self, name: &str, flow: &str, path: String, reason: &str) -> LockRequest;
}

impl JoinsFixturePath for tempfile::TempDir {
    fn path_string(&self, segment: &str) -> String {
        PathBuf::from(self.path())
            .join(segment)
            .display()
            .to_string()
    }

    fn lock_request(&self, name: &str, flow: &str, path: String, reason: &str) -> LockRequest {
        LockRequest {
            lock_name: name.to_owned(),
            flow_id: flow.to_owned(),
            lock_path_vector: vec![path],
            lock_reason: reason.to_owned(),
        }
    }
}
