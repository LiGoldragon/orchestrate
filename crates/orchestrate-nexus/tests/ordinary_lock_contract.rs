//! Behavioral contract for the ordinary Lock surface, exercised where the
//! Nexus actually applies it: through Nexus Core.

use std::path::Path;

use orchestrate_nexus::{
    OpensStore, OrchestrateStore,
    core::{FoundedCore, Founding},
};
use signal_orchestrate::{
    Lock, LockOverlap, LockRejection, LockRequest, Observation, ObserveSelection,
    OrchestrateNexusConfiguration, Query, ReleaseRejection, Response,
};

struct CoreFixture {
    directory: tempfile::TempDir,
    founded: FoundedCore,
}

trait CreatesCoreFixture: Sized {
    fn create() -> Self;
    fn lock_request(&self, name: &str, paths: &[&str]) -> LockRequest;
    fn path(&self, segment: &str) -> String;
}

impl CreatesCoreFixture for CoreFixture {
    fn create() -> Self {
        let directory = tempfile::tempdir().expect("isolated Nexus store");
        let (store, _) = OrchestrateStore::open(
            &directory.path().join("orchestrate.sema"),
            directory.path().defaults(),
        )
        .expect("open isolated store");
        Self {
            directory,
            founded: FoundedCore::found(store),
        }
    }

    fn lock_request(&self, name: &str, paths: &[&str]) -> LockRequest {
        LockRequest {
            lock_name: name.to_owned(),
            flow_id: "test-flow".to_owned(),
            lock_path_vector: paths.iter().map(|path| (*path).to_owned()).collect(),
            lock_reason: "behavioral proof".to_owned(),
        }
    }

    fn path(&self, segment: &str) -> String {
        self.directory.path().join(segment).display().to_string()
    }
}

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

trait ReadsResponse {
    fn locked(self) -> Lock;
}

impl ReadsResponse for Response {
    fn locked(self) -> Lock {
        match self {
            Self::Locked(lock) => lock,
            other => panic!("expected Locked response, found {other:?}"),
        }
    }
}

#[tokio::test]
async fn locks_are_atomic_complete_and_released_by_durable_id() {
    let fixture = CoreFixture::create();
    let first = fixture.path("first");
    let second = fixture.path("second");
    let acquired = fixture
        .founded
        .core()
        .ask(Query::Lock(
            fixture.lock_request("alpha", &[&first, &second]),
        ))
        .await
        .expect("acquire")
        .locked();

    assert_eq!(acquired.lock_name, "alpha");
    assert_eq!(acquired.flow_id, "test-flow");
    assert_eq!(acquired.lock_path_vector, vec![first, second]);
    assert_eq!(acquired.lock_reason, "behavioral proof");
    assert_eq!(
        fixture
            .founded
            .core()
            .ask(Query::Release(acquired.lock_id))
            .await
            .expect("release"),
        Response::Released(acquired),
    );
}

#[tokio::test]
async fn duplicate_names_and_overlapping_paths_are_typed_refusals() {
    let fixture = CoreFixture::create();
    let owned = fixture.path("owned");
    let held = fixture
        .founded
        .core()
        .ask(Query::Lock(fixture.lock_request("alpha", &[&owned])))
        .await
        .expect("acquire")
        .locked();

    let elsewhere = fixture.path("elsewhere");
    assert_eq!(
        fixture
            .founded
            .core()
            .ask(Query::Lock(fixture.lock_request("alpha", &[&elsewhere])))
            .await
            .expect("refuse duplicate name"),
        Response::LockRejected(LockRejection::DuplicateName(held.clone())),
    );
    let requested = format!("{owned}/child");
    assert_eq!(
        fixture
            .founded
            .core()
            .ask(Query::Lock(fixture.lock_request("beta", &[&requested])))
            .await
            .expect("refuse overlap"),
        Response::LockRejected(LockRejection::PathOverlap(LockOverlap {
            lock_path: requested,
            lock: held,
        })),
    );

    let independent = fixture.path("independent");
    assert!(matches!(
        fixture
            .founded
            .core()
            .ask(Query::Lock(
                fixture.lock_request("gamma", &[&independent, &owned])
            ))
            .await
            .expect("refuse partial overlap"),
        Response::LockRejected(LockRejection::PathOverlap(_)),
    ));
    assert!(matches!(
        fixture
            .founded
            .core()
            .ask(Query::Lock(fixture.lock_request("delta", &[&independent])))
            .await
            .expect("acquire independent"),
        Response::Locked(_),
    ));
}

#[tokio::test]
async fn observe_locks_is_name_then_id_ordered() {
    let fixture = CoreFixture::create();
    let beta_path = fixture.path("beta");
    let beta = fixture
        .founded
        .core()
        .ask(Query::Lock(fixture.lock_request("beta", &[&beta_path])))
        .await
        .expect("acquire beta")
        .locked();
    let alpha_path = fixture.path("alpha");
    let alpha = fixture
        .founded
        .core()
        .ask(Query::Lock(fixture.lock_request("alpha", &[&alpha_path])))
        .await
        .expect("acquire alpha")
        .locked();

    assert_eq!(
        fixture
            .founded
            .core()
            .ask(Query::Observe(ObserveSelection::Locks))
            .await
            .expect("observe"),
        Response::Observed(Observation::Locks(vec![alpha, beta])),
    );
}

#[tokio::test]
async fn released_ids_never_reach_a_later_lock_after_restart() {
    let directory = tempfile::tempdir().expect("isolated Nexus store");
    let store_path = directory.path().join("orchestrate.sema");
    let defaults = directory.path().defaults();
    let (store, _) = OrchestrateStore::open(&store_path, defaults.clone()).expect("open store");
    let founded = FoundedCore::found(store);
    let request = LockRequest {
        lock_name: "alpha".to_owned(),
        flow_id: "first-flow".to_owned(),
        lock_path_vector: vec![directory.path().join("first").display().to_string()],
        lock_reason: "first".to_owned(),
    };
    let first = founded
        .core()
        .ask(Query::Lock(request))
        .await
        .expect("acquire first")
        .locked();
    assert_eq!(
        founded
            .core()
            .ask(Query::Release(first.lock_id))
            .await
            .expect("release first"),
        Response::Released(first.clone()),
    );
    // The core owns the store, so the restart below is a real one: the file
    // is released when the core stops and not a moment before.
    founded.settled().await;

    let (store, _) = OrchestrateStore::open(&store_path, defaults).expect("reopen store");
    let founded = FoundedCore::found(store);
    let later = founded
        .core()
        .ask(Query::Lock(LockRequest {
            lock_name: "alpha".to_owned(),
            flow_id: "later-flow".to_owned(),
            lock_path_vector: vec![directory.path().join("later").display().to_string()],
            lock_reason: "later".to_owned(),
        }))
        .await
        .expect("acquire later")
        .locked();
    assert_ne!(first.lock_id, later.lock_id);
    assert_eq!(
        founded
            .core()
            .ask(Query::Release(first.lock_id))
            .await
            .expect("reject stale release"),
        Response::ReleaseRejected(ReleaseRejection::UnknownLockId),
    );
}
