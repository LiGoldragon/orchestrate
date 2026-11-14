//! Behavioral contract for the ordinary Lock surface, exercised where the
//! Nexus actually applies it: through Nexus Core.

use signal_orchestrate::{ConfigurationPath, LockPath, MetaSocketPath, OrdinarySocketPath};
use signal_orchestrate::{FlowId, LockId, LockName, LockReason};
use std::path::Path;

use memory_engine::{Engine, EngineOpen, Mutation};
use orchestrate_nexus::{
    OpensStore, OrchestrateStore,
    core::{FoundedCore, Founding},
    store::record::{Familial, SCHEMA_VERSION, StoredAllocator},
};
use signal_orchestrate::{
    Input, Lock, LockOverlap, LockRejection, LockRequest, Observation, ObserveSelection,
    OrchestrateNexusConfiguration, Output, Refusal, ReleaseRejection,
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
            &directory.path().join("orchestrate.memory"),
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
            lock_name: LockName(name.to_owned()),
            flow_id: FlowId("test-flow".to_owned()),
            lock_path_vector: paths
                .iter()
                .map(|path| LockPath((*path).to_owned()))
                .collect(),
            lock_reason: LockReason("behavioral proof".to_owned()),
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
            ordinary_socket_path: OrdinarySocketPath(ConfigurationPath(
                self.join("ordinary.sock").display().to_string(),
            )),
            meta_socket_path: MetaSocketPath(ConfigurationPath(
                self.join("meta.sock").display().to_string(),
            )),
        }
    }
}

trait ReadsResponse {
    fn locked(self) -> Lock;
}

impl ReadsResponse for Output {
    fn locked(self) -> Lock {
        match self {
            Self::Locked(lock) => lock,
            other => panic!("expected Locked response, found {other:?}"),
        }
    }
}

#[tokio::test]
async fn multi_path_locks_are_atomic_complete_and_released_by_durable_id() {
    let fixture = CoreFixture::create();
    let first = fixture.path("first");
    let second = fixture.path("second");
    let acquired = fixture
        .founded
        .core()
        .ask(Input::Lock(
            fixture.lock_request("alpha", &[&first, &second]),
        ))
        .await
        .expect("acquire")
        .locked();

    assert_eq!(acquired.lock_name, LockName("alpha".to_owned()));
    assert_eq!(acquired.flow_id, FlowId("test-flow".to_owned()));
    assert_eq!(
        acquired.lock_path_vector,
        vec![LockPath(first), LockPath(second)]
    );
    assert_eq!(
        acquired.lock_reason,
        LockReason("behavioral proof".to_owned())
    );
    assert_eq!(
        fixture
            .founded
            .core()
            .ask(Input::Release(acquired.lock_id))
            .await
            .expect("release"),
        Output::Released(acquired),
    );
}

#[tokio::test]
async fn duplicate_names_and_overlapping_paths_are_typed_refusals() {
    let fixture = CoreFixture::create();
    let owned = fixture.path("owned");
    let held = fixture
        .founded
        .core()
        .ask(Input::Lock(fixture.lock_request("alpha", &[&owned])))
        .await
        .expect("acquire")
        .locked();

    let elsewhere = fixture.path("elsewhere");
    assert_eq!(
        fixture
            .founded
            .core()
            .ask(Input::Lock(fixture.lock_request("alpha", &[&elsewhere])))
            .await
            .expect("refuse duplicate name"),
        Output::LockRejected(LockRejection::DuplicateName(held.clone())),
    );
    let requested = format!("{owned}/child");
    assert_eq!(
        fixture
            .founded
            .core()
            .ask(Input::Lock(fixture.lock_request("beta", &[&requested])))
            .await
            .expect("refuse overlap"),
        Output::LockRejected(LockRejection::PathOverlap(LockOverlap {
            lock_path: LockPath(requested),
            lock: held,
        })),
    );

    let independent = fixture.path("independent");
    assert!(matches!(
        fixture
            .founded
            .core()
            .ask(Input::Lock(
                fixture.lock_request("gamma", &[&independent, &owned])
            ))
            .await
            .expect("refuse partial overlap"),
        Output::LockRejected(LockRejection::PathOverlap(_)),
    ));
    assert!(matches!(
        fixture
            .founded
            .core()
            .ask(Input::Lock(fixture.lock_request("delta", &[&independent])))
            .await
            .expect("acquire independent"),
        Output::Locked(_),
    ));
}

#[tokio::test]
async fn observe_locks_is_name_then_id_ordered() {
    let fixture = CoreFixture::create();
    let beta_path = fixture.path("beta");
    let beta = fixture
        .founded
        .core()
        .ask(Input::Lock(fixture.lock_request("beta", &[&beta_path])))
        .await
        .expect("acquire beta")
        .locked();
    let alpha_path = fixture.path("alpha");
    let alpha = fixture
        .founded
        .core()
        .ask(Input::Lock(fixture.lock_request("alpha", &[&alpha_path])))
        .await
        .expect("acquire alpha")
        .locked();

    assert_eq!(
        fixture
            .founded
            .core()
            .ask(Input::Observe(ObserveSelection::Locks))
            .await
            .expect("observe"),
        Output::Observed(Observation::Locks(vec![alpha, beta])),
    );
}

#[tokio::test]
async fn released_ids_never_reach_a_later_lock_after_restart() {
    let directory = tempfile::tempdir().expect("isolated Nexus store");
    let store_path = directory.path().join("orchestrate.memory");
    let defaults = directory.path().defaults();
    let (store, _) = OrchestrateStore::open(&store_path, defaults.clone()).expect("open store");
    let founded = FoundedCore::found(store);
    let request = LockRequest {
        lock_name: LockName("alpha".to_owned()),
        flow_id: FlowId("first-flow".to_owned()),
        lock_path_vector: vec![LockPath(
            directory.path().join("first").display().to_string(),
        )],
        lock_reason: LockReason("first".to_owned()),
    };
    let first = founded
        .core()
        .ask(Input::Lock(request))
        .await
        .expect("acquire first")
        .locked();
    assert_eq!(
        founded
            .core()
            .ask(Input::Release(first.lock_id))
            .await
            .expect("release first"),
        Output::Released(first.clone()),
    );
    // The core owns the store, so the restart below is a real one: the file
    // is released when the core stops and not a moment before.
    founded.settled().await;

    let (store, _) = OrchestrateStore::open(&store_path, defaults).expect("reopen store");
    let founded = FoundedCore::found(store);
    let later = founded
        .core()
        .ask(Input::Lock(LockRequest {
            lock_name: LockName("alpha".to_owned()),
            flow_id: FlowId("later-flow".to_owned()),
            lock_path_vector: vec![LockPath(
                directory.path().join("later").display().to_string(),
            )],
            lock_reason: LockReason("later".to_owned()),
        }))
        .await
        .expect("acquire later")
        .locked();
    assert_ne!(first.lock_id, later.lock_id);
    assert_eq!(
        founded
            .core()
            .ask(Input::Release(first.lock_id))
            .await
            .expect("reject stale release"),
        Output::ReleaseRejected(ReleaseRejection::UnknownLockId),
    );
}

/// A Lock request is answered for the request alone: it asks the core and
/// returns the answer, and then shows the core still takes a sound Lock.
trait AnswersLockRequest {
    async fn answer_then_lock(&self, request: LockRequest) -> Output;
}

impl AnswersLockRequest for CoreFixture {
    async fn answer_then_lock(&self, request: LockRequest) -> Output {
        let answer = self
            .founded
            .core()
            .ask(Input::Lock(request))
            .await
            .expect("answered");
        let owned = self.path("owned");
        self.founded
            .core()
            .ask(Input::Lock(self.lock_request("sound", &[&owned])))
            .await
            .expect("the core still answers")
            .locked();
        answer
    }
}

/// A fault in a request's own paths is that request's rejection, named in
/// `LockRejection`; it is no store failure.
#[tokio::test]
async fn a_lock_naming_no_path_is_rejected_no_path() {
    let fixture = CoreFixture::create();
    assert_eq!(
        fixture
            .answer_then_lock(fixture.lock_request("empty", &[]))
            .await,
        Output::LockRejected(LockRejection::NoPath),
    );
}

#[tokio::test]
async fn a_relative_lock_path_is_rejected_relative_path() {
    let fixture = CoreFixture::create();
    assert_eq!(
        fixture
            .answer_then_lock(fixture.lock_request("relative", &["relative/path"]))
            .await,
        Output::LockRejected(LockRejection::RelativePath(LockPath(
            "relative/path".to_owned()
        ))),
    );
}

#[tokio::test]
async fn a_lock_path_with_a_parent_component_is_rejected_parent_component() {
    let fixture = CoreFixture::create();
    let climbing = format!("{}/../elsewhere", fixture.path("inner"));
    assert_eq!(
        fixture
            .answer_then_lock(fixture.lock_request("climbing", &[&climbing]))
            .await,
        Output::LockRejected(LockRejection::ParentComponent(LockPath(climbing))),
    );
}

#[tokio::test]
async fn a_lock_path_with_nul_is_rejected_nul_character() {
    let fixture = CoreFixture::create();
    let phantom = format!("{}\0phantom", fixture.path("held"));
    assert_eq!(
        fixture
            .answer_then_lock(fixture.lock_request("phantom", &[&phantom]))
            .await,
        Output::LockRejected(LockRejection::NulCharacter(LockPath(phantom))),
    );
}

#[tokio::test]
async fn a_lock_path_repeated_once_normalized_is_rejected_duplicate_path() {
    let fixture = CoreFixture::create();
    let repeated = fixture.path("repeated");
    let spelled_again = format!("{repeated}/.");
    assert_eq!(
        fixture
            .answer_then_lock(fixture.lock_request("repeated", &[&repeated, &spelled_again]))
            .await,
        Output::LockRejected(LockRejection::DuplicatePath(LockPath(repeated))),
    );
}

/// What the store cannot carry out is answered in the contract's own words,
/// `Refused.Store`, and the core goes on answering. The store here cannot
/// number another Lock: its allocator stands at the last Lock id there is.
#[tokio::test]
async fn a_request_the_store_refuses_is_answered_refused_store() {
    let directory = tempfile::tempdir().expect("isolated Nexus store");
    let store_path = directory.path().join("orchestrate.memory");
    let defaults = directory.path().defaults();
    drop(OrchestrateStore::open(&store_path, defaults.clone()).expect("open isolated store"));
    {
        let mut engine = Engine::open(EngineOpen::new(
            store_path.display().to_string(),
            SCHEMA_VERSION,
        ))
        .expect("open the store's engine");
        let allocator = engine
            .register_table(StoredAllocator::descriptor())
            .expect("register the allocator family");
        engine
            .mutate(Mutation::new(
                allocator,
                StoredAllocator {
                    lock_id: LockId(i64::MAX),
                },
            ))
            .expect("set the allocator at the last Lock id");
    }
    let (store, _) = OrchestrateStore::open(&store_path, defaults).expect("reopen store");
    let fixture = CoreFixture {
        directory,
        founded: FoundedCore::found(store),
    };
    let owned = fixture.path("owned");
    assert_eq!(
        fixture
            .founded
            .core()
            .ask(Input::Lock(fixture.lock_request("alpha", &[&owned])))
            .await
            .expect("answered"),
        Output::Refused(Refusal::Store(
            "the durable Lock ID allocator is exhausted".to_owned()
        )),
    );
    assert_eq!(
        fixture
            .founded
            .core()
            .ask(Input::Observe(ObserveSelection::Locks))
            .await
            .expect("the core still answers"),
        Output::Observed(Observation::Locks(Vec::new())),
    );
}
