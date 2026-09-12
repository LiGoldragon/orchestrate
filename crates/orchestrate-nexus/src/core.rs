//! Nexus Core: the one owner of durable state, and the change stream every
//! subscriber follows.
//!
//! An object enters here for the effect; the response follows as an effect of
//! it. That is what [`Applies`] names, once per contract this Nexus speaks.
//!
//! Locks change only through `Applies`, so the core is also the only place
//! that can announce a change. It announces the whole observation rather than
//! a delta: a subscriber that joins mid-stream and one that has followed from
//! the start then hold the same value, and no subscriber has to reassemble
//! state from fragments.
//!
//! Serialization is `Arc<Mutex<_>>`, which Vision permits while the Kameo
//! standards are undesigned. The boundary is drawn so that becoming an actor
//! is a change of this file only: nothing outside it touches the store.

use std::sync::Arc;

use meta_signal_orchestrate::{Query as MetaQuery, Response as MetaResponse};
use signal_orchestrate::{Observation, Query as OrdinaryQuery, Response as OrdinaryResponse};
use tokio::sync::{Mutex, broadcast};

use crate::{
    configuration::AnswersMeta,
    ordinary::{Locks, Observes, Releases},
    store::{Configures, OrchestrateStore, StoreError},
};

/// How many announced observations a subscriber may fall behind before the
/// core stops keeping them. A subscriber that lags past this is re-sent the
/// current state instead, which is the same value it would have converged on.
const ANNOUNCEMENT_BACKLOG: usize = 64;

/// The decision-making engine of the Orchestrate Nexus.
pub struct NexusCore {
    store: Mutex<OrchestrateStore>,
    announcements: broadcast::Sender<Observation>,
}

/// Builds the core around the one opened store.
pub trait Founding {
    fn found(store: OrchestrateStore) -> Arc<Self>;
}

impl Founding for NexusCore {
    fn found(store: OrchestrateStore) -> Arc<Self> {
        let (announcements, _) = broadcast::channel(ANNOUNCEMENT_BACKLOG);
        Arc::new(Self {
            store: Mutex::new(store),
            announcements,
        })
    }
}

/// One contract's objects enter the core and their responses follow.
pub trait Applies<Entering> {
    type Effect;

    fn apply(
        &self,
        entering: Entering,
    ) -> impl std::future::Future<Output = Result<Self::Effect, StoreError>> + Send;
}

impl Applies<OrdinaryQuery> for NexusCore {
    type Effect = OrdinaryResponse;

    // Written as a manual future rather than `async fn`: the returned future
    // must be `Send` so a connection task can hold it.
    #[allow(clippy::manual_async_fn)]
    fn apply(
        &self,
        entering: OrdinaryQuery,
    ) -> impl std::future::Future<Output = Result<OrdinaryResponse, StoreError>> + Send {
        async move {
            let mut store = self.store.lock().await;
            let response = match entering {
                OrdinaryQuery::Configure(configuration) => {
                    store.ordinary_configure(configuration)?
                }
                OrdinaryQuery::Lock(request) => store.lock(request)?,
                OrdinaryQuery::Release(lock_id) => store.release(lock_id)?,
                OrdinaryQuery::Observe(selection) => {
                    OrdinaryResponse::Observed(store.observe(selection)?)
                }
            };
            if matches!(
                response,
                OrdinaryResponse::Locked(_) | OrdinaryResponse::Released(_)
            ) {
                let _ = self
                    .announcements
                    .send(store.observe(signal_orchestrate::ObserveSelection::Locks)?);
            }
            Ok(response)
        }
    }
}

impl Applies<MetaQuery> for NexusCore {
    type Effect = MetaResponse;

    #[allow(clippy::manual_async_fn)]
    fn apply(
        &self,
        entering: MetaQuery,
    ) -> impl std::future::Future<Output = Result<MetaResponse, StoreError>> + Send {
        async move {
            let mut store = self.store.lock().await;
            let outcome = match entering {
                MetaQuery::Configure(configuration) => store.meta_configure(configuration)?,
                MetaQuery::ReverseMetaConfiguration => store.reverse_meta_configuration()?,
            };
            Ok(outcome.meta_response())
        }
    }
}

/// State is observed by subscription: the state on open, then each change.
pub trait Announcing {
    fn opening_observation(
        &self,
        selection: &signal_orchestrate::ObserveSelection,
    ) -> impl std::future::Future<Output = Result<Observation, StoreError>> + Send;

    fn announcements(&self) -> broadcast::Receiver<Observation>;
}

impl Announcing for NexusCore {
    #[allow(clippy::manual_async_fn)]
    fn opening_observation(
        &self,
        selection: &signal_orchestrate::ObserveSelection,
    ) -> impl std::future::Future<Output = Result<Observation, StoreError>> + Send {
        let selection = selection.clone();
        async move { self.store.lock().await.observe(selection) }
    }

    fn announcements(&self) -> broadcast::Receiver<Observation> {
        self.announcements.subscribe()
    }
}
