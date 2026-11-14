//! Nexus Core: the one owner of durable state, and the change stream every
//! subscriber follows.
//!
//! The core is an actor, and it is the only actor in the Nexus. What an actor
//! is for is owning exclusive mutable state and serialising access to it, so
//! the boundary is drawn at the state: one lock table is one unit of
//! consistency, so it gets one actor, and that actor owns the store outright.
//! Nothing else in the process can reach it — not through a mutex, not
//! through a reference; the only way in is a message.
//!
//! Sockets are not actors and sessions are not actors. A listener is a task
//! and a connection is a task holding a reference to this one actor, so a
//! session that fails cannot take the listener down with it and a second
//! socket is a second door onto the same state rather than a second owner of
//! it. What separates the two doors is not the actor: it is which message
//! type the session on the other side of them can construct.
//!
//! An object enters here for the effect, and the response follows as an
//! effect of it — which is what Kameo's `Message` already names, so this file
//! does not name it twice.
//!
//! The mailbox is bounded, so a peer that outruns the store is made to wait
//! rather than allowed to queue without limit. Announcements
//! go out through a broadcast channel, which never makes the core wait on a
//! subscriber: one that falls too far behind has its exchange ended with
//! signal's `Lagged`, and opens `Observe` again for the state on open.

use kameo::{
    Actor,
    actor::{ActorRef, PreparedActor},
    error::{ActorStopReason, PanicError},
    mailbox,
    message::{Context, Message},
};
use meta_signal_orchestrate::{
    Answerable as AnswerableMeta, Input as MetaInput, Output as MetaOutput,
};
use signal_orchestrate::{
    Answerable as AnswerableOrdinary, ConfigurationReceipt, Input as OrdinaryInput, Lock, LockId,
    LockRequest, Observation, ObserveSelection, OrchestrateNexusConfiguration,
    Output as OrdinaryOutput, Refusal,
};
use std::{cell::RefCell, convert::Infallible};
use tokio::sync::broadcast;

use crate::{
    ordinary::{Locks, Observes, Releases},
    store::{Configures, OrchestrateStore, StoreError},
};

/// How many announced observations a subscriber may fall behind before the
/// core stops keeping them. A subscriber that lags past this has its exchange
/// ended `Lagged`.
const ANNOUNCEMENT_BACKLOG: usize = 64;

/// How many requests may wait for the core before a peer is made to wait for
/// the mailbox. Bounded on purpose: the core is the throughput of the durable
/// store, and an unbounded queue in front of it would turn a slow disk into
/// unbounded memory instead of into the backpressure it actually is.
const MAILBOX_CAPACITY: usize = 64;

/// The one core of a Nexus, and the task that owns it.
///
/// The two are a pair because the store is inside the actor: the core is
/// reachable while the task runs, and the store it holds is released when
/// that task ends and not before. Anything that has to open the store again —
/// a restart, or a second process — waits on `ended`, never merely on the
/// reference going away.
pub struct FoundedCore {
    core: ActorRef<NexusCore>,
    ended: tokio::task::JoinHandle<Result<(NexusCore, ActorStopReason), PanicError>>,
}

/// Founds the core around the one opened store.
pub trait Founding: Sized {
    fn found(store: OrchestrateStore) -> Self;
    /// The reference every session holds. Cheap to clone, and the only way
    /// anything reaches the store.
    fn core(&self) -> &ActorRef<NexusCore>;
    /// Stops the core and waits for it to let the store go.
    fn settled(self) -> impl std::future::Future<Output = ()> + Send;
}

impl Founding for FoundedCore {
    fn found(store: OrchestrateStore) -> Self {
        let prepared: PreparedActor<NexusCore> =
            PreparedActor::new(mailbox::bounded(MAILBOX_CAPACITY));
        let core = prepared.actor_ref().clone();
        Self {
            core,
            ended: prepared.spawn(store),
        }
    }

    fn core(&self) -> &ActorRef<NexusCore> {
        &self.core
    }

    /// A graceful stop rather than a kill: a request already in the mailbox
    /// is a request a peer is still waiting on, and a Lock already accepted
    /// is one a flow believes it holds.
    async fn settled(self) {
        let _ = self.core.stop_gracefully().await;
        let _ = self.ended.await;
    }
}

/// The decision-making engine of the Orchestrate Nexus.
pub struct NexusCore {
    store: OrchestrateStore,
    announcements: broadcast::Sender<Announcement>,
}

/// There is no `on_stop` here, and that is a statement rather than an
/// omission: every durable transition commits before it answers, so a core
/// that has answered has already written and there is nothing left to flush.
/// What stopping does is drop the store and close the announcement channel,
/// which is how an open subscription learns its Nexus is gone.
impl Actor for NexusCore {
    type Args = OrchestrateStore;
    type Error = StoreError;

    /// The store is opened before the actor exists and handed over here, so
    /// the actor's first instant is also the first instant anything owns it.
    async fn on_start(store: Self::Args, _: ActorRef<Self>) -> Result<Self, Self::Error> {
        let (announcements, _) = broadcast::channel(ANNOUNCEMENT_BACKLOG);
        Ok(Self {
            store,
            announcements,
        })
    }
}

/// Each input is answered through its contract's generated dispatch,
/// `input.answer(&answerer)`, and the output it yields is the whole answer: a
/// store that could not carry the input out answers `Refused.Store` within
/// it, so the reply cannot fail (`Infallible`). A Lock taken or released is
/// then announced to every subscriber.
impl Message<OrdinaryInput> for NexusCore {
    type Reply = Result<OrdinaryOutput, Infallible>;

    async fn handle(
        &mut self,
        entering: OrdinaryInput,
        _: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let output = entering.answer(&Answerer::over(&mut self.store));
        if matches!(
            output,
            OrdinaryOutput::Locked(_) | OrdinaryOutput::Released(_)
        ) {
            self.announce();
        }
        Ok(output)
    }
}

impl Message<MetaInput> for NexusCore {
    type Reply = Result<MetaOutput, Infallible>;

    async fn handle(
        &mut self,
        entering: MetaInput,
        _: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        Ok(entering.answer(&Answerer::over(&mut self.store)))
    }
}

/// Tells every subscriber what the store holds after a change.
trait Announces {
    fn announce(&mut self);
}

impl Announces for NexusCore {
    /// A change whose state cannot be read back is announced as unkept:
    /// every subscriber's exchange then ends `Lagged`, as one that fell
    /// behind does, and opening `Observe` again gives the state on open.
    fn announce(&mut self) {
        let announcement = match self.store.observe(ObserveSelection::Locks) {
            Ok(observation) => Announcement::Changed(observation),
            Err(_) => Announcement::Unkept,
        };
        let _ = self.announcements.send(announcement);
    }
}

/// What the change stream carries to every subscriber.
#[derive(Clone, Debug)]
pub enum Announcement {
    /// The state after a change.
    Changed(Observation),
    /// A change whose state could not be read; no subscriber can be kept
    /// current by the stream past it.
    Unkept,
}

/// One input being answered against the store the core owns.
///
/// The generated `Answering` lends each capability `&self`, so the store is
/// reached through a cell. A capability answers with its contract's own
/// output: what the input's output carries, a refusal the transition chose,
/// or, when the store could not carry it out, `Refused.Store`.
struct Answerer<'core> {
    store: RefCell<&'core mut OrchestrateStore>,
}

trait Answers<'core>: Sized {
    fn over(store: &'core mut OrchestrateStore) -> Self;
    /// Runs one store transition; a store failure is the contract's refusal.
    #[allow(clippy::result_large_err)]
    fn attempt<Carried, Answered: RefusedByStore>(
        &self,
        transition: impl FnOnce(&mut OrchestrateStore) -> Result<Result<Carried, Answered>, StoreError>,
    ) -> Result<Carried, Answered>;
}

impl<'core> Answers<'core> for Answerer<'core> {
    fn over(store: &'core mut OrchestrateStore) -> Self {
        Self {
            store: RefCell::new(store),
        }
    }

    fn attempt<Carried, Answered: RefusedByStore>(
        &self,
        transition: impl FnOnce(&mut OrchestrateStore) -> Result<Result<Carried, Answered>, StoreError>,
    ) -> Result<Carried, Answered> {
        transition(&mut self.store.borrow_mut())
            .unwrap_or_else(|failure| Err(Answered::refused_by_store(failure)))
    }
}

/// A contract's output for an input its store could not carry out:
/// `Refused.Store`, in the store's own words.
pub trait RefusedByStore {
    fn refused_by_store(failure: StoreError) -> Self;
}

impl RefusedByStore for OrdinaryOutput {
    fn refused_by_store(failure: StoreError) -> Self {
        Self::Refused(Refusal::Store(failure.to_string()))
    }
}

impl RefusedByStore for MetaOutput {
    fn refused_by_store(failure: StoreError) -> Self {
        Self::Refused(Refusal::Store(failure.to_string()))
    }
}

impl signal_orchestrate::Answering for Answerer<'_> {
    fn configure(
        &self,
        orchestrate_nexus_configuration: &OrchestrateNexusConfiguration,
    ) -> Result<ConfigurationReceipt, OrdinaryOutput> {
        self.attempt(|store| store.ordinary_configure(orchestrate_nexus_configuration.clone()))
    }

    fn lock(&self, lock_request: &LockRequest) -> Result<Lock, OrdinaryOutput> {
        self.attempt(|store| store.lock(lock_request.clone()))
    }

    fn release(&self, lock_id: &LockId) -> Result<Lock, OrdinaryOutput> {
        self.attempt(|store| store.release(*lock_id))
    }

    fn observe(&self, observe_selection: &ObserveSelection) -> Result<Observation, OrdinaryOutput> {
        self.attempt(|store| store.observe(observe_selection.clone()).map(Ok))
    }
}

impl meta_signal_orchestrate::Answering for Answerer<'_> {
    fn configure(
        &self,
        orchestrate_nexus_configuration: &OrchestrateNexusConfiguration,
    ) -> Result<ConfigurationReceipt, MetaOutput> {
        self.attempt(|store| store.meta_configure(orchestrate_nexus_configuration.clone()))
    }

    fn reverse_meta_configuration(&self) -> Result<ConfigurationReceipt, MetaOutput> {
        self.attempt(|store| store.reverse_meta_configuration().map(Ok))
    }
}

/// A peer joining the change stream.
#[derive(Clone, Debug, PartialEq)]
pub struct Attending {
    pub selection: ObserveSelection,
}

/// What that peer is given: the state on open, and every change after it.
///
/// Both halves are taken in one step of the core, so nothing can commit
/// between the two. A subscriber therefore never misses a change and never
/// sees the same one twice — which two separate acquisitions could not
/// promise, and did not.
pub struct Attendance {
    pub opening: Observation,
    pub announcements: broadcast::Receiver<Announcement>,
}

/// A store that cannot read the state on open refuses the subscriber with
/// `Refused.Store`, and nothing joins the stream.
impl Message<Attending> for NexusCore {
    type Reply = Result<Attendance, OrdinaryOutput>;

    async fn handle(
        &mut self,
        attending: Attending,
        _: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        Ok(Attendance {
            opening: self
                .store
                .observe(attending.selection)
                .map_err(OrdinaryOutput::refused_by_store)?,
            announcements: self.announcements.subscribe(),
        })
    }
}
