//! The two bound sockets of the Orchestrate Nexus.
//!
//! A listener is a task; a connection is a task; neither is an actor and
//! neither holds domain state. Both hold a reference to the one actor that
//! does. A session that fails ends its own connection and is recorded against
//! the socket it arrived on; the listener keeps listening.

pub mod session;
pub mod socket;

use std::path::{Path, PathBuf};

use kameo::error::SendError;
use nexus::SocketAuthority;
use signal_orchestrate::OrchestrateNexusConfiguration;
use tokio::{
    net::UnixListener,
    signal::unix::{SignalKind, signal},
    sync::oneshot,
    task::JoinSet,
};

use crate::{
    core::{FoundedCore, Founding},
    store::{OrchestrateStore, Situates, StoreError},
};

use session::{Opening, ServingMeta, ServingOrdinary, Session};
use socket::{Claiming, OwnsSocket, SocketClaim, SocketOwner};

/// The two bound sockets and the core behind them.
pub struct TransportRuntime {
    ordinary: BoundSocket,
    meta: BoundSocket,
    core: FoundedCore,
}

/// One listening socket, with the authority it carries, the user it belongs
/// to, and the claim that says this process owns the path.
struct BoundSocket {
    listener: UnixListener,
    authority: SocketAuthority,
    owner: SocketOwner,
    /// Held for the life of the runtime. Dropping it hands the path back.
    claim: SocketClaim,
}

/// Binds both sockets of the configured Nexus around the opened store.
pub trait Binding: Sized {
    fn bind(
        configuration: OrchestrateNexusConfiguration,
        store: OrchestrateStore,
    ) -> Result<Self, TransportError>;
}

impl Binding for TransportRuntime {
    /// Both paths are claimed before either is bound, so a Nexus that loses
    /// the race for its second socket has not already taken the first one
    /// away from whoever won it. The store learns where it ended up only once
    /// both binds have succeeded, and only then does anything own it.
    fn bind(
        configuration: OrchestrateNexusConfiguration,
        mut store: OrchestrateStore,
    ) -> Result<Self, TransportError> {
        let ordinary_path = PathBuf::from(configuration.ordinary_socket_path);
        let meta_path = PathBuf::from(configuration.meta_socket_path);
        let ordinary_claim = SocketClaim::claim(&ordinary_path)?;
        let meta_claim = SocketClaim::claim(&meta_path)?;
        let ordinary = BoundSocket::bound(ordinary_claim, SocketAuthority::Ordinary)?;
        let meta = BoundSocket::bound(meta_claim, SocketAuthority::Privileged)?;
        store.situate(vec![
            ordinary_path.display().to_string(),
            meta_path.display().to_string(),
        ])?;
        Ok(Self {
            ordinary,
            meta,
            core: FoundedCore::found(store),
        })
    }
}

trait Bounds: Sized {
    fn bound(claim: SocketClaim, authority: SocketAuthority) -> Result<Self, TransportError>;
    fn socket_path(&self) -> &Path;
}

impl Bounds for BoundSocket {
    fn bound(claim: SocketClaim, authority: SocketAuthority) -> Result<Self, TransportError> {
        let listener = claim.bind_listener(authority)?;
        let owner = SocketOwner::of(claim.socket_path())?;
        Ok(Self {
            listener,
            authority,
            owner,
            claim,
        })
    }

    fn socket_path(&self) -> &Path {
        self.claim.socket_path()
    }
}

/// A session that ended badly.
///
/// It is the listener's to record, because the session is gone by the time
/// anything can be said about it. Which socket it arrived on is half of what
/// is worth saying: the same failure means one thing on the ordinary socket
/// and another on the Nexus's root. A session that panicked cannot name its
/// socket — the value carrying that name went with it — and says so rather
/// than guessing.
struct SessionEnding {
    socket_path: Option<PathBuf>,
    ending: String,
}

trait Recording {
    fn on(socket_path: &Path, error: &TransportError) -> Self;
    fn recorded(&self);
}

impl Recording for SessionEnding {
    fn on(socket_path: &Path, error: &TransportError) -> Self {
        Self {
            socket_path: Some(socket_path.to_path_buf()),
            ending: error.to_string(),
        }
    }

    fn recorded(&self) {
        match &self.socket_path {
            Some(socket_path) => eprintln!(
                "orchestrate-nexus: session on {socket_path:?}: {}",
                self.ending
            ),
            None => eprintln!("orchestrate-nexus: a session ended: {}", self.ending),
        }
    }
}

/// The two ways a service manager asks a daemon to go.
///
/// Both mean the same thing to a Nexus — stop taking work and finish what you
/// took — so both arrive as the one shutdown a `TransportRuntime` waits on. A
/// daemon that ignored them would be stopped only by `SIGKILL`, which is the
/// case its graceful path exists to avoid.
pub struct Termination;

pub trait StopsOnSignal {
    fn asked() -> Result<oneshot::Receiver<()>, TransportError>;
}

impl StopsOnSignal for Termination {
    fn asked() -> Result<oneshot::Receiver<()>, TransportError> {
        let mut terminated = signal(SignalKind::terminate())?;
        let mut interrupted = signal(SignalKind::interrupt())?;
        let (asked, shutdown) = oneshot::channel();
        tokio::spawn(async move {
            tokio::select! {
                _ = terminated.recv() => {}
                _ = interrupted.recv() => {}
            }
            let _ = asked.send(());
        });
        Ok(shutdown)
    }
}

/// Serves both sockets until asked to stop.
pub trait Serving {
    fn serve_until(
        self,
        shutdown: oneshot::Receiver<()>,
    ) -> impl std::future::Future<Output = Result<(), TransportError>> + Send;
}

impl Serving for TransportRuntime {
    #[allow(clippy::manual_async_fn)]
    fn serve_until(
        self,
        mut shutdown: oneshot::Receiver<()>,
    ) -> impl std::future::Future<Output = Result<(), TransportError>> + Send {
        async move {
            let mut connections: JoinSet<Option<SessionEnding>> = JoinSet::new();
            loop {
                tokio::select! {
                    _ = &mut shutdown => {
                        // Sessions are dropped first so that how long this
                        // takes to return depends on nothing a peer does: an
                        // open subscription and a peer that connected without
                        // speaking would both otherwise hold it. The cost is
                        // that a peer mid-request may lose a reply for work
                        // the core has already committed, which is a reply it
                        // can reconstruct by asking again.
                        //
                        // Then the core, gracefully and waited on, so that by
                        // the time this returns the store is closed and both
                        // socket claims are the next process's to take.
                        connections.abort_all();
                        while connections.join_next().await.is_some() {}
                        self.core.settled().await;
                        return Ok(());
                    }
                    accepted = self.ordinary.listener.accept() => {
                        let (stream, _) = accepted?;
                        let core = self.core.core().clone();
                        let socket_path = self.ordinary.socket_path().to_path_buf();
                        let mut session = Session::opened(
                            stream, self.ordinary.authority, self.ordinary.owner);
                        connections.spawn(async move {
                            session.serve_ordinary(core).await.err()
                                .map(|error| SessionEnding::on(&socket_path, &error))
                        });
                    }
                    accepted = self.meta.listener.accept() => {
                        let (stream, _) = accepted?;
                        let core = self.core.core().clone();
                        let socket_path = self.meta.socket_path().to_path_buf();
                        let mut session = Session::opened(
                            stream, self.meta.authority, self.meta.owner);
                        connections.spawn(async move {
                            session.serve_meta(core).await.err()
                                .map(|error| SessionEnding::on(&socket_path, &error))
                        });
                    }
                    joined = connections.join_next(), if !connections.is_empty() => {
                        match joined {
                            Some(Ok(Some(ending))) => ending.recorded(),
                            Some(Err(panicked)) => SessionEnding {
                                socket_path: None,
                                ending: panicked.to_string(),
                            }
                            .recorded(),
                            Some(Ok(None)) | None => {}
                        }
                    }
                }
            }
        }
    }
}

/// A request the core never turned into an effect.
///
/// The core is reached by message, so "the store refused" and "the core was
/// not there to ask" are different answers and must not collapse into one.
pub(crate) trait Delivered<Effect> {
    fn delivered(self) -> Result<Effect, TransportError>;
}

impl<Effect, Entering> Delivered<Effect> for Result<Effect, SendError<Entering, StoreError>> {
    fn delivered(self) -> Result<Effect, TransportError> {
        self.map_err(|refusal| match refusal {
            SendError::HandlerError(store) => TransportError::Store(store),
            SendError::MailboxFull(_) => TransportError::CoreOverloaded,
            SendError::Timeout(_) => TransportError::CoreOverloaded,
            SendError::ActorNotRunning(_)
            | SendError::ActorStopped
            | SendError::ActorRestarting(_) => TransportError::CoreStopped,
        })
    }
}

#[derive(Debug, thiserror::Error)]
pub enum TransportError {
    #[error("Unix socket I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("Signal frame: {0}")]
    Frame(#[from] signal::FrameError),
    #[error("an Orchestrate Nexus already owns socket {0:?}")]
    SocketAlreadyActive(PathBuf),
    #[error("configured socket path has no parent: {0:?}")]
    MissingSocketParent(PathBuf),
    #[error("Signal archive validation failed")]
    Archive,
    #[error("store failed: {0}")]
    Store(#[from] StoreError),
    #[error("the Nexus core has stopped")]
    CoreStopped,
    #[error("the Nexus core is behind; the request was not taken")]
    CoreOverloaded,
}
