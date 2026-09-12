//! The two bound sockets of the Orchestrate Nexus.

pub mod session;
pub mod socket;

use std::{path::PathBuf, sync::Arc};

use signal_orchestrate::OrchestrateNexusConfiguration;
use tokio::{net::UnixListener, sync::oneshot, task::JoinSet};

use crate::{
    core::{Founding, NexusCore},
    store::{OrchestrateStore, StoreError},
};

use session::{Opening, ServingMeta, ServingOrdinary, Session};
use socket::{Bindable, OwnsSocket, SocketAuthority, SocketOwner};

/// The two bound sockets and the core behind them.
pub struct TransportRuntime {
    ordinary: BoundSocket,
    meta: BoundSocket,
    core: Arc<NexusCore>,
}

/// One listening socket, with the authority it carries and the user it
/// belongs to.
struct BoundSocket {
    listener: UnixListener,
    authority: SocketAuthority,
    owner: SocketOwner,
}

/// Binds both sockets of the configured Nexus around the opened store.
pub trait Binding: Sized {
    fn bind(
        configuration: OrchestrateNexusConfiguration,
        store: OrchestrateStore,
    ) -> Result<Self, TransportError>;
}

impl Binding for TransportRuntime {
    fn bind(
        configuration: OrchestrateNexusConfiguration,
        store: OrchestrateStore,
    ) -> Result<Self, TransportError> {
        Ok(Self {
            ordinary: BoundSocket::bind(
                PathBuf::from(configuration.ordinary_socket_path),
                SocketAuthority::Ordinary,
            )?,
            meta: BoundSocket::bind(
                PathBuf::from(configuration.meta_socket_path),
                SocketAuthority::Privileged,
            )?,
            core: NexusCore::found(store),
        })
    }
}

trait BindsOne: Sized {
    fn bind(path: PathBuf, authority: SocketAuthority) -> Result<Self, TransportError>;
}

impl BindsOne for BoundSocket {
    fn bind(path: PathBuf, authority: SocketAuthority) -> Result<Self, TransportError> {
        let listener = path.bind_socket(authority)?;
        Ok(Self {
            listener,
            authority,
            owner: SocketOwner::of(&path)?,
        })
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
            let mut connections = JoinSet::new();
            loop {
                tokio::select! {
                    _ = &mut shutdown => {
                        connections.abort_all();
                        while connections.join_next().await.is_some() {}
                        return Ok(());
                    }
                    accepted = self.ordinary.listener.accept() => {
                        let (stream, _) = accepted?;
                        let core = Arc::clone(&self.core);
                        let mut session = Session::opened(
                            stream, self.ordinary.authority, self.ordinary.owner);
                        connections.spawn(async move {
                            if let Err(error) = session.serve_ordinary(core).await {
                                eprintln!("orchestrate ordinary socket: {error}");
                            }
                        });
                    }
                    accepted = self.meta.listener.accept() => {
                        let (stream, _) = accepted?;
                        let core = Arc::clone(&self.core);
                        let mut session = Session::opened(
                            stream, self.meta.authority, self.meta.owner);
                        connections.spawn(async move {
                            if let Err(error) = session.serve_meta(core).await {
                                eprintln!("orchestrate meta socket: {error}");
                            }
                        });
                    }
                    joined = connections.join_next(), if !connections.is_empty() => {
                        let _ = joined;
                    }
                }
            }
        }
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
}
