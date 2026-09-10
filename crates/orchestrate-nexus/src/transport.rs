//! Length-prefixed rkyv transport for the ordinary and meta Signal contracts.

use std::{
    fs,
    io::ErrorKind,
    os::unix::net::UnixStream as StandardUnixStream,
    path::{Path, PathBuf},
    sync::Arc,
};

use meta_signal_orchestrate::{
    ByteViewable as MetaByteViewable, Configure, Query as MetaQuery, Response as MetaResponse,
    Restorable as MetaRestorable, Signal as MetaSignal, Signalizable as MetaSignalizable,
};
use signal_orchestrate::{
    ByteViewable as OrdinaryByteViewable, Query as OrdinaryQuery, Restorable as OrdinaryRestorable,
    Signal as OrdinarySignal, Signalizable as OrdinarySignalizable,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{UnixListener, UnixStream},
    sync::{Mutex, oneshot},
    task::JoinSet,
};

use crate::{HandlesMeta, HandlesOrdinary, OrchestrateStore, ordinary::OrdinaryOutcome};

const MAXIMUM_SIGNAL_BYTES: usize = 8 * 1024 * 1024;

/// The two bound sockets and the one serialized store owner behind them.
pub struct TransportRuntime {
    ordinary: UnixListener,
    meta: UnixListener,
    store: Arc<Mutex<OrchestrateStore>>,
}

pub trait TransportBinding: Sized {
    fn bind(configure: Configure, store: OrchestrateStore) -> Result<Self, TransportError>;
}

impl TransportBinding for TransportRuntime {
    fn bind(configure: Configure, store: OrchestrateStore) -> Result<Self, TransportError> {
        let ordinary_path = Path::new(&configure.ordinary_socket_path);
        let meta_path = Path::new(&configure.meta_socket_path);
        ordinary_path.prepare_socket()?;
        meta_path.prepare_socket()?;
        Ok(Self {
            ordinary: UnixListener::bind(ordinary_path)?,
            meta: UnixListener::bind(meta_path)?,
            store: Arc::new(Mutex::new(store)),
        })
    }
}

trait SocketPreparing {
    fn prepare_socket(&self) -> Result<(), TransportError>;
}

impl SocketPreparing for Path {
    fn prepare_socket(&self) -> Result<(), TransportError> {
        let parent = self
            .parent()
            .ok_or_else(|| TransportError::MissingSocketParent(self.to_path_buf()))?;
        fs::create_dir_all(parent)?;
        match StandardUnixStream::connect(self) {
            Ok(_) => Err(TransportError::SocketAlreadyActive(self.to_path_buf())),
            Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
            Err(error) if error.kind() == ErrorKind::ConnectionRefused => {
                fs::remove_file(self)?;
                Ok(())
            }
            Err(error) => Err(error.into()),
        }
    }
}

pub trait TransportServing {
    fn serve_until(
        self,
        shutdown: oneshot::Receiver<()>,
    ) -> impl std::future::Future<Output = Result<(), TransportError>> + Send;
}

impl TransportServing for TransportRuntime {
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
                    accepted = self.ordinary.accept() => {
                        let (stream, _) = accepted?;
                        let store = Arc::clone(&self.store);
                        connections.spawn(async move {
                            if let Err(error) = (OrdinarySocket { stream }).serve(store).await {
                                eprintln!("orchestrate ordinary socket: {error}");
                            }
                        });
                    }
                    accepted = self.meta.accept() => {
                        let (stream, _) = accepted?;
                        let store = Arc::clone(&self.store);
                        connections.spawn(async move {
                            if let Err(error) = (MetaSocket { stream }).serve(store).await {
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

struct SignalPayload {
    bytes: Vec<u8>,
}

trait PayloadReading {
    async fn read_from(stream: &mut UnixStream) -> Result<Self, TransportError>
    where
        Self: Sized;
}

impl PayloadReading for SignalPayload {
    async fn read_from(stream: &mut UnixStream) -> Result<Self, TransportError> {
        let mut prefix = [0; 4];
        stream.read_exact(&mut prefix).await?;
        let length = u32::from_le_bytes(prefix) as usize;
        if length > MAXIMUM_SIGNAL_BYTES {
            return Err(TransportError::FrameTooLarge(length));
        }
        let mut bytes = vec![0; length];
        stream.read_exact(&mut bytes).await?;
        Ok(Self { bytes })
    }
}

trait PayloadWriting {
    async fn write_to(&self, stream: &mut UnixStream) -> Result<(), TransportError>;
}

impl PayloadWriting for SignalPayload {
    async fn write_to(&self, stream: &mut UnixStream) -> Result<(), TransportError> {
        if self.bytes.len() > MAXIMUM_SIGNAL_BYTES {
            return Err(TransportError::FrameTooLarge(self.bytes.len()));
        }
        let length = u32::try_from(self.bytes.len())
            .map_err(|_| TransportError::FrameTooLarge(self.bytes.len()))?;
        stream.write_all(&length.to_le_bytes()).await?;
        stream.write_all(&self.bytes).await?;
        stream.flush().await?;
        Ok(())
    }
}

struct OrdinarySocket {
    stream: UnixStream,
}

trait OrdinaryServing {
    async fn serve(&mut self, store: Arc<Mutex<OrchestrateStore>>) -> Result<(), TransportError>;
}

impl OrdinaryServing for OrdinarySocket {
    async fn serve(&mut self, store: Arc<Mutex<OrchestrateStore>>) -> Result<(), TransportError> {
        let payload = SignalPayload::read_from(&mut self.stream).await?;
        let query = OrdinarySignal::<OrdinaryQuery>::from(payload.bytes)
            .restore()
            .map_err(|_| TransportError::Archive)?;
        let OrdinaryOutcome::Response(response) = store.lock().await.ordinary(query)?;
        let signal = response.signalize().map_err(|_| TransportError::Archive)?;
        let bytes = signal.bytes().to_vec();
        SignalPayload { bytes }.write_to(&mut self.stream).await
    }
}

struct MetaSocket {
    stream: UnixStream,
}

trait MetaServing {
    async fn serve(&mut self, store: Arc<Mutex<OrchestrateStore>>) -> Result<(), TransportError>;
}

impl MetaServing for MetaSocket {
    async fn serve(&mut self, store: Arc<Mutex<OrchestrateStore>>) -> Result<(), TransportError> {
        let payload = SignalPayload::read_from(&mut self.stream).await?;
        let query = MetaSignal::<MetaQuery>::from(payload.bytes)
            .restore()
            .map_err(|_| TransportError::Archive)?;
        let response: MetaResponse = store.lock().await.meta(query)?;
        let signal = response.signalize().map_err(|_| TransportError::Archive)?;
        let bytes = signal.bytes().to_vec();
        SignalPayload { bytes }.write_to(&mut self.stream).await
    }
}

#[derive(Debug, thiserror::Error)]
pub enum TransportError {
    #[error("Unix socket I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("an Orchestrate Nexus already owns socket {0:?}")]
    SocketAlreadyActive(PathBuf),
    #[error("configured socket path has no parent: {0:?}")]
    MissingSocketParent(PathBuf),
    #[error("Signal archive validation failed")]
    Archive,
    #[error("Signal payload exceeds the 8 MiB limit: {0} bytes")]
    FrameTooLarge(usize),
    #[error("store failed: {0}")]
    Store(#[from] crate::store::StoreError),
}
