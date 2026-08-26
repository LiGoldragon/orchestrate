//! Unix transport for the generated ordinary and meta Signal contracts.
//!
//! The socket carries exactly one length-prefixed, generated Signal frame for
//! each connection.  Contract and archive validation happen before a request
//! reaches the Nexus-owned store; the transport only preserves the frame
//! exchange, route, and exchange identifier.

use std::{
    fs,
    io::ErrorKind,
    os::unix::net::UnixStream as StandardUnixStream,
    path::{Path, PathBuf},
    sync::Arc,
};

use meta_signal_orchestrate::{
    Configure, Frame as MetaFrame, FrameBody as MetaFrameBody, MetaOrchestrateReply,
    MetaOrchestrateRequest,
};
use signal_frame::{
    ExchangeFrameBody, ExchangeIdentifier, NonEmpty, OperationDispatchError, Reply, Request,
    SubReply,
};
use signal_frame_ordinary::{
    ExchangeFrameBody as OrdinaryExchangeFrameBody,
    ExchangeIdentifier as OrdinaryExchangeIdentifier, NonEmpty as OrdinaryNonEmpty,
    OperationDispatchError as OrdinaryOperationDispatchError, Reply as OrdinaryReply,
    Request as OrdinaryRequest, SubReply as OrdinarySubReply,
};
use signal_orchestrate::{
    Frame as OrdinaryFrame, FrameBody as OrdinaryFrameBody, OrchestrateReply, OrchestrateRequest,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{UnixListener, UnixStream},
    sync::{Mutex, oneshot},
    task::JoinSet,
};

use crate::{OrchestrateStore, store::StoreError};

const MAXIMUM_FRAME_BYTES: usize = 8 * 1024 * 1024;

/// Starts both generated Signal listeners and runs until the task is stopped.
///
/// The readiness line is emitted only after both Unix sockets are bound.  The
/// textual line is a process readiness event, never part of the Signal wire.
pub async fn run(configure: Configure, store: OrchestrateStore) -> Result<(), TransportError> {
    let runtime = TransportRuntime::bind(configure, store)?;
    println!("orchestrate-nexus ready");
    let (shutdown_sender, shutdown) = oneshot::channel();
    let _shutdown_sender = shutdown_sender;
    runtime.serve_until(shutdown).await
}

/// The two bound sockets and the one serialized store owner behind them.
pub struct TransportRuntime {
    ordinary: UnixListener,
    meta: UnixListener,
    store: Arc<Mutex<OrchestrateStore>>,
}

impl TransportRuntime {
    /// Binds the ordinary and privileged sockets.  Successful return is the
    /// transport readiness event used by in-process tests.
    pub fn bind(configure: Configure, store: OrchestrateStore) -> Result<Self, TransportError> {
        Self::prepare_socket_path(Path::new(&configure.ordinary_socket_path.0))?;
        Self::prepare_socket_path(Path::new(&configure.meta_socket_path.0))?;
        let ordinary = UnixListener::bind(&configure.ordinary_socket_path.0)?;
        let meta = UnixListener::bind(&configure.meta_socket_path.0)?;
        Ok(Self {
            ordinary,
            meta,
            store: Arc::new(Mutex::new(store)),
        })
    }

    fn prepare_socket_path(path: &Path) -> Result<(), TransportError> {
        let parent = path.parent().expect("configured socket path has a parent");
        fs::create_dir_all(parent)?;
        match StandardUnixStream::connect(path) {
            Ok(_) => Err(TransportError::SocketAlreadyActive {
                path: path.to_path_buf(),
            }),
            Err(error) if matches!(error.kind(), ErrorKind::NotFound) => Ok(()),
            Err(error) if matches!(error.kind(), ErrorKind::ConnectionRefused) => {
                fs::remove_file(path)?;
                Ok(())
            }
            Err(error) => Err(error.into()),
        }
    }

    /// Serves connections until the supplied shutdown event resolves.
    ///
    /// Connection tasks are cancelled and joined before this method returns,
    /// making the shutdown event a clean boundary for socket tests.
    pub async fn serve_until(
        self,
        mut shutdown: oneshot::Receiver<()>,
    ) -> Result<(), TransportError> {
        let ordinary = self.ordinary;
        let meta = self.meta;
        let store = self.store;
        let mut connections = JoinSet::new();

        loop {
            tokio::select! {
                _ = &mut shutdown => {
                    connections.abort_all();
                    while connections.join_next().await.is_some() {}
                    return Ok(());
                }
                accepted = ordinary.accept() => {
                    let (stream, _) = accepted?;
                    let store = Arc::clone(&store);
                    connections.spawn(async move {
                        let _ = OrdinarySocket::new(stream).serve(store).await;
                    });
                }
                accepted = meta.accept() => {
                    let (stream, _) = accepted?;
                    let store = Arc::clone(&store);
                    connections.spawn(async move {
                        let _ = MetaSocket::new(stream).serve(store).await;
                    });
                }
                joined = connections.join_next(), if !connections.is_empty() => {
                    let _ = joined;
                }
            }
        }
    }
}

/// Concrete client for the ordinary generated Signal contract.
pub struct OrdinarySignalTransport {
    socket_path: PathBuf,
}

impl OrdinarySignalTransport {
    pub fn new(socket_path: impl Into<PathBuf>) -> Self {
        Self {
            socket_path: socket_path.into(),
        }
    }

    /// Exchanges one fully generated ordinary frame over a fresh Unix socket.
    pub async fn exchange(&self, frame: OrdinaryFrame) -> Result<OrdinaryFrame, TransportError> {
        let mut socket = OrdinarySocket::connect(&self.socket_path).await?;
        socket.write_frame(&frame).await?;
        socket.read_frame().await
    }

    /// Sends a typed ordinary request and returns its typed frame reply.
    pub async fn request(
        &self,
        exchange: OrdinaryExchangeIdentifier,
        request: OrdinaryRequest<OrchestrateRequest>,
    ) -> Result<OrdinaryReply<OrchestrateReply>, TransportError> {
        let route = request.route()?;
        let frame = OrdinaryFrame::new(route, OrdinaryFrameBody::Request { exchange, request });
        let reply = self.exchange(frame).await?;
        match reply.into_body() {
            OrdinaryExchangeFrameBody::Reply {
                exchange: actual,
                reply,
            } if actual == exchange => Ok(reply),
            OrdinaryExchangeFrameBody::Reply { .. } => Err(TransportError::ExchangeMismatch),
            _ => Err(TransportError::UnexpectedReplyFrame),
        }
    }
}

/// Concrete client for the privileged generated Signal contract.
pub struct MetaSignalTransport {
    socket_path: PathBuf,
}

impl MetaSignalTransport {
    pub fn new(socket_path: impl Into<PathBuf>) -> Self {
        Self {
            socket_path: socket_path.into(),
        }
    }

    /// Exchanges one fully generated meta frame over a fresh Unix socket.
    pub async fn exchange(&self, frame: MetaFrame) -> Result<MetaFrame, TransportError> {
        let mut socket = MetaSocket::connect(&self.socket_path).await?;
        socket.write_frame(&frame).await?;
        socket.read_frame().await
    }

    /// Sends a typed privileged request and returns its typed frame reply.
    pub async fn request(
        &self,
        exchange: ExchangeIdentifier,
        request: Request<MetaOrchestrateRequest>,
    ) -> Result<Reply<MetaOrchestrateReply>, TransportError> {
        let route = request.route()?;
        let frame = MetaFrame::new(route, MetaFrameBody::Request { exchange, request });
        let reply = self.exchange(frame).await?;
        match reply.into_body() {
            ExchangeFrameBody::Reply {
                exchange: actual,
                reply,
            } if actual == exchange => Ok(reply),
            ExchangeFrameBody::Reply { .. } => Err(TransportError::ExchangeMismatch),
            _ => Err(TransportError::UnexpectedReplyFrame),
        }
    }
}

struct OrdinarySocket {
    stream: UnixStream,
}

impl OrdinarySocket {
    fn new(stream: UnixStream) -> Self {
        Self { stream }
    }

    async fn connect(socket_path: &std::path::Path) -> Result<Self, TransportError> {
        Ok(Self::new(UnixStream::connect(socket_path).await?))
    }

    async fn read_frame(&mut self) -> Result<OrdinaryFrame, TransportError> {
        let bytes = LengthPrefixedSignal::read(&mut self.stream).await?;
        Ok(OrdinaryFrame::decode_length_prefixed(bytes.as_slice())?)
    }

    async fn write_frame(&mut self, frame: &OrdinaryFrame) -> Result<(), TransportError> {
        LengthPrefixedSignal::from_ordinary(frame)?
            .write(&mut self.stream)
            .await
    }

    async fn serve(&mut self, store: Arc<Mutex<OrchestrateStore>>) -> Result<(), TransportError> {
        let frame = self.read_frame().await?;
        let route = frame.short_header().route();
        let OrdinaryFrameBody::Request { exchange, request } = frame.into_body() else {
            return Err(OrdinaryOperationDispatchError::UnexpectedFrameBody.into());
        };
        if request.route()? != route {
            return Err(OrdinaryOperationDispatchError::HeaderRouteMismatch {
                expected: request.route()?,
                actual: route,
            }
            .into());
        }
        let reply = {
            let mut store = store.lock().await;
            replies_from_ordinary_request(&mut store, request)?
        };
        let frame = OrdinaryFrame::new(route, OrdinaryFrameBody::Reply { exchange, reply });
        self.write_frame(&frame).await
    }
}

struct MetaSocket {
    stream: UnixStream,
}

impl MetaSocket {
    fn new(stream: UnixStream) -> Self {
        Self { stream }
    }

    async fn connect(socket_path: &std::path::Path) -> Result<Self, TransportError> {
        Ok(Self::new(UnixStream::connect(socket_path).await?))
    }

    async fn read_frame(&mut self) -> Result<MetaFrame, TransportError> {
        let bytes = LengthPrefixedSignal::read(&mut self.stream).await?;
        Ok(MetaFrame::decode_length_prefixed(bytes.as_slice())?)
    }

    async fn write_frame(&mut self, frame: &MetaFrame) -> Result<(), TransportError> {
        LengthPrefixedSignal::from_meta(frame)?
            .write(&mut self.stream)
            .await
    }

    async fn serve(&mut self, store: Arc<Mutex<OrchestrateStore>>) -> Result<(), TransportError> {
        let frame = self.read_frame().await?;
        let route = frame.short_header().route();
        let MetaFrameBody::Request { exchange, request } = frame.into_body() else {
            return Err(OperationDispatchError::UnexpectedFrameBody.into());
        };
        if request.route()? != route {
            return Err(OperationDispatchError::HeaderRouteMismatch {
                expected: request.route()?,
                actual: route,
            }
            .into());
        }
        let reply = {
            let mut store = store.lock().await;
            replies_from_meta_request(&mut store, request)?
        };
        let frame = MetaFrame::new(route, MetaFrameBody::Reply { exchange, reply });
        self.write_frame(&frame).await
    }
}

struct LengthPrefixedSignal {
    bytes: Vec<u8>,
}

impl LengthPrefixedSignal {
    async fn read(stream: &mut UnixStream) -> Result<Self, TransportError> {
        let mut prefix = [0; 4];
        stream.read_exact(&mut prefix).await?;
        let length = u32::from_be_bytes(prefix) as usize;
        if length > MAXIMUM_FRAME_BYTES {
            return Err(TransportError::FrameTooLarge {
                maximum: MAXIMUM_FRAME_BYTES,
                found: length,
            });
        }
        let mut bytes = Vec::with_capacity(4 + length);
        bytes.extend_from_slice(&prefix);
        bytes.resize(4 + length, 0);
        stream.read_exact(&mut bytes[4..]).await?;
        Ok(Self { bytes })
    }

    fn from_ordinary(frame: &OrdinaryFrame) -> Result<Self, TransportError> {
        Self::from_bytes(frame.encode_length_prefixed()?)
    }

    fn from_meta(frame: &MetaFrame) -> Result<Self, TransportError> {
        Self::from_bytes(frame.encode_length_prefixed()?)
    }

    fn from_bytes(bytes: Vec<u8>) -> Result<Self, TransportError> {
        let found = bytes.len().saturating_sub(4);
        if found > MAXIMUM_FRAME_BYTES {
            return Err(TransportError::FrameTooLarge {
                maximum: MAXIMUM_FRAME_BYTES,
                found,
            });
        }
        Ok(Self { bytes })
    }

    fn as_slice(&self) -> &[u8] {
        &self.bytes
    }

    async fn write(&self, stream: &mut UnixStream) -> Result<(), TransportError> {
        stream.write_all(&self.bytes).await?;
        stream.flush().await?;
        stream.shutdown().await?;
        Ok(())
    }
}

fn replies_from_ordinary_request(
    store: &mut OrchestrateStore,
    request: OrdinaryRequest<OrchestrateRequest>,
) -> Result<OrdinaryReply<OrchestrateReply>, TransportError> {
    let (head, tail) = request.payloads.into_head_and_tail();
    let head = OrdinarySubReply::Ok(store.ordinary(head)?);
    let tail = tail
        .into_iter()
        .map(|request| store.ordinary(request).map(OrdinarySubReply::Ok))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(OrdinaryReply::committed(
        OrdinaryNonEmpty::from_head_and_tail(head, tail),
    ))
}

fn replies_from_meta_request(
    store: &mut OrchestrateStore,
    request: Request<MetaOrchestrateRequest>,
) -> Result<Reply<MetaOrchestrateReply>, TransportError> {
    let (head, tail) = request.payloads.into_head_and_tail();
    let head = SubReply::Ok(store.meta(head)?);
    let tail = tail
        .into_iter()
        .map(|request| store.meta(request).map(SubReply::Ok))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Reply::committed(NonEmpty::from_head_and_tail(head, tail)))
}

#[derive(Debug, thiserror::Error)]
pub enum TransportError {
    #[error("Unix socket I/O failed: {0}")]
    Io(#[from] std::io::Error),

    #[error("an Orchestrate Nexus already owns socket {path:?}")]
    SocketAlreadyActive { path: PathBuf },

    #[error("generated Signal frame validation failed: {0}")]
    Frame(#[from] signal_frame::FrameError),

    #[error("generated ordinary Signal frame validation failed: {0}")]
    OrdinaryFrame(#[from] signal_frame_ordinary::FrameError),

    #[error("generated Signal request route failed: {0}")]
    Route(#[from] signal_frame::WireRouteError),

    #[error("generated ordinary Signal request route failed: {0}")]
    OrdinaryRoute(#[from] signal_frame_ordinary::WireRouteError),

    #[error("generated Signal dispatch failed: {0}")]
    Dispatch(#[from] signal_frame::OperationDispatchError),

    #[error("generated ordinary Signal dispatch failed: {0}")]
    OrdinaryDispatch(#[from] signal_frame_ordinary::OperationDispatchError),

    #[error("store failed: {0}")]
    Store(#[from] StoreError),

    #[error("length-prefixed Signal frame exceeds {maximum} bytes: {found} bytes")]
    FrameTooLarge { maximum: usize, found: usize },

    #[error("received a reply for a different exchange")]
    ExchangeMismatch,

    #[error("expected a generated Signal reply frame")]
    UnexpectedReplyFrame,
}
