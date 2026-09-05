//! Unix transport for the generated ordinary and meta Signal contracts.
//!
//! Each connection carries one complete hand-owned, length-prefixed rkyv
//! envelope. The generated contract owns the frame anatomy; this module only
//! validates that envelope and dispatches one typed request to the serialized
//! Nexus-owned store.

use std::{
    fs,
    io::ErrorKind,
    os::unix::net::UnixStream as StandardUnixStream,
    path::{Path, PathBuf},
    sync::Arc,
};

use meta_signal_orchestrate::WireConversion as MetaWireConversion;
use signal_frame::{BoundExchangeFrame, ExchangeFrameBody, FrameError, NonEmpty, Reply, SubReply};
use signal_orchestrate::WireConversion as OrdinaryWireConversion;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{UnixListener, UnixStream},
    sync::{Mutex, oneshot},
    task::JoinSet,
};

use crate::{OrchestrateStore, ordinary::OrdinaryOutcome, store::StoreError, wire};

const MAXIMUM_FRAME_BYTES: usize = 8 * 1024 * 1024;

type OrdinaryFrame = BoundExchangeFrame<
    wire::OrdinaryContract,
    signal_orchestrate::RequestWire,
    signal_orchestrate::ResponseWire,
>;
type MetaFrame = BoundExchangeFrame<
    wire::MetaContract,
    meta_signal_orchestrate::RequestWire,
    meta_signal_orchestrate::ResponseWire,
>;

/// Starts both generated Signal listeners and runs until the task is stopped.
pub async fn run(
    configure: meta_signal_orchestrate::Configure,
    store: OrchestrateStore,
) -> Result<(), TransportError> {
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
    pub fn bind(
        configure: meta_signal_orchestrate::Configure,
        store: OrchestrateStore,
    ) -> Result<Self, TransportError> {
        let ordinary_path = Path::new(configure.0.as_ref());
        let meta_path = Path::new(configure.1.as_ref());
        Self::prepare_socket_path(ordinary_path)?;
        Self::prepare_socket_path(meta_path)?;
        let ordinary = UnixListener::bind(ordinary_path)?;
        let meta = UnixListener::bind(meta_path)?;
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
                        if let Err(error) = OrdinarySocket::new(stream).serve(store).await {
                            eprintln!("orchestrate ordinary socket: {error}");
                        }
                    });
                }
                accepted = meta.accept() => {
                    let (stream, _) = accepted?;
                    let store = Arc::clone(&store);
                    connections.spawn(async move {
                        if let Err(error) = MetaSocket::new(stream).serve(store).await {
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

    pub async fn exchange(&self, frame: OrdinaryFrame) -> Result<OrdinaryFrame, TransportError> {
        let mut socket = OrdinarySocket::connect(&self.socket_path).await?;
        socket.write_frame(&frame).await?;
        socket.read_frame().await
    }

    pub async fn request(
        &self,
        request: signal_orchestrate::Request,
    ) -> Result<signal_orchestrate::Response, TransportError> {
        let route = wire::ordinary_request_route(&request);
        let frame = OrdinaryFrame::new(
            route,
            ExchangeFrameBody::Request {
                exchange: signal_frame::ExchangeIdentifier::new(
                    signal_frame::SessionEpoch::new(1),
                    signal_frame::ExchangeLane::Connector,
                    signal_frame::LaneSequence::first(),
                ),
                request: signal_frame::Request::from_payload(request.into_wire()),
            },
        );
        let frame = self.exchange(frame).await?;
        let route = frame.short_header().route();
        let ExchangeFrameBody::Reply { reply, .. } = frame.into_body() else {
            return Err(TransportError::UnexpectedReplyFrame);
        };
        let Reply::Accepted { per_operation, .. } = reply else {
            return Err(TransportError::UnexpectedReplyFrame);
        };
        let SubReply::Ok(response) = per_operation.head() else {
            return Err(TransportError::UnexpectedReplyFrame);
        };
        let response = signal_orchestrate::Response::try_from_wire(response.clone())
            .map_err(|error| TransportError::Wire(format!("{error:?}")))?;
        if route != wire::ordinary_response_route(&response) {
            return Err(TransportError::RouteMismatch);
        }
        Ok(response)
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

    pub async fn exchange(&self, frame: MetaFrame) -> Result<MetaFrame, TransportError> {
        let mut socket = MetaSocket::connect(&self.socket_path).await?;
        socket.write_frame(&frame).await?;
        socket.read_frame().await
    }

    pub async fn request(
        &self,
        request: meta_signal_orchestrate::Request,
    ) -> Result<meta_signal_orchestrate::Response, TransportError> {
        let frame = MetaFrame::new(
            wire::meta_request_route(&request),
            ExchangeFrameBody::Request {
                exchange: signal_frame::ExchangeIdentifier::new(
                    signal_frame::SessionEpoch::new(1),
                    signal_frame::ExchangeLane::Connector,
                    signal_frame::LaneSequence::first(),
                ),
                request: signal_frame::Request::from_payload(request.into_wire()),
            },
        );
        let frame = self.exchange(frame).await?;
        let route = frame.short_header().route();
        let ExchangeFrameBody::Reply { reply, .. } = frame.into_body() else {
            return Err(TransportError::UnexpectedReplyFrame);
        };
        let Reply::Accepted { per_operation, .. } = reply else {
            return Err(TransportError::UnexpectedReplyFrame);
        };
        let SubReply::Ok(response) = per_operation.head() else {
            return Err(TransportError::UnexpectedReplyFrame);
        };
        let response = meta_signal_orchestrate::Response::try_from_wire(response.clone())
            .map_err(|error| TransportError::Wire(format!("{error:?}")))?;
        if route != wire::meta_response_route(&response) {
            return Err(TransportError::RouteMismatch);
        }
        Ok(response)
    }
}

struct OrdinarySocket {
    stream: UnixStream,
}

impl OrdinarySocket {
    fn new(stream: UnixStream) -> Self {
        Self { stream }
    }

    async fn connect(socket_path: &Path) -> Result<Self, TransportError> {
        Ok(Self::new(UnixStream::connect(socket_path).await?))
    }

    async fn read_frame(&mut self) -> Result<OrdinaryFrame, TransportError> {
        let bytes = LengthPrefixedSignal::read(&mut self.stream).await?;
        OrdinaryFrame::decode_length_prefixed(bytes.as_slice()).map_err(TransportError::Frame)
    }

    async fn write_frame(&mut self, frame: &OrdinaryFrame) -> Result<(), TransportError> {
        LengthPrefixedSignal::from_bytes(
            frame
                .encode_length_prefixed()
                .map_err(TransportError::Frame)?,
        )?
        .write(&mut self.stream)
        .await
    }

    async fn serve(&mut self, store: Arc<Mutex<OrchestrateStore>>) -> Result<(), TransportError> {
        let frame = self.read_frame().await?;
        let route = frame.short_header().route();
        let ExchangeFrameBody::Request { exchange, request } = frame.into_body() else {
            return Err(TransportError::UnexpectedRequestFrame);
        };
        if !request.payloads().tail().is_empty() {
            return Err(TransportError::MultipleRequests);
        }
        let request = signal_orchestrate::Request::try_from_wire(request.payloads().head().clone())
            .map_err(|error| TransportError::Wire(format!("{error:?}")))?;
        if route != wire::ordinary_request_route(&request) {
            return Err(TransportError::RouteMismatch);
        }
        let OrdinaryOutcome::Response(response) = store.lock().await.ordinary(request)?;
        self.write_frame(&ordinary_frame(exchange, response)).await
    }
}

struct MetaSocket {
    stream: UnixStream,
}

impl MetaSocket {
    fn new(stream: UnixStream) -> Self {
        Self { stream }
    }

    async fn connect(socket_path: &Path) -> Result<Self, TransportError> {
        Ok(Self::new(UnixStream::connect(socket_path).await?))
    }

    async fn read_frame(&mut self) -> Result<MetaFrame, TransportError> {
        let bytes = LengthPrefixedSignal::read(&mut self.stream).await?;
        MetaFrame::decode_length_prefixed(bytes.as_slice()).map_err(TransportError::Frame)
    }

    async fn write_frame(&mut self, frame: &MetaFrame) -> Result<(), TransportError> {
        LengthPrefixedSignal::from_bytes(
            frame
                .encode_length_prefixed()
                .map_err(TransportError::Frame)?,
        )?
        .write(&mut self.stream)
        .await
    }

    async fn serve(&mut self, store: Arc<Mutex<OrchestrateStore>>) -> Result<(), TransportError> {
        let frame = self.read_frame().await?;
        let route = frame.short_header().route();
        let ExchangeFrameBody::Request { exchange, request } = frame.into_body() else {
            return Err(TransportError::UnexpectedRequestFrame);
        };
        if !request.payloads().tail().is_empty() {
            return Err(TransportError::MultipleRequests);
        }
        let request =
            meta_signal_orchestrate::Request::try_from_wire(request.payloads().head().clone())
                .map_err(|error| TransportError::Wire(format!("{error:?}")))?;
        if route != wire::meta_request_route(&request) {
            return Err(TransportError::RouteMismatch);
        }
        let response = store.lock().await.meta(request)?;
        self.write_frame(&meta_frame(exchange, response)).await
    }
}

fn ordinary_frame(
    exchange: signal_frame::ExchangeIdentifier,
    response: signal_orchestrate::Response,
) -> OrdinaryFrame {
    let route = wire::ordinary_response_route(&response);
    OrdinaryFrame::new(
        route,
        ExchangeFrameBody::Reply {
            exchange,
            reply: Reply::committed(NonEmpty::single(SubReply::Ok(response.into_wire()))),
        },
    )
}

fn meta_frame(
    exchange: signal_frame::ExchangeIdentifier,
    response: meta_signal_orchestrate::Response,
) -> MetaFrame {
    let route = wire::meta_response_route(&response);
    MetaFrame::new(
        route,
        ExchangeFrameBody::Reply {
            exchange,
            reply: Reply::committed(NonEmpty::single(SubReply::Ok(response.into_wire()))),
        },
    )
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

#[derive(Debug, thiserror::Error)]
pub enum TransportError {
    #[error("Unix socket I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("an Orchestrate Nexus already owns socket {path:?}")]
    SocketAlreadyActive { path: PathBuf },
    #[error("bound Signal frame validation failed: {0:?}")]
    Frame(FrameError),
    #[error("store failed: {0}")]
    Store(#[from] StoreError),
    #[error("length-prefixed Signal frame exceeds {maximum} bytes: {found} bytes")]
    FrameTooLarge { maximum: usize, found: usize },
    #[error("expected a generated Signal request frame")]
    UnexpectedRequestFrame,
    #[error("expected a generated Signal reply frame")]
    UnexpectedReplyFrame,
    #[error("Signal request contains more than one operation")]
    MultipleRequests,
    #[error("Signal header route does not match its generated payload")]
    RouteMismatch,
    #[error("generated Signal wire conversion failed: {0}")]
    Wire(String),
}
