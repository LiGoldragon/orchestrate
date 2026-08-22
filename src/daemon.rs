//! Binary Signal daemon for the ordinary and meta contracts.

use std::{
    fmt::{Display, Formatter},
    sync::Arc,
    time::Duration,
};

use signal_frame::{LogVariant, OperationDispatchError, Request, ShortHeader, WireRoute};
use tokio::{io::AsyncWriteExt, sync::Mutex};
use triad_runtime::{
    AcceptedConnection, AsyncListenerSocket, AsyncMultiConnectionRuntime, AsyncMultiListenerDaemon,
    AsyncMultiListenerDaemonError, FrameBody as TransportBody, LengthPrefixedCodec,
    MaximumFrameLength, RequestErrorLog, SocketMode,
};

use crate::{DaemonConfiguration, Error, OrchestrateService};

const MAXIMUM_REQUEST_FRAME_BYTES: usize = 8 * 1024 * 1024;
const REQUEST_READ_TIMEOUT: Duration = Duration::from_secs(10);
const OWNER_ONLY_SOCKET_MODE: u32 = 0o600;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ListenerTier {
    Ordinary,
    Meta,
    Upgrade,
}

impl Display for ListenerTier {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Ordinary => "ordinary",
            Self::Meta => "meta",
            Self::Upgrade => "upgrade",
        })
    }
}

#[derive(Clone)]
struct OrchestrateRuntime {
    service: Arc<Mutex<OrchestrateService>>,
}

impl OrchestrateRuntime {
    async fn handle_ordinary(
        &self,
        mut connection: AcceptedConnection,
    ) -> Result<(), OrchestrateDaemonError> {
        let body = Self::read_body(&mut connection).await?;
        let frame = signal_orchestrate::OrchestrateFrame::decode(body.bytes())?;
        let header = frame.short_header();
        let signal_orchestrate::OrchestrateFrameBody::Request { exchange, request } =
            frame.into_body()
        else {
            return Err(OrchestrateDaemonError::UnexpectedFrame { tier: "ordinary" });
        };
        let route = Self::validate_request_route(header, &request)?;
        let reply = self.service.lock().await.handle_request(request).await;
        let frame = signal_orchestrate::OrchestrateFrame::new(
            route,
            signal_orchestrate::OrchestrateFrameBody::Reply { exchange, reply },
        );
        Self::write_body(&mut connection, frame.encode()?).await
    }

    async fn handle_meta(
        &self,
        mut connection: AcceptedConnection,
    ) -> Result<(), OrchestrateDaemonError> {
        let body = Self::read_body(&mut connection).await?;
        let frame = meta_signal_orchestrate::Frame::decode(body.bytes())?;
        let header = frame.short_header();
        let meta_signal_orchestrate::FrameBody::Request { exchange, request } = frame.into_body()
        else {
            return Err(OrchestrateDaemonError::UnexpectedFrame { tier: "meta" });
        };
        let route = Self::validate_request_route(header, &request)?;
        let reply = self.service.lock().await.handle_meta_request(request).await;
        let frame = meta_signal_orchestrate::Frame::new(
            route,
            meta_signal_orchestrate::FrameBody::Reply { exchange, reply },
        );
        Self::write_body(&mut connection, frame.encode()?).await
    }

    async fn read_body(
        connection: &mut AcceptedConnection,
    ) -> Result<TransportBody, OrchestrateDaemonError> {
        let codec = LengthPrefixedCodec::new(MaximumFrameLength::new(MAXIMUM_REQUEST_FRAME_BYTES));
        tokio::time::timeout(
            REQUEST_READ_TIMEOUT,
            codec.read_body_async(connection.stream_mut()),
        )
        .await
        .map_err(|_| OrchestrateDaemonError::RequestReadTimedOut)?
        .map_err(OrchestrateDaemonError::from)
    }

    async fn write_body(
        connection: &mut AcceptedConnection,
        bytes: Vec<u8>,
    ) -> Result<(), OrchestrateDaemonError> {
        let codec = LengthPrefixedCodec::new(MaximumFrameLength::new(MAXIMUM_REQUEST_FRAME_BYTES));
        codec
            .write_body_async(connection.stream_mut(), &TransportBody::new(bytes))
            .await?;
        connection.stream_mut().flush().await?;
        Ok(())
    }

    fn validate_request_route<Payload>(
        header: ShortHeader,
        request: &Request<Payload>,
    ) -> Result<WireRoute, OrchestrateDaemonError>
    where
        Payload: LogVariant,
    {
        let expected = request.route()?;
        let actual = header.route();
        if expected != actual {
            return Err(OperationDispatchError::HeaderRouteMismatch { expected, actual }.into());
        }
        Ok(expected)
    }
}

impl AsyncMultiConnectionRuntime for OrchestrateRuntime {
    type Listener = ListenerTier;
    type Error = OrchestrateDaemonError;

    async fn handle_connection(
        &self,
        listener: Self::Listener,
        connection: AcceptedConnection,
    ) -> Result<(), Self::Error> {
        match listener {
            ListenerTier::Ordinary => self.handle_ordinary(connection).await,
            ListenerTier::Meta => self.handle_meta(connection).await,
            ListenerTier::Upgrade => {
                Err(OrchestrateDaemonError::UnexpectedFrame { tier: "upgrade" })
            }
        }
    }
}

pub struct OrchestrateDaemon {
    configuration: DaemonConfiguration,
    service: OrchestrateService,
}

impl OrchestrateDaemon {
    pub fn new(configuration: DaemonConfiguration) -> Result<Self, OrchestrateDaemonError> {
        Ok(Self {
            service: OrchestrateService::open(&configuration.store)?,
            configuration,
        })
    }

    pub async fn run_async(
        self,
    ) -> Result<(), AsyncMultiListenerDaemonError<OrchestrateDaemonError>> {
        let listeners = [
            AsyncListenerSocket::new(ListenerTier::Ordinary, self.configuration.ordinary_socket)
                .with_socket_mode(SocketMode::new(OWNER_ONLY_SOCKET_MODE)),
            AsyncListenerSocket::new(ListenerTier::Meta, self.configuration.meta_socket)
                .with_socket_mode(SocketMode::new(OWNER_ONLY_SOCKET_MODE)),
            AsyncListenerSocket::new(ListenerTier::Upgrade, self.configuration.upgrade_socket)
                .with_socket_mode(SocketMode::new(OWNER_ONLY_SOCKET_MODE)),
        ];
        AsyncMultiListenerDaemon::new(
            listeners,
            OrchestrateRuntime {
                service: Arc::new(Mutex::new(self.service)),
            },
            RequestErrorLog::new("orchestrate-daemon"),
        )
        .run()
        .await
    }
}

#[derive(Debug, thiserror::Error)]
pub enum OrchestrateDaemonError {
    #[error("daemon IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("length-prefixed frame error: {0}")]
    TransportFrame(#[from] triad_runtime::FrameError),
    #[error("signal frame error: {0}")]
    SignalFrame(#[from] signal_frame::FrameError),
    #[error("signal wire route error: {0}")]
    WireRoute(#[from] signal_frame::WireRouteError),
    #[error("signal operation dispatch error: {0}")]
    OperationDispatch(#[from] signal_frame::OperationDispatchError),
    #[error("orchestrate engine error: {0}")]
    Engine(#[from] Error),
    #[error("expected a request frame on the {tier} socket")]
    UnexpectedFrame { tier: &'static str },
    #[error("request frame read timed out")]
    RequestReadTimedOut,
}
