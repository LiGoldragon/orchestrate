//! Direct typed daemon boundary for the Orchestrator.
//!
//! Each socket decodes its canonical contract Frame, executes the request on
//! the single sema-owning service, and returns a Frame from the same contract.

use std::{
    fmt::{Display, Formatter},
    path::PathBuf,
    sync::Arc,
    time::Duration,
};

use signal_frame::{LogVariant, OperationDispatchError, Request, ShortHeader, WireRoute};
use tokio::{io::AsyncWriteExt, sync::Mutex};
use triad_runtime::{
    AcceptedConnection, AsyncListenerSocket, AsyncMultiConnectionRuntime, AsyncMultiListenerDaemon,
    AsyncMultiListenerDaemonError, BindingSurface, FrameBody as TransportBody, LengthPrefixedCodec,
    MaximumFrameLength, RequestErrorLog, SocketMode,
};

use crate::{
    DaemonConfiguration, Error, OrchestrateLayout, OrchestrateService, PublicSocketRetirement,
    UpgradeRequestFrame,
};

const MAXIMUM_REQUEST_FRAME_BYTES: usize = 8 * 1024 * 1024;
const REQUEST_READ_TIMEOUT: Duration = Duration::from_secs(10);
const OWNER_ONLY_SOCKET_MODE: u32 = 0o600;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ListenerTier {
    Working,
    Meta,
    Upgrade,
}

impl Display for ListenerTier {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Working => formatter.write_str("working"),
            Self::Meta => formatter.write_str("meta"),
            Self::Upgrade => formatter.write_str("upgrade"),
        }
    }
}

#[derive(Clone)]
struct OrchestratorRuntime {
    service: Arc<Mutex<OrchestrateService>>,
}

impl OrchestratorRuntime {
    fn new(service: OrchestrateService) -> Self {
        Self {
            service: Arc::new(Mutex::new(service)),
        }
    }

    async fn handle_working(
        &self,
        mut connection: AcceptedConnection,
    ) -> Result<(), OrchestrateDaemonError> {
        let body = Self::read_body(&mut connection).await?;
        let frame = signal_orchestrate::OrchestrateFrame::decode(body.bytes())?;
        let short_header = frame.short_header();
        let signal_orchestrate::OrchestrateFrameBody::Request { exchange, request } =
            frame.into_body()
        else {
            return Err(OrchestrateDaemonError::UnexpectedFrame { tier: "working" });
        };
        let route = validate_request_route(short_header, &request)?;
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
        let short_header = frame.short_header();
        let meta_signal_orchestrate::FrameBody::Request { exchange, request } = frame.into_body()
        else {
            return Err(OrchestrateDaemonError::UnexpectedFrame { tier: "meta" });
        };
        let route = validate_request_route(short_header, &request)?;
        let reply = self.service.lock().await.handle_meta_request(request).await;
        let frame = meta_signal_orchestrate::Frame::new(
            route,
            meta_signal_orchestrate::FrameBody::Reply { exchange, reply },
        );
        Self::write_body(&mut connection, frame.encode()?).await
    }

    async fn handle_upgrade(
        &self,
        mut connection: AcceptedConnection,
    ) -> Result<(), OrchestrateDaemonError> {
        let body = Self::read_body(&mut connection).await?;
        let (route, exchange, request) = UpgradeRequestFrame::decode(body.bytes())?.into_parts();
        let reply = self.service.lock().await.handle_upgrade_request(request)?;
        let frame = UpgradeRequestFrame::encode_reply(route, exchange, reply)?;
        Self::write_body(&mut connection, frame).await
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
}

fn validate_request_route<Payload>(
    short_header: ShortHeader,
    request: &Request<Payload>,
) -> Result<WireRoute, OrchestrateDaemonError>
where
    Payload: LogVariant,
{
    let expected = request.route()?;
    let actual = short_header.route();
    if expected != actual {
        return Err(OperationDispatchError::HeaderRouteMismatch { expected, actual }.into());
    }
    Ok(expected)
}

impl AsyncMultiConnectionRuntime for OrchestratorRuntime {
    type Listener = ListenerTier;
    type Error = OrchestrateDaemonError;

    async fn handle_connection(
        &self,
        listener: Self::Listener,
        connection: AcceptedConnection,
    ) -> Result<(), Self::Error> {
        match listener {
            ListenerTier::Working => self.handle_working(connection).await,
            ListenerTier::Meta => self.handle_meta(connection).await,
            ListenerTier::Upgrade => self.handle_upgrade(connection).await,
        }
    }
}

pub struct OrchestrateDaemon {
    configuration: DaemonConfiguration,
    service: OrchestrateService,
}

impl OrchestrateDaemon {
    pub fn new(configuration: DaemonConfiguration) -> Result<Self, OrchestrateDaemonError> {
        let service = OrchestrateService::open_with_layout(
            &crate::StoreLocation::new(configuration.store_path.as_str()),
            OrchestrateLayout::new(
                PathBuf::from(configuration.workspace_root.as_str()),
                PathBuf::from(configuration.git_index_root.as_str()),
            ),
        )?
        .with_public_socket_retirement(PublicSocketRetirement::new(
            PathBuf::from(configuration.ordinary_socket_path.as_str()),
            PathBuf::from(configuration.meta_socket_path.as_str()),
        ))
        .with_router_registration_endpoint(
            configuration
                .router_working_socket_path()
                .map(|path| PathBuf::from(path.as_str())),
        )
        .with_messenger_registration_endpoint(
            configuration
                .messenger_working_socket_path()
                .map(|path| PathBuf::from(path.as_str())),
        );
        Ok(Self {
            configuration,
            service,
        })
    }

    pub async fn run_async(
        self,
    ) -> Result<(), AsyncMultiListenerDaemonError<OrchestrateDaemonError>> {
        let listeners = [
            AsyncListenerSocket::new(
                ListenerTier::Working,
                self.configuration.socket_path().to_path_buf(),
            )
            .with_socket_mode(SocketMode::new(OWNER_ONLY_SOCKET_MODE)),
            AsyncListenerSocket::new(
                ListenerTier::Meta,
                self.configuration
                    .meta_socket_path()
                    .expect("meta socket is part of the typed configuration")
                    .to_path_buf(),
            )
            .with_socket_mode(SocketMode::new(OWNER_ONLY_SOCKET_MODE)),
            AsyncListenerSocket::new(
                ListenerTier::Upgrade,
                self.configuration
                    .upgrade_socket_path()
                    .expect("upgrade socket is part of the typed configuration")
                    .to_path_buf(),
            )
            .with_socket_mode(SocketMode::new(OWNER_ONLY_SOCKET_MODE)),
        ];
        AsyncMultiListenerDaemon::new(
            listeners,
            OrchestratorRuntime::new(self.service),
            RequestErrorLog::new("orchestrate-daemon"),
        )
        .with_concurrency_limit(self.configuration.request_concurrency_limit())
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

    #[error("orchestration engine error: {0}")]
    Engine(#[from] Error),

    #[error("expected a request frame on the {tier} socket")]
    UnexpectedFrame { tier: &'static str },

    #[error("request frame read timed out")]
    RequestReadTimedOut,
}
