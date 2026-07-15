//! Orchestrate's daemon hooks — the only daemon code orchestrate hand-writes.
//!
//! The uniform daemon skeleton (argv parsing, async task-backed multi-listener
//! binding, the kameo `EngineActor` that owns the engine, the
//! decode -> ask -> encode spine, and the `ExitReport` entry) is emitted into
//! `src/schema/daemon.rs` by schema-rust's daemon emitter. Orchestrate
//! fills only the record-1488 escape hatches through
//! `impl ComponentDaemon for OrchestrateDaemon`: how to load its binary
//! `Configuration`, how to open its Store/Engine (`build_runtime`), how one
//! working `Input` becomes one `Output`, and the owner-only meta request hook.
//!
//! The engine is `OrchestrateService`, owned by the generated `EngineActor`.
//! The actor mailbox serialises every request, so each handler holds `&mut
//! Engine` and no component-internal lock is required.

use std::path::PathBuf;

use thiserror::Error;
use tokio::io::AsyncWriteExt;
use triad_runtime::{
    AcceptedConnection, ConnectionContext, FrameBody as LengthPrefixedFrameBody, FrameError,
    LengthPrefixedCodec, ListenerError,
};

use meta_signal_orchestrate::schema::lib::{
    Input as MetaInput, Output as MetaOutput, SignalFrameError as MetaSignalFrameError,
};
use signal_orchestrate::schema::lib::{Input, Output, SignalFrameError};

use crate::schema::daemon::ComponentDaemon;
use crate::{
    ConfigurationError, DaemonConfiguration, Error, OrchestrateLayout, OrchestrateService,
    PublicSocketRetirement, UpgradeRequestFrame,
};

/// The type-level selector for orchestrate's emitted daemon. It carries no
/// runtime data — it is the marker the emitted `DaemonCommand<OrchestrateDaemon>`
/// and the generated runtime dispatch on, selecting orchestrate's
/// `Configuration` / `Engine` / `Error` types through the `ComponentDaemon`
/// associated types.
#[derive(Debug)]
pub struct OrchestrateDaemon;

/// Orchestrate's daemon error: the engine-facing variants the emitted spine
/// needs (`From<FrameError>` / `From<SignalFrameError>` /
/// `From<EngineRequestError>`) plus orchestrate's domain error. The emitted
/// `DaemonError<OrchestrateDaemon>` wraps this under its `Component` arm.
#[derive(Debug, Error)]
pub enum OrchestrateDaemonError {
    #[error("daemon IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("daemon frame error: {0}")]
    Frame(#[from] FrameError),

    #[error("daemon listener error: {0}")]
    Listener(#[from] ListenerError),

    #[error("daemon signal frame error: {0}")]
    SignalFrame(#[from] SignalFrameError),

    #[error("daemon meta signal frame error: {0}")]
    MetaSignalFrame(#[from] MetaSignalFrameError),

    #[error("engine actor request error: {0}")]
    EngineRequest(#[from] triad_runtime::EngineRequestError),

    #[error("orchestrate engine error: {0}")]
    Engine(#[from] Error),
}

impl ComponentDaemon for OrchestrateDaemon {
    type Configuration = DaemonConfiguration;
    type ConfigurationError = ConfigurationError;
    type Engine = OrchestrateService;
    type Error = OrchestrateDaemonError;

    const PROCESS_NAME: &'static str = "orchestrate-daemon";

    fn load_configuration(
        path: &std::path::Path,
    ) -> Result<Self::Configuration, Self::ConfigurationError> {
        DaemonConfiguration::from_signal_file(path)
    }

    fn build_runtime(configuration: &Self::Configuration) -> Result<Self::Engine, Self::Error> {
        let layout = OrchestrateLayout::new(
            PathBuf::from(configuration.workspace_root.as_str()),
            PathBuf::from(configuration.git_index_root.as_str()),
        );
        let service = OrchestrateService::open_with_layout(
            &crate::StoreLocation::new(configuration.store_path.as_str()),
            layout,
        )?
        .with_lane_reclamation_socket(PathBuf::from(configuration.ordinary_socket_path.as_str()))?
        .with_public_socket_retirement(PublicSocketRetirement::new(
            PathBuf::from(configuration.ordinary_socket_path.as_str()),
            PathBuf::from(configuration.meta_socket_path.as_str()),
        ))
        .with_router_registration_endpoint(
            configuration
                .router_working_socket_path()
                .map(|path| PathBuf::from(path.as_str())),
        );
        Ok(service)
    }

    async fn handle_working_input<'connection>(
        engine: &'connection mut Self::Engine,
        input: Input,
        connection: &'connection ConnectionContext,
    ) -> Result<Output, Self::Error> {
        // The registering peer's kernel-vouched pid (SO_PEERCRED) is the seed
        // for reachability discovery. A Unix-socket peer carries it; a TCP peer
        // does not, so registration then lands without reachability. The pid is
        // a positive kernel value; a non-positive credential is treated as
        // absent rather than coerced.
        let caller_process_id = connection
            .unix_credentials()
            .map(triad_runtime::UnixCredentials::process_id)
            .filter(|process_id| *process_id > 0)
            .map(|process_id| process_id as u32);
        Ok(engine
            .handle_signal_input_from_caller(input, caller_process_id)
            .await?)
    }

    /// Serve one owner-only meta connection: decode a meta `Input` off the
    /// accepted stream, drive it through the meta nexus path, and write the meta
    /// `Output` back. Routing the whole connection through the engine actor
    /// serialises meta policy traffic with the working state — correct for
    /// low-volume owner-only traffic.
    async fn handle_meta_connection(
        engine: &mut Self::Engine,
        mut connection: AcceptedConnection,
    ) -> Result<(), Self::Error> {
        let frame = LengthPrefixedCodec::default()
            .read_body_async(connection.stream_mut())
            .await?
            .into_bytes();
        let (_route, input) = MetaInput::decode_signal_frame(&frame)?;
        let output: MetaOutput = engine.handle_signal_meta_input(input).await?;
        LengthPrefixedCodec::default()
            .write_body_async(
                connection.stream_mut(),
                &LengthPrefixedFrameBody::new(output.encode_signal_frame()?),
            )
            .await?;
        connection
            .stream_mut()
            .flush()
            .await
            .map_err(FrameError::from)?;
        Ok(())
    }

    /// Serve one owner-only upgrade connection: decode a version-handover
    /// contract `Frame` off the accepted stream, validate its short header
    /// against the operation root, drive the handover state machine on the
    /// `&mut` engine (which retires the public sockets when a handover
    /// finalizes), and write the contract reply `Frame` back. The upgrade tier
    /// speaks the version-handover *contract* wire (not a schema-emitted frame)
    /// because the handover protocol is shared across components and is not part
    /// of orchestrate's own signal schema.
    async fn handle_upgrade_connection(
        engine: &mut Self::Engine,
        mut connection: AcceptedConnection,
    ) -> Result<(), Self::Error> {
        let body = LengthPrefixedCodec::default()
            .read_body_async(connection.stream_mut())
            .await?
            .into_bytes();
        let (exchange, request) = UpgradeRequestFrame::decode(&body)?.into_parts();
        let reply = engine.handle_upgrade_request(request)?;
        let response = UpgradeRequestFrame::encode_reply(exchange, reply)?;
        LengthPrefixedCodec::default()
            .write_body_async(
                connection.stream_mut(),
                &LengthPrefixedFrameBody::new(response),
            )
            .await?;
        connection
            .stream_mut()
            .flush()
            .await
            .map_err(FrameError::from)?;
        Ok(())
    }
}
