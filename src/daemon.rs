use std::fmt::{Display, Formatter};
use std::io::{Read, Write};
use std::os::unix::fs::FileTypeExt;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use meta_signal_orchestrate::{
    Frame as MetaOrchestrateFrame, FrameBody as MetaOrchestrateFrameBody, MetaOrchestrateRequest,
};
use signal_frame::{
    AcceptedOutcome, OperationDispatchError, Reply, Request, ShortHeader, SubReply,
};
use signal_orchestrate::{OrchestrateFrame, OrchestrateFrameBody, OrchestrateRequest};
use signal_version_handover::{
    Frame as UpgradeFrame, FrameBody as UpgradeFrameBody, Operation as UpgradeOperation,
    Reply as UpgradeReply,
};
use triad_runtime::{
    BoundedWorkers, ListenerSocket, MultiListenerDaemon, MultiListenerDaemonError,
    MultiListenerRuntime, RequestErrorLog,
};

use crate::{
    DaemonConfiguration, Error, OrchestrateLayout, OrchestrateService, Result, StoreLocation,
};

const MAX_FRAME_LENGTH: usize = 16 * 1024 * 1024;
const MAXIMUM_CONCURRENT_REQUESTS: usize = 64;

pub struct OrchestrateDaemon {
    service: Arc<OrchestrateService>,
    socket_paths: OrchestrateSocketPaths,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct OrchestrateSocketPaths {
    ordinary: PathBuf,
    meta: PathBuf,
    upgrade: PathBuf,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum OrchestrateListener {
    Ordinary,
    Meta,
    Upgrade,
}

struct OrchestrateRuntime {
    service: Arc<OrchestrateService>,
    socket_paths: OrchestrateSocketPaths,
    workers: BoundedWorkers,
}

struct OrchestrateRequestWorker {
    service: Arc<OrchestrateService>,
    socket_paths: OrchestrateSocketPaths,
}

impl OrchestrateDaemon {
    pub fn open(configuration: DaemonConfiguration) -> Result<Self> {
        let layout = OrchestrateLayout::new(
            PathBuf::from(configuration.workspace_root.as_str()),
            PathBuf::from(configuration.git_index_root.as_str()),
        );
        let service = OrchestrateService::open_with_layout(
            &StoreLocation::new(configuration.store_path.as_str()),
            layout,
        )?;
        Ok(Self {
            service: Arc::new(service),
            socket_paths: OrchestrateSocketPaths::new(
                PathBuf::from(configuration.ordinary_socket_path.as_str()),
                PathBuf::from(configuration.meta_socket_path.as_str()),
                PathBuf::from(configuration.upgrade_socket_path.as_str()),
            ),
        })
    }

    pub fn run(self) -> Result<()> {
        self.socket_paths.validate_bind_preconditions()?;
        let sockets = self.socket_paths.listener_sockets();
        let runtime = OrchestrateRuntime::new(self.service, self.socket_paths);
        MultiListenerDaemon::new(sockets, runtime, RequestErrorLog::new("orchestrate-daemon"))
            .run()
            .map_err(Self::map_daemon_error)
    }

    fn map_daemon_error(error: MultiListenerDaemonError<Error, Error>) -> Error {
        match error {
            MultiListenerDaemonError::Listener(error) => Error::DaemonListener(error),
            MultiListenerDaemonError::Start(error) | MultiListenerDaemonError::Stop(error) => error,
        }
    }
}

impl OrchestrateSocketPaths {
    fn new(ordinary: PathBuf, meta: PathBuf, upgrade: PathBuf) -> Self {
        Self {
            ordinary,
            meta,
            upgrade,
        }
    }

    fn listener_sockets(&self) -> Vec<ListenerSocket<OrchestrateListener>> {
        vec![
            ListenerSocket::new(OrchestrateListener::Ordinary, self.ordinary.clone()),
            ListenerSocket::new(OrchestrateListener::Meta, self.meta.clone()),
            ListenerSocket::new(OrchestrateListener::Upgrade, self.upgrade.clone()),
        ]
    }

    fn validate_bind_preconditions(&self) -> Result<()> {
        self.validate_bind_path(&self.ordinary)?;
        self.validate_bind_path(&self.meta)?;
        self.validate_bind_path(&self.upgrade)
    }

    fn validate_bind_path(&self, path: &Path) -> Result<()> {
        match std::fs::metadata(path) {
            Ok(metadata) if metadata.file_type().is_socket() => Ok(()),
            Ok(_) => Err(Error::SocketPathIsNotSocket(path.display().to_string())),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error.into()),
        }
    }

    fn retire_public_sockets(&self) -> Result<()> {
        self.remove_socket_path(&self.ordinary)?;
        self.remove_socket_path(&self.meta)
    }

    fn remove_socket_path(&self, path: &Path) -> Result<()> {
        match std::fs::metadata(path) {
            Ok(metadata) if metadata.file_type().is_socket() => {
                std::fs::remove_file(path)?;
                Ok(())
            }
            Ok(_) => Err(Error::SocketPathIsNotSocket(path.display().to_string())),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error.into()),
        }
    }
}

impl Display for OrchestrateListener {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Ordinary => formatter.write_str("ordinary"),
            Self::Meta => formatter.write_str("meta"),
            Self::Upgrade => formatter.write_str("upgrade"),
        }
    }
}

impl OrchestrateRuntime {
    fn new(service: Arc<OrchestrateService>, socket_paths: OrchestrateSocketPaths) -> Self {
        Self {
            service,
            socket_paths,
            workers: BoundedWorkers::new(MAXIMUM_CONCURRENT_REQUESTS),
        }
    }
}

impl MultiListenerRuntime for OrchestrateRuntime {
    type Listener = OrchestrateListener;
    type StartError = Error;
    type StopError = Error;
    type RequestError = Error;

    fn start(&mut self) -> Result<()> {
        Ok(())
    }

    fn stop(&mut self) -> Result<()> {
        Ok(())
    }

    fn handle_stream(&mut self, listener: Self::Listener, stream: UnixStream) -> Result<()> {
        let worker =
            OrchestrateRequestWorker::new(Arc::clone(&self.service), self.socket_paths.clone());
        self.workers
            .dispatch(move || worker.serve(listener, stream));
        Ok(())
    }
}

impl OrchestrateRequestWorker {
    fn new(service: Arc<OrchestrateService>, socket_paths: OrchestrateSocketPaths) -> Self {
        Self {
            service,
            socket_paths,
        }
    }

    fn serve(self, listener: OrchestrateListener, mut stream: UnixStream) {
        let response = match listener {
            OrchestrateListener::Ordinary => self.handle_ordinary_stream(&mut stream),
            OrchestrateListener::Meta => self.handle_meta_stream(&mut stream),
            OrchestrateListener::Upgrade => self.handle_upgrade_stream(&mut stream),
        };
        if let Ok(response) = response {
            let _ = stream.write_all(&response);
        }
    }

    fn handle_ordinary_stream(&self, stream: &mut UnixStream) -> Result<Vec<u8>> {
        let bytes = self.read_length_prefixed(stream)?;
        let frame = OrchestrateFrame::decode_length_prefixed(&bytes)?;
        let short_header = frame.short_header();
        let OrchestrateFrameBody::Request { exchange, request } = frame.into_body() else {
            return Err(Error::SocketExpectedRequestFrame);
        };
        self.validate_ordinary_request_header(short_header, &request)?;

        let reply = self.service.handle_request(request);

        OrchestrateFrame::new(OrchestrateFrameBody::Reply { exchange, reply })
            .encode_length_prefixed()
            .map_err(Error::SignalFrame)
    }

    fn handle_meta_stream(&self, stream: &mut UnixStream) -> Result<Vec<u8>> {
        let bytes = self.read_length_prefixed(stream)?;
        let frame = MetaOrchestrateFrame::decode_length_prefixed(&bytes)?;
        let short_header = frame.short_header();
        let MetaOrchestrateFrameBody::Request { exchange, request } = frame.into_body() else {
            return Err(Error::SocketExpectedRequestFrame);
        };
        self.validate_meta_request_header(short_header, &request)?;

        let reply = self.service.handle_meta_request(request);

        MetaOrchestrateFrame::new(MetaOrchestrateFrameBody::Reply { exchange, reply })
            .encode_length_prefixed()
            .map_err(Error::SignalFrame)
    }

    fn handle_upgrade_stream(&self, stream: &mut UnixStream) -> Result<Vec<u8>> {
        let bytes = self.read_length_prefixed(stream)?;
        let frame = UpgradeFrame::decode_length_prefixed(&bytes)?;
        let short_header = frame.short_header();
        let UpgradeFrameBody::Request { exchange, request } = frame.into_body() else {
            return Err(Error::SocketExpectedRequestFrame);
        };
        self.validate_upgrade_request_header(short_header, &request)?;

        let reply = self.service.handle_upgrade_request(request);
        if self.reply_finalized_handover(&reply) {
            self.socket_paths.retire_public_sockets()?;
        }

        UpgradeFrame::new(UpgradeFrameBody::Reply { exchange, reply })
            .encode_length_prefixed()
            .map_err(Error::SignalFrame)
    }

    fn read_length_prefixed(&self, stream: &mut UnixStream) -> Result<Vec<u8>> {
        let mut prefix = [0_u8; 4];
        stream.read_exact(&mut prefix)?;
        let length = u32::from_be_bytes(prefix) as usize;
        if length > MAX_FRAME_LENGTH {
            return Err(Error::FrameTooLarge { length });
        }
        let mut payload = vec![0_u8; length];
        stream.read_exact(&mut payload)?;
        let mut bytes = Vec::with_capacity(4 + length);
        bytes.extend_from_slice(&prefix);
        bytes.extend_from_slice(&payload);
        Ok(bytes)
    }

    fn validate_ordinary_request_header(
        &self,
        short_header: ShortHeader,
        request: &Request<OrchestrateRequest>,
    ) -> Result<()> {
        let expected = short_header.to_le_bytes()[0];
        let expected_kind = OrchestrateRequest::kind_from_short_header(short_header)
            .ok_or(OperationDispatchError::UnknownOperationRoot { root: expected })?;
        let actual_kind = request.payloads().head().kind();
        if actual_kind != expected_kind {
            return Err(OperationDispatchError::HeaderOperationMismatch {
                expected,
                actual: actual_kind as u8,
            }
            .into());
        }
        Ok(())
    }

    fn validate_meta_request_header(
        &self,
        short_header: ShortHeader,
        request: &Request<MetaOrchestrateRequest>,
    ) -> Result<()> {
        let expected = short_header.to_le_bytes()[0];
        let expected_kind = MetaOrchestrateRequest::kind_from_short_header(short_header)
            .ok_or(OperationDispatchError::UnknownOperationRoot { root: expected })?;
        let actual_kind = request.payloads().head().kind();
        if actual_kind != expected_kind {
            return Err(OperationDispatchError::HeaderOperationMismatch {
                expected,
                actual: actual_kind as u8,
            }
            .into());
        }
        Ok(())
    }

    fn validate_upgrade_request_header(
        &self,
        short_header: ShortHeader,
        request: &Request<UpgradeOperation>,
    ) -> Result<()> {
        let expected = short_header.to_le_bytes()[0];
        let expected_kind = UpgradeOperation::kind_from_short_header(short_header)
            .ok_or(OperationDispatchError::UnknownOperationRoot { root: expected })?;
        let actual_kind = request.payloads().head().kind();
        if actual_kind != expected_kind {
            return Err(OperationDispatchError::HeaderOperationMismatch {
                expected,
                actual: actual_kind as u8,
            }
            .into());
        }
        Ok(())
    }

    fn reply_finalized_handover(&self, reply: &Reply<UpgradeReply>) -> bool {
        let Reply::Accepted {
            outcome: AcceptedOutcome::Committed,
            per_operation,
        } = reply
        else {
            return false;
        };
        per_operation
            .iter()
            .any(|sub_reply| matches!(sub_reply, SubReply::Ok(UpgradeReply::HandoverFinalized(_))))
    }
}
