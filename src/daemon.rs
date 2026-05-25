use std::io::{Read, Write};
use std::os::unix::fs::FileTypeExt;
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread;

use owner_signal_orchestrate::{
    Frame as OwnerOrchestrateFrame, FrameBody as OwnerOrchestrateFrameBody, OwnerOrchestrateRequest,
};
use signal_frame::{
    AcceptedOutcome, OperationDispatchError, Reply, Request, ShortHeader, SubReply,
};
use signal_orchestrate::{OrchestrateFrame, OrchestrateFrameBody, OrchestrateRequest};
use signal_version_handover::{
    Frame as UpgradeFrame, FrameBody as UpgradeFrameBody, Operation as UpgradeOperation,
    Reply as UpgradeReply,
};

use crate::{
    DaemonConfiguration, Error, OrchestrateLayout, OrchestrateService, Result, StoreLocation,
};

const MAX_FRAME_LENGTH: usize = 16 * 1024 * 1024;

pub struct OrchestrateDaemon {
    service: Arc<OrchestrateService>,
    ordinary_socket_path: PathBuf,
    owner_socket_path: PathBuf,
    upgrade_socket_path: PathBuf,
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
            ordinary_socket_path: PathBuf::from(configuration.ordinary_socket_path.as_str()),
            owner_socket_path: PathBuf::from(configuration.owner_socket_path.as_str()),
            upgrade_socket_path: PathBuf::from(configuration.upgrade_socket_path.as_str()),
        })
    }

    pub fn run(self) -> Result<()> {
        let ordinary_listener = bind_socket(&self.ordinary_socket_path)?;
        let owner_listener = bind_socket(&self.owner_socket_path)?;
        let upgrade_listener = bind_socket(&self.upgrade_socket_path)?;
        let ordinary_service = Arc::clone(&self.service);
        let owner_service = Arc::clone(&self.service);
        let upgrade_service = Arc::clone(&self.service);
        let ordinary_socket_path = self.ordinary_socket_path.clone();
        let owner_socket_path = self.owner_socket_path.clone();

        let ordinary_thread =
            thread::spawn(move || accept_ordinary(ordinary_listener, ordinary_service));
        let owner_thread = thread::spawn(move || accept_owner(owner_listener, owner_service));
        let upgrade_thread = thread::spawn(move || {
            accept_upgrade(
                upgrade_listener,
                upgrade_service,
                ordinary_socket_path,
                owner_socket_path,
            )
        });

        ordinary_thread
            .join()
            .map_err(|_| Error::DaemonThreadPanicked)??;
        owner_thread
            .join()
            .map_err(|_| Error::DaemonThreadPanicked)??;
        upgrade_thread
            .join()
            .map_err(|_| Error::DaemonThreadPanicked)??;
        Ok(())
    }
}

fn bind_socket(path: &Path) -> Result<UnixListener> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    if path.exists() {
        let metadata = std::fs::metadata(path)?;
        if metadata.file_type().is_socket() {
            std::fs::remove_file(path)?;
        } else {
            return Err(Error::SocketPathIsNotSocket(path.display().to_string()));
        }
    }
    Ok(UnixListener::bind(path)?)
}

fn accept_ordinary(listener: UnixListener, service: Arc<OrchestrateService>) -> Result<()> {
    for stream in listener.incoming() {
        let mut stream = stream?;
        let service = Arc::clone(&service);
        thread::spawn(move || {
            if let Ok(response) = handle_ordinary_stream(&mut stream, &service) {
                let _ = stream.write_all(&response);
            }
        });
    }
    Ok(())
}

fn accept_owner(listener: UnixListener, service: Arc<OrchestrateService>) -> Result<()> {
    for stream in listener.incoming() {
        let mut stream = stream?;
        let service = Arc::clone(&service);
        thread::spawn(move || {
            if let Ok(response) = handle_owner_stream(&mut stream, &service) {
                let _ = stream.write_all(&response);
            }
        });
    }
    Ok(())
}

fn accept_upgrade(
    listener: UnixListener,
    service: Arc<OrchestrateService>,
    ordinary_socket_path: PathBuf,
    owner_socket_path: PathBuf,
) -> Result<()> {
    for stream in listener.incoming() {
        let mut stream = stream?;
        let service = Arc::clone(&service);
        let ordinary_socket_path = ordinary_socket_path.clone();
        let owner_socket_path = owner_socket_path.clone();
        thread::spawn(move || {
            if let Ok(response) = handle_upgrade_stream(
                &mut stream,
                &service,
                &ordinary_socket_path,
                &owner_socket_path,
            ) {
                let _ = stream.write_all(&response);
            }
        });
    }
    Ok(())
}

fn handle_ordinary_stream(
    stream: &mut UnixStream,
    service: &OrchestrateService,
) -> Result<Vec<u8>> {
    let bytes = read_length_prefixed(stream)?;
    let frame = OrchestrateFrame::decode_length_prefixed(&bytes)?;
    let short_header = frame.short_header();
    let OrchestrateFrameBody::Request { exchange, request } = frame.into_body() else {
        return Err(Error::SocketExpectedRequestFrame);
    };
    validate_ordinary_request_header(short_header, &request)?;

    let reply = service.handle_request(request);

    OrchestrateFrame::new(OrchestrateFrameBody::Reply { exchange, reply })
        .encode_length_prefixed()
        .map_err(Error::SignalFrame)
}

fn handle_owner_stream(stream: &mut UnixStream, service: &OrchestrateService) -> Result<Vec<u8>> {
    let bytes = read_length_prefixed(stream)?;
    let frame = OwnerOrchestrateFrame::decode_length_prefixed(&bytes)?;
    let short_header = frame.short_header();
    let OwnerOrchestrateFrameBody::Request { exchange, request } = frame.into_body() else {
        return Err(Error::SocketExpectedRequestFrame);
    };
    validate_owner_request_header(short_header, &request)?;

    let reply = service.handle_owner_request(request);

    OwnerOrchestrateFrame::new(OwnerOrchestrateFrameBody::Reply { exchange, reply })
        .encode_length_prefixed()
        .map_err(Error::SignalFrame)
}

fn handle_upgrade_stream(
    stream: &mut UnixStream,
    service: &OrchestrateService,
    ordinary_socket_path: &Path,
    owner_socket_path: &Path,
) -> Result<Vec<u8>> {
    let bytes = read_length_prefixed(stream)?;
    let frame = UpgradeFrame::decode_length_prefixed(&bytes)?;
    let short_header = frame.short_header();
    let UpgradeFrameBody::Request { exchange, request } = frame.into_body() else {
        return Err(Error::SocketExpectedRequestFrame);
    };
    validate_upgrade_request_header(short_header, &request)?;

    let reply = service.handle_upgrade_request(request);
    if reply_finalized_handover(&reply) {
        remove_socket_path(ordinary_socket_path)?;
        remove_socket_path(owner_socket_path)?;
    }

    UpgradeFrame::new(UpgradeFrameBody::Reply { exchange, reply })
        .encode_length_prefixed()
        .map_err(Error::SignalFrame)
}

fn read_length_prefixed(stream: &mut UnixStream) -> Result<Vec<u8>> {
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

fn validate_owner_request_header(
    short_header: ShortHeader,
    request: &Request<OwnerOrchestrateRequest>,
) -> Result<()> {
    let expected = short_header.to_le_bytes()[0];
    let expected_kind = OwnerOrchestrateRequest::kind_from_short_header(short_header)
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

fn reply_finalized_handover(reply: &Reply<UpgradeReply>) -> bool {
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

fn remove_socket_path(path: &Path) -> Result<()> {
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
