use std::{
    env,
    io::{Read, Write},
    os::unix::net::UnixStream,
    process::ExitCode,
};

use dotos::{DotosEncode, DotosSource};
use signal_frame::{
    ClientFrame, ExchangeIdentifier, ExchangeLane, LaneSequence, Reply, RequestPayload,
    SessionEpoch, SubReply,
};
use signal_orchestrate::{Frame, OrchestrateReply, OrchestrateRequest, PathLock, PathLockRelease};

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("orchestrate: {error}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), String> {
    let text = single_argument()?;
    let request = DotosSource::new(&text)
        .parse::<PathLock>()
        .map(OrchestrateRequest::Register)
        .or_else(|_| {
            DotosSource::new(&text)
                .parse::<PathLockRelease>()
                .map(OrchestrateRequest::Release)
        })
        .map_err(|error| error.to_string())?;
    let reply = exchange(request)?;
    println!(
        "{}",
        match reply {
            OrchestrateReply::PathLockRegistered(value) => value.to_dotos(),
            OrchestrateReply::PathLockRegistrationRejected(value) => value.to_dotos(),
            OrchestrateReply::PathLockReleased(value) => value.to_dotos(),
            OrchestrateReply::PathLockReleaseRejected(value) => value.to_dotos(),
        }
    );
    Ok(())
}

fn exchange(request: OrchestrateRequest) -> Result<OrchestrateReply, String> {
    let exchange = exchange_identifier();
    let frame = Frame::request_frame(exchange, request.into_request())
        .map_err(|error| error.to_string())?;
    let mut stream = UnixStream::connect(
        env::var("ORCHESTRATE_SOCKET").map_err(|_| "ORCHESTRATE_SOCKET is required".to_owned())?,
    )
    .map_err(|error| error.to_string())?;
    stream
        .write_all(
            &frame
                .encode_client_frame()
                .map_err(|error| error.to_string())?,
        )
        .map_err(|error| error.to_string())?;
    stream
        .shutdown(std::net::Shutdown::Write)
        .map_err(|error| error.to_string())?;
    let mut bytes = Vec::new();
    stream
        .read_to_end(&mut bytes)
        .map_err(|error| error.to_string())?;
    let reply = Frame::decode_client_frame(&bytes)
        .map_err(|error| error.to_string())?
        .reply_from_frame(exchange)
        .map_err(|error| error.to_string())?;
    take_payload(reply)
}

fn take_payload(reply: Reply<OrchestrateReply>) -> Result<OrchestrateReply, String> {
    match reply {
        Reply::Accepted { per_operation, .. } => match per_operation.into_head() {
            SubReply::Ok(value)
            | SubReply::Failed {
                detail: Some(value),
                ..
            } => Ok(value),
            other => Err(format!("unexpected Orchestrate Nexus reply: {other:?}")),
        },
        Reply::Rejected { reason } => Err(reason.to_string()),
    }
}

fn exchange_identifier() -> ExchangeIdentifier {
    ExchangeIdentifier::new(
        SessionEpoch::new(1),
        ExchangeLane::Connector,
        LaneSequence::first(),
    )
}

fn single_argument() -> Result<String, String> {
    let values: Vec<_> = env::args().skip(1).collect();
    match values.as_slice() {
        [value] if !value.starts_with('-') => Ok(value.clone()),
        _ => Err("accepts exactly one Datom object and no flags".to_owned()),
    }
}
