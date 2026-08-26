use std::{
    env,
    io::{Read, Write},
    os::unix::net::UnixStream,
    process::ExitCode,
};

use datom::DatomText;
use protos::{Realize, SourceText};
use signal_frame_ordinary::{
    ExchangeFrameBody, ExchangeIdentifier, ExchangeLane, LaneSequence, Reply, RequestPayload,
    SessionEpoch, SubReply,
};
use signal_orchestrate::{Frame, OrchestrateReply, OrchestrateRequest};

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
    let request = DatomText::<OrchestrateRequest>::from(SourceText(text))
        .realize()
        .map_err(|error| format!("{error:?}"))?;
    let reply = exchange(request)?;
    println!("{reply:?}");
    Ok(())
}

fn exchange(request: OrchestrateRequest) -> Result<OrchestrateReply, String> {
    let exchange = exchange_identifier();
    let request = request.into_request();
    let route = request.route().map_err(|error| error.to_string())?;
    let frame = Frame::new(route, ExchangeFrameBody::Request { exchange, request });
    let mut stream = UnixStream::connect(
        env::var("ORCHESTRATE_SOCKET").map_err(|_| "ORCHESTRATE_SOCKET is required".to_owned())?,
    )
    .map_err(|error| error.to_string())?;
    stream
        .write_all(
            &frame
                .encode_length_prefixed()
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
    let reply = Frame::decode_length_prefixed(&bytes).map_err(|error| error.to_string())?;
    match reply.into_body() {
        ExchangeFrameBody::Reply {
            exchange: actual,
            reply,
        } if actual == exchange => take_payload(reply),
        ExchangeFrameBody::Reply { .. } => Err("reply exchange does not match request".to_owned()),
        _ => Err("expected an Orchestrate Nexus reply frame".to_owned()),
    }
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
