use std::{
    env,
    io::{Read, Write},
    os::unix::net::UnixStream,
    process::ExitCode,
};

use dotos::{DotosEncode, DotosSource};
use meta_signal_orchestrate::{Configure, Frame, MetaOrchestrateReply, MetaOrchestrateRequest};
use signal_frame::{
    ClientFrame, ExchangeIdentifier, ExchangeLane, LaneSequence, Reply, RequestPayload,
    SessionEpoch, SubReply,
};

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("meta-orchestrate: {error}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), String> {
    let text = single_argument()?;
    let configure = DotosSource::new(&text)
        .parse::<Configure>()
        .map_err(|error| error.to_string())?;
    let reply = exchange(MetaOrchestrateRequest::Configure(configure))?;
    println!(
        "{}",
        match reply {
            MetaOrchestrateReply::Configured(value) => value.to_dotos(),
            MetaOrchestrateReply::ConfigurationRejected(value) => value.to_dotos(),
        }
    );
    Ok(())
}

fn exchange(request: MetaOrchestrateRequest) -> Result<MetaOrchestrateReply, String> {
    let exchange = ExchangeIdentifier::new(
        SessionEpoch::new(1),
        ExchangeLane::Connector,
        LaneSequence::first(),
    );
    let frame = Frame::request_frame(exchange, request.into_request())
        .map_err(|error| error.to_string())?;
    let mut stream = UnixStream::connect(
        env::var("ORCHESTRATE_META_SOCKET")
            .map_err(|_| "ORCHESTRATE_META_SOCKET is required".to_owned())?,
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
    match reply {
        Reply::Accepted { per_operation, .. } => match per_operation.into_head() {
            SubReply::Ok(value)
            | SubReply::Failed {
                detail: Some(value),
                ..
            } => Ok(value),
            other => Err(format!("unexpected daemon reply: {other:?}")),
        },
        Reply::Rejected { reason } => Err(reason.to_string()),
    }
}

fn single_argument() -> Result<String, String> {
    let values: Vec<_> = env::args().skip(1).collect();
    match values.as_slice() {
        [value] if !value.starts_with('-') => Ok(value.clone()),
        _ => Err("accepts exactly one Datom object and no flags".to_owned()),
    }
}
