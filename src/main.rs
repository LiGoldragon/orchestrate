use std::{env, process::ExitCode};

use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use meta_signal_orchestrate::{Frame, MetaOrchestrateRequest};
use orchestrate::{OrchestrateStore, transport};
use signal_frame::{ClientFrame, ExchangeFrameBody};

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("orchestrate-nexus: {error}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), String> {
    let startup = startup_configure()?;
    let (store, persisted) = OrchestrateStore::open(startup).map_err(|error| error.to_string())?;
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| error.to_string())?
        .block_on(transport::run(persisted, store))
        .map_err(|error| error.to_string())
}

fn startup_configure() -> Result<meta_signal_orchestrate::Configure, String> {
    let values: Vec<_> = env::args().skip(1).collect();
    let [encoded] = values.as_slice() else {
        return Err(
            "accepts exactly one URL-safe unpadded base64 Signal Configure frame".to_owned(),
        );
    };
    let bytes = URL_SAFE_NO_PAD
        .decode(encoded)
        .map_err(|error| error.to_string())?;
    let frame = Frame::decode_client_frame(&bytes).map_err(|error| error.to_string())?;
    let ExchangeFrameBody::Request { request, .. } = frame.into_body() else {
        return Err("startup Signal must be a meta request frame".to_owned());
    };
    if !request.payloads().tail().is_empty() {
        return Err("startup Signal carries exactly one Configure operation".to_owned());
    }
    match request.payloads().head() {
        MetaOrchestrateRequest::Configure(configure) => Ok(configure.clone()),
    }
}
