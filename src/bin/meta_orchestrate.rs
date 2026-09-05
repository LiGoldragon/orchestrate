//! Datom edge client for the privileged framed Orchestrate Signal.
use datom_codec::{Actualizable, IncorporationBudget, Potential, Textualizable};
use orchestrate::transport::MetaSignalTransport;
use std::{env, process::ExitCode};

fn main() -> ExitCode {
    let arguments = env::args().skip(1).collect::<Vec<_>>();
    let [source] = arguments.as_slice() else {
        eprintln!("meta-orchestrate: accepts one Datom request");
        return ExitCode::FAILURE;
    };
    let request = match Potential::<meta_signal_orchestrate::Request>::from(source.clone())
        .actualize(IncorporationBudget::try_from(10_000).expect("static budget"))
    {
        Ok(request) => request,
        Err(error) => {
            eprintln!("meta-orchestrate: invalid request: {error:?}");
            return ExitCode::FAILURE;
        }
    };
    let socket = match env::var("ORCHESTRATE_META_SOCKET") {
        Ok(path) => path,
        Err(_) => {
            eprintln!("meta-orchestrate: ORCHESTRATE_META_SOCKET is required");
            return ExitCode::FAILURE;
        }
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .build()
        .expect("Tokio runtime");
    match runtime.block_on(MetaSignalTransport::new(socket).request(request)) {
        Ok(response) => {
            println!("{}", response.textualize());
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("meta-orchestrate: {error}");
            ExitCode::FAILURE
        }
    }
}
