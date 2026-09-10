use std::process::ExitCode;

use orchestrate_nexus::{
    DefaultConfiguration, Openable, OrchestrateStore, ReadsDefaultConfiguration,
    transport::{Bindable, TransportRuntime, Servable},
};

fn main() -> ExitCode {
    match DefaultConfiguration::from_process()
        .map_err(|error| error.to_string())
        .and_then(|defaults| {
            let (store, configuration) =
                OrchestrateStore::open(defaults.store_path(), defaults.configuration())
                    .map_err(|error| error.to_string())?;
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|error| error.to_string())?
                .block_on(async move {
                    let transport = TransportRuntime::bind(configuration, store)?;
                    println!("orchestrate-nexus ready");
                    let (shutdown_sender, shutdown) = tokio::sync::oneshot::channel();
                    let _shutdown_sender = shutdown_sender;
                    transport.serve_until(shutdown).await
                })
                .map_err(|error| error.to_string())
        }) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("orchestrate-nexus: {error}");
            ExitCode::FAILURE
        }
    }
}
