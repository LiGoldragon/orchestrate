use std::process::ExitCode;

use orchestrate::{DefaultConfiguration, OrchestrateStore, transport};

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
                .block_on(transport::run(configuration, store))
                .map_err(|error| error.to_string())
        }) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("orchestrate-nexus: {error}");
            ExitCode::FAILURE
        }
    }
}
