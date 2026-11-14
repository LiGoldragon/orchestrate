use std::process::ExitCode;

use orchestrate_nexus::{
    DefaultConfiguration, OpensStore, OrchestrateStore, ReadsDefaultConfiguration, ReportsUpgrade,
    UpgradesDefaultStore,
    transport::{Binding, Serving, StopsOnSignal, Termination, TransportRuntime},
};

fn main() -> ExitCode {
    match DefaultConfiguration::from_process()
        .and_then(DefaultConfiguration::upgrade_store_path)
        .map_err(|error| error.to_string())
        .and_then(|defaults| {
            let opened = OrchestrateStore::open(defaults.store_path(), defaults.configuration());
            // The upgrade is told whether or not the open then succeeds: a
            // store upgraded and then refused is a memory store from now on.
            if let Some(line) = opened
                .as_ref()
                .map(|(store, _)| store)
                .upgrade_line(defaults.store_path())
            {
                println!("{line}");
            }
            let (store, configuration) = opened.map_err(|error| error.to_string())?;
            // Two workers, and two is the whole argument: the core is one
            // actor and its durable step is synchronous, so one worker runs
            // that step and one keeps accepting while it does. A
            // single-threaded runtime would let one commit stall both
            // listeners; more workers would buy nothing, because serialising
            // the store is the actor's job by design.
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .map_err(|error| error.to_string())?
                .block_on(async move {
                    let transport = TransportRuntime::bind(configuration, store)?;
                    println!("orchestrate-nexus ready");
                    transport.serve_until(Termination::asked()?).await
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
