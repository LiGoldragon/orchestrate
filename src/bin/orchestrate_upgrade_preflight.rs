use std::process::ExitCode;

use orchestrate::{DefaultConfiguration, LegacyStorePreflight, PreflightsLegacyStore};

fn main() -> ExitCode {
    match DefaultConfiguration::from_process()
        .map_err(|error| error.to_string())
        .and_then(|defaults| {
            <LegacyStorePreflight as PreflightsLegacyStore>::inspect(defaults.store_path())
                .map_err(|error| error.to_string())
        }) {
        Ok(preflight) => {
            println!(
                "active legacy PathLock rows: {}",
                preflight.active_lock_count()
            );
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("orchestrate-upgrade-preflight: {error}");
            ExitCode::FAILURE
        }
    }
}
