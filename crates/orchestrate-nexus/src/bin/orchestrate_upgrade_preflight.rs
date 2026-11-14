use std::process::ExitCode;

use orchestrate_nexus::{
    DefaultConfiguration, LegacyStorePreflight, PreflightsLegacyStore, ReadsDefaultConfiguration,
    ReportsUpgrade,
};

fn main() -> ExitCode {
    match DefaultConfiguration::from_process()
        .map_err(|error| error.to_string())
        .and_then(|defaults| {
            let inspected =
                <LegacyStorePreflight as PreflightsLegacyStore>::inspect(defaults.store_path());
            // The open may upgrade a store sema-engine wrote, as the Nexus's
            // does; it is told in the Nexus's own line either way.
            if let Some(line) = inspected.upgrade_line(defaults.store_path()) {
                println!("{line}");
            }
            inspected.map_err(|error| error.to_string())
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
