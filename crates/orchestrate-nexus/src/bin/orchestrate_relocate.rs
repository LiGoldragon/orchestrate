//! Declares that this user's Orchestrate store was moved to where it now is.
//!
//! Takes no arguments, for the same reason the Nexus takes none: it derives
//! the store path from the process environment exactly as the Nexus does, so
//! it can only ever act on the store the Nexus would open. Run it in the
//! environment the service runs in, with the service stopped, after the move
//! has actually been made.
//!
//! It writes nothing unless the move is both real and unopposed — see
//! `orchestrate_nexus::recovery`. Every refusal leaves the store as it was,
//! but for the upgrade from the sema format the engine's open makes of a
//! store sema-engine wrote; that upgrade is printed in the Nexus's own line
//! whether or not the move is then declared.

use std::process::ExitCode;

use orchestrate_nexus::{
    DefaultConfiguration, ReadsDefaultConfiguration, ReportsUpgrade,
    recovery::{DeclaresRelocation, StoreRelocation},
};

fn main() -> ExitCode {
    match DefaultConfiguration::from_process()
        .map_err(|error| error.to_string())
        .and_then(|defaults| {
            let declared = StoreRelocation::declare(defaults.store_path());
            // The upgrade is told whether or not the move is then declared,
            // in the Nexus's own line.
            if let Some(line) = declared.upgrade_line(defaults.store_path()) {
                println!("{line}");
            }
            declared.map_err(|error| error.to_string())
        }) {
        Ok(declared) => {
            println!(
                "declared: the store bound at {:?} is now at {:?}",
                declared.origin(),
                declared.destination()
            );
            for socket_path in declared.vacated_socket_vector() {
                println!("free to bind: {socket_path:?}");
            }
            println!("start orchestrate-nexus to complete the move");
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("orchestrate-relocate: {error}");
            ExitCode::FAILURE
        }
    }
}
