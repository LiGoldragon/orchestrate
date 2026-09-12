//! Declares that this user's Orchestrate store was moved to where it now is.
//!
//! Takes no arguments, for the same reason the Nexus takes none: it derives
//! the store path from the process environment exactly as the Nexus does, so
//! it can only ever act on the store the Nexus would open. Run it in the
//! environment the service runs in, with the service stopped, after the move
//! has actually been made.
//!
//! It writes nothing unless the move is both real and unopposed — see
//! `orchestrate_nexus::recovery`. Every refusal leaves the store as it was.

use std::process::ExitCode;

use orchestrate_nexus::{
    DefaultConfiguration, ReadsDefaultConfiguration,
    recovery::{DeclaresRelocation, StoreRelocation},
};

fn main() -> ExitCode {
    match DefaultConfiguration::from_process()
        .map_err(|error| error.to_string())
        .and_then(|defaults| {
            StoreRelocation::declare(defaults.store_path()).map_err(|error| error.to_string())
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
