//! Explicit offline importer for the retired Signal durable representation.
use orchestrate::OrchestrateStore;
use std::{env, path::Path, process::ExitCode};

fn main() -> ExitCode {
    let arguments = env::args().skip(1).collect::<Vec<_>>();
    let [store_path] = arguments.as_slice() else {
        eprintln!("orchestrate-store-migrate: accepts one absolute store path");
        return ExitCode::FAILURE;
    };
    let path = Path::new(store_path);
    if !path.is_absolute() {
        eprintln!("orchestrate-store-migrate: store path must be absolute");
        return ExitCode::FAILURE;
    }
    match OrchestrateStore::migrate_previous_signal(path) {
        Ok(()) => {
            println!("orchestrate-store-migrate: migrated v1 Signal records to v2");
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("orchestrate-store-migrate: {error}");
            ExitCode::FAILURE
        }
    }
}
