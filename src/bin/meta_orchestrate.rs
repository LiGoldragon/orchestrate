//! The meta-orchestrate CLI — the daemon's meta-policy client.
//!
//! It accepts one Dotos argument for the canonical privileged contract,
//! exchanges it on `PERSONA_ORCHESTRATE_META_SOCKET`, and prints the reply.

use std::{env, fs, path::PathBuf, process::ExitCode};

use dotos::{DotosDecodeError, DotosEncode, DotosSource};
use meta_signal_orchestrate::MetaOrchestrateRequest;
use orchestrate::{MetaSignalTransport, TransportError};
use thiserror::Error;
use triad_runtime::{ArgumentError, ComponentArgument, ComponentCommand};

const META_SOCKET_VARIABLE: &str = "PERSONA_ORCHESTRATE_META_SOCKET";

fn main() -> ExitCode {
    match MetaOrchestrateCli::from_environment().run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("meta-orchestrate: {error}");
            ExitCode::FAILURE
        }
    }
}

struct MetaOrchestrateCli {
    command: ComponentCommand,
}

impl MetaOrchestrateCli {
    fn from_environment() -> Self {
        Self {
            command: ComponentCommand::from_environment(),
        }
    }

    fn run(&self) -> Result<(), MetaOrchestrateCliError> {
        let input = MetaRequestText::new(self.argument_text()?).parse()?;
        let output = MetaSignalTransport::connect(self.socket_path()?)?.exchange(&input)?;
        println!("{}", output.to_dotos());
        Ok(())
    }

    fn socket_path(&self) -> Result<String, MetaOrchestrateCliError> {
        match env::var(META_SOCKET_VARIABLE) {
            Ok(socket) => Ok(socket),
            Err(_) => Ok(Self::primary_workspace_socket()?.display().to_string()),
        }
    }

    fn primary_workspace_socket() -> Result<PathBuf, MetaOrchestrateCliError> {
        let home =
            env::var("HOME").map_err(|source| MetaOrchestrateCliError::HomeDirectory { source })?;
        Ok(PathBuf::from(home)
            .join("primary")
            .join("orchestrate")
            .join("orchestrate-owner.sock"))
    }

    fn argument_text(&self) -> Result<String, MetaOrchestrateCliError> {
        match self.command.dotos_argument()? {
            ComponentArgument::InlineDotos(argument) => Ok(argument.into_string()),
            ComponentArgument::DotosFile(file) => Self::read_dotos_file(file.into_path()),
            ComponentArgument::SignalFile(file) => Self::read_dotos_file(file.into_path()),
        }
    }

    fn read_dotos_file(path: PathBuf) -> Result<String, MetaOrchestrateCliError> {
        fs::read_to_string(&path)
            .map_err(|source| MetaOrchestrateCliError::ReadDotosFile { path, source })
    }
}

/// The unparsed CLI Dotos argument awaiting meta-contract decoding.
struct MetaRequestText {
    text: String,
}

impl MetaRequestText {
    fn new(text: String) -> Self {
        Self { text }
    }

    fn parse(self) -> Result<MetaOrchestrateRequest, MetaOrchestrateCliError> {
        DotosSource::new(&self.text)
            .parse::<MetaOrchestrateRequest>()
            .map_err(MetaOrchestrateCliError::DotosDecode)
    }
}

#[derive(Debug, Error)]
enum MetaOrchestrateCliError {
    #[error("component argument error: {0}")]
    Argument(#[from] ArgumentError),

    #[error("failed to read Dotos file {}: {source}", path.display())]
    ReadDotosFile {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("invalid meta orchestrate request Dotos: {0}")]
    DotosDecode(DotosDecodeError),

    #[error("HOME environment variable is unavailable: {source}")]
    HomeDirectory {
        #[source]
        source: env::VarError,
    },

    #[error("transport error: {0}")]
    Transport(#[from] TransportError),
}
