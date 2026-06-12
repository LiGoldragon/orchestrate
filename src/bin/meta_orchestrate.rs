//! The meta-orchestrate CLI — the daemon's meta-policy client.
//!
//! It accepts one NOTA argument for the schema-emitted
//! `meta_signal_orchestrate::schema::lib::Input`, exchanges it on
//! `PERSONA_ORCHESTRATE_META_SOCKET`, and prints the meta `Output` as NOTA.

use std::{env, fs, path::PathBuf, process::ExitCode};

use meta_signal_orchestrate::schema::lib::Input;
use nota_next::{NotaDecodeError, NotaSource};
use orchestrate::{MetaSignalTransport, TransportError};
use thiserror::Error;
use triad_runtime::{ArgumentError, ComponentArgument, ComponentCommand};

const META_SOCKET_VARIABLE: &str = "PERSONA_ORCHESTRATE_META_SOCKET";
const DEFAULT_META_SOCKET: &str = "/tmp/orchestrate-meta.sock";

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
        let socket =
            env::var(META_SOCKET_VARIABLE).unwrap_or_else(|_| String::from(DEFAULT_META_SOCKET));
        let (_route, output) = MetaSignalTransport::connect(socket)?.exchange(&input)?;
        println!("{output}");
        Ok(())
    }

    fn argument_text(&self) -> Result<String, MetaOrchestrateCliError> {
        match self.command.nota_argument()? {
            ComponentArgument::InlineNota(argument) => Ok(argument.into_string()),
            ComponentArgument::NotaFile(file) => Self::read_nota_file(file.into_path()),
            ComponentArgument::SignalFile(file) => Self::read_nota_file(file.into_path()),
        }
    }

    fn read_nota_file(path: PathBuf) -> Result<String, MetaOrchestrateCliError> {
        fs::read_to_string(&path)
            .map_err(|source| MetaOrchestrateCliError::ReadNotaFile { path, source })
    }
}

/// The unparsed CLI NOTA argument awaiting meta-contract decoding.
struct MetaRequestText {
    text: String,
}

impl MetaRequestText {
    fn new(text: String) -> Self {
        Self { text }
    }

    fn parse(self) -> Result<Input, MetaOrchestrateCliError> {
        NotaSource::new(&self.text)
            .parse::<Input>()
            .map_err(MetaOrchestrateCliError::NotaDecode)
    }
}

#[derive(Debug, Error)]
enum MetaOrchestrateCliError {
    #[error("component argument error: {0}")]
    Argument(#[from] ArgumentError),

    #[error("failed to read NOTA file {}: {source}", path.display())]
    ReadNotaFile {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("invalid meta orchestrate request NOTA: {0}")]
    NotaDecode(NotaDecodeError),

    #[error("transport error: {0}")]
    Transport(#[from] TransportError),
}
