//! The orchestrate CLI — the daemon's first client.
//!
//! It speaks the schema-emitted `Input` frame the actor-shell daemon decodes
//! (the same short-header wire spirit's CLI speaks), not the contract
//! `ExchangeFrame` the retired `signal_cli!` client sent. One NOTA argument is
//! parsed as the ordinary working `Input`, exchanged on
//! `PERSONA_ORCHESTRATE_SOCKET`, and the reply `Output` is printed as NOTA.
//! Meta-policy requests belong to the sibling `meta-orchestrate` CLI.

use std::{env, fs, path::PathBuf, process::ExitCode};

use nota_next::{NotaDecodeError, NotaSource};
use orchestrate::{OrdinarySignalTransport, TransportError};
use signal_orchestrate::schema::lib::Input;
use thiserror::Error;
use triad_runtime::{ArgumentError, ComponentArgument, ComponentCommand};

const ORDINARY_SOCKET_VARIABLE: &str = "PERSONA_ORCHESTRATE_SOCKET";
const DEFAULT_SOCKET_DIRECTORY: &str = "orchestrate";
const ORDINARY_SOCKET_FILE: &str = "orchestrate.sock";

fn main() -> ExitCode {
    match OrchestrateCli::from_environment().run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("orchestrate: {error}");
            ExitCode::FAILURE
        }
    }
}

struct OrchestrateCli {
    command: ComponentCommand,
}

impl OrchestrateCli {
    fn from_environment() -> Self {
        Self {
            command: ComponentCommand::from_environment(),
        }
    }

    fn run(&self) -> Result<(), OrchestrateCliError> {
        let input = RequestText::new(self.argument_text()?).parse()?;
        let (_route, output) =
            OrdinarySignalTransport::connect(self.socket_path()?)?.exchange(&input)?;
        println!("{output}");
        Ok(())
    }

    fn socket_path(&self) -> Result<String, OrchestrateCliError> {
        match env::var(ORDINARY_SOCKET_VARIABLE) {
            Ok(socket) => Ok(socket),
            Err(_) => Ok(RuntimeSocketPath::ordinary()?.display().to_string()),
        }
    }

    fn argument_text(&self) -> Result<String, OrchestrateCliError> {
        match self.command.nota_argument()? {
            ComponentArgument::InlineNota(argument) => Ok(argument.into_string()),
            ComponentArgument::NotaFile(file) => Self::read_nota_file(file.into_path()),
            ComponentArgument::SignalFile(file) => Self::read_nota_file(file.into_path()),
        }
    }

    fn read_nota_file(path: PathBuf) -> Result<String, OrchestrateCliError> {
        fs::read_to_string(&path)
            .map_err(|source| OrchestrateCliError::ReadNotaFile { path, source })
    }
}

struct RuntimeSocketPath {
    path: PathBuf,
}

impl RuntimeSocketPath {
    fn ordinary() -> Result<Self, OrchestrateCliError> {
        Ok(Self {
            path: XdgRuntimeDirectory::from_environment()?
                .join(DEFAULT_SOCKET_DIRECTORY)
                .join(ORDINARY_SOCKET_FILE),
        })
    }

    fn display(&self) -> std::path::Display<'_> {
        self.path.display()
    }
}

struct XdgRuntimeDirectory {
    path: PathBuf,
}

impl XdgRuntimeDirectory {
    fn from_environment() -> Result<Self, OrchestrateCliError> {
        Ok(Self {
            path: PathBuf::from(
                env::var("XDG_RUNTIME_DIR")
                    .map_err(|source| OrchestrateCliError::RuntimeDirectory { source })?,
            ),
        })
    }

    fn join(&self, segment: impl AsRef<std::path::Path>) -> PathBuf {
        self.path.join(segment)
    }
}

/// The unparsed CLI NOTA argument awaiting ordinary-contract decoding.
struct RequestText {
    text: String,
}

impl RequestText {
    fn new(text: String) -> Self {
        Self { text }
    }

    fn parse(self) -> Result<Input, OrchestrateCliError> {
        NotaSource::new(&self.text)
            .parse::<Input>()
            .map_err(OrchestrateCliError::NotaDecode)
    }
}

#[derive(Debug, Error)]
enum OrchestrateCliError {
    #[error("component argument error: {0}")]
    Argument(#[from] ArgumentError),

    #[error("failed to read NOTA file {}: {source}", path.display())]
    ReadNotaFile {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("invalid ordinary orchestrate request NOTA: {0}")]
    NotaDecode(NotaDecodeError),

    #[error("XDG_RUNTIME_DIR environment variable is unavailable: {source}")]
    RuntimeDirectory {
        #[source]
        source: env::VarError,
    },

    #[error("transport error: {0}")]
    Transport(#[from] TransportError),
}
