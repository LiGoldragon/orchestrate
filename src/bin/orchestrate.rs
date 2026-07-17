//! The orchestrate CLI — the daemon's first client.
//!
//! It speaks the schema-emitted `Input` frame the actor-shell daemon decodes
//! (the same short-header wire spirit's CLI speaks), not the contract
//! `ExchangeFrame` the retired `signal_cli!` client sent. One NOTA argument is
//! parsed as the ordinary working `Input`, exchanged on
//! `PERSONA_ORCHESTRATE_SOCKET`, and the reply `Output` is printed as NOTA.
//! Meta-policy requests belong to the sibling `meta-orchestrate` CLI.

use std::{env, fs, path::PathBuf, process::ExitCode};

use nota::{NotaDecodeError, NotaSource};
use orchestrate::{LaneAgeReport, OrdinarySignalTransport, TransportError};
use signal_orchestrate::schema::lib::{Input, Output};
use thiserror::Error;
use triad_runtime::{ArgumentError, ComponentArgument, ComponentCommand};

const ORDINARY_SOCKET_VARIABLE: &str = "PERSONA_ORCHESTRATE_SOCKET";

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
        // A lane observation carries per-record ages as raw nanosecond counts on
        // the wire. Stdout stays the pure NOTA reply so machine readers can decode
        // it unchanged; the human-readable relative-age summary ("3.42 days", not
        // a nanosecond integer) goes to stderr, the human-diagnostic channel.
        if let Output::LanesObserved(lanes) = &output {
            eprint!("{}", LaneAgeReport::from_observation(lanes).render());
        }
        println!("{output}");
        Ok(())
    }

    fn socket_path(&self) -> Result<String, OrchestrateCliError> {
        match env::var(ORDINARY_SOCKET_VARIABLE) {
            Ok(socket) => Ok(socket),
            Err(_) => Ok(Self::primary_workspace_socket()?.display().to_string()),
        }
    }

    fn primary_workspace_socket() -> Result<PathBuf, OrchestrateCliError> {
        let home =
            env::var("HOME").map_err(|source| OrchestrateCliError::HomeDirectory { source })?;
        Ok(PathBuf::from(home)
            .join("primary")
            .join("orchestrate")
            .join("orchestrate.sock"))
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

    #[error("HOME environment variable is unavailable: {source}")]
    HomeDirectory {
        #[source]
        source: env::VarError,
    },

    #[error("transport error: {0}")]
    Transport(#[from] TransportError),
}
