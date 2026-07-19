//! The orchestrate CLI — the daemon's first client.
//!
//! It speaks the schema-emitted `Input` frame the actor-shell daemon decodes
//! (the same short-header wire spirit's CLI speaks), not the contract
//! `ExchangeFrame` the retired `signal_cli!` client sent. One NOTA argument
//! lowers through a single request/presentation interpretation pipeline:
//! ordinary contract input is shorthand for human presentation, while
//! `(Explicit (Canonical (Observe Lanes)))` preserves the daemon's canonical
//! reply.
//! Meta-policy requests belong to the sibling `meta-orchestrate` CLI.

use std::{env, fs, path::PathBuf, process::ExitCode};

use nota::{NotaDecodeError, NotaSource};
use orchestrate::{
    ExplicitOrchestratorInvocation, OrdinarySignalTransport, ResolvedOrchestratorInvocation,
    TransportError,
};
use signal_orchestrate::schema::lib::Input;
use thiserror::Error;
use triad_runtime::{ArgumentError, ComponentArgument, ComponentCommand};

const ORDINARY_SOCKET_VARIABLE: &str = "PERSONA_ORCHESTRATE_SOCKET";

fn main() -> ExitCode {
    match OrchestratorCli::from_environment().run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("orchestrate: {error}");
            ExitCode::FAILURE
        }
    }
}

struct OrchestratorCli {
    command: ComponentCommand,
}

impl OrchestratorCli {
    fn from_environment() -> Self {
        Self {
            command: ComponentCommand::from_environment(),
        }
    }

    fn run(&self) -> Result<(), OrchestratorCliError> {
        let invocation = RequestText::new(self.argument_text()?).parse()?;
        let (_route, output) =
            OrdinarySignalTransport::connect(self.socket_path()?)?.exchange(invocation.input())?;
        println!(
            "{}",
            invocation.presentation().present(&output).to_stdout_nota()
        );
        Ok(())
    }

    fn socket_path(&self) -> Result<String, OrchestratorCliError> {
        match env::var(ORDINARY_SOCKET_VARIABLE) {
            Ok(socket) => Ok(socket),
            Err(_) => Ok(Self::primary_workspace_socket()?.display().to_string()),
        }
    }

    fn primary_workspace_socket() -> Result<PathBuf, OrchestratorCliError> {
        let home =
            env::var("HOME").map_err(|source| OrchestratorCliError::HomeDirectory { source })?;
        Ok(PathBuf::from(home)
            .join("primary")
            .join("orchestrate")
            .join("orchestrate.sock"))
    }

    fn argument_text(&self) -> Result<String, OrchestratorCliError> {
        match self.command.nota_argument()? {
            ComponentArgument::InlineNota(argument) => Ok(argument.into_string()),
            ComponentArgument::NotaFile(file) => Self::read_nota_file(file.into_path()),
            ComponentArgument::SignalFile(file) => Self::read_nota_file(file.into_path()),
        }
    }

    fn read_nota_file(path: PathBuf) -> Result<String, OrchestratorCliError> {
        fs::read_to_string(&path)
            .map_err(|source| OrchestratorCliError::ReadNotaFile { path, source })
    }
}

/// The unparsed CLI NOTA argument awaiting shorthand/explicit lowering.
struct RequestText {
    text: String,
}

impl RequestText {
    fn new(text: String) -> Self {
        Self { text }
    }

    fn parse(self) -> Result<ResolvedOrchestratorInvocation, OrchestratorCliError> {
        let source = NotaSource::new(&self.text);
        match source.parse::<ExplicitOrchestratorInvocation>() {
            Ok(explicit) => Ok(explicit.into_resolved()),
            Err(_) => source
                .parse::<Input>()
                .map(ResolvedOrchestratorInvocation::human_shorthand)
                .map_err(OrchestratorCliError::NotaDecode),
        }
    }
}

#[derive(Debug, Error)]
enum OrchestratorCliError {
    #[error("component argument error: {0}")]
    Argument(#[from] ArgumentError),

    #[error("failed to read NOTA file {}: {source}", path.display())]
    ReadNotaFile {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("invalid ordinary orchestrate invocation NOTA: {0}")]
    NotaDecode(NotaDecodeError),

    #[error("HOME environment variable is unavailable: {source}")]
    HomeDirectory {
        #[source]
        source: env::VarError,
    },

    #[error("transport error: {0}")]
    Transport(#[from] TransportError),
}
