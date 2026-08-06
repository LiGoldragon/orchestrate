//! The orchestrate CLI — the daemon's first client.
//!
//! It speaks the canonical `signal-orchestrate` contract Frame. One Dotos argument
//! lowers through a single request/presentation interpretation pipeline:
//! ordinary contract input is shorthand for human presentation, while
//! `(Explicit (Canonical (Observe Lanes)))` preserves the daemon's canonical
//! reply.
//! Meta-policy requests belong to the sibling `meta-orchestrate` CLI.

use std::{env, fs, path::PathBuf, process::ExitCode};

use dotos::{DotosDecodeError, DotosSource};
use orchestrate::{
    ExplicitOrchestratorInvocation, OrdinarySignalTransport, ResolvedOrchestratorInvocation,
    TransportError,
};
use signal_orchestrate::OrchestrateRequest;
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
        let output =
            OrdinarySignalTransport::connect(self.socket_path()?)?.exchange(invocation.input())?;
        println!(
            "{}",
            invocation.presentation().present(&output).to_stdout_dotos()
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
        match self.command.dotos_argument()? {
            ComponentArgument::InlineDotos(argument) => Ok(argument.into_string()),
            ComponentArgument::DotosFile(file) => Self::read_dotos_file(file.into_path()),
            ComponentArgument::SignalFile(file) => Self::read_dotos_file(file.into_path()),
        }
    }

    fn read_dotos_file(path: PathBuf) -> Result<String, OrchestratorCliError> {
        fs::read_to_string(&path)
            .map_err(|source| OrchestratorCliError::ReadDotosFile { path, source })
    }
}

/// The unparsed CLI Dotos argument awaiting shorthand/explicit lowering.
struct RequestText {
    text: String,
}

impl RequestText {
    fn new(text: String) -> Self {
        Self { text }
    }

    fn parse(self) -> Result<ResolvedOrchestratorInvocation, OrchestratorCliError> {
        let source = DotosSource::new(&self.text);
        match source.parse::<ExplicitOrchestratorInvocation>() {
            Ok(explicit) => Ok(explicit.into_resolved()),
            Err(_) => source
                .parse::<OrchestrateRequest>()
                .map(ResolvedOrchestratorInvocation::human_shorthand)
                .map_err(OrchestratorCliError::DotosDecode),
        }
    }
}

#[derive(Debug, Error)]
enum OrchestratorCliError {
    #[error("component argument error: {0}")]
    Argument(#[from] ArgumentError),

    #[error("failed to read Dotos file {}: {source}", path.display())]
    ReadDotosFile {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("invalid ordinary orchestrate invocation Dotos: {0}")]
    DotosDecode(DotosDecodeError),

    #[error("HOME environment variable is unavailable: {source}")]
    HomeDirectory {
        #[source]
        source: env::VarError,
    },

    #[error("transport error: {0}")]
    Transport(#[from] TransportError),
}
