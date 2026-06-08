//! The orchestrate CLI — the daemon's first client.
//!
//! It speaks the schema-emitted `Input` frame the actor-shell daemon decodes
//! (the same short-header wire spirit's CLI speaks), not the contract
//! `ExchangeFrame` the retired `signal_cli!` client sent. One NOTA argument is
//! parsed as either an ordinary working `Input` or a meta `Input`; whichever
//! parses selects the tier, and the request is exchanged on that tier's socket
//! (`PERSONA_ORCHESTRATE_SOCKET` / `PERSONA_ORCHESTRATE_META_SOCKET`). The reply
//! `Output` is printed as NOTA.

use std::{env, fs, path::PathBuf, process::ExitCode};

use meta_signal_orchestrate::schema::lib::Input as MetaInput;
use nota_next::{NotaDecodeError, NotaSource};
use orchestrate::{MetaSignalTransport, OrdinarySignalTransport, TransportError};
use signal_orchestrate::schema::lib::Input;
use thiserror::Error;
use triad_runtime::{ArgumentError, ComponentArgument, ComponentCommand};

const ORDINARY_SOCKET_VARIABLE: &str = "PERSONA_ORCHESTRATE_SOCKET";
const META_SOCKET_VARIABLE: &str = "PERSONA_ORCHESTRATE_META_SOCKET";
const DEFAULT_ORDINARY_SOCKET: &str = "/tmp/orchestrate.sock";
const DEFAULT_META_SOCKET: &str = "/tmp/orchestrate-meta.sock";

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
        let request_text = RequestText::new(self.argument_text()?);
        match request_text.route()? {
            RequestSource::Ordinary(input) => {
                let socket = env::var(ORDINARY_SOCKET_VARIABLE)
                    .unwrap_or_else(|_| String::from(DEFAULT_ORDINARY_SOCKET));
                let (_route, output) =
                    OrdinarySignalTransport::connect(socket)?.exchange(&input)?;
                println!("{output}");
            }
            RequestSource::Meta(input) => {
                let socket = env::var(META_SOCKET_VARIABLE)
                    .unwrap_or_else(|_| String::from(DEFAULT_META_SOCKET));
                let (_route, output) = MetaSignalTransport::connect(socket)?.exchange(&input)?;
                println!("{output}");
            }
        }
        Ok(())
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

/// The parsed CLI request, tagged with its tier. The NOTA argument is tried
/// against the ordinary working `Input` first, then the meta `Input`; the
/// matching tier carries its decoded request.
enum RequestSource {
    Ordinary(Input),
    Meta(MetaInput),
}

/// The unparsed CLI NOTA argument awaiting tier routing.
struct RequestText {
    text: String,
}

impl RequestText {
    fn new(text: String) -> Self {
        Self { text }
    }

    fn route(self) -> Result<RequestSource, OrchestrateCliError> {
        if let Ok(input) = NotaSource::new(&self.text).parse::<Input>() {
            return Ok(RequestSource::Ordinary(input));
        }
        NotaSource::new(&self.text)
            .parse::<MetaInput>()
            .map(RequestSource::Meta)
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

    #[error("invalid orchestrate request NOTA (neither an ordinary nor a meta input): {0}")]
    NotaDecode(NotaDecodeError),

    #[error("transport error: {0}")]
    Transport(#[from] TransportError),
}
