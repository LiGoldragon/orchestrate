//! Native Datom text boundary for ordinary path-lock registration.

use std::{env, ffi::OsString, process::ExitCode};

use datom::{
    EvidencedRealizing, EvidencedTextualizing, PathLockText, ProjectionViewing, RealizationViewing,
};
use orchestrate::{
    NativePathLockRegistered, NativePathLockRegistrationRejected, OrchestrateReply,
    OrchestrateRequest, OrdinarySignalTransport, PathLock, TransportError,
};
use protos::SourceText;
use thiserror::Error;

const ORDINARY_SOCKET_VARIABLE: &str = "ORCHESTRATE_ORDINARY_SOCKET";

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
    input: String,
    socket: String,
}

impl OrchestrateCli {
    fn from_environment() -> Self {
        Self {
            input: String::new(),
            socket: String::new(),
        }
    }

    fn run(mut self) -> Result<(), OrchestrateCliError> {
        self.input = Self::one_textual_argument(env::args_os().skip(1))?;
        self.socket = env::var(ORDINARY_SOCKET_VARIABLE)
            .map_err(|_| OrchestrateCliError::MissingSocketConfiguration)?;
        let native = PathLockText {
            source: SourceText(self.input),
        }
        .realize_evidenced()
        .map_err(OrchestrateCliError::Datom)?
        .value()
        .clone();
        let lock = PathLock::try_from(native).map_err(OrchestrateCliError::Datom)?;
        let reply = OrdinarySignalTransport::connect(&self.socket)?
            .exchange(&OrchestrateRequest::Register(lock))?;
        println!("{}", Self::canonical_reply(reply)?);
        Ok(())
    }

    fn one_textual_argument(
        arguments: impl IntoIterator<Item = OsString>,
    ) -> Result<String, OrchestrateCliError> {
        let arguments = arguments.into_iter().collect::<Vec<_>>();
        let [input]: [OsString; 1] = arguments.try_into().map_err(|arguments: Vec<OsString>| {
            OrchestrateCliError::ArgumentCount {
                actual: arguments.len(),
            }
        })?;
        input
            .into_string()
            .map_err(|_| OrchestrateCliError::NonUtf8Argument)
    }

    fn canonical_reply(reply: OrchestrateReply) -> Result<String, OrchestrateCliError> {
        match reply {
            OrchestrateReply::PathLockRegistered(registered) => {
                Ok(NativePathLockRegistered::try_from(registered)
                    .map_err(OrchestrateCliError::Datom)?
                    .textualize_evidenced()
                    .map_err(OrchestrateCliError::Datom)?
                    .text()
                    .source
                    .0
                    .clone())
            }
            OrchestrateReply::PathLockRegistrationRejected(rejected) => {
                Ok(NativePathLockRegistrationRejected::try_from(rejected)
                    .map_err(OrchestrateCliError::Datom)?
                    .textualize_evidenced()
                    .map_err(OrchestrateCliError::Datom)?
                    .text()
                    .source
                    .0
                    .clone())
            }
        }
    }
}

#[derive(Debug, Error)]
enum OrchestrateCliError {
    #[error("expected exactly one native Datom argument, received {actual}")]
    ArgumentCount { actual: usize },
    #[error("the native Datom argument is not valid UTF-8")]
    NonUtf8Argument,
    #[error("ORCHESTRATE_ORDINARY_SOCKET must name the ordinary daemon socket")]
    MissingSocketConfiguration,
    #[error("native Datom path-lock conversion failed: {0:?}")]
    Datom(datom::DatomFault),
    #[error("Signal transport failed: {0}")]
    Transport(#[from] TransportError),
}
