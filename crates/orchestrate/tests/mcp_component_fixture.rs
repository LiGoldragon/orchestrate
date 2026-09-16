//! Process-level proof for the bounded single-string MCP component handler.

use std::{os::unix::net::UnixListener, path::PathBuf};

use signal::{FrameCapacity, FrameReading, FrameWriting, Restorable, Signal, Signalizable};
use signal_orchestrate::{Observation, ObserveSelection, Query, Response};

#[allow(dead_code)]
#[path = "../src/mcp_component.rs"]
mod mcp_component;

use mcp_component::{Component, ComponentError, ComponentResult, handle_component_at_socket};

struct OrchestrateHarness {
    _directory: tempfile::TempDir,
    socket: PathBuf,
}

trait CreatesOrchestrateHarness: Sized {
    fn create() -> Self;
    fn handle(&self, arguments: &[String]) -> Result<ComponentResult, ComponentError>;
    fn answer_once(&self, response: Response) -> std::thread::JoinHandle<Query>;
}

impl CreatesOrchestrateHarness for OrchestrateHarness {
    fn create() -> Self {
        let directory = tempfile::tempdir().expect("temporary Orchestrate socket");
        let socket = directory.path().join("orchestrate.sock");
        Self {
            _directory: directory,
            socket,
        }
    }

    fn handle(&self, arguments: &[String]) -> Result<ComponentResult, ComponentError> {
        // The handler delegates to the real CLI, which reads this boundary.
        handle_component_at_socket(
            PathBuf::from(env!("CARGO_BIN_EXE_orchestrate")),
            self.socket.clone(),
            arguments,
        )
    }

    fn answer_once(&self, response: Response) -> std::thread::JoinHandle<Query> {
        let listener = UnixListener::bind(&self.socket).expect("bind fixture socket");
        std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept client");
            let capacity = FrameCapacity::default();
            let request =
                Signal::<Query>::from(Vec::from(stream.read_frame(capacity).expect("read Signal")))
                    .restore()
                    .expect("restore typed query");
            stream
                .write_frame(&response.signalize().expect("archive response"), capacity)
                .expect("write Signal reply");
            request
        })
    }
}

#[test]
fn one_string_orchestrate_component_uses_the_cli_signal_socket_and_typed_response() {
    let fixture = OrchestrateHarness::create();
    let server = fixture.answer_once(Response::Observed(Observation::Locks(Vec::new())));
    let result = fixture.handle(&["Orchestrate".to_owned()]);
    assert_eq!(
        result,
        Ok(ComponentResult::Orchestrate("Observed.Locks.[]".to_owned()))
    );
    assert_eq!(
        server.join().expect("fixture server"),
        Query::Observe(ObserveSelection::Locks)
    );
}

#[test]
fn malformed_and_unsupported_component_arguments_are_explicit() {
    let fixture = OrchestrateHarness::create();
    assert_eq!(fixture.handle(&[]), Err(ComponentError::Arguments));
    assert_eq!(
        fixture.handle(&["Orchestrate".to_owned(), "Message".to_owned()]),
        Err(ComponentError::Arguments)
    );
    assert_eq!(
        fixture.handle(&["Unknown".to_owned()]),
        Err(ComponentError::Unknown("Unknown".to_owned()))
    );
    for component in [
        Component::Message,
        Component::Persona,
        Component::Psyche,
        Component::Flow,
    ] {
        assert_eq!(
            fixture.handle(&[format!("{component:?}")]),
            Ok(ComponentResult::Unavailable(component))
        );
    }
}
