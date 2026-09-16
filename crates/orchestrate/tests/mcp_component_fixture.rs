//! Bounded fixture for a future single-string MCP component selector.
//!
//! This fixture proves the already-declared Orchestrate leg only. The other
//! component labels are routing vocabulary, not a new public Signal contract
//! or an installed MCP server.

use std::{
    os::unix::net::UnixListener,
    path::PathBuf,
    process::{Command, Output},
};

use signal::{FrameCapacity, FrameReading, FrameWriting, Restorable, Signal, Signalizable};
use signal_orchestrate::{Observation, ObserveSelection, Query, Response};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Component {
    Orchestrate,
    Message,
    Persona,
    Psyche,
    Flow,
}

trait SingleStringComponent: Sized {
    fn select(source: &str) -> Option<Self>;
}

impl SingleStringComponent for Component {
    fn select(source: &str) -> Option<Self> {
        match source {
            "Orchestrate" => Some(Self::Orchestrate),
            "Message" => Some(Self::Message),
            "Persona" => Some(Self::Persona),
            "Psyche" => Some(Self::Psyche),
            "Flow" => Some(Self::Flow),
            _ => None,
        }
    }
}

struct OrchestrateHarness {
    _directory: tempfile::TempDir,
    socket: PathBuf,
}

trait CreatesOrchestrateHarness: Sized {
    fn create() -> Self;
    fn invoke(&self, source: &str) -> Output;
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

    fn invoke(&self, source: &str) -> Output {
        Command::new(env!("CARGO_BIN_EXE_orchestrate"))
            .env("ORCHESTRATE_SOCKET", &self.socket)
            .arg(source)
            .output()
            .expect("run Datom edge client")
    }

    fn answer_once(&self, response: Response) -> std::thread::JoinHandle<Query> {
        let listener = UnixListener::bind(&self.socket).expect("bind fixture socket");
        std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept client");
            let capacity = FrameCapacity::default();
            let request = Signal::<Query>::from(Vec::from(stream.read_frame(capacity).expect("read Signal")))
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
fn single_string_component_selection_routes_the_declared_orchestrate_fixture() {
    assert_eq!(Component::select("Orchestrate"), Some(Component::Orchestrate));
    assert_eq!(Component::select("Message"), Some(Component::Message));
    assert_eq!(Component::select("Persona"), Some(Component::Persona));
    assert_eq!(Component::select("Psyche"), Some(Component::Psyche));
    assert_eq!(Component::select("Flow"), Some(Component::Flow));
    assert_eq!(Component::select("unapproved"), None);

    let fixture = OrchestrateHarness::create();
    let server = fixture.answer_once(Response::Observed(Observation::Locks(Vec::new())));
    let output = fixture.invoke("Observe.Locks");

    assert!(output.status.success());
    assert_eq!(String::from_utf8(output.stdout).unwrap(), "Observed.Locks.[]\n");
    assert_eq!(
        server.join().expect("fixture server"),
        Query::Observe(ObserveSelection::Locks)
    );
}
