//! Process-level proof for the ordinary Datom client boundary.
//!
//! The fixture server frames with `signal`, the same crate the client frames
//! with. Before this, client and fixture both hand-rolled a little-endian
//! prefix and agreed with each other while disagreeing with every other
//! component; framing through the shared crate is what makes that
//! impossible rather than merely unobserved.

use std::{
    os::unix::net::UnixListener,
    path::PathBuf,
    process::{Command, Output},
};

use signal::{FrameCapacity, FrameReading, FrameWriting, Restorable, Signal, Signalizable};
use signal_orchestrate::{Observation, ObserveSelection, Query, Response};

struct ClientHarness {
    _directory: tempfile::TempDir,
    socket: PathBuf,
}

trait CreatesClientHarness: Sized {
    fn create() -> Self;
    fn invoke(&self, argument: Option<&str>) -> Output;
    fn answer_once(&self, response: Response) -> std::thread::JoinHandle<Query>;
}

impl CreatesClientHarness for ClientHarness {
    fn create() -> Self {
        let directory = tempfile::tempdir().expect("temporary ordinary client");
        let socket = directory.path().join("ordinary.sock");
        Self {
            _directory: directory,
            socket,
        }
    }

    fn invoke(&self, argument: Option<&str>) -> Output {
        let mut command = Command::new(env!("CARGO_BIN_EXE_orchestrate"));
        command.env("ORCHESTRATE_SOCKET", &self.socket);
        if let Some(argument) = argument {
            command.arg(argument);
        }
        command.output().expect("run ordinary client")
    }

    fn answer_once(&self, response: Response) -> std::thread::JoinHandle<Query> {
        let listener = UnixListener::bind(&self.socket).expect("bind ordinary fixture socket");
        std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept ordinary client");
            let capacity = FrameCapacity::default();
            let body = stream.read_frame(capacity).expect("read query frame");
            let query = Signal::<Query>::from(Vec::from(body))
                .restore()
                .expect("restore ordinary query");
            let signal = response.signalize().expect("archive ordinary response");
            stream
                .write_frame(&signal, capacity)
                .expect("write response frame");
            query
        })
    }
}

#[test]
fn client_actualizes_datoms_and_textualizes_typed_responses() {
    let harness = ClientHarness::create();
    let server = harness.answer_once(Response::Observed(Observation::Locks(Vec::new())));
    let output = harness.invoke(Some("Observe.Locks"));
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).unwrap(),
        "Observed.Locks.[]\n"
    );
    assert_eq!(
        server.join().expect("ordinary fixture server"),
        Query::Observe(ObserveSelection::Locks),
    );
}

#[test]
fn client_describes_contracts_and_reports_datom_failures() {
    let harness = ClientHarness::create();
    let description = harness.invoke(None);
    assert!(description.status.success());
    let stdout = String::from_utf8(description.stdout).unwrap();
    assert!(stdout.contains("Signal"));
    assert!(stdout.contains("Library"));

    let malformed = harness.invoke(Some("Lock.{ broken"));
    assert!(!malformed.status.success());
    let stderr = String::from_utf8(malformed.stderr).unwrap();
    assert!(stderr.starts_with("Unreadable."), "{stderr}");
}
