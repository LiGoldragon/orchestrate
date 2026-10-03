//! Process-level proof for the meta Datom client boundary.
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

use meta_signal_orchestrate::{Query, Response};
use signal::{
    Answer, Contracted, Delivery, Dispatch, FrameCapacity, FrameReading, FrameWriting,
    HandshakeReceipt, HandshakeRejection, Restorable, Signal, Signalizable,
};
use signal_orchestrate::{ConfigurationReceipt, OrchestrateNexusConfiguration};

/// A query this contract reads.
const QUERY: &str = "ReverseMetaConfiguration";

struct ClientHarness {
    _directory: tempfile::TempDir,
    socket: PathBuf,
}

trait CreatesClientHarness: Sized {
    fn create() -> Self;
    fn invoke(&self, argument: Option<&str>) -> Output;
    fn answer_once(&self, response: Response) -> std::thread::JoinHandle<Query>;
    fn refuse_greeting(&self, digest: i64) -> std::thread::JoinHandle<()>;
}

/// The fixture's side of signal's exchange layer, framed with the shared crate.
trait Answers {
    fn dispatched(&mut self) -> Dispatch<Query>;
    fn deliver(&mut self, delivery: Delivery<Response>);
}

impl Answers for std::os::unix::net::UnixStream {
    fn dispatched(&mut self) -> Dispatch<Query> {
        let body = self
            .read_frame(FrameCapacity::default())
            .expect("read a dispatch frame");
        Signal::<Dispatch<Query>>::from(Vec::from(body))
            .restore()
            .expect("restore the dispatch")
    }

    fn deliver(&mut self, delivery: Delivery<Response>) {
        let signal = delivery.signalize().expect("archive the delivery");
        self.write_frame(&signal, FrameCapacity::default())
            .expect("write the delivery");
    }
}

impl CreatesClientHarness for ClientHarness {
    fn create() -> Self {
        let directory = tempfile::tempdir().expect("temporary meta client");
        let socket = directory.path().join("meta.sock");
        Self {
            _directory: directory,
            socket,
        }
    }

    fn invoke(&self, argument: Option<&str>) -> Output {
        let mut command = Command::new(env!("CARGO_BIN_EXE_orchestrate-meta"));
        command.env("ORCHESTRATE_META_SOCKET", &self.socket);
        if let Some(argument) = argument {
            command.arg(argument);
        }
        command.output().expect("run meta client")
    }

    fn answer_once(&self, response: Response) -> std::thread::JoinHandle<Query> {
        let listener = UnixListener::bind(&self.socket).expect("bind meta fixture socket");
        std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept meta client");
            let Dispatch::Greet(greeting) = stream.dispatched() else {
                panic!("the client greets before anything else");
            };
            assert_eq!(
                greeting,
                Query::greeting(),
                "the client greets with the digest of the contract it was built from"
            );
            stream.deliver(Delivery::Greeted(Query::receipt(&greeting)));
            let Dispatch::Open(opening) = stream.dispatched() else {
                panic!("then opens one exchange");
            };
            stream.deliver(Delivery::Answer(Answer {
                exchange: opening.exchange,
                response,
            }));
            opening.query
        })
    }

    fn refuse_greeting(&self, digest: i64) -> std::thread::JoinHandle<()> {
        let listener = UnixListener::bind(&self.socket).expect("bind meta fixture socket");
        std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept meta client");
            stream.dispatched();
            stream.deliver(Delivery::Greeted(HandshakeReceipt::GreetingRefused(
                HandshakeRejection::ContractMismatch(digest),
            )));
        })
    }
}

#[test]
fn client_actualizes_datoms_and_textualizes_typed_responses() {
    let harness = ClientHarness::create();
    let configure = OrchestrateNexusConfiguration {
        ordinary_socket_path: "/tmp/ordinary.sock".to_owned(),
        meta_socket_path: "/tmp/meta.sock".to_owned(),
    };
    let server = harness.answer_once(Response::Configured(ConfigurationReceipt {
        orchestrate_nexus_configuration: configure.clone(),
        meta_configure_done: true,
    }));
    let output = harness.invoke(Some("Configure.{ /tmp/ordinary.sock /tmp/meta.sock }"));
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).unwrap(),
        "Configured.{ { /tmp/ordinary.sock /tmp/meta.sock } True }\n"
    );
    assert_eq!(
        server.join().expect("meta fixture server"),
        Query::Configure(configure),
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

    let malformed = harness.invoke(Some("Configure.{ broken"));
    assert!(!malformed.status.success());
    let stderr = String::from_utf8(malformed.stderr).unwrap();
    assert!(stderr.starts_with("Unreadable."), "{stderr}");
}

#[test]
fn a_refused_greeting_is_reported_as_vocabulary() {
    let harness = ClientHarness::create();
    let server = harness.refuse_greeting(42);
    let output = harness.invoke(Some(QUERY));
    assert!(!output.status.success());
    assert_eq!(
        String::from_utf8(output.stderr).unwrap(),
        "GreetingRefused.ContractMismatch.42\n"
    );
    server.join().expect("fixture server");
}
