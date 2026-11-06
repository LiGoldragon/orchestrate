//! Process-level proof for the meta Datom client boundary.
//!
//! The fixture server frames with `signal`, the same crate the client frames
//! with. Before this, client and fixture both hand-rolled a little-endian
//! prefix and agreed with each other while disagreeing with every other
//! component; framing through the shared crate is what makes that
//! impossible rather than merely unobserved.

use std::{
    ffi::OsStr,
    os::unix::{ffi::OsStringExt, net::UnixListener},
    path::PathBuf,
    process::{Command, Output},
    time::Duration,
};

use meta_signal_orchestrate::{Query, Response};
use signal::{
    Answer, ContractDigest, Contracted, Delivery, Dispatch, Ending, ExchangeFault, ExchangeId,
    FrameCapacity, FrameError, FrameReading, FrameWriting, HandshakeReceipt, HandshakeRejection,
    Restorable, Signal, Signalizable,
};
use signal_orchestrate::{
    ConfigurationPath, MetaConfigureDone, MetaSocketPath, OrdinarySocketPath,
};
use signal_orchestrate::{ConfigurationReceipt, OrchestrateNexusConfiguration};

/// A query this contract reads.
const QUERY: &str = "ReverseMetaConfiguration";

struct ClientHarness {
    _directory: tempfile::TempDir,
    socket: PathBuf,
}

#[derive(Clone, Copy)]
enum EndTarget {
    Opened,
    Other,
    Connection,
}

trait CreatesClientHarness: Sized {
    fn create() -> Self;
    fn invoke(&self, argument: Option<&str>) -> Output;
    fn invoke_os(&self, argument: Option<&OsStr>) -> Output;
    fn invoke_with_socket(&self, socket: Option<&OsStr>, argument: Option<&OsStr>) -> Output;
    fn answer_once(&self, response: Response) -> std::thread::JoinHandle<Query>;
    fn end_query(&self, target: EndTarget) -> std::thread::JoinHandle<Query>;
    fn end_greeting_on_an_exchange(&self) -> std::thread::JoinHandle<()>;
    fn refuse_greeting(&self, digest: ContractDigest) -> std::thread::JoinHandle<()>;
    fn greet_with_wrong_digest(&self, digest: ContractDigest) -> std::thread::JoinHandle<bool>;
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
        self.invoke_os(argument.map(OsStr::new))
    }

    fn invoke_os(&self, argument: Option<&OsStr>) -> Output {
        self.invoke_with_socket(Some(self.socket.as_os_str()), argument)
    }

    fn invoke_with_socket(&self, socket: Option<&OsStr>, argument: Option<&OsStr>) -> Output {
        let mut command = Command::new(env!("CARGO_BIN_EXE_orchestrate-meta"));
        command.env_remove("ORCHESTRATE_META_SOCKET");
        if let Some(socket) = socket {
            command.env("ORCHESTRATE_META_SOCKET", socket);
        }
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

    fn end_query(&self, target: EndTarget) -> std::thread::JoinHandle<Query> {
        let listener = UnixListener::bind(&self.socket).expect("bind meta fixture socket");
        std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept meta client");
            let Dispatch::Greet(greeting) = stream.dispatched() else {
                panic!("the client greets before anything else");
            };
            stream.deliver(Delivery::Greeted(Query::receipt(&greeting)));
            let Dispatch::Open(opening) = stream.dispatched() else {
                panic!("then opens one exchange");
            };
            let ending = match target {
                EndTarget::Opened => {
                    Ending::faulted(opening.exchange, ExchangeFault::UnknownExchange)
                }
                EndTarget::Other => Ending::faulted(
                    ExchangeId(opening.exchange.0.wrapping_add(1)),
                    ExchangeFault::UnknownExchange,
                ),
                EndTarget::Connection => Ending::connection_faulted(ExchangeFault::UnknownExchange),
            };
            stream.deliver(Delivery::End(ending));
            opening.query
        })
    }

    fn end_greeting_on_an_exchange(&self) -> std::thread::JoinHandle<()> {
        let listener = UnixListener::bind(&self.socket).expect("bind meta fixture socket");
        std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept meta client");
            stream.dispatched();
            stream.deliver(Delivery::End(Ending::faulted(
                ExchangeId(1),
                ExchangeFault::UnknownExchange,
            )));
        })
    }

    fn refuse_greeting(&self, digest: ContractDigest) -> std::thread::JoinHandle<()> {
        let listener = UnixListener::bind(&self.socket).expect("bind meta fixture socket");
        std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept meta client");
            stream.dispatched();
            stream.deliver(Delivery::Greeted(HandshakeReceipt::GreetingRefused(
                HandshakeRejection::ContractMismatch(digest),
            )));
        })
    }

    fn greet_with_wrong_digest(&self, digest: ContractDigest) -> std::thread::JoinHandle<bool> {
        let listener = UnixListener::bind(&self.socket).expect("bind meta fixture socket");
        std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept meta client");
            stream.dispatched();
            stream.deliver(Delivery::Greeted(HandshakeReceipt::Greeted(digest)));
            stream
                .set_read_timeout(Some(Duration::from_secs(2)))
                .expect("bound the wait for an invalid query");
            matches!(
                stream.read_frame(FrameCapacity::default()),
                Err(FrameError::Io(_))
            )
        })
    }
}

#[test]
fn socket_environment_failures_name_whether_the_value_is_missing_or_non_utf8() {
    let harness = ClientHarness::create();
    let query = OsStr::new(QUERY);
    let missing = harness.invoke_with_socket(None, Some(query));
    assert_eq!(missing.status.code(), Some(1));
    assert_eq!(
        String::from_utf8(missing.stderr).unwrap(),
        "orchestrate-meta: ORCHESTRATE_META_SOCKET is required\n"
    );

    let non_utf8 = std::ffi::OsString::from_vec(vec![0xff]);
    let unrepresentable = harness.invoke_with_socket(Some(&non_utf8), Some(query));
    assert_eq!(unrepresentable.status.code(), Some(1));
    assert_eq!(
        String::from_utf8(unrepresentable.stderr).unwrap(),
        "orchestrate-meta: ORCHESTRATE_META_SOCKET must be UTF-8\n"
    );
}

#[test]
fn a_non_utf8_query_is_a_controlled_invocation_failure() {
    let harness = ClientHarness::create();
    let argument = std::ffi::OsString::from_vec(vec![0xff]);
    let output = harness.invoke_os(Some(&argument));

    assert_eq!(output.status.code(), Some(1));
    assert_eq!(
        String::from_utf8(output.stderr).unwrap(),
        "orchestrate-meta: inline Datom query must be UTF-8\n"
    );
}

#[test]
fn client_actualizes_datoms_and_textualizes_typed_responses() {
    let harness = ClientHarness::create();
    let configure = OrchestrateNexusConfiguration {
        ordinary_socket_path: OrdinarySocketPath(ConfigurationPath(
            "/tmp/ordinary.sock".to_owned(),
        )),
        meta_socket_path: MetaSocketPath(ConfigurationPath("/tmp/meta.sock".to_owned())),
    };
    let server = harness.answer_once(Response::Configured(ConfigurationReceipt {
        orchestrate_nexus_configuration: configure.clone(),
        meta_configure_done: MetaConfigureDone(true),
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
    let server = harness.refuse_greeting(ContractDigest(42));
    let output = harness.invoke(Some(QUERY));
    assert!(!output.status.success());
    assert_eq!(
        String::from_utf8(output.stderr).unwrap(),
        "GreetingRefused.ContractMismatch.42\n"
    );
    server.join().expect("fixture server");
}

#[test]
fn a_wrong_greeted_digest_never_opens_an_exchange() {
    let harness = ClientHarness::create();
    let server =
        harness.greet_with_wrong_digest(ContractDigest(Query::contract_digest().0.wrapping_add(1)));
    let output = harness.invoke(Some(QUERY));
    assert!(!output.status.success());
    assert!(
        server.join().expect("meta fixture server"),
        "the client closes before sending a query"
    );
}

#[test]
fn an_end_for_no_exchange_the_client_opened_is_unexpected() {
    for at_greeting in [true, false] {
        let harness = ClientHarness::create();
        let server = if at_greeting {
            let handle = harness.end_greeting_on_an_exchange();
            EitherEnding::Greeting(handle)
        } else {
            let handle = harness.end_query(EndTarget::Other);
            EitherEnding::Query(handle)
        };
        let output = harness.invoke(Some(QUERY));

        assert!(!output.status.success());
        assert!(
            String::from_utf8(output.stderr)
                .unwrap()
                .contains("frame for no exchange this client opened")
        );
        server.join();
    }
}

enum EitherEnding {
    Greeting(std::thread::JoinHandle<()>),
    Query(std::thread::JoinHandle<Query>),
}

trait JoinsEnding {
    fn join(self);
}

impl JoinsEnding for EitherEnding {
    fn join(self) {
        match self {
            Self::Greeting(handle) => handle.join().expect("greeting-ending fixture"),
            Self::Query(handle) => {
                handle.join().expect("query-ending fixture");
            }
        }
    }
}

#[test]
fn ends_for_the_opened_exchange_or_connection_remain_faults() {
    for target in [EndTarget::Opened, EndTarget::Connection] {
        let harness = ClientHarness::create();
        let server = harness.end_query(target);
        let output = harness.invoke(Some(QUERY));

        assert!(!output.status.success());
        assert!(
            String::from_utf8(output.stderr)
                .unwrap()
                .starts_with("ExchangeFaulted.")
        );
        server.join().expect("query-ending fixture");
    }
}
